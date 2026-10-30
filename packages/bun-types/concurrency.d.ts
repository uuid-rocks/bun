declare module "bun" {
  /**
   * A channel between threads, like Go's: send values on one end, receive them on the other, in the order they
   * were sent.
   *
   * A channel is shared, not copied. Send it to a `Worker` with `worker.postMessage()`, or through another
   * channel, and both threads use the same channel. Every value is a structured clone, even on one thread:
   * `SharedArrayBuffer`s and channels are shared, the `ArrayBuffer`s in `transfer` are moved, the rest is copied.
   *
   * A waiting `send()` or `receive()` does not keep the process alive.
   *
   * @example
   * ```ts
   * const jobs = new Bun.Channel<number>(16);
   * const worker = new Worker("./worker.ts");
   * worker.postMessage(jobs);
   *
   * for (let i = 0; i < 100; i++) await jobs.send(i);
   * jobs.close();
   *
   * // worker.ts
   * self.onmessage = async ({ data: jobs }) => {
   *   for await (const job of jobs) console.log(job);
   * };
   * ```
   *
   * @category Concurrency
   */
  class Channel<T = unknown> implements AsyncIterable<T>, Disposable {
    /**
     * @param capacity How many values the channel holds while no receiver waits. `0` (the default) makes every
     * `send()` wait for a `receive()`. An integer from `0` to `2 ** 32 - 1`; anything else throws a `RangeError`
     * (`ERR_OUT_OF_RANGE`), or a `TypeError` (`ERR_INVALID_ARG_TYPE`) if it is not a number.
     */
    constructor(capacity?: number);

    /** What the channel was created with. */
    readonly capacity: number;

    /** How many values the channel holds now, from `0` to `capacity`. Another thread can change it at any time. */
    readonly length: number;

    /** Whether any thread has called {@link close}. Buffered values can still be received. */
    readonly closed: boolean;

    /**
     * Sends a value. The promise resolves when a receiver takes the value or the channel buffers it: at once if a
     * receiver waits or `length < capacity`, otherwise once a receiver makes room.
     *
     * Rejects with `ERR_CHANNEL_CLOSED` if the channel is closed, now or while the send waits; the value is then
     * dropped. Rejects with a `DataCloneError` if the value cannot be cloned.
     *
     * @param options.signal Stops waiting: the promise rejects with `signal.reason`, and the value is not sent. A
     * value a receiver took before the signal aborted stays sent.
     * @param options.transfer `ArrayBuffer`s and `MessagePort`s to move instead of copy. They are detached when
     * `send()` is called, even if the send then fails.
     */
    send(value: T, options?: { signal?: AbortSignal; transfer?: Transferable[] }): Promise<void>;

    /**
     * Receives the oldest value: at once if one is buffered or a sender waits, otherwise when one is sent.
     *
     * Rejects with `ERR_CHANNEL_CLOSED` once the channel is closed and every buffered value has been received.
     * Use `for await` to receive until then without catching it.
     *
     * @param options.signal Stops waiting: the promise rejects with `signal.reason`, and no value is taken. A
     * value sent before the signal aborted is received.
     */
    receive(options?: { signal?: AbortSignal }): Promise<T>;

    /**
     * Closes the channel for every thread. Waiting and later `send()`s reject with `ERR_CHANNEL_CLOSED`. Receivers
     * receive the buffered values, then end. Closing a closed channel does nothing.
     */
    close(): void;

    /**
     * Receives until the channel is closed and empty. `break` ends the loop and leaves the channel open, so other
     * receivers keep receiving.
     */
    [Symbol.asyncIterator](): AsyncIterator<T, undefined>;

    /** {@link close}: `using channel = new Bun.Channel()` closes it at the end of the scope. */
    [Symbol.dispose](): void;
  }
}
