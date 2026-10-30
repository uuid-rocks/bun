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

  /**
   * A goroutine started by {@link go}: a promise for what its function returns.
   *
   * A goroutine keeps the event loop of the thread that started it alive until it settles. `Promise.all()` waits
   * for several; derived promises (`.then()`, `.catch()`) are plain `Promise`s.
   *
   * @category Concurrency
   */
  interface Goroutine<T> extends Promise<T> {
    /** Keeps the event loop alive until the goroutine settles (the default). Returns the goroutine. */
    ref(): this;
    /** Lets the process exit while the goroutine still runs. Returns the goroutine. */
    unref(): this;
  }

  /**
   * Runs a function on another thread, like Go's `go` statement.
   *
   * Goroutines run on a pool of threads that Bun starts as they are needed: as many as `GOMAXPROCS` (2 or more),
   * or as the machine has CPU cores. A goroutine runs on one thread until it settles. While it awaits, its thread runs other
   * goroutines. Goroutines on different threads run in parallel.
   *
   * `fn` must be a function that a module exports (ES module or CommonJS). Each pool thread imports that module
   * once, and calls the export with `args`. Module state is per thread, and `import.meta.main` is `false` there. A
   * closure, a bound or native function, or a function no module exports throws a `TypeError`
   * (`ERR_GOROUTINE_NOT_SENDABLE`), because its variables can't move to another thread.
   *
   * The arguments and the result are structured clones, as with `postMessage()`: a `SharedArrayBuffer` or a
   * {@link Channel} is shared, everything else is copied. Arguments that can't be cloned throw a `DataCloneError`.
   * If the function throws, the goroutine rejects with a copy of the error (its name, message, stack, `cause` and
   * own properties). A rejection that nothing handles is an unhandled rejection on the thread that called `go()`.
   *
   * @example
   * ```ts
   * // primes.ts
   * export function countPrimes(from: number, to: number) {
   *   let count = 0;
   *   for (let n = from; n < to; n++) if (isPrime(n)) count++;
   *   return count;
   * }
   *
   * // main.ts
   * import { countPrimes } from "./primes.ts";
   * const counts = await Promise.all([Bun.go(countPrimes, 0, 1e6), Bun.go(countPrimes, 1e6, 2e6)]);
   * ```
   *
   * @category Concurrency
   */
  function go<F extends (...args: any[]) => unknown>(fn: F, ...args: Parameters<F>): Goroutine<Awaited<ReturnType<F>>>;
  /**
   * Runs the default export of a module on another thread, like {@link go} with a function. `module` is resolved
   * the way `new Worker(module)` resolves its entry point: a relative path against the current directory, so prefer
   * `new URL("./task.ts", import.meta.url)`. In an executable built with `bun build --compile`, list the module as
   * an entry point of the build.
   *
   * @category Concurrency
   */
  function go<R = unknown>(module: string | URL, ...args: unknown[]): Goroutine<R>;
}
