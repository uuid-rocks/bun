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
    [Symbol.asyncIterator](): AsyncIterableIterator<T, undefined>;

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
   * `fn` is either:
   * - a function that uses only its parameters, what it declares itself, and globals (`Bun`, `fetch`, `Atomics`,
   *   `require`, ...). Bun sends its source to the other thread and compiles it there once, as if it were where it
   *   is written: `await import("./x.ts")` and `require()` in it resolve from its module. A function that uses a
   *   variable of the code around it, an arrow function that uses the `this` or `arguments` around it, a function
   *   that uses `import.meta`, a method or a class throws a `TypeError` (`ERR_GOROUTINE_NOT_SENDABLE`) that names
   *   what it uses and where. Pass such values as arguments instead.
   * - a function that a module exports (ES module or CommonJS). Each pool thread imports that module once, and calls
   *   the export. Module state is per thread, and `import.meta.main` is `false` there. Bun prefers this when the
   *   function is exported, and it is the way for code that uses its module's imports.
   *
   * A bound or native function throws `ERR_GOROUTINE_NOT_SENDABLE`. So does a function no module exports while
   * code generation from strings is disallowed (`--disallow-code-generation-from-strings`).
   *
   * The arguments and the result are structured clones, as with `postMessage()`: a `SharedArrayBuffer` or a
   * {@link Channel} is shared, everything else is copied. An `AbortSignal` arrives as a signal that aborts when the
   * original does, with a clone of its reason (an `AbortError` if the reason can't be cloned). Arguments that can't be
   * cloned throw a `DataCloneError`. If the function throws, the goroutine rejects with a copy of the error (its name,
   * message, stack, `cause` and own properties). A rejection that nothing handles is an unhandled rejection on the
   * thread that called `go()`.
   *
   * @example
   * ```ts
   * const counts = await Promise.all(
   *   [0, 1e6].map(from =>
   *     Bun.go(
   *       (from, to) => {
   *         let count = 0;
   *         for (let n = from; n < to; n++) if (isPrime(n)) count++;
   *         return count;
   *         function isPrime(n: number) {
   *           for (let d = 2; d * d <= n; d++) if (n % d === 0) return false;
   *           return n > 1;
   *         }
   *       },
   *       from,
   *       from + 1e6,
   *     ),
   *   ),
   * );
   * ```
   *
   * @category Concurrency
   */
  function go<A extends unknown[], R>(fn: (...args: A) => R, ...args: A): Goroutine<Awaited<R>>;
  /**
   * Runs the default export of a module on another thread, like {@link go} with a function. `module` is resolved
   * the way `new Worker(module)` resolves its entry point: a relative path against the current directory, so prefer
   * `new URL("./task.ts", import.meta.url)`. The fragment of a URL names another export:
   * `new URL("./task.ts#countPrimes", import.meta.url)`.
   *
   * `bun build` finds `Bun.go(new URL("./task.ts", import.meta.url))` with a literal path, and `Bun.go(fn)` with an
   * imported `fn`, and makes each module an entry point of the build. List any other module as an entry point.
   *
   * @category Concurrency
   */
  function go<R = unknown>(module: string | URL, ...args: unknown[]): Goroutine<R>;

  /**
   * A case of {@link select}: a channel to receive from, or `[channel, value]` to send `value` on it.
   *
   * @category Concurrency
   */
  type SelectCase = Channel<any> | readonly [Channel<any>, unknown];

  /** @category Concurrency */
  interface SelectOptions {
    /**
     * Stops waiting: the promise rejects with `signal.reason`, and no case happens. Pass `AbortSignal.timeout(ms)`
     * for a timeout. A case that happened before the signal aborted stays done.
     */
    signal?: AbortSignal;
    /** Resolve with `{ index: -1 }` at once if no case is ready, like Go's `default:`. */
    default?: boolean;
  }

  /**
   * What {@link select} resolves to for the case at index `I`.
   *
   * @category Concurrency
   */
  type SelectCaseResult<Case, I extends number> = Case extends readonly [Channel<any>, unknown]
    ? { index: I; value: undefined; closed: false }
    : Case extends Channel<infer T>
      ? { index: I; value: T; closed: false } | { index: I; value: undefined; closed: true }
      : never;

  /**
   * What {@link select} resolves to: the result of the case that happened, typed by its index in `C`.
   *
   * @category Concurrency
   */
  type SelectResult<C extends readonly SelectCase[]> = number extends C["length"]
    ? SelectCaseResult<C[number], number>
    : { [K in keyof C]: K extends `${infer I extends number}` ? SelectCaseResult<C[K], I> : never }[number];

  /**
   * What {@link select} resolves to with `default: true` when no case is ready.
   *
   * @category Concurrency
   */
  type SelectDefault = { index: -1; value: undefined; closed: false };

  /** The value of each send case is a `T` of its channel. */
  type SelectCases<C extends readonly SelectCase[]> = {
    [K in keyof C]: C[K] extends readonly [Channel<infer T>, unknown] ? readonly [Channel<T>, T] : C[K];
  };

  /**
   * Waits until one of several channel operations can happen, and does exactly that one, like Go's `select`.
   *
   * Each case is a channel to receive from, or `[channel, value]` to send `value` on it. When several cases are
   * ready, one of them is picked at random; the others do nothing. The result says which case happened, by its
   * index in `cases`:
   *
   * - A receive: `{ index, value, closed: false }`, or `{ index, value: undefined, closed: true }` once the channel
   *   is closed and drained.
   * - A send: `{ index, value: undefined, closed: false }`. A send on a closed channel rejects with
   *   `ERR_CHANNEL_CLOSED` if it is picked.
   *
   * Every send value is cloned when `select()` is called, also for the cases that lose. Send cases take no
   * `transfer` list, so a losing case never detaches a buffer.
   *
   * A waiting `select()` does not keep the process alive.
   *
   * @example
   * ```ts
   * const jobs = new Bun.Channel<string>();
   * const quit = new Bun.Channel<void>();
   *
   * while (true) {
   *   const result = await Bun.select([jobs, quit], { signal: AbortSignal.timeout(1000) });
   *   if (result.index === 1 || result.closed) break;
   *   console.log("job", result.value);
   * }
   * ```
   *
   * @param cases The operations to wait for. An empty list waits until `options.signal` aborts, or resolves at once
   * with `default: true`; without either it rejects with `ERR_INVALID_ARG_VALUE`.
   *
   * @category Concurrency
   */
  function select<const C extends readonly SelectCase[]>(
    cases: C & SelectCases<C>,
    options: SelectOptions & { default: true },
  ): Promise<SelectResult<C> | SelectDefault>;
  function select<const C extends readonly SelectCase[]>(
    cases: C & SelectCases<C>,
    options?: SelectOptions & { default?: false },
  ): Promise<SelectResult<C>>;
  function select<const C extends readonly SelectCase[]>(
    cases: C & SelectCases<C>,
    options?: SelectOptions,
  ): Promise<SelectResult<C> | SelectDefault>;
}
