import { mutexWaiters } from "bun:internal-for-testing";
import { describe, expect, test } from "bun:test";

/** Polls until `condition()` holds. */
async function until(condition: () => boolean) {
  while (!condition()) await Bun.sleep(1);
}

describe("Bun.Mutex", () => {
  test("lock() and tryLock() on one thread; using and unlock() release it once", async () => {
    const mu = new Bun.Mutex();
    {
      using lock = await mu.lock();
      expect(typeof lock.unlock).toBe("function");
      expect(mu.tryLock()).toBeUndefined();
    }
    const lock = mu.tryLock();
    expect(lock).toBeDefined();
    expect(mu.tryLock()).toBeUndefined();
    lock!.unlock();
    // Only the first unlock does anything: this one must not free a lock someone else holds.
    const held = mu.tryLock()!;
    lock!.unlock();
    lock![Symbol.dispose]();
    expect(mu.tryLock()).toBeUndefined();
    held.unlock();

    // A throw out of the scope unlocks too.
    expect(
      await (async () => {
        await using _ = await mu.lock();
        throw new Error("inside");
      })().catch(e => e.message),
    ).toBe("inside");
    using again = mu.tryLock();
    expect(again).toBeDefined();
  });

  test("goroutines on several threads never hold it at once, and every increment counts", async () => {
    const mu = new Bun.Mutex();
    // [counter, holders now, times two held it at once, goroutines started]
    const state = new Int32Array(new SharedArrayBuffer(16));
    const goroutines = 4;
    const rounds = 200;
    const threads = await Promise.all(
      Array.from({ length: goroutines }, () =>
        Bun.go(
          async (mu: Bun.Mutex, buffer: SharedArrayBuffer, goroutines: number, rounds: number) => {
            const state = new Int32Array(buffer);
            // Each one blocks its thread until all have started, so they run on as many threads.
            Atomics.add(state, 3, 1);
            Atomics.notify(state, 3);
            for (let started; (started = Atomics.load(state, 3)) < goroutines; ) Atomics.wait(state, 3, started);
            for (let i = 0; i < rounds; i++) {
              using _ = await mu.lock();
              if (Atomics.add(state, 1, 1) !== 0) Atomics.add(state, 2, 1);
              const value = Atomics.load(state, 0);
              // Other goroutines run while this one holds the lock.
              await Bun.sleep(0);
              Atomics.store(state, 0, value + 1);
              Atomics.sub(state, 1, 1);
            }
            return require("node:worker_threads").threadId;
          },
          mu,
          state.buffer,
          goroutines,
          rounds,
        ),
      ),
    );
    expect(Array.from(state)).toEqual([goroutines * rounds, 0, 0, goroutines]);
    expect(new Set(threads).size).toBe(goroutines);
  });

  test("unlocking hands the lock to a waiter on another thread without freeing it in between", async () => {
    const mu = new Bun.Mutex();
    const lock = await mu.lock();
    const order = new Bun.Channel<number>(2);
    const release = new Bun.Channel<void>(2);
    const goroutines = [1, 2].map(n =>
      Bun.go(
        async (mu: Bun.Mutex, order: Bun.Channel<number>, release: Bun.Channel<void>, n: number) => {
          using _ = await mu.lock();
          await order.send(n);
          await release.receive();
        },
        mu,
        order,
        release,
        n,
      ),
    );
    await until(() => mutexWaiters(mu) === 2);
    lock.unlock();
    // On its way to a waiter, or held by it: never free.
    expect(mu.tryLock()).toBeUndefined();
    const first = await order.receive();
    expect(mu.tryLock()).toBeUndefined();
    await release.send();
    const second = await order.receive();
    await release.send();
    await Promise.all(goroutines);
    expect([first, second].sort()).toEqual([1, 2]);
    expect(mutexWaiters(mu)).toBe(0);
    using free = mu.tryLock();
    expect(free).toBeDefined();
  });

  test("an aborted lock() rejects with the reason, leaves the queue, and is never handed the lock", async () => {
    const mu = new Bun.Mutex();
    const lock = await mu.lock();

    // On this thread
    const controller = new AbortController();
    const waiting = mu.lock({ signal: controller.signal });
    expect(mutexWaiters(mu)).toBe(1);
    controller.abort(new Error("gave up"));
    expect(await waiting.catch(e => e.message)).toBe("gave up");
    expect(mutexWaiters(mu)).toBe(0);

    // On another thread, aborted from here
    const remote = new AbortController();
    const goroutine = Bun.go(
      async (mu: Bun.Mutex, signal: AbortSignal) => {
        try {
          await mu.lock({ signal });
          return "locked";
        } catch (e) {
          return (e as Error).message;
        }
      },
      mu,
      remote.signal,
    );
    await until(() => mutexWaiters(mu) === 1);
    remote.abort(new Error("cancelled there"));
    expect(await goroutine).toBe("cancelled there");
    expect(mutexWaiters(mu)).toBe(0);

    // An aborted signal rejects at once.
    expect(await mu.lock({ signal: AbortSignal.abort(new Error("already")) }).catch(e => e.message)).toBe("already");
    lock.unlock();
    using free = mu.tryLock();
    expect(free).toBeDefined();
  });

  test("a lock handed to a thread that ends is taken back", async () => {
    const mu = new Bun.Mutex();
    const lock = await mu.lock();
    const worker = new Worker(
      URL.createObjectURL(
        new Blob([
          `self.onmessage = async ({ data: mu }) => { postMessage("waiting"); await mu.lock(); postMessage("locked"); };`,
        ]),
      ),
    );
    const waiting = Promise.withResolvers<void>();
    worker.onmessage = () => waiting.resolve();
    worker.postMessage(mu);
    await waiting.promise;
    await until(() => mutexWaiters(mu) === 1);
    const closed = new Promise(resolve => worker.addEventListener("close", resolve));
    worker.terminate();
    lock.unlock();
    await closed;
    // Either the worker's waiter was gone when the lock was unlocked, or the lock was on its way to it
    // and came back: both ways it is free again.
    using again = await mu.lock();
    expect(again).toBeDefined();
  });

  test("a mutex sent through a channel or to a goroutine is the same lock", async () => {
    const mu = new Bun.Mutex();
    const ch = new Bun.Channel<Bun.Mutex>(1);
    await ch.send(mu);
    const received = await ch.receive();
    expect(received).not.toBe(mu);
    expect(received).toBeInstanceOf(Bun.Mutex);
    using lock = received.tryLock();
    expect(lock).toBeDefined();
    expect(mu.tryLock()).toBeUndefined();
    expect(await Bun.go((mu: Bun.Mutex) => mu.tryLock() === undefined, mu)).toBe(true);
  });
});
