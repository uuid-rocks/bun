import { describe, expect, test } from "bun:test";

// A goroutine that ends only when `signal` aborts, after it marks `slot` of `ended`.
async function untilAborted(signal: AbortSignal, ended: SharedArrayBuffer, slot: number) {
  try {
    await new Promise((_, reject) => {
      if (signal.aborted) reject(signal.reason);
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
  } finally {
    Atomics.store(new Int32Array(ended), slot, 1);
  }
}

describe("Bun.WaitGroup", () => {
  test("wait() waits for every goroutine, the ones started while it waits too", async () => {
    const group = new Bun.WaitGroup();
    const done = new Int32Array(new SharedArrayBuffer(16));
    const mark = (buffer: SharedArrayBuffer, i: number) => {
      Atomics.store(new Int32Array(buffer), i, 1);
      return i;
    };
    const first = group.go(mark, done.buffer, 0);
    group.go(mark, done.buffer, 1);
    first.then(() => {
      group.go(mark, done.buffer, 2).then(() => group.go(mark, done.buffer, 3));
    });
    await group.wait();
    expect(Array.from(done)).toEqual([1, 1, 1, 1]);
    expect(await first).toBe(0);
    // A group can wait again.
    group.go(mark, done.buffer, 0);
    await group.wait();
  });

  test("the first error aborts the signal with it, and wait() rejects with it once every goroutine ended", async () => {
    const group = new Bun.WaitGroup();
    const ended = new Int32Array(new SharedArrayBuffer(8));
    const started = new Bun.Channel<void>(2);
    for (const slot of [0, 1]) {
      group.go(
        async (signal: AbortSignal, ended: SharedArrayBuffer, slot: number, started: Bun.Channel<void>) => {
          await started.send();
          const wait = new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason)));
          try {
            await wait;
          } finally {
            await Bun.sleep(5);
            Atomics.store(new Int32Array(ended), slot, 1);
          }
        },
        group.signal,
        ended.buffer,
        slot,
        started,
      );
    }
    await started.receive();
    await started.receive();
    group.go(() => {
      throw new TypeError("boom");
    });
    const error = await group.wait().catch(e => e);
    expect(error).toBeInstanceOf(TypeError);
    expect(error.message).toBe("boom");
    // Both siblings saw the abort and ended before wait() rejected.
    expect(Array.from(ended)).toEqual([1, 1]);
    expect(group.signal.aborted).toBe(true);
    expect(group.signal.reason).toBe(error);
    // wait() keeps answering with the first error.
    expect(await group.wait().catch(e => e)).toBe(error);
  });

  test("a parent signal aborts the group's signal", async () => {
    const parent = new AbortController();
    const group = new Bun.WaitGroup({ signal: parent.signal });
    const ended = new Int32Array(new SharedArrayBuffer(4));
    const goroutine = group.go(untilAborted, group.signal, ended.buffer, 0);
    parent.abort(new Error("caller gave up"));
    expect(group.signal.aborted).toBe(true);
    expect(group.signal.reason.message).toBe("caller gave up");
    expect((await group.wait().catch(e => e)).message).toBe("caller gave up");
    expect(await goroutine.catch(e => e.message)).toBe("caller gave up");
    expect(ended[0]).toBe(1);

    const aborted = new Bun.WaitGroup({ signal: AbortSignal.abort("before") });
    expect(aborted.signal.reason).toBe("before");
  });

  test("await using aborts the goroutines still running and waits for them", async () => {
    const ended = new Int32Array(new SharedArrayBuffer(8));
    {
      await using group = new Bun.WaitGroup();
      group.go(untilAborted, group.signal, ended.buffer, 0);
      group.go(untilAborted, group.signal, ended.buffer, 1);
      expect(Array.from(ended)).toEqual([0, 0]);
    }
    // They ended with the AbortError disposal gave them, which is not an error of the group.
    expect(Array.from(ended)).toEqual([1, 1]);
  });

  test("await using throws an error that wait() did not", async () => {
    const thrown = await (async () => {
      await using group = new Bun.WaitGroup();
      await group
        .go(() => {
          throw new Error("not waited for");
        })
        .catch(() => {});
    })().catch(e => e);
    expect(thrown.message).toBe("not waited for");

    const waited = await (async () => {
      await using group = new Bun.WaitGroup();
      group.go(() => {
        throw new Error("waited for");
      });
      return await group.wait().catch(e => e.message);
    })();
    expect(waited).toBe("waited for");
  });

  test("a goroutine that can't start throws from go() and is not counted", async () => {
    const group = new Bun.WaitGroup();
    const captured = { n: 1 };
    expect(() => group.go(() => captured)).toThrow(expect.objectContaining({ code: "ERR_GOROUTINE_NOT_SENDABLE" }));
    expect(() => group.go(1 as any)).toThrow(expect.objectContaining({ code: "ERR_INVALID_ARG_TYPE" }));
    await group.wait();
    expect(() => new Bun.WaitGroup({ signal: 1 as any })).toThrow(
      expect.objectContaining({ code: "ERR_INVALID_ARG_TYPE" }),
    );
  });
});
