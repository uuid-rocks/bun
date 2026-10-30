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
          const wait = new Promise((_, reject) => {
            if (signal.aborted) reject(signal.reason);
            signal.addEventListener("abort", () => reject(signal.reason));
          });
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

    // A goroutine that fails while disposal waits for it, but not because of the abort.
    const late = await (async () => {
      await using group = new Bun.WaitGroup();
      group.go(() => {
        throw new Error("failed during disposal");
      });
    })().catch(e => e);
    expect(late.message).toBe("failed during disposal");
    // An AbortError of the goroutine's own is an error too.
    const ownAbort = await (async () => {
      await using group = new Bun.WaitGroup();
      group.go(() => {
        throw new DOMException("something else was aborted", "AbortError");
      });
    })().catch(e => e);
    expect(ownAbort.message).toBe("something else was aborted");

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

  test("a goroutine that rejects with its signal's reason rejects with the original reason, not a copy", async () => {
    const parent = new AbortController();
    const group = new Bun.WaitGroup({ signal: parent.signal });
    const ended = new Int32Array(new SharedArrayBuffer(8));
    const inGroup = group.go(untilAborted, group.signal, ended.buffer, 0).catch(e => e);
    const alone = Bun.go(untilAborted, parent.signal, ended.buffer, 1).catch(e => e);
    const reason = new Error("shutting down");
    parent.abort(reason);
    expect(await inGroup).toBe(reason);
    expect(await alone).toBe(reason);
    // A clean shutdown can be told from a failure by identity.
    expect(await group.wait().catch(e => e)).toBe(parent.signal.reason);
    expect(await group[Symbol.asyncDispose]().then(() => "disposed")).toBe("disposed");
  });

  test("disposal ignores its own abort reason, and not an error that only looks like it", async () => {
    const thrown = await (async () => {
      await using group = new Bun.WaitGroup();
      group.go(async (signal: AbortSignal) => {
        // The scope ends at once: the signal can have aborted before the goroutine started.
        if (!signal.aborted) await new Promise(resolve => signal.addEventListener("abort", resolve, { once: true }));
        throw new DOMException(signal.reason.message, signal.reason.name);
      }, group.signal);
    })().catch(e => e);
    expect(thrown).toBeInstanceOf(DOMException);
    expect(`${thrown.name}: ${thrown.message}`).toBe("AbortError: The WaitGroup was disposed");
  });

  test("a WaitGroup can't be sent to another thread", async () => {
    const group = new Bun.WaitGroup();
    const hint = "A WaitGroup belongs to the thread that created it: send results on a channel instead.";
    expect(() => Bun.go((group: unknown) => group, group)).toThrow(
      `Bun.go() argument 1 is a WaitGroup, which can't be sent to another thread. ${hint}`,
    );
    expect(() => group.go((nested: unknown) => nested, { nested: group })).toThrow(
      `WaitGroup.go() argument 1 has a WaitGroup at .nested, which can't be sent to another thread. ${hint}`,
    );
    const channel = new Bun.Channel(1);
    expect(await channel.send(group).catch(e => `${e.name}: ${e.message}`)).toBe(
      `DataCloneError: channel.send(): value is a WaitGroup, which can't be sent to another thread. ${hint}`,
    );
    expect(() => structuredClone(group)).toThrow(expect.objectContaining({ name: "DataCloneError" }));
    const goroutine = Bun.go(() => 1);
    expect(() => Bun.go((g: unknown) => g, goroutine)).toThrow(
      "Bun.go() argument 1 is a Goroutine, which can't be sent to another thread. A goroutine belongs to the thread that started it: await it, and send its result.",
    );
    await goroutine;
  });

  test("an invalid option throws from the call to the constructor, not from Bun's own code", () => {
    for (const options of [{ limit: 0 }, { signal: 1 }, 1]) {
      let error: any;
      try {
        new Bun.WaitGroup(options as any);
      } catch (e) {
        error = e;
      }
      expect(error.stack.split("\n")[1]).toContain(import.meta.path);
    }
  });

  test("Bun.inspect() shows what runs, what waits and whether it was cancelled", async () => {
    const group = new Bun.WaitGroup({ limit: 1 });
    expect(Object.prototype.toString.call(group)).toBe("[object WaitGroup]");
    expect(Bun.inspect(group)).toBe("WaitGroup { running: 0, queued: 0, aborted: false }");
    const gate = new Bun.Channel<void>(1);
    group.go((gate: Bun.Channel<void>) => gate.receive(), gate);
    group.go(() => 1);
    expect(Bun.inspect(group)).toBe("WaitGroup { running: 1, queued: 1, aborted: false }");
    await gate.send();
    await group.wait();
    expect(Bun.inspect({ group })).toBe("{\n  group: WaitGroup { running: 0, queued: 0, aborted: false },\n}");
  });

  describe("limit", () => {
    test("runs at most `limit` goroutines at once; go() returns at once, and the rest start as slots free", async () => {
      const group = new Bun.WaitGroup({ limit: 2 });
      const started = new Bun.Channel<number>(5);
      const gate = new Bun.Channel<void>(5);
      const goroutines = [0, 1, 2, 3, 4].map(i =>
        group.go(
          async (i: number, started: Bun.Channel<number>, gate: Bun.Channel<void>) => {
            await started.send(i);
            await gate.receive();
            return i;
          },
          i,
          started,
          gate,
        ),
      );
      const first = [await started.receive(), await started.receive()].sort();
      expect(first).toEqual([0, 1]);
      // Both slots wait on the gate: nothing else can have started.
      expect((await Bun.select([started], { default: true })).index).toBe(-1);
      await gate.send();
      expect(await started.receive()).toBe(2);
      expect((await Bun.select([started], { default: true })).index).toBe(-1);
      for (let i = 0; i < 4; i++) await gate.send();
      await group.wait();
      expect(await Promise.all(goroutines)).toEqual([0, 1, 2, 3, 4]);
    });

    test("once the group's signal aborts, a goroutine that waits for a slot rejects with its reason and never starts", async () => {
      const group = new Bun.WaitGroup({ limit: 1 });
      const ran = new Int32Array(new SharedArrayBuffer(4));
      const gate = new Bun.Channel<void>();
      const first = group.go(async (gate: Bun.Channel<void>) => {
        await gate.receive();
        throw new Error("first failed");
      }, gate);
      const queued = group.go((ran: SharedArrayBuffer) => Atomics.store(new Int32Array(ran), 0, 1), ran.buffer);
      await gate.send();
      const failure = await first.catch(e => e);
      expect(failure.message).toBe("first failed");
      expect(await queued.catch(e => e)).toBe(failure);
      expect(await group.wait().catch(e => e)).toBe(failure);
      expect(ran[0]).toBe(0);
    });

    test("a goroutine that waits for a slot rejects as soon as the signal aborts, while the slot is still taken", async () => {
      const parent = new AbortController();
      const group = new Bun.WaitGroup({ limit: 1, signal: parent.signal });
      const gate = new Bun.Channel<void>(1);
      const running = group.go((gate: Bun.Channel<void>) => gate.receive(), gate);
      const queued = group.go(() => 1).catch(e => e);
      const reason = new Error("cancelled");
      parent.abort(reason);
      expect(await queued).toBe(reason);
      // Queued after the abort: rejects at once too.
      expect(await group.go(() => 2).catch(e => e)).toBe(reason);
      await gate.send();
      await running;
      expect(await group.wait().catch(e => e)).toBe(reason);
    });

    test("a group with a limit and a long-lived parent signal can be collected once nothing waits in it", async () => {
      const parent = new AbortController();
      const refs: WeakRef<Bun.WaitGroup>[] = [];
      const make = async () => {
        for (let i = 0; i < 100; i++) {
          const group = new Bun.WaitGroup({ limit: 1, signal: parent.signal });
          const gate = new Bun.Channel<void>(1);
          group.go((gate: Bun.Channel<void>) => gate.receive(), gate);
          // Waits for the slot, then runs.
          group.go(() => 1);
          await gate.send();
          await group.wait();
          refs.push(new WeakRef(group));
        }
      };
      await make();
      let alive = refs.length;
      for (let i = 0; i < 20 && alive > 10; i++) {
        Bun.gc(true);
        await Promise.resolve();
        alive = refs.filter(ref => ref.deref() !== undefined).length;
      }
      expect(alive).toBeLessThanOrEqual(10);
      parent.abort();
    });

    test("await using drops nothing: what waits for a slot starts, with the aborted signal", async () => {
      const ran = new Int32Array(new SharedArrayBuffer(4 * 10));
      const aborted = new Int32Array(new SharedArrayBuffer(4 * 10));
      {
        await using group = new Bun.WaitGroup({ limit: 3 });
        for (let i = 0; i < 10; i++) {
          group.go(
            (ran: Int32Array, aborted: Int32Array, i: number, signal: AbortSignal) => {
              Atomics.store(ran, i, 1);
              Atomics.store(aborted, i, signal.aborted ? 1 : 0);
            },
            ran,
            aborted,
            i,
            group.signal,
          );
        }
      }
      expect(Array.from(ran)).toEqual(Array(10).fill(1));
      // The last ones started after disposal aborted the signal.
      expect(aborted[9]).toBe(1);
    });

    test("a goroutine that waits for a slot throws from go() at once if it can't start", () => {
      const group = new Bun.WaitGroup({ limit: 1 });
      const gate = new Bun.Channel<void>(1);
      group.go((gate: Bun.Channel<void>) => gate.receive(), gate);
      expect(() =>
        group.go(
          (fn: unknown) => fn,
          () => {},
        ),
      ).toThrow(expect.objectContaining({ name: "DataCloneError" }));
      const captured = 1;
      expect(() => group.go(() => captured)).toThrow(expect.objectContaining({ code: "ERR_GOROUTINE_NOT_SENDABLE" }));
      gate.close();
      return group.wait().catch(() => {});
    });

    test("must be an integer of at least 1, or Infinity", () => {
      for (const limit of [0, -1, 1.5, NaN]) {
        expect(() => new Bun.WaitGroup({ limit })).toThrow(expect.objectContaining({ code: "ERR_OUT_OF_RANGE" }));
      }
      expect(() => new Bun.WaitGroup({ limit: "2" as any })).toThrow(
        expect.objectContaining({ code: "ERR_INVALID_ARG_TYPE" }),
      );
      new Bun.WaitGroup({ limit: Infinity });
    });
  });
});
