import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, isASAN, isDebug, tempDir, withoutAsanQuarantine } from "harness";

/** A Worker running `src`. Its first message, its `error` and its exit settle `ready`. */
function startWorker(src: string, data?: unknown) {
  const worker = new Worker(URL.createObjectURL(new Blob([src], { type: "application/javascript" })));
  const closed = Promise.withResolvers<void>();
  const ready = Promise.withResolvers<unknown>();
  worker.addEventListener("message", e => ready.resolve(e.data), { once: true });
  worker.addEventListener("error", e => {
    const error = e.error ?? new Error(e.message);
    ready.reject(error);
    closed.reject(error);
  });
  worker.addEventListener("close", () => {
    ready.reject(new Error("the worker exited before it was ready"));
    closed.resolve();
  });
  // The rejection of a promise nobody awaits is not an unhandled one.
  ready.promise.catch(() => {});
  closed.promise.catch(() => {});
  if (data !== undefined) worker.postMessage(data);
  return {
    worker,
    ready: ready.promise,
    closed: closed.promise,
    [Symbol.dispose]() {
      worker.terminate();
    },
  };
}

describe("Bun.Channel", () => {
  test("constructor validates the capacity", () => {
    expect(new Bun.Channel().capacity).toBe(0);
    expect(new Bun.Channel(undefined).capacity).toBe(0);
    expect(new Bun.Channel(0).capacity).toBe(0);
    expect(new Bun.Channel(2 ** 32 - 1).capacity).toBe(2 ** 32 - 1);
    for (const capacity of [-1, 1.5, 2 ** 32, NaN, Infinity]) {
      expect(() => new Bun.Channel(capacity)).toThrow(
        expect.objectContaining({ name: "RangeError", code: "ERR_OUT_OF_RANGE" }),
      );
    }
    for (const capacity of ["1", null, 1n, {}]) {
      expect(() => new Bun.Channel(capacity as any)).toThrow(
        expect.objectContaining({ name: "TypeError", code: "ERR_INVALID_ARG_TYPE" }),
      );
    }
    // @ts-expect-error
    expect(() => Bun.Channel()).toThrow(TypeError);
  });

  test("options are validated", async () => {
    const ch = new Bun.Channel(1);
    await expect(ch.send(1, null as any)).rejects.toMatchObject({ code: "ERR_INVALID_ARG_TYPE" });
    await expect(ch.send(1, { signal: {} as any })).rejects.toMatchObject({ code: "ERR_INVALID_ARG_TYPE" });
    await expect(ch.receive({ signal: 1 as any })).rejects.toMatchObject({ code: "ERR_INVALID_ARG_TYPE" });
    await expect(ch.send(1, { transfer: 1 as any })).rejects.toBeInstanceOf(TypeError);
    // A function cannot be cloned, and a channel is shared, not transferred.
    await expect(ch.send(() => {})).rejects.toMatchObject({ name: "DataCloneError" });
    await expect(ch.send(ch, { transfer: [ch as any] })).rejects.toMatchObject({ name: "DataCloneError" });
    expect(() => structuredClone(ch, { transfer: [ch as any] })).toThrow(
      expect.objectContaining({ name: "DataCloneError" }),
    );
    // Bytes that leave the process cannot hold a channel.
    expect(() => require("bun:jsc").serialize(ch)).toThrow(expect.objectContaining({ name: "DataCloneError" }));
    expect(ch.length).toBe(0);
  });

  test("unbuffered send waits for a receiver", async () => {
    const ch = new Bun.Channel<string>();
    const log: string[] = [];
    const sent = ch.send("a").then(() => log.push("sent"));
    await Bun.sleep(0);
    log.push("receiving");
    log.push(await ch.receive());
    await sent;
    expect(log).toEqual(["receiving", "a", "sent"]);

    const received = ch.receive().then(v => log.push(`received ${v}`));
    await ch.send("b");
    log.push("send resolved");
    await received;
    expect(log).toEqual(["receiving", "a", "sent", "send resolved", "received b"]);
  });

  test("buffered send waits only when the buffer is full", async () => {
    const ch = new Bun.Channel<number>(2);
    await ch.send(1);
    await ch.send(2);
    expect(ch.length).toBe(2);
    let thirdSent = false;
    const third = ch.send(3).then(() => (thirdSent = true));
    await Bun.sleep(0);
    expect(thirdSent).toBe(false);
    expect(await ch.receive()).toBe(1);
    // The waiting sender's value moved into the buffer.
    expect(ch.length).toBe(2);
    await third;
    expect(await ch.receive()).toBe(2);
    expect(await ch.receive()).toBe(3);
    expect(ch.length).toBe(0);
  });

  test("values are cloned, even on one thread", async () => {
    const ch = new Bun.Channel<any>(1);
    const value = { nested: { n: 1 }, date: new Date(0), map: new Map([[1, 2]]) };
    await ch.send(value);
    value.nested.n = 2;
    const received = await ch.receive();
    expect(received).not.toBe(value);
    expect(received).toEqual({ nested: { n: 1 }, date: new Date(0), map: new Map([[1, 2]]) });
    await ch.send(undefined);
    expect(await ch.receive()).toBeUndefined();
  });

  test("close drains buffered values, then ends receivers and rejects senders", async () => {
    const ch = new Bun.Channel<number>(2);
    await ch.send(1);
    await ch.send(2);
    const waitingSender = ch.send(3);
    ch.close();
    ch.close();
    expect(ch.closed).toBe(true);
    await expect(waitingSender).rejects.toMatchObject({ name: "Error", code: "ERR_CHANNEL_CLOSED" });
    await expect(ch.send(4)).rejects.toMatchObject({ code: "ERR_CHANNEL_CLOSED" });
    expect(await ch.receive()).toBe(1);
    const rest: number[] = [];
    for await (const v of ch) rest.push(v);
    expect(rest).toEqual([2]);
    await expect(ch.receive()).rejects.toMatchObject({ code: "ERR_CHANNEL_CLOSED" });

    const empty = new Bun.Channel();
    const waitingReceiver = empty.receive();
    empty.close();
    await expect(waitingReceiver).rejects.toMatchObject({ code: "ERR_CHANNEL_CLOSED" });
  });

  test("for await ranges until close, and break does not close", async () => {
    const ch = new Bun.Channel<number>(4);
    for (const v of [1, 2, 3]) await ch.send(v);
    for await (const v of ch) {
      expect(v).toBe(1);
      break;
    }
    expect(ch.closed).toBe(false);
    expect(await ch.receive()).toBe(2);
    const producer = (async () => {
      await ch.send(4);
      ch.close();
    })();
    const rest: number[] = [];
    for await (const v of ch) rest.push(v);
    await producer;
    expect(rest).toEqual([3, 4]);
  });

  test("Symbol.dispose closes", async () => {
    let outer: Bun.Channel;
    {
      using ch = new Bun.Channel();
      outer = ch;
    }
    expect(outer.closed).toBe(true);
  });

  test("an aborted operation rejects with the reason and takes nothing", async () => {
    const ch = new Bun.Channel<number>();
    const already = AbortSignal.abort(new Error("already"));
    await expect(ch.receive({ signal: already })).rejects.toThrow("already");
    await expect(ch.send(1, { signal: already })).rejects.toThrow("already");

    const controller = new AbortController();
    const receiving = ch.receive({ signal: controller.signal });
    controller.abort();
    await expect(receiving).rejects.toMatchObject({ name: "AbortError" });

    const sendController = new AbortController();
    const sending = ch.send(2, { signal: sendController.signal });
    sendController.abort();
    await expect(sending).rejects.toMatchObject({ name: "AbortError" });

    // Cloning the value aborts the signal: the value is not sent.
    const getterController = new AbortController();
    const buffered = new Bun.Channel<object>(1);
    const abortingValue = {
      get field() {
        getterController.abort();
        return 1;
      },
    };
    await expect(buffered.send(abortingValue, { signal: getterController.signal })).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(buffered.length).toBe(0);

    // Neither left a waiter behind.
    const sent = ch.send(3);
    expect(await ch.receive()).toBe(3);
    await sent;
  });

  test("a value that can't be cloned rejects with a DataCloneError that says what and where", async () => {
    const channel = new Bun.Channel(1);
    const messages: string[] = [];
    for (const value of [{ cb() {} }, [1, { deep: Symbol("s") }], () => {}, new Map([["k", () => {}]])]) {
      const error = await channel.send(value).catch(e => e);
      expect(error).toBeInstanceOf(DOMException);
      expect(error.name).toBe("DataCloneError");
      messages.push(error.message);
    }
    expect(messages).toEqual([
      "channel.send(): value.cb is a function, which can't be sent to another thread.",
      "channel.send(): value[1].deep is a symbol, which can't be sent to another thread.",
      "channel.send(): value is a function, which can't be sent to another thread.",
      "channel.send(): value is a Map that holds something that can't be cloned, which can't be sent to another thread.",
    ]);
    expect(channel.length).toBe(0);
  });

  test("console.log shows a channel's state", async () => {
    const channel = new Bun.Channel<number>(4);
    await channel.send(1);
    expect(Bun.inspect(channel)).toBe("Channel { capacity: 4, length: 1, closed: false }");
    channel.close();
    expect(Bun.inspect({ channel })).toBe("{\n  channel: Channel { capacity: 4, length: 1, closed: true },\n}");
  });

  test("a structured clone is a handle to the same channel", async () => {
    const ch = new Bun.Channel<string>(1);
    const clone = structuredClone(ch);
    expect(clone).toBeInstanceOf(Bun.Channel);
    // On one thread, one channel is one object.
    expect(clone).toBe(ch);
    await clone.send("through the clone");
    expect(ch.length).toBe(1);
    expect(await ch.receive()).toBe("through the clone");
    // Within one message, one channel is one object.
    const [a, b] = structuredClone([ch, ch]);
    expect(a).toBe(b);
    clone.close();
    expect(ch.closed).toBe(true);
  });

  test("a channel can be sent through a channel", async () => {
    const outer = new Bun.Channel<Bun.Channel<number>>(1);
    const inner = new Bun.Channel<number>(1);
    await outer.send(inner);
    const received = await outer.receive();
    await received.send(42);
    expect(await inner.receive()).toBe(42);
  });

  test("SharedArrayBuffer is shared and transfer moves", async () => {
    const ch = new Bun.Channel<any>(1);
    const sab = new SharedArrayBuffer(4);
    await ch.send({ sab });
    const { sab: received } = await ch.receive();
    new Int32Array(received)[0] = 7;
    expect(new Int32Array(sab)[0]).toBe(7);

    const buffer = new Uint8Array([1, 2, 3]).buffer;
    await ch.send(buffer, { transfer: [buffer] });
    expect(buffer.byteLength).toBe(0);
    expect([...new Uint8Array(await ch.receive())]).toEqual([1, 2, 3]);

    // Without `transfer`, it is copied.
    const copied = new Uint8Array([4]).buffer;
    await ch.send(copied);
    expect(copied.byteLength).toBe(1);
    expect([...new Uint8Array(await ch.receive())]).toEqual([4]);
  });

  test("main <-> Worker ping-pong, 10k round trips", async () => {
    const ping = new Bun.Channel<number>();
    const pong = new Bun.Channel<number>();
    using w = startWorker(
      `self.onmessage = async ({ data: { ping, pong } }) => {
        postMessage("ready");
        for await (const n of ping) await pong.send(n + 1);
      };`,
      { ping, pong },
    );
    await w.ready;
    for (let i = 0; i < 10_000; i++) {
      await ping.send(i);
      expect(await pong.receive()).toBe(i + 1);
    }
    ping.close();
  });

  test("MPMC across 4 Workers: every value exactly once", async () => {
    const PRODUCERS = 2;
    const CONSUMERS = 2;
    const PER_PRODUCER = 1000;
    const work = new Bun.Channel<number>(8);
    const results = new Bun.Channel<number>(8);
    const done = new Bun.Channel<void>(PRODUCERS);
    const producers = Array.from({ length: PRODUCERS }, (_, id) =>
      startWorker(
        `self.onmessage = async ({ data: { work, done, id, count } }) => {
          postMessage("ready");
          for (let i = 0; i < count; i++) await work.send(id * count + i);
          await done.send();
        };`,
        { work, done, id, count: PER_PRODUCER },
      ),
    );
    const consumers = Array.from({ length: CONSUMERS }, () =>
      startWorker(
        `self.onmessage = async ({ data: { work, results } }) => {
          postMessage("ready");
          for await (const n of work) await results.send(n);
        };`,
        { work, results },
      ),
    );
    try {
      await Promise.all([...producers, ...consumers].map(w => w.ready));
      const seen = new Uint8Array(PRODUCERS * PER_PRODUCER);
      let sum = 0;
      for (let i = 0; i < PRODUCERS * PER_PRODUCER; i++) {
        const n = await results.receive();
        seen[n]++;
        sum += n;
      }
      for (let i = 0; i < PRODUCERS; i++) await done.receive();
      work.close();
      const total = PRODUCERS * PER_PRODUCER;
      expect(sum).toBe((total * (total - 1)) / 2);
      expect(seen.every(count => count === 1)).toBe(true);
      expect(results.length).toBe(0);
    } finally {
      for (const w of [...producers, ...consumers]) w[Symbol.dispose]();
    }
  });

  test("SharedArrayBuffer sent to a Worker is the same memory", async () => {
    const ch = new Bun.Channel<SharedArrayBuffer>();
    const reply = new Bun.Channel<string>();
    using w = startWorker(
      `self.onmessage = async ({ data: { ch, reply } }) => {
        postMessage("ready");
        const sab = await ch.receive();
        Atomics.store(new Int32Array(sab), 0, 123);
        await reply.send("stored");
      };`,
      { ch, reply },
    );
    await w.ready;
    const sab = new SharedArrayBuffer(4);
    await ch.send(sab);
    expect(await reply.receive()).toBe("stored");
    expect(Atomics.load(new Int32Array(sab), 0)).toBe(123);
  });

  test("abort racing a Worker's send never loses a value", async () => {
    const COUNT = 500;
    const ch = new Bun.Channel<number>();
    using w = startWorker(
      `self.onmessage = async ({ data: { ch, count } }) => {
        postMessage("ready");
        for (let i = 0; i < count; i++) await ch.send(i);
      };`,
      { ch, count: COUNT },
    );
    await w.ready;
    const received: number[] = [];
    let aborted = 0;
    for (let i = 0; received.length < COUNT; i++) {
      const controller = new AbortController();
      const receiving = ch.receive({ signal: controller.signal });
      if (i % 2) controller.abort();
      try {
        received.push(await receiving);
      } catch (e) {
        expect((e as Error).name).toBe("AbortError");
        aborted++;
      }
    }
    expect(received).toEqual(Array.from({ length: COUNT }, (_, i) => i));
    expect(aborted).toBeGreaterThan(0);
  });

  test("a terminated Worker's waiting receive does not take later values", async () => {
    const ch = new Bun.Channel<string>();
    const w = startWorker(
      `self.onmessage = ({ data: { ch } }) => {
        ch.receive().then(v => { throw new Error("the worker received " + v); });
        postMessage("waiting");
      };`,
      { ch },
    );
    expect(await w.ready).toBe("waiting");
    w.worker.terminate();
    await w.closed;
    const sent = ch.send("after");
    expect(await ch.receive()).toBe("after");
    await sent;
  });

  test("a Worker's buffered values outlive it", async () => {
    const ch = new Bun.Channel<number>(3);
    const w = startWorker(
      `self.onmessage = async ({ data: { ch } }) => {
        for (const n of [1, 2, 3]) await ch.send(n);
        ch.close();
        postMessage("sent");
      };`,
      { ch },
    );
    expect(await w.ready).toBe("sent");
    w.worker.terminate();
    await w.closed;
    const values: number[] = [];
    for await (const n of ch) values.push(n);
    expect(values).toEqual([1, 2, 3]);
  });

  test("a value handed to a Worker that is terminated before it reads it is received once, elsewhere", async () => {
    const ch = new Bun.Channel<string>();
    const w = startWorker(
      `self.onmessage = ({ data: { ch } }) => {
        ch.receive().then(v => { throw new Error("the worker read " + v); });
        postMessage("waiting");
        // The value is handed to the receive above, and this thread never gets to read it.
        for (;;) {}
      };`,
      { ch },
    );
    expect(await w.ready).toBe("waiting");
    // Taken by the worker's receive at once: the send has happened.
    await ch.send("handed");
    w.worker.terminate();
    await w.closed;
    ch.close();
    const values: string[] = [];
    for await (const v of ch) values.push(v);
    expect(values).toEqual(["handed"]);
  });

  test("a closed channel does not end while a value handed to a receiver could still come back", async () => {
    const ch = new Bun.Channel<number>();
    const state = new Int32Array(new SharedArrayBuffer(8));
    const w = startWorker(
      `self.onmessage = ({ data: { ch, state } }) => {
        ch.receive().catch(() => {});
        postMessage("waiting");
        // The value is handed to the receive above, and this thread never gets to read it.
        Atomics.wait(state, 0, 0);
      };`,
      { ch, state },
    );
    expect(await w.ready).toBe("waiting");
    await ch.send(42);
    ch.close();
    const values = ch[Symbol.asyncIterator]();
    // Waits: the worker may never read the value.
    const first = values.next();
    w.worker.terminate();
    await w.closed;
    expect(await first).toEqual({ value: 42, done: false });
    expect(await values.next()).toEqual({ value: undefined, done: true });
    expect(await ch.receive().catch(e => e.code)).toBe("ERR_CHANNEL_CLOSED");
  });

  test("a channel or a mutex that comes back to a thread is the same object", async () => {
    const a = new Bun.Channel<unknown>(1);
    await a.send(a);
    expect(await a.receive()).toBe(a);
    const mu = new Bun.Mutex();
    const [channel, mutex] = await Bun.go((a: Bun.Channel<unknown>, mu: Bun.Mutex) => [a, mu], a, mu);
    expect(channel).toBe(a);
    expect(mutex).toBe(mu);
    const seen = new Map([[a, "a"]]);
    await a.send({ again: a });
    expect(seen.get(((await a.receive()) as { again: Bun.Channel<unknown> }).again)).toBe("a");
  });

  test("values({ signal }): an abort rejects the loop with the reason and leaves the channel open", async () => {
    const ch = new Bun.Channel<number>(4);
    await ch.send(1);
    await ch.send(2);
    const controller = new AbortController();
    const received: number[] = [];
    const reason = new Error("stop");
    const error = await (async () => {
      for await (const value of ch.values({ signal: controller.signal })) {
        received.push(value);
        controller.abort(reason);
      }
    })().catch(e => e);
    expect(error).toBe(reason);
    expect(received).toEqual([1]);
    // The value the loop did not take is still there, for any receiver.
    expect(ch.closed).toBe(false);
    expect(await ch.receive()).toBe(2);

    // A waiting next() rejects when the signal aborts, and takes nothing.
    const later = new AbortController();
    const iterator = ch.values({ signal: later.signal });
    const next = iterator.next();
    later.abort(reason);
    expect(await next.catch(e => e)).toBe(reason);
    expect(await iterator.next().catch(e => e)).toBe(reason);
    await ch.send(3);
    expect(await ch.receive()).toBe(3);

    // Without a signal it is for await's iterator; it ends on close.
    await ch.send(4);
    ch.close();
    expect(await Array.fromAsync(ch.values())).toEqual([4]);
    expect(() => ch.values({ signal: 1 as any })).toThrow(expect.objectContaining({ code: "ERR_INVALID_ARG_TYPE" }));
    expect(() => ch.values(1 as any)).toThrow(expect.objectContaining({ code: "ERR_INVALID_ARG_TYPE" }));
  });

  test("values({ signal }): aborts racing a goroutine's sends never lose or repeat a value", async () => {
    const ch = new Bun.Channel<number>();
    const count = 200;
    const producer = Bun.go(
      async (ch: Bun.Channel<number>, count: number) => {
        for (let i = 0; i < count; i++) await ch.send(i);
        ch.close();
      },
      ch,
      count,
    );
    const received: number[] = [];
    let aborted = 0;
    for (let round = 0; ; round++) {
      const controller = new AbortController();
      const next = ch.values({ signal: controller.signal }).next();
      // Sometimes before the value arrives, sometimes after.
      if (round % 3 === 0) controller.abort();
      else if (round % 3 === 1) queueMicrotask(() => controller.abort());
      else setImmediate(() => controller.abort());
      const result = await next.catch(() => {
        aborted++;
        return undefined;
      });
      if (result?.done) break;
      if (result) received.push(result.value);
    }
    await producer;
    expect(received).toEqual(Array.from({ length: count }, (_, i) => i));
    expect(aborted).toBeGreaterThan(0);
  });
});

describe.concurrent("Bun.Channel process", () => {
  test("a waiting operation does not keep the process alive", async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `const ch = new Bun.Channel();
         ch.receive().then(v => console.log("received " + v));
         ch.send(1);
         new Bun.Channel().send(2, { signal: new AbortController().signal });
         new Bun.Channel().receive();
         console.log("end of script");`,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    // The first send meets the waiting receive.
    expect({ stdout, stderr, exitCode, signalCode: proc.signalCode }).toEqual({
      stdout: "end of script\nreceived 1\n",
      stderr: "",
      exitCode: 0,
      signalCode: null,
    });
  });

  // A wake for a waiter on the thread that fires it goes straight to that thread's task queue, with
  // no wakeup: whatever runs the send (a timer, I/O, a microtask, a finalizer), the loop runs it
  // before it next polls.
  test("operations on one thread wake each other from any callback", async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `const timer = new Bun.Channel();
         setTimeout(() => timer.send("timer"), 1);
         console.log(await timer.receive());

         const microtask = new Bun.Channel();
         queueMicrotask(() => microtask.send("microtask"));
         console.log(await microtask.receive());

         const io = new Bun.Channel();
         using server = Bun.serve({ port: 0, fetch: async () => (await io.send("io"), new Response("ok")) });
         const response = fetch(server.url);
         console.log(await io.receive(), await (await response).text());

         const selected = new Bun.Channel();
         setTimeout(() => Bun.select([[selected, "select"]]), 1);
         console.log((await Bun.select([selected])).value);

         const closed = new Bun.Channel();
         setTimeout(() => closed.close(), 1);
         for await (const _ of closed);
         console.log("closed");

         const finalized = new Bun.Channel();
         const registry = new FinalizationRegistry(() => finalized.send("finalizer"));
         (() => registry.register({}, 0))();
         const received = finalized.receive();
         const interval = setInterval(() => Bun.gc(true), 1);
         console.log(await received);
         clearInterval(interval);

         const iterator = closed[Symbol.asyncIterator]();
         const pending = new Bun.Channel(1);
         await pending.send(1);
         const next = await pending[Symbol.asyncIterator]().next();
         console.log(JSON.stringify([next, Object.keys(next), await iterator.next()]));`,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    expect({ stdout, stderr, exitCode }).toEqual({
      stdout:
        [
          "timer",
          "microtask",
          "io ok",
          "select",
          "closed",
          "finalizer",
          '[{"value":1,"done":false},["value","done"],{"done":true}]',
        ].join("\n") + "\n",
      stderr: "",
      exitCode: 0,
    });
  });

  test("traffic on one thread does not keep timers from running", async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `const channel = new Bun.Channel();
         let fired = false;
         setTimeout(() => (fired = true), 1);
         let sent = 0;
         const sender = (async () => {
           while (!fired && sent < 100_000) await channel.send(sent++);
           channel.close();
         })();
         for await (const _ of channel);
         await sender;
         console.log(fired ? "timer fired" : "timer starved");`,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    expect({ stdout, stderr, exitCode }).toEqual({ stdout: "timer fired\n", stderr: "", exitCode: 0 });
  });

  // Each runs `body` in a process of its own, where `reclaimed()` waits for the channels that only
  // unreachable channels hold to be freed and says how many channels are left beside the ones
  // that existed when it started.
  async function runReclaiming(body: string, env: Record<string, string | undefined> = {}) {
    // From a file: the goroutines of a \`bun -e\` script keep what they made until a full collection.
    using dir = tempDir("channel-reclaiming", {
      "main.js": `
        import { channelLiveCount, collectChannels } from "bun:internal-for-testing";
        const base = channelLiveCount();
        // \`threads\`: the size of the goroutine pool (GOMAXPROCS), whose threads collect too.
        async function reclaimed(threads = 0) {
          const deadline = Date.now() + 3_000;
          while (Date.now() < deadline) {
            Bun.gc(true);
            // A pool thread lets go of its wrappers when it collects its own garbage. Each of these
            // goroutines holds a thread of the pool until all of them collected.
            if (threads) {
              const gate = new Int32Array(new SharedArrayBuffer(4));
              await Promise.all(Array.from({ length: threads }, () => Bun.go((gate, threads) => {
                Bun.gc(true);
                Atomics.add(gate, 0, 1);
                Atomics.notify(gate, 0);
                for (let n; (n = Atomics.load(gate, 0)) < threads; ) Atomics.wait(gate, 0, n);
              }, gate, threads)));
            }
            collectChannels();
            if (channelLiveCount() === base) break;
            await Bun.sleep(1);
          }
          return channelLiveCount() - base;
        }
        ${body}
      `,
    });
    await using proc = Bun.spawn({
      cmd: [bunExe(), "main.js"],
      cwd: String(dir),
      env: { ...bunEnv, ...env },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    return { stdout, stderr, exitCode };
  }

  test("a channel sent into itself, and channels sent into each other, are freed", async () => {
    const result = await runReclaiming(`
      async function cycles() {
        for (let i = 0; i < 200; i++) {
          const ch = new Bun.Channel(1);
          await ch.send({ ch, payload: new Uint8Array(1024) });
          ch.close();
        }
        for (let i = 0; i < 100; i++) {
          const a = new Bun.Channel(2), b = new Bun.Channel(2), c = new Bun.Channel(1);
          // A ring of three, with a mutex and a channel that holds nothing hanging off it.
          await a.send(b);
          await b.send({ c, mutex: new Bun.Mutex(), leaf: new Bun.Channel() });
          await c.send(a);
          // A sender waiting on a full channel of the ring holds it until it is aborted.
          const controller = new AbortController();
          const waiting = a.send(a, { signal: controller.signal });
          await a.send(a, { signal: AbortSignal.abort() }).catch(() => {});
          controller.abort();
          await waiting.catch(() => {});
        }
      }
      await cycles();
      console.log(await reclaimed());
    `);
    expect(result).toEqual({ stdout: "0\n", stderr: "", exitCode: 0 });
  });

  test("a channel that only a reachable channel's queue holds keeps its values", async () => {
    const result = await runReclaiming(`
      const outer = new Bun.Channel(1);
      async function fill() {
        const inner = new Bun.Channel(3);
        await inner.send("first");
        // A reference to itself does not make it unreachable while outer holds it.
        await inner.send({ self: inner });
        await inner.send("last");
        await outer.send(inner);
      }
      await fill();
      for (let i = 0; i < 3; i++) {
        Bun.gc(true);
        collectChannels();
      }
      const inner = await outer.receive();
      const values = [await inner.receive(), await inner.receive(), await inner.receive()];
      console.log(values[0], values[1].self === inner ? "itself" : "another channel", values[2]);
      await inner.send("still works");
      console.log(await inner.receive());
    `);
    expect(result).toEqual({ stdout: "first itself last\nstill works\n", stderr: "", exitCode: 0 });
  });

  test("a cycle made across threads is freed once no thread holds it", async () => {
    const result = await runReclaiming(
      `
      async function cycles() {
        for (let i = 0; i < 5; i++) {
          // The goroutine sends each channel into the other, and sends one back.
          const a = await Bun.go(async (a, b) => {
            await a.send(b);
            await b.send(a);
            return a;
          }, new Bun.Channel(1), new Bun.Channel(1));
          if (a.length !== 1) throw new Error("a holds b");
        }
      }
      await cycles();
      console.log(await reclaimed(2));
    `,
      { GOMAXPROCS: "2", UV_THREADPOOL_SIZE: undefined },
    );
    expect(result).toEqual({ stdout: "0\n", stderr: "", exitCode: 0 });
  });

  test("a long chain of channels, each held by the next one's queue, is freed without recursing", async () => {
    const result = await runReclaiming(`
      async function chain(links) {
        let head = new Bun.Channel(1);
        for (let i = 0; i < links; i++) {
          const link = new Bun.Channel(1);
          await link.send(head);
          head = link;
        }
        // Held by a channel that holds itself: only a collection frees it, on the collector's
        // thread, and then the whole chain at once.
        const keeper = new Bun.Channel(2);
        await keeper.send(keeper);
        await keeper.send(head);
      }
      await chain(20_000);
      console.log(await reclaimed());
    `);
    expect(result).toEqual({ stdout: "0\n", stderr: "", exitCode: 0 });
  });

  test("cycles that 8 goroutines make as fast as they can are freed as they go", async () => {
    const result = await runReclaiming(
      `
      const stop = new Int32Array(new SharedArrayBuffer(4));
      const perGoroutine = 4000;
      const made = 8 * perGoroutine * 2;
      const goroutines = Promise.all(
        Array.from({ length: 8 }, () =>
          Bun.go(async (stop, count) => {
            for (let i = 0; i < count && !Atomics.load(stop, 0); i++) {
              const a = new Bun.Channel(2), b = new Bun.Channel(2);
              await a.send({ b, pad: new Uint8Array(1024) });
              await b.send({ a });
              if (i % 200 === 0) Bun.gc(false);
            }
          }, stop, perGoroutine),
        ),
      );
      let done = false;
      goroutines.then(() => (done = true));
      let most = 0;
      while (!done) {
        most = Math.max(most, channelLiveCount() - base);
        await Bun.sleep(1);
      }
      await goroutines;
      console.log("at most half alive at once:", most < made / 2);
      console.log("left:", await reclaimed(8));
    `,
      { GOMAXPROCS: "8", UV_THREADPOOL_SIZE: undefined },
    );
    expect(result).toEqual({ stdout: "at most half alive at once: true\nleft: 0\n", stderr: "", exitCode: 0 });
  }, 60_000);

  test("channels, messages and waiters are freed", async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `
        async function round() {
          for (let i = 0; i < 500; i++) {
            // A string would be shared by every clone: bytes are copied into each.
            const payload = new Uint8Array(64 * 1024);
            // A buffered message dropped with its channel.
            const dropped = new Bun.Channel(1);
            await dropped.send(payload);
            // A channel cloned into a message that is received.
            const carrier = new Bun.Channel(1);
            await carrier.send({ inner: new Bun.Channel(), payload });
            await carrier.receive();
            // A waiter that is aborted.
            const controller = new AbortController();
            const waiting = new Bun.Channel().receive({ signal: controller.signal });
            controller.abort();
            await waiting.catch(() => {});
          }
          Bun.gc(true);
        }
        await round();
        await round();
        const before = process.memoryUsage.rss();
        await round();
        await round();
        const deltaMB = (process.memoryUsage.rss() - before) / 1024 / 1024;
        // Leaking the dropped messages grows it by ~75 MB.
        if (deltaMB > ${isASAN || isDebug ? 32 : 16}) {
          console.error("RSS grew by " + deltaMB.toFixed(1) + " MB");
          process.exit(1);
        }
        console.log("ok");
        `,
      ],
      env: withoutAsanQuarantine(bunEnv),
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    expect({ stdout, stderr, exitCode }).toEqual({ stdout: "ok\n", stderr: "", exitCode: 0 });
  });
});
