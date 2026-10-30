import { channelQueueLengths } from "bun:internal-for-testing";
import { describe, expect, test } from "bun:test";
import { isASAN, isDebug } from "harness";

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

const empty = { receivers: 0, senders: 0 };

function closedChannel() {
  const ch = new Bun.Channel<number>();
  ch.close();
  return ch;
}

describe("Bun.select", () => {
  test("a ready receive wins, and only it happens", async () => {
    const a = new Bun.Channel<number>(1);
    const b = new Bun.Channel<string>(1);
    await a.send(1);
    expect(await Bun.select([b, a])).toEqual({ index: 1, value: 1, closed: false });
    expect(a.length).toBe(0);

    // A waiting sender is ready for a receive case, and its send resolves.
    const unbuffered = new Bun.Channel<string>();
    const sent = unbuffered.send("from a sender");
    expect(await Bun.select([b, unbuffered])).toEqual({ index: 1, value: "from a sender", closed: false });
    await sent;
    expect(channelQueueLengths(unbuffered)).toEqual(empty);
  });

  test("a ready send wins, and only it happens", async () => {
    const a = new Bun.Channel<number>();
    const b = new Bun.Channel<number>(1);
    expect(
      await Bun.select([
        [a, 1],
        [b, 2],
      ]),
    ).toEqual({ index: 1, value: undefined, closed: false });
    expect(await b.receive()).toBe(2);

    // A waiting receiver is ready for a send case.
    const received = a.receive();
    expect(await Bun.select([b, [a, 3]])).toEqual({ index: 1, value: undefined, closed: false });
    expect(await received).toBe(3);
    expect(b.length).toBe(0);
  });

  test("exactly one of several ready cases happens", async () => {
    for (let i = 0; i < 200; i++) {
      const a = new Bun.Channel<number>(1);
      const b = new Bun.Channel<number>(1);
      await a.send(1);
      await b.send(2);
      const { index, value } = await Bun.select([a, b]);
      expect(value).toBe(index + 1);
      expect(a.length + b.length).toBe(1);

      const c = new Bun.Channel<number>(1);
      const d = new Bun.Channel<number>(1);
      await Bun.select([
        [c, 1],
        [d, 2],
      ]);
      expect(c.length + d.length).toBe(1);
    }
  });

  test("default resolves { index: -1 } when nothing is ready, and leaves nothing behind", async () => {
    const a = new Bun.Channel<number>();
    const b = new Bun.Channel<number>(1);
    await b.send(0);
    expect(await Bun.select([a, [b, 1]], { default: true })).toEqual({ index: -1, value: undefined, closed: false });
    expect(await Bun.select([], { default: true })).toEqual({ index: -1, value: undefined, closed: false });
    expect(channelQueueLengths(a)).toEqual(empty);
    expect(channelQueueLengths(b)).toEqual(empty);
    expect(await b.receive()).toBe(0);
    expect(b.length).toBe(0);

    // A ready case wins over default.
    await b.send(5);
    expect(await Bun.select([a, b], { default: true })).toEqual({ index: 1, value: 5, closed: false });
    // default: false waits.
    const waiting = Bun.select([a, b], { default: false, signal: undefined });
    expect(channelQueueLengths(a)).toEqual({ receivers: 1, senders: 0 });
    await a.send(6);
    expect(await waiting).toEqual({ index: 0, value: 6, closed: false });
  });

  test("a waiting select is woken by a send, a receive or a close, and leaves no entries behind", async () => {
    const a = new Bun.Channel<number>();
    const b = new Bun.Channel<string>();
    const out = new Bun.Channel<number>();

    let selected = Bun.select([a, b, [out, 7]]);
    expect(channelQueueLengths(a)).toEqual({ receivers: 1, senders: 0 });
    expect(channelQueueLengths(out)).toEqual({ receivers: 0, senders: 1 });
    await b.send("x");
    expect(await selected).toEqual({ index: 1, value: "x", closed: false });
    for (const ch of [a, b, out]) expect(channelQueueLengths(ch)).toEqual(empty);

    selected = Bun.select([a, b, [out, 7]]);
    expect(await out.receive()).toBe(7);
    expect(await selected).toEqual({ index: 2, value: undefined, closed: false });
    for (const ch of [a, b, out]) expect(channelQueueLengths(ch)).toEqual(empty);

    selected = Bun.select([a, b, [out, 7]]);
    a.close();
    expect(await selected).toEqual({ index: 0, value: undefined, closed: true });
    for (const ch of [a, b, out]) expect(channelQueueLengths(ch)).toEqual(empty);
  });

  test("a Worker's send wakes a waiting select", async () => {
    const never = new Bun.Channel<string>();
    const ch = new Bun.Channel<string>();
    const go = new Bun.Channel<void>(1);
    using w = startWorker(
      `self.onmessage = async ({ data: { ch, go } }) => {
        postMessage("ready");
        await go.receive();
        await ch.send("from the worker");
      };`,
      { ch, go },
    );
    await w.ready;
    const selected = Bun.select([never, ch]);
    await go.send();
    expect(await selected).toEqual({ index: 1, value: "from the worker", closed: false });
    expect(channelQueueLengths(never)).toEqual(empty);
    expect(channelQueueLengths(ch)).toEqual(empty);
  });

  test("a signal rejects with its reason and leaves no waiter", async () => {
    const ch = new Bun.Channel<number>();
    const out = new Bun.Channel<number>();

    await expect(Bun.select([ch, [out, 1]], { signal: AbortSignal.timeout(1) })).rejects.toMatchObject({
      name: "TimeoutError",
    });
    expect(channelQueueLengths(ch)).toEqual(empty);
    expect(channelQueueLengths(out)).toEqual(empty);
    // The send case's value was dropped, not left for a receiver.
    const received = out.receive();
    await out.send(2);
    expect(await received).toBe(2);
    // A later receive gets the value a later send sends.
    const sent = ch.send(3);
    expect(await ch.receive()).toBe(3);
    await sent;

    const controller = new AbortController();
    const reason = new Error("stop");
    const selected = Bun.select([ch], { signal: controller.signal });
    controller.abort(reason);
    expect(await selected.catch(e => e)).toBe(reason);
    expect(channelQueueLengths(ch)).toEqual(empty);

    // An empty select waits for its signal.
    const nothing = new AbortController();
    const waiting = Bun.select([], { signal: nothing.signal });
    nothing.abort(reason);
    expect(await waiting.catch(e => e)).toBe(reason);
  });

  test("an aborted signal rejects before any value is cloned, even with a ready case", async () => {
    const ch = new Bun.Channel<number>(1);
    await ch.send(1);
    let cloned = false;
    const value = {
      get x() {
        cloned = true;
        return 1;
      },
    };
    const signal = AbortSignal.abort("already");
    expect(await Bun.select([ch, [new Bun.Channel(1), value]], { signal }).catch(e => e)).toBe("already");
    expect(cloned).toBe(false);
    expect(ch.length).toBe(1);

    // A getter that aborts the signal while the values are cloned.
    const controller = new AbortController();
    const aborting = {
      get x() {
        controller.abort("from a getter");
        return 1;
      },
    };
    expect(await Bun.select([ch, [new Bun.Channel(1), aborting]], { signal: controller.signal }).catch(e => e)).toBe(
      "from a getter",
    );
    expect(ch.length).toBe(1);
  });

  test("closed cases", async () => {
    // A closed receive drains, then is ready with closed: true.
    const ch = new Bun.Channel<number>(1);
    await ch.send(1);
    ch.close();
    expect(await Bun.select([ch])).toEqual({ index: 0, value: 1, closed: false });
    expect(await Bun.select([new Bun.Channel(), ch])).toEqual({ index: 1, value: undefined, closed: true });

    // A closed send rejects if it is picked.
    await expect(Bun.select([new Bun.Channel(), [ch, 2]])).rejects.toMatchObject({ code: "ERR_CHANNEL_CLOSED" });

    // A waiting send case rejects when its channel closes.
    const out = new Bun.Channel<number>();
    const never = new Bun.Channel<number>();
    const selected = Bun.select([never, [out, 3]]);
    out.close();
    await expect(selected).rejects.toMatchObject({ code: "ERR_CHANNEL_CLOSED" });
    expect(channelQueueLengths(never)).toEqual(empty);
  });

  test("ready cases are picked uniformly at random", async () => {
    const counts = [0, 0, 0];
    // A closed channel is always ready; the open one never is, which a scan from a random start would skew.
    const cases = [closedChannel(), new Bun.Channel<number>(), closedChannel()];
    const N = 4000;
    for (let i = 0; i < N; i++) counts[(await Bun.select(cases)).index]++;
    expect(counts[1]).toBe(0);
    // Uniform: 2000 ± 32 (one standard deviation). A start at random then scanning picks index 2 for 2/3.
    expect(counts[0]).toBeGreaterThan(N * 0.4);
    expect(counts[2]).toBeGreaterThan(N * 0.4);

    const sides = [0, 0];
    for (let i = 0; i < N; i++) sides[(await Bun.select([closedChannel(), closedChannel()])).index]++;
    expect(sides[0]).toBeGreaterThan(N * 0.4);
    expect(sides[1]).toBeGreaterThan(N * 0.4);
  });

  test("100k selects that lose on a channel leave no entries in it", async () => {
    // Without the cleanup, every select leaves one entry: 10k show it as well as 100k, at debug speed.
    const N = isDebug || isASAN ? 10_000 : 100_000;
    const jobs = new Bun.Channel<number>();
    const quit = new Bun.Channel<void>();
    const out = new Bun.Channel<number>();
    for (let i = 0; i < N; i++) {
      const selected = Bun.select([jobs, quit, [out, i]]);
      await jobs.send(i);
      const result = await selected;
      if (result.index !== 0 || result.value !== i) throw new Error(`select ${i}: ${JSON.stringify(result)}`);
    }
    expect(channelQueueLengths(quit)).toEqual(empty);
    expect(channelQueueLengths(out)).toEqual(empty);
    expect(channelQueueLengths(jobs)).toEqual(empty);

    for (let i = 0; i < N / 10; i++) {
      const controller = new AbortController();
      const selected = Bun.select([quit, [out, i]], { signal: controller.signal });
      controller.abort();
      await selected.catch(() => {});
    }
    expect(channelQueueLengths(quit)).toEqual(empty);
    expect(channelQueueLengths(out)).toEqual(empty);
  });

  test("two selects meet across threads", async () => {
    const N = 1000;
    const ch = new Bun.Channel<number>();
    const reply = new Bun.Channel<number>();
    using w = startWorker(
      `self.onmessage = async ({ data: { ch, reply, n } }) => {
        const never = new Bun.Channel();
        postMessage("ready");
        for (let i = 0; i < n; i++) {
          const sent = await Bun.select([never, [ch, i]]);
          if (sent.index !== 1) throw new Error("the worker's send case did not win");
          const { value } = await Bun.select([reply, never]);
          if (value !== i * 2) throw new Error("the worker received " + value);
        }
      };`,
      { ch, reply, n: N },
    );
    await w.ready;
    const never = new Bun.Channel<number>();
    for (let i = 0; i < N; i++) {
      const received = await Bun.select([ch, never]);
      if (received.index !== 0 || received.value !== i) throw new Error(`select ${i}: ${JSON.stringify(received)}`);
      await Bun.select([never, [reply, i * 2]]);
    }
    expect(channelQueueLengths(ch)).toEqual(empty);
    expect(channelQueueLengths(reply)).toEqual(empty);
    expect(channelQueueLengths(never)).toEqual(empty);
  });

  test("selects on 4 Workers and the main thread: every value exactly once", async () => {
    const WORKERS = 4;
    const PER_WORKER = 500;
    const a = new Bun.Channel<number>();
    const b = new Bun.Channel<number>(2);
    const workers = Array.from({ length: WORKERS }, (_, id) =>
      startWorker(
        `self.onmessage = async ({ data: { a, b, id, count } }) => {
          postMessage("ready");
          for (let i = 0; i < count; i++) {
            const n = id * count + i;
            await Bun.select([[a, n], [b, n]]);
          }
        };`,
        { a, b, id, count: PER_WORKER },
      ),
    );
    try {
      await Promise.all(workers.map(w => w.ready));
      const total = WORKERS * PER_WORKER;
      const seen = new Uint8Array(total);
      for (let i = 0; i < total; i++) seen[(await Bun.select([a, b])).value!]++;
      expect(seen.every(count => count === 1)).toBe(true);
      expect(b.length).toBe(0);
      expect(channelQueueLengths(a)).toEqual(empty);
      expect(channelQueueLengths(b)).toEqual(empty);
    } finally {
      for (const w of workers) w[Symbol.dispose]();
    }
  });

  test("a terminated Worker's waiting select leaves no entries", async () => {
    const ch = new Bun.Channel<string>();
    const out = new Bun.Channel<string>();
    const w = startWorker(
      `self.onmessage = ({ data: { ch, out } }) => {
        Bun.select([ch, [out, "from the worker"]]).then(r => { throw new Error("the worker's select happened"); });
        postMessage("waiting");
      };`,
      { ch, out },
    );
    expect(await w.ready).toBe("waiting");
    expect(channelQueueLengths(ch)).toEqual({ receivers: 1, senders: 0 });
    w.worker.terminate();
    await w.closed;
    expect(channelQueueLengths(ch)).toEqual(empty);
    expect(channelQueueLengths(out)).toEqual(empty);
    const sent = ch.send("after");
    expect(await ch.receive()).toBe("after");
    await sent;
  });

  test("duplicate channels", async () => {
    const ch = new Bun.Channel<number>(1);
    await ch.send(1);
    const { index, value } = await Bun.select([ch, ch]);
    expect([0, 1]).toContain(index);
    expect(value).toBe(1);
    expect(ch.length).toBe(0);

    // A waiting select is queued once per case, fires once, and leaves nothing behind.
    const selected = Bun.select([ch, ch]);
    expect(channelQueueLengths(ch)).toEqual({ receivers: 2, senders: 0 });
    await ch.send(2);
    expect(await selected).toMatchObject({ value: 2, closed: false });
    expect(ch.length).toBe(0);
    expect(channelQueueLengths(ch)).toEqual(empty);

    // A select does not meet itself.
    const unbuffered = new Bun.Channel<number>();
    expect(await Bun.select([unbuffered, [unbuffered, 3]], { default: true })).toMatchObject({ index: -1 });
    const both = Bun.select([unbuffered, [unbuffered, 4]]);
    expect(channelQueueLengths(unbuffered)).toEqual({ receivers: 1, senders: 1 });
    expect(await unbuffered.receive()).toBe(4);
    expect(await both).toEqual({ index: 1, value: undefined, closed: false });
    expect(channelQueueLengths(unbuffered)).toEqual(empty);

    // With room in the buffer, only the send is ready.
    const roomy = new Bun.Channel<number>(1);
    expect(await Bun.select([roomy, [roomy, 5]])).toEqual({ index: 1, value: undefined, closed: false });
    expect(await roomy.receive()).toBe(5);
  });

  test("send values are cloned, and never transferred", async () => {
    const ch = new Bun.Channel<ArrayBuffer>(1);
    const loser = new Bun.Channel<ArrayBuffer>();
    const buffer = new ArrayBuffer(8);
    await Bun.select([
      [loser, buffer],
      [ch, buffer],
    ]);
    expect(buffer.byteLength).toBe(8);
    const received = await ch.receive();
    expect(received).not.toBe(buffer);
    expect(received.byteLength).toBe(8);

    // Every value is cloned, a losing case's too.
    let reads = 0;
    const counted = {
      get x() {
        reads++;
        return 1;
      },
    };
    await Bun.select([[new Bun.Channel(1), counted], [new Bun.Channel(), counted], closedChannel()]);
    expect(reads).toBe(2);
  });

  test("invalid inputs reject", async () => {
    const ch = new Bun.Channel<number>(1);
    expect(Bun.select.length).toBe(1);
    const invalid: [unknown, unknown, Record<string, unknown>][] = [
      [undefined, undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [ch, undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [new Set([ch]), undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [[1], undefined, { code: "ERR_INVALID_ARG_TYPE", message: expect.stringContaining('"cases[0]"') }],
      [[ch, null], undefined, { code: "ERR_INVALID_ARG_TYPE", message: expect.stringContaining('"cases[1]"') }],
      [[[ch]], undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [[[ch, 1, { transfer: [] }]], undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [[[{}, 1]], undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [[[1, ch]], undefined, { code: "ERR_INVALID_ARG_TYPE" }],
      [[ch], null, { code: "ERR_INVALID_ARG_TYPE" }],
      [[ch], { signal: {} }, { code: "ERR_INVALID_ARG_TYPE" }],
      [[ch], { default: 1 }, { code: "ERR_INVALID_ARG_TYPE" }],
      [[], undefined, { code: "ERR_INVALID_ARG_VALUE" }],
      [[], {}, { code: "ERR_INVALID_ARG_VALUE" }],
      [[[ch, () => {}]], undefined, { name: "DataCloneError" }],
    ];
    for (const [cases, options, error] of invalid) {
      let promise: Promise<unknown>;
      expect(() => {
        promise = Bun.select(cases as any, options as any);
      }).not.toThrow();
      await expect(promise!).rejects.toMatchObject(error);
    }
    // Nothing happened on the channel.
    expect(ch.length).toBe(0);
    expect(channelQueueLengths(ch)).toEqual(empty);
  });
});
