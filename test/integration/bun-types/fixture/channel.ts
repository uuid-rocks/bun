import { expectType } from "./utilities";

const ch = new Bun.Channel<number>(4);
expectType(ch).is<Bun.Channel<number>>();
expectType(new Bun.Channel()).is<Bun.Channel<unknown>>();
expectType(ch.capacity).is<number>();
expectType(ch.length).is<number>();
expectType(ch.closed).is<boolean>();
expectType(ch.send(1)).is<Promise<void>>();
expectType(ch.send(1, { signal: AbortSignal.timeout(10) })).is<Promise<void>>();
const buffer = new ArrayBuffer(8);
new Bun.Channel<ArrayBuffer>().send(buffer, { transfer: [buffer] });
expectType(ch.receive()).is<Promise<number>>();
expectType(ch.receive({ signal: new AbortController().signal })).is<Promise<number>>();
expectType(ch.close()).is<void>();

for await (const n of ch) {
  expectType(n).is<number>();
}
const iterator = ch[Symbol.asyncIterator]();
expectType(iterator).is<AsyncIterableIterator<number, undefined>>();
for await (const n of iterator) {
  expectType(n).is<number>();
}
{
  using scoped = new Bun.Channel<string>();
  expectType(scoped).is<Bun.Channel<string>>();
}

// @ts-expect-error - the value must be a T
ch.send("not a number");
// @ts-expect-error - receive takes no transfer list
ch.receive({ transfer: [] });
// @ts-expect-error - capacity is a number
new Bun.Channel("1");

const strings = new Bun.Channel<string>();
type NumberReceived = { index: 0; value: number; closed: false } | { index: 0; value: undefined; closed: true };
{
  const result = await Bun.select([ch, [strings, "hi"]]);
  expectType(result).is<NumberReceived | { index: 1; value: undefined; closed: false }>();
  if (result.index === 0 && !result.closed) expectType(result.value).is<number>();
  expectType(await Bun.select([ch], { default: true })).is<
    NumberReceived | { index: -1; value: undefined; closed: false }
  >();
  expectType(await Bun.select([], { default: true })).is<Bun.SelectDefault>();
  expectType(await Bun.select([ch], { signal: AbortSignal.timeout(10) })).is<NumberReceived>();
  const { index, value } = await Bun.select([ch, strings]);
  expectType(index).is<0 | 1>();
  expectType(value).is<number | string | undefined>();
  const many: Bun.Channel<number>[] = [ch, ch];
  expectType(await Bun.select(many)).is<
    { index: number; value: number; closed: false } | { index: number; value: undefined; closed: true }
  >();
  const maybe = Math.random() > 0.5;
  expectType(await Bun.select([ch], { default: maybe })).is<NumberReceived | Bun.SelectDefault>();
}

// @ts-expect-error - the value must be a T of its channel
Bun.select([[strings, 1]]);
// A channel is invariant: a Channel<string> takes only strings, so it is not a Channel<unknown>.
// @ts-expect-error - a Channel<string> is not a Channel<unknown>
const anything: Bun.Channel<unknown> = strings;
// @ts-expect-error - nor a Channel<"a">
const narrow: Bun.Channel<"a"> = strings;
// @ts-expect-error - a case is a channel or [channel, value]
Bun.select([1]);
// @ts-expect-error - send cases take no transfer list
Bun.select([[ch, 1, { transfer: [] }]]);

// values({ signal })
{
  const iterator = ch.values({ signal: AbortSignal.timeout(10) });
  expectType(iterator).is<AsyncIterableIterator<number, undefined>>();
  for await (const n of ch.values()) expectType(n).is<number>();
  expectType(ch[Symbol.asyncIterator]()).is<AsyncIterableIterator<number, undefined>>();
  // @ts-expect-error - the signal is an AbortSignal
  ch.values({ signal: 1 });
}

// An AbortSignal is a case, which narrows on its index
{
  const quit = new AbortController();
  const result = await Bun.select([ch, quit.signal, AbortSignal.timeout(10)]);
  expectType(result.index).is<0 | 1 | 2>();
  if (result.index === 2) {
    expectType(result).is<{ index: 2; value: unknown; closed: false }>();
  } else if (result.index === 0 && !result.closed) {
    expectType(result.value).is<number>();
  }
  expectType(await Bun.select([AbortSignal.timeout(10)])).is<{ index: 0; value: unknown; closed: false }>();
}

// Bun.WaitGroup
{
  await using group = new Bun.WaitGroup({ signal: AbortSignal.timeout(10) });
  expectType(group.signal).is<AbortSignal>();
  expectType(group.go((a: number, b: string) => a + b.length, 1, "x")).is<Bun.Goroutine<number>>();
  expectType(group.go(async (signal: AbortSignal) => signal.aborted, group.signal)).is<Bun.Goroutine<boolean>>();
  expectType(group.go<(n: number) => string>("./task.ts", 1)).is<Bun.Goroutine<string>>();
  expectType(group.wait()).is<Promise<void>>();
  // @ts-expect-error - the arguments must fit the function
  group.go((a: number) => a, "not a number");
  // @ts-expect-error - the signal is an AbortSignal
  new Bun.WaitGroup({ signal: 1 });
}

// Bun.Mutex
{
  const mu = new Bun.Mutex();
  expectType(mu.lock()).is<Promise<Bun.MutexLock>>();
  expectType(mu.lock({ signal: AbortSignal.timeout(10) })).is<Promise<Bun.MutexLock>>();
  expectType(mu.tryLock()).is<Bun.MutexLock | undefined>();
  {
    await using lock = await mu.lock();
    expectType(lock.unlock()).is<void>();
  }
  {
    using lock = mu.tryLock();
    if (lock) expectType(lock).is<Bun.MutexLock>();
  }
  await Bun.go(async (mu: Bun.Mutex) => {
    using _ = await mu.lock();
  }, mu);
}
