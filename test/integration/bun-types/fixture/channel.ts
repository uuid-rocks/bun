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
// @ts-expect-error - a case is a channel or [channel, value]
Bun.select([1]);
// @ts-expect-error - send cases take no transfer list
Bun.select([[ch, 1, { transfer: [] }]]);
