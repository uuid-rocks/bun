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
