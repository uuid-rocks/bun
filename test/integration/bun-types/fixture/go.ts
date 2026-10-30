import { expectType } from "./utilities";

declare function add(a: number, b: number): number;
declare function fetchName(id: string): Promise<string>;
declare function produce(out: Bun.Channel<number>): Promise<void>;

expectType(Bun.go(add, 1, 2)).is<Bun.Goroutine<number>>();
expectType(await Bun.go(add, 1, 2)).is<number>();
expectType(await Bun.go(fetchName, "a")).is<string>();
expectType(Bun.go(produce, new Bun.Channel<number>())).is<Bun.Goroutine<void>>();
expectType(Bun.go(new URL("./task.ts", import.meta.url), 1)).is<Bun.Goroutine<unknown>>();
expectType(Bun.go<number>("./task.ts")).is<Bun.Goroutine<number>>();
expectType(Bun.go(add, 1, 2).unref().ref()).is<Bun.Goroutine<number>>();
expectType(await Promise.all([Bun.go(add, 1, 2), Bun.go(fetchName, "a")])).is<[number, string]>();

// An inline function's parameters take the types of the arguments.
expectType(await Bun.go((a, b) => a + b, 1, 2)).is<number>();
expectType(await Bun.go(async (ch, n) => (await ch.receive()) * n, new Bun.Channel<number>(), 3)).is<number>();
expectType(Bun.go(signal => signal.aborted, new AbortController().signal)).is<Bun.Goroutine<boolean>>();
expectType(await Bun.go(function (this: unknown, s: string) { return s.length; }, "a")).is<number>();

// @ts-expect-error - the arguments must match the function's parameters
Bun.go(add, "1", 2);
// @ts-expect-error - too few arguments
Bun.go(add, 1);
// @ts-expect-error - not a function or module
Bun.go(42);
// @ts-expect-error - the arguments must match an inline function's annotated parameters
Bun.go((s: string) => s.length, 1);
