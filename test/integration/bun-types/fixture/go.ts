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

// @ts-expect-error - the arguments must match the function's parameters
Bun.go(add, "1", 2);
// @ts-expect-error - too few arguments
Bun.go(add, 1);
// @ts-expect-error - not a function or module
Bun.go(42);
