import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, normalizeBunSnapshot, tempDir } from "harness";
import { rmSync } from "node:fs";
import { join } from "node:path";

/** Runs `main.ts` among `files` with its own goroutine pool of `threads` threads. */
async function run(files: Record<string, string>, { threads = 4, args = ["main.ts"] } = {}) {
  using dir = tempDir("bun-go", files);
  await using proc = Bun.spawn({
    cmd: [bunExe(), ...args],
    env: { ...bunEnv, GOMAXPROCS: String(threads), UV_THREADPOOL_SIZE: undefined },
    cwd: String(dir),
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
  return { stdout: normalizeBunSnapshot(stdout, dir), stderr, exitCode, dir: String(dir) };
}

describe.concurrent("Bun.go", () => {
  test("runs an exported function on another thread with cloned arguments and result", async () => {
    const { stdout, stderr, exitCode } = await run({
      "task.ts": `
        export function inspect(input: { list: number[] }) {
          input.list.push(4);
          return {
            sum: input.list.reduce((a, b) => a + b, 0),
            isMainThread: Bun.isMainThread,
            mainModule: import.meta.main,
            threadId: require("node:worker_threads").threadId,
          };
        }
        export async function later(x: number) {
          await Bun.sleep(1);
          return x * 2;
        }
      `,
      "main.ts": `
        import { inspect, later } from "./task.ts";
        const input = { list: [1, 2, 3] };
        const goroutine = Bun.go(inspect, input);
        console.log(goroutine instanceof Promise, typeof goroutine.ref, typeof goroutine.unref);
        const result = await goroutine;
        console.log(JSON.stringify(input.list), result.sum, result.isMainThread, result.mainModule, result.threadId > 0);
        console.log(await Bun.go(later, 21));
        // Derived promises are plain promises.
        console.log(goroutine.then(() => {}).constructor === Promise);
      `,
    });
    expect(stdout).toBe(["true function function", "[1,2,3] 10 false false true", "42", "true"].join("\n"));
    expect(stderr).toBe("");
    expect(exitCode).toBe(0);
  });

  // Each goroutine blocks its thread until all of them have arrived: this finishes only if they run
  // at the same time on as many threads.
  test("goroutines run in parallel on distinct threads", async () => {
    const { stdout, exitCode } = await run(
      {
        "barrier.ts": `
          export function arrive(sab: SharedArrayBuffer, parties: number) {
            const counter = new Int32Array(sab);
            Atomics.add(counter, 0, 1);
            Atomics.notify(counter, 0);
            for (let seen; (seen = Atomics.load(counter, 0)) < parties; ) Atomics.wait(counter, 0, seen);
            return require("node:worker_threads").threadId;
          }
        `,
        "main.ts": `
          import { arrive } from "./barrier.ts";
          const sab = new SharedArrayBuffer(4);
          const threadIds = await Promise.all(Array.from({ length: 4 }, () => Bun.go(arrive, sab, 4)));
          console.log(new Int32Array(sab)[0], new Set(threadIds).size, threadIds.includes(0));
        `,
      },
      { threads: 4 },
    );
    expect(stdout).toBe("4 4 false");
    expect(exitCode).toBe(0);
  });

  test("10,000 goroutines settle on a small pool, and each pool thread evaluates the module once", async () => {
    const { stdout, exitCode } = await run(
      {
        "task.ts": `
          globalThis.evaluations = (globalThis.evaluations ?? 0) + 1;
          export function identify(i: number) {
            return [i, globalThis.evaluations, require("node:worker_threads").threadId];
          }
        `,
        "main.ts": `
          import { identify } from "./task.ts";
          const results = await Promise.all(Array.from({ length: 10_000 }, (_, i) => Bun.go(identify, i)));
          const threads = new Set(results.map(r => r[2]));
          console.log(
            results.every((r, i) => r[0] === i),
            results.every(r => r[1] === 1),
            threads.size >= 1 && threads.size <= 2,
          );
        `,
      },
      { threads: 2 },
    );
    expect(stdout).toBe("true true true");
    expect(exitCode).toBe(0);
  });

  test("a goroutine keeps its spawner alive until it settles", async () => {
    const { stdout, exitCode } = await run({
      "task.ts": `
        export async function slow() {
          await Bun.sleep(20);
          return "settled";
        }
      `,
      "main.ts": `
        import { slow } from "./task.ts";
        Bun.go(slow).then(console.log);
        process.on("exit", code => console.log("exit", code));
      `,
    });
    expect(stdout).toBe("settled\nexit 0");
    expect(exitCode).toBe(0);
  });

  test("an unref()'d goroutine does not keep the process alive, even one that blocks its thread", async () => {
    const { stdout, exitCode } = await run({
      "task.ts": `
        export function block(sab: SharedArrayBuffer) {
          Atomics.wait(new Int32Array(sab), 0, 0);
        }
        export async function park(channel: Bun.Channel) {
          await channel.receive();
        }
      `,
      "main.ts": `
        import { block, park } from "./task.ts";
        Bun.go(block, new SharedArrayBuffer(4)).unref();
        Bun.go(park, new Bun.Channel()).unref().ref().unref();
        process.on("exit", code => console.log("exit", code));
      `,
    });
    expect(stdout).toBe("exit 0");
    expect(exitCode).toBe(0);
  });

  test("errors arrive cloned with name, message, stack, cause and own properties", async () => {
    const { stdout, exitCode } = await run({
      "task.ts": `
        class TaskError extends Error {
          name = "TaskError";
        }
        export function typeError() {
          const error = new TypeError("bad input", { cause: new RangeError("too big") });
          (error as any).code = "E_TASK";
          throw error;
        }
        export function custom() {
          throw new TaskError("custom");
        }
        export function notAnError() {
          throw { reason: 42 };
        }
        export function uncloneableResult() {
          return () => {};
        }
      `,
      "main.ts": `
        import { typeError, custom, notAnError, uncloneableResult } from "./task.ts";
        try {
          await Bun.go(typeError);
        } catch (e) {
          console.log(e instanceof TypeError, e.name, e.message, e.code, e.cause instanceof RangeError, e.cause.message);
          console.log(e.stack.includes("task.ts"));
        }
        await Bun.go(custom).catch(e => console.log(e instanceof Error, e.name, e.message));
        await Bun.go(notAnError).catch(e => console.log(JSON.stringify(e)));
        await Bun.go(uncloneableResult).catch(e => console.log(e.name));
      `,
    });
    expect(stdout).toBe(
      [
        "true TypeError bad input E_TASK true too big",
        "true",
        "true TaskError custom",
        '{"reason":42}',
        "DataCloneError",
      ].join("\n"),
    );
    expect(exitCode).toBe(0);
  });

  test("throws for what can't run on another thread", async () => {
    const { stdout, exitCode } = await run({
      "task.ts": `
        export function exported() {}
        export default 42;
      `,
      "main.ts": `
        import { exported } from "./task.ts";
        let captured = 1;
        function local() {
          return captured;
        }
        const attempts = {
          closure: () => Bun.go(() => captured),
          local: () => Bun.go(local),
          bound: () => Bun.go(exported.bind(null)),
          native: () => Bun.go(Math.max),
          number: () => Bun.go(42 as any),
          argument: () => Bun.go(exported, () => {}),
          missing: () => Bun.go("./missing.ts"),
        };
        for (const [name, attempt] of Object.entries(attempts)) {
          try {
            attempt();
            console.log(name, "did not throw");
          } catch (e) {
            console.log(name, e.name, e instanceof DOMException ? "" : e.code);
          }
        }
        try {
          Bun.go(local);
        } catch (e) {
          console.log(e.message.includes('"captured"'), e.message.includes("main.ts:5"));
        }
        await Bun.go("./task.ts").catch(e => console.log("default", e.name, e.code));
      `,
    });
    expect(stdout).toBe(
      [
        "closure TypeError ERR_GOROUTINE_NOT_SENDABLE",
        "local TypeError ERR_GOROUTINE_NOT_SENDABLE",
        "bound TypeError ERR_GOROUTINE_NOT_SENDABLE",
        "native TypeError ERR_GOROUTINE_NOT_SENDABLE",
        "number TypeError ERR_INVALID_ARG_TYPE",
        "argument DataCloneError ",
        "missing Error ERR_MODULE_NOT_FOUND",
        "true true",
        "default TypeError ERR_GOROUTINE_NOT_SENDABLE",
      ].join("\n"),
    );
    expect(exitCode).toBe(0);
  });

  test("an unhandled goroutine rejection is an unhandled rejection in the spawner", async () => {
    const { stdout, stderr, exitCode } = await run({
      "task.ts": `export function fail() { throw new RangeError("goroutine failed"); }`,
      "main.ts": `
        import { fail } from "./task.ts";
        Bun.go(fail);
      `,
    });
    expect(stdout).toBe("");
    expect(stderr).toContain("RangeError: goroutine failed");
    expect(exitCode).toBe(1);
  });

  test("an uncaught error on a pool thread is the program's, and the thread goes on", async () => {
    const { stdout, exitCode } = await run({
      "task.ts": `
        export async function throwLater() {
          setTimeout(() => {
            throw new Error("thrown later");
          });
          await Bun.sleep(10);
          return "survived";
        }
      `,
      "main.ts": `
        import { throwLater } from "./task.ts";
        process.on("uncaughtException", e => console.log("uncaught:", e.message));
        console.log(await Bun.go(throwLater));
      `,
    });
    expect(stdout).toBe("uncaught: thrown later\nsurvived");
    expect(exitCode).toBe(0);
  });

  test("goroutines spawn goroutines", async () => {
    const { stdout, exitCode } = await run(
      {
        "task.ts": `
          export async function depth(n: number): Promise<number> {
            return n === 0 ? 0 : 1 + (await Bun.go(depth, n - 1));
          }
          export async function fanOut(n: number) {
            const results = await Promise.all(Array.from({ length: n }, (_, i) => Bun.go(depth, i)));
            return results.reduce((a, b) => a + b, 0);
          }
        `,
        "main.ts": `
          import { depth, fanOut } from "./task.ts";
          console.log(await Bun.go(depth, 8), await Bun.go(fanOut, 10));
        `,
      },
      { threads: 2 },
    );
    expect(stdout).toBe("8 45");
    expect(exitCode).toBe(0);
  });

  test("CommonJS exports, a module's default export, and module URLs", async () => {
    const { stdout, exitCode } = await run({
      "props.cjs": `exports.multiply = (a, b) => a * b;`,
      "whole.cjs": `module.exports = function (name) { return "hello " + name; };`,
      "task.ts": `export default function (a: number, b: number) { return a - b; }`,
      "main.ts": `
        const { multiply } = require("./props.cjs");
        const greet = require("./whole.cjs");
        console.log(await Bun.go(multiply, 6, 7));
        console.log(await Bun.go(greet, "go"));
        console.log(await Bun.go(new URL("./task.ts", import.meta.url), 10, 3));
        console.log(await Bun.go(Bun.pathToFileURL(require("node:path").resolve("task.ts")).href, 10, 4));
        console.log(await Bun.go("./whole.cjs", "url"));
      `,
    });
    expect(stdout).toBe(["42", "hello go", "7", "6", "hello url"].join("\n"));
    expect(exitCode).toBe(0);
  });

  test("channels passed as arguments connect goroutines", async () => {
    const { stdout, exitCode } = await run({
      "pipeline.ts": `
        export async function produce(out: Bun.Channel<number>, n: number) {
          using _ = out;
          for (let i = 1; i <= n; i++) await out.send(i);
        }
        export async function square(input: Bun.Channel<number>, out: Bun.Channel<number>) {
          for await (const value of input) await out.send(value * value);
          out.close();
        }
        export async function sum(input: Bun.Channel<number>) {
          let total = 0;
          for await (const value of input) total += value;
          return total;
        }
      `,
      "main.ts": `
        import { produce, square, sum } from "./pipeline.ts";
        const numbers = new Bun.Channel<number>();
        const squares = new Bun.Channel<number>(8);
        Bun.go(produce, numbers, 1000);
        Bun.go(square, numbers, squares);
        console.log(await Bun.go(sum, squares));

        const fromGoroutine = new Bun.Channel<number>();
        Bun.go(produce, fromGoroutine, 10);
        let total = 0;
        for await (const value of fromGoroutine) total += value;
        console.log(total);
      `,
    });
    expect(stdout).toBe("333833500\n55");
    expect(exitCode).toBe(0);
  });

  test("bun test --isolate gives each file a fresh pool", async () => {
    const testFile = `
      import { test, expect } from "bun:test";
      import { evaluations } from "./task.ts";
      test("go", async () => {
        expect(await Promise.all([Bun.go(evaluations), Bun.go(evaluations)])).toEqual([1, 1]);
      });
    `;
    const { stderr, exitCode } = await run(
      {
        "task.ts": `
          globalThis.evaluations = (globalThis.evaluations ?? 0) + 1;
          export function evaluations() { return globalThis.evaluations; }
        `,
        "a.test.ts": testFile,
        "b.test.ts": testFile,
        "c.test.ts": testFile,
      },
      { threads: 2, args: ["test", "--isolate"] },
    );
    expect(stderr).toContain("3 pass");
    expect(exitCode).toBe(0);
  });

  // File a queues goroutines behind two that block both threads; none of them may run in b's pool.
  test("bun test --isolate drops what a finished file left queued", async () => {
    const { stderr, exitCode } = await run(
      {
        "task.ts": `
          export function block(started: SharedArrayBuffer) {
            const counter = new Int32Array(started);
            Atomics.add(counter, 0, 1);
            Atomics.notify(counter, 0);
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
          }
          export function poison() {
            globalThis.poisoned = true;
          }
          // Both threads arrive, so each has finished what it took before.
          export function report(sab: SharedArrayBuffer, parties: number) {
            const counter = new Int32Array(sab);
            Atomics.add(counter, 0, 1);
            Atomics.notify(counter, 0);
            for (let seen; (seen = Atomics.load(counter, 0)) < parties; ) Atomics.wait(counter, 0, seen);
            return globalThis.poisoned ?? false;
          }
        `,
        "a.test.ts": `
          import { test } from "bun:test";
          import { block, poison } from "./task.ts";
          test("a", async () => {
            const started = new SharedArrayBuffer(4);
            Bun.go(block, started).unref();
            Bun.go(block, started).unref();
            const counter = new Int32Array(started);
            for (let seen; (seen = Atomics.load(counter, 0)) < 2; ) await Atomics.waitAsync(counter, 0, seen).value;
            for (let i = 0; i < 4; i++) Bun.go(poison).unref().catch(() => {});
          });
        `,
        "b.test.ts": `
          import { test, expect } from "bun:test";
          import { report } from "./task.ts";
          test("b", async () => {
            const sab = new SharedArrayBuffer(4);
            expect(await Promise.all([Bun.go(report, sab, 2), Bun.go(report, sab, 2)])).toEqual([false, false]);
          });
        `,
      },
      { threads: 2, args: ["test", "--isolate", "a.test.ts", "b.test.ts"] },
    );
    expect(stderr).toContain("2 pass");
    expect(exitCode).toBe(0);
  });

  test("bun build --compile runs a task module listed as an entry point", async () => {
    using dir = tempDir("bun-go-compile", {
      "task.ts": `export default function (a: number, b: number) { return { sum: a + b, isMainThread: Bun.isMainThread }; }`,
      "main.ts": `
        console.log(JSON.stringify(await Bun.go(new URL("./task.ts", import.meta.url), 2, 3)));
        console.log(JSON.stringify(await Bun.go("./task.ts", 4, 5)));
      `,
    });
    const outfile = join(String(dir), "out", "app");
    {
      await using build = Bun.spawn({
        cmd: [bunExe(), "build", "--compile", "main.ts", "task.ts", "--outfile", outfile],
        env: bunEnv,
        cwd: String(dir),
        stdout: "pipe",
        stderr: "pipe",
      });
      const [, stderr, exitCode] = await Promise.all([build.stdout.text(), build.stderr.text(), build.exited]);
      expect(stderr).toBe("");
      expect(exitCode).toBe(0);
    }
    // Run where the sources are not.
    rmSync(join(String(dir), "task.ts"));
    rmSync(join(String(dir), "main.ts"));
    await using proc = Bun.spawn({
      cmd: [outfile],
      env: bunEnv,
      cwd: join(String(dir), "out"),
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    expect(stdout).toBe('{"sum":5,"isMainThread":false}\n{"sum":9,"isMainThread":false}\n');
    expect(stderr).toBe("");
    expect(exitCode).toBe(0);
  });
});
