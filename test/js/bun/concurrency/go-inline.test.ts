import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, normalizeBunSnapshot, tempDir } from "harness";

/** Runs `main` (default `main.ts`) among `files` with its own goroutine pool of `threads` threads. */
async function run(files: Record<string, string>, { threads = 4, main = "main.ts", args = [] as string[] } = {}) {
  using dir = tempDir("bun-go-inline", files);
  await using proc = Bun.spawn({
    cmd: [bunExe(), ...args, main],
    // UV_THREADPOOL_SIZE sizes the pool ahead of GOMAXPROCS.
    env: { ...bunEnv, GOMAXPROCS: String(threads), UV_THREADPOOL_SIZE: undefined },
    cwd: String(dir),
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
  return { stdout: normalizeBunSnapshot(stdout, dir), stderr: normalizeBunSnapshot(stderr, dir), exitCode };
}

describe.concurrent("Bun.go with an inline function", () => {
  test("runs on another thread with cloned arguments, and globals work", async () => {
    const { stdout, stderr, exitCode } = await run({
      "main.ts": `
        const result = await Bun.go(
          async (input: { list: number[] }, factor: number) => {
            input.list.push(4);
            const shared = new Int32Array(new SharedArrayBuffer(4));
            Atomics.add(shared, 0, 2);
            return {
              sum: input.list.reduce((a, b) => a + b, 0) * factor,
              isMainThread: Bun.isMainThread,
              threadId: require("node:worker_threads").threadId > 0,
              globals: [typeof fetch, typeof Bun.Channel, Atomics.load(shared, 0), typeof structuredClone],
              cloned: structuredClone({ a: [1] }),
            };
          },
          { list: [1, 2, 3] },
          2,
        );
        console.log(JSON.stringify(result));
        console.log(await Bun.go(function (this: unknown, a: number) {
          return [typeof this, arguments.length, a];
        }, 7));
      `,
    });
    expect(stdout).toBe(
      [
        '{"sum":20,"isMainThread":false,"threadId":true,"globals":["function","function",2,"function"],"cloned":{"a":[1]}}',
        '[ "undefined", 1, 7 ]',
      ].join("\n"),
    );
    expect(stderr).toBe("");
    expect(exitCode).toBe(0);
  });

  test("the error names what the function captures and where", async () => {
    const { stdout, exitCode } = await run({
      "main.ts": `
        import { join } from "node:path";
        let counter = Number(process.argv.length);
        const config = { size: counter };
        function attempt(fn: Function) {
          try {
            Bun.go(fn);
            console.log("did not throw");
          } catch (e) {
            console.log(e.code, e.message);
          }
        }
        attempt(() => counter + 1);
        attempt(function work() {
          const local = 1;
          return local + config.size +
            join("a", "b").length;
        });
        attempt(() => notDeclaredAnywhere);
        attempt(() => import.meta.url);
        function outer(...args: unknown[]) {
          attempt(() => arguments.length);
        }
        outer();
        attempt(class Task {});
        attempt({ method() {} }.method);
        attempt(Math.max);
        attempt(() => eval("counter"));
      `,
    });
    expect(stdout).toMatchInlineSnapshot(`
      "ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run this function on another thread: it uses "counter" (<dir>/main.ts:13), which is neither its own nor a global. Pass it as an argument, or import it inside the function with \`await import()\`.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run "work" on another thread: it uses "config" (<dir>/main.ts:16) and "join" (<dir>/main.ts:17), which are neither its own nor globals. Pass them as arguments, or import them inside the function with \`await import()\`.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run this function on another thread: it uses "notDeclaredAnywhere" (<dir>/main.ts:19), which is neither its own nor a global. Pass it as an argument, or import it inside the function with \`await import()\`.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run this function on another thread: it uses \`import.meta\` (<dir>/main.ts:20): pass what it needs from it as an argument.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run this function on another thread: the arrow function uses the \`arguments\` of the function around it (<dir>/main.ts:22): pass them as arguments.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run "Task" on another thread: it is a class. Pass a function or an arrow function.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run "method" on another thread: it is a method. Pass a function or an arrow function.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run "max" on another thread: it has no source of its own (it is native or bound). Pass a function written in your code.
      ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run this function on another thread: it calls \`eval()\` directly (<dir>/main.ts:28), which can read the variables around it: call \`(0, eval)()\` instead."
    `);
    expect(exitCode).toBe(0);
  });

  test("await import() inside resolves from the caller's module, which may be TypeScript", async () => {
    const { stdout, stderr, exitCode } = await run(
      {
        "src/lib/math.ts": `
          export function square(x: number): number {
            return x * x;
          }
        `,
        "src/main.ts": `
          type Pair<T> = readonly [T, T];
          interface Options {
            scale: number;
          }
          enum Mode {
            Sum = "sum",
          }
          const squares = await Bun.go(
            async <T extends number>(pair: Pair<T>, options: Options): Promise<number[]> => {
              const { square } = await import("./lib/math.ts");
              const values = pair.map(x => square(x) * options.scale) as number[];
              return values;
            },
            [2, 3] as const,
            { scale: 10 },
          );
          console.log(squares, Mode.Sum);
          // Lines in errors and stack traces are the TypeScript source's.
          let captured: Options | undefined;
          try {
            Bun.go(() => captured);
          } catch (e) {
            console.log(e.message.match(/main\\.ts:\\d+/)[0]);
          }
          await Bun.go(async (): Promise<void> => {
            await 0;
            throw new Error("thrown on the pool");
          }).catch(e => console.log(e.stack.split("\\n")[1].match(/main\\.ts:\\d+/)[0]));
          console.log(await Bun.go((x: number) => require("./lib/math.ts").square(x), 5));
        `,
      },
      { main: "src/main.ts" },
    );
    expect(stdout).toBe(["[ 40, 90 ] sum", "main.ts:22", "main.ts:28", "25"].join("\n"));
    expect(stderr).toBe("");
    expect(exitCode).toBe(0);
  });

  test("an inline goroutine spawns inline goroutines", async () => {
    const { stdout, exitCode } = await run({
      "main.ts": `
        const depths = await Bun.go(async (n: number) => {
          const inner = await Promise.all(
            Array.from({ length: n }, (_, i) => Bun.go(async (i: number) => [i, Bun.isMainThread], i)),
          );
          const local = { n };
          let captured = "did not throw";
          try {
            Bun.go(() => local.n);
          } catch (e) {
            captured = e.code + " " + e.message.includes('"local"');
          }
          return [Bun.isMainThread, inner, captured];
        }, 3);
        console.log(JSON.stringify(depths));
      `,
    });
    expect(stdout).toBe('[false,[[0,false],[1,false],[2,false]],"ERR_GOROUTINE_NOT_SENDABLE true"]');
    expect(exitCode).toBe(0);
  });

  // Each goroutine blocks its thread until all of them have arrived: this finishes only if they run
  // at the same time on as many threads.
  test("inline goroutines run in parallel on distinct threads", async () => {
    const { stdout, exitCode } = await run(
      {
        "main.ts": `
          const sab = new SharedArrayBuffer(4);
          const threadIds = await Promise.all(
            Array.from({ length: 4 }, () =>
              Bun.go((sab: SharedArrayBuffer, parties: number) => {
                const counter = new Int32Array(sab);
                Atomics.add(counter, 0, 1);
                Atomics.notify(counter, 0);
                for (let seen; (seen = Atomics.load(counter, 0)) < parties; ) Atomics.wait(counter, 0, seen);
                return require("node:worker_threads").threadId;
              }, sab, 4),
            ),
          );
          console.log(new Int32Array(sab)[0], new Set(threadIds).size, threadIds.includes(0));
        `,
      },
      { threads: 4 },
    );
    expect(stdout).toBe("4 4 false");
    expect(exitCode).toBe(0);
  });

  test("an inline producer/consumer pipeline over channels", async () => {
    const { stdout, exitCode } = await run({
      "main.ts": `
        const numbers = new Bun.Channel<number>(4);
        const squares = new Bun.Channel<number>(4);
        const producer = Bun.go(async (out: Bun.Channel<number>, count: number) => {
          using _ = out;
          for (let i = 1; i <= count; i++) await out.send(i);
        }, numbers, 100);
        const workers = Array.from({ length: 3 }, () =>
          Bun.go(async (input: Bun.Channel<number>, out: Bun.Channel<number>) => {
            let handled = 0;
            for await (const n of input) {
              await out.send(n * n);
              handled++;
            }
            return handled;
          }, numbers, squares),
        );
        Promise.all(workers).then(() => squares.close());
        let sum = 0;
        for await (const square of squares) sum += square;
        const handled = await Promise.all(workers);
        await producer;
        console.log(sum, handled.reduce((a, b) => a + b, 0));
      `,
    });
    expect(stdout).toBe("338350 100");
    expect(exitCode).toBe(0);
  });

  test("an inline function can't run when code generation from strings is disallowed", async () => {
    const { stdout, exitCode } = await run(
      {
        "task.ts": `export function exported() { return "exported"; }`,
        "main.ts": `
          import { exported } from "./task.ts";
          try {
            Bun.go(() => 1);
          } catch (e) {
            console.log(e.code, e.message);
          }
          console.log(await Bun.go(exported));
        `,
      },
      { args: ["--disallow-code-generation-from-strings"] },
    );
    expect(stdout).toBe(
      [
        "ERR_GOROUTINE_NOT_SENDABLE Bun.go() can't run this function on another thread: no module exports it, and its source can't be compiled again there while code generation from strings is disallowed. Export it from its module.",
        "exported",
      ].join("\n"),
    );
    expect(exitCode).toBe(0);
  });
});

describe.concurrent("an AbortSignal sent to a goroutine", () => {
  test("aborts there when it aborts here: listeners, channel operations and fetch", async () => {
    const { stdout, exitCode } = await run({
      "main.ts": `
        const { promise: requested, resolve: onRequest } = Promise.withResolvers<void>();
        await using server = Bun.serve({
          port: 0,
          fetch(req) {
            onRequest();
            return new Promise(() => {});
          },
        });
        const controller = new AbortController();
        const ready = new Bun.Channel<string>();
        const goroutine = Bun.go(
          async (signal: AbortSignal, ready: Bun.Channel<string>, url: string) => {
            const events: string[] = [];
            signal.addEventListener("abort", () => events.push("listener " + signal.reason.message));
            const never = new Bun.Channel<number>();
            const receive = never.receive({ signal }).then(
              () => "received",
              e => "receive rejected " + e.message,
            );
            const fetched = fetch(url, { signal }).then(
              () => "fetched",
              e => "fetch rejected " + e.message,
            );
            await ready.send(String(signal.aborted));
            const outcomes = await Promise.all([receive, fetched]);
            return [signal.aborted, signal.reason instanceof Error, ...events, ...outcomes];
          },
          controller.signal,
          ready,
          server.url.href,
        );
        console.log(await ready.receive());
        await requested;
        controller.abort(new Error("stop"));
        console.log((await goroutine).join("\\n"));
      `,
    });
    expect(stdout).toBe(
      ["false", "true", "true", "listener stop", "receive rejected stop", "fetch rejected stop"].join("\n"),
    );
    expect(exitCode).toBe(0);
  });

  test("an aborted signal arrives aborted, over a channel too, and an uncloneable reason arrives as an AbortError", async () => {
    const { stdout, exitCode } = await run({
      "main.ts": `
        console.log(await Bun.go((s: AbortSignal) => [s.aborted, s.reason], AbortSignal.abort("why")));
        console.log(
          await Bun.go(async (s: AbortSignal) => {
            if (!s.aborted) await new Promise(resolve => s.addEventListener("abort", resolve));
            return [s.aborted, s.reason.name];
          }, AbortSignal.timeout(1)),
        );

        const signals = new Bun.Channel<AbortSignal>(1);
        const controller = new AbortController();
        await signals.send(controller.signal);
        const sent = new Bun.Channel<void>();
        const viaChannel = Bun.go(async (signals: Bun.Channel<AbortSignal>, sent: Bun.Channel<void>) => {
          const signal = await signals.receive();
          const aborted = new Promise(resolve => signal.addEventListener("abort", resolve));
          await sent.send();
          await aborted;
          return [signal.reason.name, signal.reason.message, signal.reason instanceof RangeError];
        }, signals, sent);
        await sent.receive();
        controller.abort(new RangeError("too far"));
        console.log(await viaChannel);

        const uncloneable = new AbortController();
        const fallback = Bun.go(async (signal: AbortSignal, sent: Bun.Channel<void>) => {
          const aborted = new Promise(resolve => (signal.onabort = resolve));
          await sent.send();
          await aborted;
          return [signal.reason instanceof DOMException, signal.reason.name];
        }, uncloneable.signal, sent);
        await sent.receive();
        uncloneable.abort({ fn() {} });
        console.log(await fallback);

        // A reason that holds its own signal
        const own = new AbortController();
        own.abort(own.signal);
        console.log(
          await Bun.go(
            (s: AbortSignal) => [s.aborted, s.reason instanceof AbortSignal, s.reason.aborted, s.reason.reason.name],
            own.signal,
          ),
        );
      `,
    });
    expect(stdout).toBe(
      [
        '[ true, "why" ]',
        '[ true, "TimeoutError" ]',
        '[ "RangeError", "too far", true ]',
        '[ true, "AbortError" ]',
        '[ true, true, true, "AbortError" ]',
      ].join("\n"),
    );
    expect(exitCode).toBe(0);
  });

  test("only Bun.go and Bun.Channel clone an AbortSignal: postMessage and structuredClone still refuse it", async () => {
    const { stdout, exitCode } = await run({
      "main.ts": `
        const { signal } = new AbortController();
        const { port1, port2 } = new MessageChannel();
        const attempts = {
          "channel.send": () => new Bun.Channel(1).send(signal),
          "port.postMessage": () => port1.postMessage(signal),
          structuredClone: () => structuredClone(signal),
          "worker.postMessage": () => {
            const worker = new Worker("data:text/javascript,");
            try {
              worker.postMessage({ signal });
            } finally {
              worker.terminate();
            }
          },
        };
        for (const [name, attempt] of Object.entries(attempts)) {
          try {
            attempt();
            console.log(name, "did not throw");
          } catch (e) {
            console.log(name, e.name);
          }
        }
        port1.close();
        port2.close();
      `,
    });
    expect(stdout).toBe(
      [
        "channel.send did not throw",
        "port.postMessage DataCloneError",
        "structuredClone DataCloneError",
        "worker.postMessage DataCloneError",
      ].join("\n"),
    );
    expect(exitCode).toBe(0);
  });

  // Links whose signals are collected, and links to a signal that is collected without aborting,
  // must go: a long-lived controller sends one signal to many goroutines, and many controllers
  // that are dropped send theirs to goroutines that listen.
  test("links between signals do not leak", async () => {
    const { stdout, exitCode } = await run(
      {
        "main.ts": `
          import { linkedAbortSignals } from "bun:internal-for-testing";
          const threads = 2;
          const longLived = new AbortController();
          let during = Infinity;
          const timeouts: WeakRef<AbortSignal>[] = [];
          for (let round = 0; round < 5; round++) {
            const goroutines = Array.from({ length: 100 }, () => {
              const timeout = AbortSignal.timeout(1e9);
              timeouts.push(new WeakRef(timeout));
              return Bun.go(async (kept: AbortSignal, dropped: AbortSignal, timeout: AbortSignal) => {
                dropped.addEventListener("abort", () => {});
                const channel = new Bun.Channel<number>(1);
                await channel.send(1, { signal: kept });
                return (await channel.receive({ signal: timeout })) + Number(kept.aborted);
              }, longLived.signal, new AbortController().signal, timeout);
            });
            // Each argument is a link until its clone is read, and its signal collected.
            during = Math.min(during, linkedAbortSignals());
            await Promise.all(goroutines);
          }
          const sab = new SharedArrayBuffer(4);
          const alive = () => timeouts.filter(ref => ref.deref() !== undefined).length;
          let after = linkedAbortSignals();
          let attempt = 0;
          while ((after > 0 || alive() > 0) && attempt < 20) {
            attempt++;
            Bun.gc(true);
            // A collection on every thread
            await Promise.all(
              Array.from({ length: threads }, () =>
                Bun.go((sab: SharedArrayBuffer, parties: number) => {
                  const counter = new Int32Array(sab);
                  Atomics.add(counter, 0, 1);
                  Atomics.notify(counter, 0);
                  for (let seen; (seen = Atomics.load(counter, 0)) < parties; ) Atomics.wait(counter, 0, seen);
                  Bun.gc(true);
                }, sab, threads * attempt),
              ),
            );
            await Bun.sleep(0);
            after = linkedAbortSignals();
          }
          // A timeout that nothing watches any more is collected before it fires.
          console.log(during >= 300, after, alive());
        `,
      },
      { threads: 2 },
    );
    expect(stdout).toBe("true 0 0");
    expect(exitCode).toBe(0);
  });
});
