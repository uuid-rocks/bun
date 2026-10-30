import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, isWindows, normalizeBunSnapshot, tempDir } from "harness";

async function run(files: Record<string, string>, env: Record<string, string> = {}) {
  using dir = tempDir("go-deadlock", files);
  await using proc = Bun.spawn({
    cmd: [bunExe(), "main.ts"],
    env: { ...bunEnv, ...env },
    cwd: String(dir),
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
  // Frames keep their positions: normalizeBunSnapshot would hide them.
  return { stdout, stderr: normalizeBunSnapshot(stderr, dir), frames: stderr.replaceAll(String(dir), "<dir>"), exitCode };
}

describe.concurrent.skipIf(isWindows)("deadlock detection", () => {
  test("a top-level receive nothing can send to", async () => {
    const { stdout, stderr, exitCode } = await run({
      "main.ts": `const ch = new Bun.Channel<number>();
console.log("waiting");
console.log(await ch.receive());
`,
    });
    expect(stdout).toBe("waiting\n");
    expect(stderr).toMatchInlineSnapshot(`
      "error: all goroutines are asleep - deadlock!

      main thread:
        Channel.receive()
          at <dir>/main.ts:3:22

      No thread is left that can send, receive, close or unlock for these. Set BUN_GO_DEADLOCK=0 to wait anyway."
    `);
    expect(exitCode).toBe(1);
  });

  test("goroutines that wait for a mutex and a channel, and a main thread that waits for them", async () => {
    const { stdout, stderr, frames, exitCode } = await run({
      "tasks.ts": `export async function lockThenSend(mu: Bun.Mutex, ch: Bun.Channel<number>, locked: Bun.Channel<void>) {
  using _ = await mu.lock();
  await locked.send();
  await ch.send(1);
}

export async function lock(mu: Bun.Mutex) {
  using _ = await mu.lock();
}
`,
      "main.ts": `import { lock, lockThenSend } from "./tasks.ts";
const mu = new Bun.Mutex();
const locked = new Bun.Channel<void>(1);
const holder = Bun.go(lockThenSend, mu, new Bun.Channel<number>(), locked);
await locked.receive();
const waiter = Bun.go(lock, mu);
const results = new Bun.Channel<number>();
for await (const value of results) console.log(value);
await Promise.all([holder, waiter]);
`,
    });
    expect(stdout).toBe("");
    expect(stderr).toContain("error: all goroutines are asleep - deadlock!");
    expect(stderr).toContain("main thread:\n  for await on a Channel\n    at <dir>/main.ts:8:");
    expect(frames).toMatch(/Channel\.send\(\)\n {4}at async lockThenSend \(<dir>\/tasks\.ts:4:\d+\)/);
    expect(frames).toMatch(/Mutex\.lock\(\)\n {4}at async lock \(<dir>\/tasks\.ts:8:\d+\)/);
    expect(exitCode).toBe(1);
  });

  test("a select on channels nobody uses, in a goroutine nobody waits on but the main thread", async () => {
    const { stderr, frames, exitCode } = await run({
      "tasks.ts": `export async function wait(a: Bun.Channel<number>, b: Bun.Channel<string>) {
  return await Bun.select([a, b]);
}
`,
      "main.ts": `import { wait } from "./tasks.ts";
await Bun.go(wait, new Bun.Channel<number>(), new Bun.Channel<string>());
`,
    });
    expect(frames).toContain("Bun.go thread 1:\n  Bun.select()\n    at async wait (<dir>/tasks.ts:2:");
    expect(stderr).not.toContain("internal:");
    expect(exitCode).toBe(1);
  });

  test("process 'exit' listeners run", async () => {
    const { stdout, stderr, exitCode } = await run({
      "main.ts": `process.on("exit", code => console.log("exit", code));
await new Bun.Channel().send(1);
`,
    });
    expect(stderr).toContain("all goroutines are asleep - deadlock!");
    expect(stdout).toBe("exit 1\n");
    expect(exitCode).toBe(1);
  });

  // Each of these waits on a channel or a mutex, and something other than a channel or a mutex
  // wakes it: none of them is a deadlock.
  const notDeadlocked: Record<string, string> = {
    "a timer": `const ch = new Bun.Channel<number>();
setTimeout(() => ch.send(1), 20);
console.log(await ch.receive());`,
    "an unref'd timer": `const ch = new Bun.Channel<number>();
setTimeout(() => ch.send(1), 20).unref();
console.log(await ch.receive());`,
    "an AbortSignal.timeout() case": `console.log((await Bun.select([new Bun.Channel(), AbortSignal.timeout(20)])).index);`,
    "a mutex a timer unlocks": `const mu = new Bun.Mutex();
const lock = await mu.lock();
setTimeout(() => lock.unlock(), 20);
using again = await mu.lock();
console.log(1);`,
    "fetch() in a goroutine": `const ch = new Bun.Channel<string>();
using server = Bun.serve({ port: 0, async fetch() { await Bun.sleep(20); return new Response("1"); } });
Bun.go(async (url, ch) => { await ch.send(await (await fetch(url)).text()); }, server.url.href, ch);
console.log(await ch.receive());`,
    "an unref'd server that a child process requests": `const ch = new Bun.Channel<string>();
const server = Bun.serve({ port: 0, async fetch() { await ch.send("1"); return new Response("ok"); } });
server.unref();
Bun.spawn([process.execPath, "-e", "await fetch(" + JSON.stringify(server.url.href) + ")"], { env: process.env }).unref();
console.log(await ch.receive());
server.stop(true);`,
    "an unref'd child process": `const ch = new Bun.Channel<number>();
const child = Bun.spawn([process.execPath, "-e", "1"], { env: process.env });
child.unref();
child.exited.then(code => ch.send(code + 1));
console.log(await ch.receive());`,
    "Atomics.waitAsync()": `const ch = new Bun.Channel<number>();
const i32 = new Int32Array(new SharedArrayBuffer(4));
Atomics.waitAsync(i32, 0, 0, 20).value.then(() => ch.send(1));
console.log(await ch.receive());`,
    "a signal listener": `const ch = new Bun.Channel<number>();
process.on("SIGUSR2", () => ch.send(1));
Bun.spawn(["sh", "-c", "sleep 0.05; kill -USR2 " + process.pid]).unref();
console.log(await ch.receive());
process.exit(0);`,
    "an unref'd Worker": `const ch = new Bun.Channel<number>();
const worker = new Worker(URL.createObjectURL(new Blob(["self.onmessage = async ({ data }) => { await Bun.sleep(20); await data.send(1); process.exit(0); }"])));
worker.unref();
worker.postMessage(ch);
console.log(await ch.receive());`,
    "a goroutine busy computing": `const ch = new Bun.Channel<number>();
Bun.go(ch => { let x = 0; for (let i = 0; i < 2e6; i++) x += i; return ch.send(x > 0 ? 1 : 0); }, ch);
console.log(await ch.receive());`,
    "a goroutine reading a file": `const ch = new Bun.Channel<number>();
Bun.go(async (ch, path) => { await ch.send((await Bun.file(path).text()).length > 0 ? 1 : 0); }, ch, import.meta.path);
console.log(await ch.receive());`,
    "goroutines that pass values and a mutex around until they finish": `const jobs = new Bun.Channel<number>();
const results = new Bun.Channel<number>();
const mu = new Bun.Mutex();
const counter = new Int32Array(new SharedArrayBuffer(4));
const workers = Array.from({ length: 4 }, () =>
  Bun.go(async (jobs, results, mu, counter) => {
    for await (const job of jobs) {
      { using _ = await mu.lock(); counter[0]++; }
      await results.send(job);
    }
  }, jobs, results, mu, counter));
(async () => { for (let i = 0; i < 200; i++) await jobs.send(i); jobs.close(); })();
let sum = 0;
for (let i = 0; i < 200; i++) sum += await results.receive();
await Promise.all(workers);
console.log(sum === 199 * 100 && counter[0] === 200 ? 1 : 0);`,
  };
  test.each(Object.keys(notDeadlocked))("not a deadlock: %s", async name => {
    const { stdout, stderr, exitCode } = await run({ "main.ts": notDeadlocked[name] });
    expect({ stdout: stdout.trim(), stderr }).toEqual({ stdout: "1", stderr: "" });
    expect(exitCode).toBe(0);
  });
});
