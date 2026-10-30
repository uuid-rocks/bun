// Runs the example app in examples/concurrency. Times vary, so they are masked; the counts are exact.
import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe } from "harness";
import { join } from "node:path";

const examples = join(import.meta.dir, "../../../../examples/concurrency");

async function run(file: string, args: string[]) {
  await using proc = Bun.spawn({
    cmd: [bunExe(), join(examples, file), ...args],
    env: { ...bunEnv, GOMAXPROCS: "4", UV_THREADPOOL_SIZE: undefined },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
  const masked = stdout
    .replace(/ +[\d,]+ ms/g, " <ms>")
    .replace(/\d+\.\dx as fast/g, "<x> as fast")
    .replace(/\d+ pings/g, "<n> pings")
    .replace(/stopped after \d+ of/g, "stopped after <n> of")
    .trim();
  return { stdout: masked, stderr, exitCode };
}

describe.concurrent("examples/concurrency", () => {
  test("pipeline.ts counts the primes on goroutines and on the main thread", async () => {
    const result = await run("pipeline.ts", ["--limit", "200000", "--size", "10000", "--workers", "4"]);
    expect(result).toEqual({
      stdout: [
        "Counting the primes below 200,000, in 20 ranges of 10,000:",
        "  4 goroutines <ms>   17,984 primes",
        "  main thread <ms>   17,984 primes",
        "  <x> as fast on 4 goroutines",
      ].join("\n"),
      stderr: "",
      exitCode: 0,
    });
  });

  test("pipeline.ts --timeout stops the goroutines", async () => {
    const result = await run("pipeline.ts", ["--limit", "1000000000", "--workers", "4", "--timeout", "1"]);
    expect(result).toEqual({
      stdout: [
        "Counting the primes below 1,000,000,000 with 4 goroutines, for at most <ms>:",
        "  stopped after <n> of 10000 ranges",
      ].join("\n"),
      stderr: "",
      exitCode: 0,
    });
  });

  test("server.ts --demo answers /primes from goroutines and from the main thread", async () => {
    const result = await run("server.ts", ["--demo", "--below", "200000", "--requests", "2"]);
    expect(result).toEqual({
      stdout: [
        "2 requests for the primes below 200,000, and /ping meanwhile:",
        "  on goroutines  17,984 primes   all answered in <ms>   <n> pings, slowest <ms>",
        "  on main        17,984 primes   all answered in <ms>   <n> pings, slowest <ms>",
      ].join("\n"),
      stderr: "",
      exitCode: 0,
    });
  });
});
