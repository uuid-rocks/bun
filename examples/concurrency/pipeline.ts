// A CPU-bound pipeline on goroutines.
//
// The producer sends ranges of numbers on a bounded channel. N goroutines take ranges from it,
// count the primes in each, and send the counts on a second channel, which the main thread reads
// with `for await`. The same worker function then runs on the main thread alone, and the program
// prints both times.
//
//   bun pipeline.ts [--limit 20000000] [--size 100000] [--workers <cores>] [--timeout <ms>] [--no-single]
//
// --workers 0 runs only the main thread; --no-single skips it.
//
// With --timeout, an AbortSignal stops the pipeline: the producer selects between sending the next
// range and a quit channel, and the goroutines stop at the next range.

import { parseArgs } from "node:util";

type Range = [from: number, to: number];

// Counts the primes in each range it receives, and sends each count on. It uses only its
// parameters and globals, so Bun.go() runs it on another thread just as it is written, and the
// main thread can call it too.
async function worker(ranges: Bun.Channel<Range>, counts: Bun.Channel<number>, signal: AbortSignal) {
  const { countPrimes } = await import("./primes.ts");
  for await (const [from, to] of ranges) {
    const count = countPrimes(from, to, signal);
    if (signal.aborted) return;
    await counts.send(count);
  }
}

// Sends the ranges below `limit`, until there are no more or `quit` closes.
async function produce(ranges: Bun.Channel<Range>, quit: Bun.Channel<never>, limit: number, size: number) {
  for (let from = 0; from < limit; from += size) {
    const { index } = await Bun.select([[ranges, [from, Math.min(from + size, limit)]], quit]);
    if (index === 1) break;
  }
  ranges.close();
}

// Runs the pipeline with `workers` goroutines, or with the worker on this thread for 0.
async function run(workers: number, limit: number, size: number, signal: AbortSignal) {
  using ranges = new Bun.Channel<Range>(Math.max(workers, 1) * 2);
  using counts = new Bun.Channel<number>(Math.max(workers, 1) * 2);
  using quit = new Bun.Channel<never>();
  signal.addEventListener("abort", () => quit.close(), { once: true });

  const start = performance.now();
  const produced = produce(ranges, quit, limit, size);
  const running =
    workers === 0
      ? [worker(ranges, counts, signal)]
      : Array.from({ length: workers }, () => Bun.go(worker, ranges, counts, signal));
  // No more counts once every worker has returned.
  Promise.allSettled(running).then(() => counts.close());

  let primes = 0;
  let counted = 0;
  for await (const count of counts) {
    primes += count;
    counted++;
  }
  await Promise.all([produced, ...running]);
  return { primes, counted, ms: performance.now() - start };
}

const { values } = parseArgs({
  options: {
    limit: { type: "string", default: "20000000" },
    size: { type: "string", default: "100000" },
    workers: { type: "string", default: String(navigator.hardwareConcurrency) },
    timeout: { type: "string" },
    single: { type: "boolean", default: true },
  },
  allowNegative: true,
});
const limit = Number(values.limit);
const size = Number(values.size);
const workers = Number(values.workers);
const ranges = Math.ceil(limit / size);

const n = (value: number) => value.toLocaleString("en-US");
const ms = (value: number) => `${n(Math.round(value))} ms`.padStart(10);
const label = (value: string) => value.padEnd(16);

if (values.timeout !== undefined) {
  const timeout = Number(values.timeout);
  console.log(`Counting the primes below ${n(limit)} with ${workers} goroutines, for at most ${n(timeout)} ms:`);
  const { counted } = await run(workers, limit, size, AbortSignal.timeout(timeout));
  console.log(counted < ranges ? `  stopped after ${counted} of ${ranges} ranges` : `  done: all ${ranges} ranges`);
} else {
  console.log(`Counting the primes below ${n(limit)}, in ${n(ranges)} ranges of ${n(size)}:`);
  const never = new AbortController().signal;
  const runs = workers === 0 || !values.single ? [workers] : [workers, 0];
  const results = [];
  for (const count of runs) {
    const result = await run(count, limit, size, never);
    const name = count === 0 ? "main thread" : count === 1 ? "1 goroutine" : `${count} goroutines`;
    console.log(`  ${label(name)}${ms(result.ms)}   ${n(result.primes)} primes`);
    results.push(result);
  }
  if (results.length === 2) {
    const [parallel, single] = results;
    if (single.primes !== parallel.primes) throw new Error("The counts differ");
    console.log(`  ${(single.ms / parallel.ms).toFixed(1)}x as fast on ${workers} goroutines`);
  }
}
