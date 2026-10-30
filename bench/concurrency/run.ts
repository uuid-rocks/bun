// Runs the concurrency benchmarks, each in a fresh process, and prints a Markdown table of the
// median and range of the samples. Use a release build: a debug build measures its assertions.
//
//   bun run.ts [--samples 5] [--only spawn,memory,channel,select,mutex,pipeline]
//   BUN=/path/to/bun bun run.ts       # benchmark another build

import { parseArgs } from "node:util";
import { availableParallelism, cpus, release } from "node:os";

const { values } = parseArgs({
  options: {
    samples: { type: "string", default: "5" },
    only: { type: "string", default: "spawn,memory,channel,select,mutex,pipeline" },
  },
});
const samples = Number(values.samples);
const only = new Set(values.only.split(","));
const bun = process.env.BUN ?? process.execPath;
const cores = availableParallelism();

async function sample(args: string[], env: Record<string, string> = {}) {
  await using proc = Bun.spawn({
    cmd: [bun, ...args],
    cwd: import.meta.dir,
    env: { ...process.env, ...env },
    stdout: "pipe",
    stderr: "inherit",
  });
  const [stdout, exitCode] = await Promise.all([proc.stdout.text(), proc.exited]);
  if (exitCode !== 0) throw new Error(`${args.join(" ")} exited with ${exitCode}:\n${stdout}`);
  return stdout;
}

const rows: [string, number[]][] = [];
function record(name: string, value: number) {
  let row = rows.find(([n]) => n === name);
  if (!row) rows.push((row = [name, []]));
  row[1].push(value);
}

for (const name of ["spawn", "memory", "channel", "select", "mutex"]) {
  if (!only.has(name)) continue;
  for (let i = 0; i < samples; i++) {
    const result = JSON.parse(await sample([`${name}.ts`]));
    for (const [metric, value] of Object.entries(result)) record(`${name}: ${metric}`, value as number);
  }
}

if (only.has("pipeline")) {
  const pipeline = new URL("../../examples/concurrency/pipeline.ts", import.meta.url).pathname;
  const timeOf = (stdout: string) => Number(/ ([\d,]+) ms /.exec(stdout)![1].replaceAll(",", ""));
  const sizes = [0, ...[1, 2, 4, 8, 16].filter(n => n <= cores)];
  const medians = new Map<number, number>();
  for (const workers of sizes) {
    const times = [];
    for (let i = 0; i < samples; i++) {
      // The pool has at least 2 threads; 1 goroutine keeps only one of them busy.
      const env = workers === 0 ? {} : { GOMAXPROCS: String(Math.max(workers, 2)) };
      const ms = timeOf(await sample([pipeline, "--workers", String(workers), "--no-single"], env));
      times.push(ms);
      record(`pipeline: ${name(workers)} (ms)`, ms);
    }
    medians.set(workers, median(times));
  }
  for (const workers of sizes.slice(1)) {
    record(`pipeline: speedup, ${name(workers)} (x)`, medians.get(0)! / medians.get(workers)!);
  }
}

function name(workers: number) {
  return workers === 0 ? "main thread" : workers === 1 ? "1 goroutine" : `${workers} goroutines`;
}

function median(values: number[]) {
  const sorted = values.toSorted((a, b) => a - b);
  const middle = sorted.length >> 1;
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function format(value: number) {
  const digits = value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return value.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

const revision = (await sample(["--revision"])).trim();
console.log(`Bun ${revision}, ${cpus()[0]?.model ?? "unknown CPU"}, ${cores} cores, ${process.platform} ${release()}`);
console.log(`${samples} samples each, median (min–max)\n`);
console.log("| benchmark | median | range |");
console.log("| --- | ---: | ---: |");
for (const [name, values] of rows) {
  const range = values.length > 1 ? `${format(Math.min(...values))}–${format(Math.max(...values))}` : "";
  console.log(`| ${name} | ${format(median(values))} | ${range} |`);
}
