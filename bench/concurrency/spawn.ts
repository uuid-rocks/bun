// What a goroutine costs to start. Prints one JSON line; run.ts runs it in a fresh process.

const noop = () => 0;

// The first goroutine starts a pool thread and compiles the function there.
let start = performance.now();
await Bun.go(noop);
const coldMs = performance.now() - start;

for (let i = 0; i < 1_000; i++) await Bun.go(noop);
const roundTrips = 5_000;
start = performance.now();
for (let i = 0; i < roundTrips; i++) await Bun.go(noop);
const warmUs = ((performance.now() - start) * 1000) / roundTrips;

// Many at once, 10,000 at a time.
const total = 200_000;
const batch = 10_000;
start = performance.now();
for (let done = 0; done < total; done += batch) await Promise.all(Array.from({ length: batch }, () => Bun.go(noop)));
const perSecond = total / ((performance.now() - start) / 1000);

console.log(
  JSON.stringify({
    "first goroutine, cold (ms)": coldMs,
    "round trip, warm (µs)": warmUs,
    "goroutines per second": perSecond,
  }),
);
