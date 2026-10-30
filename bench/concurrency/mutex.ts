// Bun.Mutex: lock/unlock on one thread, and a shared counter that goroutines on different threads
// increment under the lock. Prints one JSON line.

const total = 160_000;

// `count` increments of `counter[0]`, each under the lock. Self-contained, so it runs as a goroutine.
async function increment(mu: Bun.Mutex, counter: Int32Array, count: number) {
  for (let i = 0; i < count; i++) {
    using _ = await mu.lock();
    counter[0]++;
  }
}

async function contended(goroutines: number) {
  const mu = new Bun.Mutex();
  const counter = new Int32Array(new SharedArrayBuffer(4));
  const start = performance.now();
  await Promise.all(Array.from({ length: goroutines }, () => Bun.go(increment, mu, counter, total / goroutines)));
  const elapsed = performance.now() - start;
  if (counter[0] !== total) throw new Error(`counted ${counter[0]} of ${total}`);
  return (elapsed * 1e6) / total;
}

// Goroutines first, so the pool is up before anything is timed.
await Promise.all(Array.from({ length: 8 }, () => Bun.go(() => 0)));
const result: Record<string, number> = {};
{
  const mu = new Bun.Mutex();
  const counter = new Int32Array(1);
  await increment(mu, counter, 10_000);
  const start = performance.now();
  await increment(mu, counter, total);
  result["uncontended lock/unlock (ns)"] = ((performance.now() - start) * 1e6) / total;
}
for (const goroutines of [2, 8]) {
  await contended(goroutines);
  result[`contended, ${goroutines} goroutines (ns/lock)`] = await contended(goroutines);
}
console.log(JSON.stringify(result));
