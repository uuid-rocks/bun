// Resident memory of a pool thread and of a waiting goroutine. Prints one JSON line.

const threads = Number(process.env.GOMAXPROCS ?? navigator.hardwareConcurrency);
const rss = () => process.memoryUsage.rss();
const mb = (bytes: number) => bytes / 1024 / 1024;

async function settle() {
  Bun.gc(true);
  await Bun.sleep(200);
  Bun.gc(true);
}

await settle();
let before = rss();

// Goroutines that wait at a barrier until all have arrived each need a thread of their own: this
// starts every thread of the pool.
const sab = new SharedArrayBuffer(4);
await Promise.all(
  Array.from({ length: threads }, () =>
    Bun.go(
      (sab: SharedArrayBuffer, parties: number) => {
        const arrived = new Int32Array(sab);
        Atomics.add(arrived, 0, 1);
        Atomics.notify(arrived, 0);
        for (let seen; (seen = Atomics.load(arrived, 0)) < parties;) Atomics.wait(arrived, 0, seen);
      },
      sab,
      threads,
    ),
  ),
);
await settle();
const perThread = (rss() - before) / threads;

// Goroutines that all wait on one channel at once.
const count = 20_000;
const started = new Int32Array(new SharedArrayBuffer(4));
using release = new Bun.Channel<void>();
before = rss();
const waiting = Array.from({ length: count }, () =>
  Bun.go(
    async (started: Int32Array, release: Bun.Channel<void>) => {
      Atomics.add(started, 0, 1);
      await release.receive().catch(() => {});
    },
    started,
    release,
  ),
);
while (Atomics.load(started, 0) < count) await Bun.sleep(5);
await settle();
const perGoroutine = (rss() - before) / count;
release.close();
await Promise.all(waiting);

console.log(
  JSON.stringify({
    "per pool thread (MB)": mb(perThread),
    "per waiting goroutine (KB)": perGoroutine / 1024,
  }),
);
