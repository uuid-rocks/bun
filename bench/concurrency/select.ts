// Bun.select under contention: producers send on 8 channels, and selectors each wait on all 8 at
// once. Prints one JSON line.

const channelCount = 8;
const perChannel = 25_000;

// Receives from whichever of `channels` is ready until all are closed; returns how many it got.
async function selector(channels: Bun.Channel<number>[]) {
  let cases = channels;
  let received = 0;
  while (cases.length > 0) {
    const result = await Bun.select(cases);
    if (result.closed) cases = cases.filter((_, i) => i !== result.index);
    else received++;
  }
  return received;
}

async function producer(channel: Bun.Channel<number>, count: number) {
  for (let i = 0; i < count; i++) await channel.send(i);
  channel.close();
}

async function measure(selectors: number) {
  const channels = Array.from({ length: channelCount }, () => new Bun.Channel<number>(64));
  const start = performance.now();
  const producers = channels.map(channel => Bun.go(producer, channel, perChannel));
  const counts =
    selectors === 0
      ? [await selector(channels)]
      : await Promise.all(Array.from({ length: selectors }, () => Bun.go(selector, channels)));
  await Promise.all(producers);
  const received = counts.reduce((a, b) => a + b, 0);
  if (received !== channelCount * perChannel) throw new Error(`received ${received}`);
  return received / ((performance.now() - start) / 1000);
}

await measure(0);
console.log(
  JSON.stringify({
    "1 selector on the main thread (values/s)": await measure(0),
    "8 selector goroutines (values/s)": await measure(8),
  }),
);
