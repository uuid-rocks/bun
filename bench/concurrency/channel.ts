// Channel throughput: one sender and one receiver on the main thread, or a goroutine sending to the
// main thread, for small values and 64 KiB buffers, copied or transferred. Prints one JSON line.

type Kind = "small" | "copy" | "transfer";

// Sends `count` values of `kind`. Self-contained, so it also runs as a goroutine.
async function produce(channel: Bun.Channel<unknown>, count: number, kind: Kind) {
  const buffer = new Uint8Array(64 * 1024);
  for (let i = 0; i < count; i++) {
    if (kind === "small") await channel.send(i);
    else if (kind === "copy") await channel.send(buffer);
    else {
      const moved = new ArrayBuffer(64 * 1024);
      await channel.send(moved, { transfer: [moved] });
    }
  }
  channel.close();
}

async function measure(kind: Kind, crossThread: boolean, count: number) {
  const channel = new Bun.Channel<unknown>(1024);
  const start = performance.now();
  const produced = crossThread ? Bun.go(produce, channel, count, kind) : produce(channel, count, kind);
  let received = 0;
  for await (const _ of channel) received++;
  await produced;
  if (received !== count) throw new Error(`received ${received} of ${count}`);
  return count / ((performance.now() - start) / 1000);
}

// A goroutine first, so the pool is up before anything is timed.
await Bun.go(() => 0);
const result: Record<string, number> = {};
for (const crossThread of [false, true]) {
  const where = crossThread ? "cross-thread" : "same thread";
  // Warm both paths up.
  await measure("small", crossThread, 10_000);
  result[`small, ${where} (msg/s)`] = await measure("small", crossThread, 200_000);
  result[`64 KiB copy, ${where} (MB/s)`] = (await measure("copy", crossThread, 20_000)) / 16;
  result[`64 KiB transfer, ${where} (MB/s)`] = (await measure("transfer", crossThread, 20_000)) / 16;
}
console.log(JSON.stringify(result));
