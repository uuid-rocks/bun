import * as b from "../../codegen/bindgen-lib.ts";

export default b.module({ rustPath: "crate::concurrency::channel" });

/** `bun:internal-for-testing`: `{ receivers, senders }`, the entries a channel's queues hold. */
export const queueLengthsForTesting = b.fn({
  args: { channel: b.any },
  returns: b.any,
  capability: "js",
  throws: true,
});

/** `bun:internal-for-testing`: how many channels exist, on every thread. */
export const liveCountForTesting = b.fn({ returns: b.f64 });

/** `bun:internal-for-testing`: reclaims the channels that only unreachable channels hold, now, on this thread; how many. */
export const collectForTesting = b.fn({ returns: b.f64 });
