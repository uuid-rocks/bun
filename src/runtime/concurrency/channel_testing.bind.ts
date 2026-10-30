import * as b from "../../codegen/bindgen-lib.ts";

export default b.module({ rustPath: "crate::concurrency::channel" });

/** `bun:internal-for-testing`: `{ receivers, senders }`, the entries a channel's queues hold. */
export const queueLengthsForTesting = b.fn({
  args: { channel: b.any },
  returns: b.any,
  capability: "js",
  throws: true,
});
