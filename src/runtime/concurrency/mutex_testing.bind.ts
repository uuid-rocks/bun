import * as b from "../../codegen/bindgen-lib.ts";

export default b.module({ rustPath: "crate::concurrency::mutex" });

/** `bun:internal-for-testing`: how many entries a mutex's queue of waiters holds. */
export const waitersForTesting = b.fn({
  args: { mutex: b.any },
  returns: b.any,
  capability: "js",
  throws: true,
});
