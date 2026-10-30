import * as b from "../../codegen/bindgen-lib.ts";

export default b.module({ rustPath: "crate::concurrency::select" });

/** `Bun.select`. */
export const select = b.fn({
  args: { cases: b.any, options: b.any },
  returns: b.any.returning("Promise"),
  capability: "js",
  throws: true,
  rejects: true,
  length: 1,
});
