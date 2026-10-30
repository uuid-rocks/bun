import * as b from "../../codegen/bindgen-lib.ts";

/** `Bun.Mutex`. A structured clone is a handle to the same lock. */
export const Mutex = b.class({
  rustPath: "crate::concurrency::mutex::Mutex",
  construct: {},
  proto: {
    lock: { builtin: true, length: 0 },
    tryLock: { builtin: true, length: 0 },
    __lock: {
      args: { options: b.any },
      returns: b.any.returning("Promise"),
      capability: "js",
      throws: true,
      rejects: true,
      privateSymbol: "mutexLock",
    },
    __tryLock: { returns: b.bool, capability: "js", throws: true, privateSymbol: "mutexTryLock" },
    __unlock: { privateSymbol: "mutexUnlock" },
    "@@inspectCustom": { returns: b.string.utf8, rustName: "inspect", length: 2 },
  },
});
