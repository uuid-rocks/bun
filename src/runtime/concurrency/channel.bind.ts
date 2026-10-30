import * as b from "../../codegen/bindgen-lib.ts";

const op = { returns: b.any.returning("Promise"), capability: "js", throws: true, rejects: true } as const;

/** `Bun.Channel`. A structured clone is a handle to the same channel. */
export const Channel = b.class({
  rustPath: "crate::concurrency::channel::Channel",
  construct: { args: { capacity: b.any }, capability: "js", throws: true, length: 0 },
  proto: {
    capacity: { getter: b.u32 },
    length: { getter: b.u32 },
    closed: { getter: b.bool },
    send: { ...op, args: { value: b.any, options: b.any }, length: 1 },
    receive: { ...op, args: { options: b.any }, length: 0 },
    close: {},
    "@@dispose": { rustName: "close" },
    "@@asyncIterator": { builtin: true, length: 0 },
    "@@inspectCustom": { builtin: true, length: 2 },
    __next: { ...op, privateSymbol: "channelNext" },
  },
});
