use core::ffi::c_void;
use core::ptr::NonNull;

use bun_jsc::{JSGlobalObject, JSValue, JsResult};

unsafe extern "C" {
    safe fn Bun__ChannelMessage__create(
        global: &JSGlobalObject,
        value: JSValue,
        transfer: JSValue,
    ) -> *mut c_void;
    fn Bun__ChannelMessage__deserialize(global: &JSGlobalObject, message: NonNull<c_void>) -> JSValue;
    fn Bun__ChannelMessage__destroy(message: NonNull<c_void>);
}

/// A structured clone made like `worker.postMessage()`'s: `SharedArrayBuffer`s, `WebAssembly.Memory`
/// and `Bun.Channel`s are shared, what `transfer` lists is moved, the rest is copied. Any VM of the
/// process can read it, once. Owns a C++ `WebCore::MessageWithMessagePorts` (`ChannelMessage.cpp`).
pub(crate) struct ChannelMessage(NonNull<c_void>);

// SAFETY: the message holds no VM or JS state. Its bytes are its own; the buffers it moved or
// shares, the native objects it shares and the ports it moves are refcounted thread-safely, so it
// can be read or dropped on any thread. It is owned, so one thread uses it at a time.
unsafe impl Send for ChannelMessage {}

impl ChannelMessage {
    /// Clones `value`, moving what `transfer` (`undefined` or an iterable) lists. Throws what
    /// `postMessage()` throws: a `DataCloneError`, or the `TypeError` of a bad transfer list.
    pub(crate) fn create(global: &JSGlobalObject, value: JSValue, transfer: JSValue) -> JsResult<Self> {
        let message = bun_jsc::call_check_slow(global, || {
            Bun__ChannelMessage__create(global, value, transfer)
        })?;
        Ok(Self(
            NonNull::new(message).expect("a message, or an exception"),
        ))
    }

    /// The value, in `global`'s realm. Throws if the value cannot be made there.
    pub(crate) fn deserialize(self, global: &JSGlobalObject) -> JsResult<JSValue> {
        let message = core::mem::ManuallyDrop::new(self).0;
        // SAFETY: `message` is ours; `deserialize` consumes it.
        bun_jsc::host_fn::from_js_host_call(global, || unsafe {
            Bun__ChannelMessage__deserialize(global, message)
        })
    }
}

impl Drop for ChannelMessage {
    fn drop(&mut self) {
        // SAFETY: ours, and not read.
        unsafe { Bun__ChannelMessage__destroy(self.0) }
    }
}
