use core::ffi::c_void;
use core::ptr::NonNull;

use bun_collections::smallvec::SmallVec;
use bun_jsc::{JSGlobalObject, JSValue, JsResult};

unsafe extern "C" {
    safe fn Bun__ChannelMessage__createFor(
        global: &JSGlobalObject,
        value: JSValue,
        transfer: JSValue,
        site: u8,
        index: u32,
    ) -> *mut c_void;
    fn Bun__ChannelMessage__deserialize(
        global: &JSGlobalObject,
        message: NonNull<c_void>,
    ) -> JSValue;
    fn Bun__ChannelMessage__destroy(message: NonNull<c_void>);
    fn Bun__ChannelMessage__channelIds(
        message: NonNull<c_void>,
        out: *mut u64,
        capacity: usize,
    ) -> usize;
}

/// The channel operation a value is cloned for, which its `DataCloneError` names.
#[derive(Clone, Copy)]
pub(crate) enum CloneSite {
    /// `channel.send(value)`
    Send,
    /// The value of `Bun.select()`'s send case at this index
    SelectSend(u32),
}

/// A structured clone made like `worker.postMessage()`'s: `SharedArrayBuffer`s, `WebAssembly.Memory`
/// and `Bun.Channel`s are shared, what `transfer` lists is moved, the rest is copied. Any VM of the
/// process can read it, once. Owns a C++ `WebCore::MessageWithMessagePorts` (`ChannelMessage.cpp`).
pub(crate) struct ChannelMessage {
    message: NonNull<c_void>,
    /// It shares a `Bun.Channel`: in a channel's queue, an edge of the channel graph
    /// ([`graph`](super::graph)).
    carries_channels: bool,
}

// SAFETY: the message holds no VM or JS state. Its bytes are its own; the buffers it moved or
// shares, the native objects it shares and the ports it moves are refcounted thread-safely, so it
// can be read or dropped on any thread. It is owned, so one thread uses it at a time.
unsafe impl Send for ChannelMessage {}

impl ChannelMessage {
    /// Clones `value`, moving what `transfer` (`undefined` or an iterable) lists. Throws what
    /// `postMessage()` throws: a `DataCloneError` (which says what in `value` can't be cloned, and
    /// where), or the `TypeError` of a bad transfer list.
    pub(crate) fn create(
        global: &JSGlobalObject,
        value: JSValue,
        transfer: JSValue,
        site: CloneSite,
    ) -> JsResult<Self> {
        let (site, index) = match site {
            CloneSite::Send => (0, 0),
            CloneSite::SelectSend(index) => (1, index),
        };
        let message = bun_jsc::call_check_slow(global, || {
            Bun__ChannelMessage__createFor(global, value, transfer, site, index)
        })?;
        let message = NonNull::new(message).expect("a message, or an exception");
        // SAFETY: ours; a capacity of 0 writes nothing.
        let carries_channels =
            unsafe { Bun__ChannelMessage__channelIds(message, core::ptr::null_mut(), 0) } != 0;
        Ok(Self {
            message,
            carries_channels,
        })
    }

    pub(super) fn carries_channels(&self) -> bool {
        self.carries_channels
    }

    /// The `Channel__retainForStructuredClone` ids of the channels it shares.
    pub(super) fn channel_ids(&self) -> SmallVec<[u64; 4]> {
        let mut ids = SmallVec::<[u64; 4]>::new();
        if !self.carries_channels {
            return ids;
        }
        loop {
            let capacity = ids.capacity();
            // SAFETY: ours; `ids` has room for `capacity` ids.
            let count = unsafe {
                Bun__ChannelMessage__channelIds(self.message, ids.as_mut_ptr(), capacity)
            };
            if count <= capacity {
                // SAFETY: the first `count` were written.
                unsafe { ids.set_len(count) };
                return ids;
            }
            ids.reserve(count);
        }
    }

    /// The value, in `global`'s realm. Throws if the value cannot be made there.
    pub(crate) fn deserialize(self, global: &JSGlobalObject) -> JsResult<JSValue> {
        let message = core::mem::ManuallyDrop::new(self).message;
        // SAFETY: `message` is ours; `deserialize` consumes it.
        bun_jsc::host_fn::from_js_host_call(global, || unsafe {
            Bun__ChannelMessage__deserialize(global, message)
        })
    }
}

impl Drop for ChannelMessage {
    fn drop(&mut self) {
        // SAFETY: ours, and not read.
        unsafe { Bun__ChannelMessage__destroy(self.message) }
    }
}
