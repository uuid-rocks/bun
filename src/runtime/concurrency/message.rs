use core::ffi::c_void;
use core::ptr::NonNull;

use bun_collections::smallvec::SmallVec;

use super::channel::ChannelCore;
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

unsafe extern "C" {
    safe fn Bun__SharedHandle__wrapper(global: &JSGlobalObject, address: *const c_void) -> JSValue;
    safe fn Bun__SharedHandle__remember(wrapper: JSValue, address: *const c_void);
}

/// The wrapper `global`'s realm has for the shared native object at `address` (a channel's or a
/// mutex's core) while it is alive, else the one `create` makes, which it then remembers: a
/// channel that comes back to the thread it left is the same object.
pub(crate) fn wrapper_of<T>(
    global: &JSGlobalObject,
    address: *const T,
    create: impl FnOnce() -> JSValue,
) -> JSValue {
    let address = address.cast::<c_void>();
    let wrapper = Bun__SharedHandle__wrapper(global, address);
    if !wrapper.is_empty() {
        return wrapper;
    }
    let wrapper = create();
    Bun__SharedHandle__remember(wrapper, address);
    wrapper
}

/// `wrapper` is its realm's wrapper of the shared native object at `address`.
pub(crate) fn remember_wrapper<T>(wrapper: JSValue, address: *const T) {
    Bun__SharedHandle__remember(wrapper, address.cast::<c_void>());
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
    /// The cores of the `Bun.Channel`s it shares, which its clone holds while it exists. In a
    /// channel's queue, its edges in the channel graph ([`graph`](super::graph)).
    targets: Box<[*const ChannelCore]>,
}

// SAFETY: the message holds no VM or JS state. Its bytes are its own; the buffers it moved or
// shares, the native objects it shares and the ports it moves are refcounted thread-safely, so it
// can be read or dropped on any thread. It is owned, so one thread uses it at a time. `targets`
// point to channel cores, which are `Sync`, and which its clone keeps alive.
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
        let ids = channel_ids(message);
        let targets = if ids.is_empty() {
            Box::default()
        } else {
            super::channel::cores_of(&ids)
        };
        Ok(Self { message, targets })
    }

    pub(super) fn carries_channels(&self) -> bool {
        !self.targets.is_empty()
    }

    /// The cores of the channels it shares, alive while it is.
    pub(super) fn targets(&self) -> &[*const ChannelCore] {
        &self.targets
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

/// The `Channel__retainForStructuredClone` ids of the channels `message` shares.
fn channel_ids(message: NonNull<c_void>) -> SmallVec<[u64; 4]> {
    let mut ids = SmallVec::<[u64; 4]>::new();
    loop {
        let capacity = ids.capacity();
        // SAFETY: a live message; `ids` has room for `capacity` ids.
        let count =
            unsafe { Bun__ChannelMessage__channelIds(message, ids.as_mut_ptr(), capacity) };
        if count <= capacity {
            // SAFETY: the first `count` were written.
            unsafe { ids.set_len(count) };
            return ids;
        }
        ids.reserve(count);
    }
}

impl Drop for ChannelMessage {
    fn drop(&mut self) {
        // SAFETY: ours, and not read.
        unsafe { Bun__ChannelMessage__destroy(self.message) }
    }
}
