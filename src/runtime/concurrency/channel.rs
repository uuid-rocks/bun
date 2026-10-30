//! `Bun.Channel`: Go's channels, between the threads of the process.
//!
//! The channel is native and shared: every VM that has one (`worker.postMessage(channel)`, a
//! channel sent through a channel) holds an `Arc` of the same [`ChannelCore`]. Its operations are
//! Go's `chansend`/`chanrecv` under one mutex: a send hands its value straight to the oldest
//! waiting receiver, else buffers it while there is room, else waits; a receive takes the oldest
//! buffered value (refilling the buffer from the oldest waiting sender), else the oldest waiting
//! sender's value, else waits. Values are cloned on the sending VM and read on the receiving one,
//! even when they are the same VM.
//!
//! An operation that waits is a [`Waiter`] in the channel's queue and a [`ChannelOp`] (its
//! promise) in its VM's table. Whoever settles the waiter first wins it: another thread handing it
//! a value or closing the channel ([`Waiter::fire`]), or its own VM giving up on it — its signal
//! aborted, its context stopped ([`Waiter::cancel`]). The winner on another thread posts the
//! outcome to the waiter's VM ([`ChannelWake`]). A waiting operation does not keep its VM alive.

use core::sync::atomic::{AtomicU32, Ordering};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bun_event_loop::{ConcurrentTask::ConcurrentTask, ContextId, Task, TaskTag, Taskable, task_tag};
use bun_jsc::bindgen::{JsLocal, JsPayload as _, JsScope};
use bun_jsc::virtual_machine::VirtualMachine;
use bun_jsc::{
    AbortCause, AbortHandle, AbortSignal, AbortSignalRef, ErrorCode, JSGlobalObject, JSPromise,
    JSPromiseStrong, JSValue, JsResult, LoopKind, Posted, VmHandle,
};
use bun_threading::Guarded;

use super::message::ChannelMessage;
use super::ops::OpTable;
use crate::node::util::validators;

pub(crate) struct ChannelCore {
    capacity: u32,
    state: Guarded<State>,
}

#[derive(Default)]
struct State {
    /// At most `capacity` values. Empty while a receiver waits.
    buffer: VecDeque<ChannelMessage>,
    /// Waiting receivers, oldest first. Empty while anything is buffered or a sender waits.
    receivers: VecDeque<Arc<Waiter>>,
    /// Waiting senders and their values, oldest first. Empty while a receiver waits.
    senders: VecDeque<(Arc<Waiter>, ChannelMessage)>,
    closed: bool,
}

/// One waiting operation, as the channel's queue holds it.
pub(crate) struct Waiter {
    vm: VmHandle,
    loop_kind: LoopKind,
    op_id: u64,
    state: AtomicU32,
}

const WAITING: u32 = 0;
const FIRED: u32 = 1;
const CANCELLED: u32 = 2;

impl Waiter {
    /// Wins the waiter for a result. Any thread.
    fn fire(&self) -> bool {
        self.state
            .compare_exchange(WAITING, FIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Wins the waiter for its own VM, which drops it. JS thread of that VM.
    fn cancel(&self) -> bool {
        self.state
            .compare_exchange(WAITING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Posts the result of a waiter this thread [fired](Self::fire) to its VM. Nothing locked.
    fn wake(&self, result: WakeResult) {
        let wake = bun_core::heap::into_raw(Box::new(ChannelWake {
            op_id: self.op_id,
            result,
        }));
        let task = ConcurrentTask::create(Task::init(wake));
        if let Posted::Refused(task) = self.vm.post(self.loop_kind, task) {
            // The VM is gone, and the operation with it.
            // SAFETY: a refused task was never queued: the carrier and the payload are ours.
            unsafe {
                ConcurrentTask::release_refused(task);
                drop(bun_core::heap::take(wake));
            }
        }
    }
}

enum WakeResult {
    Received(ChannelMessage),
    Sent,
    Closed,
}

/// The outcome of a waiting operation, on its way to the operation's VM.
pub(crate) struct ChannelWake {
    op_id: u64,
    result: WakeResult,
}

impl Taskable for ChannelWake {
    const TAG: TaskTag = task_tag::ChannelWake;
    unsafe fn release_unrun(this: *mut Self) {
        // SAFETY: trait contract — the boxed wake, ours.
        drop(unsafe { bun_core::heap::take(this) });
    }
    /// The operation decides: it left the table if its context stopped.
    unsafe fn context(_: *const Self) -> ContextId {
        ContextId::NONE
    }
}

impl ChannelWake {
    pub(crate) fn run(this: *mut Self, global: &JSGlobalObject) -> JsResult<()> {
        // SAFETY: the dispatch hands over the boxed wake.
        let wake = unsafe { bun_core::heap::take(this) };
        match ops().remove(wake.op_id) {
            Some(op) => op.settle(global, wake.result),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
enum OpKind {
    Send,
    Receive,
    /// A receive for an async iterator: `{ value, done }`, and the end is `done: true`.
    Next,
}

/// A waiting operation, as its VM holds it: in [`ops`] by its waiter's `op_id`.
pub(crate) struct ChannelOp {
    abort_handle: AbortHandle,
    id: u64,
    kind: OpKind,
    promise: JSPromiseStrong,
    waiter: Arc<Waiter>,
    channel: Arc<ChannelCore>,
}

bun_jsc::impl_abort_handle_owner!(ChannelOp, abort_handle, |this, cause| {
    // SAFETY: trait contract — `this` is live, in the table.
    let (id, waiter) = unsafe { ((*this).id, &(*this).waiter) };
    let won = waiter.cancel();
    if won {
        // SAFETY: as above.
        unsafe { (*this).channel.forget(waiter) };
    } else if matches!(cause, AbortCause::Signal(_)) {
        // The outcome is on its way and settles it.
        return;
    }
    let op = ops().remove(id).expect("an operation is in the table until it settles");
    if let AbortCause::Signal(reason) = cause {
        let global = VirtualMachine::get().global();
        let mut promise = op.promise;
        let _ = promise.reject(global, Ok(reason));
    }
});

impl ChannelOp {
    fn settle(self: Box<Self>, global: &JSGlobalObject, result: WakeResult) -> JsResult<()> {
        let ChannelOp {
            kind, mut promise, ..
        } = *self;
        let value = match (kind, result) {
            (OpKind::Send, WakeResult::Sent) => Ok(JSValue::UNDEFINED),
            (OpKind::Send | OpKind::Receive, WakeResult::Closed) => {
                return promise.reject(global, Ok(closed_error(global)));
            }
            (OpKind::Receive, WakeResult::Received(message)) => message.deserialize(global),
            (OpKind::Next, WakeResult::Received(message)) => message
                .deserialize(global)
                .map(|value| iterator_result(global, value, false)),
            (OpKind::Next, WakeResult::Closed) => {
                Ok(iterator_result(global, JSValue::UNDEFINED, true))
            }
            (OpKind::Send, WakeResult::Received(_))
            | (OpKind::Receive | OpKind::Next, WakeResult::Sent) => {
                unreachable!("a sender is only sent to, a receiver only received from")
            }
        };
        promise.settle(global, value)
    }
}

/// This VM's waiting channel operations.
fn ops() -> &'static mut OpTable<ChannelOp> {
    let state = crate::jsc_hooks::runtime_state();
    debug_assert!(!state.is_null());
    // SAFETY: the live per-thread `RuntimeState`; JS thread. Every use is one statement, and
    // nothing it calls reaches the table.
    unsafe { &mut (*state).channel_ops }
}

fn closed_error(global: &JSGlobalObject) -> JSValue {
    global
        .err(ErrorCode::CHANNEL_CLOSED, format_args!("Channel is closed"))
        .to_js()
}

fn iterator_result(global: &JSGlobalObject, value: JSValue, done: bool) -> JSValue {
    let result = JSValue::create_empty_object(global, 2);
    result.put(global, "value", value);
    result.put(global, "done", JSValue::js_boolean(done));
    result
}

enum SendStep {
    Done,
    Closed,
    Wait(Arc<Waiter>),
}

impl ChannelCore {
    fn send(&self, waiter: impl FnOnce() -> Arc<Waiter>, message: ChannelMessage) -> SendStep {
        let mut state = self.state.lock();
        if state.closed {
            drop(state);
            return SendStep::Closed;
        }
        while let Some(receiver) = state.receivers.pop_front() {
            if receiver.fire() {
                drop(state);
                receiver.wake(WakeResult::Received(message));
                return SendStep::Done;
            }
        }
        if state.buffer.len() < self.capacity as usize {
            state.buffer.push_back(message);
            return SendStep::Done;
        }
        let waiter = waiter();
        state.senders.push_back((waiter.clone(), message));
        SendStep::Wait(waiter)
    }

    /// `Ok(None)`: closed and drained.
    fn receive(&self, waiter: impl FnOnce() -> Arc<Waiter>) -> Result<Option<ChannelMessage>, Arc<Waiter>> {
        let mut state = self.state.lock();
        let mut sender = None;
        let message = if let Some(message) = state.buffer.pop_front() {
            if let Some((waiter, refill)) = Self::pop_sender(&mut state) {
                state.buffer.push_back(refill);
                sender = Some(waiter);
            }
            message
        } else if let Some((waiter, message)) = Self::pop_sender(&mut state) {
            sender = Some(waiter);
            message
        } else if state.closed {
            return Ok(None);
        } else {
            let waiter = waiter();
            state.receivers.push_back(waiter.clone());
            return Err(waiter);
        };
        drop(state);
        if let Some(sender) = sender {
            sender.wake(WakeResult::Sent);
        }
        Ok(Some(message))
    }

    /// The oldest sender still waiting, fired.
    fn pop_sender(state: &mut State) -> Option<(Arc<Waiter>, ChannelMessage)> {
        while let Some((sender, message)) = state.senders.pop_front() {
            if sender.fire() {
                return Some((sender, message));
            }
        }
        None
    }

    fn close(&self) {
        let (receivers, senders) = {
            let mut state = self.state.lock();
            if state.closed {
                return;
            }
            state.closed = true;
            (
                core::mem::take(&mut state.receivers),
                core::mem::take(&mut state.senders),
            )
        };
        for waiter in receivers.iter().chain(senders.iter().map(|(waiter, _)| waiter)) {
            if waiter.fire() {
                waiter.wake(WakeResult::Closed);
            }
        }
    }

    /// Takes a [cancelled](Waiter::cancel) waiter out of its queue.
    fn forget(&self, waiter: &Arc<Waiter>) {
        let dropped = {
            let mut state = self.state.lock();
            if let Some(i) = state.receivers.iter().position(|w| Arc::ptr_eq(w, waiter)) {
                state.receivers.remove(i);
                None
            } else {
                let i = state.senders.iter().position(|(w, _)| Arc::ptr_eq(w, waiter));
                i.and_then(|i| state.senders.remove(i))
            }
        };
        // Its value, outside the lock: it can hold the last handle of another channel.
        drop(dropped);
    }
}

/// `Bun.Channel`: the JS object of one VM, which shares the channel.
pub(crate) struct Channel {
    core: Arc<ChannelCore>,
}

impl Channel {
    pub(crate) fn constructor(cx: &mut JsScope<'_>, capacity: JsLocal<'_>) -> JsResult<Self> {
        let capacity = capacity.to_js();
        let capacity = if capacity.is_undefined() {
            0
        } else {
            validators::validate_integer(cx.global(), capacity, "capacity", Some(0), Some(u32::MAX.into()))?
                as u32
        };
        Ok(Self {
            core: Arc::new(ChannelCore {
                capacity,
                state: Guarded::new(State::default()),
            }),
        })
    }

    pub(crate) fn get_capacity(&self) -> u32 {
        self.core.capacity
    }

    pub(crate) fn get_length(&self) -> u32 {
        self.core.state.lock().buffer.len() as u32
    }

    pub(crate) fn get_closed(&self) -> bool {
        self.core.state.lock().closed
    }

    pub(crate) fn close(&self) {
        self.core.close();
    }

    pub(crate) fn send(
        &self,
        cx: &mut JsScope<'_>,
        value: JsLocal<'_>,
        options: JsLocal<'_>,
    ) -> JsResult<JSValue> {
        let global = cx.global();
        let options = Options::parse(global, options.to_js(), true)?;
        if let Some(rejected) = options.rejected_if_aborted(global) {
            return Ok(rejected);
        }
        if self.core.state.lock().closed {
            return Ok(JSPromise::rejected_promise(global, closed_error(global)).to_js());
        }
        let message = ChannelMessage::create(global, value.to_js(), options.transfer)?;
        let id = ops().next_id();
        Ok(match self.core.send(|| self.waiter(global, id), message) {
            SendStep::Done => JSPromise::resolved_promise_value(global, JSValue::UNDEFINED),
            SendStep::Closed => JSPromise::rejected_promise(global, closed_error(global)).to_js(),
            SendStep::Wait(waiter) => self.wait(global, OpKind::Send, waiter, options.signal),
        })
    }

    pub(crate) fn receive(&self, cx: &mut JsScope<'_>, options: JsLocal<'_>) -> JsResult<JSValue> {
        let global = cx.global();
        let options = Options::parse(global, options.to_js(), false)?;
        self.receive_as(global, OpKind::Receive, options)
    }

    pub(crate) fn next(&self, cx: &mut JsScope<'_>) -> JsResult<JSValue> {
        let global = cx.global();
        self.receive_as(global, OpKind::Next, Options::NONE)
    }

    fn receive_as(&self, global: &JSGlobalObject, kind: OpKind, options: Options) -> JsResult<JSValue> {
        if let Some(rejected) = options.rejected_if_aborted(global) {
            return Ok(rejected);
        }
        let id = ops().next_id();
        match self.core.receive(|| self.waiter(global, id)) {
            Ok(Some(message)) => {
                let value = message.deserialize(global)?;
                let value = match kind {
                    OpKind::Next => iterator_result(global, value, false),
                    _ => value,
                };
                Ok(JSPromise::resolved_promise_value(global, value))
            }
            Ok(None) => Ok(match kind {
                OpKind::Next => JSPromise::resolved_promise_value(
                    global,
                    iterator_result(global, JSValue::UNDEFINED, true),
                ),
                _ => JSPromise::rejected_promise(global, closed_error(global)).to_js(),
            }),
            Err(waiter) => Ok(self.wait(global, kind, waiter, options.signal)),
        }
    }

    fn waiter(&self, global: &JSGlobalObject, op_id: u64) -> Arc<Waiter> {
        let vm = global.bun_vm();
        Arc::new(Waiter {
            vm: vm.handle(),
            loop_kind: vm.current_loop_kind(),
            op_id,
            state: AtomicU32::new(WAITING),
        })
    }

    /// The promise of an operation whose waiter is queued. Nothing can settle it before this
    /// returns: its wake runs on this thread, and so does its signal.
    fn wait(
        &self,
        global: &JSGlobalObject,
        kind: OpKind,
        waiter: Arc<Waiter>,
        signal: Option<AbortSignalRef>,
    ) -> JSValue {
        let id = waiter.op_id;
        let op = Box::new(ChannelOp {
            abort_handle: AbortHandle::for_owner::<ChannelOp>(),
            id,
            kind,
            promise: JSPromiseStrong::init(global),
            waiter,
            channel: self.core.clone(),
        });
        let promise = op.promise.value();
        let op = ops().insert(id, op).as_ptr();
        let vm = global.bun_vm();
        // SAFETY: `op` is in the table, which frees it only through `remove`; the handle is
        // disarmed when it drops.
        unsafe {
            AbortHandle::arm_owner(op, vm.context_of_caller());
            if let Some(signal) = signal {
                AbortHandle::follow_owner(op, signal);
            }
        }
        promise
    }
}

struct Options {
    signal: Option<AbortSignalRef>,
    transfer: JSValue,
}

impl Options {
    const NONE: Options = Options {
        signal: None,
        transfer: JSValue::UNDEFINED,
    };

    fn parse(global: &JSGlobalObject, options: JSValue, send: bool) -> JsResult<Self> {
        let mut parsed = Options::NONE;
        if options.is_undefined() {
            return Ok(parsed);
        }
        validators::validate_object(global, options, "options", Default::default())?;
        if let Some(signal) = options.get(global, "signal")? {
            if !signal.is_undefined() {
                let Some(signal) = AbortSignal::ref_from_js(signal) else {
                    return Err(validators::throw_err_invalid_arg_type(
                        global,
                        "options.signal",
                        "AbortSignal",
                        signal,
                    ));
                };
                parsed.signal = Some(signal);
            }
        }
        if send {
            if let Some(transfer) = options.get(global, "transfer")? {
                parsed.transfer = transfer;
            }
        }
        Ok(parsed)
    }

    fn rejected_if_aborted(&self, global: &JSGlobalObject) -> Option<JSValue> {
        let signal = self.signal.as_ref()?;
        if !signal.aborted() {
            return None;
        }
        Some(JSPromise::rejected_promise(global, signal.js_reason(global)).to_js())
    }
}

/// The channels in the structured clone messages that exist, on any thread, by what the message
/// has in their place.
static IN_FLIGHT: Guarded<BTreeMap<u64, Arc<ChannelCore>>> = Guarded::new(BTreeMap::new());

/// `channel` is a `Bun.Channel`.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Channel__retainForStructuredClone(channel: JSValue) -> u64 {
    let Some(channel) = channel.as_class_ref::<Channel>() else {
        return 0;
    };
    // Not one that bytes from elsewhere (IPC, `v8.deserialize()`) can guess.
    let mut id = [0; 8];
    bun_boringssl_sys::rand_bytes(&mut id);
    let id = u64::from_ne_bytes(id);
    IN_FLIGHT.lock().insert(id, channel.core.clone());
    id
}

/// `JSValue::ZERO` if no message has this id. A message can be read many times (`BroadcastChannel`).
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Channel__fromStructuredClone(global: &JSGlobalObject, id: u64) -> JSValue {
    let Some(core) = IN_FLIGHT.lock().get(&id).cloned() else {
        return JSValue::ZERO;
    };
    Channel { core }.into_js_wrapper(global).to_js()
}

/// The message with this id is gone.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Channel__releaseStructuredClone(id: u64) {
    // The channel, outside the lock: it can be the last handle, and its buffer can hold messages
    // with channels of their own.
    let core = IN_FLIGHT.lock().remove(&id);
    drop(core);
}
