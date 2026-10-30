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
//!
//! A [`select`](super::select) queues one waiter in the channels of all its cases, each entry
//! ([`Queued`]) with the index of its case. The entries of a waiter that fired or was cancelled
//! stay queued until its VM takes them out ([`ChannelCore::forget`]); until then whoever reaches
//! one fails to fire it and drops it.
//!
//! A value handed to a waiting receiver is a [`Delivery`] until the receiver's VM reads it. If that
//! VM is gone first, or the operation is, the value goes back to the channel: the sender was told
//! its send happened, so the value is still received exactly once.
//!
//! Values that share channels make a graph of channels; [`graph`](super::graph) reclaims the
//! channels that only the queues of unreachable channels hold.

use core::mem::ManuallyDrop;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bun_collections::smallvec::SmallVec;
use bun_event_loop::{
    ConcurrentTask::ConcurrentTask, ContextId, Task, TaskTag, Taskable, task_tag,
};
use bun_jsc::bindgen::{JsLocal, JsPayload as _, JsScope};
use bun_jsc::virtual_machine::VirtualMachine;
use bun_jsc::{
    AbortCause, AbortHandle, AbortSignal, AbortSignalRef, ErrorCode, JSGlobalObject, JSPromise,
    JSPromiseStrong, JSValue, JsResult, LoopKind, Posted, VmHandle,
};
use bun_threading::Guarded;

use super::graph;
use super::message::{self, ChannelMessage, CloneSite};
use super::mutex::{Handoff, MutexCore, Wakeup};
use super::ops::OpTable;
use crate::node::util::validators;

pub(crate) struct ChannelCore {
    pub(super) capacity: u32,
    pub(super) state: Guarded<State>,
    /// Its queues may hold a value that shares a channel: set under `state`'s lock when they do,
    /// cleared by the [`graph`]'s collection when they don't.
    pub(super) holds_channels: AtomicBool,
    /// Queued for the [`graph`]'s collection. Apart, so that a release can register the channel
    /// after it dropped its reference.
    pub(super) candidate: Arc<AtomicBool>,
    /// How many values in channels' queues share this channel: the edges into it. A channel with
    /// more references than that has a root.
    pub(super) incoming: AtomicU32,
}

/// How many channels exist, for `bun:internal-for-testing`.
static LIVE: AtomicU32 = AtomicU32::new(0);

impl ChannelCore {
    fn new(capacity: u32) -> Arc<Self> {
        LIVE.fetch_add(1, Ordering::Relaxed);
        Arc::new(ChannelCore {
            capacity,
            state: Guarded::new(State::default()),
            holds_channels: AtomicBool::new(false),
            candidate: Arc::new(AtomicBool::new(false)),
            incoming: AtomicU32::new(0),
        })
    }
}

impl Drop for ChannelCore {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Of the entries whose waiter can still fire, and but for a select that waits both to send and
/// to receive on one unbuffered channel: no receiver waits while anything is buffered or a sender
/// waits.
#[derive(Default)]
pub(super) struct State {
    /// At most `capacity` values, but for values given back by receivers that went away
    /// ([`Delivery`]), which go first.
    buffer: VecDeque<ChannelMessage>,
    /// Waiting receivers, oldest first.
    pub(super) receivers: VecDeque<Queued>,
    /// Waiting senders and their values, oldest first.
    senders: VecDeque<(Queued, ChannelMessage)>,
    closed: bool,
    /// How many values of `buffer` and `senders` share channels: the channel's edges in the
    /// [`graph`].
    pub(super) edges: u32,
    /// Values handed to receivers that have not read them yet ([`Delivery`]). Each may come back:
    /// a closed channel ends only once none is out.
    deliveries: u32,
}

/// A waiter in a channel's queue, for one case of its operation: `0` but in a select.
pub(super) struct Queued {
    pub(super) waiter: Arc<Waiter>,
    pub(super) case: u32,
}

impl Queued {
    fn fire(&self) -> bool {
        self.waiter.fire()
    }

    /// Posts the result of a waiter this thread fired to its VM. Nothing locked.
    pub(super) fn wake(self, result: WakeResult) {
        self.waiter.wake(self.case, result);
    }
}

/// A send that happened at once.
pub(super) enum SendNow {
    /// Nothing was sent: drop the value once the channel is unlocked.
    Closed(ChannelMessage),
    /// To this receiver, which fired: wake it with the value once the channel is unlocked.
    Handed(Queued, ChannelMessage),
    Buffered,
}

/// A receive that happened at once.
pub(super) enum ReceiveNow {
    /// And the sender, fired, that it came from or that refilled the buffer: wake it once the
    /// channel is unlocked.
    Value(ChannelMessage, Option<Queued>),
    /// Closed and drained.
    Closed,
}

impl State {
    /// `message` enters a queue: its channels are edges of this one.
    fn enter(&mut self, message: &ChannelMessage) {
        if !message.carries_channels() {
            return;
        }
        self.edges += 1;
        for &target in message.targets() {
            // SAFETY: the message holds the core.
            unsafe { &*target }.incoming.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// `message` leaves a queue.
    fn leave(&mut self, message: &ChannelMessage) {
        if !message.carries_channels() {
            return;
        }
        self.edges -= 1;
        for &target in message.targets() {
            // SAFETY: the message holds the core.
            unsafe { &*target }.incoming.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn push_buffer(&mut self, message: ChannelMessage) {
        self.enter(&message);
        self.buffer.push_back(message);
    }

    fn pop_buffer(&mut self) -> Option<ChannelMessage> {
        let message = self.buffer.pop_front()?;
        self.leave(&message);
        Some(message)
    }

    pub(super) fn push_sender(&mut self, sender: Queued, message: ChannelMessage) {
        self.enter(&message);
        self.senders.push_back((sender, message));
    }

    fn pop_sender_entry(&mut self) -> Option<(Queued, ChannelMessage)> {
        let (sender, message) = self.senders.pop_front()?;
        self.leave(&message);
        Some((sender, message))
    }

    /// Every value it holds, for the [`graph`] to reclaim.
    pub(super) fn values(&self) -> impl Iterator<Item = &ChannelMessage> {
        self.buffer
            .iter()
            .chain(self.senders.iter().map(|(_, message)| message))
    }

    /// Empties a channel that is unreachable. Its values and entries, to drop once it is unlocked.
    pub(super) fn take_all(
        &mut self,
    ) -> (VecDeque<ChannelMessage>, VecDeque<(Queued, ChannelMessage)>) {
        self.receivers.clear();
        let buffer = core::mem::take(&mut self.buffer);
        let senders = core::mem::take(&mut self.senders);
        for message in buffer
            .iter()
            .chain(senders.iter().map(|(_, message)| message))
        {
            self.leave(message);
        }
        (buffer, senders)
    }

    /// `Err`: the send would wait. The value back.
    pub(super) fn try_send(
        &mut self,
        capacity: u32,
        message: ChannelMessage,
    ) -> Result<SendNow, ChannelMessage> {
        if self.closed {
            return Ok(SendNow::Closed(message));
        }
        while let Some(receiver) = self.receivers.pop_front() {
            if receiver.fire() {
                self.deliveries += 1;
                return Ok(SendNow::Handed(receiver, message));
            }
        }
        if self.buffer.len() < capacity as usize {
            self.push_buffer(message);
            return Ok(SendNow::Buffered);
        }
        Err(message)
    }

    /// `None`: the receive would wait.
    pub(super) fn try_receive(&mut self, capacity: u32) -> Option<ReceiveNow> {
        if let Some(message) = self.pop_buffer() {
            let sender = if self.buffer.len() < capacity as usize {
                self.pop_sender().map(|(sender, refill)| {
                    self.push_buffer(refill);
                    sender
                })
            } else {
                None
            };
            return Some(ReceiveNow::Value(message, sender));
        }
        if let Some((sender, message)) = self.pop_sender() {
            return Some(ReceiveNow::Value(message, Some(sender)));
        }
        self.has_ended().then_some(ReceiveNow::Closed)
    }

    /// Closed, and nothing is left to receive or can come back.
    fn has_ended(&self) -> bool {
        self.closed && self.deliveries == 0
    }

    /// The oldest sender still waiting, fired.
    fn pop_sender(&mut self) -> Option<(Queued, ChannelMessage)> {
        while let Some((sender, message)) = self.pop_sender_entry() {
            if sender.fire() {
                return Some((sender, message));
            }
        }
        None
    }
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
    pub(super) fn fire(&self) -> bool {
        self.state
            .compare_exchange(WAITING, FIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// A waiter that [fired](Self::fire) for a [`Wakeup`] waits again. JS thread of its VM, which
    /// queues it again under the lock of what it waits for.
    pub(super) fn rearm(&self) {
        self.state.store(WAITING, Ordering::Release);
    }

    /// Wins the waiter for its own VM, which drops it. JS thread of that VM.
    fn cancel(&self) -> bool {
        self.state
            .compare_exchange(WAITING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Posts the result of a waiter this thread [fired](Self::fire) to its VM. Nothing locked.
    pub(super) fn wake(&self, case: u32, result: WakeResult) {
        let wake = bun_core::heap::into_raw(Box::new(ChannelWake {
            op_id: self.op_id,
            case,
            result,
        }));
        if self.vm.is_current() {
            let vm = VirtualMachine::get();
            if vm.current_loop_kind() == self.loop_kind {
                // Its own thread, which runs that loop. After a yield, as for a post from another
                // thread: channel traffic on one thread must not keep timers and I/O waiting.
                vm.event_loop_mut()
                    .enqueue_task_after_yield(Task::init(wake));
                return;
            }
        }
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

pub(super) enum WakeResult {
    Received(Delivery),
    Sent,
    Closed,
    /// A `Bun.Mutex` was handed to the waiter.
    Acquired(Handoff),
    /// A `Bun.Mutex` may be free: the waiter tries for it again.
    Retry(Wakeup),
}

/// A value handed to a receiver, on its way to the receiver's VM. Given back to its channel if it
/// is dropped unread: its send happened. Counted in the channel's `deliveries` by whoever handed it,
/// under the channel's lock.
pub(super) struct Delivery {
    message: Option<ChannelMessage>,
    channel: ManuallyDrop<Arc<ChannelCore>>,
}

impl Delivery {
    pub(super) fn new(message: ChannelMessage, channel: &Arc<ChannelCore>) -> Self {
        Self {
            message: Some(message),
            channel: ManuallyDrop::new(Arc::clone(channel)),
        }
    }

    fn take(mut self) -> ChannelMessage {
        let message = self.message.take().expect("a delivery is taken once");
        self.channel.delivered();
        message
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if let Some(message) = self.message.take() {
            self.channel.give_back(message);
        }
        // SAFETY: dropping; not used again.
        graph::release(unsafe { ManuallyDrop::take(&mut self.channel) });
    }
}

/// The outcome of a waiting operation, on its way to the operation's VM.
pub(crate) struct ChannelWake {
    op_id: u64,
    case: u32,
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
        if ending(global) {
            // Nothing would read a value. Its operation leaves the table when its context stops,
            // and a value goes back to its channel.
            return Ok(());
        }
        if let Some(op) = ops().get(wake.op_id) {
            // SAFETY: in the table, so live.
            let owner = unsafe { op.as_ref() }.owner;
            if owner != 0 && Bun__Go__ownerEnded(owner) {
                // Ahead of the `Bun__Go__cancelOwner` on its way here. What the wake carries
                // passes on as it drops.
                cancel(global, wake.op_id);
                return Ok(());
            }
        }
        if let WakeResult::Retry(wakeup) = wake.result {
            return match ops().get(wake.op_id) {
                Some(op) => ChannelOp::retry(op, global, wakeup),
                None => Ok(()),
            };
        }
        match ops().remove(wake.op_id) {
            Some(mut op) => op.settle(global, wake.case, wake.result),
            None => Ok(()),
        }
    }
}

pub(super) enum OpKind {
    Send,
    Receive,
    /// A receive for an async iterator: `{ value, done }`, and the end is `done: true`.
    Next,
    /// `Bun.select`: `{ index, value, closed }`. Which cases send.
    Select(Box<[bool]>),
    /// `Bun.Mutex`'s `lock()`.
    Lock,
}

/// The channels a waiter is queued in, each once.
pub(super) type Channels = SmallVec<[Arc<ChannelCore>; 2]>;

/// A waiting operation, as its VM holds it: in [`ops`] by its waiter's `op_id`.
pub(crate) struct ChannelOp {
    abort_handle: AbortHandle,
    id: u64,
    kind: OpKind,
    promise: JSPromiseStrong,
    waiter: Arc<Waiter>,
    channels: OpChannels,
    /// A select's `AbortSignal` cases, which it follows while it waits.
    signal_cases: Box<[SignalCase]>,
    /// The mutex a `lock()` waits for, and the signal that aborts it: a lock woken to try again
    /// whose signal aborted meanwhile rejects, as the signal could not cancel it.
    mutex: Option<(Arc<MutexCore>, Option<AbortSignalRef>)>,
    /// Where module code waits: no async function's frame shows a top-level `await`.
    top_level: Option<(bun_core::String, u32, u32)>,
    /// The owner of the goroutine that waits (BunGo.cpp), or 0.
    owner: u32,
}

/// A waiting operation's channels, which it [releases](graph::release).
struct OpChannels(Channels);

impl Drop for OpChannels {
    fn drop(&mut self) {
        for channel in core::mem::take(&mut self.0) {
            graph::release(channel);
        }
    }
}

/// A select's `AbortSignal` case: ready once the signal aborts, with `signal.reason` as its value.
pub(super) struct SignalCase {
    abort_handle: AbortHandle,
    op_id: u64,
    case: u32,
}

bun_jsc::impl_abort_handle_owner!(SignalCase, abort_handle, |this, cause| {
    // Followed, never armed in a context: the select's own handle is.
    let AbortCause::Signal(reason) = cause else {
        return;
    };
    // SAFETY: trait contract — `this` is live, owned by its operation in the table.
    let (id, case) = unsafe { ((*this).op_id, (*this).case) };
    let Some(op) = ops().get(id) else {
        return;
    };
    // SAFETY: in the table, so live.
    if !unsafe { op.as_ref() }.waiter.cancel() {
        // Another case won; its outcome is on its way.
        return;
    }
    let op = ops()
        .remove(id)
        .expect("an operation is in the table until it settles");
    op.forget();
    let global = VirtualMachine::get().global();
    // The rest of the operation, `this` among it, drops at the end of this scope.
    let mut promise = op.promise;
    let _ = promise.settle(
        global,
        Ok(select_result(global, i64::from(case), reason, false)),
    );
});

bun_jsc::impl_abort_handle_owner!(ChannelOp, abort_handle, |this, cause| {
    // SAFETY: trait contract — `this` is live, in the table.
    let id = unsafe { (*this).id };
    // SAFETY: as above.
    if !unsafe { (*this).waiter.cancel() } && matches!(cause, AbortCause::Signal(_)) {
        // The outcome is on its way and settles it.
        return;
    }
    let op = ops()
        .remove(id)
        .expect("an operation is in the table until it settles");
    op.forget();
    if let AbortCause::Signal(reason) = cause {
        let global = VirtualMachine::get().global();
        let mut promise = op.promise;
        let _ = promise.reject(global, Ok(reason));
    }
});

impl ChannelOp {
    /// What waits, and the async functions that await it, for a deadlock's report.
    pub(super) fn describe(&self, global: &JSGlobalObject) -> String {
        let what = match self.kind {
            OpKind::Send => "Channel.send()",
            OpKind::Receive => "Channel.receive()",
            OpKind::Next => "for await on a Channel",
            OpKind::Select(_) => "Bun.select()",
            OpKind::Lock => "Mutex.lock()",
        };
        let error = global.create_error_instance(format_args!("{what}"));
        error.attach_async_stack_from_promise(global, self.promise.get());
        let stack = match error
            .get(global, "stack")
            .and_then(|stack| stack.unwrap_or(JSValue::UNDEFINED).to_bun_string(global))
        {
            Ok(stack) => stack.to_string(),
            Err(_) => {
                global.clear_exception();
                String::new()
            }
        };
        let mut out = String::from(what);
        let mut frames = 0;
        for line in bun_core::strings::split(stack.as_bytes(), b"\n") {
            let line = line.trim_ascii();
            if line.starts_with(b"at ") && !bun_core::strings::contains(line, b"(internal:") {
                out.push_str("\n    ");
                let start = line.as_ptr() as usize - stack.as_ptr() as usize;
                out.push_str(&stack[start..start + line.len()]);
                frames += 1;
            }
        }
        if let (Some((url, line, column)), 0) = (&self.top_level, frames) {
            out.push_str(&format!("\n    at {url}:{line}:{column}"));
        }
        out
    }

    /// Takes the waiter out of the channels that still queue it.
    fn forget(&self) {
        for channel in &self.channels.0 {
            channel.forget(&self.waiter);
        }
        if let Some((mutex, _)) = &self.mutex {
            mutex.forget(&self.waiter);
        }
    }

    /// A `lock()` woken to try for its mutex again.
    fn retry(op: NonNull<ChannelOp>, global: &JSGlobalObject, wakeup: Wakeup) -> JsResult<()> {
        // SAFETY: in the table, so live; nothing below runs script until it leaves.
        let this = unsafe { op.as_ref() };
        let (mutex, signal) = this.mutex.as_ref().expect("only a lock is woken to retry");
        let mutex = Arc::clone(mutex);
        let aborted = signal
            .as_ref()
            .filter(|signal| signal.aborted())
            .map(|signal| signal.js_reason(global));
        match aborted {
            // The next waiter tries instead.
            Some(_) => drop(wakeup),
            None if mutex.retry(wakeup, &this.waiter) => {}
            None => return Ok(()),
        }
        let mut op = ops()
            .remove(this.id)
            .expect("an operation is in the table until it settles");
        op.abort_handle.disarm();
        match aborted {
            Some(reason) => op.promise.reject(global, Ok(reason)),
            None => {
                Handoff::kept(mutex);
                op.promise.settle(global, Ok(JSValue::UNDEFINED))
            }
        }
    }

    fn settle(&mut self, global: &JSGlobalObject, case: u32, result: WakeResult) -> JsResult<()> {
        // Out of the table, so out of reach of a context stop that settling runs (a `then` getter).
        self.abort_handle.disarm();
        if let OpKind::Select(sends) = &self.kind {
            // The cases that lost.
            self.forget();
            let promise = &mut self.promise;
            let value = match result {
                WakeResult::Received(delivery) => delivery
                    .take()
                    .deserialize(global)
                    .map(|value| select_result(global, i64::from(case), value, false)),
                WakeResult::Sent => Ok(select_result(
                    global,
                    i64::from(case),
                    JSValue::UNDEFINED,
                    false,
                )),
                WakeResult::Closed if sends[case as usize] => {
                    return promise.reject(global, Ok(closed_error(global)));
                }
                WakeResult::Closed => Ok(select_result(
                    global,
                    i64::from(case),
                    JSValue::UNDEFINED,
                    true,
                )),
                WakeResult::Acquired(_) | WakeResult::Retry(_) => {
                    unreachable!("only a lock is handed a mutex")
                }
            };
            return promise.settle(global, value);
        }
        let promise = &mut self.promise;
        let value = match (&self.kind, result) {
            (OpKind::Send, WakeResult::Sent) => Ok(JSValue::UNDEFINED),
            (OpKind::Send | OpKind::Receive, WakeResult::Closed) => {
                return promise.reject(global, Ok(closed_error(global)));
            }
            (OpKind::Receive, WakeResult::Received(delivery)) => {
                delivery.take().deserialize(global)
            }
            (OpKind::Next, WakeResult::Received(delivery)) => delivery
                .take()
                .deserialize(global)
                .map(|value| iterator_result(global, value, false)),
            (OpKind::Next, WakeResult::Closed) => {
                Ok(iterator_result(global, JSValue::UNDEFINED, true))
            }
            (OpKind::Lock, WakeResult::Acquired(handoff)) => {
                // Its signal aborted while the lock was on its way, which it could not cancel.
                let aborted = self
                    .mutex
                    .as_ref()
                    .and_then(|(_, signal)| signal.as_ref())
                    .filter(|signal| signal.aborted());
                if let Some(signal) = aborted {
                    // The lock goes on to the next waiter.
                    drop(handoff);
                    return promise.reject(global, Ok(signal.js_reason(global)));
                }
                handoff.keep();
                Ok(JSValue::UNDEFINED)
            }
            (OpKind::Send, WakeResult::Received(_))
            | (OpKind::Receive | OpKind::Next, WakeResult::Sent) => {
                unreachable!("a sender is only sent to, a receiver only received from")
            }
            (OpKind::Lock, _) | (_, WakeResult::Acquired(_) | WakeResult::Retry(_)) => {
                unreachable!("only a lock is handed a mutex, and a retry is not settled here")
            }
            (OpKind::Select(_), _) => unreachable!("settled above"),
        };
        promise.settle(global, value)
    }
}

unsafe extern "C" {
    /// BunGo.cpp: the owner of the goroutine whose code runs now, or 0.
    safe fn Bun__Go__currentOwner(global: &JSGlobalObject) -> u32;
    /// BunGo.cpp: the goroutine whose code runs now was cancelled: its owner's thread ended.
    safe fn Bun__Go__isCancelled(global: &JSGlobalObject) -> bool;
    /// BunGo.cpp: the thread of `owner` ended while goroutines of its ran.
    safe fn Bun__Go__ownerEnded(owner: u32) -> bool;
    safe fn Bun__Go__moduleCallSite(
        global: &JSGlobalObject,
        url: &mut bun_core::String,
        line: &mut u32,
        column: &mut u32,
    ) -> bool;
}

fn module_call_site(global: &JSGlobalObject) -> Option<(bun_core::String, u32, u32)> {
    let (mut url, mut line, mut column) = (bun_core::String::default(), 0, 0);
    Bun__Go__moduleCallSite(global, &mut url, &mut line, &mut column).then_some((url, line, column))
}

/// This VM's waiting channel operations.
pub(super) fn ops() -> &'static mut OpTable<ChannelOp> {
    let state = crate::jsc_hooks::runtime_state();
    debug_assert!(!state.is_null());
    // SAFETY: the live per-thread `RuntimeState`; JS thread. Every use is one statement, and
    // nothing it calls reaches the table.
    unsafe { &mut (*state).channel_ops }
}

/// The error of an operation of a goroutine that was cancelled, if this one was: its owner's thread
/// ended.
pub(super) fn cancelled(global: &JSGlobalObject) -> Option<JSValue> {
    Bun__Go__isCancelled(global).then(|| cancelled_error(global))
}

fn cancelled_error(global: &JSGlobalObject) -> JSValue {
    global
        .err(
            ErrorCode::GOROUTINE_THREAD_EXITED,
            format_args!("The Worker that started this goroutine exited"),
        )
        .to_js()
}

/// BunGo.cpp: the thread of `owner`, a Worker that started goroutines, ended. Rejects the operations
/// of this thread that its goroutines wait in. A wake on its way to one finds nothing, and passes
/// on what it carries: a value, a lock.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Bun__Go__cancelOwner(global: &JSGlobalObject, owner: u32) {
    let ids: Vec<u64> = ops()
        .iter()
        .filter(|op| op.owner == owner)
        .map(|op| op.id)
        .collect();
    for id in ids {
        cancel(global, id);
    }
}

fn cancel(global: &JSGlobalObject, id: u64) {
    let Some(op) = ops().remove(id) else {
        return;
    };
    op.waiter.cancel();
    op.forget();
    let mut promise = op.promise;
    let _ = promise.reject(global, Ok(cancelled_error(global)));
}

/// This VM is ending, or its thread was asked to: script it runs from here on may be cut short, so
/// it takes no value from a channel.
pub(super) fn ending(global: &JSGlobalObject) -> bool {
    let vm = global.bun_vm();
    !vm.script_allowed()
        || vm
            .worker_ref()
            .is_some_and(|worker| worker.has_requested_terminate())
}

/// What a receive returns on a VM that is [ending](ending): it waits for good.
pub(super) fn never(global: &JSGlobalObject) -> JSValue {
    JSPromise::create(global).to_js()
}

pub(super) fn closed_error(global: &JSGlobalObject) -> JSValue {
    global
        .err(ErrorCode::CHANNEL_CLOSED, format_args!("Channel is closed"))
        .to_js()
}

unsafe extern "C" {
    safe fn Bun__Channel__iteratorResult(
        global: &JSGlobalObject,
        value: JSValue,
        done: bool,
    ) -> JSValue;
}

fn iterator_result(global: &JSGlobalObject, value: JSValue, done: bool) -> JSValue {
    Bun__Channel__iteratorResult(global, value, done)
}

/// What `Bun.select` resolves to. `index` is `-1` for `default`.
pub(super) fn select_result(
    global: &JSGlobalObject,
    index: i64,
    value: JSValue,
    closed: bool,
) -> JSValue {
    let result = JSValue::create_empty_object(global, 3);
    result.put(global, "index", JSValue::js_number(index as f64));
    result.put(global, "value", value);
    result.put(global, "closed", JSValue::js_boolean(closed));
    result
}

enum SendStep {
    Done,
    Closed,
    Wait(Arc<Waiter>),
}

impl ChannelCore {
    fn send(
        self: &Arc<Self>,
        waiter: impl FnOnce() -> Arc<Waiter>,
        message: ChannelMessage,
    ) -> SendStep {
        let mut state = self.state.lock();
        let message = match state.try_send(self.capacity, message) {
            Ok(sent) => {
                graph::note_edges(self, &state);
                drop(state);
                return match sent {
                    SendNow::Closed(_) => SendStep::Closed,
                    SendNow::Handed(receiver, message) => {
                        receiver.wake(WakeResult::Received(Delivery::new(message, self)));
                        SendStep::Done
                    }
                    SendNow::Buffered => SendStep::Done,
                };
            }
            Err(message) => message,
        };
        let waiter = waiter();
        state.push_sender(
            Queued {
                waiter: Arc::clone(&waiter),
                case: 0,
            },
            message,
        );
        graph::note_edges(self, &state);
        SendStep::Wait(waiter)
    }

    /// A value handed to a receiver was read. If it was the last one out of a closed channel, the
    /// receivers that waited for it to come back find the channel ended. Nothing locked.
    fn delivered(&self) {
        let receivers = {
            let mut state = self.state.lock();
            state.deliveries -= 1;
            if !state.has_ended() || !state.buffer.is_empty() {
                return;
            }
            core::mem::take(&mut state.receivers)
        };
        for queued in receivers {
            if queued.fire() {
                queued.wake(WakeResult::Closed);
            }
        }
    }

    /// A value handed to a receiver that went away: to the next receiver, else first in the buffer,
    /// whether or not there is room. Nothing locked.
    fn give_back(self: &Arc<Self>, message: ChannelMessage) {
        let mut state = self.state.lock();
        state.deliveries -= 1;
        while let Some(receiver) = state.receivers.pop_front() {
            if receiver.fire() {
                state.deliveries += 1;
                drop(state);
                receiver.wake(WakeResult::Received(Delivery::new(message, self)));
                return;
            }
        }
        state.enter(&message);
        state.buffer.push_front(message);
        graph::note_edges(self, &state);
    }

    /// `Ok(None)`: closed and drained.
    fn receive(
        &self,
        waiter: impl FnOnce() -> Arc<Waiter>,
    ) -> Result<Option<ChannelMessage>, Arc<Waiter>> {
        let mut state = self.state.lock();
        let Some(received) = state.try_receive(self.capacity) else {
            let waiter = waiter();
            state.receivers.push_back(Queued {
                waiter: Arc::clone(&waiter),
                case: 0,
            });
            return Err(waiter);
        };
        drop(state);
        Ok(match received {
            ReceiveNow::Value(message, sender) => {
                if let Some(sender) = sender {
                    sender.wake(WakeResult::Sent);
                }
                Some(message)
            }
            ReceiveNow::Closed => None,
        })
    }

    fn close(&self) {
        let (receivers, senders) = {
            let mut state = self.state.lock();
            if state.closed {
                return;
            }
            state.closed = true;
            let senders = core::mem::take(&mut state.senders);
            for (_, message) in &senders {
                state.leave(message);
            }
            // A value handed out may still come back to them.
            let receivers = match state.has_ended() {
                true => core::mem::take(&mut state.receivers),
                false => VecDeque::new(),
            };
            (receivers, senders)
        };
        let mut waiting: Vec<Queued> = receivers
            .into_iter()
            .chain(senders.into_iter().map(|(queued, _)| queued))
            .collect();
        // A select that waits both to receive and to send here has both cases ready now.
        shuffle(&mut waiting);
        for queued in waiting {
            if queued.fire() {
                queued.wake(WakeResult::Closed);
            }
        }
    }

    /// Takes the entries of a waiter that fired or was [cancelled](Waiter::cancel) out of the
    /// queues.
    fn forget(&self, waiter: &Arc<Waiter>) {
        let mut dropped = SmallVec::<[ChannelMessage; 1]>::new();
        {
            let mut state = self.state.lock();
            state
                .receivers
                .retain(|queued| !Arc::ptr_eq(&queued.waiter, waiter));
            while let Some(i) = state
                .senders
                .iter()
                .position(|(queued, _)| Arc::ptr_eq(&queued.waiter, waiter))
            {
                if let Some((_, message)) = state.senders.remove(i) {
                    state.leave(&message);
                    dropped.push(message);
                }
            }
        }
        // Its values, outside the lock: one can hold the last handle of another channel.
        drop(dropped);
    }

    /// How many entries the queues hold, live or not.
    fn queue_lengths(&self) -> (usize, usize) {
        let state = self.state.lock();
        (state.receivers.len(), state.senders.len())
    }
}

/// `Bun.Channel`: the JS object of one VM, which shares the channel.
pub(crate) struct Channel {
    core: ManuallyDrop<Arc<ChannelCore>>,
}

impl Drop for Channel {
    fn drop(&mut self) {
        // SAFETY: dropping; not used again.
        graph::release(unsafe { ManuallyDrop::take(&mut self.core) });
    }
}

impl Channel {
    pub(crate) fn constructor(cx: &mut JsScope<'_>, capacity: JsLocal<'_>) -> JsResult<Self> {
        let capacity = capacity.to_js();
        let capacity = if capacity.is_undefined() {
            0
        } else {
            validators::validate_integer(
                cx.global(),
                capacity,
                "capacity",
                Some(0),
                Some(u32::MAX.into()),
            )? as u32
        };
        graph::assist();
        Ok(Self {
            core: ManuallyDrop::new(ChannelCore::new(capacity)),
        })
    }

    pub(super) fn core(&self) -> &Arc<ChannelCore> {
        &self.core
    }

    fn channels(&self) -> Channels {
        SmallVec::from_elem(Arc::clone(&self.core), 1)
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
        let message =
            ChannelMessage::create(global, value.to_js(), options.transfer, CloneSite::Send)?;
        // A getter the clone ran can have aborted it.
        if let Some(rejected) = options.rejected_if_aborted(global) {
            return Ok(rejected);
        }
        let id = ops().next_id();
        Ok(match self.core.send(|| new_waiter(global, id), message) {
            SendStep::Done => JSPromise::resolved_promise_value(global, JSValue::UNDEFINED),
            SendStep::Closed => JSPromise::rejected_promise(global, closed_error(global)).to_js(),
            SendStep::Wait(waiter) => wait(
                global,
                OpKind::Send,
                waiter,
                self.channels(),
                options.signal,
                Vec::new(),
            ),
        })
    }

    pub(crate) fn receive(&self, cx: &mut JsScope<'_>, options: JsLocal<'_>) -> JsResult<JSValue> {
        let global = cx.global();
        let options = Options::parse(global, options.to_js(), false)?;
        self.receive_as(global, OpKind::Receive, options)
    }

    /// `signal`: `values({ signal })`'s, which validated it.
    pub(crate) fn next(&self, cx: &mut JsScope<'_>, signal: JsLocal<'_>) -> JsResult<JSValue> {
        let global = cx.global();
        let signal = signal.to_js();
        let options = Options {
            signal: if signal.is_undefined() {
                None
            } else {
                Some(AbortSignal::ref_from_js(signal).ok_or_else(|| {
                    validators::throw_err_invalid_arg_type(
                        global,
                        "options.signal",
                        "AbortSignal",
                        signal,
                    )
                })?)
            },
            ..Options::NONE
        };
        self.receive_as(global, OpKind::Next, options)
    }

    fn receive_as(
        &self,
        global: &JSGlobalObject,
        kind: OpKind,
        options: Options,
    ) -> JsResult<JSValue> {
        if let Some(rejected) = options.rejected_if_aborted(global) {
            return Ok(rejected);
        }
        if ending(global) {
            return Ok(never(global));
        }
        let id = ops().next_id();
        match self.core.receive(|| new_waiter(global, id)) {
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
            Err(waiter) => Ok(wait(
                global,
                kind,
                waiter,
                self.channels(),
                options.signal,
                Vec::new(),
            )),
        }
    }
}

/// Uniformly at random.
pub(super) fn shuffle<T>(items: &mut [T]) {
    for i in (1..items.len()).rev() {
        items.swap(i, (bun_core::fast_random() % (i as u64 + 1)) as usize);
    }
}

pub(super) fn new_waiter(global: &JSGlobalObject, op_id: u64) -> Arc<Waiter> {
    let vm = global.bun_vm();
    Arc::new(Waiter {
        vm: vm.handle(),
        loop_kind: vm.current_loop_kind(),
        op_id,
        state: AtomicU32::new(WAITING),
    })
}

/// The promise of an operation whose waiter is queued in `channels`. Nothing can settle it before
/// this returns: its wake runs on this thread, and so does its signal.
pub(super) fn wait(
    global: &JSGlobalObject,
    kind: OpKind,
    waiter: Arc<Waiter>,
    channels: Channels,
    signal: Option<AbortSignalRef>,
    signal_cases: Vec<(u32, AbortSignalRef)>,
) -> JSValue {
    wait_for(global, kind, waiter, channels, None, signal, signal_cases)
}

pub(super) fn wait_for(
    global: &JSGlobalObject,
    kind: OpKind,
    waiter: Arc<Waiter>,
    channels: Channels,
    mutex: Option<Arc<MutexCore>>,
    signal: Option<AbortSignalRef>,
    signal_cases: Vec<(u32, AbortSignalRef)>,
) -> JSValue {
    let id = waiter.op_id;
    let op = Box::new(ChannelOp {
        abort_handle: AbortHandle::for_owner::<ChannelOp>(),
        id,
        kind,
        promise: JSPromiseStrong::init(global),
        waiter,
        channels: OpChannels(channels),
        signal_cases: signal_cases
            .iter()
            .map(|&(case, _)| SignalCase {
                abort_handle: AbortHandle::for_owner::<SignalCase>(),
                op_id: id,
                case,
            })
            .collect(),
        mutex: mutex.map(|mutex| (mutex, signal.clone())),
        top_level: module_call_site(global),
        owner: Bun__Go__currentOwner(global),
    });
    let promise = op.promise.value();
    let op = ops().insert(id, op).as_ptr();
    let vm = global.bun_vm();
    // SAFETY: `op` is in the table, which frees it only through `remove`; the handles are
    // disarmed when they drop. A signal that has aborted settles the operation before
    // `follow_owner` returns, and then the rest are not followed.
    unsafe {
        AbortHandle::arm_owner(op, vm.context_of_caller());
        if let Some(signal) = signal {
            AbortHandle::follow_owner(op, signal);
        }
        for (i, (_, signal)) in signal_cases.into_iter().enumerate() {
            let Some(op) = ops().get(id) else {
                break;
            };
            let cases = &mut (*op.as_ptr()).signal_cases;
            let case: *mut SignalCase = &raw mut cases[i];
            AbortHandle::follow_owner(case, signal);
        }
    }
    promise
}

/// `bun:internal-for-testing`: how many entries the channel's queues hold, live or not.
pub(crate) fn queue_lengths_for_testing(
    cx: &mut JsScope<'_>,
    channel: JsLocal<'_>,
) -> JsResult<JSValue> {
    let global = cx.global();
    let Some(channel) = channel.to_js().as_class_ref::<Channel>() else {
        return Err(validators::throw_err_invalid_arg_type(
            global,
            "channel",
            "Bun.Channel",
            channel.to_js(),
        ));
    };
    let (receivers, senders) = channel.core.queue_lengths();
    let result = JSValue::create_empty_object(global, 2);
    result.put(global, "receivers", JSValue::js_number(receivers as f64));
    result.put(global, "senders", JSValue::js_number(senders as f64));
    Ok(result)
}

pub(super) struct Options {
    pub(super) signal: Option<AbortSignalRef>,
    transfer: JSValue,
}

impl Options {
    const NONE: Options = Options {
        signal: None,
        transfer: JSValue::UNDEFINED,
    };

    pub(super) fn parse(global: &JSGlobalObject, options: JSValue, send: bool) -> JsResult<Self> {
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

    /// Also if the goroutine that runs it was cancelled.
    pub(super) fn rejected_if_aborted(&self, global: &JSGlobalObject) -> Option<JSValue> {
        if let Some(error) = cancelled(global) {
            return Some(JSPromise::rejected_promise(global, error).to_js());
        }
        let signal = self.signal.as_ref()?;
        if !signal.aborted() {
            return None;
        }
        Some(JSPromise::rejected_promise(global, signal.js_reason(global)).to_js())
    }
}

/// The channels in the structured clone messages that exist, on any thread, by what the message
/// has in their place.
/// Sharded by id: every send of a value that shares a channel takes one.
static IN_FLIGHT: [Guarded<BTreeMap<u64, Arc<ChannelCore>>>; 16] =
    [const { Guarded::new(BTreeMap::new()) }; 16];

fn in_flight(id: u64) -> &'static Guarded<BTreeMap<u64, Arc<ChannelCore>>> {
    &IN_FLIGHT[(id % IN_FLIGHT.len() as u64) as usize]
}

/// The cores of the channels that messages share by these ids, while those messages exist.
pub(super) fn cores_of(ids: &[u64]) -> Box<[*const ChannelCore]> {
    ids.iter()
        .filter_map(|&id| in_flight(id).lock().get(&id).map(Arc::as_ptr))
        .collect()
}

/// `channel` is a `Bun.Channel`.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Channel__retainForStructuredClone(channel: JSValue) -> u64 {
    let channel_value = channel;
    let Some(channel) = channel.as_class_ref::<Channel>() else {
        return 0;
    };
    // Not one that bytes from elsewhere (IPC, `v8.deserialize()`) can guess.
    let mut id = [0; 8];
    bun_boringssl_sys::rand_bytes(&mut id);
    let id = u64::from_ne_bytes(id);
    in_flight(id).lock().insert(id, Arc::clone(&channel.core));
    message::remember_wrapper(channel_value, Arc::as_ptr(&channel.core));
    id
}

/// `JSValue::ZERO` if no message has this id. A message can be read many times (`BroadcastChannel`).
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Channel__fromStructuredClone(global: &JSGlobalObject, id: u64) -> JSValue {
    let Some(core) = in_flight(id).lock().get(&id).cloned() else {
        return JSValue::ZERO;
    };
    let address = Arc::as_ptr(&core);
    let mut core = Some(core);
    let wrapper = message::wrapper_of(global, address, || {
        Channel {
            core: ManuallyDrop::new(core.take().expect("made once")),
        }
        .into_js_wrapper(global)
        .to_js()
    });
    // The wrapper this realm already had.
    if let Some(core) = core {
        graph::release(core);
    }
    wrapper
}

/// The message with this id is gone.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Channel__releaseStructuredClone(id: u64) {
    // The channel, outside the lock: it can be the last handle, and its buffer can hold messages
    // with channels of their own.
    let core = in_flight(id).lock().remove(&id);
    if let Some(core) = core {
        graph::release(core);
    }
}

pub(crate) fn live_count_for_testing() -> f64 {
    f64::from(LIVE.load(Ordering::Relaxed))
}

pub(crate) fn sharing_count_for_testing() -> f64 {
    message::sharing_count_for_testing()
}

pub(crate) fn collect_for_testing() -> f64 {
    (graph::collect_all() + graph::collect_large(true)) as f64
}

pub(crate) fn set_collect_reach_for_testing(reach: f64) {
    graph::set_reach_for_testing(reach as usize);
}
