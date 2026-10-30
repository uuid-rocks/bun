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

use core::sync::atomic::{AtomicU32, Ordering};
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

use super::message::ChannelMessage;
use super::ops::OpTable;
use crate::node::util::validators;

pub(crate) struct ChannelCore {
    pub(super) capacity: u32,
    pub(super) state: Guarded<State>,
}

/// Of the entries whose waiter can still fire, and but for a select that waits both to send and
/// to receive on one unbuffered channel: no receiver waits while anything is buffered or a sender
/// waits.
#[derive(Default)]
pub(super) struct State {
    /// At most `capacity` values.
    buffer: VecDeque<ChannelMessage>,
    /// Waiting receivers, oldest first.
    pub(super) receivers: VecDeque<Queued>,
    /// Waiting senders and their values, oldest first.
    pub(super) senders: VecDeque<(Queued, ChannelMessage)>,
    closed: bool,
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
                return Ok(SendNow::Handed(receiver, message));
            }
        }
        if self.buffer.len() < capacity as usize {
            self.buffer.push_back(message);
            return Ok(SendNow::Buffered);
        }
        Err(message)
    }

    /// `None`: the receive would wait.
    pub(super) fn try_receive(&mut self) -> Option<ReceiveNow> {
        if let Some(message) = self.buffer.pop_front() {
            let sender = self.pop_sender().map(|(sender, refill)| {
                self.buffer.push_back(refill);
                sender
            });
            return Some(ReceiveNow::Value(message, sender));
        }
        if let Some((sender, message)) = self.pop_sender() {
            return Some(ReceiveNow::Value(message, Some(sender)));
        }
        self.closed.then_some(ReceiveNow::Closed)
    }

    /// The oldest sender still waiting, fired.
    fn pop_sender(&mut self) -> Option<(Queued, ChannelMessage)> {
        while let Some((sender, message)) = self.senders.pop_front() {
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
    fn wake(&self, case: u32, result: WakeResult) {
        let wake = bun_core::heap::into_raw(Box::new(ChannelWake {
            op_id: self.op_id,
            case,
            result,
        }));
        if self.vm.is_current() {
            let vm = VirtualMachine::get();
            if vm.current_loop_kind() == self.loop_kind {
                // Its own thread, which runs that loop and checks the queue before it polls.
                vm.event_loop_mut().enqueue_task(Task::init(wake));
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
    Received(ChannelMessage),
    Sent,
    Closed,
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
    channels: Channels,
}

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
    /// Takes the waiter out of the channels that still queue it.
    fn forget(&self) {
        for channel in &self.channels {
            channel.forget(&self.waiter);
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
                WakeResult::Received(message) => message
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
            };
            return promise.settle(global, value);
        }
        let promise = &mut self.promise;
        let value = match (&self.kind, result) {
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
            (OpKind::Select(_), _) => unreachable!("settled above"),
        };
        promise.settle(global, value)
    }
}

/// This VM's waiting channel operations.
pub(super) fn ops() -> &'static mut OpTable<ChannelOp> {
    let state = crate::jsc_hooks::runtime_state();
    debug_assert!(!state.is_null());
    // SAFETY: the live per-thread `RuntimeState`; JS thread. Every use is one statement, and
    // nothing it calls reaches the table.
    unsafe { &mut (*state).channel_ops }
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
    fn send(&self, waiter: impl FnOnce() -> Arc<Waiter>, message: ChannelMessage) -> SendStep {
        let mut state = self.state.lock();
        let message = match state.try_send(self.capacity, message) {
            Ok(sent) => {
                drop(state);
                return match sent {
                    SendNow::Closed(_) => SendStep::Closed,
                    SendNow::Handed(receiver, message) => {
                        receiver.wake(WakeResult::Received(message));
                        SendStep::Done
                    }
                    SendNow::Buffered => SendStep::Done,
                };
            }
            Err(message) => message,
        };
        let waiter = waiter();
        state.senders.push_back((
            Queued {
                waiter: Arc::clone(&waiter),
                case: 0,
            },
            message,
        ));
        SendStep::Wait(waiter)
    }

    /// `Ok(None)`: closed and drained.
    fn receive(
        &self,
        waiter: impl FnOnce() -> Arc<Waiter>,
    ) -> Result<Option<ChannelMessage>, Arc<Waiter>> {
        let mut state = self.state.lock();
        let Some(received) = state.try_receive() else {
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
            (
                core::mem::take(&mut state.receivers),
                core::mem::take(&mut state.senders),
            )
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
                dropped.extend(state.senders.remove(i).map(|(_, message)| message));
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
    core: Arc<ChannelCore>,
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
        Ok(Self {
            core: Arc::new(ChannelCore {
                capacity,
                state: Guarded::new(State::default()),
            }),
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
        let message = ChannelMessage::create(global, value.to_js(), options.transfer)?;
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
            ),
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

    fn receive_as(
        &self,
        global: &JSGlobalObject,
        kind: OpKind,
        options: Options,
    ) -> JsResult<JSValue> {
        if let Some(rejected) = options.rejected_if_aborted(global) {
            return Ok(rejected);
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
            Err(waiter) => Ok(wait(global, kind, waiter, self.channels(), options.signal)),
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
) -> JSValue {
    let id = waiter.op_id;
    let op = Box::new(ChannelOp {
        abort_handle: AbortHandle::for_owner::<ChannelOp>(),
        id,
        kind,
        promise: JSPromiseStrong::init(global),
        waiter,
        channels,
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

    pub(super) fn rejected_if_aborted(&self, global: &JSGlobalObject) -> Option<JSValue> {
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
    IN_FLIGHT.lock().insert(id, Arc::clone(&channel.core));
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
