//! `Bun.select`: Go's `select`, over channels that any thread of the process shares.
//!
//! A select locks the distinct channels of its cases in address order, the order every select
//! locks in, then tries its cases in a random order, so that each ready one is as likely to win.
//! The first that can happen does. If none can, it queues one waiter in every case's channel under
//! the same locks, so no send or close between the try and the wait is missed. Whoever fires the
//! waiter ([`Waiter::fire`](super::channel::Waiter)) picks the case that wins; its VM then takes the
//! waiter out of the other channels. Two selects meet on a channel when one finds the other's
//! waiter while it tries its cases: one select is never queued as both.
//!
//! An `AbortSignal` case is ready once its signal aborts. A select that waits follows its signals;
//! the first to abort wins the waiter for its case the way its own VM cancels it.

use std::sync::Arc;

use bun_collections::smallvec::SmallVec;
use bun_jsc::bindgen::{JsLocal, JsScope};
use bun_jsc::{
    AbortSignal, AbortSignalRef, ErrorCode, JSGlobalObject, JSPromise, JSValue, JsResult,
};

use super::channel::{
    Channel, ChannelCore, Channels, OpKind, Options, Queued, ReceiveNow, SendNow, WakeResult,
    closed_error, new_waiter, ops, select_result, shuffle, wait,
};
use super::message::{ChannelMessage, CloneSite};
use crate::node::util::validators;

struct Case {
    channel: Arc<ChannelCore>,
    /// The value of a send. Taken when the send happens or waits.
    send: Option<ChannelMessage>,
    is_send: bool,
    /// The index of the channel in the select's distinct channels.
    lock: usize,
}

enum Read {
    Channel(Case),
    Signal(AbortSignalRef),
}

/// What the case that won did, once the channels are unlocked.
enum Won {
    Received(ReceiveNow),
    Sent(SendNow),
    Aborted,
}

pub(crate) fn select(
    cx: &mut JsScope<'_>,
    cases: JsLocal<'_>,
    options: JsLocal<'_>,
) -> JsResult<JSValue> {
    let global = cx.global();
    let (cases, options) = (cases.to_js(), options.to_js());
    validators::validate_array(global, cases, "cases", None)?;
    let parsed = Options::parse(global, options, false)?;
    let default = match options.is_undefined() {
        true => false,
        false => match options.get(global, "default")? {
            Some(default) if !default.is_undefined() => {
                validators::validate_boolean(global, default, "options.default")?
            }
            _ => false,
        },
    };
    let len = cases.get_length(global)?;
    if len == 0 && !default && parsed.signal.is_none() {
        return Err(validators::throw_err_invalid_arg_value(
            global,
            format_args!(
                "Bun.select() with no cases waits forever: pass options.signal or options.default"
            ),
        ));
    }
    if let Some(rejected) = parsed.rejected_if_aborted(global) {
        return Ok(rejected);
    }

    // Each value is cloned as its case is read, losing cases' too: the clone is what waits.
    // Not `len` at once: a sparse array's can be 2 ** 32 - 1, and its first hole throws.
    let mut read = Vec::new();
    for i in 0..len as u32 {
        read.push(read_case(global, cases.get_index(global, i)?, i)?);
    }
    // A getter can have aborted it.
    if let Some(rejected) = parsed.rejected_if_aborted(global) {
        return Ok(rejected);
    }

    let mut channels: Channels = read
        .iter()
        .filter_map(|case| match case {
            Read::Channel(case) => Some(Arc::clone(&case.channel)),
            Read::Signal(_) => None,
        })
        .collect();
    channels.sort_unstable_by_key(|channel| Arc::as_ptr(channel) as usize);
    channels.dedup_by(|a, b| Arc::ptr_eq(a, b));
    for case in &mut read {
        let Read::Channel(case) = case else {
            continue;
        };
        case.lock = channels
            .binary_search_by_key(&(Arc::as_ptr(&case.channel) as usize), |channel| {
                Arc::as_ptr(channel) as usize
            })
            .expect("every case's channel is one of the select's");
    }

    let mut order: SmallVec<[u32; 8]> = (0..len as u32).collect();
    shuffle(&mut order);

    let mut locks: SmallVec<[_; 2]> = channels
        .iter()
        .map(|channel| channel.state.lock())
        .collect();
    let mut won = None;
    for &i in &order {
        let case = match &mut read[i as usize] {
            Read::Channel(case) => case,
            Read::Signal(signal) => {
                if signal.aborted() {
                    won = Some((i, Won::Aborted));
                    break;
                }
                continue;
            }
        };
        let state = &mut *locks[case.lock];
        let done = match case.send.take() {
            None => state.try_receive().map(Won::Received),
            Some(message) => match state.try_send(case.channel.capacity, message) {
                Ok(sent) => Some(Won::Sent(sent)),
                Err(message) => {
                    case.send = Some(message);
                    None
                }
            },
        };
        if let Some(done) = done {
            won = Some((i, done));
            break;
        }
    }

    let Some((index, won)) = won else {
        if default {
            drop(locks);
            drop(read);
            return Ok(JSPromise::resolved_promise_value(
                global,
                select_result(global, -1, JSValue::UNDEFINED, false),
            ));
        }
        let id = ops().next_id();
        let waiter = new_waiter(global, id);
        // In the random order too: cases on one channel are ready together.
        for &i in &order {
            let Read::Channel(case) = &mut read[i as usize] else {
                continue;
            };
            let queued = Queued {
                waiter: Arc::clone(&waiter),
                case: i,
            };
            let state = &mut *locks[case.lock];
            match case.send.take() {
                Some(message) => state.senders.push_back((queued, message)),
                None => state.receivers.push_back(queued),
            }
        }
        drop(locks);
        let sends = read
            .iter()
            .map(|case| matches!(case, Read::Channel(case) if case.is_send))
            .collect();
        let signals = read
            .into_iter()
            .enumerate()
            .filter_map(|(i, case)| match case {
                Read::Signal(signal) => Some((i as u32, signal)),
                Read::Channel(_) => None,
            })
            .collect();
        return Ok(wait(
            global,
            OpKind::Select(sends),
            waiter,
            channels,
            parsed.signal,
            signals,
        ));
    };
    drop(locks);
    let reason = match (&won, &read[index as usize]) {
        (Won::Aborted, Read::Signal(signal)) => signal.js_reason(global),
        _ => JSValue::UNDEFINED,
    };
    // The values of the cases that lost, outside the locks.
    drop(read);

    let index = i64::from(index);
    let value = match won {
        Won::Aborted => select_result(global, index, reason, false),
        Won::Received(ReceiveNow::Value(message, sender)) => {
            if let Some(sender) = sender {
                sender.wake(WakeResult::Sent);
            }
            select_result(global, index, message.deserialize(global)?, false)
        }
        Won::Received(ReceiveNow::Closed) => select_result(global, index, JSValue::UNDEFINED, true),
        Won::Sent(SendNow::Handed(receiver, message)) => {
            receiver.wake(WakeResult::Received(message));
            select_result(global, index, JSValue::UNDEFINED, false)
        }
        Won::Sent(SendNow::Buffered) => select_result(global, index, JSValue::UNDEFINED, false),
        Won::Sent(SendNow::Closed(message)) => {
            drop(message);
            return Ok(JSPromise::rejected_promise(global, closed_error(global)).to_js());
        }
    };
    Ok(JSPromise::resolved_promise_value(global, value))
}

/// A receive is a channel; a send is `[channel, value]`, whose value this clones; an
/// `AbortSignal` is ready once it aborts.
fn read_case(global: &JSGlobalObject, case: JSValue, i: u32) -> JsResult<Read> {
    if let Some(channel) = case.as_class_ref::<Channel>() {
        return Ok(Read::Channel(Case {
            channel: Arc::clone(channel.core()),
            send: None,
            is_send: false,
            lock: 0,
        }));
    }
    if let Some(signal) = AbortSignal::ref_from_js(case) {
        return Ok(Read::Signal(signal));
    }
    if case.js_type().is_array() && case.get_length(global)? == 2 {
        if let Some(channel) = case.get_index(global, 0)?.as_class_ref::<Channel>() {
            let channel = Arc::clone(channel.core());
            let value = case.get_index(global, 1)?;
            let message = ChannelMessage::create(
                global,
                value,
                JSValue::UNDEFINED,
                CloneSite::SelectSend(i),
            )?;
            return Ok(Read::Channel(Case {
                channel,
                send: Some(message),
                is_send: true,
                lock: 0,
            }));
        }
    }
    Err(global
        .err(
            ErrorCode::INVALID_ARG_TYPE,
            format_args!(
                "The \"cases[{i}]\" argument must be a Bun.Channel to receive from, a [Bun.Channel, value] array to send on, or an AbortSignal"
            ),
        )
        .throw())
}
