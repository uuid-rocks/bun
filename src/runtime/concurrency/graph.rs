//! Reclaiming channels that only the queues of unreachable channels hold.
//!
//! A value in a channel's queue that shares a channel holds that channel (the `Arc` its structured
//! clone keeps in the channel module's `IN_FLIGHT`). A channel sent into itself, or two sent into
//! each other, then hold each other, and their counts never reach zero. References from queues
//! are the edges of a graph of channels. Every other reference is a root: a JS wrapper, a waiting
//! operation, a value on its way to a VM, a goroutine's arguments.
//!
//! Channels whose queues hold a value that shares a channel are in [`REGISTRY`]. A collection
//! locks all of them, in address order as a select does, so no value enters or leaves their
//! queues. It counts the edges into each one from those queues. A channel with more references
//! than edges is a root. A channel that is not a root, and that no root reaches through the edges,
//! is held only through the queues of channels like it. No thread can reach it, or take another
//! reference to it: that would mean taking a value out of a locked queue. The collection takes the
//! values out of those queues and drops them once everything is unlocked, which drops the channels.
//!
//! Values that share no channel skip all of this. A channel joins the registry only when its queues
//! hold a value that shares one. A collection runs on a thread of its own when a reference to a
//! registered channel goes away, and when the registry has doubled since the last one, at most
//! once per [`INTERVAL`]: traffic that keeps sending a channel through itself must not keep its
//! lock busy.
//!
//! Lock order: channels in address order, then the registry or `IN_FLIGHT`. A collection lets the
//! registry go before it locks the channels it found there.

use core::sync::atomic::{AtomicBool, Ordering, fence};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bun_threading::{Condition, Guarded};

use super::channel::{ChannelCore, State, with_in_flight};

struct Registry {
    channels: Vec<Weak<ChannelCore>>,
    /// Collect once `channels` is this long.
    collect_at: usize,
}

const MIN_COLLECT_AT: usize = 256;

static REGISTRY: Guarded<Registry> = Guarded::new(Registry {
    channels: Vec::new(),
    collect_at: MIN_COLLECT_AT,
});

/// One collection at a time.
static COLLECTING: Guarded<()> = Guarded::new(());

const INTERVAL: Duration = Duration::from_millis(10);

/// A collection was asked for since the last one started.
static WANTED: AtomicBool = AtomicBool::new(false);
/// Whether the collector's thread was started. Its wait is on this lock.
static STARTED: Guarded<bool> = Guarded::new(false);
static WAKE: Condition = Condition::new();

/// `state` is `channel`'s, locked. Registers the channel if its queues hold a value that shares a
/// channel.
pub(super) fn register(channel: &Arc<ChannelCore>, state: &State) {
    if state.edges == 0 || channel.registered.load(Ordering::Relaxed) {
        return;
    }
    channel.registered.store(true, Ordering::Relaxed);
    let grown = {
        let mut registry = REGISTRY.lock();
        registry.channels.push(Arc::downgrade(channel));
        registry.channels.len() >= registry.collect_at
    };
    if grown {
        request();
    }
}

/// Drops a reference to `channel` that is not an edge. Nothing locked.
pub(super) fn release(channel: Arc<ChannelCore>) {
    let registered = channel.registered.load(Ordering::Relaxed);
    drop(channel);
    if registered {
        request();
    }
}

/// A collection on the collector's thread, soon.
fn request() {
    if WANTED.swap(true, Ordering::AcqRel) {
        return;
    }
    let mut started = STARTED.lock();
    if *started {
        WAKE.signal();
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("Bun.Channel collector".into())
        .spawn(run);
    if spawned.is_ok() {
        *started = true;
    } else {
        WANTED.store(false, Ordering::Release);
    }
}

fn run() {
    let mut last: Option<Instant> = None;
    loop {
        {
            let mut started = STARTED.lock();
            while !WANTED.load(Ordering::Acquire) {
                WAKE.wait_guarded(&mut started);
            }
        }
        if let Some(wait) = last.and_then(|last| INTERVAL.checked_sub(last.elapsed())) {
            std::thread::sleep(wait);
        }
        // What is asked for from here on, this collection may miss: it runs another.
        WANTED.store(false, Ordering::Release);
        collect();
        last = Some(Instant::now());
    }
}

/// Reclaims the registered channels that no root reaches. Returns how many. No channel locked.
pub(super) fn collect() -> usize {
    let _one = COLLECTING.lock();
    let mut channels: Vec<Arc<ChannelCore>> = {
        let mut registry = REGISTRY.lock();
        registry
            .channels
            .retain(|channel| channel.strong_count() > 0);
        registry.channels.iter().filter_map(Weak::upgrade).collect()
    };
    channels.sort_unstable_by_key(|channel| Arc::as_ptr(channel) as usize);
    channels.dedup_by(|a, b| Arc::ptr_eq(a, b));
    let mut states: Vec<_> = channels
        .iter()
        .map(|channel| channel.state.lock())
        .collect();
    // Each count is read after every lock is held. A reference that is not an edge can only be
    // taken from another that is not, so once a count reads as all edges it stays that way.
    fence(Ordering::SeqCst);

    let index_of = |channel: *const ChannelCore| {
        channels
            .binary_search_by_key(&(channel as usize), |c| Arc::as_ptr(c) as usize)
            .ok()
    };
    let len = channels.len();
    // Of each channel: the edges into it, and the channels its queues hold.
    let mut held = vec![0usize; len];
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); len];
    with_in_flight(|in_flight| {
        for (i, state) in states.iter().enumerate() {
            for message in state.values() {
                for id in message.channel_ids() {
                    let Some(target) = in_flight.get(&id) else {
                        continue;
                    };
                    if let Some(j) = index_of(Arc::as_ptr(target)) {
                        held[j] += 1;
                        edges[i].push(j);
                    }
                }
            }
        }
    });

    let mut reachable = vec![false; len];
    let mut stack = Vec::new();
    for (i, channel) in channels.iter().enumerate() {
        // `channels` holds one reference itself.
        if Arc::strong_count(channel) - 1 > held[i] {
            reachable[i] = true;
            stack.push(i);
        }
    }
    while let Some(i) = stack.pop() {
        for &j in &edges[i] {
            if !reachable[j] {
                reachable[j] = true;
                stack.push(j);
            }
        }
    }

    let mut unreachable = Vec::new();
    let mut unregistered = Vec::new();
    for (i, state) in states.iter_mut().enumerate() {
        if !reachable[i] {
            unreachable.push(state.take_all());
        }
        if state.edges == 0 {
            channels[i].registered.store(false, Ordering::Relaxed);
            unregistered.push(Arc::as_ptr(&channels[i]));
        }
    }
    {
        let mut registry = REGISTRY.lock();
        if !unregistered.is_empty() {
            registry
                .channels
                .retain(|channel| unregistered.binary_search(&channel.as_ptr()).is_err());
        }
        registry.collect_at = (registry.channels.len() * 2).max(MIN_COLLECT_AT);
    }
    let reclaimed = unreachable.len();
    drop(states);
    // The values, which drop the channels they share, outside the locks.
    drop(unreachable);
    drop(channels);
    reclaimed
}
