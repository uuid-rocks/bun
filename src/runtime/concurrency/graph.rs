//! Reclaiming channels that only the queues of unreachable channels hold.
//!
//! A value in a channel's queue that shares a channel holds that channel (the `Arc` its structured
//! clone keeps in the channel module's `IN_FLIGHT`). A channel sent into itself, or two sent into
//! each other, then hold each other, and their counts never reach zero. References from queues
//! are the edges of a graph of channels (the message keeps their cores beside it, `targets`).
//! Every other reference is a root: a JS wrapper, a waiting operation, a value on its way to a VM,
//! a goroutine's arguments.
//!
//! A channel can only become garbage when a root of it goes away ([`release`]). If its queues hold
//! a value that shares a channel, it becomes a candidate. A collection takes candidates and the
//! channels their queues reach, through values that share channels, locking one at a time. It
//! then locks all of them, in address order as a select does, so no value enters or leaves their
//! queues, and counts the edges into each one from those queues. A channel with more references
//! than edges is a root. A channel that is not a root, and that no root reaches through the edges,
//! is held only through the queues of channels like it. No thread can reach it, or take another
//! reference to it: that would mean taking a value out of a locked queue. The collection takes the
//! values out of those queues and drops them once everything is unlocked, which drops the channels.
//! Channels the candidates do not reach are not locked.
//!
//! Collections run on a thread of their own, without pause while they find garbage and at most
//! once per [`IDLE_INTERVAL`] while they do not: traffic that keeps sending a channel through
//! itself must not keep its lock busy. A thread that makes a channel while many candidates wait
//! collects a batch first when no collection runs, and waits for one when far too many wait
//! ([`assist`]): threads that make cycles faster than they are collected slow down to that pace.
//! One collection runs at a time: the references a collection holds are roots to another, which
//! would find nothing to free and leave no candidate behind.
//!
//! Values that share no channel skip all of this: a release of a channel that holds none is a load
//! of a flag.
//!
//! Lock order: channels in address order, then the candidates or `IN_FLIGHT`.

use core::cell::{Cell, RefCell};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bun_collections::smallvec::SmallVec;
use bun_threading::{Condition, Guarded};

use super::channel::{ChannelCore, State};

static CANDIDATES: Guarded<Vec<Weak<ChannelCore>>> = Guarded::new(Vec::new());
/// How many candidates wait, read without the lock.
static BACKLOG: AtomicUsize = AtomicUsize::new(0);

/// Candidates one collection takes at most.
const BATCH: usize = 1024;
/// Channels one collection locks at most. Leaving out what lies beyond keeps it correct (their
/// edges count as roots), and bounds how long it holds the locks; a cycle of more channels than
/// this is not reclaimed.
const REACH: usize = 256 * BATCH;
/// Candidates waiting past which a thread that makes a channel collects a batch first, if no
/// collection runs, and past which it waits for one to.
const ASSIST_AT: usize = 4 * BATCH;
const WAIT_AT: usize = 16 * BATCH;

/// One collection at a time.
static COLLECTING: Guarded<()> = Guarded::new(());
/// A collection runs, read without the lock.
static RUNNING: AtomicBool = AtomicBool::new(false);

struct Collecting(#[allow(dead_code)] bun_threading::guarded::GuardedLock<'static, (), bun_threading::Mutex>);

impl Collecting {
    fn lock() -> Self {
        let lock = COLLECTING.lock();
        RUNNING.store(true, Ordering::Relaxed);
        Collecting(lock)
    }
}

impl Drop for Collecting {
    fn drop(&mut self) {
        RUNNING.store(false, Ordering::Relaxed);
    }
}
const IDLE_INTERVAL: Duration = Duration::from_millis(10);
/// How long the collector's thread lets candidates wait: garbage that counts free (a chain whose
/// head went away) is gone by then, and costs the collection nothing.
const AGE: Duration = Duration::from_millis(50);

/// A collection was asked for since the last one started.
static WANTED: AtomicBool = AtomicBool::new(false);
/// Whether the collector's thread was started. Its wait is on this lock.
static STARTED: Guarded<bool> = Guarded::new(false);
static WAKE: Condition = Condition::new();

/// `state` is `channel`'s, locked: a value was queued in it.
pub(super) fn note_edges(channel: &ChannelCore, state: &State) {
    if state.edges > 0 && !channel.holds_channels.load(Ordering::Relaxed) {
        channel.holds_channels.store(true, Ordering::Relaxed);
    }
}

/// Drops a reference to `channel` that is not an edge. Nothing locked.
pub(super) fn release(channel: Arc<ChannelCore>) {
    // A channel whose queues hold no channel is in no cycle, and drops no channel. (If this is its
    // last reference, nothing can be queueing one meanwhile.)
    if !channel.holds_channels.load(Ordering::Relaxed) {
        return drop(channel);
    }
    if !channel.candidate.swap(true, Ordering::AcqRel) {
        CANDIDATES.lock().push(Arc::downgrade(&channel));
        BACKLOG.fetch_add(1, Ordering::Relaxed);
        request();
    }
    drop_channel(channel);
}

thread_local! {
    /// This thread is dropping a channel: channels its values drop wait in `DEFERRED`.
    static DROPPING: Cell<bool> = const { Cell::new(false) };
    static DEFERRED: RefCell<Vec<Arc<ChannelCore>>> = const { RefCell::new(Vec::new()) };
}

/// Drops `channel` without recursing into the channels its values hold, however long the chain:
/// inside another drop on this thread, it is dropped after that one.
pub(super) fn drop_channel(channel: Arc<ChannelCore>) {
    let mut channel = Some(channel);
    let deferred = DEFERRED.try_with(|deferred| {
        if DROPPING.get() {
            deferred.borrow_mut().push(channel.take().expect("taken once"));
        }
    });
    // Thread-locals are gone (the thread is ending), or this is the outermost drop.
    if let Some(channel) = channel {
        dropping(move || drop(channel));
    }
    let _ = deferred;
}

/// Runs `drop`, then the drops it deferred, one at a time.
fn dropping(drop: impl FnOnce()) {
    match DROPPING.try_with(|dropping| dropping.replace(true)) {
        Err(_) | Ok(true) => drop(),
        Ok(false) => {
            drop();
            while let Some(next) = DEFERRED.with(|deferred| deferred.borrow_mut().pop()) {
                core::mem::drop(next);
            }
            DROPPING.with(|dropping| dropping.set(false));
        }
    }
}

/// A channel is about to be made on a JS thread. Collects a batch first if many candidates wait.
pub(super) fn assist() {
    let backlog = BACKLOG.load(Ordering::Relaxed);
    if backlog <= ASSIST_AT {
        return;
    }
    if backlog > WAIT_AT || !RUNNING.load(Ordering::Relaxed) {
        let _collecting = Collecting::lock();
        collect_newest();
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
    // SAFETY: VM-free: the thread holds no VM or JS state, only channel cores and the messages in
    // their queues, which any thread may lock and drop.
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
    let mut idle_since: Option<Instant> = None;
    loop {
        {
            let mut started = STARTED.lock();
            while !WANTED.load(Ordering::Acquire) {
                WAKE.wait_guarded(&mut started);
            }
        }
        let wait = idle_since
            .and_then(|since| IDLE_INTERVAL.checked_sub(since.elapsed()))
            .unwrap_or_default()
            .max(AGE);
        std::thread::sleep(wait);
        // What is asked for from here on, this collection may miss: it runs another.
        WANTED.store(false, Ordering::Release);
        idle_since = (collect_all() == 0).then(Instant::now);
    }
}

/// Collects the candidates that wait now, in batches. Returns how many channels it reclaimed.
pub(super) fn collect_all() -> usize {
    let _collecting = Collecting::lock();
    let candidates = take_candidates(usize::MAX);
    // Shared by the batches, so none walks what another did. What a batch found reachable that
    // stops being so is released after it, a candidate of the next pass.
    let mut seen = Seen::default();
    let mut reclaimed = 0;
    let mut candidates = candidates.into_iter();
    loop {
        let batch: Vec<_> = candidates.by_ref().take(BATCH).collect();
        if batch.is_empty() {
            return reclaimed;
        }
        reclaimed += collect_batch(batch, &mut seen);
    }
}

type Seen = bun_collections::HashMap<usize, ()>;

/// The newest `count` candidates, which stop being candidates: a release from here on, while the
/// pass that takes them reads their queues or after, makes them candidates of the next pass.
fn take_candidates(count: usize) -> Vec<Weak<ChannelCore>> {
    let taken = {
        let mut candidates = CANDIDATES.lock();
        let at = candidates.len().saturating_sub(count);
        candidates.split_off(at)
    };
    BACKLOG.fetch_sub(taken.len(), Ordering::Relaxed);
    for candidate in &taken {
        if let Some(channel) = candidate.upgrade() {
            channel.candidate.store(false, Ordering::SeqCst);
            drop_channel(channel);
        }
    }
    taken
}

/// Collects a batch of the newest candidates.
fn collect_newest() {
    let batch = take_candidates(BATCH);
    if !batch.is_empty() {
        collect_batch(batch, &mut Seen::default());
    }
}

/// Collects `batch` and what it reaches, but what `seen` has. Returns how many channels it
/// reclaimed. No channel locked.
fn collect_batch(batch: Vec<Weak<ChannelCore>>, seen: &mut Seen) -> usize {
    // What the candidates reach through values that share channels, one channel locked at a time.
    // Channels that hold none are left out: they are in no cycle, and drop with what holds them.
    // Weakly: one that the counts free meanwhile is not kept, and drops out.
    let mut reached: Vec<Weak<ChannelCore>> = Vec::with_capacity(batch.len());
    for candidate in batch {
        if seen.insert(candidate.as_ptr() as usize, ()).is_none() {
            reached.push(candidate);
        }
    }
    let mut i = 0;
    while i < reached.len() && reached.len() < REACH {
        let Some(channel) = reached[i].upgrade() else {
            i += 1;
            continue;
        };
        {
            let state = channel.state.lock();
            for message in state.values() {
                for &target in message.targets() {
                    // SAFETY: the message holds the core, and the lock holds the message.
                    let core = unsafe { &*target };
                    if core.holds_channels.load(Ordering::Relaxed)
                        && seen.insert(target as usize, ()).is_none()
                    {
                        // SAFETY: as above: a live core of an `Arc`, which this does not drop.
                        let target = core::mem::ManuallyDrop::new(unsafe { Arc::from_raw(target) });
                        reached.push(Arc::downgrade(&target));
                    }
                }
            }
        }
        drop_channel(channel);
        i += 1;
    }
    let channels = reached.iter().filter_map(Weak::upgrade).collect();
    drop(reached);
    collect(channels)
}

/// Reclaims the channels of `channels` that no root reaches. Returns how many. No channel locked.
fn collect(mut channels: Vec<Arc<ChannelCore>>) -> usize {
    channels.sort_unstable_by_key(|channel| Arc::as_ptr(channel) as usize);
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
    let mut edges: Vec<SmallVec<[usize; 2]>> = vec![SmallVec::new(); len];
    for (i, state) in states.iter().enumerate() {
        for message in state.values() {
            for &target in message.targets() {
                if let Some(j) = index_of(target) {
                    held[j] += 1;
                    edges[i].push(j);
                }
            }
        }
    }

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
    for (i, state) in states.iter_mut().enumerate() {
        if !reachable[i] {
            unreachable.push(state.take_all());
        }
        if state.edges == 0 {
            channels[i].holds_channels.store(false, Ordering::Relaxed);
        }
    }
    let reclaimed = unreachable.len();
    drop(states);
    // The values, which drop the channels they share, outside the locks.
    dropping(move || {
        drop(unreachable);
        drop(channels);
    });
    reclaimed
}
