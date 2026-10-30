//! `Bun.Mutex`: a lock that every thread of the process shares, waited for like a channel.
//!
//! Like a channel, the lock is native and shared: a structured clone of a `Bun.Mutex` holds an
//! `Arc` of the same [`MutexCore`]. A `lock()` that has to wait is a [`Waiter`] in the mutex's queue
//! and a channel operation in its VM's table, and is settled, aborted and torn down the same way.
//!
//! Go's two modes. Normally unlocking frees the lock and wakes the oldest waiter to try for it
//! again ([`Wakeup`]), and a `lock()` that finds it free takes it: a thread that unlocks and locks
//! again keeps the lock without waiting for another thread to wake up. One waiter is woken at a
//! time. A woken waiter that finds the lock taken goes back to the head of the queue. Once one has
//! waited over [`STARVING_AFTER`], the mutex starves: unlocking hands the lock straight to the
//! oldest waiter ([`Handoff`]), which never looks free while anyone waits, until a waiter takes it
//! that waited less than that, or the last one does.
//!
//! A wakeup or handoff whose waiter is gone by the time it arrives (dropped unsettled) is passed on
//! to the next waiter or unlocks again, and a thread that ends holding a lock ([`Held`]) unlocks
//! it, the way a process that exits releases its file locks.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bun_collections::smallvec::SmallVec;
use bun_jsc::bindgen::{JsLocal, JsPayload as _, JsScope};
use bun_jsc::{JSGlobalObject, JSPromise, JSValue, JsResult};
use bun_threading::Guarded;

use super::channel::{OpKind, Options, Waiter, WakeResult, new_waiter, ops, wait_for};

/// Go's `starvationThresholdNs`.
const STARVING_AFTER: Duration = Duration::from_millis(1);

pub(crate) struct MutexCore {
    state: Guarded<State>,
}

#[derive(Default)]
struct State {
    locked: bool,
    /// Oldest first, with when each started to wait. Entries of waiters that were cancelled stay
    /// until their VM forgets them.
    waiters: VecDeque<(Arc<Waiter>, Instant)>,
    /// A [`Wakeup`] is on its way.
    woken: bool,
    /// Unlocking hands the lock to the oldest waiter.
    starving: bool,
}

impl State {
    /// The oldest waiter that can still be fired, fired.
    fn pop_waiter(&mut self) -> Option<(Arc<Waiter>, Instant)> {
        while let Some((waiter, since)) = self.waiters.pop_front() {
            if waiter.fire() {
                return Some((waiter, since));
            }
        }
        None
    }
}

/// A waiter woken to try for the lock again, on its way to the waiter's VM. Wakes the next one if
/// it is dropped before the waiter's VM tries.
pub(super) struct Wakeup {
    mutex: Option<Arc<MutexCore>>,
    since: Instant,
}

impl Drop for Wakeup {
    fn drop(&mut self) {
        if let Some(mutex) = self.mutex.take() {
            let next = {
                let mut state = mutex.state.lock();
                state.woken = false;
                mutex.next_to_wake(&mut state)
            };
            if let Some(next) = next {
                mutex.wake(next);
            }
        }
    }
}

/// The lock, on its way to the waiter it was handed to. Unlocks if it is dropped before the
/// waiter's VM takes it.
pub(super) struct Handoff(Option<Arc<MutexCore>>);

impl Handoff {
    /// The waiter's VM holds the lock now.
    pub(super) fn keep(mut self) {
        Self::kept(self.0.take().expect("a handoff is kept once"));
    }

    /// This VM holds `mutex` now.
    pub(super) fn kept(mutex: Arc<MutexCore>) {
        held().0.push(mutex);
    }
}

/// The mutexes a VM holds, in its `RuntimeState`. Unlocked when the VM ends.
#[derive(Default)]
pub(crate) struct Held(Vec<Arc<MutexCore>>);

impl Drop for Held {
    fn drop(&mut self) {
        for mutex in self.0.drain(..) {
            mutex.unlock();
        }
    }
}

fn held() -> &'static mut Held {
    let state = crate::jsc_hooks::runtime_state();
    debug_assert!(!state.is_null());
    // SAFETY: the live per-thread `RuntimeState`; JS thread. Nothing that a use of it calls reaches
    // it again.
    unsafe { &mut (*state).mutexes_held }
}

impl Drop for Handoff {
    fn drop(&mut self) {
        if let Some(mutex) = self.0.take() {
            mutex.unlock();
        }
    }
}

impl MutexCore {
    fn try_lock(&self) -> bool {
        let mut state = self.state.lock();
        !core::mem::replace(&mut state.locked, true)
    }

    /// `None`: locked at once. Otherwise the waiter `waiter()` made, queued.
    fn lock(&self, waiter: impl FnOnce() -> Arc<Waiter>) -> Option<Arc<Waiter>> {
        let mut state = self.state.lock();
        if !state.locked {
            state.locked = true;
            return None;
        }
        let waiter = waiter();
        state.waiters.push_back((Arc::clone(&waiter), Instant::now()));
        Some(waiter)
    }

    fn unlock(self: &Arc<Self>) {
        let mut state = self.state.lock();
        debug_assert!(state.locked, "only a holder unlocks");
        if state.starving {
            if let Some((waiter, since)) = state.pop_waiter() {
                if since.elapsed() < STARVING_AFTER || state.waiters.is_empty() {
                    state.starving = false;
                }
                drop(state);
                waiter.wake(0, WakeResult::Acquired(Handoff(Some(Arc::clone(self)))));
                return;
            }
            state.starving = false;
        }
        state.locked = false;
        let next = self.next_to_wake(&mut state);
        drop(state);
        if let Some(next) = next {
            self.wake(next);
        }
    }

    /// The waiter to wake now that the lock may be free, if none is on its way.
    fn next_to_wake(&self, state: &mut State) -> Option<(Arc<Waiter>, Instant)> {
        if state.woken || state.locked {
            return None;
        }
        let next = state.pop_waiter()?;
        state.woken = true;
        Some(next)
    }

    /// Nothing locked.
    fn wake(self: &Arc<Self>, (waiter, since): (Arc<Waiter>, Instant)) {
        let wakeup = Wakeup {
            mutex: Some(Arc::clone(self)),
            since,
        };
        waiter.wake(0, WakeResult::Retry(wakeup));
    }

    /// The waiter `wakeup` woke tries for the lock: true if it took it, else it is queued again,
    /// first. JS thread of the waiter's VM.
    pub(super) fn retry(&self, mut wakeup: Wakeup, waiter: &Arc<Waiter>) -> bool {
        // Not passed on: this waiter is the one that tries.
        wakeup.mutex = None;
        let mut state = self.state.lock();
        state.woken = false;
        if !state.locked {
            state.locked = true;
            return true;
        }
        if wakeup.since.elapsed() >= STARVING_AFTER {
            state.starving = true;
        }
        waiter.rearm();
        state.waiters.push_front((Arc::clone(waiter), wakeup.since));
        false
    }

    /// Takes the entry of a waiter that fired or was cancelled out of the queue.
    pub(super) fn forget(&self, waiter: &Arc<Waiter>) {
        self.state
            .lock()
            .waiters
            .retain(|(queued, _)| !Arc::ptr_eq(queued, waiter));
    }
}

/// `Bun.Mutex`: the JS object of one VM, which shares the lock.
pub(crate) struct Mutex {
    core: Arc<MutexCore>,
}

impl Mutex {
    pub(crate) fn constructor() -> Self {
        Self {
            core: Arc::new(MutexCore {
                state: Guarded::new(State::default()),
            }),
        }
    }

    /// Resolves once this VM holds the lock. `lock()` (`src/js/builtins/Mutex.ts`) makes what
    /// unlocks it.
    pub(crate) fn lock(&self, cx: &mut JsScope<'_>, options: JsLocal<'_>) -> JsResult<JSValue> {
        let global = cx.global();
        let options = Options::parse(global, options.to_js(), false)?;
        if let Some(rejected) = options.rejected_if_aborted(global) {
            return Ok(rejected);
        }
        let id = ops().next_id();
        Ok(match self.core.lock(|| new_waiter(global, id)) {
            None => {
                held().0.push(Arc::clone(&self.core));
                JSPromise::resolved_promise_value(global, JSValue::UNDEFINED)
            }
            Some(waiter) => wait_for(
                global,
                OpKind::Lock,
                waiter,
                SmallVec::new(),
                Some(Arc::clone(&self.core)),
                options.signal,
                Vec::new(),
            ),
        })
    }

    pub(crate) fn try_lock(&self) -> bool {
        let locked = self.core.try_lock();
        if locked {
            held().0.push(Arc::clone(&self.core));
        }
        locked
    }

    /// Once per `lock()` or `tryLock()` that took the lock, on its thread.
    pub(crate) fn unlock(&self) {
        let held = &mut held().0;
        if let Some(i) = held.iter().position(|mutex| Arc::ptr_eq(mutex, &self.core)) {
            held.swap_remove(i);
            self.core.unlock();
        }
    }
}

impl Mutex {
    /// `console.log(mutex)` shows its state, as for a channel, not its methods.
    pub(crate) fn inspect(&self) -> String {
        let state = self.core.state.lock();
        format!(
            "Mutex {{ locked: {}, waiters: {} }}",
            state.locked,
            state.waiters.len()
        )
    }
}

/// `bun:internal-for-testing`: how many entries the mutex's queue of waiters holds, live or not.
pub(crate) fn waiters_for_testing(cx: &mut JsScope<'_>, mutex: JsLocal<'_>) -> JsResult<JSValue> {
    let global = cx.global();
    let Some(mutex) = mutex.to_js().as_class_ref::<Mutex>() else {
        return Err(crate::node::util::validators::throw_err_invalid_arg_type(
            global,
            "mutex",
            "Bun.Mutex",
            mutex.to_js(),
        ));
    };
    Ok(JSValue::js_number(
        mutex.core.state.lock().waiters.len() as f64
    ))
}

/// The mutexes in the structured clone messages that exist, on any thread, by what the message has
/// in their place.
static IN_FLIGHT: Guarded<BTreeMap<u64, Arc<MutexCore>>> = Guarded::new(BTreeMap::new());

/// `mutex` is a `Bun.Mutex`.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Mutex__retainForStructuredClone(mutex: JSValue) -> u64 {
    let Some(mutex) = mutex.as_class_ref::<Mutex>() else {
        return 0;
    };
    let mut id = [0; 8];
    bun_boringssl_sys::rand_bytes(&mut id);
    let id = u64::from_ne_bytes(id);
    IN_FLIGHT.lock().insert(id, Arc::clone(&mutex.core));
    id
}

/// `JSValue::ZERO` if no message has this id.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Mutex__fromStructuredClone(global: &JSGlobalObject, id: u64) -> JSValue {
    let Some(core) = IN_FLIGHT.lock().get(&id).cloned() else {
        return JSValue::ZERO;
    };
    Mutex { core }.into_js_wrapper(global).to_js()
}

#[unsafe(no_mangle)]
pub(crate) extern "C" fn Mutex__releaseStructuredClone(id: u64) {
    let core = IN_FLIGHT.lock().remove(&id);
    drop(core);
}
