//! `Bun.Mutex`: a lock that every thread of the process shares, waited for like a channel.
//!
//! Like a channel, the lock is native and shared: a structured clone of a `Bun.Mutex` holds an
//! `Arc` of the same [`MutexCore`]. A `lock()` that has to wait is a [`Waiter`] in the mutex's queue
//! and a channel operation in its VM's table, and is settled, aborted and torn down the same way.
//! Unlocking hands the lock straight to the oldest waiter that can still take it, so the lock
//! never looks free while anyone waits, and waiters take it in the order they came. A handoff
//! whose waiter is gone by the time it arrives ([`Handoff`] dropped unsettled) unlocks again, and
//! so does a thread that ends holding a lock ([`Held`]), the way a process that exits releases its
//! file locks.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bun_collections::smallvec::SmallVec;
use bun_jsc::bindgen::{JsLocal, JsPayload as _, JsScope};
use bun_jsc::{JSGlobalObject, JSPromise, JSValue, JsResult};
use bun_threading::Guarded;

use super::channel::{OpKind, Options, WakeResult, Waiter, new_waiter, ops, wait_for};

pub(crate) struct MutexCore {
    state: Guarded<State>,
}

#[derive(Default)]
struct State {
    locked: bool,
    /// Oldest first. Entries of waiters that were cancelled stay until their VM forgets them.
    waiters: VecDeque<Arc<Waiter>>,
}

/// The lock, on its way to the waiter it was handed to. Unlocks if it is dropped before the
/// waiter's VM takes it.
pub(super) struct Handoff(Option<Arc<MutexCore>>);

impl Handoff {
    /// The waiter's VM holds the lock now.
    pub(super) fn keep(mut self) {
        held().0.push(self.0.take().expect("a handoff is kept once"));
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
        state.waiters.push_back(Arc::clone(&waiter));
        Some(waiter)
    }

    fn unlock(self: &Arc<Self>) {
        let next = {
            let mut state = self.state.lock();
            debug_assert!(state.locked, "only a holder unlocks");
            loop {
                match state.waiters.pop_front() {
                    Some(waiter) if waiter.fire() => break Some(waiter),
                    Some(_) => {}
                    None => {
                        state.locked = false;
                        break None;
                    }
                }
            }
        };
        if let Some(waiter) = next {
            waiter.wake(0, WakeResult::Acquired(Handoff(Some(Arc::clone(self)))));
        }
    }

    /// Takes the entry of a waiter that fired or was cancelled out of the queue.
    pub(super) fn forget(&self, waiter: &Arc<Waiter>) {
        self.state
            .lock()
            .waiters
            .retain(|queued| !Arc::ptr_eq(queued, waiter));
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
        mutex.core.state.lock().waiters.len() as f64,
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
