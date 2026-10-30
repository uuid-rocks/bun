//! Go's `fatal error: all goroutines are asleep - deadlock!`.
//!
//! Right before its event loop sleeps, a VM checks whether anything it holds could wake it: a
//! queued or posted task, a handle or poll (ref'd or not), a timer (ref'd or not), a signal
//! listener, an inspector, async work of JSC's (`Atomics.waitAsync`), a ref on its loop other than a
//! goroutine's. If nothing could, the VM is asleep: only another thread can wake it, by posting to
//! it. When every VM of the process is asleep, none has work posted to it, the `Bun.go` pool has
//! nothing queued or starting, and a channel, select or mutex operation waits, then no thread can
//! ever run again. Each VM then describes its waiting operations on its own thread (where the
//! async functions that await them are), and the main thread prints them and exits with code 1. A
//! program that waits only on something else (a promise nothing resolves) is never reported.
//!
//! A VM marks itself asleep under [`REGISTRY`]'s lock and unmarks itself under it as soon as its
//! poll returns, before it runs anything. The thread that marks the last VM asleep checks the rest
//! under that lock: the others are in their polls or waiting for the lock, so what they hold stays
//! as it is while it reads it. A thread posts to a VM before it can mark itself asleep, so a wake
//! still on its way shows as posted work.

use core::cell::Cell;

use bun_jsc::JSGlobalObject;
use bun_jsc::virtual_machine::{ThreadKind, VirtualMachine};
use bun_threading::Guarded;

use crate::jsc_hooks::RuntimeState;

/// What a VM tracks about itself, in its `RuntimeState`.
#[derive(Default)]
pub(crate) struct Local {
    /// Refs on the loop that only goroutines hold: one per goroutine this VM waits for, and on a
    /// pool thread, the one that keeps it running.
    goroutine_refs: Cell<u32>,
}

struct Entry {
    vm: *const VirtualMachine,
    state: *const RuntimeState,
    asleep: bool,
}

/// The deadlock being reported.
struct Report {
    /// VMs that have not described their operations yet.
    pending: usize,
    /// What each VM described: one entry per waiting operation.
    sections: Vec<(ThreadKind, Vec<String>)>,
}

struct Registry {
    vms: Vec<Entry>,
    asleep: usize,
    report: Option<Report>,
}

// SAFETY: an entry's pointers are read only under the lock, while their VM is registered and
// asleep (blocked in its poll or on this lock).
unsafe impl Send for Registry {}

static REGISTRY: Guarded<Registry> = Guarded::new(Registry {
    vms: Vec::new(),
    asleep: 0,
    report: None,
});

unsafe extern "C" {
    /// BunGo.cpp: nothing is queued for the pool and no pool thread is starting or being woken.
    safe fn Bun__Go__poolIsIdle() -> bool;
    /// BunGo.cpp: the VM of the context runs [`Bun__Deadlock__describe`]. False if it is ending.
    safe fn Bun__Go__postDeadlockDescribe(context: u32) -> bool;
    /// BunGo.cpp: the main thread runs [`Bun__Deadlock__exit`].
    safe fn Bun__Go__postDeadlockExit();
}

fn enabled() -> bool {
    cfg!(unix) && bun_core::env_var::BUN_GO_DEADLOCK::get().unwrap_or(true)
}

/// The VM's `RuntimeState` was made.
pub(crate) fn register(vm: *const VirtualMachine, state: *const RuntimeState) {
    REGISTRY.lock().vms.push(Entry {
        vm,
        state,
        asleep: false,
    });
}

/// The VM's `RuntimeState` is being dropped.
pub(crate) fn unregister(state: *const RuntimeState) {
    let mut registry = REGISTRY.lock();
    if let Some(i) = registry.vms.iter().position(|entry| entry.state == state) {
        if registry.vms.swap_remove(i).asleep {
            registry.asleep -= 1;
        }
    }
}

/// BunGo.cpp: a goroutine's ref on this thread's loop was taken (`1`) or released (`-1`).
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Bun__Deadlock__goroutineRef(delta: i32) {
    let state = crate::jsc_hooks::runtime_state();
    if state.is_null() {
        return;
    }
    // SAFETY: this thread's live `RuntimeState`.
    let refs = unsafe { &(*state).deadlock.goroutine_refs };
    refs.set(refs.get().saturating_add_signed(delta));
}

/// Right before `vm`'s loop polls. Returns whether the VM was marked asleep, for [`after_poll`].
///
/// # Safety
/// `vm` is this thread's live VM and `state` its `RuntimeState`.
pub(crate) unsafe fn before_poll(vm: *const VirtualMachine, state: *const RuntimeState) -> bool {
    if !enabled() {
        return false;
    }
    // SAFETY: fn contract.
    let (vm_ref, state_ref) = unsafe { (&*vm, &*state) };
    // A change to a file starts the program again.
    if !matches!(
        vm_ref.hot_reload,
        bun_options_types::context::HotReload::None
    ) || !state_ref.timer.quiet()
        || !vm_ref.deadlock_quiet(state_ref.deadlock.goroutine_refs.get())
    {
        return false;
    }
    let mut registry = REGISTRY.lock();
    let Some(entry) = registry.vms.iter_mut().find(|entry| entry.state == state) else {
        return false;
    };
    entry.asleep = true;
    registry.asleep += 1;
    if registry.report.is_none() && is_deadlocked(&registry) {
        let contexts: Vec<u32> = registry
            .vms
            .iter()
            // SAFETY: as in `is_deadlocked`.
            .filter(|entry| unsafe { !(*entry.state).channel_ops.is_empty() })
            // SAFETY: as in `is_deadlocked`.
            .map(|entry| unsafe { (*entry.vm).root_context_id() })
            .collect();
        registry.report = Some(Report {
            pending: contexts.len(),
            sections: Vec::new(),
        });
        drop(registry);
        for context in contexts {
            if !Bun__Go__postDeadlockDescribe(context) {
                described(None);
            }
        }
    }
    true
}

/// Right after the poll [`before_poll`] marked the VM asleep for returns.
pub(crate) fn after_poll(state: *const RuntimeState) {
    let mut registry = REGISTRY.lock();
    if let Some(entry) = registry.vms.iter_mut().find(|entry| entry.state == state) {
        if core::mem::replace(&mut entry.asleep, false) {
            registry.asleep -= 1;
        }
    }
}

fn is_deadlocked(registry: &Registry) -> bool {
    let vms = &registry.vms;
    // A worker whose VM is starting, or whose VM is gone but whose thread is not, can still run.
    // So can a threadsafe function an addon's thread may call.
    if registry.asleep != vms.len()
        || crate::napi::napi_body::threadsafe_function_live_count() != 0
        || vms.len()
            != 1 + bun_jsc::web_worker::LIVE_WORKERS.load(core::sync::atomic::Ordering::SeqCst)
        || !Bun__Go__poolIsIdle()
    {
        return false;
    }
    let mut waiting = false;
    for entry in vms {
        // SAFETY: registered and asleep: the VM is in its poll or waiting for this lock.
        let (vm, state) = unsafe { (&*entry.vm, &*entry.state) };
        if vm.has_posted_work() {
            return false;
        }
        waiting |= !state.channel_ops.is_empty();
    }
    waiting
}

/// BunGo.cpp, on each VM with operations that wait, once a deadlock is found.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Bun__Deadlock__describe(global: &JSGlobalObject) {
    let ops: Vec<String> = super::channel::ops()
        .iter()
        .map(|op| op.describe(global))
        .collect();
    described(Some((global.bun_vm().thread_kind(), ops)));
}

fn described(section: Option<(ThreadKind, Vec<String>)>) {
    let mut registry = REGISTRY.lock();
    let Some(report) = &mut registry.report else {
        return;
    };
    report.sections.extend(section);
    report.pending -= 1;
    if report.pending == 0 {
        drop(registry);
        Bun__Go__postDeadlockExit();
    }
}

/// BunGo.cpp, on the main thread, once every VM described what waits.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn Bun__Deadlock__exit(global: &JSGlobalObject) {
    let Some(mut report) = REGISTRY.lock().report.take() else {
        return;
    };
    // Goroutines run on whichever pool thread is free: theirs are one section.
    let mut main = Vec::new();
    let mut goroutines = Vec::new();
    let mut workers = Vec::new();
    for (kind, ops) in report.sections.drain(..) {
        match kind {
            ThreadKind::Main => main.extend(ops),
            ThreadKind::Goroutine => goroutines.extend(ops),
            ThreadKind::Worker => workers.push(ops),
        }
    }
    let mut text = String::new();
    let mut section = |label: &str, ops: Vec<String>| {
        if !ops.is_empty() {
            text.push_str(&format!("\n{label}:\n  {}\n", grouped(ops).join("\n  ")));
        }
    };
    section("main thread", main);
    section("goroutines", goroutines);
    for (i, ops) in workers.into_iter().enumerate() {
        section(&format!("Worker {}", i + 1), ops);
    }
    bun_core::pretty_errorln!(
        "<r><red>error<r><d>:<r> <b>all goroutines are asleep - deadlock!<r>\n{}\nNo thread is left that can send, receive, close or unlock for these. Set BUN_GO_DEADLOCK=0 to wait anyway.",
        text
    );
    let vm = global.bun_vm().as_mut();
    vm.exit_handler.exit_code = 1;
    vm.exit_handler.requested = true;
    vm.on_exit();
    vm.global_exit();
}

/// Sorted, with identical waits (the same operation at the same stack) once, and how many there are.
fn grouped(mut ops: Vec<String>) -> Vec<String> {
    ops.sort();
    let mut out: Vec<String> = Vec::with_capacity(ops.len());
    let mut i = 0;
    while i < ops.len() {
        let count = ops[i..].iter().take_while(|op| **op == ops[i]).count();
        let op = &ops[i];
        out.push(if count == 1 {
            op.clone()
        } else {
            let first_line =
                bun_core::strings::index_of_char_usize(op.as_bytes(), b'\n').unwrap_or(op.len());
            format!("{} \u{d7} {count}{}", &op[..first_line], &op[first_line..])
        });
        i += count;
    }
    out
}
