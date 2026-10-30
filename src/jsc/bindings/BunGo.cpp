// The goroutine pool behind Bun.go.
//
// One process-wide scheduler (below) owns the goroutines that have not started yet. The pool is up
// to Bun__Go__maxPoolThreads() Workers of kind Goroutine, children of the main thread, created as
// goroutines wait for one. Each runs internal/go/runtime, which takes one goroutine per event loop
// turn (`take`); a thread with nothing to take is idle until `spawn` hands it the next one. A
// started goroutine stays on its thread; while it awaits, the thread runs others.
//
// Arguments and results cross as Bun.Channel values do (ChannelMessage.cpp: the clone
// worker.postMessage() makes; a SharedArrayBuffer or a Bun.Channel is shared). Everything posted between threads goes through
// ScriptExecutionContext::postTaskTo by context identifier, so a thread that has ended just drops
// what was meant for it. No lock is held while script runs, a value is cloned, or a task is posted.

#include "root.h"
#include "BunGo.h"

#include "ErrorStackTrace.h"
#include "InternalModuleRegistry.h"
#include "MessageWithMessagePorts.h"
#include "ZigGlobalObject.h"
#include "webcore/Worker.h"
#include "webcore/WorkerOptions.h"
#include <JavaScriptCore/JSModuleLoader.h>
#include <JavaScriptCore/ModuleRegistryEntry.h>
#include <JavaScriptCore/AbstractModuleRecord.h>
#include <JavaScriptCore/JSModuleNamespaceObject.h>
#include <wtf/Deque.h>
#include <wtf/HashMap.h>
#include <wtf/HashSet.h>
#include <wtf/Lock.h>
#include <wtf/NeverDestroyed.h>

extern "C" uint32_t Bun__Go__maxPoolThreads();
// ChannelMessage.cpp
extern "C" WebCore::MessageWithMessagePorts* Bun__ChannelMessage__create(JSC::JSGlobalObject*, JSC::EncodedJSValue value, JSC::EncodedJSValue transfer);
extern "C" JSC::EncodedJSValue Bun__ChannelMessage__deserialize(JSC::JSGlobalObject*, WebCore::MessageWithMessagePorts*);
extern "C" bool Bun__Go__resolveModule(void* bunVM, const BunString* specifier, BunString* out, BunString* errorMessage);

namespace Bun::Go {
using namespace JSC;
using namespace WebCore;

namespace {

constexpr ScriptExecutionContextIdentifier mainThreadContext = 1;

using Message = std::unique_ptr<MessageWithMessagePorts>;

struct Task {
    uint64_t id;
    // Module key the pool thread imports, and the export to call (see internal/go/spawn).
    String module;
    String exportName;
    bool fromCommonJSExports;
    Message args;
};

// internal/go/spawn's `Outcome`.
enum class Outcome : int32_t {
    Fulfilled = 0,
    Rejected = 1,
    Failed = 2,
};

struct Spawner {
    ScriptExecutionContextIdentifier context;
    BunLoopKind loopKind;
};

struct Running {
    ScriptExecutionContextIdentifier thread;
    Spawner spawner;
};

class Scheduler {
public:
    // Spawner thread. Returns the goroutine's id.
    uint64_t spawn(Task&&, Spawner);
    // Pool thread: the next goroutine for it, or none (it is idle until it is woken).
    std::optional<Task> take(ScriptExecutionContextIdentifier thread);
    // Pool thread: who is waiting for goroutine `id`, which is no longer running.
    std::optional<Spawner> finish(uint64_t id);
    // Main thread: the thread is ending (asked to, or on its own). Idempotent. It may be called
    // while the main thread's context stops its active objects, so it starts no thread.
    void retire(ScriptExecutionContextIdentifier thread);
    // Main thread: `threads` (the whole pool) are retired, and what is queued is dropped. Nothing
    // started before the reset runs in the pool that grows after it.
    void reset(const Vector<ScriptExecutionContextIdentifier>& threads);
    void threadFailedToStart();
    // Wake an idle thread for what is queued, or grow the pool. Called without the lock.
    void dispatch();

private:
    void retireLocked(ScriptExecutionContextIdentifier, Vector<std::pair<uint64_t, Spawner>>& lost) WTF_REQUIRES_LOCK(m_lock);
    // Whether to start another thread (counted as starting) for what is queued.
    bool shouldGrow() WTF_REQUIRES_LOCK(m_lock);
    void grow();

    Lock m_lock;
    Deque<std::pair<Task, Spawner>> m_queue WTF_GUARDED_BY_LOCK(m_lock);
    // Threads that found nothing to take, most recent last.
    Vector<ScriptExecutionContextIdentifier> m_idle WTF_GUARDED_BY_LOCK(m_lock);
    // Threads that were woken and have not taken since.
    HashSet<ScriptExecutionContextIdentifier> m_woken WTF_GUARDED_BY_LOCK(m_lock);
    // Threads that have taken at least once and are not retired.
    HashSet<ScriptExecutionContextIdentifier> m_threads WTF_GUARDED_BY_LOCK(m_lock);
    HashSet<ScriptExecutionContextIdentifier> m_retired WTF_GUARDED_BY_LOCK(m_lock);
    HashMap<uint64_t, Running> m_running WTF_GUARDED_BY_LOCK(m_lock);
    // Threads not retired, and of those, the ones that have not taken yet.
    unsigned m_threadCount WTF_GUARDED_BY_LOCK(m_lock) { 0 };
    unsigned m_startingCount WTF_GUARDED_BY_LOCK(m_lock) { 0 };
    uint64_t m_nextId WTF_GUARDED_BY_LOCK(m_lock) { 1 };
};

Scheduler& scheduler()
{
    static NeverDestroyed<Scheduler> instance;
    return instance;
}

// Main thread only: the pool's Worker objects (they have no JS wrapper).
Vector<Ref<Worker>>& poolWorkers()
{
    static NeverDestroyed<Vector<Ref<Worker>>> workers;
    return workers;
}

unsigned maxThreads()
{
    static const unsigned max = Bun__Go__maxPoolThreads();
    return max;
}

// Null with an exception pending if `value` can't be cloned.
Message serialize(JSGlobalObject* globalObject, JSValue value)
{
    return Message { Bun__ChannelMessage__create(globalObject, JSValue::encode(value), JSValue::encode(jsUndefined())) };
}

JSValue deserialize(Zig::GlobalObject* globalObject, Message&& message)
{
    return JSValue::decode(Bun__ChannelMessage__deserialize(globalObject, message.release()));
}

void startPoolThread(ScriptExecutionContext& mainContext)
{
    WorkerOptions options;
    options.kind = WorkerOptions::Kind::Goroutine;
    options.name = "Bun.go"_s;
    options.unref = true;
    auto worker = Worker::create(mainContext, emptyString(), WTF::move(options));
    if (worker.hasException() || worker.returnValue()->hasExited()) {
        scheduler().threadFailedToStart();
        return;
    }
    poolWorkers().append(worker.releaseReturnValue());
}

JSObject* runtimeExports(Zig::GlobalObject* globalObject)
{
    JSValue exports = BUN_REQUIRE_INTERNAL_MODULE(globalObject, InternalGoRuntime, "Bun.go", "pull"_s);
    return exports.getObject();
}

// Pool thread: it was idle, and something is queued.
void wake(ScriptExecutionContext& context)
{
    auto* globalObject = defaultGlobalObject(context.jsGlobalObject());
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    JSObject* exports = runtimeExports(globalObject);
    JSValue pull;
    if (!scope.exception())
        pull = exports->get(globalObject, Identifier::fromString(vm, "pull"_s));
    if (!scope.exception())
        JSC::call(globalObject, pull, MarkedArgumentBuffer {}, "Bun.go"_s);
    if (JSC::Exception* exception = scope.exception(); exception && !vm.isTerminationException(exception)) {
        (void)scope.tryClearException();
        Bun__reportUnhandledError(globalObject, JSValue::encode(exception->value()));
    }
}

bool Scheduler::shouldGrow()
{
    // Starting and woken threads will take what is queued; grow while more is queued than that.
    if (m_threadCount >= maxThreads() || m_queue.size() <= m_startingCount + m_woken.size())
        return false;
    m_threadCount++;
    m_startingCount++;
    return true;
}

// A goroutine whose thread ended before it settled.
void rejectAsThreadExited(uint64_t id, Spawner spawner);

uint64_t Scheduler::spawn(Task&& task, Spawner spawner)
{
    uint64_t id;
    bool refused = false;
    {
        Locker locker { m_lock };
        id = m_nextId++;
        task.id = id;
        // A thread of a pool that was reset runs on only until it notices: what it spawns is not
        // for the new pool.
        if (!m_retired.contains(spawner.context))
            m_queue.append({ WTF::move(task), spawner });
        else
            refused = true;
    }
    if (refused)
        rejectAsThreadExited(id, spawner);
    else
        dispatch();
    return id;
}

void Scheduler::dispatch()
{
    for (;;) {
        std::optional<ScriptExecutionContextIdentifier> thread;
        bool grows = false;
        {
            Locker locker { m_lock };
            if (m_queue.size() <= m_startingCount + m_woken.size())
                return;
            if (!m_idle.isEmpty()) {
                thread = m_idle.takeLast();
                m_woken.add(*thread);
            } else {
                grows = shouldGrow();
            }
        }
        if (grows)
            grow();
        if (!thread)
            return;
        // What is queued stays queued until a thread takes it: a thread that is ending refuses the
        // wake (or drops it unrun), and the goroutine goes to another.
        if (ScriptExecutionContext::postTaskTo(*thread, BunLoopKind::Regular, wake))
            return;
        Locker locker { m_lock };
        m_woken.remove(*thread);
    }
}

std::optional<Task> Scheduler::take(ScriptExecutionContextIdentifier thread)
{
    Locker locker { m_lock };
    if (m_retired.contains(thread))
        return std::nullopt;
    if (m_threads.add(thread).isNewEntry) {
        ASSERT(m_startingCount);
        m_startingCount--;
    }
    m_woken.remove(thread);
    if (m_queue.isEmpty()) {
        if (!m_idle.contains(thread))
            m_idle.append(thread);
        return std::nullopt;
    }
    auto [task, spawner] = m_queue.takeFirst();
    m_running.add(task.id, Running { thread, spawner });
    return WTF::move(task);
}

std::optional<Spawner> Scheduler::finish(uint64_t id)
{
    Locker locker { m_lock };
    auto it = m_running.find(id);
    if (it == m_running.end())
        return std::nullopt;
    auto spawner = it->value.spawner;
    m_running.remove(it);
    return spawner;
}

void Scheduler::threadFailedToStart()
{
    Locker locker { m_lock };
    m_threadCount--;
    m_startingCount--;
}

void Scheduler::retireLocked(ScriptExecutionContextIdentifier thread, Vector<std::pair<uint64_t, Spawner>>& lost)
{
    if (!m_retired.add(thread).isNewEntry)
        return;
    m_threadCount--;
    if (!m_threads.remove(thread))
        m_startingCount--;
    m_woken.remove(thread);
    m_idle.removeFirst(thread);
    m_running.removeIf([&](auto& entry) {
        if (entry.value.thread != thread)
            return false;
        lost.append({ entry.key, entry.value.spawner });
        return true;
    });
}

void Scheduler::retire(ScriptExecutionContextIdentifier thread)
{
    Vector<std::pair<uint64_t, Spawner>> lost;
    {
        Locker locker { m_lock };
        retireLocked(thread, lost);
    }
    for (auto& [id, spawner] : lost)
        rejectAsThreadExited(id, spawner);
}

void Scheduler::reset(const Vector<ScriptExecutionContextIdentifier>& threads)
{
    Vector<std::pair<uint64_t, Spawner>> lost;
    {
        Locker locker { m_lock };
        for (auto thread : threads)
            retireLocked(thread, lost);
        for (auto& [task, spawner] : m_queue)
            lost.append({ task.id, spawner });
        m_queue.clear();
    }
    for (auto& [id, spawner] : lost)
        rejectAsThreadExited(id, spawner);
}

void Scheduler::grow()
{
    if (auto* context = ScriptExecutionContext::getMainThreadScriptExecutionContext(); context && context->isContextThread()) {
        startPoolThread(*context);
        return;
    }
    bool posted = ScriptExecutionContext::postTaskTo(mainThreadContext, BunLoopKind::Regular, [](ScriptExecutionContext& context) {
        startPoolThread(context);
    });
    if (!posted)
        threadFailedToStart();
}

JSObject* spawnExports(Zig::GlobalObject* globalObject)
{
    JSValue exports = BUN_REQUIRE_INTERNAL_MODULE(globalObject, InternalGoSpawn, "Bun.go", "settle"_s);
    return exports.getObject();
}

JSArray* taskToJS(Zig::GlobalObject* globalObject, Task&& task)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    JSValue args = deserialize(globalObject, WTF::move(task.args));
    RETURN_IF_EXCEPTION(scope, nullptr);
    MarkedArgumentBuffer values;
    values.append(jsNumber(task.id));
    values.append(jsString(vm, WTF::move(task.module)));
    values.append(jsString(vm, WTF::move(task.exportName)));
    values.append(jsBoolean(task.fromCommonJSExports));
    values.append(args);
    RELEASE_AND_RETURN(scope, constructArray(globalObject, static_cast<ArrayAllocationProfile*>(nullptr), values));
}

// Spawner thread: hand the outcome of goroutine `id` to internal/go/spawn.
void settleOnSpawner(ScriptExecutionContext& context, uint64_t id, Outcome outcome, Message&& message)
{
    auto* globalObject = defaultGlobalObject(context.jsGlobalObject());
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    JSValue value = deserialize(globalObject, WTF::move(message));
    if (JSC::Exception* exception = scope.exception()) {
        if (vm.isTerminationException(exception))
            return;
        (void)scope.tryClearException();
        value = exception->value();
        outcome = Outcome::Failed;
    }
    JSObject* exports = spawnExports(globalObject);
    RETURN_IF_EXCEPTION(scope, );
    JSValue settle = exports->get(globalObject, Identifier::fromString(vm, "settle"_s));
    RETURN_IF_EXCEPTION(scope, );
    MarkedArgumentBuffer args;
    args.append(jsNumber(id));
    args.append(jsNumber(static_cast<int32_t>(outcome)));
    args.append(value);
    JSC::call(globalObject, settle, args, "Bun.go"_s);
    if (JSC::Exception* exception = scope.exception(); exception && !vm.isTerminationException(exception)) {
        (void)scope.tryClearException();
        Bun__reportUnhandledError(globalObject, JSValue::encode(exception->value()));
    }
}

void rejectAsThreadExited(uint64_t id, Spawner spawner)
{
    ScriptExecutionContext::postTaskTo(spawner.context, spawner.loopKind, [id](ScriptExecutionContext& context) {
        auto* globalObject = defaultGlobalObject(context.jsGlobalObject());
        auto& vm = globalObject->vm();
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
        JSObject* exports = spawnExports(globalObject);
        RETURN_IF_EXCEPTION(scope, );
        JSValue settle = exports->get(globalObject, Identifier::fromString(vm, "settle"_s));
        RETURN_IF_EXCEPTION(scope, );
        MarkedArgumentBuffer args;
        args.append(jsNumber(id));
        args.append(jsNumber(static_cast<int32_t>(Outcome::Failed)));
        args.append(createError(globalObject, ErrorCode::ERR_GOROUTINE_THREAD_EXITED, "The thread running this goroutine exited before it settled"_s));
        JSC::call(globalObject, settle, args, "Bun.go"_s);
        if (JSC::Exception* exception = scope.exception(); exception && !vm.isTerminationException(exception)) {
            (void)scope.tryClearException();
            Bun__reportUnhandledError(globalObject, JSValue::encode(exception->value()));
        }
    });
}

ScriptExecutionContext& contextOf(JSGlobalObject* lexicalGlobalObject)
{
    return *defaultGlobalObject(lexicalGlobalObject)->scriptExecutionContext();
}

} // namespace

void poolThreadWillExit(ScriptExecutionContextIdentifier thread)
{
    scheduler().retire(thread);
}

void poolThreadDidExit(ScriptExecutionContextIdentifier thread)
{
    scheduler().retire(thread);
    poolWorkers().removeFirstMatching([&](auto& worker) {
        return worker->clientIdentifier() == thread;
    });
    scheduler().dispatch();
}

// spawn(module, exportName, fromCommonJSExports, args): queue a goroutine; returns its id.
// Throws (DataCloneError) if `args` can't be cloned.
JSC_DEFINE_HOST_FUNCTION(jsGoSpawn, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    String module = callFrame->argument(0).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    String exportName = callFrame->argument(1).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    bool fromCommonJSExports = callFrame->argument(2).toBoolean(lexicalGlobalObject);
    auto args = serialize(lexicalGlobalObject, callFrame->argument(3));
    RETURN_IF_EXCEPTION(scope, {});
    auto& context = contextOf(lexicalGlobalObject);
    Task task { 0, WTF::move(module).isolatedCopy(), WTF::move(exportName).isolatedCopy(), fromCommonJSExports, WTF::move(args) };
    uint64_t id = scheduler().spawn(WTF::move(task), Spawner { context.identifier(), context.currentLoopKind() });
    return JSValue::encode(jsNumber(id));
}

// keepAlive(delta): ref (+1) or unref (-1) this thread's event loop for a goroutine.
JSC_DEFINE_HOST_FUNCTION(jsGoKeepAlive, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& context = contextOf(lexicalGlobalObject);
    if (callFrame->argument(0).asNumber() > 0)
        context.refEventLoop();
    else
        context.unrefEventLoop();
    return JSValue::encode(jsUndefined());
}

// moduleOf(fn): [sourceURL, namespace | undefined] of the module whose source defines `fn`, or
// undefined for a function with no source of its own (native, bound). The namespace is there for
// an ES module that has started evaluating; a CommonJS module is found in the require map.
JSC_DEFINE_HOST_FUNCTION(jsGoModuleOf, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* function = dynamicDowncast<JSFunction>(callFrame->argument(0));
    if (!function || function->isHostOrBuiltinFunction())
        return JSValue::encode(jsUndefined());
    String url = Zig::sourceURL(vm, function);
    if (url.isEmpty())
        return JSValue::encode(jsUndefined());
    JSValue ns = jsUndefined();
    if (auto* loader = lexicalGlobalObject->moduleLoader()) {
        if (auto* entry = loader->registryEntry(Identifier::fromString(vm, url))) {
            if (auto* record = entry->record(); record && record->moduleEnvironmentMayBeNull()) {
                ns = record->getModuleNamespace(lexicalGlobalObject, false);
                RETURN_IF_EXCEPTION(scope, {});
            }
        }
    }
    MarkedArgumentBuffer values;
    values.append(jsString(vm, url));
    values.append(ns);
    RELEASE_AND_RETURN(scope, JSValue::encode(constructArray(lexicalGlobalObject, static_cast<ArrayAllocationProfile*>(nullptr), values)));
}

// resolveModule(specifier): the module key of Bun.go(specifier), resolved as `new Worker(specifier)`.
JSC_DEFINE_HOST_FUNCTION(jsGoResolveModule, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    String specifier = callFrame->argument(0).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    BunString specifierString = Bun::toString(specifier);
    BunString out = BunStringEmpty;
    BunString errorMessage = BunStringEmpty;
    if (!Bun__Go__resolveModule(defaultGlobalObject(lexicalGlobalObject)->bunVM(), &specifierString, &out, &errorMessage)) {
        String message = errorMessage.transferToWTFString();
        if (message.isEmpty())
            message = makeString("Cannot find module \""_s, specifier, "\""_s);
        return Bun::throwError(lexicalGlobalObject, scope, ErrorCode::ERR_MODULE_NOT_FOUND, message);
    }
    return JSValue::encode(jsString(vm, out.transferToWTFString()));
}

// take(): [id, module, exportName, fromCommonJSExports, args] of the next goroutine for this pool
// thread, or undefined; then `run` is called with the next one handed to it.
JSC_DEFINE_HOST_FUNCTION(jsGoTake, (JSGlobalObject * lexicalGlobalObject, CallFrame*))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* globalObject = defaultGlobalObject(lexicalGlobalObject);
    auto task = scheduler().take(globalObject->scriptExecutionContext()->identifier());
    if (!task)
        return JSValue::encode(jsUndefined());
    auto id = task->id;
    JSArray* value = taskToJS(globalObject, WTF::move(*task));
    if (JSC::Exception* exception = scope.exception()) {
        if (vm.isTerminationException(exception))
            return {};
        (void)scope.tryClearException();
        MarkedArgumentBuffer values;
        values.append(jsNumber(id));
        values.append(exception->value());
        RELEASE_AND_RETURN(scope, JSValue::encode(constructArray(globalObject, static_cast<ArrayAllocationProfile*>(nullptr), values)));
    }
    return JSValue::encode(value);
}

// settle(id, outcome, value): goroutine `id` returned (0) or threw (1) `value`. Throws (DataCloneError)
// if `value` can't be cloned; the goroutine is still running until a settle() succeeds.
JSC_DEFINE_HOST_FUNCTION(jsGoSettle, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    uint64_t id = static_cast<uint64_t>(callFrame->argument(0).asNumber());
    auto outcome = callFrame->argument(1).asNumber() ? Outcome::Rejected : Outcome::Fulfilled;
    auto message = serialize(lexicalGlobalObject, callFrame->argument(2));
    RETURN_IF_EXCEPTION(scope, {});
    auto spawner = scheduler().finish(id);
    if (!spawner)
        return JSValue::encode(jsUndefined());
    ScriptExecutionContext::postTaskTo(spawner->context, spawner->loopKind, [id, outcome, message = WTF::move(message)](ScriptExecutionContext& context) mutable {
        settleOnSpawner(context, id, outcome, WTF::move(message));
    });
    return JSValue::encode(jsUndefined());
}

} // namespace Bun::Go

namespace WebCore {

JSValue createGoSpawnBinding(Zig::GlobalObject* globalObject)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    JSObject* binding = constructEmptyObject(globalObject, globalObject->objectPrototype(), 4);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "spawn"_s), 4, Bun::Go::jsGoSpawn, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "keepAlive"_s), 1, Bun::Go::jsGoKeepAlive, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "moduleOf"_s), 1, Bun::Go::jsGoModuleOf, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "resolveModule"_s), 1, Bun::Go::jsGoResolveModule, ImplementationVisibility::Public, NoIntrinsic, 0);
    RELEASE_AND_RETURN(scope, binding);
}

JSValue createGoRuntimeBinding(Zig::GlobalObject* globalObject)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    JSObject* binding = constructEmptyObject(globalObject, globalObject->objectPrototype(), 2);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "take"_s), 0, Bun::Go::jsGoTake, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "settle"_s), 3, Bun::Go::jsGoSettle, ImplementationVisibility::Public, NoIntrinsic, 0);
    RELEASE_AND_RETURN(scope, binding);
}

// `--hot` reloads and `bun test --isolate` retires a file: the pool's threads end, with every
// goroutine they run or that waits for one. Main thread.
extern "C" void Bun__Go__restartPool()
{
    auto workers = WTF::move(Bun::Go::poolWorkers());
    Vector<ScriptExecutionContextIdentifier> threads;
    for (auto& worker : workers)
        threads.append(worker->clientIdentifier());
    Bun::Go::scheduler().reset(threads);
    for (auto& worker : workers)
        worker->terminate();
}

// A pool thread's stand-in for an entry point. The thread's loop stays alive until it is asked to
// stop; the pool never keeps the process alive (its Workers are unref'd).
extern "C" void Bun__Worker__loadGoroutineRuntime(Zig::GlobalObject* globalObject)
{
    globalObject->scriptExecutionContext()->refEventLoop();
    BUN_REQUIRE_INTERNAL_MODULE(globalObject, InternalGoRuntime, "Bun.go");
}

// An uncaught error on a pool thread is the program's, as a panic in a goroutine is in Go: it is
// reported on the main thread (process.on("uncaughtException") there, or the process exits).
extern "C" void Bun__Go__reportUncaughtError(Zig::GlobalObject* globalObject, JSC::EncodedJSValue encodedError)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    JSValue error = JSValue::decode(encodedError);
    auto message = Bun::Go::serialize(globalObject, error);
    CLEAR_IF_EXCEPTION(scope);
    if (!message) {
        // What can't be cloned is reported as its text.
        auto text = error.toWTFString(globalObject);
        CLEAR_IF_EXCEPTION(scope);
        message = Bun::Go::serialize(globalObject, jsString(vm, text));
        CLEAR_IF_EXCEPTION(scope);
        if (!message)
            return;
    }
    ScriptExecutionContext::postTaskTo(Bun::Go::mainThreadContext, BunLoopKind::Regular, [message = WTF::move(message)](ScriptExecutionContext& context) mutable {
        auto* globalObject = defaultGlobalObject(context.jsGlobalObject());
        auto& vm = globalObject->vm();
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
        JSValue error = Bun::Go::deserialize(globalObject, WTF::move(message));
        if (JSC::Exception* exception = scope.exception()) {
            if (vm.isTerminationException(exception))
                return;
            (void)scope.tryClearException();
            error = exception->value();
        }
        Bun__reportUnhandledError(globalObject, JSValue::encode(error));
    });
}

}
