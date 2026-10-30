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

#include "BunProcess.h"
#include "ChannelMessage.h"
#include "CodeGenerationFromStrings.h"
#include "ErrorStackTrace.h"
#include "InternalModuleRegistry.h"
#include "JSCommonJSModule.h"
#include "WebCoreJSBuiltins.h"
#include "MessageWithMessagePorts.h"
#include "ZigGlobalObject.h"
#include "webcore/Worker.h"
#include "webcore/WorkerOptions.h"
#include <JavaScriptCore/JSModuleLoader.h>
#include <JavaScriptCore/MicrotaskQueue.h>
#include <JavaScriptCore/ModuleRegistryEntry.h>
#include <JavaScriptCore/AbstractModuleRecord.h>
#include <JavaScriptCore/JSModuleNamespaceObject.h>
#include <JavaScriptCore/Completion.h>
#include <JavaScriptCore/CyclicModuleRecord.h>
#include <JavaScriptCore/FunctionExecutable.h>
#include <JavaScriptCore/FunctionExecutableInlines.h>
#include <JavaScriptCore/JSLexicalEnvironment.h>
#include <JavaScriptCore/JSModuleEnvironment.h>
#include <JavaScriptCore/JSSymbolTableObject.h>
#include <JavaScriptCore/JSWithScope.h>
#include <JavaScriptCore/SourceProvider.h>
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
// web_worker.rs: a module's source map, shared with the pool thread that compiles a function of it.
extern "C" void* Bun__Go__retainSourceMap(void* bunVM, const BunString* path);
extern "C" void Bun__Go__adoptSourceMap(void* bunVM, const BunString* path, void* map);
extern "C" void Bun__Go__releaseSourceMap(void* map);

// bun_runtime::concurrency::go. Zero-based, in bytes of the text; the line is -1 for none.
struct NamePosition {
    int32_t line;
    int32_t column;
};
struct FunctionNames {
    // bun_js_parser's FunctionTextKind
    uint8_t kind;
    NamePosition thisPosition;
    NamePosition importMetaPosition;
    // It calls `eval` directly, which reads the variables around it.
    bool directEval;
};
enum class FunctionTextKind : uint8_t {
    Other = 0,
    Function = 1,
    Arrow = 2,
    Class = 3,
};
extern "C" bool Bun__Go__functionNames(const char8_t* text, size_t length, void* context, void (*onName)(void*, const uint8_t* name, size_t length, NamePosition), FunctionNames* out);

namespace Bun::Go {
using namespace JSC;
using namespace WebCore;

namespace {

constexpr ScriptExecutionContextIdentifier mainThreadContext = 1;

using Message = std::unique_ptr<MessageWithMessagePorts>;

struct SourceMapRelease {
    void operator()(void* map) const { Bun__Go__releaseSourceMap(map); }
};
// The source map of the module a function no module exports is written in: the pool thread maps
// the lines of its stack traces with it, as the spawner would (Bun transpiled that text).
using SourceMap = std::unique_ptr<void, SourceMapRelease>;

struct Task {
    uint64_t id;
    // Module key the pool thread imports, and the export to call (see internal/go/spawn). For a
    // function no module exports, the module it is written in.
    String module;
    String exportName;
    bool fromCommonJSExports;
    Message args;
    // A function no module exports: its source, compiled on the pool thread where it starts in
    // `module` (zero-based line and column).
    String source;
    int32_t line { 0 };
    int32_t column { 0 };
    bool strict { false };
    SourceMap sourceMap {};
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
    // A thread counted as starting won't take: what was queued for it goes to an idle thread.
    void threadFailedToStart();
    // Main thread: a thread that grow() asked it for is being created.
    void willCreateThread();
    enum class Grow : bool { No,
        Yes };
    // Wake an idle thread for what is queued, or grow the pool. Called without the lock.
    void dispatch(Grow = Grow::Yes);

private:
    void retireLocked(ScriptExecutionContextIdentifier, Vector<std::pair<uint64_t, Spawner>>& lost) WTF_REQUIRES_LOCK(m_lock);
    // Whether to start another thread (counted as starting) for what is queued. `onMainThread`:
    // whether this thread can create it now.
    bool shouldGrow(bool onMainThread) WTF_REQUIRES_LOCK(m_lock);
    void grow(bool onMainThread);

    Lock m_lock;
    Deque<std::pair<Task, Spawner>> m_queue WTF_GUARDED_BY_LOCK(m_lock);
    // Threads that found nothing to take, most recent last.
    Vector<ScriptExecutionContextIdentifier> m_idle WTF_GUARDED_BY_LOCK(m_lock);
    // How many goroutines each thread runs, if any.
    HashMap<ScriptExecutionContextIdentifier, unsigned> m_load WTF_GUARDED_BY_LOCK(m_lock);
    // Threads that were woken and have not taken since.
    HashSet<ScriptExecutionContextIdentifier> m_woken WTF_GUARDED_BY_LOCK(m_lock);
    // Threads that have taken at least once and are not retired.
    HashSet<ScriptExecutionContextIdentifier> m_threads WTF_GUARDED_BY_LOCK(m_lock);
    HashSet<ScriptExecutionContextIdentifier> m_retired WTF_GUARDED_BY_LOCK(m_lock);
    HashMap<uint64_t, Running> m_running WTF_GUARDED_BY_LOCK(m_lock);
    // Threads not retired, and of those, the ones that have not taken yet.
    unsigned m_threadCount WTF_GUARDED_BY_LOCK(m_lock) { 0 };
    unsigned m_startingCount WTF_GUARDED_BY_LOCK(m_lock) { 0 };
    // Of the starting threads, the ones the main thread has yet to create. A busy main thread
    // would hold up what is queued for them, so it is not theirs.
    unsigned m_uncreatedCount WTF_GUARDED_BY_LOCK(m_lock) { 0 };
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

bool Scheduler::shouldGrow(bool onMainThread)
{
    // Starting and woken threads will take what is queued; grow while more is queued than that.
    if (m_threadCount >= maxThreads() || m_queue.size() <= m_startingCount + m_woken.size())
        return false;
    m_threadCount++;
    m_startingCount++;
    if (!onMainThread)
        m_uncreatedCount++;
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

void Scheduler::dispatch(Grow mayGrow)
{
    auto* mainContext = ScriptExecutionContext::getMainThreadScriptExecutionContext();
    bool onMainThread = mainContext && mainContext->isContextThread();
    for (;;) {
        std::optional<ScriptExecutionContextIdentifier> thread;
        bool grows = false;
        {
            Locker locker { m_lock };
            if (m_queue.size() <= m_startingCount + m_woken.size())
                return;
            // A thread that runs no goroutine, else a new thread, else a thread whose goroutines
            // all wait: a goroutine that computes keeps its thread to itself.
            auto unloaded = m_idle.reverseFindIf([&](auto thread) {
                return !m_load.contains(thread);
            });
            if (unloaded != notFound) {
                thread = m_idle[unloaded];
                m_idle.removeAt(unloaded);
            } else {
                grows = mayGrow == Grow::Yes && shouldGrow(onMainThread);
                // Off the main thread, a new thread waits for the main thread to create it, and the
                // idle thread may be this one, about to block: ask both, the first to take runs it.
                // The new thread is the one counted on, so what is queued next gets another.
                if ((!grows || !onMainThread) && !m_idle.isEmpty())
                    thread = m_idle.takeLast();
            }
            if (thread && !grows)
                m_woken.add(*thread);
        }
        if (grows)
            grow(onMainThread);
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
    bool isNew = m_threads.add(thread).isNewEntry;
    if (isNew) {
        ASSERT(m_startingCount);
        m_startingCount--;
    }
    m_woken.remove(thread);
    // What is queued for the threads that are starting is theirs. A started goroutine never moves,
    // so a thread that took it all as the pool grows would run it while the new threads idle. (A
    // woken thread gets no such claim: a thread that is running takes the next goroutine sooner.)
    ASSERT(m_startingCount >= m_uncreatedCount);
    size_t reserved = isNew ? 0 : m_startingCount - m_uncreatedCount;
    if (m_queue.size() <= reserved) {
        if (!m_idle.contains(thread))
            m_idle.append(thread);
        return std::nullopt;
    }
    auto [task, spawner] = m_queue.takeFirst();
    m_running.add(task.id, Running { thread, spawner });
    m_load.add(thread, 0).iterator->value++;
    return WTF::move(task);
}

std::optional<Spawner> Scheduler::finish(uint64_t id)
{
    Locker locker { m_lock };
    auto it = m_running.find(id);
    if (it == m_running.end())
        return std::nullopt;
    auto spawner = it->value.spawner;
    auto load = m_load.find(it->value.thread);
    if (--load->value == 0)
        m_load.remove(load);
    m_running.remove(it);
    return spawner;
}

void Scheduler::threadFailedToStart()
{
    {
        Locker locker { m_lock };
        m_threadCount--;
        m_startingCount--;
    }
    // Not grow: that would start (and fail to start) another thread from here.
    dispatch(Grow::No);
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
    m_load.remove(thread);
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

void Scheduler::grow(bool onMainThread)
{
    if (onMainThread) {
        startPoolThread(*ScriptExecutionContext::getMainThreadScriptExecutionContext());
        return;
    }
    bool posted = ScriptExecutionContext::postTaskTo(mainThreadContext, BunLoopKind::Regular, [](ScriptExecutionContext& context) {
        scheduler().willCreateThread();
        startPoolThread(context);
    });
    if (!posted) {
        willCreateThread();
        threadFailedToStart();
    }
}

void Scheduler::willCreateThread()
{
    Locker locker { m_lock };
    ASSERT(m_uncreatedCount);
    m_uncreatedCount--;
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
    values.append(args);
    if (task.source.isNull()) {
        values.append(jsNumber(0));
        values.append(jsString(vm, WTF::move(task.module)));
        values.append(jsString(vm, WTF::move(task.exportName)));
        values.append(jsBoolean(task.fromCommonJSExports));
    } else {
        if (task.sourceMap) {
            BunString path = Bun::toString(task.module);
            Bun__Go__adoptSourceMap(globalObject->bunVM(), &path, task.sourceMap.release());
        }
        values.append(jsNumber(1));
        values.append(jsString(vm, WTF::move(task.module)));
        values.append(jsString(vm, WTF::move(task.source)));
        values.append(jsNumber(task.line));
        values.append(jsNumber(task.column));
        values.append(jsBoolean(task.strict));
    }
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

// spawnInline(source, module, line, column, strict, args): queue a goroutine that runs a function
// no module exports, from its source (see sourceOf); returns its id. Throws (DataCloneError) if
// `args` can't be cloned.
JSC_DEFINE_HOST_FUNCTION(jsGoSpawnInline, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    String source = callFrame->argument(0).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    String module = callFrame->argument(1).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    int32_t line = callFrame->argument(2).toInt32(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    int32_t column = callFrame->argument(3).toInt32(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    bool strict = callFrame->argument(4).toBoolean(lexicalGlobalObject);
    auto args = serialize(lexicalGlobalObject, callFrame->argument(5));
    RETURN_IF_EXCEPTION(scope, {});
    auto& context = contextOf(lexicalGlobalObject);
    BunString path = Bun::toString(module);
    SourceMap sourceMap { Bun__Go__retainSourceMap(defaultGlobalObject(lexicalGlobalObject)->bunVM(), &path) };
    Task task { 0, WTF::move(module).isolatedCopy(), String(), false, WTF::move(args), WTF::move(source).isolatedCopy(), line, column, strict, WTF::move(sourceMap) };
    uint64_t id = scheduler().spawn(WTF::move(task), Spawner { context.identifier(), context.currentLoopKind() });
    return JSValue::encode(jsNumber(id));
}

// sourceOf(fn): [source, module, line, column, strict] of a function with source of its own, or
// undefined (native, bound). `line` and `column` are zero-based, where its text starts in `module`.
JSC_DEFINE_HOST_FUNCTION(jsGoSourceOf, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* function = dynamicDowncast<JSFunction>(callFrame->argument(0));
    if (!function || function->isHostOrBuiltinFunction())
        return JSValue::encode(jsUndefined());
    FunctionExecutable* executable = function->jsExecutable();
    JSString* source = executable->toString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    auto position = executable->source().provider()->documentZeroBasedLineColumnForOffset(executable->functionStart());
    MarkedArgumentBuffer values;
    values.append(source);
    values.append(jsString(vm, Zig::sourceURL(vm, function)));
    values.append(jsNumber(position.line));
    values.append(jsNumber(position.column));
    values.append(jsBoolean(executable->isInStrictContext()));
    RELEASE_AND_RETURN(scope, JSValue::encode(constructArray(lexicalGlobalObject, static_cast<ArrayAllocationProfile*>(nullptr), values)));
}

enum class Binding : uint8_t {
    // A global, or nothing
    Global,
    // A variable of the code around the function
    Captured,
    // What the function's module imports
    Imported,
};

struct BindingOf {
    Binding binding { Binding::Global };
    // Its value, if it can be read without running script (empty in its TDZ).
    JSValue value;
    // Imported: the specifier it is imported from, and the name it has there (empty for a namespace).
    String from;
    String importName;
};

// What `name` in `function` is, if the function itself does not declare it.
static BindingOf bindingOf(JSGlobalObject* globalObject, JSFunction* function, const Identifier& name)
{
    auto& vm = globalObject->vm();
    auto valueIn = [&](JSObject* object, PropertySlot::InternalMethodType method = PropertySlot::InternalMethodType::VMInquiry) -> JSValue {
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
        PropertySlot slot(object, method, &vm);
        bool found = object->getOwnPropertySlot(object, globalObject, name, slot);
        if (scope.exception()) {
            (void)scope.tryClearException();
            return {};
        }
        if (!found || !slot.isValue())
            return {};
        return slot.getValue(globalObject, name);
    };
    for (JSScope* scope = function->scope(); scope; scope = scope->next()) {
        if (scope->isGlobalObject())
            return { Binding::Global, valueIn(scope) };
        // A `with` statement's object may have any name.
        if (scope->type() == WithScopeType)
            return { Binding::Captured, valueIn(uncheckedDowncast<JSWithScope>(scope)->object()) };
        if (auto* variables = dynamicDowncast<JSSymbolTableObject>(scope); variables && !variables->symbolTable()->get(name.impl()).isNull()) {
            JSValue value;
            if (auto* environment = dynamicDowncast<JSLexicalEnvironment>(scope)) {
                PropertySlot slot(environment, PropertySlot::InternalMethodType::VMInquiry, &vm);
                if (symbolTableGet(environment, name, slot) && slot.isValue())
                    value = slot.getValue(globalObject, name);
                if (!value)
                    value = valueIn(environment, PropertySlot::InternalMethodType::Get);
            }
            return { Binding::Captured, value };
        }
        if (auto* module = dynamicDowncast<JSModuleEnvironment>(scope)) {
            auto& imports = module->moduleRecord()->importEntries();
            auto it = imports.find(RefPtr<UniquedStringImpl> { name.impl() });
            if (it != imports.end()) {
                auto& entry = it->value;
                String importName = entry.type == AbstractModuleRecord::ImportEntryType::Namespace ? String() : String(entry.importName.impl());
                JSValue value;
                {
                    auto catchScope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
                    auto resolution = module->moduleRecord()->resolveImport(globalObject, name);
                    if (catchScope.exception())
                        (void)catchScope.tryClearException();
                    else if (resolution.type == AbstractModuleRecord::Resolution::Type::Resolved && resolution.moduleRecord) {
                        if (auto* environment = resolution.moduleRecord->moduleEnvironmentMayBeNull()) {
                            PropertySlot slot(environment, PropertySlot::InternalMethodType::VMInquiry, &vm);
                            if (symbolTableGet(environment, resolution.localName, slot) && slot.isValue())
                                value = slot.getValue(globalObject, resolution.localName);
                        }
                        // What the module environment does not hold yet (a function declaration), its namespace has.
                        if (!value && entry.type != AbstractModuleRecord::ImportEntryType::Namespace) {
                            auto* exports = resolution.moduleRecord->getModuleNamespace(globalObject, false);
                            if (exports && !catchScope.exception())
                                value = exports->get(globalObject, entry.importName);
                        }
                        if (catchScope.exception()) {
                            (void)catchScope.tryClearException();
                            value = {};
                        }
                    }
                }
                return { Binding::Imported, value, String(entry.moduleRequest.impl()), WTF::move(importName) };
            }
        }
    }
    return {};
}

// Whether `value` is the `require` Bun gives the code of `module` (a CommonJS module's, or an ES
// module's `import.meta.require`): the pool thread gives the function one of the same module's.
static bool isModuleRequire(Zig::GlobalObject* globalObject, JSValue value, const String& module)
{
    if (!value)
        return false;
    auto* bound = dynamicDowncast<JSBoundFunction>(value);
    if (!bound || bound->targetFunction() != globalObject->requireFunctionUnbound())
        return false;
    auto* requirer = dynamicDowncast<Bun::JSCommonJSModule>(bound->boundThis());
    if (!requirer)
        return false;
    JSValue filename = requirer->filename();
    return filename.isString() && filename.getString(globalObject) == module;
}

// What a captured or imported value is, for the advice of the error: "function", "class",
// "namespace" (of a module), "data", or "bun" for what the Bun global has under the same name
// (`import { sleep } from "bun"`).
static ASCIILiteral valueKindOf(JSGlobalObject* globalObject, const String& name, JSValue value)
{
    if (!value)
        return ""_s;
    if (value.isObject()) {
        auto& vm = globalObject->vm();
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
        JSValue bun = globalObject->get(globalObject, Identifier::fromString(vm, "Bun"_s));
        JSValue same = !scope.exception() && bun.isObject() ? asObject(bun)->getIfPropertyExists(globalObject, Identifier::fromString(vm, name)) : JSValue();
        if (scope.exception())
            (void)scope.tryClearException();
        else if (same == value)
            return "bun"_s;
    }
    if (auto* function = dynamicDowncast<JSFunction>(value); function && !function->isHostOrBuiltinFunction() && function->jsExecutable()->isClassConstructorFunction())
        return "class"_s;
    if (value.inherits<JSModuleNamespaceObject>())
        return "namespace"_s;
    if (value.isCallable())
        return "function"_s;
    return "data"_s;
}

// check(fn, source, module, line, column): what keeps `fn` from running on another thread, or
// undefined. Its source starts at zero-based `line` and `column` of `module` (sourceOf), which Bun
// may have transpiled. Each problem is [kind, name, line, valueKind, from, importName], with a
// one-based line of the module's own text. `kind` is "unparsable" (a method, a getter), "class",
// "this", "arguments" (an arrow function's, from the code around it), "import.meta", "eval" (called
// directly), "captured" (a variable of the code around it), "imported" (by its module) or
// "undeclared" (not a global either). `valueKind` (valueKindOf) is what a captured or imported
// binding holds; `from` and `importName` are where an import comes from.
JSC_DEFINE_HOST_FUNCTION(jsGoCheck, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* function = dynamicDowncast<JSFunction>(callFrame->argument(0));
    if (!function || function->isHostOrBuiltinFunction())
        return throwVMTypeError(lexicalGlobalObject, scope);
    String source = callFrame->argument(1).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    String module = callFrame->argument(2).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    int32_t firstLine = callFrame->argument(3).toInt32(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    int32_t firstColumn = callFrame->argument(4).toInt32(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});

    using Names = Vector<std::pair<String, NamePosition>>;
    Names names;
    FunctionNames found {};
    auto text = makeString('(', source, ')').utf8();
    bool parsed = Bun__Go__functionNames(text.data(), text.length(), &names, [](void* context, const uint8_t* name, size_t length, NamePosition position) { static_cast<Names*>(context)->append({ String::fromUTF8(std::span { name, length }), position }); }, &found);

    // Where a position of the text is in the module's own text, which Bun transpiled (TypeScript,
    // JSX). The text begins with the "(" around the source.
    auto original = [&](NamePosition position) {
        Bun::OwnedZigStackFrames frames(1);
        frames[0].position.line_zero_based = firstLine + std::max(position.line, 0);
        frames[0].position.column_zero_based = position.line > 0 ? position.column : firstColumn + std::max(position.column - 1, 0);
        frames[0].source_url = Bun::toStringRef(module);
        frames.remap(defaultGlobalObject(lexicalGlobalObject)->bunVM());
        return NamePosition { frames[0].position.line_zero_based, frames[0].position.column_zero_based };
    };
    MarkedArgumentBuffer problems;
    // Check for an exception after each.
    auto add = [&](ASCIILiteral kind, const String& name, NamePosition position, const BindingOf& found = {}) {
        MarkedArgumentBuffer problem;
        problem.append(jsString(vm, String(kind)));
        problem.append(jsString(vm, name));
        problem.append(jsNumber(original(position).line + 1));
        problem.append(jsString(vm, String(valueKindOf(lexicalGlobalObject, name, found.value))));
        problem.append(found.from.isNull() ? jsUndefined() : jsString(vm, found.from));
        problem.append(found.importName.isNull() ? jsUndefined() : jsString(vm, found.importName));
        if (auto* array = constructArray(lexicalGlobalObject, static_cast<ArrayAllocationProfile*>(nullptr), problem))
            problems.append(array);
    };
    auto kind = static_cast<FunctionTextKind>(found.kind);
    if (!parsed || kind == FunctionTextKind::Other) {
        add("unparsable"_s, emptyString(), {});
        RETURN_IF_EXCEPTION(scope, {});
    } else if (kind == FunctionTextKind::Class || function->jsExecutable()->isClassConstructorFunction()) {
        add("class"_s, emptyString(), {});
        RETURN_IF_EXCEPTION(scope, {});
    } else {
        if (found.thisPosition.line >= 0) {
            add("this"_s, "this"_s, found.thisPosition);
            RETURN_IF_EXCEPTION(scope, {});
        }
        if (found.importMetaPosition.line >= 0) {
            add("import.meta"_s, "import.meta"_s, found.importMetaPosition);
            RETURN_IF_EXCEPTION(scope, {});
        }
        for (auto& [name, position] : names) {
            if ((kind == FunctionTextKind::Arrow && name == "arguments"_s) || (found.directEval && name == "eval"_s)) {
                add(name == "eval"_s ? "eval"_s : "arguments"_s, name, position);
                RETURN_IF_EXCEPTION(scope, {});
                continue;
            }
            auto identifier = Identifier::fromString(vm, name);
            auto found = bindingOf(lexicalGlobalObject, function, identifier);
            // The thread gives the function a `require` of its module's (internal/go/runtime), so
            // only that one may be used; a `require` of the code's own is captured like any other.
            if (name == "require"_s && isModuleRequire(defaultGlobalObject(lexicalGlobalObject), found.value, module))
                continue;
            if (found.binding != Binding::Global) {
                add(found.binding == Binding::Imported ? "imported"_s : "captured"_s, name, position, found);
                RETURN_IF_EXCEPTION(scope, {});
                continue;
            }
            bool isGlobal = lexicalGlobalObject->hasProperty(lexicalGlobalObject, identifier);
            RETURN_IF_EXCEPTION(scope, {});
            if (!isGlobal) {
                add("undeclared"_s, name, position);
                RETURN_IF_EXCEPTION(scope, {});
            }
        }
    }
    if (problems.isEmpty())
        return JSValue::encode(jsUndefined());
    RELEASE_AND_RETURN(scope, JSValue::encode(constructArray(lexicalGlobalObject, static_cast<ArrayAllocationProfile*>(nullptr), problems)));
}

// hasModuleRequire(fn, module): whether `require` in `fn` is the module's own, which the goroutine's
// thread gives it. What check() found of the code that made `fn` holds for another closure of it,
// except this: the binding may hold another value there, or since.
JSC_DEFINE_HOST_FUNCTION(jsGoHasModuleRequire, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* function = dynamicDowncast<JSFunction>(callFrame->argument(0));
    if (!function || function->isHostOrBuiltinFunction())
        return JSValue::encode(jsBoolean(false));
    String module = callFrame->argument(1).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    auto found = bindingOf(lexicalGlobalObject, function, Identifier::fromString(vm, "require"_s));
    return JSValue::encode(jsBoolean(isModuleRequire(defaultGlobalObject(lexicalGlobalObject), found.value, module)));
}

// evaluating(module): whether this thread has started to evaluate the ES module `module` (a module
// key) and not finished, as when its top-level await is pending.
JSC_DEFINE_HOST_FUNCTION(jsGoEvaluating, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    String module = callFrame->argument(0).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    auto* loader = lexicalGlobalObject->moduleLoader();
    auto* entry = loader ? loader->registryEntry(Identifier::fromString(vm, module)) : nullptr;
    auto* record = entry ? dynamicDowncast<CyclicModuleRecord>(entry->record()) : nullptr;
    bool evaluating = record && (record->status() == CyclicModuleRecord::Status::Evaluating || record->status() == CyclicModuleRecord::Status::EvaluatingAsync);
    return JSValue::encode(jsBoolean(evaluating));
}

// describe(value): [path, what] of what in `value` can't be cloned (Bun::findUncloneable), or
// undefined: for the DataCloneError of an argument or a result.
JSC_DEFINE_HOST_FUNCTION(jsGoDescribe, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto found = Bun::findUncloneable(lexicalGlobalObject, callFrame->argument(0));
    RETURN_IF_EXCEPTION(scope, {});
    if (!found)
        return JSValue::encode(jsUndefined());
    MarkedArgumentBuffer values;
    values.append(jsString(vm, found->path));
    values.append(jsString(vm, found->what));
    RELEASE_AND_RETURN(scope, JSValue::encode(constructArray(lexicalGlobalObject, static_cast<ArrayAllocationProfile*>(nullptr), values)));
}

// codeGenerationAllowed(): whether script may be made from a string here, which an inline
// goroutine's thread does with its source.
JSC_DEFINE_HOST_FUNCTION(jsGoCodeGenerationAllowed, (JSGlobalObject * lexicalGlobalObject, CallFrame*))
{
    return JSValue::encode(jsBoolean(lexicalGlobalObject->evalEnabled() && !Bun::isCodeGenerationFromStringsStrict()));
}

// compile(source, module, line, column, strict): `(require) => fn`, where fn is the function
// `source` is (spawnInline), compiled as if it were where it is in `module`: an import() in it
// resolves from there, and a stack trace names it. `line` and `column` are in the text Bun
// transpiled `module` to; the module's source map, which came with the goroutine, maps them to the
// module's own text. Throws what compiling throws.
JSC_DEFINE_HOST_FUNCTION(jsGoCompile, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    auto& vm = lexicalGlobalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    String source = callFrame->argument(0).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    String module = callFrame->argument(1).toWTFString(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    int32_t line = callFrame->argument(2).toInt32(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    int32_t column = callFrame->argument(3).toInt32(lexicalGlobalObject);
    RETURN_IF_EXCEPTION(scope, {});
    bool strict = callFrame->argument(4).toBoolean(lexicalGlobalObject);

    auto prefix = strict ? "(function (require) { 'use strict'; return ("_s : "(function (require) { return ("_s;
    int32_t prefixLength = static_cast<int32_t>(prefix.length());
    line = std::max(line, 0);
    column = std::max(column, 0);
    // The text starts at `column` of `line`. The prefix goes before it on that line, or, if it is
    // longer than `column`, on the line before, so that the text's columns stay what they are.
    String code;
    TextPosition position;
    if (column < prefixLength && line > 0) {
        StringBuilder builder;
        builder.append(prefix, '\n');
        for (int32_t i = 0; i < column; i++)
            builder.append(' ');
        builder.append(source, ")})"_s);
        code = builder.toString();
        position = { OrdinalNumber::fromZeroBasedInt(line - 1), OrdinalNumber::fromZeroBasedInt(0) };
    } else {
        code = makeString(prefix, source, ")})"_s);
        position = { OrdinalNumber::fromZeroBasedInt(line), OrdinalNumber::fromZeroBasedInt(std::max(column - prefixLength, 0)) };
    }
    URL url;
    if (!module.isEmpty() && module[0] == '/')
        url = URL::fileURLWithFileSystemPath(module);
#if OS(WINDOWS)
    else if (module.length() > 2 && module[1] == ':')
        url = URL::fileURLWithFileSystemPath(module);
#endif
    else
        url = URL({}, module);
    SourceCode sourceCode { StringSourceProvider::create(code, url.isValid() ? SourceOrigin { url } : SourceOrigin {}, String(module), SourceTaintedOrigin::Untainted, position) };
    NakedPtr<JSC::Exception> exception;
    JSValue value = JSC::evaluate(lexicalGlobalObject, sourceCode, lexicalGlobalObject, exception);
    if (exception) {
        throwException(lexicalGlobalObject, scope, exception.get());
        return {};
    }
    RETURN_IF_EXCEPTION(scope, {});
    return JSValue::encode(value);
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

// moduleOf(fn): [sourceURL, namespace | undefined, bundled] of the module whose source defines `fn`,
// or undefined for a function with no source of its own (native, bound). The namespace is there for
// an ES module that has started evaluating; a CommonJS module is found in the require map.
// `bundled`: the source is `bun build` output (it starts with the `// @bun` pragma).
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
    bool bundled = false;
    if (auto* provider = function->jsExecutable()->source().provider())
        bundled = provider->source().startsWith("// @bun"_s);
    MarkedArgumentBuffer values;
    values.append(jsString(vm, url));
    values.append(ns);
    values.append(jsBoolean(bundled));
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

// run(job, fn, args, settled): job(fn, args, settled) on a microtask of its own. `job` is the
// binding's `job`, the private builtin runGoroutine (builtins/Goroutine.ts): no frame of the pool's
// runtime is under the goroutine's.
JSC_DEFINE_HOST_FUNCTION(jsGoRun, (JSGlobalObject * lexicalGlobalObject, CallFrame* callFrame))
{
    JSC::QueuedTask task { nullptr, JSC::InternalMicrotask::BunInvokeJobWithArguments, 0, lexicalGlobalObject, callFrame->argument(0), callFrame->argument(1), callFrame->argument(2), callFrame->argument(3) };
    lexicalGlobalObject->vm().queueMicrotask(WTF::move(task));
    return JSValue::encode(jsUndefined());
}

// take(): the next goroutine for this pool thread (internal/go/runtime's `Task`), or undefined.
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
    JSObject* binding = constructEmptyObject(globalObject, globalObject->objectPrototype(), 11);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "spawn"_s), 4, Bun::Go::jsGoSpawn, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "spawnInline"_s), 6, Bun::Go::jsGoSpawnInline, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "sourceOf"_s), 1, Bun::Go::jsGoSourceOf, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "check"_s), 5, Bun::Go::jsGoCheck, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "codeGenerationAllowed"_s), 0, Bun::Go::jsGoCodeGenerationAllowed, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "keepAlive"_s), 1, Bun::Go::jsGoKeepAlive, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "moduleOf"_s), 1, Bun::Go::jsGoModuleOf, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "resolveModule"_s), 1, Bun::Go::jsGoResolveModule, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "evaluating"_s), 1, Bun::Go::jsGoEvaluating, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "describe"_s), 1, Bun::Go::jsGoDescribe, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "hasModuleRequire"_s), 2, Bun::Go::jsGoHasModuleRequire, ImplementationVisibility::Public, NoIntrinsic, 0);
    RELEASE_AND_RETURN(scope, binding);
}

JSValue createGoRuntimeBinding(Zig::GlobalObject* globalObject)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    JSObject* binding = constructEmptyObject(globalObject, globalObject->objectPrototype(), 6);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "run"_s), 4, Bun::Go::jsGoRun, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirect(vm, Identifier::fromString(vm, "job"_s), JSFunction::create(vm, globalObject, goroutineRunGoroutineCodeGenerator(vm), globalObject), 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "take"_s), 0, Bun::Go::jsGoTake, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "describe"_s), 1, Bun::Go::jsGoDescribe, ImplementationVisibility::Public, NoIntrinsic, 0);
    binding->putDirectNativeFunction(vm, globalObject, Identifier::fromString(vm, "compile"_s), 5, Bun::Go::jsGoCompile, ImplementationVisibility::Public, NoIntrinsic, 0);
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

// process.exit() in a goroutine ends the program with `code`, as os.Exit does in Go: the main thread
// calls process.exit(code). Its thread stops running script right away (WebWorker::exit); this task
// is posted before that thread retires, so it reaches the main thread ahead of the rejections of
// the goroutines that were still running there.
extern "C" void Bun__Go__exitProcess(uint8_t code)
{
    ScriptExecutionContext::postTaskTo(Bun::Go::mainThreadContext, BunLoopKind::Regular, [code](ScriptExecutionContext& context) {
        auto* globalObject = defaultGlobalObject(context.jsGlobalObject());
        auto& vm = globalObject->vm();
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
        auto* process = globalObject->processObject();
        JSValue exit = process->get(globalObject, Identifier::fromString(vm, "exit"_s));
        if (!scope.exception()) {
            MarkedArgumentBuffer args;
            args.append(jsNumber(code));
            JSC::call(globalObject, exit, process, args, "process.exit is not a function"_s);
        }
        if (JSC::Exception* exception = scope.exception(); exception && !vm.isTerminationException(exception)) {
            (void)scope.tryClearException();
            Bun__reportUnhandledError(globalObject, JSValue::encode(exception->value()));
        }
    });
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
