// An AbortSignal sent to another thread (LinkedAbortSignal.h).
//
// A signal that has been sent has one Source, which knows a Link for each clone. Reading a clone
// makes a signal on the reading thread and points the Link at it. When the original aborts, each
// Link gets its own clone of the reason, posted to its thread; a Link not read yet keeps it for the
// read. When the original is destroyed without aborting, the signals linked to it stop waiting, so
// one with abort listeners can be collected. A linked signal that is collected, or a clone that is
// dropped unread, leaves its Source. No lock is held while script runs or a task is posted.

#include "root.h"
#include "LinkedAbortSignal.h"

#include "AbortSignal.h"
#include "JSAbortSignal.h"
#include "MessageWithMessagePorts.h"
#include "ScriptExecutionContext.h"
#include "ZigGlobalObject.h"
#include <wtf/HashMap.h>
#include <wtf/HashSet.h>
#include <wtf/Lock.h>
#include <wtf/NeverDestroyed.h>
#include <wtf/ThreadSafeRefCounted.h>

// ChannelMessage.cpp
extern "C" WebCore::MessageWithMessagePorts* Bun__ChannelMessage__create(JSC::JSGlobalObject*, JSC::EncodedJSValue value, JSC::EncodedJSValue transfer);
extern "C" JSC::EncodedJSValue Bun__ChannelMessage__deserialize(JSC::JSGlobalObject*, WebCore::MessageWithMessagePorts*);

namespace Bun::LinkedAbortSignal {
using namespace JSC;
using namespace WebCore;

namespace {

// A clone of the reason; null if it could not be cloned (it arrives as an AbortError).
using Reason = std::unique_ptr<MessageWithMessagePorts>;

class Link;

std::atomic<uint32_t> liveLinks { 0 };

class Source : public ThreadSafeRefCounted<Source> {
public:
    static Ref<Source> create(JSGlobalObject* globalObject) { return adoptRef(*new Source(globalObject)); }

    // False if the original has aborted or is gone already.
    bool add(Link&);
    void remove(Link&);

    // The original's thread.
    void abort(JSValue reason);
    void gone();

private:
    explicit Source(JSGlobalObject* globalObject)
        : m_globalObject(globalObject)
    {
    }

    Vector<Ref<Link>> settle();

    // The original's thread only. It outlives the signal.
    JSGlobalObject* m_globalObject;
    Lock m_lock;
    bool m_settled WTF_GUARDED_BY_LOCK(m_lock) { false };
    HashSet<RefPtr<Link>> m_links WTF_GUARDED_BY_LOCK(m_lock);
};

class Link : public ThreadSafeRefCounted<Link> {
public:
    enum class State : uint8_t {
        // Not read. The original may still abort.
        Waiting,
        // Not read. The original aborted, with `m_reason`.
        Aborted,
        // Not read. The original is gone.
        Gone,
        // Read: a signal on `m_context` follows the original.
        Linked,
        // Nothing follows the original through this link any more.
        Done,
    };

    static Ref<Link> create(RefPtr<Source>&& source, State state, Reason&& reason = nullptr)
    {
        return adoptRef(*new Link(WTF::move(source), state, WTF::move(reason)));
    }

    // The original's thread.
    void abort(Reason&&);
    void gone();

    // The reading thread. Empty if it was read already.
    JSValue read(Zig::GlobalObject*);
    // The clone is dropped.
    void release();
    // The linked signal's thread: it aborted, or is being destroyed.
    void detach();

private:
    Link(RefPtr<Source>&& source, State state, Reason&& reason)
        : m_source(WTF::move(source))
        , m_state(state)
        , m_reason(WTF::move(reason))
    {
        liveLinks.fetch_add(1, std::memory_order_relaxed);
    }

public:
    ~Link() { liveLinks.fetch_sub(1, std::memory_order_relaxed); }

private:

    // The linked signal's thread.
    void abortSignal(ScriptExecutionContext&, Reason&&);
    void unlink();

    const RefPtr<Source> m_source;
    Lock m_lock;
    State m_state WTF_GUARDED_BY_LOCK(m_lock);
    Reason m_reason WTF_GUARDED_BY_LOCK(m_lock);
    ScriptExecutionContextIdentifier m_context WTF_GUARDED_BY_LOCK(m_lock) {};
    // m_context's thread only: the linked signal, and the algorithm it keeps this link alive with.
    AbortSignal* m_signal { nullptr };
    uint32_t m_algorithm { 0 };
};

// Lives in the algorithms of a linked signal: it goes when the signal aborts or is destroyed.
struct LinkedSignalGuard {
    explicit LinkedSignalGuard(Ref<Link>&& link)
        : link(WTF::move(link))
    {
    }
    LinkedSignalGuard(LinkedSignalGuard&&) = default;
    ~LinkedSignalGuard();
    RefPtr<Link> link;
};

// Lives in the algorithms of a signal that was sent: it goes when the signal aborts or is destroyed.
struct SourceGuard {
    SourceGuard(AbortSignal& signal, Ref<Source>&& source)
        : signal(&signal)
        , source(WTF::move(source))
    {
    }
    SourceGuard(SourceGuard&&) = default;
    ~SourceGuard();
    AbortSignal* signal;
    RefPtr<Source> source;
};

struct Sources {
    Lock lock;
    // A signal's Source, while it has one.
    HashMap<AbortSignal*, Ref<Source>> bySignal WTF_GUARDED_BY_LOCK(lock);
};

Sources& sources()
{
    static NeverDestroyed<Sources> instance;
    return instance;
}

struct Clones {
    Lock lock;
    HashMap<uint64_t, RefPtr<Link>> byId WTF_GUARDED_BY_LOCK(lock);
    uint64_t nextId WTF_GUARDED_BY_LOCK(lock) { 1 };
};

Clones& clones()
{
    static NeverDestroyed<Clones> instance;
    return instance;
}

LinkedSignalGuard::~LinkedSignalGuard()
{
    if (link)
        link->detach();
}

SourceGuard::~SourceGuard()
{
    if (!source)
        return;
    {
        auto& all = sources();
        Locker locker { all.lock };
        all.bySignal.remove(signal);
    }
    source->gone();
}

Reason cloneReason(JSGlobalObject* globalObject, JSValue reason)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    Reason clone { Bun__ChannelMessage__create(globalObject, JSValue::encode(reason), JSValue::encode(jsUndefined())) };
    if (JSC::Exception* exception = scope.exception(); exception && !vm.isTerminationException(exception))
        (void)scope.tryClearException();
    return clone;
}

JSValue readReason(Zig::GlobalObject* globalObject, Reason&& reason, bool& terminated)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    if (reason) {
        JSValue value = JSValue::decode(Bun__ChannelMessage__deserialize(globalObject, reason.release()));
        if (JSC::Exception* exception = scope.exception()) {
            if (vm.isTerminationException(exception)) {
                terminated = true;
                return {};
            }
            (void)scope.tryClearException();
        } else {
            return value;
        }
    }
    return toJS(globalObject, CommonAbortReason::UserAbort);
}

bool Source::add(Link& link)
{
    Locker locker { m_lock };
    if (m_settled)
        return false;
    m_links.add(&link);
    return true;
}

void Source::remove(Link& link)
{
    Locker locker { m_lock };
    m_links.remove(&link);
}

Vector<Ref<Link>> Source::settle()
{
    Locker locker { m_lock };
    if (m_settled)
        return {};
    m_settled = true;
    Vector<Ref<Link>> links;
    links.reserveInitialCapacity(m_links.size());
    for (auto& link : m_links)
        links.append(*link);
    m_links.clear();
    return links;
}

void Source::abort(JSValue reason)
{
    for (auto& link : settle())
        link->abort(cloneReason(m_globalObject, reason));
}

void Source::gone()
{
    for (auto& link : settle())
        link->gone();
}

void Link::abort(Reason&& reason)
{
    ScriptExecutionContextIdentifier context;
    {
        Locker locker { m_lock };
        if (m_state == State::Waiting) {
            m_state = State::Aborted;
            m_reason = WTF::move(reason);
            return;
        }
        if (m_state != State::Linked)
            return;
        m_state = State::Done;
        context = m_context;
    }
    ScriptExecutionContext::postTaskTo(context, BunLoopKind::Regular, [link = Ref { *this }, reason = WTF::move(reason)](ScriptExecutionContext& context) mutable {
        link->abortSignal(context, WTF::move(reason));
    });
}

void Link::gone()
{
    ScriptExecutionContextIdentifier context;
    {
        Locker locker { m_lock };
        if (m_state == State::Waiting) {
            m_state = State::Gone;
            return;
        }
        if (m_state != State::Linked)
            return;
        m_state = State::Done;
        context = m_context;
    }
    ScriptExecutionContext::postTaskTo(context, BunLoopKind::Regular, [link = Ref { *this }](ScriptExecutionContext&) {
        link->unlink();
    });
}

void Link::abortSignal(ScriptExecutionContext& context, Reason&& reason)
{
    RefPtr signal = m_signal;
    if (!signal)
        return;
    auto* globalObject = defaultGlobalObject(context.jsGlobalObject());
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    bool terminated = false;
    JSValue value = readReason(globalObject, WTF::move(reason), terminated);
    if (terminated)
        return;
    signal->signalAbort(value);
    signal->decrementPendingActivityCount();
    if (JSC::Exception* exception = scope.exception(); exception && !vm.isTerminationException(exception)) {
        (void)scope.tryClearException();
        Bun__reportUnhandledError(globalObject, JSValue::encode(exception->value()));
    }
}

void Link::unlink()
{
    RefPtr signal = m_signal;
    if (!signal)
        return;
    // Destroys the guard, which detaches this link.
    signal->removeAlgorithm(m_algorithm);
    signal->decrementPendingActivityCount();
}

void Link::detach()
{
    m_signal = nullptr;
    {
        Locker locker { m_lock };
        m_state = State::Done;
    }
    if (m_source)
        m_source->remove(*this);
}

void Link::release()
{
    Reason reason;
    {
        Locker locker { m_lock };
        if (m_state == State::Linked || m_state == State::Done)
            return;
        m_state = State::Done;
        reason = WTF::move(m_reason);
    }
    if (m_source)
        m_source->remove(*this);
}

JSValue Link::read(Zig::GlobalObject* globalObject)
{
    auto& context = *globalObject->scriptExecutionContext();
    State state;
    Reason reason;
    {
        Locker locker { m_lock };
        state = m_state;
        if (state == State::Waiting) {
            m_state = State::Linked;
            m_context = context.identifier();
        } else if (state == State::Aborted || state == State::Gone) {
            m_state = State::Done;
            reason = WTF::move(m_reason);
        }
    }
    switch (state) {
    case State::Waiting: {
        auto signal = AbortSignal::create(&context);
        m_signal = signal.ptr();
        // While the original can abort, abort listeners keep the signal (JSAbortSignalOwner).
        signal->incrementPendingActivityCount();
        m_algorithm = signal->addAlgorithm([guard = LinkedSignalGuard(Ref { *this })](JSValue) {});
        return toJSNewlyCreated(globalObject, globalObject, WTF::move(signal));
    }
    case State::Aborted: {
        bool terminated = false;
        JSValue value = readReason(globalObject, WTF::move(reason), terminated);
        if (terminated)
            return {};
        return toJSNewlyCreated(globalObject, globalObject, AbortSignal::abort(context, value));
    }
    case State::Gone:
        return toJSNewlyCreated(globalObject, globalObject, AbortSignal::create(&context));
    case State::Linked:
    case State::Done:
        break;
    }
    return {};
}

// The original's thread.
Ref<Source> sourceOf(JSGlobalObject* globalObject, AbortSignal& signal)
{
    auto& all = sources();
    {
        Locker locker { all.lock };
        if (auto it = all.bySignal.find(&signal); it != all.bySignal.end())
            return it->value.copyRef();
    }
    auto source = Source::create(globalObject);
    {
        Locker locker { all.lock };
        all.bySignal.add(&signal, source.copyRef());
    }
    signal.addNativeCallback({ source.ptr(), [](void* source, EncodedJSValue reason) {
                                  static_cast<Source*>(source)->abort(JSValue::decode(reason));
                              } });
    signal.addAlgorithm([guard = SourceGuard(signal, source.copyRef())](JSValue) {});
    return source;
}

} // namespace

uint64_t retainForStructuredClone(JSGlobalObject* globalObject, AbortSignal& signal)
{
    RefPtr<Link> link;
    if (signal.aborted()) {
        link = Link::create(nullptr, Link::State::Aborted, cloneReason(globalObject, signal.jsReason(*globalObject)));
    } else {
        auto source = sourceOf(globalObject, signal);
        link = Link::create(source.copyRef(), Link::State::Waiting);
        if (!source->add(*link))
            link = Link::create(nullptr, Link::State::Gone);
    }
    auto& all = clones();
    Locker locker { all.lock };
    uint64_t id = all.nextId++;
    all.byId.add(id, WTF::move(link));
    return id;
}

JSValue fromStructuredClone(JSGlobalObject* globalObject, uint64_t id)
{
    RefPtr<Link> link;
    {
        auto& all = clones();
        Locker locker { all.lock };
        link = all.byId.get(id);
    }
    if (!link)
        return {};
    return link->read(defaultGlobalObject(globalObject));
}

void releaseStructuredClone(uint64_t id)
{
    RefPtr<Link> link;
    {
        auto& all = clones();
        Locker locker { all.lock };
        link = all.byId.take(id);
    }
    if (link)
        link->release();
}

}

namespace Bun {

// bun:internal-for-testing: how many links between AbortSignals and their clones are alive.
JSC_DEFINE_HOST_FUNCTION(linkedAbortSignalsForTesting, (JSC::JSGlobalObject*, JSC::CallFrame*))
{
    return JSC::JSValue::encode(JSC::jsNumber(LinkedAbortSignal::liveLinks.load(std::memory_order_relaxed)));
}

}
