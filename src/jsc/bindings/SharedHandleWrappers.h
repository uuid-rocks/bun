#pragma once

// A realm's wrapper of each Bun.Channel and Bun.Mutex, by the address of the native core it shares,
// while the wrapper is alive (ChannelMessage.cpp): a channel that comes back to a thread is the
// same object. Not a JSC::WeakGCMap: an eden collection leaves its dead entries until a full one,
// and goroutines that make channels in a loop see few full collections. This one drops them
// whenever it doubles.

#include "root.h"
#include <JavaScriptCore/Weak.h>
#include <JavaScriptCore/WeakInlines.h>
#include <wtf/HashMap.h>

namespace Bun {

class SharedHandleWrappers {
    WTF_DEPRECATED_MAKE_FAST_ALLOCATED(SharedHandleWrappers);

public:
    JSC::JSObject* get(uintptr_t address) const
    {
        auto it = m_wrappers.find(address);
        return it == m_wrappers.end() ? nullptr : it->value.get();
    }

    void set(uintptr_t address, JSC::JSObject* wrapper)
    {
        m_wrappers.set(address, JSC::Weak<JSC::JSObject>(wrapper));
        if (m_wrappers.size() < m_pruneAt)
            return;
        m_wrappers.removeIf([](auto& entry) { return !entry.value.get(); });
        m_pruneAt = std::max<size_t>(minimumPruneAt, m_wrappers.size() * 2);
    }

private:
    static constexpr size_t minimumPruneAt = 64;
    HashMap<uintptr_t, JSC::Weak<JSC::JSObject>> m_wrappers;
    size_t m_pruneAt { minimumPruneAt };
};

} // namespace Bun
