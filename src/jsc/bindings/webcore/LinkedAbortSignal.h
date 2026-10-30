#pragma once

#include "root.h"

namespace WebCore {
class AbortSignal;
}

// An AbortSignal in a Bun.go argument or a Bun.Channel value (SerializationContext::Channel): the
// clone is a signal on the receiving thread that aborts, with a clone of the reason, when the
// original does. A clone is an id that keeps the link until `releaseStructuredClone`.
namespace Bun::LinkedAbortSignal {

// The signal's thread. Never throws: a reason that can't be cloned arrives as an AbortError.
uint64_t retainForStructuredClone(JSC::JSGlobalObject*, WebCore::AbortSignal&);
// The receiving thread. Empty if no clone in flight has this id, or it was read already.
JSC::JSValue fromStructuredClone(JSC::JSGlobalObject*, uint64_t id);
// Any thread.
void releaseStructuredClone(uint64_t id);
// The linked signal's thread (AbortSignal::observeLinkedAbort): if its original has aborted, it
// aborts now. Exceptions of its listeners are reported; a termination stays pending.
void observe(WebCore::AbortSignal&);

}

namespace Bun {
JSC_DECLARE_HOST_FUNCTION(linkedAbortSignalsForTesting);
}
