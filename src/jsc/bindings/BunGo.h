#pragma once

#include "root.h"
#include "ScriptExecutionContext.h"

namespace Zig {
class GlobalObject;
}

namespace Bun::Go {

// Main thread: the pool thread of `context` was asked to stop, or has ended. Its goroutines that
// had not settled are rejected, and the pool may grow again.
void poolThreadWillExit(WebCore::ScriptExecutionContextIdentifier context);
void poolThreadDidExit(WebCore::ScriptExecutionContextIdentifier context);
// The thread of a Worker (not the pool's) is ending. The goroutines it started, and the ones they
// started, are cancelled: those not started yet are dropped, and the channel, select and mutex
// operations of those running reject, as does every one they start. Idempotent.
void ownerWillExit(WebCore::ScriptExecutionContextIdentifier context);

}

namespace WebCore {

// internal/go/spawn: Bun.go on any JS thread.
JSC::JSValue createGoSpawnBinding(Zig::GlobalObject*);
// internal/go/runtime: the loop of a pool thread.
JSC::JSValue createGoRuntimeBinding(Zig::GlobalObject*);

}
