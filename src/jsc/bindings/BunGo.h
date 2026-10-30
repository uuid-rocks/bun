#pragma once

#include "root.h"
#include "ScriptExecutionContext.h"

namespace Zig {
class GlobalObject;
}

namespace Bun::Go {

// The pool thread of `context` has ended (main thread): its goroutines that had not settled are
// rejected, and the pool may grow again.
void poolThreadDidExit(WebCore::ScriptExecutionContextIdentifier context);

}

namespace WebCore {

// internal/go/spawn: Bun.go on any JS thread.
JSC::JSValue createGoSpawnBinding(Zig::GlobalObject*);
// internal/go/runtime: the loop of a pool thread.
JSC::JSValue createGoRuntimeBinding(Zig::GlobalObject*);

}
