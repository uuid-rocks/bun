#pragma once

#include "root.h"

namespace Bun {

// What in `value` a Bun.go or Bun.Channel clone (ChannelMessage.cpp) can't clone: the path to it
// from `value` (".cb", "[2].handler", or "" for `value` itself), what it is ("a function"), and for
// an object that belongs to its thread (a WaitGroup), what to do instead. Runs the getters that the
// clone runs; leaves no exception pending.
struct Uncloneable {
    String path;
    String what;
    String hint;
};
std::optional<Uncloneable> findUncloneable(JSC::JSGlobalObject*, JSC::JSValue);

}
