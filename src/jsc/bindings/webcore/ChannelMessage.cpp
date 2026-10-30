// The value a `Bun.Channel` operation moves between threads: a structured clone made the way
// `worker.postMessage()` makes one (SharedArrayBuffers and channels shared, `transfer` moved), and
// an AbortSignal is a signal linked to it (LinkedAbortSignal.h). Owned by Rust as
// `bun_runtime::concurrency::message::ChannelMessage`.

#include "root.h"
#include "ChannelMessage.h"

#include "ExceptionOr.h"
#include "JSDOMException.h"
#include "JSDOMConvertObject.h"
#include "JSDOMConvertSequences.h"
#include "JSDOMExceptionHandling.h"
#include "MessagePort.h"
#include "MessageWithMessagePorts.h"
#include "ScriptExecutionContext.h"
#include "SerializedScriptValue.h"
#include "ZigGlobalObject.h"
#include "BunClientData.h"
#include <JavaScriptCore/ErrorInstance.h>
#include <JavaScriptCore/IteratorOperations.h>
#include <JavaScriptCore/JSMap.h>
#include <JavaScriptCore/JSSet.h>

using namespace JSC;
using namespace WebCore;

// `transfer` is `undefined` or an iterable of what the clone moves. Null with an exception pending
// if `value` cannot be cloned or `transfer` is not a valid transfer list; nothing is detached then.
extern "C" MessageWithMessagePorts* Bun__ChannelMessage__create(JSGlobalObject* globalObject, EncodedJSValue encodedValue, EncodedJSValue encodedTransfer)
{
    auto& vm = JSC::getVM(globalObject);
    auto scope = DECLARE_THROW_SCOPE(vm);

    Vector<JSC::Strong<JSC::JSObject>> transferList;
    JSValue transfer = JSValue::decode(encodedTransfer);
    if (!transfer.isUndefined()) {
        transferList = convert<IDLSequence<IDLObject>>(*globalObject, transfer);
        RETURN_IF_EXCEPTION(scope, nullptr);
    }

    Vector<RefPtr<MessagePort>> ports;
    auto serialized = SerializedScriptValue::create(*globalObject, JSValue::decode(encodedValue), WTF::move(transferList), ports, SerializationForStorage::No, SerializationContext::Channel);
    if (serialized.hasException()) {
        WebCore::propagateException(*globalObject, scope, serialized.releaseException());
        return nullptr;
    }
    RETURN_IF_EXCEPTION(scope, nullptr);

    auto disentangledPorts = MessagePort::disentanglePorts(WTF::move(ports));
    if (disentangledPorts.hasException()) {
        WebCore::propagateException(*globalObject, scope, disentangledPorts.releaseException());
        return nullptr;
    }

    return new MessageWithMessagePorts { serialized.releaseReturnValue(), disentangledPorts.releaseReturnValue() };
}

namespace {

// Whether `value` alone clones as a channel value does. False with a termination pending.
bool clones(JSGlobalObject* globalObject, JSValue value)
{
    auto& vm = JSC::getVM(globalObject);
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    Vector<RefPtr<MessagePort>> ports;
    auto serialized = SerializedScriptValue::create(*globalObject, value, {}, ports, SerializationForStorage::No, SerializationContext::Channel);
    if (scope.exception()) {
        (void)scope.tryClearException();
        return false;
    }
    return !serialized.hasException();
}

String keyOf(const String& name)
{
    bool identifier = !name.isEmpty() && !isASCIIDigit(name[0]);
    for (unsigned i = 0; identifier && i < name.length(); i++)
        identifier = isASCIIAlphanumeric(name[i]) || name[i] == '_' || name[i] == '$';
    if (identifier)
        return makeString('.', name);
    StringBuilder quoted;
    quoted.append("[\""_s);
    for (unsigned i = 0; i < name.length(); i++) {
        if (name[i] == '"' || name[i] == '\\')
            quoted.append('\\');
        quoted.append(name[i]);
    }
    quoted.append("\"]"_s);
    return quoted.toString();
}

String withArticle(const String& noun)
{
    bool vowel = !noun.isEmpty() && (toASCIILower(noun[0]) == 'a' || toASCIILower(noun[0]) == 'e' || toASCIILower(noun[0]) == 'i' || toASCIILower(noun[0]) == 'o' || toASCIILower(noun[0]) == 'u');
    return makeString(vowel ? "an "_s : "a "_s, noun);
}

constexpr unsigned maxDepth = 64;

std::optional<Bun::Uncloneable> find(JSGlobalObject* globalObject, JSValue value, const String& path, HashSet<JSObject*>& seen, unsigned depth)
{
    auto& vm = JSC::getVM(globalObject);
    if (value.isSymbol())
        return Bun::Uncloneable { path, "a symbol"_s };
    if (!value.isObject())
        return std::nullopt;
    if (auto marked = Bun::markedUncloneable(vm, value)) {
        marked->path = path;
        return marked;
    }
    JSObject* object = asObject(value);
    if (object->isCallable()) {
        String name;
        if (auto* function = dynamicDowncast<JSFunction>(object))
            name = function->name(vm);
        // A function's name is news only if the path does not end with it.
        bool named = !name.isEmpty() && !path.endsWith(makeString('.', name));
        return Bun::Uncloneable { path, named ? makeString("a function ("_s, name, ')') : "a function"_str };
    }
    if (depth > maxDepth || !seen.add(object).isNewEntry || clones(globalObject, value) || vm.hasPendingTerminationException())
        return std::nullopt;

    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    auto inner = [&](JSValue child, const String& at) -> std::optional<Bun::Uncloneable> {
        if (scope.exception()) {
            (void)scope.tryClearException();
            return std::nullopt;
        }
        return find(globalObject, child, at, seen, depth + 1);
    };
    bool isMapOrSet = object->inherits<JSMap>() || object->inherits<JSSet>();
    if (auto* array = dynamicDowncast<JSArray>(object)) {
        unsigned length = array->length();
        for (unsigned i = 0; i < length && i <= 1000; i++) {
            JSValue element = array->getIndex(globalObject, i);
            if (auto found = inner(element, makeString(path, '[', i, ']')))
                return found;
        }
    } else if (!isMapOrSet) {
        // A structured clone copies an object's own enumerable string-keyed properties.
        PropertyNameArrayBuilder names(vm, PropertyNameMode::Strings, PrivateSymbolMode::Exclude);
        object->methodTable()->getOwnPropertyNames(object, globalObject, names, DontEnumPropertiesMode::Exclude);
        if (scope.exception()) {
            (void)scope.tryClearException();
            return std::nullopt;
        }
        for (auto& name : names) {
            JSValue property = object->get(globalObject, name);
            if (auto found = inner(property, makeString(path, keyOf(name.string()))))
                return found;
        }
        if (object->inherits<ErrorInstance>()) {
            JSValue cause = object->getIfPropertyExists(globalObject, Identifier::fromString(vm, "cause"_s));
            if (cause) {
                if (auto found = inner(cause, makeString(path, ".cause"_s)))
                    return found;
            }
        }
    }
    if (scope.exception())
        (void)scope.tryClearException();
    if (vm.hasPendingTerminationException())
        return std::nullopt;
    // Nothing in it is to blame: it is what can't be cloned.
    if (isMapOrSet)
        return Bun::Uncloneable { path, makeString(object->inherits<JSMap>() ? "a Map"_s : "a Set"_s, " that holds something that can't be cloned"_s) };
    String className = JSObject::calculatedClassName(object);
    if (className.isEmpty() || className == "Object"_s)
        return Bun::Uncloneable { path, "an object that can't be cloned"_s };
    return Bun::Uncloneable { path, withArticle(className) };
}

}

std::optional<Bun::Uncloneable> Bun::markedUncloneable(JSC::VM& vm, JSValue value)
{
    if (!value.isObject())
        return std::nullopt;
    JSObject* object = asObject(value);
    JSValue hint = object->getDirect(vm, WebCore::builtinNames(vm).isUncloneablePrivateName());
    if (!hint || !hint.isString())
        return std::nullopt;
    // A lock is an object literal: its own Symbol.toStringTag names it.
    String className;
    if (JSValue tag = object->getDirect(vm, vm.propertyNames->toStringTagSymbol); tag && tag.isString())
        className = asString(tag)->tryGetValue();
    if (className.isEmpty())
        className = JSObject::calculatedClassName(object);
    return Bun::Uncloneable { emptyString(), className.isEmpty() || className == "Object"_s ? "an object"_str : withArticle(className), asString(hint)->tryGetValue() };
}

String Bun::hintSentence(const String& hint)
{
    if (hint.isEmpty())
        return emptyString();
    return makeString(' ', toASCIIUpper(hint[0]), StringView(hint).substring(1), '.');
}

std::optional<Bun::Uncloneable> Bun::findUncloneable(JSGlobalObject* globalObject, JSValue value)
{
    HashSet<JSObject*> seen;
    return find(globalObject, value, emptyString(), seen, 0);
}

// Bun__ChannelMessage__create for the value of a channel operation, whose DataCloneError says what
// in the value can't be cloned: `site` 0 is channel.send()'s value, 1 the value of Bun.select()
// case `index`.
extern "C" MessageWithMessagePorts* Bun__ChannelMessage__createFor(JSGlobalObject* globalObject, EncodedJSValue encodedValue, EncodedJSValue encodedTransfer, uint8_t site, uint32_t index)
{
    auto& vm = JSC::getVM(globalObject);
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* message = Bun__ChannelMessage__create(globalObject, encodedValue, encodedTransfer);
    if (message)
        RELEASE_AND_RETURN(scope, message);
    JSC::Exception* exception = scope.exception();
    if (!exception || vm.isTerminationException(exception))
        return nullptr;
    auto* domException = dynamicDowncast<JSDOMException>(exception->value());
    if (!domException || domException->wrapped().name() != "DataCloneError"_s)
        return nullptr;
    String original = domException->wrapped().message();
    (void)scope.tryClearException();
    auto found = Bun::findUncloneable(globalObject, JSValue::decode(encodedValue));
    RETURN_IF_EXCEPTION(scope, nullptr);
    String where = site ? "Bun.select()"_s : "channel.send()"_s;
    String root = site ? makeString("cases["_s, index, "][1]"_s) : "value"_str;
    String text = found
        ? makeString(where, ": "_s, root, found->path, " is "_s, found->what, ", which can't be sent to another thread."_s, Bun::hintSentence(found->hint))
        : makeString(where, ": "_s, root, " can't be sent to another thread: "_s, original);
    throwException(globalObject, scope, createDOMException(globalObject, ExceptionCode::DataCloneError, text));
    return nullptr;
}

// Consumes `message`. Empty with an exception pending if it cannot be read in `globalObject`.
extern "C" EncodedJSValue Bun__ChannelMessage__deserialize(JSGlobalObject* globalObject, MessageWithMessagePorts* message)
{
    std::unique_ptr<MessageWithMessagePorts> owned { message };
    auto& vm = JSC::getVM(globalObject);
    auto scope = DECLARE_THROW_SCOPE(vm);
    auto* context = defaultGlobalObject(globalObject)->scriptExecutionContext();
    auto ports = MessagePort::entanglePorts(*context, WTF::move(owned->transferredPorts));
    JSValue value = owned->message->deserialize(*globalObject, globalObject, ports, SerializationErrorMode::Throwing);
    RETURN_IF_EXCEPTION(scope, {});
    return JSValue::encode(value);
}

// Any thread: what a message holds is thread-safe to release (the clone's buffers and shared
// handles are refcounted; ports that were never entangled close their side of the pipe).
extern "C" void Bun__ChannelMessage__destroy(MessageWithMessagePorts* message)
{
    delete message;
}

// `{ value, done }` of a channel's async iterator, with the structure JSC's iterators use.
extern "C" EncodedJSValue Bun__Channel__iteratorResult(JSGlobalObject* globalObject, EncodedJSValue value, bool done)
{
    return JSValue::encode(JSC::createIteratorResultObject(globalObject, JSValue::decode(value), done));
}
