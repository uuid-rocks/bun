// The value a `Bun.Channel` operation moves between threads: a structured clone made the way
// `worker.postMessage()` makes one (SharedArrayBuffers and channels shared, `transfer` moved), and
// an AbortSignal is a signal linked to it (LinkedAbortSignal.h). Owned by Rust as
// `bun_runtime::concurrency::message::ChannelMessage`.

#include "root.h"

#include "ExceptionOr.h"
#include "JSDOMConvertObject.h"
#include "JSDOMConvertSequences.h"
#include "JSDOMExceptionHandling.h"
#include "MessagePort.h"
#include "MessageWithMessagePorts.h"
#include "ScriptExecutionContext.h"
#include "SerializedScriptValue.h"
#include "ZigGlobalObject.h"

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
