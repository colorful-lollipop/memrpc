#ifndef INCLUDE_VIRUS_EXECUTOR_SERVICE_RPC_HANDLER_REGISTRAR_H_
#define INCLUDE_VIRUS_EXECUTOR_SERVICE_RPC_HANDLER_REGISTRAR_H_

#include <utility>

#include "memrpc/core/codec.h"
#include "memrpc/server/rpc_server.h"

namespace VirusExecutorService {

class RpcHandlerSink {
public:
    virtual ~RpcHandlerSink() = default;
    virtual void RegisterHandler(MemRpc::Opcode opcode, MemRpc::RpcHandler handler) = 0;
};

class RpcServerHandlerSink final : public RpcHandlerSink {
public:
    explicit RpcServerHandlerSink(MemRpc::RpcServer* server)
        : server_(server)
    {
    }

    void RegisterHandler(MemRpc::Opcode opcode, MemRpc::RpcHandler handler) override
    {
        if (server_ == nullptr) {
            return;
        }
        server_->RegisterHandler(opcode, std::move(handler));
    }

private:
    MemRpc::RpcServer* server_ = nullptr;
};

class RpcHandlerRegistrar {
public:
    virtual ~RpcHandlerRegistrar() = default;
    virtual void RegisterHandlers(RpcHandlerSink* sink) = 0;
};

inline void RegisterHandlersToServer(RpcHandlerRegistrar* registrar, MemRpc::RpcServer* server)
{
    if (registrar == nullptr) {
        return;
    }
    RpcServerHandlerSink sink(server);
    registrar->RegisterHandlers(&sink);
}

template <typename Request, typename Reply, typename Decoder, typename Handler>
inline MemRpc::RpcHandler MakeTypedHandlerWithDecoder(Decoder decoder, Handler handler)
{
    return [decode = std::move(decoder), h = std::move(handler)](const MemRpc::RpcServerCall& call,
                                                                 MemRpc::RpcReply* reply) {
        if (reply == nullptr) {
            return;
        }

        Request request;
        if (!decode(call, &request)) {
            reply->status = MemRpc::StatusCode::ProtocolMismatch;
            return;
        }

        if (!MemRpc::EncodeMessage<Reply>(h(request), &reply->payload)) {
            reply->status = MemRpc::StatusCode::EngineInternalError;
            reply->payload.clear();
        }
    };
}

template <typename Request, typename Reply, typename Handler>
inline MemRpc::RpcHandler MakeTypedHandler(Handler handler)
{
    return MakeTypedHandlerWithDecoder<Request, Reply>(
        [](const MemRpc::RpcServerCall& call, Request* request) {
            return MemRpc::DecodeMessage<Request>(call.payload, request);
        },
        std::move(handler));
}

template <typename Request, typename Reply, typename Decoder, typename Handler>
inline void RegisterTypedHandlerWithDecoder(RpcHandlerSink* sink,
                                            MemRpc::Opcode opcode,
                                            Decoder decoder,
                                            Handler handler)
{
    if (sink == nullptr) {
        return;
    }
    sink->RegisterHandler(opcode, MakeTypedHandlerWithDecoder<Request, Reply>(std::move(decoder), std::move(handler)));
}

template <typename Request, typename Reply, typename Handler>
inline void RegisterTypedHandler(RpcHandlerSink* sink, MemRpc::Opcode opcode, Handler handler)
{
    if (sink == nullptr) {
        return;
    }
    sink->RegisterHandler(opcode, MakeTypedHandler<Request, Reply>(std::move(handler)));
}

}  // namespace VirusExecutorService

#endif  // INCLUDE_VIRUS_EXECUTOR_SERVICE_RPC_HANDLER_REGISTRAR_H_
