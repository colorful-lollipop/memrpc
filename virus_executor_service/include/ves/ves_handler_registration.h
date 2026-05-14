#ifndef INCLUDE_VIRUS_EXECUTOR_SERVICE_VES_VES_HANDLER_REGISTRATION_H_
#define INCLUDE_VIRUS_EXECUTOR_SERVICE_VES_VES_HANDLER_REGISTRATION_H_

#include <utility>

#include "service/rpc_handler_registrar.h"
#include "ves/ves_file_payload.h"

namespace VirusExecutorService {

template <typename Request, typename Reply, typename Handler>
inline MemRpc::RpcHandler MakeVesTypedHandler(Handler handler)
{
    return MakeTypedHandlerWithDecoder<Request, Reply>(
        [](const MemRpc::RpcServerCall& call, Request* request) { return DecodeVesRequestPayload(call, request); },
        std::move(handler));
}

template <typename Request, typename Reply, typename Handler>
inline void RegisterVesTypedHandler(RpcHandlerSink* sink, MemRpc::Opcode opcode, Handler handler)
{
    if (sink == nullptr) {
        return;
    }
    sink->RegisterHandler(opcode, MakeVesTypedHandler<Request, Reply>(std::move(handler)));
}

}  // namespace VirusExecutorService

#endif  // INCLUDE_VIRUS_EXECUTOR_SERVICE_VES_VES_HANDLER_REGISTRATION_H_
