#ifndef INCLUDE_VIRUS_EXECUTOR_SERVICE_VES_VES_FILE_PAYLOAD_H_
#define INCLUDE_VIRUS_EXECUTOR_SERVICE_VES_VES_FILE_PAYLOAD_H_

#include <vector>

#include "memrpc/core/codec.h"
#include "memrpc/core/protocol.h"
#include "memrpc/server/handler.h"

namespace VirusExecutorService {

inline constexpr MemRpc::RequestFlags VES_REQUEST_FLAG_FILE_PAYLOAD_REF = MemRpc::REQUEST_FLAG_APPLICATION_0;

namespace detail {
bool ReadVesFilePayloadForDecode(MemRpc::PayloadView payload, std::vector<uint8_t>* resolvedPayload);
}  // namespace detail

bool ClearVesFilePayloads();
MemRpc::StatusCode PrepareVesFilePayloadForMemRpc(std::vector<uint8_t>* payload, MemRpc::RequestFlags* flags);

template <typename Request>
bool DecodeVesRequestPayload(const MemRpc::RpcServerCall& call, Request* request)
{
    if ((call.flags & VES_REQUEST_FLAG_FILE_PAYLOAD_REF) == 0U) {
        return MemRpc::DecodeMessage<Request>(call.payload, request);
    }

    std::vector<uint8_t> resolvedPayload;
    return detail::ReadVesFilePayloadForDecode(call.payload, &resolvedPayload) &&
           MemRpc::DecodeMessage<Request>(resolvedPayload, request);
}

}  // namespace VirusExecutorService

#endif  // INCLUDE_VIRUS_EXECUTOR_SERVICE_VES_VES_FILE_PAYLOAD_H_
