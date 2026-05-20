#ifndef MEMRPC_CORE_FILE_PAYLOAD_H_
#define MEMRPC_CORE_FILE_PAYLOAD_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace MemRpc {

inline constexpr const char* DEFAULT_FILE_PAYLOAD_DIR = "/tmp/cache/file_payload";
inline constexpr uint32_t FILE_PAYLOAD_ENVELOPE_MAGIC = 0x56465031U;  // VFP1
inline constexpr uint32_t FILE_PAYLOAD_ENVELOPE_VERSION = 1U;
inline constexpr uint8_t PAYLOAD_KIND_INLINE = 0U;
inline constexpr uint8_t PAYLOAD_KIND_FILE_REF = 1U;
inline constexpr std::size_t MAX_FILE_PAYLOAD_BYTES = 128ULL * 1024ULL * 1024ULL;

struct FilePayloadRef {
    std::string path;
    uint32_t size = 0;
};

bool ClearFilePayloads(const std::string& dir = DEFAULT_FILE_PAYLOAD_DIR);
bool RemoveFilePayload(const std::string& dir, const FilePayloadRef& ref);
bool WriteFilePayload(const std::string& dir, const std::vector<uint8_t>& payload, FilePayloadRef* ref);
bool ReadAndRemoveFilePayload(const std::string& dir, const FilePayloadRef& ref, std::vector<uint8_t>* payload);
bool EncodeFilePayloadEnvelope(const FilePayloadRef& ref, std::vector<uint8_t>* payload);
bool DecodeFilePayloadEnvelope(const uint8_t* bytes, std::size_t size, FilePayloadRef* ref);
bool RemoveFilePayloadFromTransport(const std::string& dir,
                                    const uint8_t* bytes,
                                    std::size_t size,
                                    uint8_t payloadKind);
bool PrepareFilePayloadForTransport(const std::string& dir,
                                    const std::vector<uint8_t>& payload,
                                    std::size_t maxTransportBytes,
                                    std::vector<uint8_t>* transportPayload,
                                    uint8_t* payloadKind);
bool ResolveFilePayloadFromTransport(const std::string& dir,
                                     const uint8_t* bytes,
                                     std::size_t size,
                                     uint8_t payloadKind,
                                     std::vector<uint8_t>* payload);

}  // namespace MemRpc

#endif  // MEMRPC_CORE_FILE_PAYLOAD_H_
