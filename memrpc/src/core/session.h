#ifndef MEMRPC_CORE_SESSION_H_
#define MEMRPC_CORE_SESSION_H_

#include <cstdint>
#include <string>

#include "core/shm_layout.h"
#include "memrpc/core/bootstrap.h"
#include "memrpc/core/protocol.h"
#include "memrpc/core/types.h"

namespace MemRpc {

enum class QueueKind : uint8_t {
    HighRequest,
    NormalRequest,
    Response,
};

class Session {
public:
    enum class AttachRole : uint8_t {
        Client,
        Server,
    };

    enum class SessionState : uint32_t {  // NOLINT(performance-enum-size)
        Alive = 0,
        Broken = 1,
    };

    struct RingAccess {
        // RingAccess 把某一条 ring 的 cursor/entries 指针打包返回。
        RingCursor* cursor = nullptr;
        void* entries = nullptr;
    };

    Session();
    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Attach 会接管 handles 中的 fd 所有权；无论成功还是失败，返回时 handles 都会被清空。
    StatusCode Attach(BootstrapHandles* handles, AttachRole role = AttachRole::Client);
    void Reset();

    [[nodiscard]] bool Valid() const;
    [[nodiscard]] const SharedMemoryHeader* Header() const;
    [[nodiscard]] const BootstrapHandles& Handles() const;
    [[nodiscard]] SessionState State() const;
    [[nodiscard]] uint32_t MaxRequestBytes() const;
    [[nodiscard]] uint32_t MaxResponseBytes() const;
    [[nodiscard]] uint32_t ResponseRingSize() const;
    void SetState(SessionState state);

    // Push/Pop 接口封装 ring + eventfd 对应的共享内存协议细节。
    StatusCode PushRequest(QueueKind queue, const RequestRingEntry& entry);
    bool PopRequest(QueueKind queue, RequestRingEntry* entry);
    StatusCode PushResponse(const ResponseRingEntry& entry);
    bool PopResponse(ResponseRingEntry* entry);
    void RemovePendingFilePayloads(const std::string& dir);

private:
    // 根据 queue 类型解析出实际 ring 的 cursor/mutex/entries 指针。
    RingAccess ResolveRing(QueueKind queue);
    StatusCode MapAndValidateHeader(int shmFd);
    StatusCode RemapWithActualLayout(int shmFd);
    StatusCode TryAcquireClientAttachment();
    [[nodiscard]] std::size_t CurrentMappedSize() const;
    std::size_t mappedSize_ = 0;
    std::size_t initialMappedSize_ = 0;
    void* mappedRegion_ = nullptr;
    BootstrapHandles handles_ = MakeDefaultBootstrapHandles();
    SharedMemoryHeader* header_ = nullptr;
    Layout layout_{};
    uint32_t maxRequestBytes_ = 0;
    uint32_t maxResponseBytes_ = 0;
    uint32_t responseRingSize_ = 0;
    AttachRole attachRole_ = AttachRole::Server;
    bool ownsClientAttachment_ = false;
};

}  // namespace MemRpc

#endif  // MEMRPC_CORE_SESSION_H_
