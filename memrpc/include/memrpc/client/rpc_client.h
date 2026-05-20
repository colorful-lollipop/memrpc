#ifndef MEMRPC_CLIENT_RPC_CLIENT_H_
#define MEMRPC_CLIENT_RPC_CLIENT_H_

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

#include "memrpc/core/bootstrap.h"
#include "memrpc/core/codec.h"
#include "memrpc/core/protocol.h"
#include "memrpc/core/types.h"

namespace MemRpc {

struct RpcCall {
    Opcode opcode = OPCODE_INVALID;
    Priority priority = Priority::Normal;
    // exec_timeout_ms 从 client 侧请求成功发布到 request ring 后开始计时，
    // 直到收到最终 reply 为止。超时后返回 ExecTimeout，但不会取消服务端执行；
    // 如果真实 reply 晚到，client 会直接忽略。
    uint32_t execTimeoutMs = 30000;
    std::vector<uint8_t> payload;
};

class RpcFuture {
public:
    RpcFuture();
    ~RpcFuture();

    RpcFuture(const RpcFuture&) = delete;
    RpcFuture& operator=(const RpcFuture&) = delete;
    RpcFuture(RpcFuture&&) noexcept = default;
    RpcFuture& operator=(RpcFuture&&) noexcept = default;

    // IsReady 只表示 reply 已经落地，不区分成功或失败。
    [[nodiscard]] bool IsReady() const;
    // Wait 会阻塞到 reply 就绪，并消费内部 reply。
    StatusCode Wait(RpcReply* reply) &&;
    StatusCode Wait(RpcReply* reply) & = delete;
    StatusCode Wait(RpcReply* reply) const& = delete;

private:
    struct State;
    explicit RpcFuture(std::shared_ptr<State> state);

    std::shared_ptr<State> state_;

    friend class RpcClient;
};

// Wait on a future and decode the reply payload.
template <typename Rep>
StatusCode WaitAndDecode(RpcFuture&& future, Rep* reply)
{
    if (reply == nullptr) {
        return StatusCode::InvalidArgument;
    }
    RpcReply rpcReply;
    const StatusCode status = std::move(future).Wait(&rpcReply);
    if (status != StatusCode::Ok) {
        return status;
    }
    return DecodeMessage<Rep>(rpcReply.payload, reply) ? rpcReply.status : StatusCode::ProtocolMismatch;
}

struct RpcFailure {
    StatusCode status = StatusCode::Ok;
    Opcode opcode = OPCODE_INVALID;
    Priority priority = Priority::Normal;
    uint32_t execTimeoutMs = 0;
    uint64_t requestId = 0;
    uint64_t sessionId = 0;
};

struct EngineDeathReport {
    uint64_t deadSessionId = 0;
};

enum class RecoveryAction {  // NOLINT(performance-enum-size)
    Ignore,
    Restart,
    IdleClose,
    // internal-only terminal transition used by explicit Shutdown()
    ManualShutdown,
};

struct RecoveryDecision {
    RecoveryAction action = RecoveryAction::Ignore;
    uint32_t delayMs = 0;
};

enum class ClientLifecycleState : uint8_t {
    Uninitialized = 0,
    Active = 1,
    NoSession = 2,
    Cooldown = 3,
    IdleClosed = 4,
    Recovering = 5,
    Closed = 6,
};

enum class ClientInitMode : uint8_t {
    EagerSession = 0,
    LazySession = 1,
};

struct RecoveryRuntimeSnapshot {
    ClientLifecycleState lifecycleState = ClientLifecycleState::Uninitialized;
    bool recoveryPending = false;
    uint32_t cooldownRemainingMs = 0;
    uint64_t currentSessionId = 0;
};

struct RecoveryEventReport {
    ClientLifecycleState previousState = ClientLifecycleState::Uninitialized;
    ClientLifecycleState state = ClientLifecycleState::Uninitialized;
    bool recoveryPending = false;
    uint32_t cooldownDelayMs = 0;
    uint32_t cooldownRemainingMs = 0;
    uint64_t sessionId = 0;
    uint64_t monotonicMs = 0;
};

struct RecoveryPolicy {
    std::function<RecoveryDecision(const RpcFailure&)> onFailure;
    // onIdle 收到的是“自最后一次活动以来累计 idle 总时长”。
    // 框架内部按固定 watchdog 节拍在“无 pending / 无排队 / session live”时采样回调；
    // idle 阈值是否成立、是否需要 CloseSession / Restart，由业务回调自己判断。
    std::function<RecoveryDecision(uint64_t idle_ms)> onIdle;
    std::function<RecoveryDecision(const EngineDeathReport&)> onEngineDeath;
};

using RecoveryEventCallback = std::function<void(const RecoveryEventReport&)>;

struct ExternalRecoveryRequest {
    uint64_t sessionId = 0;
    uint32_t delayMs = 0;
};

class RpcClient {
public:
    explicit RpcClient(std::shared_ptr<IBootstrapChannel> bootstrap = nullptr);
    ~RpcClient();

    RpcClient(const RpcClient&) = delete;
    RpcClient& operator=(const RpcClient&) = delete;
    RpcClient(RpcClient&&) noexcept = default;
    RpcClient& operator=(RpcClient&&) noexcept = default;

    void SetBootstrapChannel(std::shared_ptr<IBootstrapChannel> bootstrap);
    void SetEventCallback(RpcEventCallback callback);
    void SetRecoveryEventCallback(RecoveryEventCallback callback);
    void SetRecoveryPolicy(RecoveryPolicy policy);
    void RequestExternalRecovery(ExternalRecoveryRequest request);
    // Init 负责启动 client 运行时。默认 eager 模式会立即建立 session；
    // lazy 模式只启动线程与状态机，等首次实际提交时再按需 OpenSession。
    StatusCode Init(ClientInitMode mode = ClientInitMode::EagerSession);
    // InvokeAsync 是框架层一等接口；失败时返回 ready future。
    RpcFuture InvokeAsync(const RpcCall& call);
    RpcFuture InvokeAsync(RpcCall&& call);
    // RetryUntilRecoverySettles 只在 client 处于内部恢复窗口时等待并重试 invoke。
    // 它不会透明重放已经成功发布到旧 session 的 pending 请求；仅用于对
    // CooldownActive / PeerDisconnected 这类“尚未稳定进入可用 session”的调用做包装。
    StatusCode RetryUntilRecoverySettles(const std::function<StatusCode()>& invoke);
    [[nodiscard]] RecoveryRuntimeSnapshot GetRecoveryRuntimeSnapshot() const;
    void Shutdown();

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace MemRpc

#endif  // MEMRPC_CLIENT_RPC_CLIENT_H_
