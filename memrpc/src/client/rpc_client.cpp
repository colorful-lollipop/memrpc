#include "memrpc/client/rpc_client.h"

#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>

#include "core/session.h"
#include "memrpc/core/file_payload.h"
#include "memrpc/core/runtime_utils.h"
#include "virus_protection_executor_log.h"

namespace MemRpc {

namespace {

using namespace std::chrono_literals;

// File-local policy and timing helpers shared by the internal client components.
constexpr auto kPendingTimeoutPollPeriod = 100ms;
constexpr auto kHealthCheckPeriod = 100ms;
constexpr auto kIdlePollPeriod = 100ms;
constexpr auto kRecoveryRetryPollPeriod = 20ms;
constexpr auto kRetryUntilRecoverySettlesGrace = 100ms;
constexpr auto kDisconnectedPushRetryDelay = 10ms;

bool IsHighPriority(const RpcCall& call)
{
    return call.priority == Priority::High;
}

bool ShouldRetryRecoveryStatus(StatusCode status, const RecoveryRuntimeSnapshot& snapshot)
{
    if (snapshot.lifecycleState == ClientLifecycleState::Closed) {
        return false;
    }
    const bool recoveryInProgress = snapshot.lifecycleState == ClientLifecycleState::Cooldown ||
                                    snapshot.lifecycleState == ClientLifecycleState::Recovering;
    const bool retryableStatus = status == StatusCode::CooldownActive || status == StatusCode::PeerDisconnected;
    return recoveryInProgress && retryableStatus;
}

std::chrono::milliseconds CooldownRemaining(uint64_t cooldownUntilMs)
{
    const uint64_t nowMs = MonotonicNowMs();
    if (cooldownUntilMs <= nowMs) {
        return std::chrono::milliseconds::zero();
    }
    return std::chrono::milliseconds{cooldownUntilMs - nowMs};
}

class UniqueFd final {
public:
    UniqueFd() = default;

    explicit UniqueFd(int fd)
        : fd_(fd)
    {
    }

    ~UniqueFd()
    {
        Reset();
    }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept
        : fd_(other.Release())
    {
    }

    UniqueFd& operator=(UniqueFd&& other) noexcept
    {
        if (this != &other) {
            Reset(other.Release());
        }
        return *this;
    }

    [[nodiscard]] int Get() const
    {
        return fd_;
    }

    [[nodiscard]] int Release()
    {
        const int releasedFd = fd_;
        fd_ = -1;
        return releasedFd;
    }

    void Reset(int fd = -1)
    {
        if (fd_ >= 0) {
            (void)close(fd_);
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

}  // namespace

struct RpcFuture::State {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;
    RpcReply reply;
};

RpcFuture::RpcFuture() = default;

RpcFuture::RpcFuture(std::shared_ptr<State> state)
    : state_(std::move(state))
{
}

RpcFuture::~RpcFuture() = default;

bool RpcFuture::IsReady() const
{
    if (!state_) {
        return false;
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->ready;
}

StatusCode RpcFuture::Wait(RpcReply* reply) &&
{
    std::shared_ptr<State> state = std::move(state_);
    if (!state) {
        HILOGE("RpcFuture::Wait failed: state is null");
        if (reply != nullptr) {
            *reply = RpcReply{};
            reply->status = StatusCode::PeerDisconnected;
        }
        return StatusCode::PeerDisconnected;
    }
    std::unique_lock<std::mutex> lock(state->mutex);
    state->cv.wait(lock, [&state] { return state->ready; });
    if (reply != nullptr) {
        *reply = std::move(state->reply);
    }
    return reply != nullptr ? reply->status : state->reply.status;
}

struct RpcClient::Impl : public std::enable_shared_from_this<RpcClient::Impl> {
    struct PendingSubmit {
        RpcCall call;
        uint64_t requestId = 0;
        std::shared_ptr<RpcFuture::State> future;
    };

    struct PendingInfo {
        Opcode opcode = OPCODE_INVALID;
        Priority priority = Priority::Normal;
        uint32_t execTimeoutMs = 0;
        uint64_t requestId = 0;
        uint64_t sessionId = 0;
    };

    struct PendingRequest {
        std::shared_ptr<RpcFuture::State> future;
        PendingInfo info;
        std::chrono::steady_clock::time_point waitDeadline = std::chrono::steady_clock::time_point::max();
    };

    class ClientRequestStore {
    public:
        void Put(uint64_t requestId, PendingRequest pending)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_[requestId] = std::move(pending);
        }

        std::optional<PendingRequest> Take(uint64_t requestId)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = pending_.find(requestId);
            if (it == pending_.end()) {
                return std::nullopt;
            }
            PendingRequest pending = std::move(it->second);
            pending_.erase(it);
            return pending;
        }

        std::vector<PendingRequest> TakeAll()
        {
            std::vector<PendingRequest> drained;
            std::lock_guard<std::mutex> lock(mutex_);
            drained.reserve(pending_.size());
            for (auto& [requestId, pending] : pending_) {
                static_cast<void>(requestId);
                drained.push_back(std::move(pending));
            }
            pending_.clear();
            return drained;
        }

        std::vector<PendingRequest> TakeExpired(std::chrono::steady_clock::time_point now)
        {
            std::vector<PendingRequest> expired;
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto it = pending_.begin(); it != pending_.end();) {
                const auto deadline = it->second.waitDeadline;
                if (deadline != std::chrono::steady_clock::time_point::max() && now >= deadline) {
                    expired.push_back(std::move(it->second));
                    it = pending_.erase(it);
                } else {
                    ++it;
                }
            }
            return expired;
        }

        [[nodiscard]] bool Empty() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return pending_.empty();
        }

        [[nodiscard]] uint32_t Size() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return static_cast<uint32_t>(pending_.size());
        }

    private:
        mutable std::mutex mutex_;
        std::unordered_map<uint64_t, PendingRequest> pending_;
    };

    enum class ApiLifecycleState : uint8_t {
        Uninitialized,
        Open,
        Closing,
        Closed,
    };

    enum class PendingRequestRecoveryAction : uint8_t {
        FailWithRecoveryPolicy,
        ResolvePeerDisconnected,
        KeepCurrentState,
    };

    class ClientSessionTransport {
    public:
        struct DeathCallbackLease {};
        struct SessionWaitHandleUpdate {
            enum class Action : uint8_t {
                KeepCurrent,
                ClearCurrent,
                ReplaceCurrent,
            };

            Action action = Action::KeepCurrent;
            uint64_t sessionId = 0;
            int fd = -1;
        };

        explicit ClientSessionTransport(std::shared_ptr<IBootstrapChannel> bootstrap)
            : bootstrap_(std::move(bootstrap))
        {
        }

        void SetBootstrapChannel(std::shared_ptr<IBootstrapChannel> bootstrap,
                                 const std::function<void(uint64_t)>& deathCallback)
        {
            std::lock_guard<std::mutex> lock(bootstrapMutex_);
            if (bootstrap_ != nullptr && bootstrap_ != bootstrap) {
                deathCallbackLease_.reset();
                bootstrap_->SetEngineDeathCallback({});
            }
            bootstrap_ = std::move(bootstrap);
            InstallDeathCallbackLocked(deathCallback);
        }

        void InstallDeathCallback(const std::function<void(uint64_t)>& deathCallback)
        {
            std::lock_guard<std::mutex> lock(bootstrapMutex_);
            InstallDeathCallbackLocked(deathCallback);
        }

        void ClearDeathCallback()
        {
            std::lock_guard<std::mutex> lock(bootstrapMutex_);
            deathCallbackLease_.reset();
            if (bootstrap_ != nullptr) {
                bootstrap_->SetEngineDeathCallback({});
            }
        }

        std::shared_ptr<IBootstrapChannel> LoadBootstrap() const
        {
            std::lock_guard<std::mutex> lock(bootstrapMutex_);
            return bootstrap_;
        }

        uint64_t LoadSessionIdSnapshot() const
        {
            return snapshot_.load(std::memory_order_relaxed);
        }

        uint64_t CurrentSessionId() const
        {
            return LoadSessionIdSnapshot();
        }

        bool HasLiveSession() const
        {
            return LoadSessionIdSnapshot() != 0;
        }

        SessionWaitHandleUpdate DuplicateRequestCreditWaitHandle(uint64_t activeSessionId) const
        {
            return DuplicateSessionWaitHandle(activeSessionId,
                                              [](const Session& session) { return session.Handles().reqCreditEventFd; },
                                              "RpcClient::DuplicateRequestCreditWaitHandle");
        }

        SessionWaitHandleUpdate DuplicateResponseWaitHandle(uint64_t activeSessionId) const
        {
            return DuplicateSessionWaitHandle(activeSessionId,
                                              [](const Session& session) { return session.Handles().respEventFd; },
                                              "RpcClient::DuplicateResponseWaitHandle");
        }

        StatusCode OpenSession()
        {
            auto bootstrap = LoadBootstrap();
            if (bootstrap == nullptr) {
                HILOGE("RpcClient::OpenSession failed: bootstrap channel is null");
                return StatusCode::InvalidArgument;
            }

            BootstrapHandles handles = MakeDefaultBootstrapHandles();
            const StatusCode openStatus = bootstrap->OpenSession(handles);
            if (openStatus != StatusCode::Ok) {
                HILOGE("RpcClient::OpenSession failed: bootstrap OpenSession status=%{public}d",
                       static_cast<int>(openStatus));
                return openStatus;
            }

            const uint64_t sessionId = handles.sessionId;
            std::lock_guard<std::mutex> lock(sessionMutex_);
            const StatusCode attachStatus = session_.Attach(&handles, Session::AttachRole::Client);
            if (attachStatus != StatusCode::Ok) {
                HILOGE("RpcClient::OpenSession failed: session attach status=%{public}d session_id=%{public}llu",
                       static_cast<int>(attachStatus),
                       static_cast<unsigned long long>(sessionId));
                return attachStatus;
            }
            session_.SetState(Session::SessionState::Alive);
            PublishSessionIdLocked(sessionId);
            return StatusCode::Ok;
        }

        uint64_t CloseLiveSession()
        {
            const auto bootstrap = LoadBootstrap();
            uint64_t closedSessionId = 0;
            {
                std::lock_guard<std::mutex> lock(sessionMutex_);
                closedSessionId = LoadSessionIdSnapshot();
                PublishSessionIdLocked(0);
                session_.Reset();
            }
            if (bootstrap != nullptr) {
                const StatusCode status = bootstrap->CloseSession();
                if (status != StatusCode::Ok) {
                    HILOGW("RpcClient::CloseLiveSession bootstrap CloseSession failed: status=%{public}d",
                           static_cast<int>(status));
                }
            }
            return closedSessionId;
        }

        void CloseLiveSessionIfSnapshotMatches(uint64_t expectedSessionId)
        {
            const auto bootstrap = LoadBootstrap();
            bool shouldCloseBootstrap = false;
            {
                std::lock_guard<std::mutex> lock(sessionMutex_);
                const uint64_t currentSessionId = LoadSessionIdSnapshot();
                if (currentSessionId != expectedSessionId) {
                    return;
                }
                if (currentSessionId == 0 || !session_.Valid()) {
                    return;
                }
                PublishSessionIdLocked(0);
                session_.Reset();
                shouldCloseBootstrap = true;
            }
            if (shouldCloseBootstrap && bootstrap != nullptr) {
                const StatusCode status = bootstrap->CloseSession();
                if (status != StatusCode::Ok) {
                    HILOGW("RpcClient::CloseLiveSessionIfSnapshotMatches bootstrap CloseSession failed: "
                           "status=%{public}d",
                           static_cast<int>(status));
                }
            }
        }

        template <typename Fn>
        auto WithSessionLocked(Fn&& fn) -> decltype(std::forward<Fn>(fn)(std::declval<Session&>()))
        {
            std::lock_guard<std::mutex> lock(sessionMutex_);
            return std::forward<Fn>(fn)(session_);
        }

        template <typename Fn>
        auto WithSessionLocked(Fn&& fn) const -> decltype(std::forward<Fn>(fn)(std::declval<const Session&>()))
        {
            std::lock_guard<std::mutex> lock(sessionMutex_);
            return std::forward<Fn>(fn)(session_);
        }

    private:
        template <typename FdSelector>
        SessionWaitHandleUpdate DuplicateSessionWaitHandle(uint64_t activeSessionId,
                                                           FdSelector&& selectFd,
                                                           const char* logContext) const
        {
            SessionWaitHandleUpdate waitHandle;
            const uint64_t snapshotSessionId = LoadSessionIdSnapshot();
            if (snapshotSessionId == activeSessionId && snapshotSessionId != 0) {
                waitHandle.action = SessionWaitHandleUpdate::Action::KeepCurrent;
                waitHandle.sessionId = snapshotSessionId;
                return waitHandle;
            }
            std::lock_guard<std::mutex> lock(sessionMutex_);
            waitHandle.sessionId = LoadSessionIdSnapshot();
            if (waitHandle.sessionId == activeSessionId && waitHandle.sessionId != 0) {
                waitHandle.action = SessionWaitHandleUpdate::Action::KeepCurrent;
                return waitHandle;
            }
            if (!session_.Valid()) {
                waitHandle.action = SessionWaitHandleUpdate::Action::ClearCurrent;
                if (activeSessionId != 0) {
                    HILOGI("%{public}s clearing stale wait fd: active_session=%{public}llu "
                           "current_session=%{public}llu",
                           logContext,
                           static_cast<unsigned long long>(activeSessionId),
                           static_cast<unsigned long long>(waitHandle.sessionId));
                }
                return waitHandle;
            }
            const int sourceFd = std::forward<FdSelector>(selectFd)(session_);
            if (sourceFd < 0) {
                waitHandle.action = SessionWaitHandleUpdate::Action::ClearCurrent;
                return waitHandle;
            }
            waitHandle.fd = dup(sourceFd);
            if (waitHandle.fd < 0) {
                HILOGE("%{public}s dup failed: fd=%{public}d", logContext, sourceFd);
                waitHandle.action = SessionWaitHandleUpdate::Action::ClearCurrent;
                return waitHandle;
            }
            waitHandle.action = SessionWaitHandleUpdate::Action::ReplaceCurrent;
            HILOGI("%{public}s refreshed wait fd: active_session=%{public}llu current_session=%{public}llu",
                   logContext,
                   static_cast<unsigned long long>(activeSessionId),
                   static_cast<unsigned long long>(waitHandle.sessionId));
            return waitHandle;
        }

        void InstallDeathCallbackLocked(const std::function<void(uint64_t)>& deathCallback)
        {
            if (bootstrap_ == nullptr) {
                deathCallbackLease_.reset();
                return;
            }
            if (!deathCallback) {
                deathCallbackLease_.reset();
                bootstrap_->SetEngineDeathCallback({});
                return;
            }

            auto lease = std::make_shared<DeathCallbackLease>();
            deathCallbackLease_ = lease;
            std::weak_ptr<DeathCallbackLease> weakLease = lease;
            bootstrap_->SetEngineDeathCallback([weakLease, deathCallback](uint64_t sessionId) {
                const auto retainedLease = weakLease.lock();
                if (retainedLease == nullptr) {
                    return;
                }
                deathCallback(sessionId);
            });
        }

        void PublishSessionIdLocked(uint64_t sessionId)
        {
            // The snapshot is only a session-generation token. session_ itself stays
            // behind sessionMutex_, so readers do not rely on this atomic to publish
            // any other state.
            snapshot_.store(sessionId, std::memory_order_relaxed);
        }

        mutable std::mutex bootstrapMutex_;
        std::shared_ptr<IBootstrapChannel> bootstrap_;
        std::shared_ptr<DeathCallbackLease> deathCallbackLease_;
        Session session_;
        mutable std::mutex sessionMutex_;
        std::atomic<uint64_t> snapshot_{0};
    };

    class ClientRecoveryState {
    public:
        enum class CooldownWindowChange : uint8_t {
            Keep,
            Clear,
            Set,
        };

        void SetRecoveryEventCallback(RecoveryEventCallback callback)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            recoveryEventCallback_ = std::move(callback);
        }

        void ClearRecoveryEventCallback()
        {
            std::lock_guard<std::mutex> lock(mutex_);
            recoveryEventCallback_ = {};
        }

        ClientLifecycleState LifecycleState() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return lifecycleState_;
        }

        void PrepareForInitOpen(ClientLifecycleState initialState)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cooldownUntilMs_ = 0;
            lifecycleState_ = initialState;
        }

        void BeginSessionOpen(ClientLifecycleState lifecycleState, uint64_t currentSessionId)
        {
            if (lifecycleState != ClientLifecycleState::Recovering &&
                lifecycleState != ClientLifecycleState::Uninitialized) {
                TransitionLifecycle(ClientLifecycleState::Recovering, 0, currentSessionId);
            }
        }

        void FinalizeSessionOpen(uint64_t currentSessionId)
        {
            TransitionLifecycle(ClientLifecycleState::Active,
                                0,
                                currentSessionId,
                                CooldownWindowChange::Clear,
                                0);
            NotifyRecoveryStateChanged();
        }

        void HandleSessionOpenFailure(StatusCode status, uint64_t currentSessionId)
        {
            const ClientLifecycleState lifecycleState = LifecycleState();
            if (lifecycleState == ClientLifecycleState::Recovering || lifecycleState == ClientLifecycleState::Cooldown) {
                HILOGW("RpcClient::EnsureLiveSession abandoning recovery after open failure: status=%{public}d",
                       static_cast<int>(status));
                EnterNoSession(currentSessionId);
            }
            HILOGE("RpcClient::EnsureLiveSession failed to open session: status=%{public}d lifecycle=%{public}d",
                   static_cast<int>(status),
                   static_cast<int>(lifecycleState));
        }

        void TransitionLifecycle(ClientLifecycleState state,
                                 uint32_t cooldownDelayMs,
                                 uint64_t currentSessionId,
                                 CooldownWindowChange cooldownWindowChange = CooldownWindowChange::Keep,
                                 uint64_t cooldownUntilMs = 0)
        {
            RecoveryEventCallback callback;
            RecoveryEventReport report;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                const ClientLifecycleState previousState =
                    ApplyTransitionLocked(state, cooldownWindowChange, cooldownUntilMs);
                callback = recoveryEventCallback_;
                report = BuildTransitionReportLocked(previousState, state, cooldownDelayMs, currentSessionId);
            }
            DispatchRecoveryEvent(callback, report);
        }

        void StartRecovery(uint32_t delayMs, uint64_t currentSessionId)
        {
            const uint64_t cooldownUntilMs = MonotonicNowMs() + delayMs;
            TransitionLifecycle(delayMs == 0 ? ClientLifecycleState::Recovering : ClientLifecycleState::Cooldown,
                                delayMs,
                                currentSessionId,
                                CooldownWindowChange::Set,
                                cooldownUntilMs);
            NotifyRecoveryStateChanged();
        }

        void EnterNoSession(uint64_t currentSessionId)
        {
            TransitionLifecycle(ClientLifecycleState::NoSession, 0, currentSessionId, CooldownWindowChange::Clear, 0);
            NotifyRecoveryStateChanged();
        }

        void EnterIdleClosed(uint64_t currentSessionId)
        {
            TransitionLifecycle(ClientLifecycleState::IdleClosed,
                                0,
                                currentSessionId,
                                CooldownWindowChange::Clear,
                                0);
            NotifyRecoveryStateChanged();
        }

        void EnterTerminalClosed(uint64_t currentSessionId)
        {
            TransitionLifecycle(ClientLifecycleState::Closed, 0, currentSessionId, CooldownWindowChange::Clear, 0);
            NotifyRecoveryStateChanged();
        }

        [[nodiscard]] bool CooldownActive() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return MonotonicNowMs() < cooldownUntilMs_;
        }

        [[nodiscard]] std::chrono::milliseconds CooldownRemaining() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return CooldownRemainingLocked();
        }

        [[nodiscard]] bool RecoveryPending() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return HasPendingRecoveryLocked();
        }

        void NotifyRecoveryStateChanged()
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                ++notificationGeneration_;
            }
            recoveryStateCv_.notify_all();
        }

        [[nodiscard]] uint64_t NotificationGeneration() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return notificationGeneration_;
        }

        void WaitForNotification(uint64_t observedGeneration, const std::atomic<bool>& workersRunning)
        {
            std::unique_lock<std::mutex> lock(mutex_);
            recoveryStateCv_.wait(lock, [this, observedGeneration, &workersRunning] {
                return notificationGeneration_ != observedGeneration ||
                       !workersRunning.load(std::memory_order_acquire);
            });
        }

        // `workersRunning` answers whether background loops should keep waiting.
        // `clientControlState` answers whether the public API is still allowed to
        // accept/complete recovery work while those loops are alive.
        StatusCode WaitForCooldownToSettle(std::chrono::steady_clock::time_point deadline,
                                           const std::atomic<bool>& workersRunning,
                                           const std::atomic<ApiLifecycleState>& clientControlState)
        {
            return WaitForRecoveryState(deadline, workersRunning, clientControlState, RecoveryWaitMode::Cooldown);
        }

        StatusCode WaitOneRecoveryRetryTick(std::chrono::steady_clock::time_point deadline,
                                            const std::atomic<bool>& workersRunning,
                                            const std::atomic<ApiLifecycleState>& clientControlState)
        {
            return WaitForRecoveryState(deadline, workersRunning, clientControlState, RecoveryWaitMode::RetryTick);
        }

        RecoveryRuntimeSnapshot GetSnapshot(uint64_t currentSessionId) const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            RecoveryRuntimeSnapshot snapshot;
            snapshot.lifecycleState = lifecycleState_;
            snapshot.recoveryPending = HasPendingRecoveryLocked();
            snapshot.cooldownRemainingMs = static_cast<uint32_t>(CooldownRemainingLocked().count());
            snapshot.currentSessionId = currentSessionId;
            return snapshot;
        }

    private:
        enum class RecoveryWaitMode : uint8_t {
            Cooldown,
            RetryTick,
        };

        enum class RecoveryWaitStep : uint8_t {
            ContinueWaiting,
            Complete,
        };

        [[nodiscard]] ClientLifecycleState ApplyTransitionLocked(ClientLifecycleState nextState,
                                                                 CooldownWindowChange cooldownWindowChange,
                                                                 uint64_t cooldownUntilMs)
        {
            const ClientLifecycleState previousState = lifecycleState_;
            switch (cooldownWindowChange) {
                case CooldownWindowChange::Keep:
                    break;
                case CooldownWindowChange::Clear:
                    cooldownUntilMs_ = 0;
                    break;
                case CooldownWindowChange::Set:
                    cooldownUntilMs_ = cooldownUntilMs;
                    break;
            }
            lifecycleState_ = nextState;
            return previousState;
        }

        StatusCode WorkerStoppedStatus(RecoveryWaitMode mode,
                                       const std::atomic<ApiLifecycleState>& clientControlState) const
        {
            HILOGW("RpcClient::%{public}s aborted because workers stopped",
                   mode == RecoveryWaitMode::Cooldown ? "WaitForCooldownToSettle" : "WaitOneRecoveryRetryTick");
            return clientControlState.load(std::memory_order_acquire) == ApiLifecycleState::Open
                     ? StatusCode::PeerDisconnected
                     : StatusCode::ClientClosed;
        }

        [[nodiscard]] StatusCode RecoveryStateWaitResultLocked() const
        {
            switch (lifecycleState_) {
                case ClientLifecycleState::Active:
                case ClientLifecycleState::Cooldown:
                case ClientLifecycleState::Recovering:
                    return StatusCode::Ok;
                case ClientLifecycleState::Closed:
                    return StatusCode::ClientClosed;
                case ClientLifecycleState::NoSession:
                case ClientLifecycleState::IdleClosed:
                case ClientLifecycleState::Uninitialized:
                    return StatusCode::PeerDisconnected;
            }
            return StatusCode::PeerDisconnected;
        }

        RecoveryWaitStep WaitForCooldownStepLocked(std::unique_lock<std::mutex>& lock,
                                                   std::chrono::steady_clock::time_point deadline,
                                                   StatusCode& status)
        {
            if (lifecycleState_ != ClientLifecycleState::Cooldown) {
                status = RecoveryStateWaitResultLocked();
                return RecoveryWaitStep::Complete;
            }

            const uint64_t nowMs = MonotonicNowMs();
            if (nowMs >= cooldownUntilMs_) {
                status = StatusCode::Ok;
                return RecoveryWaitStep::Complete;
            }

            const auto now = std::chrono::steady_clock::now();
            if (deadline != std::chrono::steady_clock::time_point::max() && now >= deadline) {
                HILOGW("RpcClient::WaitForCooldownToSettle timed out");
                status = StatusCode::CooldownActive;
                return RecoveryWaitStep::Complete;
            }

            auto wakeAt = now + std::chrono::milliseconds{cooldownUntilMs_ - nowMs};
            if (deadline != std::chrono::steady_clock::time_point::max()) {
                wakeAt = std::min(wakeAt, deadline);
            }
            const bool stateChanged = recoveryStateCv_.wait_until(lock, wakeAt, [this] {
                return lifecycleState_ != ClientLifecycleState::Cooldown;
            });
            if (stateChanged) {
                status = RecoveryStateWaitResultLocked();
                return RecoveryWaitStep::Complete;
            }
            return RecoveryWaitStep::ContinueWaiting;
        }

        RecoveryWaitStep WaitForRetryStepLocked(std::unique_lock<std::mutex>& lock,
                                                std::chrono::steady_clock::time_point deadline,
                                                StatusCode& status)
        {
            if (lifecycleState_ != ClientLifecycleState::Recovering) {
                status = RecoveryStateWaitResultLocked();
                return RecoveryWaitStep::Complete;
            }

            const auto now = std::chrono::steady_clock::now();
            if (deadline != std::chrono::steady_clock::time_point::max() && now >= deadline) {
                HILOGW("RpcClient::WaitOneRecoveryRetryTick timed out");
                status = StatusCode::PeerDisconnected;
                return RecoveryWaitStep::Complete;
            }

            auto waitFor = kRecoveryRetryPollPeriod;
            if (deadline != std::chrono::steady_clock::time_point::max()) {
                waitFor = std::min(waitFor, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
            }
            if (waitFor <= std::chrono::milliseconds::zero()) {
                HILOGW("RpcClient::WaitOneRecoveryRetryTick reached zero wait budget");
                status = StatusCode::PeerDisconnected;
                return RecoveryWaitStep::Complete;
            }

            static_cast<void>(recoveryStateCv_.wait_for(lock, waitFor, [this] {
                return lifecycleState_ != ClientLifecycleState::Recovering;
            }));
            status = lifecycleState_ == ClientLifecycleState::Recovering ? StatusCode::Ok
                                                                         : RecoveryStateWaitResultLocked();
            return RecoveryWaitStep::Complete;
        }

        StatusCode WaitForRecoveryState(std::chrono::steady_clock::time_point deadline,
                                        const std::atomic<bool>& workersRunning,
                                        const std::atomic<ApiLifecycleState>& clientControlState,
                                        RecoveryWaitMode mode)
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (workersRunning.load(std::memory_order_acquire)) {
                if (clientControlState.load(std::memory_order_acquire) != ApiLifecycleState::Open) {
                    return StatusCode::ClientClosed;
                }
                StatusCode status = StatusCode::Ok;
                const RecoveryWaitStep step =
                    mode == RecoveryWaitMode::Cooldown ? WaitForCooldownStepLocked(lock, deadline, status)
                                                       : WaitForRetryStepLocked(lock, deadline, status);
                if (step == RecoveryWaitStep::ContinueWaiting) {
                    continue;
                }
                return status;
            }
            return WorkerStoppedStatus(mode, clientControlState);
        }

        [[nodiscard]] bool HasPendingRecoveryLocked() const
        {
            return lifecycleState_ == ClientLifecycleState::Recovering ||
                   lifecycleState_ == ClientLifecycleState::Cooldown;
        }

        [[nodiscard]] RecoveryEventReport BuildTransitionReportLocked(
            ClientLifecycleState previousState,
            ClientLifecycleState nextState,
            uint32_t cooldownDelayMs,
            uint64_t currentSessionId)
        {
            RecoveryEventReport report;
            report.previousState = previousState;
            report.state = nextState;
            report.recoveryPending = HasPendingRecoveryLocked();
            report.cooldownDelayMs = cooldownDelayMs;
            report.cooldownRemainingMs = static_cast<uint32_t>(CooldownRemainingLocked().count());
            report.sessionId = currentSessionId;
            report.monotonicMs = MonotonicNowMs();
            return report;
        }

        static void DispatchRecoveryEvent(const RecoveryEventCallback& callback, const RecoveryEventReport& report)
        {
            if (callback) {
                callback(report);
            }
        }

        std::chrono::milliseconds CooldownRemainingLocked() const
        {
            return ::MemRpc::CooldownRemaining(cooldownUntilMs_);
        }

        mutable std::mutex mutex_;
        std::condition_variable recoveryStateCv_;
        RecoveryEventCallback recoveryEventCallback_;
        ClientLifecycleState lifecycleState_ = ClientLifecycleState::Uninitialized;
        uint64_t cooldownUntilMs_ = 0;
        uint64_t notificationGeneration_ = 0;
    };

    class SubmitWorker {
    public:
        explicit SubmitWorker(Impl& owner)
            : owner_(&owner)
        {
        }

        void Run()
        {
            while (true) {
                PendingSubmit submit;
                {
                    std::unique_lock<std::mutex> lock(owner_->submitMutex_);
                    owner_->submitCv_.wait(lock,
                                           [this] { return owner_->submitStopRequested_ || !owner_->submitQueue_.empty(); });
                    if (owner_->submitStopRequested_ && owner_->submitQueue_.empty()) {
                        break;
                    }
                    submit = std::move(owner_->submitQueue_.front());
                    owner_->submitQueue_.pop_front();
                }
                SubmitOne(submit);
            }
        }

    private:
        void ResetActiveRequestCreditWait()
        {
            activeRequestCreditWaitFd_.Reset();
            activeRequestCreditSessionId_ = 0;
        }

        void SyncActiveRequestCreditWait()
        {
            const auto waitHandleUpdate =
                owner_->sessionTransport_.DuplicateRequestCreditWaitHandle(activeRequestCreditSessionId_);
            if (waitHandleUpdate.action == ClientSessionTransport::SessionWaitHandleUpdate::Action::KeepCurrent) {
                return;
            }
            ResetActiveRequestCreditWait();
            if (waitHandleUpdate.action == ClientSessionTransport::SessionWaitHandleUpdate::Action::ReplaceCurrent) {
                activeRequestCreditWaitFd_.Reset(waitHandleUpdate.fd);
                activeRequestCreditSessionId_ = waitHandleUpdate.sessionId;
            }
        }

        PollEventFdResult WaitForRequestCredit()
        {
            SyncActiveRequestCreditWait();
            if (activeRequestCreditWaitFd_.Get() < 0) {
                return owner_->CurrentSessionId() == 0 ? PollEventFdResult::Retry : PollEventFdResult::Failed;
            }

            owner_->submitterWaitingForCredit_.store(true, std::memory_order_release);
            [[maybe_unused]] const auto clearWaiting = MakeScopeExit([this] {
                owner_->submitterWaitingForCredit_.store(false, std::memory_order_release);
            });

            pollfd pollFd{activeRequestCreditWaitFd_.Get(), POLLIN, 0};
            while (owner_->WorkersShouldRun()) {
                const auto waitResult = PollEventFd(&pollFd, 100);
                if (waitResult == PollEventFdResult::Retry || waitResult == PollEventFdResult::Timeout) {
                    const uint64_t currentSessionId = owner_->CurrentSessionId();
                    if (currentSessionId != activeRequestCreditSessionId_) {
                        return PollEventFdResult::Retry;
                    }
                    continue;
                }
                if (waitResult == PollEventFdResult::Failed) {
                    const uint64_t currentSessionId = owner_->CurrentSessionId();
                    if (currentSessionId != activeRequestCreditSessionId_) {
                        return PollEventFdResult::Retry;
                    }
                    HILOGE("RpcClient::WaitForRequestCredit poll failed fd=%{public}d",
                           activeRequestCreditWaitFd_.Get());
                }
                return waitResult;
            }
            HILOGW("RpcClient::WaitForRequestCredit aborted because workers stopped");
            return PollEventFdResult::Failed;
        }

        StatusCode WaitUntilSessionReadyForSubmit()
        {
                while (owner_->WorkersShouldRun()) {
                const StatusCode sessionStatus = owner_->EnsureLiveSession();
                if (sessionStatus == StatusCode::Ok) {
                    return StatusCode::Ok;
                }
                if (sessionStatus == StatusCode::CooldownActive) {
                    const StatusCode waitStatus =
                        owner_->WaitForCooldownToSettle(std::chrono::steady_clock::time_point::max());
                    if (waitStatus == StatusCode::Ok) {
                        continue;
                    }
                    HILOGE("RpcClient::SubmitOne recovery wait failed: status=%{public}d",
                           static_cast<int>(waitStatus));
                    return waitStatus;
                }
                if (sessionStatus == StatusCode::PeerDisconnected) {
                    const StatusCode waitStatus =
                        owner_->WaitOneRecoveryRetryTick(std::chrono::steady_clock::time_point::max());
                    if (waitStatus == StatusCode::Ok) {
                        continue;
                    }
                    HILOGE("RpcClient::SubmitOne recovery retry failed: status=%{public}d",
                           static_cast<int>(waitStatus));
                    return waitStatus;
                }
                HILOGE("RpcClient::SubmitOne session unavailable: status=%{public}d",
                       static_cast<int>(sessionStatus));
                return sessionStatus;
            }
            return owner_->RuntimeStopStatus();
        }

        StatusCode TryAdmitSubmit(const PendingSubmit& submit)
        {
            const StatusCode pushStatus = owner_->TryPushRequest(submit);
            if (pushStatus == StatusCode::Ok) {
                return StatusCode::Ok;
            }
            if (pushStatus == StatusCode::PayloadTooLarge) {
                HILOGE("RpcClient::SubmitOne payload rejected: request_id=%{public}llu opcode=%{public}u",
                       static_cast<unsigned long long>(submit.requestId),
                       submit.call.opcode);
                return pushStatus;
            }
            if (pushStatus == StatusCode::PeerDisconnected) {
                owner_->CloseLiveSessionForTransportFailure();
                std::this_thread::sleep_for(kDisconnectedPushRetryDelay);
                return pushStatus;
            }
            if (pushStatus == StatusCode::QueueFull) {
                const auto waitResult = WaitForRequestCredit();
                if (waitResult == PollEventFdResult::Ready || waitResult == PollEventFdResult::Retry) {
                    return pushStatus;
                }
                HILOGE("RpcClient::SubmitOne request credit wait failed: request_id=%{public}llu opcode=%{public}u",
                       static_cast<unsigned long long>(submit.requestId),
                       submit.call.opcode);
                owner_->CloseLiveSessionForTransportFailure();
                return StatusCode::PeerDisconnected;
            }
            HILOGE("RpcClient::SubmitOne push failed: request_id=%{public}llu opcode=%{public}u status=%{public}d",
                   static_cast<unsigned long long>(submit.requestId),
                   submit.call.opcode,
                   static_cast<int>(pushStatus));
            return pushStatus;
        }

        void ResolveSubmitFailure(const PendingSubmit& submit, const PendingInfo& info, StatusCode status)
        {
            if (!owner_->WorkersShouldRun()) {
                HILOGE("RpcClient::SubmitOne aborted because workers stopped: request_id=%{public}llu opcode=%{public}u",
                       static_cast<unsigned long long>(submit.requestId),
                       submit.call.opcode);
            }
            owner_->FailAndResolve(info, status, submit.future);
        }

        void SubmitOne(const PendingSubmit& submit)
        {
            PendingInfo info = owner_->MakePendingInfo(submit);
            while (owner_->WorkersShouldRun()) {
                const StatusCode sessionStatus = WaitUntilSessionReadyForSubmit();
                if (sessionStatus != StatusCode::Ok) {
                    ResolveSubmitFailure(submit, info, sessionStatus);
                    return;
                }

                const StatusCode pushStatus = TryAdmitSubmit(submit);
                if (pushStatus == StatusCode::Ok) {
                    return;
                }
                if (pushStatus == StatusCode::PeerDisconnected || pushStatus == StatusCode::QueueFull) {
                    continue;
                }
                ResolveSubmitFailure(submit, info, pushStatus);
                return;
            }
            ResolveSubmitFailure(submit, info, owner_->RuntimeStopStatus());
        }

        Impl* owner_;
        UniqueFd activeRequestCreditWaitFd_;
        uint64_t activeRequestCreditSessionId_ = 0;
    };

    class ResponseWorker {
    public:
        explicit ResponseWorker(Impl& owner)
            : owner_(&owner)
        {
        }

        void Run()
        {
            UniqueFd activePollFd;
            uint64_t activePollSessionId = 0;
            const auto resetActivePollFd = [&]() { ResetActivePollFd(&activePollFd, &activePollSessionId); };
            const auto closeActivePollFd = MakeScopeExit([&]() { resetActivePollFd(); });

            while (owner_->WorkersShouldRun()) {
                if (DrainResponseRing()) {
                    continue;
                }

                if (!WaitUntilResponsePollReady(&activePollFd, &activePollSessionId, resetActivePollFd)) {
                    continue;
                }
                WaitForResponseSignal(activePollFd.Get(), activePollSessionId, resetActivePollFd);
            }
        }

    private:
        static void ResetActivePollFd(UniqueFd* activePollFd, uint64_t* activePollSessionId)
        {
            activePollFd->Reset();
            *activePollSessionId = 0;
        }

        static StatusCode ResolveEntryPayload(const ResponseRingEntry& entry, std::vector<uint8_t>* payload)
        {
            if (payload == nullptr) {
                return StatusCode::InvalidArgument;
            }
            payload->clear();
            if (entry.resultSize > ResponseRingEntry::INLINE_PAYLOAD_BYTES) {
                return StatusCode::ProtocolMismatch;
            }
            if (entry.payloadKind == PAYLOAD_KIND_INLINE) {
                payload->assign(entry.payload.begin(), entry.payload.begin() + entry.resultSize);
                return StatusCode::Ok;
            }
            if (entry.payloadKind == PAYLOAD_KIND_FILE_REF) {
                return ResolveFilePayloadFromTransport(DEFAULT_FILE_PAYLOAD_DIR,
                                                       entry.payload.data(),
                                                       entry.resultSize,
                                                       entry.payloadKind,
                                                       payload)
                         ? StatusCode::Ok
                         : StatusCode::PeerDisconnected;
            }
            return StatusCode::ProtocolMismatch;
        }

        static void DiscardEntryPayloadIfPresent(const ResponseRingEntry& entry)
        {
            if (entry.payloadKind != PAYLOAD_KIND_FILE_REF) {
                return;
            }
            (void)RemoveFilePayloadFromTransport(DEFAULT_FILE_PAYLOAD_DIR,
                                                 entry.payload.data(),
                                                 entry.resultSize,
                                                 entry.payloadKind);
        }

        void SyncActivePollSession(UniqueFd* activePollFd,
                                   uint64_t* activePollSessionId,
                                   const std::function<void()>& resetActivePollFd)
        {
            const auto waitHandleUpdate = owner_->sessionTransport_.DuplicateResponseWaitHandle(*activePollSessionId);
            if (waitHandleUpdate.action == ClientSessionTransport::SessionWaitHandleUpdate::Action::KeepCurrent) {
                return;
            }
            resetActivePollFd();
            if (waitHandleUpdate.action == ClientSessionTransport::SessionWaitHandleUpdate::Action::ReplaceCurrent) {
                activePollFd->Reset(waitHandleUpdate.fd);
                *activePollSessionId = waitHandleUpdate.sessionId;
            }
        }

        bool PrepareToWaitForResponses(UniqueFd* activePollFd,
                                       uint64_t* activePollSessionId,
                                       const std::function<void()>& resetActivePollFd)
        {
            // Draining may finish on one session while recovery has already swapped in another.
            // Re-sync the duplicated poll fd before blocking so the wait always tracks the live session.
            SyncActivePollSession(activePollFd, activePollSessionId, resetActivePollFd);
            return activePollFd->Get() >= 0;
        }

        bool WaitUntilResponsePollReady(UniqueFd* activePollFd,
                                        uint64_t* activePollSessionId,
                                        const std::function<void()>& resetActivePollFd)
        {
            while (owner_->WorkersShouldRun()) {
                const uint64_t observedWakeGeneration = owner_->RecoveryNotificationGeneration();
                if (PrepareToWaitForResponses(activePollFd, activePollSessionId, resetActivePollFd)) {
                    return true;
                }
                owner_->WaitForRecoveryNotification(observedWakeGeneration);
            }
            return false;
        }

        void WaitForResponseSignal(int activePollFd,
                                   uint64_t activePollSessionId,
                                   const std::function<void()>& resetActivePollFd)
        {
            pollfd pollFd{activePollFd, POLLIN, 0};
            const auto waitResult = PollEventFd(&pollFd, 100);
            if (waitResult != PollEventFdResult::Failed) {
                return;
            }

            HILOGE("RpcClient::ResponseLoop detected response poll failure session_id=%{public}llu",
                   static_cast<unsigned long long>(activePollSessionId));
            resetActivePollFd();
            owner_->HandleEngineDeath(activePollSessionId, activePollSessionId);
        }

        void ResolveCompletedRequest(const ResponseRingEntry& entry)
        {
            std::optional<PendingRequest> pending = owner_->requestStore_.Take(entry.requestId);
            if (!pending.has_value()) {
                HILOGW("RpcClient::ResolveCompletedRequest ignored late reply request_id=%{public}llu",
                       static_cast<unsigned long long>(entry.requestId));
                DiscardEntryPayloadIfPresent(entry);
                return;
            }
            RpcReply reply;
            reply.status = static_cast<StatusCode>(entry.statusCode);
            const StatusCode payloadStatus = ResolveEntryPayload(entry, &reply.payload);
            if (payloadStatus != StatusCode::Ok) {
                HILOGE("RpcClient::ResolveCompletedRequest failed to resolve payload request_id=%{public}llu "
                       "payload_kind=%{public}u payload_status=%{public}d",
                       static_cast<unsigned long long>(entry.requestId),
                       entry.payloadKind,
                       static_cast<int>(payloadStatus));
                reply.status = payloadStatus;
                reply.payload.clear();
            }
            if (reply.status != StatusCode::Ok) {
                owner_->ApplyFailureRecoveryDecision(pending->info, reply.status);
            }
            owner_->ResolveState(pending->future, std::move(reply));
            owner_->TouchActivity();
        }

        void DeliverEvent(const ResponseRingEntry& entry)
        {
            RpcEventCallback callback;
            {
                std::lock_guard<std::mutex> lock(owner_->eventMutex_);
                callback = owner_->eventCallback_;
            }
            if (!callback) {
                DiscardEntryPayloadIfPresent(entry);
                return;
            }
            RpcEvent event;
            event.eventDomain = entry.eventDomain;
            event.eventType = entry.eventType;
            event.flags = entry.flags;
            const StatusCode payloadStatus = ResolveEntryPayload(entry, &event.payload);
            if (payloadStatus != StatusCode::Ok) {
                HILOGE("RpcClient::DeliverEvent failed to resolve payload event_domain=%{public}u "
                       "event_type=%{public}u payload_kind=%{public}u status=%{public}d",
                       entry.eventDomain,
                       entry.eventType,
                       entry.payloadKind,
                       static_cast<int>(payloadStatus));
                const uint64_t observedSessionId = owner_->CurrentSessionId();
                owner_->HandleEngineDeath(observedSessionId, observedSessionId);
                return;
            }
            callback(event);
            owner_->TouchActivity();
        }

        bool DrainResponseRing()
        {
            bool drained = false;
            while (true) {
                ResponseRingEntry entry;
                bool ringBecameNotFull = false;
                int respCreditEventFd = -1;
                const bool popped = owner_->sessionTransport_.WithSessionLocked([&](Session& session) {
                    if (!session.Valid() || session.Header() == nullptr) {
                        return false;
                    }
                    const RingCursor& cursor = session.Header()->responseRing;
                    ringBecameNotFull = cursor.capacity != 0 && RingCount(cursor) == cursor.capacity;
                    if (!session.PopResponse(&entry)) {
                        return false;
                    }
                    respCreditEventFd = session.Handles().respCreditEventFd;
                    return true;
                });
                if (!popped) {
                    return drained;
                }
                drained = true;
                if (ringBecameNotFull) {
                    (void)SignalEventFd(respCreditEventFd);
                }
                if (entry.resultSize > ResponseRingEntry::INLINE_PAYLOAD_BYTES) {
                    HILOGE("RpcClient::DrainResponseRing detected oversized response: request_id=%{public}llu "
                           "result_size=%{public}u",
                           static_cast<unsigned long long>(entry.requestId),
                           entry.resultSize);
                    const uint64_t observedSessionId = owner_->CurrentSessionId();
                    owner_->HandleEngineDeath(observedSessionId, observedSessionId);
                    return true;
                }
                if (entry.messageKind == ResponseMessageKind::Event) {
                    DeliverEvent(entry);
                } else {
                    ResolveCompletedRequest(entry);
                }
            }
        }

        Impl* owner_;
    };

    static std::shared_ptr<Impl> Create(std::shared_ptr<IBootstrapChannel> bootstrap)
    {
        auto impl = std::make_shared<Impl>(std::move(bootstrap));
        impl->InstallBootstrapDeathCallback();
        return impl;
    }

    explicit Impl(std::shared_ptr<IBootstrapChannel> bootstrap)
        : sessionTransport_(std::move(bootstrap))
    {
    }

    // Component ownership: session transport, recovery state, request stores, and
    // worker threads each have a dedicated helper. Impl keeps only the shared
    // orchestration and cross-component glue.
    // Session transport and external callbacks.
    // Session transport boundary: bootstrap binding, session attach/detach, snapshot publication.
    ClientSessionTransport sessionTransport_;
    mutable std::mutex submitMutex_;
    mutable std::mutex eventMutex_;
    mutable std::mutex watchdogMutex_;
    RpcEventCallback eventCallback_;

    // Recovery lifecycle and policy.
    // Recovery boundary: lifecycle state, cooldown windows, callbacks, waiter wakeups.
    ClientRecoveryState recoveryState_;
    mutable std::mutex recoveryPolicyMutex_;
    RecoveryPolicy recoveryPolicy_;
    mutable std::mutex lifecycleMutex_;

    // Submission and in-flight request state.
    std::deque<PendingSubmit> submitQueue_;
    bool submitStopRequested_ = false;
    bool watchdogStopRequested_ = false;
    // Request-store boundary: admitted in-flight requests only.
    ClientRequestStore requestStore_;

    // Worker coordination.
    std::condition_variable submitCv_;
    std::condition_variable watchdogCv_;
    SubmitWorker submitWorker_{*this};
    ResponseWorker responseWorker_{*this};
    std::thread submitThread_;
    std::thread responseThread_;
    std::thread watchdogThread_;
    mutable std::mutex runtimeMutex_;
    std::atomic<ApiLifecycleState> clientControlState_{ApiLifecycleState::Uninitialized};
    std::atomic<bool> workersRunning_{false};

    // Runtime counters and snapshots.
    std::atomic<bool> submitterWaitingForCredit_{false};
    std::atomic<uint64_t> nextRequestId_{1};
    std::atomic<uint64_t> lastActivityMs_{0};

    // Shared state snapshots and callback plumbing.
    ApiLifecycleState LoadClientControlState() const
    {
        return clientControlState_.load(std::memory_order_acquire);
    }

    bool IsApiOpen() const
    {
        return LoadClientControlState() == ApiLifecycleState::Open;
    }

    bool CanEnsureLiveSession() const
    {
        const ApiLifecycleState state = LoadClientControlState();
        return state == ApiLifecycleState::Uninitialized || state == ApiLifecycleState::Open;
    }

    bool IsApiTerminal() const
    {
        return LoadClientControlState() == ApiLifecycleState::Closed;
    }

    bool AcceptsInvoke() const
    {
        return LoadClientControlState() == ApiLifecycleState::Open;
    }

    bool FinishInit()
    {
        ApiLifecycleState expected = ApiLifecycleState::Uninitialized;
        if (clientControlState_.compare_exchange_strong(expected,
                                                        ApiLifecycleState::Open,
                                                        std::memory_order_acq_rel,
                                                        std::memory_order_acquire)) {
            return true;
        }
        return expected == ApiLifecycleState::Open;
    }

    bool CanStartShutdown() const
    {
        const ApiLifecycleState state = LoadClientControlState();
        return state != ApiLifecycleState::Closing && state != ApiLifecycleState::Closed;
    }

    bool BeginShutdown()
    {
        ApiLifecycleState expected = LoadClientControlState();
        while (true) {
            if (!CanStartShutdown()) {
                return false;
            }
            if (clientControlState_.compare_exchange_weak(expected,
                                                          ApiLifecycleState::Closing,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_acquire)) {
                return true;
            }
        }
    }

    void FinishShutdown()
    {
        clientControlState_.store(ApiLifecycleState::Closed, std::memory_order_release);
    }

    bool WorkersShouldRun() const
    {
        return workersRunning_.load(std::memory_order_acquire);
    }

    bool StartRuntime()
    {
        std::lock_guard<std::mutex> lock(runtimeMutex_);
        if (workersRunning_.load(std::memory_order_acquire)) {
            return false;
        }
        workersRunning_.store(true, std::memory_order_release);
        {
            std::lock_guard<std::mutex> submitLock(submitMutex_);
            submitStopRequested_ = false;
        }
        {
            std::lock_guard<std::mutex> watchdogLock(watchdogMutex_);
            watchdogStopRequested_ = false;
        }
        return true;
    }

    bool StopRuntime()
    {
        std::lock_guard<std::mutex> lock(runtimeMutex_);
        if (!workersRunning_.load(std::memory_order_acquire)) {
            return false;
        }
        workersRunning_.store(false, std::memory_order_release);
        return true;
    }

    StatusCode RuntimeStopStatus() const
    {
        return IsApiOpen() ? StatusCode::PeerDisconnected : StatusCode::ClientClosed;
    }

    StatusCode AdmissionStatusForInvoke() const
    {
        return AcceptsInvoke() ? StatusCode::Ok : StatusCode::ClientClosed;
    }

    RpcFuture RejectInvoke(const RpcCall& call, StatusCode status)
    {
        auto state = std::make_shared<RpcFuture::State>();
        const uint64_t requestId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t sessionId = CurrentSessionId();
        PendingInfo info;
        info.opcode = call.opcode;
        info.priority = call.priority;
        info.execTimeoutMs = call.execTimeoutMs;
        info.requestId = requestId;
        info.sessionId = sessionId;
        if (status != StatusCode::ClientClosed) {
            HILOGE("RpcClient::InvokeAsync rejected because client not running opcode=%{public}u status=%{public}d",
                   call.opcode,
                   static_cast<int>(status));
            ApplyFailureRecoveryDecision(info, status);
        }
        RpcReply reply;
        reply.status = status;
        ResolveState(state, std::move(reply));
        return RpcFuture(state);
    }

    void EnqueueSubmitLocked(RpcCall&& call, const std::shared_ptr<RpcFuture::State>& futureState)
    {
        PendingSubmit submit;
        submit.requestId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);
        submit.call = std::move(call);
        submit.future = futureState;
        submitQueue_.push_back(std::move(submit));
    }

    void ClearCallbacks()
    {
        {
            std::lock_guard<std::mutex> lock(recoveryPolicyMutex_);
            recoveryPolicy_ = {};
        }
        recoveryState_.ClearRecoveryEventCallback();
        sessionTransport_.ClearDeathCallback();
    }

    void FailQueuedSubmissions(StatusCode status)
    {
        std::deque<PendingSubmit> queued;
        {
            std::lock_guard<std::mutex> lock(submitMutex_);
            queued.swap(submitQueue_);
        }
        for (auto& submit : queued) {
            RpcReply reply;
            reply.status = status;
            ResolveState(submit.future, std::move(reply));
        }
    }

    void FailOutstandingWork(StatusCode status)
    {
        FailAllPendingWithRecoveryPolicy(status);
        FailQueuedSubmissions(status);
    }

    void TouchActivity()
    {
        lastActivityMs_.store(MonotonicNowMs(), std::memory_order_release);
    }

    uint64_t CurrentSessionId() const
    {
        return sessionTransport_.CurrentSessionId();
    }

    void SetBootstrapChannel(std::shared_ptr<IBootstrapChannel> bootstrap)
    {
        sessionTransport_.SetBootstrapChannel(std::move(bootstrap), MakeBootstrapDeathCallback());
    }

    void SetRecoveryEventCallback(RecoveryEventCallback callback)
    {
        recoveryState_.SetRecoveryEventCallback(std::move(callback));
    }

    ClientLifecycleState LifecycleState() const
    {
        return recoveryState_.LifecycleState();
    }

    void PrepareForInitOpen(ClientLifecycleState initialState)
    {
        recoveryState_.PrepareForInitOpen(initialState);
    }

    void FinalizeSessionOpen()
    {
        recoveryState_.FinalizeSessionOpen(CurrentSessionId());
    }

    std::function<void(uint64_t)> MakeBootstrapDeathCallback()
    {
        std::weak_ptr<Impl> weakSelf = weak_from_this();
        return [weakSelf](uint64_t sessionId) {
            const auto self = weakSelf.lock();
            if (self == nullptr) {
                return;
            }
            self->HandleEngineDeath(self->CurrentSessionId(), sessionId);
        };
    }

    void InstallBootstrapDeathCallback()
    {
        sessionTransport_.InstallDeathCallback(MakeBootstrapDeathCallback());
    }

    RecoveryPolicy LoadRecoveryPolicy() const
    {
        std::lock_guard<std::mutex> lock(recoveryPolicyMutex_);
        return recoveryPolicy_;
    }

    void LogIgnoredStaleRecovery(const char* context, uint64_t observedSessionId, uint64_t currentSessionId) const
    {
        HILOGI("%{public}s ignored stale recovery: observed_session_id=%{public}llu current_session_id=%{public}llu",
               context,
               static_cast<unsigned long long>(observedSessionId),
               static_cast<unsigned long long>(currentSessionId));
    }

    bool IsStaleObservedSession(uint64_t observedSessionId, uint64_t currentSessionId) const
    {
        return currentSessionId != observedSessionId;
    }

    void NotifyRecoveryStateChanged()
    {
        recoveryState_.NotifyRecoveryStateChanged();
    }

    [[nodiscard]] uint64_t RecoveryNotificationGeneration() const
    {
        return recoveryState_.NotificationGeneration();
    }

    void WaitForRecoveryNotification(uint64_t observedGeneration)
    {
        recoveryState_.WaitForNotification(observedGeneration, workersRunning_);
    }

    // Shared reply resolution and recovery-decision helpers.
    PendingInfo MakePendingInfo(const PendingSubmit& submit) const
    {
        PendingInfo info;
        info.opcode = submit.call.opcode;
        info.priority = submit.call.priority;
        info.execTimeoutMs = submit.call.execTimeoutMs;
        info.requestId = submit.requestId;
        info.sessionId = CurrentSessionId();
        return info;
    }

    void ResolveState(const std::shared_ptr<RpcFuture::State>& state, RpcReply reply)
    {
        if (state == nullptr) {
            return;
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        if (state->ready) {
            return;
        }
        state->reply = std::move(reply);
        state->ready = true;
        state->cv.notify_all();
    }

    RpcFailure BuildFailureReport(const PendingInfo& info, StatusCode status) const
    {
        RpcFailure failure;
        failure.status = status;
        failure.opcode = info.opcode;
        failure.priority = info.priority;
        failure.execTimeoutMs = info.execTimeoutMs;
        failure.requestId = info.requestId;
        failure.sessionId = info.sessionId;
        return failure;
    }

    // Each recovery source closes the session the same way, but differs in how
    // already-admitted requests should be completed afterwards.
    void HandlePendingRequestsForRecoveryLocked(PendingRequestRecoveryAction action)
    {
        switch (action) {
            case PendingRequestRecoveryAction::FailWithRecoveryPolicy:
                FailAllPendingWithRecoveryPolicyLocked(StatusCode::PeerDisconnected);
                return;
            case PendingRequestRecoveryAction::ResolvePeerDisconnected:
                ResolveAllPending(StatusCode::PeerDisconnected);
                return;
            case PendingRequestRecoveryAction::KeepCurrentState:
                return;
        }
    }

    void ApplyRecoveryDecisionLocked(const RecoveryDecision& decision,
                                     uint64_t observedSessionId,
                                     PendingRequestRecoveryAction pendingAction)
    {
        if (!IsApiOpen()) {
            return;
        }
        switch (decision.action) {
            case RecoveryAction::Ignore:
                return;
            case RecoveryAction::IdleClose:
                CloseSessionAndEnterIdleClosedLocked(observedSessionId);
                return;
            case RecoveryAction::ManualShutdown:
                return;
            case RecoveryAction::Restart:
                ScheduleRecoveryLocked(observedSessionId, decision.delayMs, pendingAction);
                return;
        }
    }

    void ApplyFailureRecoveryDecisionLocked(const PendingInfo& info, StatusCode status)
    {
        if (!IsApiOpen()) {
            return;
        }
        const uint64_t currentSessionId = CurrentSessionId();
        const uint64_t observedSessionId = info.sessionId;
        if (IsStaleObservedSession(observedSessionId, currentSessionId)) {
            LogIgnoredStaleRecovery("RpcClient::ApplyFailureRecoveryDecision", observedSessionId, currentSessionId);
            return;
        }
        const RecoveryPolicy policy = LoadRecoveryPolicy();
        if (!policy.onFailure) {
            return;
        }
        const RpcFailure failure = BuildFailureReport(info, status);
        const RecoveryDecision decision = policy.onFailure(failure);
        ApplyRecoveryDecisionLocked(decision, observedSessionId, PendingRequestRecoveryAction::FailWithRecoveryPolicy);
    }

    void ApplyFailureRecoveryDecision(const PendingInfo& info, StatusCode status)
    {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        ApplyFailureRecoveryDecisionLocked(info, status);
    }

    void HandleIdleRecovery(uint64_t observedSessionId, uint64_t idleMs)
    {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (!IsApiOpen()) {
            return;
        }
        const uint64_t currentSessionId = CurrentSessionId();
        if (IsStaleObservedSession(observedSessionId, currentSessionId)) {
            LogIgnoredStaleRecovery("RpcClient::HandleIdleRecovery", observedSessionId, currentSessionId);
            return;
        }
        const RecoveryPolicy policy = LoadRecoveryPolicy();
        if (!policy.onIdle) {
            return;
        }
        const RecoveryDecision decision = policy.onIdle(idleMs);
        ApplyRecoveryDecisionLocked(decision, observedSessionId, PendingRequestRecoveryAction::FailWithRecoveryPolicy);
    }

    void HandleExternalRecoveryLocked(uint64_t observedSessionId, ExternalRecoveryRequest request)
    {
        if (request.sessionId != 0 && request.sessionId != observedSessionId) {
            HILOGW("RpcClient::RequestExternalRecovery ignored stale request: session_id=%{public}llu "
                   "observed_session_id=%{public}llu",
                   static_cast<unsigned long long>(request.sessionId),
                   static_cast<unsigned long long>(observedSessionId));
            return;
        }
        HILOGW("RpcClient::RequestExternalRecovery requested: session_id=%{public}llu delay_ms=%{public}u",
               static_cast<unsigned long long>(request.sessionId),
               request.delayMs);
        ScheduleRecoveryLocked(observedSessionId, request.delayMs, PendingRequestRecoveryAction::ResolvePeerDisconnected);
    }

    void HandleExternalRecovery(uint64_t observedSessionId, ExternalRecoveryRequest request)
    {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (!IsApiOpen()) {
            return;
        }
        const uint64_t currentSessionId = CurrentSessionId();
        if (IsStaleObservedSession(observedSessionId, currentSessionId)) {
            LogIgnoredStaleRecovery("RpcClient::HandleExternalRecovery", observedSessionId, currentSessionId);
            return;
        }
        HandleExternalRecoveryLocked(observedSessionId, request);
    }

    void HandleEngineDeathLocked(uint64_t observedSessionId, uint64_t deadSessionId)
    {
        if (observedSessionId == 0) {
            HILOGI("RpcClient::HandleEngineDeath ignored because observed session is no longer live");
            return;
        }
        const uint64_t resolvedDeadSessionId = deadSessionId != 0 ? deadSessionId : observedSessionId;
        if (resolvedDeadSessionId != 0 && observedSessionId != 0 && resolvedDeadSessionId != observedSessionId) {
            HILOGI("RpcClient::HandleEngineDeath ignored mismatched event dead_session_id=%{public}llu "
                   "observed_session_id=%{public}llu",
                   static_cast<unsigned long long>(resolvedDeadSessionId),
                   static_cast<unsigned long long>(observedSessionId));
            return;
        }

        EngineDeathReport report;
        report.deadSessionId = resolvedDeadSessionId;

        sessionTransport_.CloseLiveSessionIfSnapshotMatches(observedSessionId);
        ResolveAllPending(StatusCode::CrashedDuringExecution);

        const RecoveryPolicy policy = LoadRecoveryPolicy();
        if (!policy.onEngineDeath) {
            HILOGE("RpcClient::HandleEngineDeath has no recovery policy session_id=%{public}llu",
                   static_cast<unsigned long long>(report.deadSessionId));
            EnterNoSessionWithoutLiveSessionLocked();
            return;
        }

        const RecoveryDecision decision = policy.onEngineDeath(report);
        if (decision.action == RecoveryAction::Ignore) {
            HILOGW("RpcClient::HandleEngineDeath policy ignored recovery dead_session_id=%{public}llu",
                   static_cast<unsigned long long>(report.deadSessionId));
            EnterNoSessionWithoutLiveSessionLocked();
            return;
        }
        ApplyRecoveryDecisionLocked(decision, observedSessionId, PendingRequestRecoveryAction::KeepCurrentState);
    }

    void HandleEngineDeath(uint64_t observedSessionId, uint64_t deadSessionId)
    {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (!IsApiOpen()) {
            return;
        }
        const uint64_t currentSessionId = CurrentSessionId();
        if (IsStaleObservedSession(observedSessionId, currentSessionId)) {
            LogIgnoredStaleRecovery("RpcClient::HandleEngineDeath", observedSessionId, currentSessionId);
            return;
        }
        HandleEngineDeathLocked(observedSessionId, deadSessionId);
    }

    void FailAndResolve(const PendingInfo& info, StatusCode status, const std::shared_ptr<RpcFuture::State>& future)
    {
        ApplyFailureRecoveryDecision(info, status);
        RpcReply reply;
        reply.status = status;
        ResolveState(future, std::move(reply));
    }

    void FailAndResolveLocked(const PendingInfo& info, StatusCode status, const std::shared_ptr<RpcFuture::State>& future)
    {
        ApplyFailureRecoveryDecisionLocked(info, status);
        RpcReply reply;
        reply.status = status;
        ResolveState(future, std::move(reply));
    }

    void ResolveAllPending(StatusCode status)
    {
        std::vector<PendingRequest> toFail = requestStore_.TakeAll();
        for (auto& pending : toFail) {
            RpcReply reply;
            reply.status = status;
            ResolveState(pending.future, std::move(reply));
        }
    }

    void FailAllPendingWithRecoveryPolicy(StatusCode status)
    {
        std::vector<PendingRequest> toFail = requestStore_.TakeAll();
        for (auto& pending : toFail) {
            FailAndResolve(pending.info, status, pending.future);
        }
    }

    void FailAllPendingWithRecoveryPolicyLocked(StatusCode status)
    {
        std::vector<PendingRequest> toFail = requestStore_.TakeAll();
        for (auto& pending : toFail) {
            FailAndResolveLocked(pending.info, status, pending.future);
        }
    }

    std::chrono::steady_clock::time_point MakePendingWaitDeadline(uint32_t execTimeoutMs) const
    {
        if (execTimeoutMs == 0) {
            return std::chrono::steady_clock::time_point::max();
        }
        return std::chrono::steady_clock::now() + std::chrono::milliseconds{execTimeoutMs};
    }

    // Lifecycle and session orchestration.
    void HandleSessionOpenFailureLocked(StatusCode status)
    {
        recoveryState_.HandleSessionOpenFailure(status, CurrentSessionId());
    }

    void MaybeReconnectImmediatelyLocked(uint32_t delayMs)
    {
        if (delayMs == 0) {
            (void)EnsureLiveSessionLocked();
        }
    }

    StatusCode OpenSessionLocked()
    {
        const StatusCode status = sessionTransport_.OpenSession();
        if (status != StatusCode::Ok) {
            return status;
        }
        TouchActivity();
        FinalizeSessionOpen();
        return StatusCode::Ok;
    }

    void CloseLiveSessionLocked()
    {
        (void)sessionTransport_.CloseLiveSession();
        NotifyRecoveryStateChanged();
    }

    void CloseLiveSessionForTransportFailure()
    {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        CloseLiveSessionLocked();
    }

    void CloseSessionAndEnterIdleClosedLocked(uint64_t observedSessionId)
    {
        if (!IsApiOpen()) {
            return;
        }
        sessionTransport_.CloseLiveSessionIfSnapshotMatches(observedSessionId);
        recoveryState_.EnterIdleClosed(CurrentSessionId());
    }

    // This transition only updates recovery state. Callers must already know
    // there is no live session, either because recovery open failed before one
    // existed or because the session was already closed earlier in the path.
    void EnterNoSessionWithoutLiveSessionLocked()
    {
        if (!IsApiOpen()) {
            return;
        }
        recoveryState_.EnterNoSession(CurrentSessionId());
    }

    void CloseSessionAndEnterTerminalClosedLocked()
    {
        CloseLiveSessionLocked();
        recoveryState_.EnterTerminalClosed(CurrentSessionId());
    }

    void ScheduleRecoveryLocked(uint64_t observedSessionId,
                                uint32_t delayMs,
                                PendingRequestRecoveryAction pendingAction)
    {
        if (!IsApiOpen()) {
            return;
        }
        recoveryState_.StartRecovery(delayMs, observedSessionId);
        sessionTransport_.CloseLiveSessionIfSnapshotMatches(observedSessionId);
        HandlePendingRequestsForRecoveryLocked(pendingAction);
        MaybeReconnectImmediatelyLocked(delayMs);
    }

    bool CooldownActive() const
    {
        return recoveryState_.CooldownActive();
    }

    std::chrono::milliseconds CooldownRemaining() const
    {
        return recoveryState_.CooldownRemaining();
    }

    bool RecoveryPending() const
    {
        return recoveryState_.RecoveryPending();
    }

    StatusCode EnsureLiveSessionLocked()
    {
        if (!CanEnsureLiveSession()) {
            HILOGW("RpcClient::EnsureLiveSession rejected: client not accepting session open");
            return StatusCode::ClientClosed;
        }
        if (CooldownActive()) {
            HILOGW("RpcClient::EnsureLiveSession delayed by cooldown remaining_ms=%{public}lld",
                   static_cast<long long>(CooldownRemaining().count()));
            return StatusCode::CooldownActive;
        }
        if (sessionTransport_.HasLiveSession()) {
            return StatusCode::Ok;
        }
        recoveryState_.BeginSessionOpen(LifecycleState(), CurrentSessionId());
        const StatusCode status = OpenSessionLocked();
        if (status != StatusCode::Ok) {
            HandleSessionOpenFailureLocked(status);
        }
        return status;
    }

    StatusCode EnsureLiveSession()
    {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        return EnsureLiveSessionLocked();
    }

    void StartThreads()
    {
        if (!StartRuntime()) {
            return;
        }
        submitThread_ = std::thread([this] { submitWorker_.Run(); });
        responseThread_ = std::thread([this] { responseWorker_.Run(); });
        watchdogThread_ = std::thread([this] { WatchdogLoop(); });
    }

    void StopThreads()
    {
        if (!StopRuntime()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(submitMutex_);
            submitStopRequested_ = true;
        }
        submitCv_.notify_all();
        {
            std::lock_guard<std::mutex> lock(watchdogMutex_);
            watchdogStopRequested_ = true;
            watchdogCv_.notify_all();
        }
        NotifyRecoveryStateChanged();
        if (submitThread_.joinable()) {
            submitThread_.join();
        }
        if (responseThread_.joinable()) {
            responseThread_.join();
        }
        if (watchdogThread_.joinable()) {
            watchdogThread_.join();
        }
    }

    void Shutdown()
    {
        if (!BeginShutdown()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
            CloseSessionAndEnterTerminalClosedLocked();
        }
        ClearCallbacks();
        StopThreads();
        FailOutstandingWork(StatusCode::ClientClosed);
        FinishShutdown();
    }

    // Recovery wait helpers bridge the two internal control dimensions:
    // worker threads may still be alive while the public control state has
    // already started shutdown and must reject new work.
    StatusCode WaitForCooldownToSettle(std::chrono::steady_clock::time_point deadline)
    {
        return recoveryState_.WaitForCooldownToSettle(deadline, workersRunning_, clientControlState_);
    }

    StatusCode WaitOneRecoveryRetryTick(std::chrono::steady_clock::time_point deadline)
    {
        return recoveryState_.WaitOneRecoveryRetryTick(deadline, workersRunning_, clientControlState_);
    }

    StatusCode ValidatePushSession(const PendingSubmit& submit, const Session& session) const
    {
        if (!session.Valid() || session.Header() == nullptr) {
            HILOGE("RpcClient::TryPushRequest failed: session not valid request_id=%{public}llu opcode=%{public}u",
                   static_cast<unsigned long long>(submit.requestId),
                   submit.call.opcode);
            return StatusCode::PeerDisconnected;
        }
        return StatusCode::Ok;
    }

    static StatusCode PrepareRequestPayloadForTransport(const PendingSubmit& submit,
                                                        const Session& session,
                                                        std::vector<uint8_t>* transportPayload,
                                                        uint8_t* payloadKind)
    {
        if (transportPayload == nullptr || payloadKind == nullptr) {
            return StatusCode::InvalidArgument;
        }
        const std::size_t maxTransportBytes =
            std::min<std::size_t>(session.MaxRequestBytes(), RequestRingEntry::INLINE_PAYLOAD_BYTES);
        if (!PrepareFilePayloadForTransport(
                DEFAULT_FILE_PAYLOAD_DIR, submit.call.payload, maxTransportBytes, transportPayload, payloadKind)) {
            HILOGE("RpcClient::TryPushRequest failed to prepare payload request_id=%{public}llu "
                   "opcode=%{public}u size=%{public}zu max=%{public}zu",
                   static_cast<unsigned long long>(submit.requestId),
                   submit.call.opcode,
                   submit.call.payload.size(),
                   maxTransportBytes);
            return StatusCode::PayloadTooLarge;
        }
        if (*payloadKind == PAYLOAD_KIND_FILE_REF) {
            HILOGW("RpcClient::TryPushRequest oversized request uses file payload: request_id=%{public}llu "
                   "opcode=%{public}u payload_size=%{public}zu envelope_size=%{public}zu max=%{public}zu",
                   static_cast<unsigned long long>(submit.requestId),
                   submit.call.opcode,
                   submit.call.payload.size(),
                   transportPayload->size(),
                   maxTransportBytes);
        }
        return StatusCode::Ok;
    }

    static RequestRingEntry BuildRequestEntry(const PendingSubmit& submit,
                                              const std::vector<uint8_t>& transportPayload,
                                              uint8_t payloadKind)
    {
        RequestRingEntry entry;
        entry.requestId = submit.requestId;
        entry.execTimeoutMs = submit.call.execTimeoutMs;
        entry.opcode = submit.call.opcode;
        entry.priority = static_cast<uint8_t>(submit.call.priority);
        entry.payloadSize = static_cast<uint32_t>(transportPayload.size());
        entry.payloadKind = payloadKind;
        if (!transportPayload.empty()) {
            std::memcpy(entry.payload.data(), transportPayload.data(), transportPayload.size());
        }
        return entry;
    }

    PendingRequest BuildPendingRequest(const PendingSubmit& submit) const
    {
        PendingRequest pending;
        pending.future = submit.future;
        pending.info = MakePendingInfo(submit);
        pending.waitDeadline = MakePendingWaitDeadline(submit.call.execTimeoutMs);
        return pending;
    }

    StatusCode PushRequestToSession(Session& session,
                                    const PendingSubmit& submit,
                                    const RequestRingEntry& entry,
                                    const std::vector<uint8_t>& transportPayload,
                                    uint8_t payloadKind)
    {
        const bool highPriority = IsHighPriority(submit.call);
        const StatusCode status =
            session.PushRequest(highPriority ? QueueKind::HighRequest : QueueKind::NormalRequest, entry);
        if (status != StatusCode::Ok) {
            (void)requestStore_.Take(submit.requestId);
            if (payloadKind == PAYLOAD_KIND_FILE_REF) {
                (void)RemoveFilePayloadFromTransport(DEFAULT_FILE_PAYLOAD_DIR,
                                                     transportPayload.data(),
                                                     transportPayload.size(),
                                                     payloadKind);
            }
            if (status != StatusCode::QueueFull) {
                HILOGE("RpcClient::TryPushRequest failed: status=%{public}d request_id=%{public}llu opcode=%{public}u",
                       static_cast<int>(status),
                       static_cast<unsigned long long>(submit.requestId),
                       submit.call.opcode);
            }
            return status;
        }

        const RingCursor& cursor = highPriority ? session.Header()->highRing : session.Header()->normalRing;
        if (RingCountIsOneAfterPush(cursor)) {
            const int fd = highPriority ? session.Handles().highReqEventFd : session.Handles().normalReqEventFd;
            if (!SignalEventFd(fd)) {
                HILOGW("RpcClient::TryPushRequest failed to signal request event fd=%{public}d request_id=%{public}llu",
                       fd,
                       static_cast<unsigned long long>(submit.requestId));
            }
        }
        TouchActivity();
        return StatusCode::Ok;
    }

    StatusCode TryPushRequest(const PendingSubmit& submit)
    {
        return sessionTransport_.WithSessionLocked([&](Session& session) -> StatusCode {
            const StatusCode validationStatus = ValidatePushSession(submit, session);
            if (validationStatus != StatusCode::Ok) {
                return validationStatus;
            }

            std::vector<uint8_t> transportPayload;
            uint8_t payloadKind = PAYLOAD_KIND_INLINE;
            const StatusCode prepareStatus =
                PrepareRequestPayloadForTransport(submit, session, &transportPayload, &payloadKind);
            if (prepareStatus != StatusCode::Ok) {
                return prepareStatus;
            }

            const RequestRingEntry entry = BuildRequestEntry(submit, transportPayload, payloadKind);
            requestStore_.Put(submit.requestId, BuildPendingRequest(submit));
            return PushRequestToSession(session, submit, entry, transportPayload, payloadKind);
        });
    }

    // Watchdog-driven monitoring and recovery-decision hooks.
    void MaybeRunPendingTimeouts()
    {
        const auto now = std::chrono::steady_clock::now();
        std::vector<PendingRequest> expired = requestStore_.TakeExpired(now);
        for (auto& pending : expired) {
            FailAndResolve(pending.info, StatusCode::ExecTimeout, pending.future);
        }
    }

    void MaybeRunIdlePolicy()
    {
        if (!IsApiOpen()) {
            return;
        }
        const RecoveryPolicy policy = LoadRecoveryPolicy();
        if (!policy.onIdle) {
            return;
        }
        if (!requestStore_.Empty()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(submitMutex_);
            if (!submitQueue_.empty()) {
                return;
            }
        }
        const uint64_t observedSessionId = CurrentSessionId();
        if (observedSessionId == 0) {
            return;
        }
        const uint64_t nowMs = MonotonicNowMs();
        const uint64_t idleMs = nowMs - lastActivityMs_.load(std::memory_order_acquire);
        HandleIdleRecovery(observedSessionId, idleMs);
    }

    bool ShouldRequestRecoveryForHealthStatus(ChannelHealthStatus status) const
    {
        switch (status) {
            case ChannelHealthStatus::Timeout:
            case ChannelHealthStatus::Malformed:
            case ChannelHealthStatus::Unhealthy:
            case ChannelHealthStatus::SessionMismatch:
                return true;
            case ChannelHealthStatus::Healthy:
            case ChannelHealthStatus::Unsupported:
                return false;
        }
        return false;
    }

    void RequestHealthCheckRecovery(ChannelHealthStatus status, uint64_t observedSessionId)
    {
        if (!ShouldRequestRecoveryForHealthStatus(status)) {
            return;
        }

        HILOGE("RpcClient::MaybeRunHealthCheck status=%{public}d session_id=%{public}llu",
               static_cast<int>(status),
               static_cast<unsigned long long>(observedSessionId));
        HandleExternalRecovery(observedSessionId, {observedSessionId, 0});
    }

    void MaybeRunHealthCheck()
    {
        if (!IsApiOpen()) {
            return;
        }
        std::shared_ptr<IBootstrapChannel> bootstrap = sessionTransport_.LoadBootstrap();
        if (bootstrap == nullptr) {
            return;
        }
        const uint64_t observedSessionId = CurrentSessionId();
        if (observedSessionId == 0) {
            return;
        }

        const ChannelHealthResult result = bootstrap->CheckHealth(observedSessionId);
        RequestHealthCheckRecovery(result.status, observedSessionId);
    }

    void WatchdogLoop()
    {
        auto nextPendingTimeoutScan = std::chrono::steady_clock::now();
        auto nextHealthCheck = nextPendingTimeoutScan;
        auto nextIdlePoll = nextPendingTimeoutScan;

        while (true) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= nextPendingTimeoutScan) {
                MaybeRunPendingTimeouts();
                nextPendingTimeoutScan = std::chrono::steady_clock::now() + kPendingTimeoutPollPeriod;
            }
            if (now >= nextHealthCheck) {
                MaybeRunHealthCheck();
                nextHealthCheck = std::chrono::steady_clock::now() + kHealthCheckPeriod;
            }
            if (now >= nextIdlePoll) {
                MaybeRunIdlePolicy();
                nextIdlePoll = std::chrono::steady_clock::now() + kIdlePollPeriod;
            }

            std::unique_lock<std::mutex> lock(watchdogMutex_);
            const auto nextWakeAt = std::min(nextPendingTimeoutScan, std::min(nextHealthCheck, nextIdlePoll));
            watchdogCv_.wait_until(lock, nextWakeAt, [this] {
                return watchdogStopRequested_;
            });
            if (watchdogStopRequested_) {
                break;
            }
        }
    }

    void RequestExternalRecovery(ExternalRecoveryRequest request)
    {
        if (!IsApiOpen()) {
            return;
        }
        HandleExternalRecovery(CurrentSessionId(), request);
    }

    // Public-operation entrypoints and runtime introspection.
    RpcFuture InvokeAsync(RpcCall call)
    {
        auto futureState = std::make_shared<RpcFuture::State>();
        {
            std::lock_guard<std::mutex> lock(submitMutex_);
            const StatusCode status = AdmissionStatusForInvoke();
            if (status != StatusCode::Ok) {
                return RejectInvoke(call, status);
            }
            EnqueueSubmitLocked(std::move(call), futureState);
        }
        submitCv_.notify_one();
        return RpcFuture(futureState);
    }

    RecoveryRuntimeSnapshot GetRecoveryRuntimeSnapshot() const
    {
        return recoveryState_.GetSnapshot(CurrentSessionId());
    }

    StatusCode RetryUntilRecoverySettles(const std::function<StatusCode()>& invoke)
    {
        if (!invoke) {
            HILOGE("RpcClient::RetryUntilRecoverySettles failed: invoke is null");
            return StatusCode::InvalidArgument;
        }

        const auto computeWaitBudget = [](const RecoveryRuntimeSnapshot& snapshot) {
            return std::chrono::milliseconds{snapshot.cooldownRemainingMs} + kRetryUntilRecoverySettlesGrace;
        };

        auto deadline = std::chrono::steady_clock::now() + computeWaitBudget(GetRecoveryRuntimeSnapshot());
        StatusCode status = invoke();
        if (status == StatusCode::Ok) {
            return status;
        }
        while (true) {
            const auto snapshot = GetRecoveryRuntimeSnapshot();
            deadline = std::max(deadline, std::chrono::steady_clock::now() + computeWaitBudget(snapshot));
            if (!ShouldRetryRecoveryStatus(status, snapshot)) {
                return status;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                HILOGE("RpcClient::RetryUntilRecoverySettles timed out: status=%{public}d "
                       "lifecycle=%{public}d cooldown_ms=%{public}u",
                       static_cast<int>(status),
                       static_cast<int>(snapshot.lifecycleState),
                       snapshot.cooldownRemainingMs);
                return status;
            }

            StatusCode waitStatus = StatusCode::PeerDisconnected;
            if (snapshot.lifecycleState == ClientLifecycleState::Cooldown) {
                waitStatus = WaitForCooldownToSettle(deadline);
            } else if (snapshot.lifecycleState == ClientLifecycleState::Recovering) {
                waitStatus = WaitOneRecoveryRetryTick(deadline);
            }
            if (waitStatus != StatusCode::Ok) {
                return status;
            }
            status = invoke();
        }
    }
};

RpcClient::RpcClient(std::shared_ptr<IBootstrapChannel> bootstrap)
    : impl_(Impl::Create(std::move(bootstrap)))
{
}

RpcClient::~RpcClient()
{
    Shutdown();
}

void RpcClient::SetBootstrapChannel(std::shared_ptr<IBootstrapChannel> bootstrap)
{
    impl_->SetBootstrapChannel(std::move(bootstrap));
}

void RpcClient::SetEventCallback(RpcEventCallback callback)
{
    std::lock_guard<std::mutex> lock(impl_->eventMutex_);
    impl_->eventCallback_ = std::move(callback);
}

void RpcClient::SetRecoveryEventCallback(RecoveryEventCallback callback)
{
    impl_->SetRecoveryEventCallback(std::move(callback));
}

void RpcClient::SetRecoveryPolicy(RecoveryPolicy policy)
{
    std::lock_guard<std::mutex> lock(impl_->recoveryPolicyMutex_);
    impl_->recoveryPolicy_ = std::move(policy);
}

void RpcClient::RequestExternalRecovery(ExternalRecoveryRequest request)
{
    impl_->RequestExternalRecovery(request);
}

StatusCode RpcClient::Init(ClientInitMode mode)
{
    // `clientControlState_` gates whether public APIs may still enter the client.
    // `workersRunning_` only tracks whether background loops have been started.
    if (impl_->IsApiTerminal()) {
        HILOGE("RpcClient::Init failed: client already closed");
        return StatusCode::ClientClosed;
    }
    if (impl_->WorkersShouldRun()) {
        HILOGW("RpcClient::Init called while already running");
        return mode == ClientInitMode::EagerSession ? impl_->EnsureLiveSession() : StatusCode::Ok;
    }
    impl_->lastActivityMs_.store(MonotonicNowMs(), std::memory_order_release);
    impl_->PrepareForInitOpen(mode == ClientInitMode::LazySession ? ClientLifecycleState::IdleClosed
                                                                  : ClientLifecycleState::Uninitialized);
    impl_->StartThreads();
    if (mode == ClientInitMode::EagerSession) {
        const StatusCode status = impl_->EnsureLiveSession();
        if (status != StatusCode::Ok) {
            HILOGE("RpcClient::Init failed: EnsureLiveSession status=%{public}d", static_cast<int>(status));
            impl_->Shutdown();
            return status;
        }
    }
    if (!impl_->FinishInit()) {
        HILOGW("RpcClient::Init completed after shutdown started");
        return StatusCode::ClientClosed;
    }
    return StatusCode::Ok;
}

RpcFuture RpcClient::InvokeAsync(const RpcCall& call)
{
    return impl_->InvokeAsync(call);
}

RpcFuture RpcClient::InvokeAsync(RpcCall&& call)
{
    return impl_->InvokeAsync(std::move(call));
}

StatusCode RpcClient::RetryUntilRecoverySettles(const std::function<StatusCode()>& invoke)
{
    return impl_->RetryUntilRecoverySettles(invoke);
}

RecoveryRuntimeSnapshot RpcClient::GetRecoveryRuntimeSnapshot() const
{
    return impl_->GetRecoveryRuntimeSnapshot();
}

void RpcClient::Shutdown()
{
    // Shutdown first flips the public control state, then stops background
    // workers, then resolves any leftover work as ClientClosed.
    impl_->Shutdown();
}

}  // namespace MemRpc
