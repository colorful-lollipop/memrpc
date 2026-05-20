#include "client/ves_client.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <thread>

#include "iremote_broker.h"
#include "iservice_registry.h"
#include "transport/ves_control_interface.h"
#include "ves/ves_codec.h"
#include "ves/ves_protocol.h"
#include "virus_protection_executor_log.h"

namespace VirusExecutorService {
namespace {
constexpr uint32_t DEFAULT_RESTART_DELAY_MS = 200;
std::atomic<uint64_t> g_nextVesClientGeneration{1};
std::atomic<uint64_t> g_activeVesClientGeneration{0};

bool EngineDeathLimitReached(const std::shared_ptr<std::atomic<uint32_t>>& engineDeathCount, uint32_t limit)
{
    return limit > 0 && engineDeathCount != nullptr && engineDeathCount->load(std::memory_order_acquire) >= limit;
}

void ResetRestartBudget(const std::shared_ptr<std::atomic<uint32_t>>& engineDeathCount)
{
    if (engineDeathCount != nullptr) {
        engineDeathCount->store(0, std::memory_order_release);
    }
}

MemRpc::RecoveryDecision ApplyRestartBudget(const MemRpc::RecoveryDecision& decision,
                                            const std::shared_ptr<std::atomic<uint32_t>>& engineDeathCount,
                                            uint32_t maxDeaths,
                                            const char* reason)
{
    if (decision.action != MemRpc::RecoveryAction::Restart) {
        return decision;
    }

    uint32_t restartCount = 0;
    if (engineDeathCount != nullptr) {
        restartCount = engineDeathCount->fetch_add(1, std::memory_order_acq_rel) + 1;
    }
    if (maxDeaths > 0 && restartCount >= maxDeaths) {
        HILOGE("restart budget exhausted: reason=%{public}s count=%{public}u limit=%{public}u",
               reason,
               restartCount,
               maxDeaths);
        return MemRpc::RecoveryDecision{MemRpc::RecoveryAction::Ignore, 0};
    }
    return decision;
}

MemRpc::RecoveryPolicy BuildRecoveryPolicy(const VesClientOptions& options,
                                           const std::shared_ptr<std::atomic<uint32_t>>& engineDeathCount)
{
    MemRpc::RecoveryPolicy policy = options.recoveryPolicy;
    auto onFailure = policy.onFailure;
    if (!onFailure) {
        onFailure = [](const MemRpc::RpcFailure& failure) {
            if (failure.status == MemRpc::StatusCode::ExecTimeout) {
                return MemRpc::RecoveryDecision{MemRpc::RecoveryAction::Restart, DEFAULT_RESTART_DELAY_MS};
            }
            return MemRpc::RecoveryDecision{MemRpc::RecoveryAction::Ignore, 0};
        };
    }
    policy.onFailure = [onFailure = std::move(onFailure),
                        engineDeathCount,
                        maxDeaths = options.maxEngineDeathsBeforePermanentStop](const MemRpc::RpcFailure& failure) {
        return ApplyRestartBudget(onFailure(failure), engineDeathCount, maxDeaths, "failure");
    };
    auto onEngineDeath = policy.onEngineDeath;
    if (!onEngineDeath) {
        onEngineDeath = [](const MemRpc::EngineDeathReport& report) {
            HILOGW("engine death: session=%{public}llu", static_cast<unsigned long long>(report.deadSessionId));
            return MemRpc::RecoveryDecision{
                MemRpc::RecoveryAction::Restart,
                DEFAULT_RESTART_DELAY_MS,
            };
        };
    }
    policy.onEngineDeath =
        [onEngineDeath = std::move(onEngineDeath),
         engineDeathCount,
         maxDeaths = options.maxEngineDeathsBeforePermanentStop](const MemRpc::EngineDeathReport& report) mutable {
            return ApplyRestartBudget(onEngineDeath(report), engineDeathCount, maxDeaths, "engine_death");
        };
    if (!policy.onIdle && options.idleShutdownTimeoutMs > 0) {
        policy.onIdle = [timeout = options.idleShutdownTimeoutMs](uint64_t idleMs) {
            if (idleMs < timeout) {
                return MemRpc::RecoveryDecision{MemRpc::RecoveryAction::Ignore, 0};
            }
            return MemRpc::RecoveryDecision{MemRpc::RecoveryAction::IdleClose, 0};
        };
    }
    return policy;
}

VesClient::ControlLoader BuildControlLoader(VesClientConnectOptions connectOptions)
{
    return [connectOptions]() -> OHOS::sptr<IVirusProtectionExecutor> {
        auto sam = OHOS::SystemAbilityManagerClient::GetInstance().GetSystemAbilityManager();
        if (sam == nullptr) {
            HILOGE("GetSystemAbilityManager failed");
            return nullptr;
        }
        OHOS::sptr<OHOS::IRemoteObject> remote = sam->CheckSystemAbility(VIRUS_PROTECTION_EXECUTOR_SA_ID);
        if (remote != nullptr) {
            auto control = OHOS::iface_cast<IVirusProtectionExecutor>(remote);
            if (control != nullptr) {
                control->CloseSession();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        remote = sam->LoadSystemAbility(VIRUS_PROTECTION_EXECUTOR_SA_ID, connectOptions.loadTimeoutMs);
        return remote != nullptr ? OHOS::iface_cast<IVirusProtectionExecutor>(remote) : nullptr;
    };
}

[[noreturn]] void AbortForMissingControlLoader()
{
    HILOGE("VesClient requires a non-null control loader");
    std::abort();
}

struct VesInvokeExecutionContext {
    MemRpc::RpcClient* client = nullptr;
};

struct VesInvokeRequestView {
    MemRpc::Opcode opcode = MemRpc::OPCODE_INVALID;
    MemRpc::Priority priority = MemRpc::Priority::Normal;
    uint32_t execTimeoutMs = 0;
    const std::vector<uint8_t>* payload = nullptr;
};

struct VesPreparedInvoke {
    std::vector<uint8_t> payload;
};

template <typename Reply>
MemRpc::StatusCode InvokeMemRpcApi(const VesInvokeExecutionContext& context,
                                   const VesInvokeRequestView& request,
                                   Reply* reply)
{
    if (context.client == nullptr || request.payload == nullptr) {
        return MemRpc::StatusCode::InvalidArgument;
    }

    MemRpc::RpcCall call;
    call.opcode = request.opcode;
    call.priority = request.priority;
    call.execTimeoutMs = request.execTimeoutMs;
    call.payload = *request.payload;
    return MemRpc::WaitAndDecode<Reply>(context.client->InvokeAsync(std::move(call)), reply);
}

template <typename Request>
MemRpc::StatusCode EncodeInvokePayload(MemRpc::Opcode opcode, const Request& request, std::vector<uint8_t>* payload)
{
    if (!MemRpc::EncodeMessage<Request>(request, payload)) {
        HILOGE("VesClient::InvokeApi encode failed opcode=%{public}u", opcode);
        return MemRpc::StatusCode::ProtocolMismatch;
    }
    return MemRpc::StatusCode::Ok;
}

template <typename Request>
MemRpc::StatusCode PrepareInvokePayload(MemRpc::Opcode opcode, const Request& request, VesPreparedInvoke* prepared)
{
    if (prepared == nullptr) {
        return MemRpc::StatusCode::InvalidArgument;
    }
    prepared->payload.clear();

    return EncodeInvokePayload(opcode, request, &prepared->payload);
}

}  // namespace

VesClient::VesClient(ControlLoader controlLoader, VesClientOptions options)
    : controlLoader_(std::move(controlLoader)),
      options_(std::move(options)),
      engineDeathCount_(std::make_shared<std::atomic<uint32_t>>(0)),
      instanceGeneration_(g_nextVesClientGeneration.fetch_add(1, std::memory_order_relaxed))
{
    if (!controlLoader_) {
        AbortForMissingControlLoader();
    }
}

VesClient::~VesClient()
{
    Shutdown();
}

std::unique_ptr<VesClient> VesClient::Connect(VesClientOptions options, VesClientConnectOptions connectOptions)
{
    auto client = std::make_unique<VesClient>(BuildControlLoader(connectOptions), std::move(options));
    if (client->InitClient(MemRpc::ClientInitMode::LazySession) != MemRpc::StatusCode::Ok) {
        HILOGE("VesClient init failed");
        return nullptr;
    }
    return client;
}

MemRpc::StatusCode VesClient::Init()
{
    return InitClient(MemRpc::ClientInitMode::EagerSession);
}

MemRpc::StatusCode VesClient::InitClient(MemRpc::ClientInitMode mode)
{
    ClaimProcessOwnership();
    if (bootstrapChannel_ == nullptr) {
        bootstrapChannel_ = std::make_shared<VesBootstrapChannel>(
            controlLoader_,
            options_.openSessionRequest,
            [this, engineDeathCount = engineDeathCount_, maxDeaths = options_.maxEngineDeathsBeforePermanentStop]() {
                return IsProcessOwner() && !EngineDeathLimitReached(engineDeathCount, maxDeaths);
            });
    }
    client_.SetBootstrapChannel(bootstrapChannel_);
    client_.SetRecoveryPolicy(BuildRecoveryPolicy(options_, engineDeathCount_));
    const MemRpc::StatusCode status = client_.Init(mode);
    if (status != MemRpc::StatusCode::Ok) {
        HILOGE("VesClient::Init failed for saId=%{public}d status=%{public}d",
               VIRUS_PROTECTION_EXECUTOR_SA_ID,
               static_cast<int>(status));
        return status;
    }
    ResetRestartBudget(engineDeathCount_);
    return MemRpc::StatusCode::Ok;
}

void VesClient::ClaimProcessOwnership() const
{
    g_activeVesClientGeneration.store(instanceGeneration_, std::memory_order_release);
}

bool VesClient::IsProcessOwner() const
{
    return g_activeVesClientGeneration.load(std::memory_order_acquire) == instanceGeneration_;
}

void VesClient::SetEventCallback(EventCallback callback)
{
    client_.SetEventCallback(std::move(callback));
}

void VesClient::Shutdown()
{
    client_.SetRecoveryPolicy({});
    client_.Shutdown();
    bootstrapChannel_.reset();
}

OHOS::sptr<IVirusProtectionExecutor> VesClient::CurrentControl()
{
    if (bootstrapChannel_ == nullptr) {
        return nullptr;
    }
    return bootstrapChannel_->CurrentControl();
}

template <typename Request, typename Reply>
MemRpc::StatusCode VesClient::InvokeApi(MemRpc::Opcode opcode,
                                        const Request& request,
                                        Reply* reply,
                                        MemRpc::Priority priority,
                                        uint32_t execTimeoutMs)
{
    if (reply == nullptr) {
        HILOGE("VesClient::InvokeApi failed: reply is null opcode=%{public}u", opcode);
        return MemRpc::StatusCode::InvalidArgument;
    }
    if (client_.GetRecoveryRuntimeSnapshot().lifecycleState == MemRpc::ClientLifecycleState::Closed) {
        return MemRpc::StatusCode::ClientClosed;
    }

    VesPreparedInvoke prepared;
    MemRpc::StatusCode status = PrepareInvokePayload(opcode, request, &prepared);
    if (status != MemRpc::StatusCode::Ok) {
        return status;
    }

    const VesInvokeExecutionContext context{
        &client_,
    };
    const VesInvokeRequestView invokeRequest{
        opcode,
        priority,
        execTimeoutMs,
        &prepared.payload,
    };
    status = client_.RetryUntilRecoverySettles([&]() {
        const MemRpc::StatusCode invokeStatus = InvokeMemRpcApi(context, invokeRequest, reply);
        if (invokeStatus == MemRpc::StatusCode::Ok) {
            ResetRestartBudget(engineDeathCount_);
        }
        return invokeStatus;
    });
    if (status == MemRpc::StatusCode::ClientClosed && bootstrapChannel_ != nullptr &&
        bootstrapChannel_->HasFatalControlLoadFailure()) {
        Shutdown();
    }
    return status;
}

MemRpc::StatusCode VesClient::ScanFile(const ScanTask& scanTask,
                                       ScanFileReply* reply,
                                       MemRpc::Priority priority,
                                       uint32_t execTimeoutMs)
{
    return InvokeApi<ScanTask, ScanFileReply>(static_cast<MemRpc::Opcode>(VesOpcode::ScanFile),
                                              scanTask,
                                              reply,
                                              priority,
                                              execTimeoutMs);
}

template MemRpc::StatusCode VesClient::InvokeApi<ScanTask, ScanFileReply>(MemRpc::Opcode opcode,
                                                                          const ScanTask& request,
                                                                          ScanFileReply* reply,
                                                                          MemRpc::Priority priority,
                                                                          uint32_t execTimeoutMs);
}  // namespace VirusExecutorService
