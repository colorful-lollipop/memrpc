#include "service/virus_executor_service.h"

#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "iservice_registry.h"
#include "memrpc/core/codec.h"
#include "ves/ves_codec.h"
#include "ves/ves_file_payload.h"
#include "ves/ves_protocol.h"
#include "virus_protection_executor_log.h"

namespace VirusExecutorService {

namespace {

constexpr uint32_t LONG_RUNNING_REQUEST_THRESHOLD_MS = 100;

bool IsTestkitFaultInjectionEnabled()
{
    const char* value = std::getenv("VES_ENABLE_TESTKIT_FAULTS");
    return value != nullptr && value[0] == '1' && value[1] == '\0';
}

void PopulateHealthyReply(const MemRpc::RpcServerRuntimeStats& stats, uint64_t sessionId, VesHeartbeatReply* reply)
{
    if (reply == nullptr) {
        return;
    }
    reply->sessionId = sessionId;
    reply->flags = VES_HEARTBEAT_FLAG_HAS_SESSION | VES_HEARTBEAT_FLAG_INITIALIZED;
    reply->inFlight = stats.activeRequestExecutions;
    reply->lastTaskAgeMs = stats.oldestExecutionAgeMs;
    if (stats.activeRequestExecutions == 0) {
        reply->status = static_cast<uint32_t>(VesHeartbeatStatus::OkIdle);
        reply->reasonCode = static_cast<uint32_t>(VesHeartbeatReasonCode::None);
        std::snprintf(reply->currentTask, sizeof(reply->currentTask), "%s", "idle");
        return;
    }

    reply->flags |= VES_HEARTBEAT_FLAG_BUSY;
    std::snprintf(reply->currentTask, sizeof(reply->currentTask), "%s", "active");
    if (stats.oldestExecutionAgeMs >= LONG_RUNNING_REQUEST_THRESHOLD_MS) {
        reply->status = static_cast<uint32_t>(VesHeartbeatStatus::DegradedLongRunning);
        reply->reasonCode = static_cast<uint32_t>(VesHeartbeatReasonCode::LongRunning);
        reply->flags |= VES_HEARTBEAT_FLAG_LONG_RUNNING;
        return;
    }

    reply->status = static_cast<uint32_t>(VesHeartbeatStatus::OkBusy);
    reply->reasonCode = static_cast<uint32_t>(VesHeartbeatReasonCode::Busy);
}

}  // namespace

VirusExecutorService::VirusExecutorService()
    : OHOS::SystemAbility(VIRUS_PROTECTION_EXECUTOR_SA_ID, true)
{
}

bool VirusExecutorService::Publish(VirusExecutorService* service)
{
    const bool published = OHOS::SystemAbility::Publish(service);
    if (!published) {
        return false;
    }
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    published_ = true;
    return true;
}

MemRpc::StatusCode VirusExecutorService::OpenSession(const VesOpenSessionRequest& request,
                                                     MemRpc::BootstrapHandles& handles)
{
    if (stopping_.load(std::memory_order_acquire)) {
        return MemRpc::StatusCode::PeerDisconnected;
    }
    const MemRpc::StatusCode configStatus = service_.ConfigureEngines(request);
    if (configStatus != MemRpc::StatusCode::Ok) {
        return configStatus;
    }

    std::shared_ptr<EngineSessionService> sessionService;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        sessionService = session_service_;
    }
    if (!sessionService) {
        HILOGE("session service not initialized");
        return MemRpc::StatusCode::InvalidArgument;
    }
    return sessionService->OpenSession(handles);
}

MemRpc::StatusCode VirusExecutorService::CloseSession()
{
    std::shared_ptr<EngineSessionService> sessionService;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        sessionService = session_service_;
    }
    if (!sessionService) {
        return MemRpc::StatusCode::Ok;
    }
    return sessionService->CloseSession();
}

MemRpc::StatusCode VirusExecutorService::Heartbeat(VesHeartbeatReply& heartbeat)
{
    heartbeat = VesHeartbeatReply{};
    if (stopping_.load()) {
        heartbeat.status = static_cast<uint32_t>(VesHeartbeatStatus::UnhealthyStopping);
        heartbeat.reasonCode = static_cast<uint32_t>(VesHeartbeatReasonCode::Stopping);
        return MemRpc::StatusCode::Ok;
    }
    std::shared_ptr<EngineSessionService> sessionService;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        sessionService = session_service_;
    }
    if (!sessionService) {
        if (service_.initialized()) {
            heartbeat.flags |= VES_HEARTBEAT_FLAG_INITIALIZED;
        }
        std::snprintf(heartbeat.currentTask, sizeof(heartbeat.currentTask), "%s", "idle");
        return MemRpc::StatusCode::Ok;
    }
    const uint64_t sessionId = sessionService->session_id();
    if (!service_.initialized()) {
        heartbeat.status = static_cast<uint32_t>(VesHeartbeatStatus::UnhealthyInternalError);
        heartbeat.reasonCode = static_cast<uint32_t>(VesHeartbeatReasonCode::InternalError);
        return MemRpc::StatusCode::Ok;
    }
    if (sessionId == 0) {
        heartbeat.flags |= VES_HEARTBEAT_FLAG_INITIALIZED;
        heartbeat.status = static_cast<uint32_t>(VesHeartbeatStatus::UnhealthyNoSession);
        heartbeat.reasonCode = static_cast<uint32_t>(VesHeartbeatReasonCode::NoSession);
        return MemRpc::StatusCode::Ok;
    }
    PopulateHealthyReply(sessionService->GetRuntimeStats(), sessionId, &heartbeat);
    return MemRpc::StatusCode::Ok;
}

MemRpc::StatusCode VirusExecutorService::AnyCall(const VesAnyCallRequest& request, VesAnyCallReply& vesReply)
{
    vesReply = VesAnyCallReply{};
    if (stopping_.load(std::memory_order_acquire)) {
        vesReply.status = MemRpc::StatusCode::PeerDisconnected;
        return MemRpc::StatusCode::Ok;
    }
    if (!service_.initialized()) {
        vesReply.status = MemRpc::StatusCode::PeerDisconnected;
        return MemRpc::StatusCode::Ok;
    }

    std::shared_ptr<EngineSessionService> sessionService;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        sessionService = session_service_;
    }
    if (!sessionService) {
        vesReply.status = MemRpc::StatusCode::PeerDisconnected;
        return MemRpc::StatusCode::Ok;
    }

    MemRpc::RpcServerCall call;
    call.opcode = static_cast<MemRpc::Opcode>(request.opcode);
    call.priority = static_cast<MemRpc::Priority>(request.priority);
    call.payload =
        MemRpc::PayloadView(reinterpret_cast<const uint8_t*>(request.payload.data()), request.payload.size());

    MemRpc::RpcReply reply;
    vesReply.status = sessionService->InvokeAnyCall(call, &reply);
    if (reply.payload.empty()) {
        vesReply.payload.clear();
    } else {
        vesReply.payload.assign(reinterpret_cast<const char*>(reply.payload.data()), reply.payload.size());
    }
    return MemRpc::StatusCode::Ok;
}

void VirusExecutorService::OnStart()
{
    HILOGI("OnStart sa_id=%{public}d", GetSystemAbilityId());
    stopping_.store(false);
    ClearVesFilePayloads();
    testkitService_.SetFaultInjectionEnabled(IsTestkitFaultInjectionEnabled());
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        published_ = false;
        unloadRequested_ = false;
        session_service_ =
            std::make_shared<EngineSessionService>(std::vector<RpcHandlerRegistrar*>{&service_, &testkitService_});
        service_.SetEventPublisher(session_service_);
    }
    service_.Initialize();
}

void VirusExecutorService::OnStop()
{
    HILOGI("OnStop");
    stopping_.store(true, std::memory_order_release);
    std::shared_ptr<EngineSessionService> sessionService;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        sessionService = std::move(session_service_);
    }
    if (sessionService) {
        sessionService->CloseSession();
    }
    service_.SetEventPublisher({});
    service_.ResetEngines();
    ClearVesFilePayloads();
    OHOS::SystemAbility::OnStop();
}

}  // namespace VirusExecutorService
