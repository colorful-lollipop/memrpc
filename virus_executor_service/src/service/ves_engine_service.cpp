#include "ves/ves_engine_service.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <thread>

#include "ves/ves_codec.h"
#include "ves/ves_handler_registration.h"
#include "ves/ves_protocol.h"
#include "ves/ves_sample_rules.h"
#include "ves/ves_session_service.h"
#include "ves/ves_types.h"
#include "virus_protection_executor_log.h"

namespace VirusExecutorService {

namespace {

std::vector<uint32_t> ResolveEngineKinds(const VesOpenSessionRequest& request)
{
    std::vector<uint32_t> engineKinds = NormalizeVesEngineKinds(request.engineKinds);
    if (engineKinds.empty()) {
        engineKinds.push_back(static_cast<uint32_t>(VesEngineKind::Scan));
    }
    return engineKinds;
}

}  // namespace

void VesEngineService::Initialize()
{
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    HILOGI("VesEngineService initialized");
}

MemRpc::StatusCode VesEngineService::ConfigureEngines(const VesOpenSessionRequest& request)
{
    if (!IsValidVesOpenSessionRequest(request)) {
        HILOGE("invalid open-session request version=%{public}u count=%{public}zu",
               request.version,
               request.engineKinds.size());
        return MemRpc::StatusCode::InvalidArgument;
    }

    const std::vector<uint32_t> resolvedEngineKinds = ResolveEngineKinds(request);
    std::lock_guard<std::mutex> lock(engineConfigMutex_);
    if (!enginesConfigured_) {
        configuredEngineKinds_ = resolvedEngineKinds;
        enginesConfigured_ = true;
        HILOGI("configured %{public}zu engine kinds", configuredEngineKinds_.size());
        return MemRpc::StatusCode::Ok;
    }
    if (configuredEngineKinds_ != resolvedEngineKinds) {
        HILOGE("open-session engine list mismatch");
        return MemRpc::StatusCode::InvalidArgument;
    }
    return MemRpc::StatusCode::Ok;
}

bool VesEngineService::initialized() const
{
    return initialized_.load(std::memory_order_acquire);
}

std::vector<uint32_t> VesEngineService::configuredEngineKinds() const
{
    std::lock_guard<std::mutex> lock(engineConfigMutex_);
    return configuredEngineKinds_;
}

void VesEngineService::ResetEngines()
{
    std::lock_guard<std::mutex> lock(engineConfigMutex_);
    configuredEngineKinds_.clear();
    enginesConfigured_ = false;
}

void VesEngineService::SetEventPublisher(std::weak_ptr<VesEventPublisher> publisher)
{
    std::lock_guard<std::mutex> lock(eventPublisherMutex_);
    eventPublisher_ = std::move(publisher);
}

ScanFileReply VesEngineService::ScanFile(const ScanTask& request) const
{
    ScanFileReply result;
    if (!initialized() || !IsScanEngineEnabled()) {
        result.code = -1;
    } else {
        const auto behavior = EvaluateSamplePath(request.path);
        if (behavior.shouldCrash) {
            HILOGE("ScanFile(%{public}s): crash requested", request.path.c_str());
            std::abort();
        }
        if (behavior.sleepMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(behavior.sleepMs));
        }
        result.code = 0;
        result.threatLevel = behavior.threatLevel;
    }
    HILOGI("ScanFile(%{public}s): threat=%{public}d", request.path.c_str(), result.threatLevel);

    return result;
}

bool VesEngineService::IsScanEngineEnabled() const
{
    std::lock_guard<std::mutex> lock(engineConfigMutex_);
    return !enginesConfigured_ || std::find(configuredEngineKinds_.begin(),
                                            configuredEngineKinds_.end(),
                                            static_cast<uint32_t>(VesEngineKind::Scan)) != configuredEngineKinds_.end();
}

MemRpc::StatusCode VesEngineService::PublishEvent(uint32_t eventType,
                                                  const std::vector<uint8_t>& payload,
                                                  uint32_t flags,
                                                  uint32_t eventDomain) const
{
    std::shared_ptr<VesEventPublisher> publisher;
    {
        std::lock_guard<std::mutex> lock(eventPublisherMutex_);
        publisher = eventPublisher_.lock();
    }
    if (publisher == nullptr) {
        return MemRpc::StatusCode::PeerDisconnected;
    }

    MemRpc::RpcEvent event;
    event.eventDomain = eventDomain;
    event.eventType = eventType;
    event.flags = flags;
    event.payload = payload;
    return publisher->PublishEventBlocking(event);
}

MemRpc::StatusCode VesEngineService::PublishTextEvent(uint32_t eventType,
                                                      const std::string& payload,
                                                      uint32_t flags,
                                                      uint32_t eventDomain) const
{
    return PublishEvent(eventType, std::vector<uint8_t>(payload.begin(), payload.end()), flags, eventDomain);
}

void VesEngineService::RegisterHandlers(RpcHandlerSink* sink)
{
    RegisterVesTypedHandler<ScanTask, ScanFileReply>(sink,
                                                     static_cast<MemRpc::Opcode>(VesOpcode::ScanFile),
                                                     [this](const ScanTask& request) { return ScanFile(request); });
}

}  // namespace VirusExecutorService
