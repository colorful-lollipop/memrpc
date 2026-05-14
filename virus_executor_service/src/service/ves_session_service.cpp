#include "ves/ves_session_service.h"

#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include <utility>

#include "memrpc/server/rpc_server.h"
#include "ves/ves_engine_service.h"
#include "ves/ves_protocol.h"
#include "virus_protection_executor_log.h"

namespace VirusExecutorService {

namespace {
class AnyCallHandlerSinkImpl final : public RpcHandlerSink {
public:
    explicit AnyCallHandlerSinkImpl(std::unordered_map<uint16_t, MemRpc::RpcHandler>* handlers)
        : handlers_(handlers)
    {
    }

    void RegisterHandler(MemRpc::Opcode opcode, MemRpc::RpcHandler handler) override
    {
        if (handlers_ == nullptr) {
            return;
        }
        const uint16_t key = static_cast<uint16_t>(opcode);
        if (!handler) {
            handlers_->erase(key);
            return;
        }
        (*handlers_)[key] = std::move(handler);
    }

private:
    std::unordered_map<uint16_t, MemRpc::RpcHandler>* handlers_ = nullptr;
};

MemRpc::StatusCode InvokeAnyCallHandler(const std::unordered_map<uint16_t, MemRpc::RpcHandler>& handlers,
                                        const MemRpc::RpcServerCall& call,
                                        MemRpc::RpcReply* reply)
{
    if (reply == nullptr) {
        return MemRpc::StatusCode::InvalidArgument;
    }

    *reply = MemRpc::RpcReply{};
    const auto it = handlers.find(static_cast<uint16_t>(call.opcode));
    if (it == handlers.end()) {
        reply->status = MemRpc::StatusCode::InvalidArgument;
        return reply->status;
    }

    it->second(call, reply);
    return reply->status;
}

constexpr auto EVENT_PUBLISH_PERIOD_MIN = std::chrono::milliseconds(80);
constexpr auto EVENT_PUBLISH_PERIOD_MAX = std::chrono::milliseconds(180);
constexpr uint32_t DEFAULT_CLOSE_SESSION_CHAOS_YIELDS = 8;
constexpr uint32_t MAX_CLOSE_SESSION_CHAOS_YIELDS = 1024;
constexpr uint32_t MAX_CLOSE_SESSION_CHAOS_SLEEP_MS = 1000;

bool EnvFlagEnabled(const char* name)
{
    const char* value = std::getenv(name);
    return value != nullptr && value[0] == '1' && value[1] == '\0';
}

uint32_t ReadEnvUintOrDefault(const char* name, uint32_t defaultValue, uint32_t maxValue)
{
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return defaultValue;
    }

    char* end = nullptr;
    const unsigned long parsed = std::strtoul(value, &end, 10);
    if (end == value || (end != nullptr && *end != '\0')) {
        return defaultValue;
    }
    return static_cast<uint32_t>(std::min<unsigned long>(parsed, maxValue));
}

const char* CloseSessionStageName(CloseSessionStage stage)
{
    switch (stage) {
        case CloseSessionStage::AfterMarkClosing:
            return "after_mark_closing";
        case CloseSessionStage::BeforeRpcServerStop:
            return "before_rpc_server_stop";
        case CloseSessionStage::BeforeSessionHostClose:
            return "before_session_host_close";
        default:
            return "unknown";
    }
}
}  // namespace

EngineSessionService::EngineSessionService(std::vector<RpcHandlerRegistrar*> registrars,
                                           std::shared_ptr<MemRpc::IServerSessionHost> sessionHost,
                                           EngineSessionServiceOptions options)
    : registrars_(std::move(registrars)),
      options_(std::move(options)),
      sessionHost_(std::move(sessionHost))
{
}

EngineSessionService::~EngineSessionService()
{
    (void)CloseSession();
}

MemRpc::StatusCode EngineSessionService::EnsureInitialized()
{
    std::lock_guard<std::mutex> lock(initMutex_);
    if (closing_) {
        return MemRpc::StatusCode::PeerDisconnected;
    }
    if (initialized_) {
        return MemRpc::StatusCode::Ok;
    }

    if (!sessionHost_) {
        sessionHost_ = std::make_shared<MemRpc::SharedMemorySessionHost>();
    }

    const MemRpc::StatusCode ensureStatus = sessionHost_->EnsureSession();
    if (ensureStatus != MemRpc::StatusCode::Ok) {
        HILOGE("session host EnsureSession failed");
        return ensureStatus;
    }

    const MemRpc::BootstrapHandles serverHandles = sessionHost_->serverHandles();
    rpcServer_ = std::make_shared<MemRpc::RpcServer>(serverHandles);
    anyCallHandlers_.clear();
    RpcServerHandlerSink rpcServerSink(rpcServer_.get());
    AnyCallHandlerSinkImpl anyCallSink(&anyCallHandlers_);
    for (auto* registrar : registrars_) {
        if (registrar != nullptr) {
            registrar->RegisterHandlers(&rpcServerSink);
            registrar->RegisterHandlers(&anyCallSink);
        }
    }
    const MemRpc::StatusCode start_status = rpcServer_->Start();
    if (start_status != MemRpc::StatusCode::Ok) {
        HILOGE("RpcServer start failed");
        rpcServer_.reset();
        sessionHost_.reset();
        anyCallHandlers_.clear();
        return start_status;
    }

    initialized_ = true;
    HILOGI("EngineSessionService initialized");
    return MemRpc::StatusCode::Ok;
}

void EngineSessionService::StartEventPublisherLocked()
{
    if (eventPublisherRunning_.load(std::memory_order_acquire)) {
        return;
    }
    eventPublisherRunning_.store(true, std::memory_order_release);
    eventPublisherThread_ = std::thread([this] { EventPublisherLoop(); });
}

MemRpc::StatusCode EngineSessionService::OpenSession(MemRpc::BootstrapHandles& handles)
{
    const MemRpc::StatusCode init_status = EnsureInitialized();
    if (init_status != MemRpc::StatusCode::Ok) {
        return init_status;
    }
    MemRpc::StatusCode status = MemRpc::StatusCode::InvalidArgument;
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (sessionHost_ == nullptr) {
            return MemRpc::StatusCode::InvalidArgument;
        }
        status = sessionHost_->OpenSession(handles);
        if (status == MemRpc::StatusCode::Ok) {
            sessionId_.store(handles.sessionId, std::memory_order_release);
            StartEventPublisherLocked();
        }
    }
    return status;
}

MemRpc::StatusCode EngineSessionService::PublishEventBlocking(const MemRpc::RpcEvent& event)
{
    std::shared_ptr<MemRpc::RpcServer> rpcServer;
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (!initialized_ || rpcServer_ == nullptr || sessionId_.load(std::memory_order_acquire) == 0) {
            return MemRpc::StatusCode::PeerDisconnected;
        }
        rpcServer = rpcServer_;
    }
    return rpcServer->PublishEvent(event);
}

MemRpc::StatusCode EngineSessionService::InvokeAnyCall(const MemRpc::RpcServerCall& call, MemRpc::RpcReply* reply)
{
    const MemRpc::StatusCode initStatus = EnsureInitialized();
    if (initStatus != MemRpc::StatusCode::Ok) {
        return initStatus;
    }
    return InvokeAnyCallHandler(anyCallHandlers_, call, reply);
}

MemRpc::StatusCode EngineSessionService::CloseSession()
{
    std::thread eventPublisherThread;
    std::shared_ptr<MemRpc::RpcServer> rpcServer;
    std::shared_ptr<MemRpc::IServerSessionHost> sessionHost;
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (closing_ || !initialized_) {
            return MemRpc::StatusCode::Ok;
        }
        closing_ = true;
        eventPublisherRunning_.store(false, std::memory_order_release);
        eventPublisherThread = std::move(eventPublisherThread_);
        rpcServer = std::move(rpcServer_);
        sessionHost = std::move(sessionHost_);
        initialized_ = false;
        sessionId_.store(0, std::memory_order_release);
    }
    RunCloseSessionChaos(CloseSessionStage::AfterMarkClosing);
    eventPublisherCv_.notify_all();
    if (eventPublisherThread.joinable()) {
        eventPublisherThread.join();
    }
    RunCloseSessionChaos(CloseSessionStage::BeforeRpcServerStop);
    if (rpcServer) {
        rpcServer->Stop();
    }
    RunCloseSessionChaos(CloseSessionStage::BeforeSessionHostClose);
    if (sessionHost) {
        (void)sessionHost->CloseSession();
    }
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        closing_ = false;
    }
    HILOGI("EngineSessionService closed");
    return MemRpc::StatusCode::Ok;
}

uint64_t EngineSessionService::session_id() const
{
    return sessionId_.load(std::memory_order_acquire);
}

MemRpc::RpcServerRuntimeStats EngineSessionService::GetRuntimeStats() const
{
    std::lock_guard<std::mutex> lock(initMutex_);
    if (rpcServer_ == nullptr) {
        return {};
    }
    return rpcServer_->GetRuntimeStats();
}

void EngineSessionService::RunCloseSessionChaos(CloseSessionStage stage) const
{
    if (options_.closeSessionHook) {
        options_.closeSessionHook(stage);
    }
    if (!EnvFlagEnabled("VES_ENABLE_CLOSE_SESSION_CHAOS")) {
        return;
    }

    const uint32_t yieldCount =
        ReadEnvUintOrDefault("VES_CLOSE_SESSION_CHAOS_YIELDS", DEFAULT_CLOSE_SESSION_CHAOS_YIELDS,
                             MAX_CLOSE_SESSION_CHAOS_YIELDS);
    const uint32_t sleepMs =
        ReadEnvUintOrDefault("VES_CLOSE_SESSION_CHAOS_SLEEP_MS", 0, MAX_CLOSE_SESSION_CHAOS_SLEEP_MS);
    for (uint32_t i = 0; i < yieldCount; ++i) {
        std::this_thread::yield();
    }
    if (sleepMs > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
    }
    HILOGW("EngineSessionService close-session chaos stage=%{public}s yields=%{public}u sleep_ms=%{public}u",
           CloseSessionStageName(stage),
           yieldCount,
           sleepMs);
}

void EngineSessionService::EventPublisherLoop()
{
    std::random_device device;
    std::mt19937 rng(device());
    std::uniform_int_distribution<int> delayDist(static_cast<int>(EVENT_PUBLISH_PERIOD_MIN.count()),
                                                 static_cast<int>(EVENT_PUBLISH_PERIOD_MAX.count()));
    std::uniform_int_distribution<uint32_t> eventTypeDist(static_cast<uint32_t>(VesEventType::RandomScanResult),
                                                          static_cast<uint32_t>(VesEventType::RandomLifecycle));
    std::uniform_int_distribution<int> stateDist(0, 99);

    while (eventPublisherRunning_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> waitLock(eventPublisherMutex_);
            if (eventPublisherCv_.wait_for(waitLock, std::chrono::milliseconds(delayDist(rng)), [this] {
                    return !eventPublisherRunning_.load(std::memory_order_acquire);
                })) {
                break;
            }
        }

        uint64_t sessionId = 0;
        {
            std::lock_guard<std::mutex> lock(initMutex_);
            if (!initialized_ || rpcServer_ == nullptr) {
                continue;
            }
            sessionId = sessionId_.load(std::memory_order_acquire);
        }

        if (sessionId == 0) {
            continue;
        }

        const uint32_t eventType = eventTypeDist(rng);
        const uint32_t sequence = eventSequence_.fetch_add(1, std::memory_order_relaxed) + 1;
        const std::string payloadText = "ves-random-event session=" + std::to_string(sessionId) +
                                        " seq=" + std::to_string(sequence) + " type=" + std::to_string(eventType) +
                                        " state=" + std::to_string(stateDist(rng));

        MemRpc::RpcEvent event;
        event.eventDomain = VES_EVENT_DOMAIN_RUNTIME;
        event.eventType = eventType;
        event.payload.assign(payloadText.begin(), payloadText.end());
        const MemRpc::StatusCode status = PublishEventBlocking(event);
        if (status != MemRpc::StatusCode::Ok && status != MemRpc::StatusCode::QueueFull &&
            status != MemRpc::StatusCode::PeerDisconnected) {
            HILOGW("random event publish failed, status=%{public}d", static_cast<int>(status));
        }
    }
}

}  // namespace VirusExecutorService
