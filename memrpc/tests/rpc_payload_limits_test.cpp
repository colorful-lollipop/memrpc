#include <gtest/gtest.h>

#include <dirent.h>
#include <unistd.h>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "core/session.h"
#include "core/shm_layout.h"
#include "memrpc/test_support/dev_bootstrap.h"
#include "memrpc/client/rpc_client.h"
#include "memrpc/core/file_payload.h"
#include "memrpc/core/runtime_utils.h"
#include "memrpc/server/rpc_server.h"

namespace {

constexpr MemRpc::Opcode kPayloadLimitOpcode = 301;

class ScopedFilePayloadDir {
public:
    ScopedFilePayloadDir()
    {
        MemRpc::ClearFilePayloads();
    }

    ~ScopedFilePayloadDir()
    {
        MemRpc::ClearFilePayloads();
    }

    [[nodiscard]] const std::string& path() const
    {
        static const std::string kPath = MemRpc::DEFAULT_FILE_PAYLOAD_DIR;
        return kPath;
    }
};

int CountFilePayloadFiles(const std::string& path)
{
    DIR* stream = opendir(path.c_str());
    if (stream == nullptr) {
        return 0;
    }
    int count = 0;
    while (dirent* entry = readdir(stream)) {
        const std::string name = entry->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        ++count;
    }
    closedir(stream);
    return count;
}

bool WaitForCondition(const std::function<bool()>& condition, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return condition();
}

void CloseHandles(MemRpc::BootstrapHandles& handles)
{
    if (handles.shmFd >= 0) {
        close(handles.shmFd);
    }
    if (handles.highReqEventFd >= 0) {
        close(handles.highReqEventFd);
    }
    if (handles.normalReqEventFd >= 0) {
        close(handles.normalReqEventFd);
    }
    if (handles.respEventFd >= 0) {
        close(handles.respEventFd);
    }
    if (handles.reqCreditEventFd >= 0) {
        close(handles.reqCreditEventFd);
    }
    if (handles.respCreditEventFd >= 0) {
        close(handles.respCreditEventFd);
    }
}

std::vector<uint8_t> MakePatternPayload(std::size_t size, uint32_t seed)
{
    std::vector<uint8_t> payload(size);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>((seed * 17U + static_cast<uint32_t>(i) * 31U) & 0xffU);
    }
    return payload;
}

}  // namespace

namespace MemRpc {

TEST(RpcPayloadLimitsTest, OversizedRequestUsesFilePayloadAndReturnsPayload)
{
    ScopedFilePayloadDir payloadDir;
    auto bootstrap = std::make_shared<DevBootstrapChannel>();
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);
    CloseHandles(handles);

    RpcServer server(bootstrap->serverHandles());
    server.RegisterHandler(kPayloadLimitOpcode, [](const RpcServerCall& call, RpcServerReply* reply) {
        reply->status = StatusCode::Ok;
        reply->payload = call.payload;
    });
    ASSERT_EQ(server.Start(), StatusCode::Ok);

    RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), StatusCode::Ok);

    RpcCall call;
    call.opcode = kPayloadLimitOpcode;
    call.payload.resize(DEFAULT_MAX_REQUEST_BYTES + 1U, 0x5a);
    const std::vector<uint8_t> expectedPayload = call.payload;

    RpcReply reply;
    EXPECT_EQ(client.InvokeAsync(call).Wait(&reply), StatusCode::Ok);
    EXPECT_EQ(reply.status, StatusCode::Ok);
    EXPECT_EQ(reply.payload, expectedPayload);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);

    client.Shutdown();
    server.Stop();
}

TEST(RpcPayloadLimitsTest, OversizedResponseUsesFilePayloadAndReturnsPayload)
{
    ScopedFilePayloadDir payloadDir;
    auto bootstrap = std::make_shared<DevBootstrapChannel>();
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);
    CloseHandles(handles);

    std::atomic<int> callCount{0};
    RpcServer server(bootstrap->serverHandles());
    server.RegisterHandler(kPayloadLimitOpcode, [&callCount](const RpcServerCall&, RpcServerReply* reply) {
        callCount.fetch_add(1, std::memory_order_relaxed);
        reply->status = StatusCode::Ok;
        reply->payload.resize(DEFAULT_MAX_RESPONSE_BYTES + 1U, 0x7b);
    });
    ASSERT_EQ(server.Start(), StatusCode::Ok);

    RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), StatusCode::Ok);

    RpcCall call;
    call.opcode = kPayloadLimitOpcode;
    call.payload = {1, 2, 3};

    RpcReply reply;
    EXPECT_EQ(client.InvokeAsync(call).Wait(&reply), StatusCode::Ok);
    EXPECT_EQ(reply.status, StatusCode::Ok);
    EXPECT_EQ(reply.payload, std::vector<uint8_t>(DEFAULT_MAX_RESPONSE_BYTES + 1U, 0x7b));
    EXPECT_EQ(callCount.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);

    client.Shutdown();
    server.Stop();
}

TEST(RpcPayloadLimitsTest, FailedRequestPushRemovesPreparedFilePayload)
{
    ScopedFilePayloadDir payloadDir;
    SharedMemorySessionConfig config;
    config.highRingSize = 1;
    config.normalRingSize = 1;
    config.responseRingSize = 1;

    auto bootstrap = std::make_shared<DevBootstrapChannel>(config);
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);

    Session session;
    ASSERT_EQ(session.Attach(&handles), StatusCode::Ok);

    RequestRingEntry first;
    first.requestId = 1;
    ASSERT_EQ(session.PushRequest(QueueKind::NormalRequest, first), StatusCode::Ok);

    std::vector<uint8_t> oversizedPayload(DEFAULT_MAX_REQUEST_BYTES + 1U, 0x44);
    std::vector<uint8_t> transportPayload;
    uint8_t payloadKind = PAYLOAD_KIND_INLINE;
    ASSERT_TRUE(PrepareFilePayloadForTransport(
        DEFAULT_FILE_PAYLOAD_DIR, oversizedPayload, RequestRingEntry::INLINE_PAYLOAD_BYTES, &transportPayload, &payloadKind));
    ASSERT_EQ(payloadKind, PAYLOAD_KIND_FILE_REF);
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), 1);

    RequestRingEntry second;
    second.requestId = 2;
    second.payloadKind = payloadKind;
    second.payloadSize = static_cast<uint32_t>(transportPayload.size());
    std::memcpy(second.payload.data(), transportPayload.data(), transportPayload.size());
    ASSERT_EQ(session.PushRequest(QueueKind::NormalRequest, second), StatusCode::QueueFull);
    EXPECT_TRUE(RemoveFilePayloadFromTransport(
        DEFAULT_FILE_PAYLOAD_DIR, transportPayload.data(), transportPayload.size(), payloadKind));
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(RpcPayloadLimitsTest, FailedOversizedResponseWriteRemovesFailedAndAbandonedFilePayloads)
{
    ScopedFilePayloadDir payloadDir;
    SharedMemorySessionConfig config;
    config.highRingSize = 1;
    config.normalRingSize = 2;
    config.responseRingSize = 1;

    auto bootstrap = std::make_shared<DevBootstrapChannel>(config);
    BootstrapHandles serverHandles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(serverHandles), StatusCode::Ok);

    ServerOptions options;
    options.completionQueueCapacity = 1;
    std::atomic<int> handledResponses{0};
    RpcServer server(serverHandles, options);
    server.RegisterHandler(kPayloadLimitOpcode, [&handledResponses](const RpcServerCall&, RpcServerReply* reply) {
        ASSERT_NE(reply, nullptr);
        handledResponses.fetch_add(1, std::memory_order_relaxed);
        reply->status = StatusCode::Ok;
        reply->payload.resize(DEFAULT_MAX_RESPONSE_BYTES + 256U, 0x6d);
    });
    ASSERT_EQ(server.Start(), StatusCode::Ok);

    BootstrapHandles clientHandles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(clientHandles), StatusCode::Ok);
    Session clientSession;
    ASSERT_EQ(clientSession.Attach(&clientHandles), StatusCode::Ok);

    RequestRingEntry first;
    first.requestId = 1;
    first.opcode = kPayloadLimitOpcode;
    ASSERT_EQ(clientSession.PushRequest(QueueKind::NormalRequest, first), StatusCode::Ok);
    ASSERT_TRUE(SignalEventFd(clientSession.Handles().normalReqEventFd));

    ASSERT_TRUE(WaitForCondition([&server] {
        return server.GetRuntimeStats().responseRingPending == 1U;
    }, std::chrono::seconds{1}));
    ASSERT_EQ(handledResponses.load(std::memory_order_relaxed), 1);
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), 1);

    RequestRingEntry second;
    second.requestId = 2;
    second.opcode = kPayloadLimitOpcode;
    ASSERT_EQ(clientSession.PushRequest(QueueKind::NormalRequest, second), StatusCode::Ok);
    ASSERT_TRUE(SignalEventFd(clientSession.Handles().normalReqEventFd));

    ASSERT_TRUE(WaitForCondition([&server, &payloadDir, &handledResponses] {
        const RpcServerRuntimeStats stats = server.GetRuntimeStats();
        return handledResponses.load(std::memory_order_relaxed) == 2 && stats.activeRequestExecutions == 0U &&
               stats.responseRingPending == 1U && CountFilePayloadFiles(payloadDir.path()) == 0;
    }, std::chrono::seconds{2}));

    ResponseRingEntry response;
    EXPECT_FALSE(clientSession.PopResponse(&response));
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);

    server.Stop();
}

TEST(RpcPayloadLimitsTest, BrokenSessionCleanupRemovesPendingRequestAndResponseFilePayloads)
{
    ScopedFilePayloadDir payloadDir;
    SharedMemorySessionConfig config;
    config.highRingSize = 1;
    config.normalRingSize = 1;
    config.responseRingSize = 1;

    auto bootstrap = std::make_shared<DevBootstrapChannel>(config);
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);

    Session session;
    ASSERT_EQ(session.Attach(&handles), StatusCode::Ok);

    std::vector<uint8_t> requestPayload(DEFAULT_MAX_REQUEST_BYTES + 64U, 0x21);
    std::vector<uint8_t> requestTransport;
    uint8_t requestKind = PAYLOAD_KIND_INLINE;
    ASSERT_TRUE(PrepareFilePayloadForTransport(DEFAULT_FILE_PAYLOAD_DIR,
                                               requestPayload,
                                               RequestRingEntry::INLINE_PAYLOAD_BYTES,
                                               &requestTransport,
                                               &requestKind));
    ASSERT_EQ(requestKind, PAYLOAD_KIND_FILE_REF);

    std::vector<uint8_t> responsePayload(DEFAULT_MAX_RESPONSE_BYTES + 64U, 0x31);
    std::vector<uint8_t> responseTransport;
    uint8_t responseKind = PAYLOAD_KIND_INLINE;
    ASSERT_TRUE(PrepareFilePayloadForTransport(DEFAULT_FILE_PAYLOAD_DIR,
                                               responsePayload,
                                               ResponseRingEntry::INLINE_PAYLOAD_BYTES,
                                               &responseTransport,
                                               &responseKind));
    ASSERT_EQ(responseKind, PAYLOAD_KIND_FILE_REF);

    RequestRingEntry request;
    request.requestId = 1;
    request.payloadKind = requestKind;
    request.payloadSize = static_cast<uint32_t>(requestTransport.size());
    std::memcpy(request.payload.data(), requestTransport.data(), requestTransport.size());
    ASSERT_EQ(session.PushRequest(QueueKind::NormalRequest, request), StatusCode::Ok);

    ResponseRingEntry response;
    response.requestId = 1;
    response.payloadKind = responseKind;
    response.resultSize = static_cast<uint32_t>(responseTransport.size());
    std::memcpy(response.payload.data(), responseTransport.data(), responseTransport.size());
    ASSERT_EQ(session.PushResponse(response), StatusCode::Ok);
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), 2);

    session.SetState(Session::SessionState::Broken);
    session.RemovePendingFilePayloads(DEFAULT_FILE_PAYLOAD_DIR);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(RpcPayloadLimitsTest, ConcurrentOversizedRoundTripsDoNotLosePayloadsAndCleanFiles)
{
    ScopedFilePayloadDir payloadDir;
    SharedMemorySessionConfig config;
    config.highRingSize = 8;
    config.normalRingSize = 32;
    config.responseRingSize = 32;

    auto bootstrap = std::make_shared<DevBootstrapChannel>(config);
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);
    CloseHandles(handles);

    std::atomic<int> handledCount{0};
    ServerOptions options;
    options.normalWorkerThreads = 4;
    options.completionQueueCapacity = 32;
    RpcServer server(bootstrap->serverHandles(), options);
    server.RegisterHandler(kPayloadLimitOpcode, [&handledCount](const RpcServerCall& call, RpcServerReply* reply) {
        handledCount.fetch_add(1, std::memory_order_relaxed);
        reply->status = StatusCode::Ok;
        reply->payload = call.payload;
    });
    ASSERT_EQ(server.Start(), StatusCode::Ok);

    RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), StatusCode::Ok);

    constexpr std::size_t kThreads = 4;
    constexpr std::size_t kCallsPerThread = 6;
    constexpr std::size_t kCallCount = kThreads * kCallsPerThread;
    std::vector<std::vector<uint8_t>> expectedPayloads;
    expectedPayloads.reserve(kCallCount);
    for (std::size_t i = 0; i < kCallCount; ++i) {
        expectedPayloads.push_back(MakePatternPayload(DEFAULT_MAX_REQUEST_BYTES + 128U + (i % 7U),
                                                      static_cast<uint32_t>(i + 1)));
    }

    std::atomic<bool> start{false};
    std::vector<StatusCode> waitStatuses(kCallCount, StatusCode::EngineInternalError);
    std::vector<StatusCode> replyStatuses(kCallCount, StatusCode::EngineInternalError);
    std::vector<std::vector<uint8_t>> actualPayloads(kCallCount);
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (std::size_t threadIndex = 0; threadIndex < kThreads; ++threadIndex) {
        workers.emplace_back([&, threadIndex] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (std::size_t callIndex = 0; callIndex < kCallsPerThread; ++callIndex) {
                const std::size_t index = threadIndex * kCallsPerThread + callIndex;
                RpcCall call;
                call.opcode = kPayloadLimitOpcode;
                call.execTimeoutMs = 5000;
                call.payload = expectedPayloads[index];
                RpcReply reply;
                waitStatuses[index] = client.InvokeAsync(std::move(call)).Wait(&reply);
                replyStatuses[index] = reply.status;
                actualPayloads[index] = std::move(reply.payload);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }

    for (std::size_t i = 0; i < kCallCount; ++i) {
        EXPECT_EQ(waitStatuses[i], StatusCode::Ok) << "call " << i;
        EXPECT_EQ(replyStatuses[i], StatusCode::Ok) << "call " << i;
        EXPECT_EQ(actualPayloads[i], expectedPayloads[i]) << "call " << i;
    }
    EXPECT_EQ(handledCount.load(std::memory_order_relaxed), kCallCount);
    EXPECT_TRUE(WaitForCondition([&payloadDir] {
        return CountFilePayloadFiles(payloadDir.path()) == 0;
    }, std::chrono::seconds{2}));

    client.Shutdown();
    server.Stop();
}

TEST(RpcPayloadLimitsTest, OversizedEventUsesFilePayloadAndReturnsPayload)
{
    ScopedFilePayloadDir payloadDir;
    auto bootstrap = std::make_shared<DevBootstrapChannel>();
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);
    CloseHandles(handles);

    RpcServer server(bootstrap->serverHandles());
    server.RegisterHandler(kPayloadLimitOpcode,
                           [](const RpcServerCall&, RpcServerReply* reply) { reply->status = StatusCode::Ok; });
    ASSERT_EQ(server.Start(), StatusCode::Ok);

    RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), StatusCode::Ok);

    std::mutex eventMutex;
    std::condition_variable eventCv;
    bool eventReady = false;
    RpcEvent receivedEvent;
    client.SetEventCallback([&](const RpcEvent& event) {
        std::lock_guard<std::mutex> lock(eventMutex);
        receivedEvent = event;
        eventReady = true;
        eventCv.notify_one();
    });

    RpcEvent event;
    event.eventDomain = 7;
    event.eventType = 11;
    event.flags = 0x55U;
    event.payload = MakePatternPayload(DEFAULT_MAX_RESPONSE_BYTES + 1U, 0x33U);

    ASSERT_EQ(server.PublishEvent(event), StatusCode::Ok);

    std::unique_lock<std::mutex> lock(eventMutex);
    ASSERT_TRUE(eventCv.wait_for(lock, std::chrono::seconds{2}, [&eventReady] { return eventReady; }));
    EXPECT_EQ(receivedEvent.eventDomain, event.eventDomain);
    EXPECT_EQ(receivedEvent.eventType, event.eventType);
    EXPECT_EQ(receivedEvent.flags, event.flags);
    EXPECT_EQ(receivedEvent.payload, event.payload);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);

    client.Shutdown();
    server.Stop();
}

TEST(RpcPayloadLimitsTest, OversizedEventWithoutCallbackStillCleansFilePayload)
{
    ScopedFilePayloadDir payloadDir;
    auto bootstrap = std::make_shared<DevBootstrapChannel>();
    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(handles), StatusCode::Ok);
    CloseHandles(handles);

    RpcServer server(bootstrap->serverHandles());
    server.RegisterHandler(kPayloadLimitOpcode,
                           [](const RpcServerCall&, RpcServerReply* reply) { reply->status = StatusCode::Ok; });
    ASSERT_EQ(server.Start(), StatusCode::Ok);

    RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), StatusCode::Ok);

    RpcEvent event;
    event.eventDomain = 17;
    event.eventType = 19;
    event.payload = MakePatternPayload(DEFAULT_MAX_RESPONSE_BYTES + 128U, 0x41U);

    ASSERT_EQ(server.PublishEvent(event), StatusCode::Ok);
    EXPECT_TRUE(WaitForCondition([&payloadDir] { return CountFilePayloadFiles(payloadDir.path()) == 0; },
                                 std::chrono::seconds{2}));

    client.Shutdown();
    server.Stop();
}

}  // namespace MemRpc
