#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

#include "memrpc/client/rpc_client.h"
#include "memrpc/test_support/dev_bootstrap.h"
#include "core/session.h"
#include "memrpc/core/task_executor.h"
#include "memrpc/server/rpc_server.h"

constexpr MemRpc::Opcode kTestOpcode = 1u;

namespace {

class TestExecutor final : public MemRpc::TaskExecutor {
public:
    explicit TestExecutor(uint32_t max_inflight)
        : max_inflight_(max_inflight)
    {
    }

    void SetMaxInflight(uint32_t max_inflight)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        max_inflight_ = max_inflight;
        cv_.notify_all();
    }

    bool TrySubmit(std::function<void()> task) override
    {
        if (!task) {
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_ || inflight_ >= max_inflight_) {
                return false;
            }
            ++inflight_;
        }
        task();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (inflight_ > 0) {
                --inflight_;
            }
            cv_.notify_all();
        }
        return true;
    }

    bool HasCapacity() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return !stopped_ && inflight_ < max_inflight_;
    }

    bool WaitForCapacity(std::chrono::milliseconds timeout) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return stopped_ || inflight_ < max_inflight_; });
    }

    void Stop() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopped_ = true;
        cv_.notify_all();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    uint32_t max_inflight_ = 0;
    uint32_t inflight_ = 0;
    bool stopped_ = false;
};

class AsyncStopExecutor final : public MemRpc::TaskExecutor {
public:
    AsyncStopExecutor()
    {
        worker_ = std::thread([this] { WorkerLoop(); });
    }

    ~AsyncStopExecutor() override
    {
        Shutdown();
    }

    bool TrySubmit(std::function<void()> task) override
    {
        if (!task) {
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (rejectNewTasks_) {
                return false;
            }
            queue_.push(std::move(task));
        }
        cv_.notify_one();
        return true;
    }

    bool HasCapacity() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return !rejectNewTasks_;
    }

    bool WaitForCapacity(std::chrono::milliseconds timeout) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return !rejectNewTasks_; });
    }

    void Stop() override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            rejectNewTasks_ = true;
        }
        cv_.notify_all();
    }

private:
    void Shutdown()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            rejectNewTasks_ = true;
            shutdown_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void WorkerLoop()
    {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return shutdown_ || !queue_.empty(); });
                if (shutdown_ && queue_.empty()) {
                    return;
                }
                task = std::move(queue_.front());
                queue_.pop();
            }
            task();
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> queue_;
    std::thread worker_;
    bool rejectNewTasks_ = false;
    bool shutdown_ = false;
};

bool WaitForCondition(const std::function<bool()>& condition, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return condition();
}

}  // namespace

TEST(RpcServerExecutorTest, CustomExecutorGatesDrain)
{
    auto bootstrap = std::make_shared<MemRpc::DevBootstrapChannel>();
    MemRpc::BootstrapHandles unused_handles = MemRpc::MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(unused_handles), MemRpc::StatusCode::Ok);

    MemRpc::RpcServer server;
    server.SetBootstrapHandles(bootstrap->serverHandles());

    auto executor = std::make_shared<TestExecutor>(0);
    MemRpc::ServerOptions options;
    options.highExecutor = executor;
    options.normalExecutor = executor;
    server.SetOptions(options);
    server.RegisterHandler(kTestOpcode, [](const MemRpc::RpcServerCall& call, MemRpc::RpcServerReply* reply) {
        ASSERT_NE(reply, nullptr);
        reply->status = MemRpc::StatusCode::Ok;
        reply->payload = call.payload;
    });
    ASSERT_EQ(server.Start(), MemRpc::StatusCode::Ok);

    MemRpc::RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), MemRpc::StatusCode::Ok);

    MemRpc::RpcCall call;
    call.opcode = kTestOpcode;
    call.payload = std::vector<uint8_t>{1, 2, 3};

    auto future = client.InvokeAsync(call);
    EXPECT_FALSE(WaitForCondition([&future] { return future.IsReady(); }, 20));

    executor->SetMaxInflight(1);
    MemRpc::RpcReply reply;
    EXPECT_EQ(std::move(future).Wait(&reply), MemRpc::StatusCode::Ok);
    EXPECT_EQ(reply.payload, call.payload);

    client.Shutdown();
    server.Stop();
}

TEST(RpcServerExecutorTest, RuntimeStatsTrackActiveRequestExecution)
{
    auto bootstrap = std::make_shared<MemRpc::DevBootstrapChannel>();
    MemRpc::BootstrapHandles unusedHandles = MemRpc::MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(unusedHandles), MemRpc::StatusCode::Ok);

    MemRpc::RpcServer server;
    server.SetBootstrapHandles(bootstrap->serverHandles());

    std::mutex mutex;
    std::condition_variable cv;
    bool handlerRunning = false;
    bool allowReply = false;
    server.RegisterHandler(kTestOpcode, [&](const MemRpc::RpcServerCall&, MemRpc::RpcServerReply* reply) {
        ASSERT_NE(reply, nullptr);
        {
            std::lock_guard<std::mutex> lock(mutex);
            handlerRunning = true;
        }
        cv.notify_all();

        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&] { return allowReply; });
        reply->status = MemRpc::StatusCode::Ok;
    });
    ASSERT_EQ(server.Start(), MemRpc::StatusCode::Ok);

    MemRpc::RpcClient client(bootstrap);
    ASSERT_EQ(client.Init(), MemRpc::StatusCode::Ok);

    MemRpc::RpcCall call;
    call.opcode = kTestOpcode;
    call.execTimeoutMs = 200;
    auto future = client.InvokeAsync(call);

    ASSERT_TRUE(WaitForCondition(
        [&] {
            std::lock_guard<std::mutex> lock(mutex);
            return handlerRunning;
        },
        200));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    const MemRpc::RpcServerRuntimeStats busyStats = server.GetRuntimeStats();
    EXPECT_EQ(busyStats.activeRequestExecutions, 1u);
    EXPECT_GE(busyStats.oldestExecutionAgeMs, 1u);

    {
        std::lock_guard<std::mutex> lock(mutex);
        allowReply = true;
    }
    cv.notify_all();

    MemRpc::RpcReply reply;
    EXPECT_EQ(std::move(future).Wait(&reply), MemRpc::StatusCode::Ok);

    ASSERT_TRUE(WaitForCondition(
        [&] {
            const auto stats = server.GetRuntimeStats();
            return stats.activeRequestExecutions == 0 && stats.oldestExecutionAgeMs == 0;
        },
        200));

    client.Shutdown();
    server.Stop();
}

TEST(RpcServerExecutorTest, StopWaitsForAcceptedTasksWhenExecutorStopReturnsEarly)
{
    auto bootstrap = std::make_shared<MemRpc::DevBootstrapChannel>();
    MemRpc::BootstrapHandles clientHandles = MemRpc::MakeDefaultBootstrapHandles();
    ASSERT_EQ(bootstrap->OpenSession(clientHandles), MemRpc::StatusCode::Ok);

    MemRpc::RpcServer server;
    server.SetBootstrapHandles(bootstrap->serverHandles());

    MemRpc::Session clientSession;
    ASSERT_EQ(clientSession.Attach(&clientHandles, MemRpc::Session::AttachRole::Client), MemRpc::StatusCode::Ok);

    auto executor = std::make_shared<AsyncStopExecutor>();
    MemRpc::ServerOptions options;
    options.highExecutor = executor;
    options.normalExecutor = executor;
    server.SetOptions(options);

    std::mutex mutex;
    std::condition_variable cv;
    bool handlerRunning = false;
    bool allowReply = false;
    server.RegisterHandler(kTestOpcode, [&](const MemRpc::RpcServerCall&, MemRpc::RpcServerReply* reply) {
        ASSERT_NE(reply, nullptr);
        {
            std::lock_guard<std::mutex> lock(mutex);
            handlerRunning = true;
        }
        cv.notify_all();

        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&] { return allowReply; });
        reply->status = MemRpc::StatusCode::Ok;
    });
    ASSERT_EQ(server.Start(), MemRpc::StatusCode::Ok);

    MemRpc::RequestRingEntry request{};
    request.requestId = 1;
    request.opcode = kTestOpcode;
    request.execTimeoutMs = 200;
    ASSERT_EQ(clientSession.PushRequest(MemRpc::QueueKind::NormalRequest, request), MemRpc::StatusCode::Ok);

    ASSERT_TRUE(WaitForCondition(
        [&] {
            std::lock_guard<std::mutex> lock(mutex);
            return handlerRunning;
        },
        200));

    std::atomic<bool> stopReturned{false};
    std::thread stopThread([&] {
        server.Stop();
        stopReturned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(stopReturned.load(std::memory_order_acquire));

    MemRpc::RequestRingEntry rejectedRequest{};
    rejectedRequest.requestId = 2;
    rejectedRequest.opcode = kTestOpcode;
    rejectedRequest.execTimeoutMs = 200;
    EXPECT_EQ(clientSession.PushRequest(MemRpc::QueueKind::NormalRequest, rejectedRequest),
              MemRpc::StatusCode::PeerDisconnected);

    {
        std::lock_guard<std::mutex> lock(mutex);
        allowReply = true;
    }
    cv.notify_all();

    stopThread.join();
    EXPECT_TRUE(stopReturned.load(std::memory_order_acquire));
}
