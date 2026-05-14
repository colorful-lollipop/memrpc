#include "memrpc/server/rpc_server.h"

#include <poll.h>
#include <unistd.h>
#include <algorithm>
#include <array>

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <future>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/session.h"
#include "memrpc/core/protocol.h"
#include "memrpc/core/runtime_utils.h"
#include "memrpc/core/task_executor.h"
#include "memrpc/server/handler.h"
#include "virus_protection_executor_log.h"

namespace MemRpc {

namespace {

using namespace std::chrono_literals;

constexpr auto RESPONSE_RETRY_BUDGET = 50ms;
constexpr auto EVENT_RETRY_BUDGET = 10ms;
constexpr auto STOP_TIMEOUT = 5s;

class ThreadPoolExecutor final : public TaskExecutor {
public:
    explicit ThreadPoolExecutor(uint32_t threadCount)
        : running_(true)
    {
        const uint32_t threads = std::max(1U, threadCount);
        queueCapacity_ = threads;
        for (uint32_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this] { WorkerLoop(); });
        }
    }

    ~ThreadPoolExecutor() override
    {
        Stop();
    }

    bool TrySubmit(std::function<void()> task) override
    {
        if (!task) {
            HILOGE("ThreadPoolExecutor::TrySubmit failed: task is null");
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_ || queue_.size() >= queueCapacity_) {
                HILOGW(
                    "ThreadPoolExecutor::TrySubmit rejected: running=%{public}d queue_size=%{public}zu "
                    "capacity=%{public}u",
                    running_ ? 1 : 0,
                    queue_.size(),
                    queueCapacity_);
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
        return running_ && queue_.size() < queueCapacity_;
    }

    bool WaitForCapacity(std::chrono::milliseconds timeout) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return !running_ || queue_.size() < queueCapacity_; });
    }

    void Stop() override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) {
                return;
            }
            running_ = false;
        }
        cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers_.clear();
    }

private:
    void WorkerLoop()
    {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return !running_ || !queue_.empty(); });
                if (!running_ && queue_.empty()) {
                    return;
                }
                task = std::move(queue_.front());
                queue_.pop();
                cv_.notify_one();
            }
            task();
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> queue_;
    std::vector<std::thread> workers_;
    bool running_ = false;
    uint32_t queueCapacity_ = 1;
};

bool IsHighPriority(const RequestRingEntry& entry)
{
    return entry.priority == static_cast<uint8_t>(Priority::High);
}

}  // namespace

struct RpcServer::Impl : public std::enable_shared_from_this<RpcServer::Impl> {
    struct CompletionState {
        std::mutex mutex;
        std::condition_variable cv;
        bool ready = false;
        StatusCode status = StatusCode::EngineInternalError;
    };

    struct CompletionItem {
        ResponseRingEntry entry;
        std::chrono::milliseconds retryBudget{0};
        bool breakSessionOnFailure = false;
        std::shared_ptr<CompletionState> completion;
    };

    BootstrapHandles handles = MakeDefaultBootstrapHandles();
    ServerOptions options{};
    Session session;
    std::shared_ptr<TaskExecutor> highExecutor;
    std::shared_ptr<TaskExecutor> normalExecutor;
    std::mutex executionMutex;
    std::mutex completionMutex;
    std::condition_variable completionCv;
    std::queue<CompletionItem> completionQueue;
    uint32_t completionQueueCapacity = 0;
    uint32_t pendingCompletionCount = 0;
    std::thread responseWriterThread;
    std::atomic<bool> responseWriterRunning{false};
    std::atomic<bool> responseWriterWaitingForCredit{false};
    std::thread dispatcherThread;
    std::atomic<bool> running{false};
    std::unordered_map<uint16_t, RpcHandler> handlers;
    std::unordered_map<uint64_t, uint64_t> activeExecutions;
    std::mutex submittedTaskMutex;
    std::condition_variable submittedTaskCv;
    uint32_t pendingSubmittedTasks = 0;

    void MarkExecutionStarted(uint64_t requestId)
    {
        std::lock_guard<std::mutex> lock(executionMutex);
        activeExecutions[requestId] = MonotonicNowMs();
    }

    void MarkExecutionFinished(uint64_t requestId)
    {
        std::lock_guard<std::mutex> lock(executionMutex);
        activeExecutions.erase(requestId);
    }

    std::shared_ptr<void> MakeSubmittedTaskToken()
    {
        {
            std::lock_guard<std::mutex> lock(submittedTaskMutex);
            ++pendingSubmittedTasks;
        }

        auto self = shared_from_this();
        return {nullptr, [self = std::move(self)](void*) {
            std::lock_guard<std::mutex> lock(self->submittedTaskMutex);
            if (self->pendingSubmittedTasks > 0) {
                --self->pendingSubmittedTasks;
            }
            if (self->pendingSubmittedTasks == 0) {
                self->submittedTaskCv.notify_all();
            }
        }};
    }

    void WaitForSubmittedTasks()
    {
        std::unique_lock<std::mutex> lock(submittedTaskMutex);
        submittedTaskCv.wait(lock, [this] { return pendingSubmittedTasks == 0; });
    }

    PollEventFdResult WaitForResponseCredit(std::chrono::steady_clock::time_point deadline)
    {
        const int fd = session.Handles().respCreditEventFd;
        if (fd < 0) {
            HILOGE("RpcServer::WaitForResponseCredit failed: invalid respCreditEventFd=%{public}d", fd);
            return PollEventFdResult::Failed;
        }
        pollfd pollFd{fd, POLLIN, 0};
        responseWriterWaitingForCredit.store(true);
        [[maybe_unused]] const auto clearWaiting =
            MakeScopeExit([this] { responseWriterWaitingForCredit.store(false); });
        while (responseWriterRunning.load(std::memory_order_acquire)) {
            const int64_t remainingMs = RemainingTimeoutMs(deadline);
            if (remainingMs <= 0) {
                HILOGW("RpcServer::WaitForResponseCredit timed out");
                return PollEventFdResult::Timeout;
            }
            const auto waitResult = PollEventFd(&pollFd, static_cast<int>(remainingMs));
            if (waitResult == PollEventFdResult::Retry) {
                continue;
            }
            if (waitResult == PollEventFdResult::Failed) {
                HILOGE("RpcServer::WaitForResponseCredit poll failed fd=%{public}d", fd);
            }
            return waitResult;
        }
        HILOGW("RpcServer::WaitForResponseCredit aborted because response writer stopped");
        return PollEventFdResult::Failed;
    }

    StatusCode PushResponseWithRetry(const ResponseRingEntry& response, std::chrono::milliseconds retryBudget)
    {
        const auto deadline = std::chrono::steady_clock::now() + retryBudget;
        while (true) {
            const StatusCode status = session.PushResponse(response);
            if (status == StatusCode::Ok || status != StatusCode::QueueFull) {
                if (status != StatusCode::Ok) {
                    HILOGE("RpcServer::PushResponseWithRetry failed: request_id=%{public}llu status=%{public}d",
                           static_cast<unsigned long long>(response.requestId),
                           static_cast<int>(status));
                }
                return status;
            }
            if (DeadlineReached(deadline)) {
                HILOGE("RpcServer::PushResponseWithRetry timed out: request_id=%{public}llu",
                       static_cast<unsigned long long>(response.requestId));
                return StatusCode::QueueFull;
            }
            const auto waitResult = WaitForResponseCredit(deadline);
            if (waitResult == PollEventFdResult::Ready) {
                continue;
            }
            HILOGE(
                "RpcServer::PushResponseWithRetry failed waiting for response credit: request_id=%{public}llu "
                "wait_result=%{public}d",
                static_cast<unsigned long long>(response.requestId),
                static_cast<int>(waitResult));
            return waitResult == PollEventFdResult::Timeout ? StatusCode::QueueFull : StatusCode::PeerDisconnected;
        }
    }

    void MarkSessionBroken()
    {
        session.SetState(Session::SessionState::Broken);
        if (session.Handles().respEventFd >= 0) {
            (void)SignalEventFd(session.Handles().respEventFd);
        }
    }

    static void CompleteItem(const std::shared_ptr<CompletionState>& completion, StatusCode status)
    {
        if (completion == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(completion->mutex);
        completion->status = status;
        completion->ready = true;
        completion->cv.notify_one();
    }

    void OnCompletionItemFinished()
    {
        std::lock_guard<std::mutex> lock(completionMutex);
        if (pendingCompletionCount > 0) {
            --pendingCompletionCount;
        }
    }

    bool EnqueueCompletion(CompletionItem item)
    {
        std::lock_guard<std::mutex> lock(completionMutex);
        if (!responseWriterRunning.load(std::memory_order_relaxed) ||
            pendingCompletionCount >= completionQueueCapacity) {
            HILOGE(
                "RpcServer::EnqueueCompletion failed: running=%{public}d pending=%{public}u capacity=%{public}u "
                "request_id=%{public}llu",
                responseWriterRunning.load(std::memory_order_relaxed) ? 1 : 0,
                pendingCompletionCount,
                completionQueueCapacity,
                static_cast<unsigned long long>(item.entry.requestId));
            return false;
        }
        completionQueue.push(std::move(item));
        ++pendingCompletionCount;
        completionCv.notify_one();
        return true;
    }

    bool WaitAndPopCompletionItem(CompletionItem* item)
    {
        if (item == nullptr) {
            HILOGE("RpcServer::WaitAndPopCompletionItem failed: item is null");
            return false;
        }
        std::unique_lock<std::mutex> lock(completionMutex);
        completionCv.wait(lock, [this] {
            return !responseWriterRunning.load(std::memory_order_relaxed) || !completionQueue.empty();
        });
        if (!responseWriterRunning.load(std::memory_order_relaxed) && completionQueue.empty()) {
            return false;
        }
        *item = std::move(completionQueue.front());
        completionQueue.pop();
        return true;
    }

    void FailPendingCompletionItems(StatusCode status)
    {
        std::queue<CompletionItem> queued;
        {
            std::lock_guard<std::mutex> lock(completionMutex);
            queued.swap(completionQueue);
            pendingCompletionCount = 0;
        }
        while (!queued.empty()) {
            CompleteItem(queued.front().completion, status);
            queued.pop();
        }
    }

    void StopResponseWriter()
    {
        responseWriterRunning.store(false);
        if (session.Handles().respCreditEventFd >= 0) {
            (void)SignalEventFd(session.Handles().respCreditEventFd);
        }
        completionCv.notify_all();
        if (responseWriterThread.joinable()) {
            responseWriterThread.join();
        }
        FailPendingCompletionItems(StatusCode::PeerDisconnected);
    }

    void ResponseWriterLoop()
    {
        CompletionItem item;
        while (WaitAndPopCompletionItem(&item)) {
            const StatusCode status = PushResponseWithRetry(item.entry, item.retryBudget);
            if (status == StatusCode::Ok && !SignalEventFd(session.Handles().respEventFd)) {
                HILOGW("response published without wakeup signal, request_id=%{public}llu",
                       static_cast<unsigned long long>(item.entry.requestId));
            }
            if (status != StatusCode::Ok) {
                HILOGE(
                    "RpcServer::ResponseWriterLoop completion failed: request_id=%{public}llu status=%{public}d "
                    "break_session=%{public}d",
                    static_cast<unsigned long long>(item.entry.requestId),
                    static_cast<int>(status),
                    item.breakSessionOnFailure ? 1 : 0);
            }
            if (status != StatusCode::Ok && item.breakSessionOnFailure) {
                MarkSessionBroken();
            }
            CompleteItem(item.completion, status);
            OnCompletionItemFinished();
        }
    }

    RpcServerCall BuildServerCall(const RequestRingEntry& requestEntry) const
    {
        RpcServerCall call;
        call.opcode = requestEntry.opcode;
        call.priority = IsHighPriority(requestEntry) ? Priority::High : Priority::Normal;
        call.flags = requestEntry.flags;
        call.execTimeoutMs = requestEntry.execTimeoutMs;
        call.payload = PayloadView(requestEntry.payload.data(), requestEntry.payloadSize);
        return call;
    }

    RpcReply InvokeHandler(const RequestRingEntry& requestEntry)
    {
        RpcReply reply;
        const auto it = handlers.find(requestEntry.opcode);
        if (it == handlers.end()) {
            HILOGE("RpcServer::InvokeHandler missing handler opcode=%{public}u request_id=%{public}llu",
                   requestEntry.opcode,
                   static_cast<unsigned long long>(requestEntry.requestId));
            reply.status = StatusCode::InvalidArgument;
            return reply;
        }

        RpcServerCall call = BuildServerCall(requestEntry);
        it->second(call, &reply);
        return reply;
    }

    bool ValidateResponsePayloadSize(const RequestRingEntry& requestEntry, RpcReply* reply) const
    {
        if (!session.Valid() || session.Header() == nullptr) {
            HILOGE("RpcServer::WriteResponse failed: invalid session request_id=%{public}llu",
                   static_cast<unsigned long long>(requestEntry.requestId));
            return false;
        }

        if (reply->payload.size() > session.MaxResponseBytes() ||
            reply->payload.size() > ResponseRingEntry::INLINE_PAYLOAD_BYTES) {
            HILOGE(
                "RpcServer::WriteResponse payload too large request_id=%{public}llu payload_size=%{public}zu "
                "max=%{public}u inline_max=%{public}u",
                static_cast<unsigned long long>(requestEntry.requestId),
                reply->payload.size(),
                session.MaxResponseBytes(),
                ResponseRingEntry::INLINE_PAYLOAD_BYTES);
            reply->status = StatusCode::PayloadTooLarge;
            reply->payload.clear();
        }
        return true;
    }

    static ResponseRingEntry BuildReplyEntry(const RequestRingEntry& requestEntry, const RpcReply& reply)
    {
        ResponseRingEntry entry;
        entry.requestId = requestEntry.requestId;
        entry.messageKind = ResponseMessageKind::Reply;
        entry.statusCode = static_cast<uint32_t>(reply.status);
        entry.resultSize = static_cast<uint32_t>(reply.payload.size());
        if (!reply.payload.empty()) {
            std::memcpy(entry.payload.data(), reply.payload.data(), reply.payload.size());
        }
        return entry;
    }

    static CompletionItem BuildResponseCompletionItem(const ResponseRingEntry& entry,
                                                      const std::shared_ptr<CompletionState>& completion)
    {
        CompletionItem item;
        item.entry = entry;
        item.retryBudget = RESPONSE_RETRY_BUDGET;
        item.breakSessionOnFailure = true;
        item.completion = completion;
        return item;
    }

    StatusCode AwaitWriteCompletion(const RequestRingEntry& requestEntry,
                                    const std::shared_ptr<CompletionState>& completion)
    {
        std::unique_lock<std::mutex> lock(completion->mutex);
        completion->cv.wait(lock, [&completion] { return completion->ready; });
        if (completion->status != StatusCode::Ok) {
            HILOGE("RpcServer::WriteResponse completion failed request_id=%{public}llu status=%{public}d",
                   static_cast<unsigned long long>(requestEntry.requestId),
                   static_cast<int>(completion->status));
        }
        return completion->status;
    }

    StatusCode WriteResponse(const RequestRingEntry& requestEntry, RpcReply reply)
    {
        if (!ValidateResponsePayloadSize(requestEntry, &reply)) {
            return StatusCode::PeerDisconnected;
        }

        const ResponseRingEntry entry = BuildReplyEntry(requestEntry, reply);
        auto completion = std::make_shared<CompletionState>();
        CompletionItem item = BuildResponseCompletionItem(entry, completion);
        if (!EnqueueCompletion(std::move(item))) {
            HILOGE("RpcServer::WriteResponse failed to enqueue completion request_id=%{public}llu",
                   static_cast<unsigned long long>(requestEntry.requestId));
            MarkSessionBroken();
            return StatusCode::PeerDisconnected;
        }
        return AwaitWriteCompletion(requestEntry, completion);
    }

    StatusCode PublishEvent(const RpcEvent& event)
    {
        if (!running.load(std::memory_order_acquire) || session.State() != Session::SessionState::Alive) {
            HILOGW("RpcServer::PublishEvent rejected: server stopping or session broken");
            return StatusCode::PeerDisconnected;
        }
        if (!session.Valid() || session.Handles().respEventFd < 0 || session.Header() == nullptr) {
            HILOGE("PublishEvent failed, session is not ready");
            return StatusCode::EngineInternalError;
        }
        if (event.payload.size() > session.MaxResponseBytes() ||
            event.payload.size() > ResponseRingEntry::INLINE_PAYLOAD_BYTES) {
            HILOGE(
                "RpcServer::PublishEvent failed: payload too large size=%{public}zu max=%{public}u "
                "inline_max=%{public}u",
                event.payload.size(),
                session.MaxResponseBytes(),
                ResponseRingEntry::INLINE_PAYLOAD_BYTES);
            return StatusCode::PayloadTooLarge;
        }

        ResponseRingEntry entry;
        entry.messageKind = ResponseMessageKind::Event;
        entry.eventDomain = event.eventDomain;
        entry.eventType = event.eventType;
        entry.flags = event.flags;
        entry.resultSize = static_cast<uint32_t>(event.payload.size());
        if (!event.payload.empty()) {
            std::memcpy(entry.payload.data(), event.payload.data(), event.payload.size());
        }

        auto completion = std::make_shared<CompletionState>();
        CompletionItem item;
        item.entry = entry;
        item.retryBudget = EVENT_RETRY_BUDGET;
        item.completion = completion;
        if (!EnqueueCompletion(std::move(item))) {
            HILOGE("RpcServer::PublishEvent failed to enqueue completion");
            return StatusCode::QueueFull;
        }

        std::unique_lock<std::mutex> lock(completion->mutex);
        completion->cv.wait(lock, [&completion] { return completion->ready; });
        if (completion->status != StatusCode::Ok) {
            HILOGE("RpcServer::PublishEvent completion failed status=%{public}d", static_cast<int>(completion->status));
        }
        return completion->status;
    }

    void ProcessEntry(const RequestRingEntry& requestEntry)
    {
        if (!session.Valid() || session.Header() == nullptr) {
            HILOGE("RpcServer::ProcessEntry aborted: invalid session request_id=%{public}llu",
                   static_cast<unsigned long long>(requestEntry.requestId));
            return;
        }
        if (requestEntry.payloadSize > session.MaxRequestBytes() ||
            requestEntry.payloadSize > RequestRingEntry::INLINE_PAYLOAD_BYTES) {
            HILOGE(
                "RpcServer::ProcessEntry payload too large request_id=%{public}llu payload_size=%{public}u "
                "max=%{public}u inline_max=%{public}u",
                static_cast<unsigned long long>(requestEntry.requestId),
                requestEntry.payloadSize,
                session.MaxRequestBytes(),
                RequestRingEntry::INLINE_PAYLOAD_BYTES);
            RpcReply reply;
            reply.status = StatusCode::PayloadTooLarge;
            (void)WriteResponse(requestEntry, std::move(reply));
            return;
        }
        MarkExecutionStarted(requestEntry.requestId);
        const auto clearExecution =
            MakeScopeExit([this, requestId = requestEntry.requestId] { MarkExecutionFinished(requestId); });
        RpcReply reply = InvokeHandler(requestEntry);
        const StatusCode status = WriteResponse(requestEntry, std::move(reply));
        if (status != StatusCode::Ok) {
            HILOGE("RpcServer::ProcessEntry failed to write response request_id=%{public}llu status=%{public}d",
                   static_cast<unsigned long long>(requestEntry.requestId),
                   static_cast<int>(status));
        }
    }

    bool DrainQueue(QueueKind kind, TaskExecutor* executor)
    {
        if (executor == nullptr || session.Header() == nullptr) {
            return false;
        }

        bool drained = false;
        RequestRingEntry entry;
        bool ringBecameNotFull = false;
        while (executor->HasCapacity() && session.PopRequest(kind, &entry)) {
            const RingCursor& cursor =
                kind == QueueKind::HighRequest ? session.Header()->highRing : session.Header()->normalRing;
            if (cursor.capacity != 0 && RingCount(cursor) + 1U == cursor.capacity) {
                ringBecameNotFull = true;
            }
            drained = true;
            const RequestRingEntry captured = entry;
            std::shared_ptr<void> submittedTask = MakeSubmittedTaskToken();
            auto self = shared_from_this();
            if (!executor->TrySubmit([self = std::move(self), captured, submittedTask = std::move(submittedTask)] {
                    self->ProcessEntry(captured);
                })) {
                HILOGE("RpcServer::DrainQueue failed to submit task request_id=%{public}llu opcode=%{public}u",
                       static_cast<unsigned long long>(captured.requestId),
                       captured.opcode);
            }
        }
        if (ringBecameNotFull) {
            if (!SignalEventFd(session.Handles().reqCreditEventFd)) {
                HILOGW("RpcServer::DrainQueue failed to signal request credit");
            }
        }
        return drained;
    }

    bool HandleBackloggedQueues()
    {
        if (session.Header() == nullptr || highExecutor == nullptr || normalExecutor == nullptr) {
            return false;
        }
        const bool highBacklogged = RingCount(session.Header()->highRing) > 0 && !highExecutor->HasCapacity();
        const bool normalBacklogged = RingCount(session.Header()->normalRing) > 0 && !normalExecutor->HasCapacity();
        if (highBacklogged) {
            (void)highExecutor->WaitForCapacity(std::chrono::milliseconds(100));
            return true;
        }
        if (normalBacklogged) {
            (void)normalExecutor->WaitForCapacity(std::chrono::milliseconds(100));
            return true;
        }
        return false;
    }

    bool SpinForRingActivity(int iterations) const
    {
        if (session.Header() == nullptr) {
            return false;
        }
        for (int i = 0; i < iterations; ++i) {
            if (RingCount(session.Header()->highRing) > 0 || RingCount(session.Header()->normalRing) > 0) {
                return true;
            }
            CpuRelax();
        }
        return false;
    }

    void DispatcherLoop()
    {
        constexpr int SPIN_ITERATIONS = 256;
        std::array<pollfd, 2> fds{{
            {session.Handles().highReqEventFd, POLLIN, 0},
            {session.Handles().normalReqEventFd, POLLIN, 0},
        }};

        while (running.load(std::memory_order_acquire)) {
            bool highWork = DrainQueue(QueueKind::HighRequest, highExecutor.get());
            if (!highWork) {
                DrainQueue(QueueKind::NormalRequest, normalExecutor.get());
            }

            if (HandleBackloggedQueues()) {
                continue;
            }
            if (SpinForRingActivity(SPIN_ITERATIONS)) {
                continue;
            }

            const int pollResult = poll(fds.data(), static_cast<nfds_t>(fds.size()), 100);
            if (pollResult < 0) {
                HILOGE("RpcServer::DispatcherLoop poll failed");
                continue;
            }
            if (pollResult > 0 && (fds[0].revents & POLLIN) != 0) {
                (void)DrainEventFd(fds[0].fd);
                highWork = DrainQueue(QueueKind::HighRequest, highExecutor.get());
            }
            if (pollResult > 0 && !highWork && (fds[1].revents & POLLIN) != 0) {
                (void)DrainEventFd(fds[1].fd);
                DrainQueue(QueueKind::NormalRequest, normalExecutor.get());
            }
        }
    }
};

RpcServer::RpcServer()
    : impl_(std::make_shared<Impl>())
{
}

RpcServer::RpcServer(BootstrapHandles handles, ServerOptions options)
    : impl_(std::make_shared<Impl>())
{
    impl_->handles = handles;
    impl_->options = std::move(options);
}

RpcServer::~RpcServer()
{
    Stop();
}

void RpcServer::SetBootstrapHandles(BootstrapHandles handles)
{
    impl_->handles = handles;
}

void RpcServer::RegisterHandler(Opcode opcode, RpcHandler handler)
{
    if (!handler) {
        impl_->handlers.erase(static_cast<uint16_t>(opcode));
        return;
    }
    impl_->handlers[static_cast<uint16_t>(opcode)] = std::move(handler);
}

void RpcServer::SetOptions(ServerOptions options)
{
    impl_->options = std::move(options);
}

StatusCode RpcServer::PublishEvent(const RpcEvent& event)
{
    return impl_->PublishEvent(event);
}

StatusCode RpcServer::Start()
{
    if (impl_->handlers.empty()) {
        HILOGE("RpcServer::Start failed: no handlers registered");
        return StatusCode::InvalidArgument;
    }
    if (impl_->running.exchange(true)) {
        HILOGW("RpcServer::Start ignored: server already running");
        return StatusCode::Ok;
    }

    const StatusCode attachStatus = impl_->session.Attach(&impl_->handles, Session::AttachRole::Server);
    if (attachStatus != StatusCode::Ok) {
        HILOGE("RpcServer::Start failed: session attach status=%{public}d", static_cast<int>(attachStatus));
        impl_->running.store(false);
        return attachStatus;
    }

    impl_->highExecutor = impl_->options.highExecutor != nullptr
                            ? impl_->options.highExecutor
                            : std::make_shared<ThreadPoolExecutor>(impl_->options.highWorkerThreads);
    impl_->normalExecutor = impl_->options.normalExecutor != nullptr
                              ? impl_->options.normalExecutor
                              : std::make_shared<ThreadPoolExecutor>(impl_->options.normalWorkerThreads);
    impl_->completionQueueCapacity = impl_->options.completionQueueCapacity == 0
                                       ? impl_->session.ResponseRingSize()
                                       : impl_->options.completionQueueCapacity;

    impl_->responseWriterRunning.store(true);
    auto impl = impl_;
    impl_->responseWriterThread = std::thread([impl] { impl->ResponseWriterLoop(); });
    impl_->dispatcherThread = std::thread([impl] { impl->DispatcherLoop(); });
    return StatusCode::Ok;
}

RpcServerRuntimeStats RpcServer::GetRuntimeStats() const
{
    RpcServerRuntimeStats stats;
    {
        std::lock_guard<std::mutex> lock(impl_->completionMutex);
        stats.completionBacklog = impl_->pendingCompletionCount;
        stats.completionBacklogCapacity = impl_->completionQueueCapacity;
    }
    if (impl_->session.Header() != nullptr) {
        stats.highRequestRingPending = RingCount(impl_->session.Header()->highRing);
        stats.normalRequestRingPending = RingCount(impl_->session.Header()->normalRing);
        stats.responseRingPending = RingCount(impl_->session.Header()->responseRing);
    }
    {
        std::lock_guard<std::mutex> lock(impl_->executionMutex);
        stats.activeRequestExecutions = static_cast<uint32_t>(impl_->activeExecutions.size());
        if (!impl_->activeExecutions.empty()) {
            const uint64_t nowMs = MonotonicNowMs();
            auto it = impl_->activeExecutions.begin();
            uint64_t oldestStartMs = it->second;
            for (; it != impl_->activeExecutions.end(); ++it) {
                const auto& [requestId, startMonoMs] = *it;
                (void)requestId;
                oldestStartMs = std::min(oldestStartMs, startMonoMs);
            }
            const uint64_t ageMs = nowMs - oldestStartMs;
            stats.oldestExecutionAgeMs =
                static_cast<uint32_t>(std::min<uint64_t>(ageMs, std::numeric_limits<uint32_t>::max()));
        }
    }
    stats.waitingForResponseCredit = impl_->responseWriterWaitingForCredit.load(std::memory_order_acquire);
    return stats;
}

void RpcServer::Stop()
{
    if (!impl_->running.exchange(false)) {
        return;
    }

    impl_->session.SetState(Session::SessionState::Broken);
    impl_->responseWriterRunning.store(false, std::memory_order_release);

    auto shutdownFuture = std::async(std::launch::async, [impl = impl_] {
        impl->StopResponseWriter();
        if (impl->dispatcherThread.joinable()) {
            if (impl->session.Handles().highReqEventFd >= 0) {
                (void)SignalEventFd(impl->session.Handles().highReqEventFd);
            }
            if (impl->session.Handles().normalReqEventFd >= 0) {
                (void)SignalEventFd(impl->session.Handles().normalReqEventFd);
            }
            impl->dispatcherThread.join();
        }
        auto highExecutor = std::move(impl->highExecutor);
        auto normalExecutor = std::move(impl->normalExecutor);
        if (highExecutor) {
            highExecutor->Stop();
        }
        if (normalExecutor) {
            normalExecutor->Stop();
        }
        highExecutor.reset();
        normalExecutor.reset();
        impl->WaitForSubmittedTasks();
        impl->session.Reset();
    });

    if (shutdownFuture.wait_for(STOP_TIMEOUT) != std::future_status::ready) {
        HILOGE("RpcServer::Stop timed out after %{public}lld ms; forcing process exit",
               static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(STOP_TIMEOUT).count()));
        std::_Exit(EXIT_FAILURE);
    }
    shutdownFuture.get();
}

}  // namespace MemRpc
