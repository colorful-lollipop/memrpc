#include <gtest/gtest.h>

#include <unistd.h>

#include <dirent.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "ves/ves_codec.h"
#include "ves/ves_file_payload.h"
#include "ves/ves_handler_registration.h"

namespace VirusExecutorService {
namespace {

class ScopedFilePayloadDir {
public:
    ScopedFilePayloadDir()
    {
        path_ = "/tmp/ves_file_payload_test_" + std::to_string(getpid());
        setenv("VES_FILE_PAYLOAD_DIR", path_.c_str(), 1);
        ClearVesFilePayloads();
    }

    ~ScopedFilePayloadDir()
    {
        ClearVesFilePayloads();
        rmdir(path_.c_str());
        unsetenv("VES_FILE_PAYLOAD_DIR");
    }

    [[nodiscard]] const std::string& path() const
    {
        return path_;
    }

private:
    std::string path_;
};

bool IsFilePayloadName(const std::string& name)
{
    return name.rfind("ves_file_payload_", 0) == 0 || name.rfind("ves_payload_", 0) == 0;
}

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
        std::string entryPath = path;
        entryPath.append("/").append(name);
        if (IsFilePayloadName(name)) {
            ++count;
        } else {
            count += CountFilePayloadFiles(entryPath);
        }
    }
    closedir(stream);
    return count;
}

std::vector<uint8_t> EncodeScanTaskPayload(const ScanTask& task)
{
    std::vector<uint8_t> payload;
    EXPECT_TRUE(MemRpc::EncodeMessage(task, &payload));
    return payload;
}

}  // namespace

TEST(VesFilePayloadTest, ClearRemovesOnlyFilePayloadFiles)
{
    ScopedFilePayloadDir payloadDir;
    const std::string filePayload = payloadDir.path() + "/ves_file_payload_leftover";
    const std::string legacyFilePayload = payloadDir.path() + "/ves_payload_leftover";
    const std::string otherFile = payloadDir.path() + "/unrelated";
    {
        std::ofstream(filePayload) << "payload";
        std::ofstream(legacyFilePayload) << "legacy";
        std::ofstream(otherFile) << "other";
    }

    ASSERT_TRUE(ClearVesFilePayloads());
    EXPECT_NE(access(filePayload.c_str(), F_OK), 0);
    EXPECT_NE(access(legacyFilePayload.c_str(), F_OK), 0);
    EXPECT_EQ(access(otherFile.c_str(), F_OK), 0);
    unlink(otherFile.c_str());
}

TEST(VesFilePayloadTest, ClearCreatesFilePayloadDirectory)
{
    ScopedFilePayloadDir payloadDir;
    ClearVesFilePayloads();
    rmdir(payloadDir.path().c_str());

    EXPECT_TRUE(ClearVesFilePayloads());
    EXPECT_EQ(access(payloadDir.path().c_str(), F_OK), 0);
}

TEST(VesFilePayloadTest, SmallPayloadStaysInlineAndDecodesThroughFastPath)
{
    ScopedFilePayloadDir payloadDir;
    ScanTask task{"/data/scan/clean.apk"};
    std::vector<uint8_t> payload = EncodeScanTaskPayload(task);
    MemRpc::RequestFlags flags = VES_REQUEST_FLAG_FILE_PAYLOAD_REF;

    ASSERT_EQ(PrepareVesFilePayloadForMemRpc(&payload, &flags), MemRpc::StatusCode::Ok);
    EXPECT_EQ(flags, MemRpc::REQUEST_FLAG_NONE);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);

    MemRpc::RpcServerCall call;
    call.payload = MemRpc::PayloadView(payload.data(), payload.size());
    ScanTask decoded;
    ASSERT_TRUE(DecodeVesRequestPayload(call, &decoded));
    EXPECT_EQ(decoded.path, task.path);
}

TEST(VesFilePayloadTest, OversizedPayloadUsesFileFlagAndDecodesThroughFilePath)
{
    ScopedFilePayloadDir payloadDir;
    ScanTask task{"/data/" + std::string(MemRpc::DEFAULT_MAX_REQUEST_BYTES + 128U, 'x')};
    std::vector<uint8_t> payload = EncodeScanTaskPayload(task);
    ASSERT_GT(payload.size(), MemRpc::DEFAULT_MAX_REQUEST_BYTES);
    MemRpc::RequestFlags flags = MemRpc::REQUEST_FLAG_NONE;

    ASSERT_EQ(PrepareVesFilePayloadForMemRpc(&payload, &flags), MemRpc::StatusCode::Ok);
    EXPECT_NE(flags & VES_REQUEST_FLAG_FILE_PAYLOAD_REF, 0U);
    EXPECT_LE(payload.size(), MemRpc::DEFAULT_MAX_REQUEST_BYTES);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 1);

    MemRpc::RpcServerCall call;
    call.flags = flags;
    call.payload = MemRpc::PayloadView(payload.data(), payload.size());
    ScanTask decoded;
    ASSERT_TRUE(DecodeVesRequestPayload(call, &decoded));
    EXPECT_EQ(decoded.path, task.path);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(VesFilePayloadTest, FailedDecodeStillConsumesFilePayload)
{
    ScopedFilePayloadDir payloadDir;
    std::vector<uint8_t> payload(MemRpc::DEFAULT_MAX_REQUEST_BYTES + 128U, 0x5a);
    MemRpc::RequestFlags flags = MemRpc::REQUEST_FLAG_NONE;
    ASSERT_EQ(PrepareVesFilePayloadForMemRpc(&payload, &flags), MemRpc::StatusCode::Ok);
    ASSERT_NE(flags & VES_REQUEST_FLAG_FILE_PAYLOAD_REF, 0U);
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), 1);

    MemRpc::RpcServerCall call;
    call.flags = flags;
    call.payload = MemRpc::PayloadView(payload.data(), payload.size());
    ScanTask decoded;
    EXPECT_FALSE(DecodeVesRequestPayload(call, &decoded));
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(VesFilePayloadTest, TypedHandlerWrapperConsumesFilePayloadAfterSuccessfulDecode)
{
    ScopedFilePayloadDir payloadDir;
    ScanTask task{"/data/" + std::string(MemRpc::DEFAULT_MAX_REQUEST_BYTES + 256U, 'h')};
    std::vector<uint8_t> payload = EncodeScanTaskPayload(task);
    MemRpc::RequestFlags flags = MemRpc::REQUEST_FLAG_NONE;
    ASSERT_EQ(PrepareVesFilePayloadForMemRpc(&payload, &flags), MemRpc::StatusCode::Ok);
    ASSERT_NE(flags & VES_REQUEST_FLAG_FILE_PAYLOAD_REF, 0U);

    auto handler = MakeVesTypedHandler<ScanTask, ScanFileReply>([](const ScanTask& request) {
        ScanFileReply reply;
        reply.code = 0;
        reply.threatLevel = request.path.size() > MemRpc::DEFAULT_MAX_REQUEST_BYTES ? 1 : 0;
        return reply;
    });

    MemRpc::RpcServerCall call;
    call.flags = flags;
    call.payload = MemRpc::PayloadView(payload.data(), payload.size());
    MemRpc::RpcReply rpcReply;
    handler(call, &rpcReply);

    ScanFileReply decodedReply;
    ASSERT_EQ(rpcReply.status, MemRpc::StatusCode::Ok);
    ASSERT_TRUE(MemRpc::DecodeMessage<ScanFileReply>(rpcReply.payload, &decodedReply));
    EXPECT_EQ(decodedReply.threatLevel, 1);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(VesFilePayloadTest, ConcurrentTypedHandlerWrapperUsesDistinctFilesAndCleanUp)
{
    ScopedFilePayloadDir payloadDir;
    constexpr int kThreadCount = 8;
    constexpr int kPayloadsPerThread = 8;
    constexpr int kPayloadCount = kThreadCount * kPayloadsPerThread;
    struct PreparedCall {
        std::string path;
        std::vector<uint8_t> payload;
        MemRpc::RequestFlags flags = MemRpc::REQUEST_FLAG_NONE;
        MemRpc::StatusCode status = MemRpc::StatusCode::InvalidArgument;
    };
    std::vector<PreparedCall> calls(kPayloadCount);

    std::vector<std::future<bool>> prepareTasks;
    for (int threadIndex = 0; threadIndex < kThreadCount; ++threadIndex) {
        prepareTasks.emplace_back(std::async(std::launch::async, [threadIndex, &calls]() {
            for (int localIndex = 0; localIndex < kPayloadsPerThread; ++localIndex) {
                const int index = threadIndex * kPayloadsPerThread + localIndex;
                calls[index].path = "/data/concurrent_" + std::to_string(index) + "_" +
                                    std::string(MemRpc::DEFAULT_MAX_REQUEST_BYTES + 64U, 'p');
                if (!MemRpc::EncodeMessage(ScanTask{calls[index].path}, &calls[index].payload)) {
                    return false;
                }
                calls[index].status = PrepareVesFilePayloadForMemRpc(&calls[index].payload, &calls[index].flags);
                if (calls[index].status != MemRpc::StatusCode::Ok ||
                    (calls[index].flags & VES_REQUEST_FLAG_FILE_PAYLOAD_REF) == 0U) {
                    return false;
                }
            }
            return true;
        }));
    }
    for (auto& task : prepareTasks) {
        ASSERT_TRUE(task.get());
    }
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), kPayloadCount);

    const MemRpc::RpcHandler handler = MakeVesTypedHandler<ScanTask, ScanFileReply>([](const ScanTask& request) {
        ScanFileReply reply;
        reply.code = 0;
        reply.threatLevel = request.path.find("concurrent") != std::string::npos ? 1 : 0;
        return reply;
    });

    std::vector<std::future<bool>> decodeTasks;
    for (int threadIndex = 0; threadIndex < kThreadCount; ++threadIndex) {
        decodeTasks.emplace_back(std::async(std::launch::async, [threadIndex, &calls, &handler]() {
            for (int localIndex = 0; localIndex < kPayloadsPerThread; ++localIndex) {
                const int index = threadIndex * kPayloadsPerThread + localIndex;
                MemRpc::RpcServerCall call;
                call.flags = calls[index].flags;
                call.payload = MemRpc::PayloadView(calls[index].payload.data(), calls[index].payload.size());
                MemRpc::RpcReply rpcReply;
                handler(call, &rpcReply);
                ScanFileReply decodedReply;
                if (rpcReply.status != MemRpc::StatusCode::Ok ||
                    !MemRpc::DecodeMessage<ScanFileReply>(rpcReply.payload, &decodedReply) ||
                    decodedReply.threatLevel != 1) {
                    return false;
                }
            }
            return true;
        }));
    }
    for (auto& task : decodeTasks) {
        ASSERT_TRUE(task.get());
    }
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

}  // namespace VirusExecutorService
