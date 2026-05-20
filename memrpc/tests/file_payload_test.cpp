#include <gtest/gtest.h>

#include <dirent.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "memrpc/core/file_payload.h"
#include "memrpc/core/protocol.h"

namespace MemRpc {
namespace {

class ScopedFilePayloadDir {
public:
    ScopedFilePayloadDir()
    {
        ClearFilePayloads(path());
    }

    ~ScopedFilePayloadDir()
    {
        ClearFilePayloads(path());
    }

    [[nodiscard]] const std::string& path() const
    {
        static const std::string kPath = std::string(DEFAULT_FILE_PAYLOAD_DIR) + "_test_" + std::to_string(getpid());
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

std::vector<uint8_t> MakePatternPayload(std::size_t size, uint32_t seed)
{
    std::vector<uint8_t> payload(size);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>((seed * 13U + static_cast<uint32_t>(i) * 29U) & 0xffU);
    }
    return payload;
}

}  // namespace

TEST(FilePayloadTest, ClearRemovesAllFilesInPayloadDir)
{
    ScopedFilePayloadDir payloadDir;
    const std::string filePayload = payloadDir.path() + "/leftover";
    const std::string otherFile = payloadDir.path() + "/unrelated";
    {
        std::ofstream(filePayload) << "payload";
        std::ofstream(otherFile) << "other";
    }

    ASSERT_TRUE(ClearFilePayloads(payloadDir.path()));
    EXPECT_NE(access(filePayload.c_str(), F_OK), 0);
    EXPECT_NE(access(otherFile.c_str(), F_OK), 0);
}

TEST(FilePayloadTest, ClearCreatesFilePayloadDirectory)
{
    ScopedFilePayloadDir payloadDir;
    ClearFilePayloads(payloadDir.path());
    rmdir(payloadDir.path().c_str());

    EXPECT_TRUE(ClearFilePayloads(payloadDir.path()));
    EXPECT_EQ(access(payloadDir.path().c_str(), F_OK), 0);
}

TEST(FilePayloadTest, SmallPayloadStaysInline)
{
    ScopedFilePayloadDir payloadDir;
    std::vector<uint8_t> payload{1, 2, 3, 4};

    std::vector<uint8_t> transportPayload;
    uint8_t payloadKind = PAYLOAD_KIND_INLINE;
    ASSERT_TRUE(PrepareFilePayloadForTransport(
        payloadDir.path(), payload, RequestRingEntry::INLINE_PAYLOAD_BYTES, &transportPayload, &payloadKind));
    EXPECT_EQ(payloadKind, PAYLOAD_KIND_INLINE);
    EXPECT_EQ(transportPayload, payload);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(FilePayloadTest, OversizedPayloadUsesFileTransportAndConsumesFile)
{
    ScopedFilePayloadDir payloadDir;
    std::vector<uint8_t> payload(RequestRingEntry::INLINE_PAYLOAD_BYTES + 128U, 0x5a);

    std::vector<uint8_t> transportPayload;
    uint8_t payloadKind = PAYLOAD_KIND_INLINE;
    ASSERT_TRUE(PrepareFilePayloadForTransport(
        payloadDir.path(), payload, RequestRingEntry::INLINE_PAYLOAD_BYTES, &transportPayload, &payloadKind));
    EXPECT_EQ(payloadKind, PAYLOAD_KIND_FILE_REF);
    EXPECT_LE(transportPayload.size(), RequestRingEntry::INLINE_PAYLOAD_BYTES);
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), 1);

    std::vector<uint8_t> resolvedPayload;
    ASSERT_TRUE(ResolveFilePayloadFromTransport(payloadDir.path(),
                                                transportPayload.data(),
                                                transportPayload.size(),
                                                payloadKind,
                                                &resolvedPayload));
    EXPECT_EQ(resolvedPayload, payload);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(FilePayloadTest, RemovesFilePayloadFromTransportEnvelope)
{
    ScopedFilePayloadDir payloadDir;
    std::vector<uint8_t> payload(RequestRingEntry::INLINE_PAYLOAD_BYTES + 128U, 0x7b);

    std::vector<uint8_t> transportPayload;
    uint8_t payloadKind = PAYLOAD_KIND_INLINE;
    ASSERT_TRUE(PrepareFilePayloadForTransport(
        payloadDir.path(), payload, RequestRingEntry::INLINE_PAYLOAD_BYTES, &transportPayload, &payloadKind));
    ASSERT_EQ(payloadKind, PAYLOAD_KIND_FILE_REF);
    ASSERT_EQ(CountFilePayloadFiles(payloadDir.path()), 1);

    EXPECT_TRUE(RemoveFilePayloadFromTransport(payloadDir.path(),
                                               transportPayload.data(),
                                               transportPayload.size(),
                                               payloadKind));
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(FilePayloadTest, ConcurrentPrepareAndResolveUsesIndependentFilesAndCleansUp)
{
    ScopedFilePayloadDir payloadDir;
    constexpr int kThreads = 8;
    constexpr int kIterations = 8;

    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int threadIndex = 0; threadIndex < kThreads; ++threadIndex) {
        workers.emplace_back([&, threadIndex] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int iteration = 0; iteration < kIterations; ++iteration) {
                const uint32_t seed = static_cast<uint32_t>(threadIndex * kIterations + iteration + 1);
                const std::vector<uint8_t> payload =
                    MakePatternPayload(RequestRingEntry::INLINE_PAYLOAD_BYTES + 64U + seed, seed);
                std::vector<uint8_t> transportPayload;
                uint8_t payloadKind = PAYLOAD_KIND_INLINE;
                if (!PrepareFilePayloadForTransport(payloadDir.path(),
                                                    payload,
                                                    RequestRingEntry::INLINE_PAYLOAD_BYTES,
                                                    &transportPayload,
                                                    &payloadKind) ||
                    payloadKind != PAYLOAD_KIND_FILE_REF) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                std::vector<uint8_t> resolvedPayload;
                if (!ResolveFilePayloadFromTransport(payloadDir.path(),
                                                     transportPayload.data(),
                                                     transportPayload.size(),
                                                     payloadKind,
                                                     &resolvedPayload) ||
                    resolvedPayload != payload) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(CountFilePayloadFiles(payloadDir.path()), 0);
}

TEST(FilePayloadTest, RejectsUnknownPayloadKind)
{
    ScopedFilePayloadDir payloadDir;
    std::vector<uint8_t> payload;
    const uint8_t data = 0;

    EXPECT_FALSE(ResolveFilePayloadFromTransport(payloadDir.path(), &data, 1U, 99U, &payload));
}

}  // namespace MemRpc
