#include <gtest/gtest.h>

#include <cstddef>

#include "core/shm_layout.h"
#include "memrpc/test_support/dev_bootstrap.h"
#include "memrpc/core/protocol.h"

TEST(ProtocolLayoutTest, ConstantsAndEntrySizesAreStable)
{
    EXPECT_EQ(MemRpc::SHARED_MEMORY_MAGIC, 0x4d454d52U);
    EXPECT_EQ(MemRpc::PROTOCOL_VERSION, 9U);
    EXPECT_EQ(MemRpc::RING_ENTRY_BYTES, 8192U);
    EXPECT_EQ(MemRpc::RequestRingEntry::HEADER_BYTES, 20U);
    EXPECT_EQ(MemRpc::RequestRingEntry::INLINE_PAYLOAD_BYTES, 8172U);
    EXPECT_EQ(MemRpc::ResponseRingEntry::HEADER_BYTES, 31U);
    EXPECT_EQ(MemRpc::ResponseRingEntry::INLINE_PAYLOAD_BYTES, 8161U);
    EXPECT_EQ(MemRpc::DEFAULT_MAX_REQUEST_BYTES, static_cast<uint32_t>(MemRpc::RequestRingEntry::INLINE_PAYLOAD_BYTES));
    EXPECT_EQ(MemRpc::DEFAULT_MAX_RESPONSE_BYTES,
              static_cast<uint32_t>(MemRpc::ResponseRingEntry::INLINE_PAYLOAD_BYTES));
    EXPECT_EQ(sizeof(MemRpc::RequestRingEntry), MemRpc::RING_ENTRY_BYTES);
    EXPECT_EQ(sizeof(MemRpc::ResponseRingEntry), MemRpc::RING_ENTRY_BYTES);
}

TEST(ProtocolLayoutTest, InlinePayloadLimitsStayWithinEntryBudget)
{
    EXPECT_EQ(sizeof(MemRpc::RequestRingEntry) - MemRpc::RequestRingEntry::INLINE_PAYLOAD_BYTES,
              MemRpc::RequestRingEntry::HEADER_BYTES);
    EXPECT_EQ(sizeof(MemRpc::ResponseRingEntry) - MemRpc::ResponseRingEntry::INLINE_PAYLOAD_BYTES,
              MemRpc::ResponseRingEntry::HEADER_BYTES);
}

TEST(ProtocolLayoutTest, DemoBootstrapDefaultsAreSizedForSmallSessions)
{
    MemRpc::SharedMemorySessionConfig config;
    EXPECT_EQ(config.highRingSize, MemRpc::DEFAULT_HIGH_RING_SIZE);
    EXPECT_EQ(config.normalRingSize, MemRpc::DEFAULT_NORMAL_RING_SIZE);
    EXPECT_EQ(config.responseRingSize, MemRpc::DEFAULT_RESPONSE_RING_SIZE);
    EXPECT_EQ(config.maxRequestBytes, MemRpc::DEFAULT_MAX_REQUEST_BYTES);
    EXPECT_EQ(config.maxResponseBytes, MemRpc::DEFAULT_MAX_RESPONSE_BYTES);
}

TEST(ProtocolLayoutTest, OffsetsIncreaseMonotonically)
{
    const MemRpc::LayoutConfig config{
        8,
        16,
        32,
        MemRpc::DEFAULT_MAX_REQUEST_BYTES,
        MemRpc::DEFAULT_MAX_RESPONSE_BYTES,
    };
    const MemRpc::Layout layout = MemRpc::ComputeLayout(config);
    EXPECT_LT(layout.highRingOffset, layout.normalRingOffset);
    EXPECT_LT(layout.normalRingOffset, layout.responseRingOffset);
    EXPECT_LT(layout.responseRingOffset, layout.totalSize);
}

TEST(ProtocolLayoutTest, ResponseRingRegionIsProperlyAligned)
{
    const MemRpc::LayoutConfig config{
        8,
        8,
        8,
        MemRpc::DEFAULT_MAX_REQUEST_BYTES,
        MemRpc::DEFAULT_MAX_RESPONSE_BYTES,
    };
    const MemRpc::Layout layout = MemRpc::ComputeLayout(config);

    EXPECT_EQ(layout.highRingOffset % alignof(MemRpc::RequestRingEntry), 0U);
    EXPECT_EQ(layout.normalRingOffset % alignof(MemRpc::RequestRingEntry), 0U);
    EXPECT_EQ(layout.responseRingOffset % alignof(MemRpc::ResponseRingEntry), 0U);
}

TEST(ProtocolLayoutTest, RingCursorLayoutMatchesSpscHeadTailModel)
{
    EXPECT_EQ(sizeof(MemRpc::RingCursor), 16U);
    MemRpc::RingCursor cursor;
    cursor.head.store(2, std::memory_order_relaxed);
    cursor.tail.store(5, std::memory_order_relaxed);
    cursor.capacity = 8;
    EXPECT_EQ(MemRpc::RingCount(cursor), 3U);
}

TEST(ProtocolLayoutTest, ResponseRingEntryDistinguishesReplyAndEventMessages)
{
    MemRpc::ResponseRingEntry reply;
    reply.messageKind = MemRpc::ResponseMessageKind::Reply;
    reply.requestId = 123;
    reply.resultSize = 3;

    MemRpc::ResponseRingEntry event;
    event.messageKind = MemRpc::ResponseMessageKind::Event;
    event.eventDomain = 7;
    event.eventType = 9;
    event.flags = 11;
    event.resultSize = 2;

    EXPECT_NE(reply.messageKind, event.messageKind);
    EXPECT_EQ(event.requestId, 0U);
    EXPECT_EQ(reply.resultSize, 3U);
    EXPECT_EQ(event.resultSize, 2U);
}
