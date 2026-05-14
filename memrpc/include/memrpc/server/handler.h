#ifndef MEMRPC_SERVER_HANDLER_H_
#define MEMRPC_SERVER_HANDLER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <vector>

#include "memrpc/core/protocol.h"
#include "memrpc/core/types.h"

namespace MemRpc {

class PayloadView {
public:
    PayloadView() = default;
    PayloadView(const uint8_t* data, std::size_t size)
        : data_(data),
          size_(size)
    {
    }

    [[nodiscard]] const uint8_t* data() const
    {
        return data_;
    }
    [[nodiscard]] std::size_t size() const
    {
        return size_;
    }
    [[nodiscard]] bool empty() const
    {
        return size_ == 0;
    }
    [[nodiscard]] const uint8_t& front() const
    {
        return *data_;
    }
    [[nodiscard]] const uint8_t* begin() const
    {
        return data_;
    }
    [[nodiscard]] const uint8_t* end() const
    {
        return std::next(data_, static_cast<std::ptrdiff_t>(size_));
    }

    // Compatibility aliases (PascalCase)
    [[nodiscard]] const uint8_t* Data() const
    {
        return data_;
    }
    [[nodiscard]] std::size_t Size() const
    {
        return size_;
    }
    [[nodiscard]] bool Empty() const
    {
        return empty();
    }
    [[nodiscard]] const uint8_t& Front() const
    {
        return front();
    }
    [[nodiscard]] const uint8_t* Begin() const
    {
        return begin();
    }
    [[nodiscard]] const uint8_t* End() const
    {
        return end();
    }

    [[nodiscard]] std::vector<uint8_t> ToVector() const
    {
        return {Begin(), End()};
    }

    operator std::vector<uint8_t>() const
    {
        return ToVector();
    }

private:
    const uint8_t* data_ = nullptr;
    std::size_t size_ = 0;
};

inline bool operator==(PayloadView lhs, const std::vector<uint8_t>& rhs)
{
    return lhs.Size() == rhs.size() && std::equal(lhs.Begin(), lhs.End(), rhs.begin(), rhs.end());
}

inline bool operator==(const std::vector<uint8_t>& lhs, PayloadView rhs)
{
    return rhs == lhs;
}

struct RpcServerCall {
    Opcode opcode = OPCODE_INVALID;
    Priority priority = Priority::Normal;
    RequestFlags flags = REQUEST_FLAG_NONE;
    uint32_t execTimeoutMs = 0;
    PayloadView payload;
};

using RpcServerReply = RpcReply;  // compatibility alias while server call sites migrate.
using RpcHandler = std::function<void(const RpcServerCall&, RpcReply*)>;

}  // namespace MemRpc

#endif  // MEMRPC_SERVER_HANDLER_H_
