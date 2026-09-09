#pragma once

#include <cstdint>
#include <string>

namespace kafka
{
    enum class RequestType
    {
        PRODUCE,
        FETCH,
        PING,
        INVALID
    };

    struct Request
    {
        RequestType type = RequestType::INVALID;
        std::string topic;
        int partition = 0;
        std::uint64_t offset = 0;
        std::string payload;
    };

    // Parse a raw request string into a Request struct.
    // Protocol syntax:
    //   PRODUCE <topic> <partition> <payload>
    //   FETCH <topic> <partition> <offset>
    //   PING
    Request parse_request(const std::string &raw_request);

} // namespace kafka
