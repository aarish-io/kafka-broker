#pragma once

#include <cstdint>
#include <string>

namespace kafka
{
    enum class RequestType
    {
        PRODUCE,
        FETCH,
        JOIN,
        LEAVE,
        GROUP_POLL,
        COMMIT,
        REPLICATE,
        REPLICATION_PROGRESS,
        PING,
        INVALID
    };

    struct Request
    {
        RequestType type = RequestType::INVALID;
        std::string group_id;
        std::string consumer_id;
        std::string topic;
        int partition = 0;
        std::uint64_t offset = 0;
        std::string payload;
    };

    // Parse a raw request string into a Request struct.
    // Protocol syntax:
    //   PRODUCE <topic> <partition> <payload>
    //   FETCH <topic> <partition> <offset>
    //   JOIN <group> <consumer_id> <topic>
    //   LEAVE <group> <consumer_id>
    //   GROUP_POLL <group> <consumer_id>
    //   COMMIT <group> <consumer_id> <topic> <partition> <offset>
    //   REPLICATE <topic> <partition> <offset> <payload>
    //   PING
    //
    // GROUP_POLL response rows are:
    //   <topic> <partition> <committed_offset>
    Request parse_request(const std::string &raw_request);

} // namespace kafka
