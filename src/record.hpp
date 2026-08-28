#pragma once

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <string>

namespace kafka
{
    // Persistent record representation for topic log storage.
    struct Record
    {
        std::string payload;
    };

    // Serialize a record into standard binary log format:
    // [4-byte uint32 payload length (network/big-endian order)][payload bytes]
    inline std::string serialize_record(const Record &record)
    {
        std::uint32_t length = static_cast<std::uint32_t>(record.payload.size());
        std::uint32_t net_length = htonl(length);

        std::string buffer;
        buffer.reserve(sizeof(net_length) + record.payload.size());
        buffer.append(reinterpret_cast<const char *>(&net_length), sizeof(net_length));
        buffer.append(record.payload);

        return buffer;
    }

    // Deserialize a record from a binary buffer starting at buffer_offset.
    // Returns true on success and advances buffer_offset by the size of the record (4 + length).
    // Returns false if buffer does not contain a complete record.
    inline bool deserialize_record(const std::string &buffer, std::size_t &buffer_offset, Record &out_record)
    {
        if (buffer_offset + sizeof(std::uint32_t) > buffer.size())
        {
            return false;
        }

        std::uint32_t net_length = 0;
        std::memcpy(&net_length, buffer.data() + buffer_offset, sizeof(net_length));
        std::uint32_t payload_length = ntohl(net_length);

        if (buffer_offset + sizeof(std::uint32_t) + payload_length > buffer.size())
        {
            return false;
        }

        buffer_offset += sizeof(std::uint32_t);
        out_record.payload = buffer.substr(buffer_offset, payload_length);
        buffer_offset += payload_length;

        return true;
    }

    // Returns standard topic log path: data/<topic>.log
    inline std::string get_log_path(const std::string &topic)
    {
        return "data/" + topic + ".log";
    }

} // namespace kafka
