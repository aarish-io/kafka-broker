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

    inline std::uint32_t crc32(const char *data, std::size_t size)
    {
        std::uint32_t crc = 0xFFFFFFFFu;

        for (std::size_t i = 0; i < size; ++i)
        {
            crc ^= static_cast<unsigned char>(data[i]);
            for (int bit = 0; bit < 8; ++bit)
            {
                if ((crc & 1u) != 0)
                {
                    crc = (crc >> 1) ^ 0xEDB88320u;
                }
                else
                {
                    crc >>= 1;
                }
            }
        }

        return crc ^ 0xFFFFFFFFu;
    }

    // Serialize a record into standard binary log format:
    // [4-byte uint32 payload length (network/big-endian order)][payload bytes][4-byte CRC32]
    // CRC32 covers the encoded length bytes followed by the payload bytes.
    inline std::string serialize_record(const Record &record)
    {
        std::uint32_t length = static_cast<std::uint32_t>(record.payload.size());
        std::uint32_t net_length = htonl(length);

        std::string buffer;
        buffer.reserve(sizeof(net_length) + record.payload.size() + sizeof(std::uint32_t));
        buffer.append(reinterpret_cast<const char *>(&net_length), sizeof(net_length));
        buffer.append(record.payload);

        std::uint32_t checksum = crc32(buffer.data(), buffer.size());
        std::uint32_t net_checksum = htonl(checksum);
        buffer.append(reinterpret_cast<const char *>(&net_checksum), sizeof(net_checksum));

        return buffer;
    }

    // Deserialize a record from a binary buffer starting at buffer_offset.
    // Returns true on success and advances buffer_offset by the size of the record (4 + length + 4).
    // Returns false if buffer does not contain a complete record or CRC validation fails.
    inline bool deserialize_record(const std::string &buffer, std::size_t &buffer_offset, Record &out_record)
    {
        const std::size_t record_start = buffer_offset;
        const std::size_t length_size = sizeof(std::uint32_t);
        const std::size_t checksum_size = sizeof(std::uint32_t);

        if (record_start > buffer.size() || buffer.size() - record_start < length_size)
        {
            return false;
        }

        std::uint32_t net_length = 0;
        std::memcpy(&net_length, buffer.data() + record_start, sizeof(net_length));
        std::uint32_t payload_length = ntohl(net_length);

        if (buffer.size() - record_start - length_size < payload_length)
        {
            return false;
        }

        const std::size_t payload_start = record_start + length_size;
        const std::size_t checksum_start = payload_start + payload_length;
        if (buffer.size() - checksum_start < checksum_size)
        {
            return false;
        }

        std::uint32_t net_stored_checksum = 0;
        std::memcpy(&net_stored_checksum, buffer.data() + checksum_start, sizeof(net_stored_checksum));
        std::uint32_t stored_checksum = ntohl(net_stored_checksum);
        std::uint32_t calculated_checksum = crc32(buffer.data() + record_start, length_size + payload_length);

        if (calculated_checksum != stored_checksum)
        {
            return false;
        }

        out_record.payload = buffer.substr(payload_start, payload_length);
        buffer_offset = checksum_start + checksum_size;

        return true;
    }

    // Returns partition-specific log path: data/<topic>/partition-<id>.log
    inline std::string get_log_path(const std::string &topic, int partition_id)
    {
        return "data/" + topic + "/partition-" + std::to_string(partition_id) + ".log";
    }

} // namespace kafka
