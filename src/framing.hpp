#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace kafka
{
    // Maximum allowable request/response payload size (64 KiB)
    constexpr std::uint32_t kMaxPayloadSize = 64 * 1024;

    // Read exactly 'bytes' bytes from the socket into buffer.
    // Returns true on success, false if connection closed before all bytes read.
    // Throws on error (except EINTR which is retried).
    inline bool read_exactly(int fd, char *buffer, std::size_t bytes)
    {
        std::size_t total_read = 0;
        while (total_read < bytes)
        {
            ssize_t bytes_read = ::recv(fd, buffer + total_read, bytes - total_read, 0);
            if (bytes_read < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                throw std::runtime_error("recv failed: " + std::string(std::strerror(errno)));
            }
            if (bytes_read == 0)
            {
                return false;
            }
            total_read += static_cast<std::size_t>(bytes_read);
        }
        return true;
    }

    // Write exactly 'bytes' bytes to the socket from buffer.
    // Throws on error (except EINTR which is retried).
    // Uses MSG_NOSIGNAL to prevent SIGPIPE on broken connection.
    inline void write_exactly(int fd, const char *buffer, std::size_t bytes)
    {
        std::size_t total_sent = 0;
        while (total_sent < bytes)
        {
            ssize_t bytes_sent = ::send(fd, buffer + total_sent, bytes - total_sent, MSG_NOSIGNAL);
            if (bytes_sent < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                throw std::runtime_error("send failed: " + std::string(std::strerror(errno)));
            }
            if (bytes_sent == 0)
            {
                throw std::runtime_error("send returned zero bytes");
            }
            total_sent += static_cast<std::size_t>(bytes_sent);
        }
    }

    // Write a length-prefixed frame to the socket.
    // Frame format: [4-byte uint32 network-order length][payload bytes]
    inline void write_frame(int fd, const std::string &payload)
    {
        if (payload.size() > kMaxPayloadSize)
        {
            throw std::runtime_error("Frame payload exceeds maximum allowed size");
        }

        std::uint32_t payload_size = static_cast<std::uint32_t>(payload.size());
        std::uint32_t network_length = htonl(payload_size);

        write_exactly(fd, reinterpret_cast<const char *>(&network_length), sizeof(network_length));

        if (!payload.empty())
        {
            write_exactly(fd, payload.data(), payload.size());
        }
    }

    // Read a length-prefixed frame from the socket.
    // Returns true on success, false if connection closed.
    // Throws if frame exceeds maximum size or on read error.
    inline bool read_frame(int fd, std::string &payload)
    {
        char header[sizeof(std::uint32_t)];
        if (!read_exactly(fd, header, sizeof(header)))
        {
            return false;
        }

        std::uint32_t network_length = 0;
        std::memcpy(&network_length, header, sizeof(network_length));
        std::uint32_t payload_size = ntohl(network_length);

        if (payload_size > kMaxPayloadSize)
        {
            throw std::runtime_error("Frame payload exceeds maximum allowed size");
        }

        payload.resize(payload_size);
        if (payload_size == 0)
        {
            return true;
        }

        if (!read_exactly(fd, payload.data(), payload_size))
        {
            throw std::runtime_error("Connection closed while reading frame payload");
        }

        return true;
    }

} // namespace kafka
