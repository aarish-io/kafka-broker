#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace kafka
{
    // Maximum allowable request/response payload size (64 KiB), matching broker limits.
    constexpr std::uint32_t kMaxClientPayloadSize = 64 * 1024;

    // BrokerClient provides a minimal TCP networking layer for communicating with the Kafka broker.
    // It encapsulates socket lifecycle, length-prefixed message framing, and partial read/write handling.
    class BrokerClient
    {
    public:
        BrokerClient(std::string host = "127.0.0.1", int port = 9092)
            : host_(std::move(host)), port_(port), fd_(-1)
        {
        }

        ~BrokerClient()
        {
            close();
        }

        // Prevent copying to prevent duplicate socket file descriptor ownership
        BrokerClient(const BrokerClient &) = delete;
        BrokerClient &operator=(const BrokerClient &) = delete;

        // Allow moving socket ownership
        BrokerClient(BrokerClient &&other) noexcept
            : host_(std::move(other.host_)), port_(other.port_), fd_(other.fd_)
        {
            other.fd_ = -1;
        }

        BrokerClient &operator=(BrokerClient &&other) noexcept
        {
            if (this != &other)
            {
                close();
                host_ = std::move(other.host_);
                port_ = other.port_;
                fd_ = other.fd_;
                other.fd_ = -1;
            }
            return *this;
        }

        // 5.1.1 Create socket and connect to the broker
        void connect()
        {
            if (fd_ >= 0)
            {
                return;
            }

            fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd_ < 0)
            {
                throw std::runtime_error("Failed to create socket: " + std::string(std::strerror(errno)));
            }

            sockaddr_in addr{};
            std::memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons(static_cast<std::uint16_t>(port_));

            if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0)
            {
                ::close(fd_);
                fd_ = -1;
                throw std::runtime_error("Invalid IP address specified: " + host_);
            }

            // 5.1.2 Connect to the broker
            if (::connect(fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0)
            {
                std::string err = std::strerror(errno);
                ::close(fd_);
                fd_ = -1;
                throw std::runtime_error("Failed to connect to broker " + host_ + ":" + std::to_string(port_) + ": " + err);
            }
        }

        // 5.1.3 & 5.1.4 Send framed request and receive framed response over the active TCP connection
        std::string request(const std::string &req_payload)
        {
            if (fd_ < 0)
            {
                throw std::runtime_error("Client is not connected to broker");
            }

            write_frame(req_payload);

            std::string resp_payload;
            read_frame(resp_payload);
            return resp_payload;
        }

        void close()
        {
            if (fd_ >= 0)
            {
                ::close(fd_);
                fd_ = -1;
            }
        }

        bool is_connected() const
        {
            return fd_ >= 0;
        }

    private:
        bool read_exactly(char *buffer, std::size_t bytes)
        {
            std::size_t total_read = 0;
            while (total_read < bytes)
            {
                ssize_t bytes_read = ::recv(fd_, buffer + total_read, bytes - total_read, 0);
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

        void write_exactly(const char *buffer, std::size_t bytes)
        {
            std::size_t total_sent = 0;
            while (total_sent < bytes)
            {
                ssize_t bytes_sent = ::send(fd_, buffer + total_sent, bytes - total_sent, MSG_NOSIGNAL);
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

        void write_frame(const std::string &payload)
        {
            if (payload.size() > kMaxClientPayloadSize)
            {
                throw std::runtime_error("Request payload exceeds maximum allowed size");
            }

            std::uint32_t payload_size = static_cast<std::uint32_t>(payload.size());
            std::uint32_t network_length = htonl(payload_size);

            write_exactly(reinterpret_cast<const char *>(&network_length), sizeof(network_length));

            if (!payload.empty())
            {
                write_exactly(payload.data(), payload.size());
            }
        }

        void read_frame(std::string &payload)
        {
            char header[sizeof(std::uint32_t)];
            if (!read_exactly(header, sizeof(header)))
            {
                throw std::runtime_error("Connection closed by broker while reading frame header");
            }

            std::uint32_t network_length = 0;
            std::memcpy(&network_length, header, sizeof(network_length));
            std::uint32_t payload_size = ntohl(network_length);

            if (payload_size > kMaxClientPayloadSize)
            {
                throw std::runtime_error("Response frame payload exceeds maximum allowed size");
            }

            payload.resize(payload_size);
            if (payload_size == 0)
            {
                return;
            }

            if (!read_exactly(payload.data(), payload_size))
            {
                throw std::runtime_error("Connection closed by broker while reading frame payload");
            }
        }

        std::string host_;
        int port_;
        int fd_;
    };
} // namespace kafka
