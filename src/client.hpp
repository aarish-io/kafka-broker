#pragma once

#include "framing.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace kafka
{
    // BrokerClient provides a minimal TCP networking layer for communicating with the Kafka broker.
    // It encapsulates socket lifecycle and uses the shared framing module for length-prefixed messaging.
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

        // Create socket and connect to the broker
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

            if (::connect(fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0)
            {
                std::string err = std::strerror(errno);
                ::close(fd_);
                fd_ = -1;
                throw std::runtime_error("Failed to connect to broker " + host_ + ":" + std::to_string(port_) + ": " + err);
            }
        }

        // Send framed request and receive framed response over the active TCP connection
        std::string request(const std::string &req_payload)
        {
            if (fd_ < 0)
            {
                throw std::runtime_error("Client is not connected to broker");
            }

            write_frame(fd_, req_payload);

            std::string resp_payload;
            if (!read_frame(fd_, resp_payload))
            {
                throw std::runtime_error("Connection closed by broker");
            }
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
        std::string host_;
        int port_;
        int fd_;
    };
} // namespace kafka
