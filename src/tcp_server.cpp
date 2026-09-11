#include "tcp_server.hpp"
#include "protocol.hpp"
#include "framing.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace kafka
{
    // Helper to get socket error message
    static std::string socket_error()
    {
        return std::string(std::strerror(errno));
    }

    TcpServer::TcpServer(Broker &broker, int port, int backlog)
        : broker_(broker), port_(port), backlog_(backlog), server_fd_(-1)
    {
    }

    TcpServer::~TcpServer()
    {
        if (server_fd_ >= 0)
        {
            ::close(server_fd_);
        }
    }

    void TcpServer::start()
    {
        server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd_ < 0)
        {
            throw std::runtime_error("socket failed: " + socket_error());
        }

        int reuse_address = 1;
        ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char *>(&reuse_address), sizeof(reuse_address));

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = htonl(INADDR_ANY);
        server_address.sin_port = htons(static_cast<std::uint16_t>(port_));

        if (::bind(server_fd_, reinterpret_cast<sockaddr *>(&server_address),
                   sizeof(server_address)) < 0)
        {
            ::close(server_fd_);
            server_fd_ = -1;
            throw std::runtime_error("bind failed: " + socket_error());
        }

        if (::listen(server_fd_, backlog_) < 0)
        {
            ::close(server_fd_);
            server_fd_ = -1;
            throw std::runtime_error("listen failed: " + socket_error());
        }

        std::cout << "broker listening on port " << port_ << '\n';
        std::cout << "waiting for clients to connect...\n";

        while (true)
        {
            sockaddr_in client_address{};
            socklen_t client_address_size = sizeof(client_address);

            int client_fd = ::accept(server_fd_, reinterpret_cast<sockaddr *>(&client_address), &client_address_size);
            if (client_fd < 0)
            {
                std::cerr << "accept failed: " << socket_error() << '\n';
                continue;
            }

            std::thread client_thread(&TcpServer::handle_client, this, client_fd);
            client_thread.detach();
        }
    }

    void TcpServer::handle_client(int client_fd)
    {
        std::vector<std::pair<std::string, std::string>> joined_groups;

        try
        {
            while (true)
            {
                std::string request_payload;
                if (!read_frame(client_fd, request_payload))
                {
                    break; // Connection closed
                }

                std::cout << "received: " << request_payload << '\n';

                Request req = parse_request(request_payload);
                std::string response = broker_.handle_request(req);

                // 6.2.1 A TCP connection owns the group memberships it successfully joined.
                if (response == "OK" && req.type == RequestType::JOIN)
                {
                    joined_groups.emplace_back(req.group_id, req.consumer_id);
                }
                else if (response == "OK" && req.type == RequestType::LEAVE)
                {
                    for (auto it = joined_groups.begin(); it != joined_groups.end(); ++it)
                    {
                        if (it->first == req.group_id && it->second == req.consumer_id)
                        {
                            joined_groups.erase(it);
                            break;
                        }
                    }
                }

                write_frame(client_fd, response);

                std::cout << "sent: " << response << '\n';
            }
        }
        catch (const std::exception &error)
        {
            std::cerr << "client handling failed: " << error.what() << '\n';
        }

        for (const auto &membership : joined_groups)
        {
            broker_.remove_consumer_from_group(membership.first, membership.second);
        }

        ::close(client_fd);
    }

} // namespace kafka
