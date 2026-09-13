#include "epoll_server.hpp"

#include "broker.hpp"
#include "framing.hpp"
#include "protocol.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace kafka
{
    static std::string socket_error()
    {
        return std::string(std::strerror(errno));
    }

    // 8.4.4 The existing wire format is [4-byte network-order uint32 payload
    // length][payload]. This is the size of that length header.
    constexpr std::size_t kFrameHeaderSize = sizeof(std::uint32_t);

    EpollServer::EpollServer(Broker &broker, int port, int backlog)
        : broker_(broker), port_(port), backlog_(backlog), server_fd_(-1), epoll_fd_(-1)
    {
    }

    EpollServer::~EpollServer()
    {
        for (const auto &entry : clients_)
        {
            ::close(entry.second.fd);
        }

        if (epoll_fd_ >= 0)
        {
            ::close(epoll_fd_);
        }

        if (server_fd_ >= 0)
        {
            ::close(server_fd_);
        }
    }

    void EpollServer::start()
    {
        try
        {
            setup_server();
            setup_epoll();
            run_event_loop();
        }
        catch (...)
        {
            if (epoll_fd_ >= 0)
            {
                ::close(epoll_fd_);
                epoll_fd_ = -1;
            }

            if (server_fd_ >= 0)
            {
                ::close(server_fd_);
                server_fd_ = -1;
            }

            for (const auto &entry : clients_)
            {
                ::close(entry.second.fd);
            }
            clients_.clear();

            throw;
        }
    }

    void EpollServer::setup_server()
    {
        // 8.2.1 Create the listening socket.
        server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd_ < 0)
        {
            throw std::runtime_error("socket failed: " + socket_error());
        }

        int reuse_address = 1;
        ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char *>(&reuse_address), sizeof(reuse_address));

        // 8.2.2 Configure the socket as non-blocking.
        int flags = ::fcntl(server_fd_, F_GETFL, 0);
        if (flags < 0)
        {
            throw std::runtime_error("fcntl F_GETFL failed: " + socket_error());
        }

        if (::fcntl(server_fd_, F_SETFL, flags | O_NONBLOCK) < 0)
        {
            throw std::runtime_error("fcntl F_SETFL failed: " + socket_error());
        }

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = htonl(INADDR_ANY);
        server_address.sin_port = htons(static_cast<std::uint16_t>(port_));

        if (::bind(server_fd_, reinterpret_cast<sockaddr *>(&server_address),
                   sizeof(server_address)) < 0)
        {
            throw std::runtime_error("bind failed: " + socket_error());
        }

        if (::listen(server_fd_, backlog_) < 0)
        {
            throw std::runtime_error("listen failed: " + socket_error());
        }

        std::cout << "epoll server listening on port " << port_ << '\n';
    }

    void EpollServer::setup_epoll()
    {
        // 8.2.3 Create the epoll instance.
        epoll_fd_ = ::epoll_create1(0);
        if (epoll_fd_ < 0)
        {
            throw std::runtime_error("epoll_create1 failed: " + socket_error());
        }

        // 8.2.4 Register the listening socket for EPOLLIN.
        epoll_event server_event{};
        server_event.events = EPOLLIN;
        server_event.data.fd = server_fd_;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &server_event) < 0)
        {
            throw std::runtime_error("epoll_ctl failed: " + socket_error());
        }
    }

    void EpollServer::run_event_loop()
    {
        epoll_event events[8]{};

        while (true)
        {
            // 8.2.5 Wait for readiness events.
            int ready_count = ::epoll_wait(epoll_fd_, events, 8, -1);
            if (ready_count < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                throw std::runtime_error("epoll_wait failed: " + socket_error());
            }

            for (int index = 0; index < ready_count; ++index)
            {
                int event_fd = events[index].data.fd;

                // 8.3.4 Distinguish server and client events.
                if (event_fd == server_fd_)
                {
                    if ((events[index].events & EPOLLIN) != 0)
                    {
                        accept_clients();
                    }
                    continue;
                }

                if (clients_.find(event_fd) == clients_.end())
                {
                    std::cerr << "ignoring event for unknown file descriptor " << event_fd << '\n';
                    continue;
                }

                if ((events[index].events & (EPOLLERR | EPOLLHUP)) != 0)
                {
                    std::cerr << "closing client " << event_fd << " after epoll error or hangup\n";
                    close_client(event_fd);
                    continue;
                }

                if ((events[index].events & EPOLLIN) != 0)
                {
                    read_from_client(event_fd);
                }

                if ((events[index].events & EPOLLOUT) != 0 &&
                    clients_.find(event_fd) != clients_.end())
                {
                    write_to_client(event_fd);
                }
            }
        }
    }

    void EpollServer::accept_clients()
    {
        // 8.3.1 Accept every client currently pending on the non-blocking socket.
        while (true)
        {
            int client_fd = ::accept(server_fd_, nullptr, nullptr);
            if (client_fd < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    return;
                }

                std::cerr << "accept failed: " << socket_error() << '\n';
                return;
            }

            // 8.3.2 Configure the accepted client as non-blocking.
            int flags = ::fcntl(client_fd, F_GETFL, 0);
            if (flags < 0 || ::fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) < 0)
            {
                std::cerr << "failed to configure client socket: " << socket_error() << '\n';
                ::close(client_fd);
                continue;
            }

            // 8.3.3 Register the client with epoll for read readiness only.
            epoll_event client_event{};
            client_event.events = EPOLLIN;
            client_event.data.fd = client_fd;

            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &client_event) < 0)
            {
                std::cerr << "failed to register client with epoll: " << socket_error() << '\n';
                ::close(client_fd);
                continue;
            }

            try
            {
                // 8.4.1 Create the per-client read state together with the
                // epoll registration, preserving the existing rollback path.
                ClientState client_state{client_fd, std::string(), std::string(), 0, {}};
                clients_.emplace(client_fd, std::move(client_state));
            }
            catch (...)
            {
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
                ::close(client_fd);
                throw;
            }

            std::cout << "accepted client " << client_fd << '\n';
        }
    }

    void EpollServer::read_from_client(int client_fd)
    {
        auto client_it = clients_.find(client_fd);
        if (client_it == clients_.end())
        {
            std::cerr << "no client state for file descriptor " << client_fd << '\n';
            return;
        }

        ClientState &client = client_it->second;

        while (true)
        {
            // 8.4.2 Read available bytes without blocking.
            char read_buffer[4096];
            ssize_t bytes_read = ::recv(client_fd, read_buffer, sizeof(read_buffer), 0);

            if (bytes_read > 0)
            {
                // 8.4.3 Handle partial TCP data: retain every byte even when it
                // does not yet form a complete frame.
                client.read_buffer.append(read_buffer, static_cast<std::size_t>(bytes_read));

                if (!extract_complete_frames(client))
                {
                    close_client(client_fd);
                    return;
                }

                continue; // more data may already be available; read again
            }

            if (bytes_read == 0)
            {
                // Peer performed an orderly shutdown. This is not a protocol error;
                // close and remove the client.
                std::cout << "client " << client_fd << " closed the connection\n";
                close_client(client_fd);
                return;
            }

            if (errno == EINTR)
            {
                continue; // interrupted by a signal; retry recv()
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // The receive buffer is drained for now. Keep any incomplete
                // bytes in the client buffer and return to epoll_wait().
                return;
            }

            std::cerr << "recv from client " << client_fd << " failed: " << socket_error() << '\n';
            close_client(client_fd);
            return;
        }
    }

    bool EpollServer::extract_complete_frames(ClientState &client)
    {
        while (client.read_buffer.size() >= kFrameHeaderSize)
        {
            // 8.4.4 Detect complete frames using the existing wire format:
            // the first 4 bytes are the network-order uint32 payload length.
            std::uint32_t network_length = 0;
            std::memcpy(&network_length, client.read_buffer.data(), kFrameHeaderSize);
            std::uint32_t payload_length = ntohl(network_length);

            // 8.4.6 Validate the frame length against the existing framing
            // maximum payload size; reject the client on violation.
            if (payload_length > kMaxPayloadSize)
            {
                std::cerr << "closing client " << client.fd << ": frame payload length "
                          << payload_length << " exceeds maximum " << kMaxPayloadSize << '\n';
                return false;
            }

            std::size_t frame_size = kFrameHeaderSize + payload_length;
            if (client.read_buffer.size() < frame_size)
            {
                break; // incomplete frame; wait for more EPOLLIN events
            }

            std::string payload = client.read_buffer.substr(kFrameHeaderSize, payload_length);
            std::cout << "received: " << payload << '\n';

            // 8.6.2 Use the existing protocol parser and Broker path exactly
            // like TcpServer: parse the frame, run the Broker operation, and
            // queue the same logical response.
            try
            {
                Request req = parse_request(payload);
                std::string response = broker_.handle_request(req);

                // 8.6.3 A connection owns the group memberships it
                // successfully joined, mirroring TcpServer so disconnect
                // cleanup removes the same members.
                if (response == "OK" && req.type == RequestType::JOIN)
                {
                    client.joined_groups.emplace_back(req.group_id, req.consumer_id);
                }
                else if (response == "OK" && req.type == RequestType::LEAVE)
                {
                    for (auto membership = client.joined_groups.begin();
                         membership != client.joined_groups.end(); ++membership)
                    {
                        if (membership->first == req.group_id && membership->second == req.consumer_id)
                        {
                            client.joined_groups.erase(membership);
                            break;
                        }
                    }
                }

                // 8.6.4 Mirror the framed TcpServer write path, which rejects
                // responses larger than the maximum payload size and closes
                // the connection.
                if (response.size() > kMaxPayloadSize)
                {
                    std::cerr << "client " << client.fd << ": response payload length "
                              << response.size() << " exceeds maximum " << kMaxPayloadSize << '\n';
                    return false;
                }

                std::cout << "sent: " << response << '\n';

                if (!queue_response(client.fd, response))
                {
                    return false;
                }
            }
            catch (const std::exception &error)
            {
                std::cerr << "client " << client.fd << " request handling failed: "
                          << error.what() << '\n';
                return false;
            }

            // 8.4.5 Remove the completed frame from the buffer while preserving
            // any trailing bytes that belong to the next frame. The loop then
            // keeps extracting while another complete frame is present.
            client.read_buffer.erase(0, frame_size);
        }

        return true;
    }

    bool EpollServer::queue_response(int client_fd, const std::string &response)
    {
        auto client_it = clients_.find(client_fd);
        if (client_it == clients_.end())
        {
            return false;
        }

        // 8.5.2 Queue a stage-specific response using the existing frame format.
        std::uint32_t response_length = htonl(static_cast<std::uint32_t>(response.size()));
        ClientState &client = client_it->second;
        client.write_buffer.append(reinterpret_cast<const char *>(&response_length),
                                   sizeof(response_length));
        client.write_buffer.append(response);

        // 8.5.3 Keep reads enabled and request write readiness while output is pending.
        epoll_event client_event{};
        client_event.events = EPOLLIN | EPOLLOUT;
        client_event.data.fd = client_fd;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &client_event) < 0)
        {
            std::cerr << "failed to enable EPOLLOUT for client " << client_fd
                      << ": " << socket_error() << '\n';
            return false;
        }

        return true;
    }

    void EpollServer::write_to_client(int client_fd)
    {
        auto client_it = clients_.find(client_fd);
        if (client_it == clients_.end())
        {
            return;
        }

        ClientState &client = client_it->second;

        while (client.write_offset < client.write_buffer.size())
        {
            // 8.5.4 Write as many currently available bytes as possible without blocking.
            const char *pending_data = client.write_buffer.data() + client.write_offset;
            std::size_t pending_size = client.write_buffer.size() - client.write_offset;
            ssize_t bytes_sent = ::send(client_fd, pending_data, pending_size, MSG_NOSIGNAL);

            if (bytes_sent > 0)
            {
                // 8.5.5 Advance past sent bytes and preserve the unsent suffix.
                client.write_offset += static_cast<std::size_t>(bytes_sent);
                continue;
            }

            if (bytes_sent == 0)
            {
                std::cerr << "send to client " << client_fd << " returned zero bytes\n";
                close_client(client_fd);
                return;
            }

            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }

            std::cerr << "send to client " << client_fd << " failed: " << socket_error() << '\n';
            close_client(client_fd);
            return;
        }

        client.write_buffer.clear();
        client.write_offset = 0;

        // 8.5.6 Stop EPOLLOUT notifications once all queued bytes are sent.
        epoll_event client_event{};
        client_event.events = EPOLLIN;
        client_event.data.fd = client_fd;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &client_event) < 0)
        {
            std::cerr << "failed to disable EPOLLOUT for client " << client_fd
                      << ": " << socket_error() << '\n';
            close_client(client_fd);
        }
    }

    void EpollServer::close_client(int client_fd)
    {
        // 8.6.5 A TCP connection owns the group memberships it successfully
        // joined; remove them when the connection closes, exactly like the
        // threaded TcpServer disconnect path.
        auto client_it = clients_.find(client_fd);
        if (client_it != clients_.end())
        {
            for (const auto &membership : client_it->second.joined_groups)
            {
                broker_.remove_consumer_from_group(membership.first, membership.second);
            }
        }

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr) < 0 && errno != ENOENT)
        {
            std::cerr << "failed to remove client from epoll: " << socket_error() << '\n';
        }

        ::close(client_fd);
        clients_.erase(client_fd); // drop the stale per-client state
    }

} // namespace kafka
