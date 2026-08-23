#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <cstdint>

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace
{

    constexpr int kPort = 9092;
    constexpr int kBacklog = 8;
    constexpr std::uint32_t kMaxPayloadSize = 64 * 1024;

    std::string socket_error()
    {
        return std::strerror(errno);
    }

    bool read_exactly(int fd, char *buffer, std::size_t bytes)
    {
        std::size_t total_read = 0;

        while (total_read < bytes)
        {
            // 1.4.1 Read until the requested number of bytes arrives.
            ssize_t bytes_read =
                recv(fd, buffer + total_read, bytes - total_read, 0);

            if (bytes_read < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                throw std::runtime_error("recv failed: " + socket_error());
            }

            if (bytes_read == 0)
            {
                return false;
            }

            total_read += static_cast<std::size_t>(bytes_read);
        }

        return true;
    }

    void write_exactly(int fd, const char *buffer, std::size_t bytes)
    {
        std::size_t total_sent = 0;

        while (total_sent < bytes)
        {
            // 1.4.2 Send until the complete buffer has been transmitted.
            ssize_t bytes_sent =
                send(fd, buffer + total_sent, bytes - total_sent, MSG_NOSIGNAL);

            if (bytes_sent < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                throw std::runtime_error("send failed: " + socket_error());
            }

            if (bytes_sent == 0)
            {
                throw std::runtime_error("send returned zero bytes");
            }

            total_sent += static_cast<std::size_t>(bytes_sent);
        }
    }

    bool read_frame(int fd, std::string &payload)
    {
        char header[sizeof(std::uint32_t)];

        // 1.4.3 Read the fixed-size 4-byte frame header.
        if (!read_exactly(fd, header, sizeof(header)))
        {
            return false;
        }

        std::uint32_t network_length = 0;
        std::memcpy(&network_length, header, sizeof(network_length));

        std::uint32_t payload_size = ntohl(network_length);

        if (payload_size > kMaxPayloadSize)
        {
            throw std::runtime_error("frame payload is too large");
        }

        payload.resize(payload_size);

        if (payload_size == 0)
        {
            return true;
        }

        // 1.4.4 Read exactly the payload bytes described by the header.
        if (!read_exactly(fd, payload.data(), payload_size))
        {
            return false;
        }

        return true;
    }

    void write_frame(int fd, const std::string &payload)
    {
        if (payload.size() > kMaxPayloadSize)
        {
            throw std::runtime_error("response payload is too large");
        }

        // 1.4.5 Encode the payload size as a 4-byte network-order integer.
        std::uint32_t payload_size =
            static_cast<std::uint32_t>(payload.size());

        std::uint32_t network_length = htonl(payload_size);

        write_exactly(
            fd,
            reinterpret_cast<const char *>(&network_length),
            sizeof(network_length));

        if (payload.empty())
        {
            return;
        }

        // 1.4.6 Send the complete payload after its length header.
        write_exactly(fd, payload.data(), payload.size());
    }

    void handle_client(int client_fd)
    {
        try
        {
            std::string request;

            // 1.4.7 Read one complete framed request.
            if (!read_frame(client_fd, request))
            {
                std::cout << "client disconnected before sending a complete request\n";
                close(client_fd);
                return;
            }

            std::cout << "received: " << request << '\n';

            // 1.4.8 The protocol from 1.2 stays unchanged: PING -> PONG, otherwise ERROR.
            std::string response;

            if (request == "PING")
            {
                response = "PONG";
            }
            else
            {
                response = "ERROR";
            }

            // 1.4.9 Send the response as a complete framed message.
            write_frame(client_fd, response);

            std::cout << "sent: " << response << '\n';
        }
        catch (const std::exception &error)
        {
            std::cerr << "client handling failed: " << error.what() << '\n';
        }

        // 1.4.10 Close this client's connection after one request/response.
        close(client_fd);
    }

} // namespace

int main()
{
    try
    {
        // 1.1.1 Create the listening socket: this is the server's door.
        int server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0)
        {
            throw std::runtime_error("socket failed: " + socket_error());
        }

        int reuse_address = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char *>(&reuse_address), sizeof(reuse_address));

        // 1.1.2 Bind gives the server socket a local address: 0.0.0.0:9092.
        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = htonl(INADDR_ANY);
        server_address.sin_port = htons(kPort);

        if (bind(server_fd, reinterpret_cast<sockaddr *>(&server_address),
                 sizeof(server_address)) < 0)
        {
            close(server_fd);
            throw std::runtime_error("bind failed: " + socket_error());
        }

        // 1.1.3 Listen changes the socket into a passive socket that waits for clients.
        if (listen(server_fd, kBacklog) < 0)
        {
            close(server_fd);
            throw std::runtime_error("listen failed: " + socket_error());
        }

        std::cout << "broker listening on port " << kPort << '\n';
        std::cout << "waiting for clients to connect...\n";

        // 1.3.3 Main now stays in the accept loop instead of handling the client itself.
        while (true)
        {
            sockaddr_in client_address{};
            socklen_t client_address_size = sizeof(client_address);

            int client_fd =
                accept(server_fd, reinterpret_cast<sockaddr *>(&client_address), &client_address_size);
            if (client_fd < 0)
            {
                std::cerr << "accept failed: " << socket_error() << '\n';
                continue;
            }

            // 1.3.4 detach lets the worker continue while main immediately accepts again.
            std::thread client_thread(handle_client, client_fd);
            client_thread.detach();
        }
    }
    catch (const std::exception &error)
    {
        std::cerr << "broker failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
