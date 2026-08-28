#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <cstdint>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <mutex>
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

    std::unordered_map<std::string, std::vector<std::string>> topics;

    // Mutex to protect concurrent access to the shared topics map
    std::mutex topics_mutex;

    enum class RequestType
    {
        PING,
        PRODUCE,
        FETCH,
        INVALID
    };

    struct Request
    {
        RequestType type = RequestType::INVALID;
        std::string topic;
        std::string payload;
    };

    Request parse_request(const std::string &raw_request)
    {
        Request request;

        std::istringstream stream(raw_request);
        std::string command;

        if (!(stream >> command))
        {
            request.type = RequestType::INVALID;
            return request;
        }

        if (command == "PING")
        {
            request.type = RequestType::PING;
        }
        else if (command == "FETCH")
        {
            std::string topic;
            if (!(stream >> topic))
            {
                request.type = RequestType::INVALID;
                return request;
            }
            if (topic.empty())
            {
                request.type = RequestType::INVALID;
                return request;
            }
            request.type = RequestType::FETCH;
            request.topic = topic;
        }
        else if (command == "PRODUCE")
        {
            std::string topic;
            if (!(stream >> topic))
            {
                request.type = RequestType::INVALID;
                return request;
            }
            if (topic.empty())
            {
                request.type = RequestType::INVALID;
                return request;
            }

            // The rest of the stream is the payload (can contain spaces)
            std::string payload;
            std::getline(stream, payload);

            // Remove leading space from space separator between topic and payload
            if (!payload.empty() && payload[0] == ' ')
            {
                payload.erase(0, 1);
            }

            if (payload.empty())
            {
                request.type = RequestType::INVALID;
                return request;
            }

            request.type = RequestType::PRODUCE;
            request.topic = topic;
            request.payload = payload;
        }
        else
        {
            request.type = RequestType::INVALID;
        }

        return request;
    }

    std::string socket_error()
    {
        return std::strerror(errno);
    }

    bool read_exactly(int fd, char *buffer, std::size_t bytes)
    {
        std::size_t total_read = 0;

        while (total_read < bytes)
        {
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

        write_exactly(fd, payload.data(), payload.size());
    }

    void handle_client(int client_fd)
    {
        try
        {
            while (true)
            {
                std::string request;

                if (!read_frame(client_fd, request))
                {
                    std::cout << "client disconnected\n";
                    break;
                }

                std::cout << "received: " << request << '\n';

                std::string response;

                Request parsed = parse_request(request);

                switch (parsed.type)
                {
                    case RequestType::PING:
                        response = "PONG";
                        break;

                    case RequestType::PRODUCE:
                        {
                            // Using operator[] intentionally creates the topic on first PRODUCE
                            std::lock_guard<std::mutex> lock(topics_mutex);
                            topics[parsed.topic].push_back(parsed.payload);
                        }
                        response = "OK";
                        break;

                    case RequestType::FETCH:
                        {
                            // Look up topic using find to avoid auto-creating topic on FETCH
                            std::lock_guard<std::mutex> lock(topics_mutex);
                            auto it = topics.find(parsed.topic);

                            if (it == topics.end())
                            {
                                response = "ERROR";
                            }
                            else
                            {
                                std::vector<std::string> messages = it->second;

                                if (messages.empty())
                                {
                                    response = "";
                                }
                                else
                                {
                                    response = "";
                                    for (size_t i = 0; i < messages.size(); ++i)
                                    {
                                        response += messages[i];
                                        if (i < messages.size() - 1)
                                        {
                                            response += '\n';
                                        }
                                    }
                                }
                            }
                        }
                        break;

                    case RequestType::INVALID:
                    default:
                        response = "ERROR";
                        break;
                }

                write_frame(client_fd, response);

                std::cout << "sent: " << response << '\n';
            }
        }
        catch (const std::exception &error)
        {
            std::cerr << "client handling failed: " << error.what() << '\n';
        }

        close(client_fd);
    }

} // namespace

int main()
{
    try
    {
        int server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0)
        {
            throw std::runtime_error("socket failed: " + socket_error());
        }

        int reuse_address = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char *>(&reuse_address), sizeof(reuse_address));

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

        if (listen(server_fd, kBacklog) < 0)
        {
            close(server_fd);
            throw std::runtime_error("listen failed: " + socket_error());
        }

        std::cout << "broker listening on port " << kPort << '\n';
        std::cout << "waiting for clients to connect...\n";

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
