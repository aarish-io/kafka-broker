#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace kafka
{
    class Broker;

    // 8.4.1 Maintain per-client read state.
    // Each client keeps the bytes that have arrived but do not yet form a
    // complete frame, so partial TCP reads can be accumulated across events.
    struct ClientState
    {
        int fd;
        std::string read_buffer;
        // 8.5.1 Maintain bytes that have not yet been written to the client.
        std::string write_buffer;
        std::size_t write_offset = 0;
        // 8.6.1 A TCP connection owns the group memberships it successfully
        // joined, mirroring TcpServer so disconnect cleanup is identical.
        std::vector<std::pair<std::string, std::string>> joined_groups;
    };

    // 8.1.1 Define the future epoll server boundary without changing runtime behavior.
    class EpollServer
    {
    public:
        EpollServer(Broker &broker, int port = 9092, int backlog = 8);
        ~EpollServer();

        void start();

    private:
        void setup_server();
        void setup_epoll();
        void run_event_loop();
        void accept_clients();
        void read_from_client(int client_fd);
        bool extract_complete_frames(ClientState &client);
        bool queue_response(int client_fd, const std::string &response);
        void write_to_client(int client_fd);
        void close_client(int client_fd);

        // 8.1.2 Keep Broker independent of the future networking implementation.
        Broker &broker_;
        int port_;
        int backlog_;
        int server_fd_;
        int epoll_fd_;
        std::unordered_map<int, ClientState> clients_;
    };

} // namespace kafka
