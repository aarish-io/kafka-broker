#pragma once

#include "broker.hpp"

namespace kafka
{
    // TCP Server handles socket lifecycle and client connections.
    // Uses thread-per-client model.
    class TcpServer
    {
    public:
        TcpServer(Broker &broker, int port = 9092, int backlog = 8);
        ~TcpServer();

        // Start the server (bind, listen, then accept connections).
        // This method blocks and runs indefinitely.
        void start();

    private:
        // Handle a single client connection (runs in separate thread).
        void handle_client(int client_fd);

        Broker &broker_;
        int port_;
        int backlog_;
        int server_fd_;
    };

} // namespace kafka
