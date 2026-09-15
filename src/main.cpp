#include "broker.hpp"
#include "epoll_server.hpp"
#include "server_mode.hpp"
#include "tcp_server.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace
{
    bool parse_port(const std::string &value, int &port)
    {
        try
        {
            std::size_t position = 0;
            port = std::stoi(value, &position);

            return position == value.size() && port >= 1 && port <= 65535;
        }
        catch (...)
        {
            return false;
        }
    }
}

int main(int argc, char *argv[])
{
    std::vector<std::string> remaining_args;
    kafka::ServerMode server_mode = kafka::parse_server_mode(argc, argv, remaining_args);

    if (remaining_args.size() != 3 && remaining_args.size() != 4)
    {
        std::cerr << "Usage: ./kafka-broker [--epoll] <port> <data-directory> <leader|follower> [follower-port]\n";
        return 1;
    }

    int port = 0;
    if (!parse_port(remaining_args[0], port))
    {
        std::cerr << "ERROR: Invalid port.\n";
        return 1;
    }

    std::string data_dir = remaining_args[1];

    if (data_dir.empty())
    {
        std::cerr << "ERROR: Data directory cannot be empty.\n";
        return 1;
    }

    std::string role_arg = remaining_args[2];
    kafka::BrokerRole role = kafka::BrokerRole::LEADER;
    int follower_port = -1;

    if (role_arg == "leader")
    {
        role = kafka::BrokerRole::LEADER;
        if (remaining_args.size() != 4)
        {
            std::cerr << "ERROR: follower-port is required for leader.\n";
            return 1;
        }

        if (!parse_port(remaining_args[3], follower_port))
        {
            std::cerr << "ERROR: Invalid follower-port.\n";
            return 1;
        }
    }
    else if (role_arg == "follower")
    {
        if (remaining_args.size() != 3)
        {
            std::cerr << "ERROR: follower-port is only valid for leader.\n";
            return 1;
        }

        role = kafka::BrokerRole::FOLLOWER;
    }
    else
    {
        std::cerr << "ERROR: Role must be 'leader' or 'follower'.\n";
        return 1;
    }

    try
    {
        kafka::Broker broker;
        broker.configure(role, follower_port);
        broker.recover_from_disk(data_dir);

        std::cout << "server mode: " << kafka::server_mode_name(server_mode) << '\n';
        std::cout << "broker configured on port " << port
                  << " with data directory '" << data_dir << "'\n";

        if (server_mode == kafka::ServerMode::EPOLL)
        {
            kafka::EpollServer server(broker, port);
            server.start();
        }
        else
        {
            kafka::TcpServer server(broker, port);
            server.start();
        }
    }
    catch (const std::exception &error)
    {
        std::cerr << "broker failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
