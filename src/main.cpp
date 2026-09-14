#include "broker.hpp"
#include "tcp_server.hpp"

#include <iostream>
#include <string>

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
    if (argc != 4 && argc != 5)
    {
        std::cerr << "Usage: ./kafka-broker <port> <data-directory> <leader|follower> [follower-port]\n";
        return 1;
    }

    int port = 0;
    if (!parse_port(argv[1], port))
    {
        std::cerr << "ERROR: Invalid port.\n";
        return 1;
    }

    std::string data_dir = argv[2];

    if (data_dir.empty())
    {
        std::cerr << "ERROR: Data directory cannot be empty.\n";
        return 1;
    }

    std::string role_arg = argv[3];
    kafka::BrokerRole role = kafka::BrokerRole::LEADER;
    int follower_port = -1;

    if (role_arg == "leader")
    {
        role = kafka::BrokerRole::LEADER;
        if (argc != 5)
        {
            std::cerr << "ERROR: follower-port is required for leader.\n";
            return 1;
        }

        if (!parse_port(argv[4], follower_port))
        {
            std::cerr << "ERROR: Invalid follower-port.\n";
            return 1;
        }
    }
    else if (role_arg == "follower")
    {
        if (argc != 4)
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

        // 10.2.1 Recover this broker from its own data directory.
        broker.recover_from_disk(data_dir);

        // 10.2.2 Start this broker on its configured port.
        kafka::TcpServer server(broker, port);
        std::cout << "broker configured on port " << port
                  << " with data directory '" << data_dir << "'\n";

        server.start();
    }
    catch (const std::exception &error)
    {
        std::cerr << "broker failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
