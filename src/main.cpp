#include "broker.hpp"
#include "tcp_server.hpp"

#include <iostream>

int main()
{
    try
    {
        kafka::Broker broker;
        broker.recover_from_disk();

        kafka::TcpServer server(broker);
        server.start();
    }
    catch (const std::exception &error)
    {
        std::cerr << "broker failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
