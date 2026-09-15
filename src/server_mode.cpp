#include "server_mode.hpp"

#include <string>
#include <vector>

namespace kafka
{
    ServerMode parse_server_mode(int argc, char *argv[], std::vector<std::string> &remaining_args)
    {
        remaining_args.clear();
        bool saw_epoll = false;

        for (int index = 1; index < argc; ++index)
        {
            std::string argument = argv[index];
            if (argument == "--epoll")
            {
                saw_epoll = true;
                continue;
            }
            remaining_args.push_back(argument);
        }

        return saw_epoll ? ServerMode::EPOLL : ServerMode::TCP;
    }

    std::string server_mode_name(ServerMode mode)
    {
        return mode == ServerMode::EPOLL ? "epoll" : "tcp";
    }

} // namespace kafka
