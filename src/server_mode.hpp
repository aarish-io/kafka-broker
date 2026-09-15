#pragma once

#include <string>
#include <vector>

namespace kafka
{
    enum class ServerMode
    {
        TCP,
        EPOLL
    };

    ServerMode parse_server_mode(int argc, char *argv[], std::vector<std::string> &remaining_args);
    std::string server_mode_name(ServerMode mode);

} // namespace kafka
