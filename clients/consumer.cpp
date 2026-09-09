#include "../src/client.hpp"
#include <algorithm>
#include <iostream>
#include <string>

int main(int argc, char* argv[])
{
    // 5.4.1 Validate command-line arguments
    if (argc != 4)
    {
        std::cerr << "Usage: ./consumer <topic> <partition> <offset>\n";
        return 1;
    }

    std::string topic = argv[1];
    std::string partition_str = argv[2];
    std::string offset_str = argv[3];

    if (topic.empty())
    {
        std::cerr << "ERROR: Topic name cannot be empty.\n";
        return 1;
    }

    // Parse and validate partition argument (must be integer between 0 and 2)
    int partition = 0;
    try
    {
        std::size_t pos = 0;
        partition = std::stoi(partition_str, &pos);
        if (pos != partition_str.length() || partition < 0 || partition > 2)
        {
            std::cerr << "ERROR: Invalid partition '" << partition_str << "'. Partition must be an integer between 0 and 2.\n";
            return 1;
        }
    }
    catch (...)
    {
        std::cerr << "ERROR: Invalid partition '" << partition_str << "'. Partition must be an integer between 0 and 2.\n";
        return 1;
    }

    // Parse and validate initial offset argument (must be a non-negative integer)
    long long current_offset = 0;
    try
    {
        std::size_t pos = 0;
        current_offset = std::stoll(offset_str, &pos);
        if (pos != offset_str.length() || current_offset < 0)
        {
            std::cerr << "ERROR: Invalid offset '" << offset_str << "'. Offset must be a non-negative integer.\n";
            return 1;
        }
    }
    catch (...)
    {
        std::cerr << "ERROR: Invalid offset '" << offset_str << "'. Offset must be a non-negative integer.\n";
        return 1;
    }

    // 5.4.2 Connect to the broker once over a persistent TCP connection
    try
    {
        kafka::BrokerClient client("127.0.0.1", 9092);
        client.connect();

        // 5.4.3 Sequentially FETCH messages and advance current offset position
        while (true)
        {
            std::string request = "FETCH " + topic + " " + std::to_string(partition) + " " + std::to_string(current_offset);

            // Send FETCH request and receive response over the active connection
            std::string response = client.request(request);

            // 5.4.4 Parse response, display messages, and update offset position
            if (response == "ERROR")
            {
                std::cout << response << std::endl;
                break;
            }

            if (response.empty())
            {
                // No more messages available at current_offset
                break;
            }

            // Print received messages
            std::cout << response << std::endl;

            // Count messages in response (newline-separated) to advance current_offset
            std::size_t message_count = std::count(response.begin(), response.end(), '\n') + 1;
            current_offset += static_cast<long long>(message_count);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "ERROR: Client network exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

