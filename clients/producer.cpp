#include "../src/client.hpp"
#include <iostream>
#include <string>

int main(int argc, char* argv[])
{
    // 5.2.1 Validate command-line arguments
    if (argc != 4)
    {
        std::cerr << "Usage: ./producer <topic> <partition> <message>\n";
        return 1;
    }

    std::string topic = argv[1];
    std::string partition_str = argv[2];
    std::string message = argv[3];

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

    if (message.empty())
    {
        std::cerr << "ERROR: Message payload cannot be empty.\n";
        return 1;
    }

    // 5.2.2 Build the PRODUCE request
    std::string request = "PRODUCE " + topic + " " + std::to_string(partition) + " " + message;

    // 5.2.3 Connect to the broker and send the request
    try
    {
        kafka::BrokerClient client("127.0.0.1", 9092);
        client.connect();

        // 5.2.4 Send request and receive response
        std::string response = client.request(request);

        // Display the result from the broker (e.g. "OK" or "ERROR ...")
        std::cout << response << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "ERROR: Client network exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
