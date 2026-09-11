#include "../src/client.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

struct Assignment
{
    std::string topic;
    int partition = 0;
    std::uint64_t offset = 0;
};

static std::vector<Assignment> parse_assignments(const std::string &response)
{
    std::vector<Assignment> assignments;
    std::istringstream lines(response);
    std::string line;

    while (std::getline(lines, line))
    {
        if (line.empty())
        {
            continue;
        }

        std::istringstream fields(line);
        Assignment assignment;
        if (!(fields >> assignment.topic >> assignment.partition >> assignment.offset))
        {
            throw std::runtime_error("Invalid GROUP_POLL assignment response from broker");
        }

        std::string extra;
        if (fields >> extra)
        {
            throw std::runtime_error("Invalid GROUP_POLL assignment response from broker");
        }

        assignments.push_back(std::move(assignment));
    }

    return assignments;
}

static std::size_t count_messages(const std::string &response)
{
    if (response.empty())
    {
        return 0;
    }

    return std::count(response.begin(), response.end(), '\n') + 1;
}

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        std::cerr << "Usage: ./consumer <topic> <group_id> <consumer_id>\n";
        return 1;
    }

    std::string topic = argv[1];
    std::string group_id = argv[2];
    std::string consumer_id = argv[3];

    if (topic.empty())
    {
        std::cerr << "ERROR: Topic name cannot be empty.\n";
        return 1;
    }

    if (group_id.empty())
    {
        std::cerr << "ERROR: Group ID cannot be empty.\n";
        return 1;
    }

    if (consumer_id.empty())
    {
        std::cerr << "ERROR: Consumer ID cannot be empty.\n";
        return 1;
    }

    try
    {
        kafka::BrokerClient client("127.0.0.1", 9092);
        client.connect();

        bool joined = false;
        std::string join_response = client.request("JOIN " + group_id + " " + consumer_id + " " + topic);
        if (join_response != "OK")
        {
            std::cout << join_response << std::endl;
            return 1;
        }
        joined = true;

        while (true)
        {
            std::string poll_response = client.request("GROUP_POLL " + group_id + " " + consumer_id);
            if (poll_response == "ERROR")
            {
                std::cout << poll_response << std::endl;
                break;
            }

            std::vector<Assignment> assignments = parse_assignments(poll_response);
            if (assignments.empty())
            {
                break;
            }

            bool consumed_any = false;

            for (Assignment &assignment : assignments)
            {
                std::string fetch_request = "FETCH " + assignment.topic + " " +
                                            std::to_string(assignment.partition) + " " +
                                            std::to_string(assignment.offset);

                std::string fetch_response = client.request(fetch_request);
                if (fetch_response == "ERROR")
                {
                    std::cout << fetch_response << std::endl;
                    consumed_any = false;
                    break;
                }

                std::size_t message_count = count_messages(fetch_response);
                if (message_count == 0)
                {
                    continue;
                }

                std::cout << fetch_response << std::endl;
                assignment.offset += static_cast<std::uint64_t>(message_count);

                std::string commit_request = "COMMIT " + group_id + " " + consumer_id + " " +
                                             assignment.topic + " " +
                                             std::to_string(assignment.partition) + " " +
                                             std::to_string(assignment.offset);

                std::string commit_response = client.request(commit_request);
                if (commit_response != "OK")
                {
                    std::cout << commit_response << std::endl;
                    consumed_any = false;
                    break;
                }

                consumed_any = true;
            }

            if (!consumed_any)
            {
                break;
            }
        }

        if (joined)
        {
            client.request("LEAVE " + group_id + " " + consumer_id);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "ERROR: Client network exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
