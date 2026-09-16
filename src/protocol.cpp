#include "protocol.hpp"

#include <sstream>
#include <stdexcept>

namespace kafka
{
    static bool has_extra_tokens(std::istringstream &iss)
    {
        std::string extra;
        return static_cast<bool>(iss >> extra);
    }

    static bool parse_partition(const std::string &partition_str, int &partition)
    {
        try
        {
            size_t pos = 0;
            int parsed_partition = std::stoi(partition_str, &pos);
            if (pos != partition_str.length() || parsed_partition < 0 || parsed_partition >= 3)
            {
                return false;
            }
            partition = parsed_partition;
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    static bool parse_non_negative_int(const std::string &value, int &result)
    {
        try
        {
            size_t pos = 0;
            int parsed_value = std::stoi(value, &pos);
            if (pos != value.length() || parsed_value < 0)
            {
                return false;
            }
            result = parsed_value;
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    static bool parse_offset(const std::string &offset_str, std::uint64_t &offset)
    {
        try
        {
            size_t pos = 0;
            long long parsed_offset = std::stoll(offset_str, &pos);
            if (pos != offset_str.length() || parsed_offset < 0)
            {
                return false;
            }
            offset = static_cast<std::uint64_t>(parsed_offset);
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    Request parse_request(const std::string &raw_request)
    {
        Request req;
        req.type = RequestType::INVALID;

        std::istringstream iss(raw_request);
        std::string command;
        iss >> command;

        if (command == "PING")
        {
            if (has_extra_tokens(iss))
            {
                return req; // INVALID
            }
            req.type = RequestType::PING;
            return req;
        }

        if (command == "METRICS")
        {
            if (has_extra_tokens(iss))
            {
                return req; // INVALID
            }
            req.type = RequestType::METRICS;
            return req;
        }

        if (command == "PRODUCE")
        {
            std::string topic;
            std::string partition_str;
            std::string payload;

            if (!(iss >> topic >> partition_str))
            {
                return req; // INVALID
            }

            // Read the rest of the line as payload
            std::getline(iss, payload);
            // Remove leading whitespace from payload
            size_t first_non_space = payload.find_first_not_of(' ');
            if (first_non_space != std::string::npos)
            {
                payload = payload.substr(first_non_space);
            }

            if (topic.empty() || payload.empty())
            {
                return req; // INVALID
            }

            if (!parse_partition(partition_str, req.partition))
            {
                return req; // INVALID
            }

            req.type = RequestType::PRODUCE;
            req.topic = topic;
            req.payload = payload;
            return req;
        }

        if (command == "FETCH")
        {
            std::string topic;
            std::string partition_str;
            std::string offset_str;

            if (!(iss >> topic >> partition_str >> offset_str))
            {
                return req; // INVALID
            }

            if (has_extra_tokens(iss))
            {
                return req; // INVALID
            }

            if (!parse_partition(partition_str, req.partition))
            {
                return req; // INVALID
            }

            if (!parse_offset(offset_str, req.offset))
            {
                return req; // INVALID
            }

            req.type = RequestType::FETCH;
            req.topic = topic;
            return req;
        }

        if (command == "REPLICATE")
        {
            std::string topic;
            std::string partition_str;
            std::string offset_str;
            std::string payload;

            if (!(iss >> topic >> partition_str >> offset_str))
            {
                return req; // INVALID
            }

            std::getline(iss, payload);
            size_t first_non_space = payload.find_first_not_of(' ');
            if (first_non_space != std::string::npos)
            {
                payload = payload.substr(first_non_space);
            }

            if (topic.empty() || payload.empty())
            {
                return req; // INVALID
            }

            if (!parse_non_negative_int(partition_str, req.partition))
            {
                return req; // INVALID
            }

            if (!parse_offset(offset_str, req.offset))
            {
                return req; // INVALID
            }

            req.type = RequestType::REPLICATE;
            req.topic = topic;
            req.payload = payload;
            return req;
        }

        if (command == "REPLICATION_PROGRESS")
        {
            std::string topic;
            std::string partition_str;

            if (!(iss >> topic >> partition_str) || has_extra_tokens(iss))
            {
                return req; // INVALID
            }

            if (!parse_non_negative_int(partition_str, req.partition))
            {
                return req; // INVALID
            }

            req.type = RequestType::REPLICATION_PROGRESS;
            req.topic = topic;
            return req;
        }

        if (command == "JOIN")
        {
            std::string group_id;
            std::string consumer_id;
            std::string topic;

            if (!(iss >> group_id >> consumer_id >> topic) || has_extra_tokens(iss))
            {
                return req; // INVALID
            }

            req.type = RequestType::JOIN;
            req.group_id = group_id;
            req.consumer_id = consumer_id;
            req.topic = topic;
            return req;
        }

        if (command == "LEAVE")
        {
            std::string group_id;
            std::string consumer_id;

            if (!(iss >> group_id >> consumer_id) || has_extra_tokens(iss))
            {
                return req; // INVALID
            }

            req.type = RequestType::LEAVE;
            req.group_id = group_id;
            req.consumer_id = consumer_id;
            return req;
        }

        if (command == "GROUP_POLL")
        {
            std::string group_id;
            std::string consumer_id;

            if (!(iss >> group_id >> consumer_id) || has_extra_tokens(iss))
            {
                return req; // INVALID
            }

            req.type = RequestType::GROUP_POLL;
            req.group_id = group_id;
            req.consumer_id = consumer_id;
            return req;
        }

        if (command == "COMMIT")
        {
            std::string group_id;
            std::string consumer_id;
            std::string topic;
            std::string partition_str;
            std::string offset_str;

            if (!(iss >> group_id >> consumer_id >> topic >> partition_str >> offset_str) || has_extra_tokens(iss))
            {
                return req; // INVALID
            }

            if (!parse_partition(partition_str, req.partition))
            {
                return req; // INVALID
            }

            if (!parse_offset(offset_str, req.offset))
            {
                return req; // INVALID
            }

            req.type = RequestType::COMMIT;
            req.group_id = group_id;
            req.consumer_id = consumer_id;
            req.topic = topic;
            return req;
        }

        // Unknown command
        return req;
    }

} // namespace kafka
