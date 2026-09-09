#include "protocol.hpp"

#include <sstream>
#include <stdexcept>

namespace kafka
{
    Request parse_request(const std::string &raw_request)
    {
        Request req;
        req.type = RequestType::INVALID;

        std::istringstream iss(raw_request);
        std::string command;
        iss >> command;

        if (command == "PING")
        {
            req.type = RequestType::PING;
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

            // Parse partition
            try
            {
                size_t pos = 0;
                int partition = std::stoi(partition_str, &pos);
                if (pos != partition_str.length() || partition < 0 || partition >= 3)
                {
                    return req; // INVALID - out of range partition
                }
                req.partition = partition;
            }
            catch (...)
            {
                return req; // INVALID - non-numeric partition
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

            // Parse partition
            try
            {
                size_t pos = 0;
                int partition = std::stoi(partition_str, &pos);
                if (pos != partition_str.length() || partition < 0 || partition >= 3)
                {
                    return req; // INVALID - out of range partition
                }
                req.partition = partition;
            }
            catch (...)
            {
                return req; // INVALID - non-numeric partition
            }

            // Parse offset
            try
            {
                size_t pos = 0;
                long long offset = std::stoll(offset_str, &pos);
                if (pos != offset_str.length() || offset < 0)
                {
                    return req; // INVALID - negative offset
                }
                req.offset = static_cast<std::uint64_t>(offset);
            }
            catch (...)
            {
                return req; // INVALID - non-numeric offset
            }

            req.type = RequestType::FETCH;
            req.topic = topic;
            return req;
        }

        // Unknown command
        return req;
    }

} // namespace kafka
