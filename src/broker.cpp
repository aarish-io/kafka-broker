#include "broker.hpp"

#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace kafka
{
    // Topic constructor - initializes with default partitions
    Topic::Topic(const std::string &topic_name) : name(topic_name)
    {
        partitions.reserve(Broker::kDefaultPartitionCount);
        for (int i = 0; i < Broker::kDefaultPartitionCount; ++i)
        {
            partitions.emplace_back(i);
        }
    }

    void Broker::recover_from_disk(const std::string &data_dir)
    {
        std::error_code ec;
        if (!std::filesystem::exists(data_dir, ec) || !std::filesystem::is_directory(data_dir, ec))
        {
            std::cout << "data directory '" << data_dir << "' does not exist. Starting with empty state.\n";
            return;
        }

        std::size_t loaded_topics = 0;
        std::size_t loaded_messages = 0;

        // Iterate over topic directories (data/<topic>/)
        for (const auto &topic_entry : std::filesystem::directory_iterator(data_dir, ec))
        {
            if (ec)
            {
                std::cerr << "error iterating data directory: " << ec.message() << '\n';
                break;
            }

            if (!topic_entry.is_directory())
            {
                continue;
            }

            std::string topic_name = topic_entry.path().filename().string();
            if (topic_name.empty())
            {
                continue;
            }

            Topic topic(topic_name);
            bool found_any_partition = false;

            // Iterate over partition log files within each topic directory
            for (const auto &partition_entry : std::filesystem::directory_iterator(topic_entry.path(), ec))
            {
                if (ec)
                {
                    std::cerr << "error iterating topic directory: " << ec.message() << '\n';
                    break;
                }

                if (!partition_entry.is_regular_file() || partition_entry.path().extension() != ".log")
                {
                    continue;
                }

                std::string filename = partition_entry.path().stem().string();
                if (filename.rfind("partition-", 0) != 0)
                {
                    continue;
                }

                std::string partition_id_str = filename.substr(std::string("partition-").length());
                int partition_id = std::stoi(partition_id_str);

                // Ensure partition vector is large enough
                if (partition_id >= static_cast<int>(topic.partitions.size()))
                {
                    topic.partitions.resize(partition_id + 1, Partition(-1));
                }

                topic.partitions[partition_id].id = partition_id;
                found_any_partition = true;

                // Read records from the partition log file
                TopicLog log(topic_name, partition_id, data_dir);
                std::vector<Record> records = log.read_all();

                for (const auto &record : records)
                {
                    topic.partitions[partition_id].messages.push_back(record.payload);
                    ++loaded_messages;
                }
            }

            if (found_any_partition)
            {
                topics_[topic_name] = std::move(topic);
                ++loaded_topics;
            }
        }

        std::cout << "recovered " << loaded_topics << " topics with " << loaded_messages << " total messages from disk.\n";
    }

    std::string Broker::handle_request(const Request &req)
    {
        switch (req.type)
        {
        case RequestType::PING:
            return "PONG";

        case RequestType::PRODUCE:
        {
            std::lock_guard<std::mutex> lock(topics_mutex_);

            // Get or create topic
            auto it = topics_.find(req.topic);
            if (it == topics_.end())
            {
                it = topics_.emplace(req.topic, Topic(req.topic)).first;
            }
            Topic &topic = it->second;

            // Validate partition
            if (req.partition < 0 || req.partition >= static_cast<int>(topic.partitions.size()))
            {
                return "ERROR";
            }

            // Append to in-memory partition
            topic.partitions[req.partition].messages.push_back(req.payload);

            // Persist to disk
            TopicLog log(req.topic, req.partition);
            log.append(Record{req.payload});

            return "OK";
        }

        case RequestType::FETCH:
        {
            std::lock_guard<std::mutex> lock(topics_mutex_);
            auto it = topics_.find(req.topic);

            if (it == topics_.end())
            {
                return "ERROR";
            }

            Topic &topic = it->second;

            // Validate partition
            if (req.partition < 0 || req.partition >= static_cast<int>(topic.partitions.size()))
            {
                return "ERROR";
            }

            const std::vector<std::string> &messages = topic.partitions[req.partition].messages;

            if (req.offset >= messages.size())
            {
                return "";
            }

            std::string response;
            for (size_t i = req.offset; i < messages.size(); ++i)
            {
                response += messages[i];
                if (i < messages.size() - 1)
                {
                    response += '\n';
                }
            }
            return response;
        }

        case RequestType::INVALID:
        default:
            return "ERROR";
        }
    }

} // namespace kafka
