#pragma once

#include "record.hpp"
#include "topic_log.hpp"
#include "protocol.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace kafka
{
    // Partition holds messages for a single partition ID.
    struct Partition
    {
        int id;
        std::vector<std::string> messages;

        explicit Partition(int partition_id) : id(partition_id) {}
    };

    // Topic holds multiple partitions.
    struct Topic
    {
        std::string name;
        std::vector<Partition> partitions;

        Topic() = default;

        explicit Topic(const std::string &topic_name);
    };

    // Broker manages topics, partitions, and handles produce/fetch operations.
    class Broker
    {
    public:
        static constexpr int kDefaultPartitionCount = 3;

        Broker() = default;

        // Recover topics from disk on startup.
        void recover_from_disk(const std::string &data_dir = "data");

        // Handle a parsed request and return the response.
        std::string handle_request(const Request &req);

    private:
        std::unordered_map<std::string, Topic> topics_;
        std::mutex topics_mutex_;
    };

} // namespace kafka
