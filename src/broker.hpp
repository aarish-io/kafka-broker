#pragma once

#include "record.hpp"
#include "topic_log.hpp"
#include "protocol.hpp"

#include <cstdint>
#include <map>
#include <optional>
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

    struct TopicPartition
    {
        std::string topic;
        int partition = 0;

        bool operator<(const TopicPartition &other) const
        {
            if (topic != other.topic)
            {
                return topic < other.topic;
            }
            return partition < other.partition;
        }
    };

    struct GroupMember
    {
        std::string consumer_id;
        std::string topic;
        std::vector<TopicPartition> assignments;
    };

    struct ConsumerGroup
    {
        std::string group_id;
        std::unordered_map<std::string, GroupMember> members;

        // 6.1.1 Committed offsets are modeled now, but writes/persistence/recovery are Stage 6.2+ work.
        std::map<TopicPartition, std::uint64_t> committed_offsets;
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

        // Remove a group member when a client leaves or its connection closes.
        void remove_consumer_from_group(const std::string &group_id, const std::string &consumer_id);

        // Return the committed offset for a group/topic/partition, if one has been committed.
        std::optional<std::uint64_t> get_committed_offset(const std::string &group_id,
                                                          const std::string &topic,
                                                          int partition);

    private:
        std::string join_group(const Request &req);
        std::string leave_group(const Request &req);
        std::string group_poll(const Request &req);
        std::string commit_offset(const Request &req);
        void rebalance_group(ConsumerGroup &group);

        std::unordered_map<std::string, Topic> topics_;
        std::unordered_map<std::string, ConsumerGroup> consumer_groups_;
        std::mutex topics_mutex_;
        std::mutex consumer_groups_mutex_;
    };

} // namespace kafka
