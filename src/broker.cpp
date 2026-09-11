#include "broker.hpp"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <sstream>
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

        case RequestType::JOIN:
            return join_group(req);

        case RequestType::LEAVE:
            return leave_group(req);

        case RequestType::GROUP_POLL:
            return group_poll(req);

        case RequestType::COMMIT:
            return commit_offset(req);

        case RequestType::INVALID:
        default:
            return "ERROR";
        }
    }

    std::string Broker::join_group(const Request &req)
    {
        std::lock_guard<std::mutex> lock(consumer_groups_mutex_);

        auto group_it = consumer_groups_.find(req.group_id);
        if (group_it == consumer_groups_.end())
        {
            ConsumerGroup group;
            group.group_id = req.group_id;
            group_it = consumer_groups_.emplace(req.group_id, std::move(group)).first;
        }

        ConsumerGroup &group = group_it->second;
        if (group.members.find(req.consumer_id) != group.members.end())
        {
            return "ERROR";
        }

        GroupMember member;
        member.consumer_id = req.consumer_id;
        member.topic = req.topic;
        group.members.emplace(req.consumer_id, std::move(member));
        rebalance_group(group);

        return "OK";
    }

    std::string Broker::leave_group(const Request &req)
    {
        std::lock_guard<std::mutex> lock(consumer_groups_mutex_);

        auto group_it = consumer_groups_.find(req.group_id);
        if (group_it == consumer_groups_.end())
        {
            return "ERROR";
        }

        std::size_t removed = group_it->second.members.erase(req.consumer_id);
        if (removed == 0)
        {
            return "ERROR";
        }

        if (group_it->second.members.empty())
        {
            consumer_groups_.erase(group_it);
        }
        else
        {
            rebalance_group(group_it->second);
        }

        return "OK";
    }

    void Broker::remove_consumer_from_group(const std::string &group_id, const std::string &consumer_id)
    {
        std::lock_guard<std::mutex> lock(consumer_groups_mutex_);

        auto group_it = consumer_groups_.find(group_id);
        if (group_it == consumer_groups_.end())
        {
            return;
        }

        group_it->second.members.erase(consumer_id);
        if (group_it->second.members.empty())
        {
            consumer_groups_.erase(group_it);
        }
        else
        {
            rebalance_group(group_it->second);
        }
    }

    std::string Broker::group_poll(const Request &req)
    {
        std::lock_guard<std::mutex> lock(consumer_groups_mutex_);

        auto group_it = consumer_groups_.find(req.group_id);
        if (group_it == consumer_groups_.end())
        {
            return "ERROR";
        }

        auto member_it = group_it->second.members.find(req.consumer_id);
        if (member_it == group_it->second.members.end())
        {
            return "ERROR";
        }

        const std::vector<TopicPartition> &assignments = member_it->second.assignments;
        std::ostringstream response;

        for (std::size_t i = 0; i < assignments.size(); ++i)
        {
            std::uint64_t committed_offset = 0;
            auto offset_it = group_it->second.committed_offsets.find(assignments[i]);
            if (offset_it != group_it->second.committed_offsets.end())
            {
                committed_offset = offset_it->second;
            }

            response << assignments[i].topic << ' ' << assignments[i].partition << ' ' << committed_offset;
            if (i + 1 < assignments.size())
            {
                response << '\n';
            }
        }

        return response.str();
    }

    std::string Broker::commit_offset(const Request &req)
    {
        std::lock_guard<std::mutex> group_lock(consumer_groups_mutex_);

        auto group_it = consumer_groups_.find(req.group_id);
        if (group_it == consumer_groups_.end())
        {
            return "ERROR";
        }

        if (group_it->second.members.find(req.consumer_id) == group_it->second.members.end())
        {
            return "ERROR";
        }

        {
            std::lock_guard<std::mutex> topics_lock(topics_mutex_);
            auto topic_it = topics_.find(req.topic);
            if (topic_it == topics_.end())
            {
                return "ERROR";
            }

            if (req.partition < 0 || req.partition >= static_cast<int>(topic_it->second.partitions.size()))
            {
                return "ERROR";
            }
        }

        // 6.4.1 The committed offset belongs to the group/topic/partition, not to the current consumer.
        group_it->second.committed_offsets[TopicPartition{req.topic, req.partition}] = req.offset;
        return "OK";
    }

    std::optional<std::uint64_t> Broker::get_committed_offset(const std::string &group_id,
                                                              const std::string &topic,
                                                              int partition)
    {
        std::lock_guard<std::mutex> lock(consumer_groups_mutex_);

        auto group_it = consumer_groups_.find(group_id);
        if (group_it == consumer_groups_.end())
        {
            return std::nullopt;
        }

        auto offset_it = group_it->second.committed_offsets.find(TopicPartition{topic, partition});
        if (offset_it == group_it->second.committed_offsets.end())
        {
            return std::nullopt;
        }

        return offset_it->second;
    }

    void Broker::rebalance_group(ConsumerGroup &group)
    {
        for (auto &entry : group.members)
        {
            entry.second.assignments.clear();
        }

        std::map<std::string, std::vector<std::string>> topic_to_consumers;
        for (const auto &entry : group.members)
        {
            topic_to_consumers[entry.second.topic].push_back(entry.first);
        }

        for (auto &topic_entry : topic_to_consumers)
        {
            std::vector<std::string> &consumer_ids = topic_entry.second;
            std::sort(consumer_ids.begin(), consumer_ids.end());

            // 6.3.1 Sorted consumer IDs make round-robin assignment deterministic.
            for (int partition = 0; partition < kDefaultPartitionCount; ++partition)
            {
                const std::string &consumer_id = consumer_ids[static_cast<std::size_t>(partition) % consumer_ids.size()];
                group.members[consumer_id].assignments.push_back(TopicPartition{topic_entry.first, partition});
            }
        }
    }

} // namespace kafka
