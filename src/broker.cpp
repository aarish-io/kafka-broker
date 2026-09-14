#include "broker.hpp"
#include "client.hpp"

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

    void Broker::configure(BrokerRole role, int follower_port)
    {
        role_ = role;
        follower_port_ = follower_port;
    }

    void Broker::promote_to_leader()
    {
        role_ = BrokerRole::LEADER;
        follower_port_ = -1;
    }

    void Broker::recover_from_disk(const std::string &data_dir)
    {
        data_dir_ = data_dir;
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
                TopicLog log(topic_name, partition_id, data_dir_);
                std::vector<Record> records = log.read_all();

                for (const auto &record : records)
                {
                    topic.partitions[partition_id].messages.push_back(record.payload);
                    ++loaded_messages;
                }

                if (role_ == BrokerRole::FOLLOWER && !records.empty())
                {
                    replication_progress_[TopicPartition{topic_name, partition_id}] =
                        static_cast<std::uint64_t>(records.size() - 1);
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

        case RequestType::REPLICATION_PROGRESS:
        {
            if (role_ != BrokerRole::FOLLOWER)
            {
                return "ERROR";
            }

            std::optional<std::uint64_t> progress = get_replication_progress(req.topic, req.partition);
            return progress.has_value() ? std::to_string(*progress) : "NONE";
        }

        case RequestType::PRODUCE:
        {
            if (role_ == BrokerRole::LEADER && follower_port_ > 0)
            {
                synchronize_follower();
            }

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

            const std::uint64_t offset = topic.partitions[req.partition].messages.size();

            // Append to in-memory partition
            topic.partitions[req.partition].messages.push_back(req.payload);

            // Persist to disk
            TopicLog log(req.topic, req.partition, data_dir_);
            log.append(Record{req.payload});

            if (role_ == BrokerRole::LEADER && follower_port_ > 0)
            {
                try
                {
                    BrokerClient follower_client("127.0.0.1", follower_port_);
                    follower_client.connect();

                    std::ostringstream replication_request;
                    replication_request << "REPLICATE " << req.topic << ' '
                                        << req.partition << ' ' << offset << ' '
                                        << req.payload;
                    if (follower_client.request(replication_request.str()) != "OK")
                    {
                        return "ERROR";
                    }
                }
                catch (const std::exception &)
                {
                    return "ERROR";
                }
            }

            return "OK";
        }

        case RequestType::REPLICATE:
        {
            if (role_ != BrokerRole::FOLLOWER)
            {
                return "ERROR";
            }

            std::lock_guard<std::mutex> lock(topics_mutex_);

            auto it = topics_.find(req.topic);
            if (it == topics_.end())
            {
                it = topics_.emplace(req.topic, Topic(req.topic)).first;
            }
            Topic &topic = it->second;

            if (req.partition < 0 || req.partition >= static_cast<int>(topic.partitions.size()))
            {
                return "ERROR";
            }

            try
            {
                TopicLog log(req.topic, req.partition, data_dir_);
                log.append(Record{req.payload});
            }
            catch (const std::exception &)
            {
                return "ERROR";
            }

            replication_progress_[TopicPartition{req.topic, req.partition}] = req.offset;
            topic.partitions[req.partition].messages.push_back(req.payload);
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

    std::optional<std::uint64_t> Broker::get_replication_progress(const std::string &topic,
                                                                  int partition)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

        auto it = replication_progress_.find(TopicPartition{topic, partition});
        if (it == replication_progress_.end())
        {
            return std::nullopt;
        }

        return it->second;
    }

    std::vector<std::string> Broker::get_missing_records(const std::string &topic,
                                                         int partition,
                                                         std::optional<std::uint64_t> follower_progress)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

        auto topic_it = topics_.find(topic);
        if (topic_it == topics_.end() || partition < 0 ||
            partition >= static_cast<int>(topic_it->second.partitions.size()))
        {
            return {};
        }

        const std::vector<std::string> &messages = topic_it->second.partitions[partition].messages;
        if (messages.empty())
        {
            return {};
        }

        std::size_t first_missing = 0;
        if (follower_progress.has_value())
        {
            if (*follower_progress >= messages.size() - 1)
            {
                return {};
            }
            first_missing = static_cast<std::size_t>(*follower_progress + 1);
        }

        return std::vector<std::string>(messages.begin() + static_cast<std::ptrdiff_t>(first_missing),
                                        messages.end());
    }

    bool Broker::catch_up_follower(const std::string &topic,
                                   int partition,
                                   std::optional<std::uint64_t> follower_progress)
    {
        if (role_ != BrokerRole::LEADER)
        {
            return false;
        }

        std::vector<std::string> missing_records =
            get_missing_records(topic, partition, follower_progress);
        if (missing_records.empty())
        {
            return true;
        }

        if (follower_port_ <= 0)
        {
            return false;
        }

        const std::uint64_t first_missing_offset = follower_progress.has_value()
                                                       ? *follower_progress + 1
                                                       : 0;

        try
        {
            BrokerClient follower_client("127.0.0.1", follower_port_);
            follower_client.connect();

            for (std::size_t index = 0; index < missing_records.size(); ++index)
            {
                std::ostringstream replication_request;
                replication_request << "REPLICATE " << topic << ' ' << partition << ' '
                                    << first_missing_offset + index << ' ' << missing_records[index];
                if (follower_client.request(replication_request.str()) != "OK")
                {
                    return false;
                }
            }
        }
        catch (const std::exception &)
        {
            return false;
        }

        return true;
    }

    std::optional<std::uint64_t> Broker::query_follower_progress(const std::string &topic,
                                                                 int partition)
    {
        BrokerClient follower_client("127.0.0.1", follower_port_);
        follower_client.connect();

        std::ostringstream progress_request;
        progress_request << "REPLICATION_PROGRESS " << topic << ' ' << partition;
        std::string response = follower_client.request(progress_request.str());

        if (response == "NONE")
        {
            return std::nullopt;
        }

        std::size_t parsed_length = 0;
        std::uint64_t progress = std::stoull(response, &parsed_length);
        if (parsed_length != response.size())
        {
            throw std::runtime_error("invalid follower replication progress response");
        }

        return progress;
    }

    bool Broker::synchronize_follower()
    {
        if (role_ != BrokerRole::LEADER || follower_port_ <= 0)
        {
            return false;
        }

        std::vector<TopicPartition> partitions;
        {
            std::lock_guard<std::mutex> lock(topics_mutex_);
            for (const auto &topic_entry : topics_)
            {
                for (const Partition &partition : topic_entry.second.partitions)
                {
                    if (!partition.messages.empty())
                    {
                        partitions.push_back(TopicPartition{topic_entry.first, partition.id});
                    }
                }
            }
        }

        for (const TopicPartition &topic_partition : partitions)
        {
            try
            {
                std::optional<std::uint64_t> progress =
                    query_follower_progress(topic_partition.topic, topic_partition.partition);
                if (!catch_up_follower(topic_partition.topic, topic_partition.partition, progress))
                {
                    return false;
                }
            }
            catch (const std::exception &)
            {
                return false;
            }
        }

        return true;
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
