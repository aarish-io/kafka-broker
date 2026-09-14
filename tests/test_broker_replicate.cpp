#include "../src/broker.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>

void test_follower_persists_replicated_record()
{
    const std::string data_dir = "test_data_broker_replicate";
    std::filesystem::remove_all(data_dir);

    kafka::Broker broker;
    broker.configure(kafka::BrokerRole::FOLLOWER);
    broker.recover_from_disk(data_dir);

    kafka::Request request;
    request.type = kafka::RequestType::REPLICATE;
    request.topic = "orders";
    request.partition = 1;
    request.offset = 42;
    request.payload = "replicated order";

    assert(broker.handle_request(request) == "OK");

    kafka::TopicLog log("orders", 1, data_dir);
    std::vector<kafka::Record> records = log.read_all();
    assert(records.size() == 1);
    assert(records[0].payload == "replicated order");

    std::filesystem::remove_all(data_dir);
    std::cout << "[PASS] test_follower_persists_replicated_record\n";
}

void test_leader_rejects_replicated_record()
{
    const std::string data_dir = "test_data_broker_replicate_leader";
    std::filesystem::remove_all(data_dir);

    kafka::Broker broker;
    broker.configure(kafka::BrokerRole::LEADER);
    broker.recover_from_disk(data_dir);

    kafka::Request request;
    request.type = kafka::RequestType::REPLICATE;
    request.topic = "orders";
    request.partition = 0;
    request.payload = "should be rejected";

    assert(broker.handle_request(request) == "ERROR");
    assert(!std::filesystem::exists(data_dir + "/orders/partition-0.log"));

    std::filesystem::remove_all(data_dir);
    std::cout << "[PASS] test_leader_rejects_replicated_record\n";
}

int main()
{
    test_follower_persists_replicated_record();
    test_leader_rejects_replicated_record();
    std::cout << "All broker replicate tests passed.\n";
    return 0;
}