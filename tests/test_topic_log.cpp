#include "../src/topic_log.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

void test_topic_log_append_and_readback()
{
    std::string test_dir = "test_data";
    std::filesystem::remove_all(test_dir);

    kafka::TopicLog log("orders", test_dir);
    assert(log.topic() == "orders");
    assert(log.log_path() == "test_data/orders.log");

    kafka::Record r1{"order-101"};
    kafka::Record r2{"order-102 payload with spaces"};
    kafka::Record r3{"order-103!"};

    log.append(r1);
    log.append(r2);
    log.append(r3);

    // Read log file directly from disk and deserialize
    std::ifstream file(log.log_path(), std::ios::binary);
    assert(file.is_open());

    std::string buffer((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
    file.close();

    std::size_t offset = 0;
    std::vector<std::string> read_payloads;
    kafka::Record read_rec;

    while (kafka::deserialize_record(buffer, offset, read_rec))
    {
        read_payloads.push_back(read_rec.payload);
    }

    assert(offset == buffer.size());
    assert(read_payloads.size() == 3);
    assert(read_payloads[0] == "order-101");
    assert(read_payloads[1] == "order-102 payload with spaces");
    assert(read_payloads[2] == "order-103!");

    // Test read_all() method directly
    std::vector<kafka::Record> records = log.read_all();
    assert(records.size() == 3);
    assert(records[0].payload == "order-101");
    assert(records[1].payload == "order-102 payload with spaces");
    assert(records[2].payload == "order-103!");

    std::filesystem::remove_all(test_dir);
    std::cout << "[PASS] test_topic_log_append_and_readback\n";
}

void test_topic_log_empty_topic_throws()
{
    try
    {
        kafka::TopicLog log("");
        assert(false && "Should have thrown std::invalid_argument");
    }
    catch (const std::invalid_argument &ex)
    {
        // Expected
    }

    std::cout << "[PASS] test_topic_log_empty_topic_throws\n";
}

int main()
{
    test_topic_log_append_and_readback();
    test_topic_log_empty_topic_throws();

    std::cout << "All TopicLog tests passed successfully!\n";
    return 0;
}