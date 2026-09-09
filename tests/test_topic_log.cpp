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

    kafka::TopicLog log("orders", 0, test_dir);
    assert(log.topic() == "orders");
    assert(log.partition_id() == 0);
    assert(log.log_path() == "test_data/orders/partition-0.log");

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

void test_topic_log_invalid_constructor_args()
{
    try
    {
        kafka::TopicLog log("", 0);
        assert(false && "Should have thrown std::invalid_argument for empty topic");
    }
    catch (const std::invalid_argument &ex)
    {
        // Expected
    }

    try
    {
        kafka::TopicLog log("orders", -1);
        assert(false && "Should have thrown std::invalid_argument for negative partition_id");
    }
    catch (const std::invalid_argument &ex)
    {
        // Expected
    }

    std::cout << "[PASS] test_topic_log_invalid_constructor_args\n";
}

void test_topic_log_incomplete_final_record_truncation()
{
    std::string test_dir = "test_data_truncation";
    std::filesystem::remove_all(test_dir);

    kafka::TopicLog log("events", 1, test_dir);
    assert(log.log_path() == "test_data_truncation/events/partition-1.log");

    log.append(kafka::Record{"event-1"});
    log.append(kafka::Record{"event-2"});

    std::uintmax_t valid_file_size = std::filesystem::file_size(log.log_path());

    // Corrupt log by appending incomplete trailing bytes (simulating crash mid-write)
    std::ofstream file(log.log_path(), std::ios::binary | std::ios::app);
    file.write("PARTIAL", 7);
    file.close();

    assert(std::filesystem::file_size(log.log_path()) == valid_file_size + 7);

    // read_all() should recover the 2 complete records and truncate the 7 incomplete bytes
    std::vector<kafka::Record> records = log.read_all();
    assert(records.size() == 2);
    assert(records[0].payload == "event-1");
    assert(records[1].payload == "event-2");

    // Verify topic log file was truncated back to valid_file_size
    assert(std::filesystem::file_size(log.log_path()) == valid_file_size);

    std::filesystem::remove_all(test_dir);
    std::cout << "[PASS] test_topic_log_incomplete_final_record_truncation\n";
}

int main()
{
    test_topic_log_append_and_readback();
    test_topic_log_invalid_constructor_args();
    test_topic_log_incomplete_final_record_truncation();

    std::cout << "All TopicLog tests passed successfully!\n";
    return 0;
}