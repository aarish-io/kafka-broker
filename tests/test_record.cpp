#include "../src/record.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

void test_serialize_deserialize_basic()
{
    kafka::Record record{"hello world"};
    std::string serialized = kafka::serialize_record(record);

    // Verify 4-byte big-endian header length (11 bytes = 0x0000000B)
    assert(serialized.size() == 4 + 11);
    std::uint32_t net_len = 0;
    std::memcpy(&net_len, serialized.data(), 4);
    assert(ntohl(net_len) == 11);
    assert(serialized.substr(4) == "hello world");

    // Deserialize back
    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized, offset, deserialized);
    assert(ok);
    assert(offset == serialized.size());
    assert(deserialized.payload == "hello world");

    std::cout << "[PASS] test_serialize_deserialize_basic\n";
}

void test_serialize_empty_payload()
{
    kafka::Record record{""};
    std::string serialized = kafka::serialize_record(record);

    assert(serialized.size() == 4);
    std::uint32_t net_len = 0;
    std::memcpy(&net_len, serialized.data(), 4);
    assert(ntohl(net_len) == 0);

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized, offset, deserialized);
    assert(ok);
    assert(offset == 4);
    assert(deserialized.payload.empty());

    std::cout << "[PASS] test_serialize_empty_payload\n";
}

void test_deserialize_partial_buffer()
{
    kafka::Record record{"test payload"};
    std::string serialized = kafka::serialize_record(record);

    // Header incomplete (< 4 bytes)
    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized.substr(0, 2), offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    // Payload incomplete
    offset = 0;
    ok = kafka::deserialize_record(serialized.substr(0, 8), offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    std::cout << "[PASS] test_deserialize_partial_buffer\n";
}

void test_multiple_sequential_records()
{
    std::vector<std::string> payloads = {"msg1", "msg2 with spaces", "msg3!"};
    std::string log_stream;

    for (const auto &p : payloads)
    {
        log_stream += kafka::serialize_record(kafka::Record{p});
    }

    std::size_t offset = 0;
    std::vector<std::string> read_payloads;
    kafka::Record rec;
    std::size_t logical_offset = 0;

    while (kafka::deserialize_record(log_stream, offset, rec))
    {
        read_payloads.push_back(rec.payload);
        logical_offset++;
    }

    assert(offset == log_stream.size());
    assert(logical_offset == 3);
    assert(read_payloads == payloads);

    std::cout << "[PASS] test_multiple_sequential_records\n";
}

void test_log_path_generation()
{
    assert(kafka::get_log_path("orders") == "data/orders.log");
    assert(kafka::get_log_path("user-events") == "data/user-events.log");

    std::cout << "[PASS] test_log_path_generation\n";
}

int main()
{
    test_serialize_deserialize_basic();
    test_serialize_empty_payload();
    test_deserialize_partial_buffer();
    test_multiple_sequential_records();
    test_log_path_generation();

    std::cout << "All record serialization tests passed successfully!\n";
    return 0;
}