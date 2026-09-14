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
    assert(serialized.size() == 4 + 11 + 4);
    std::uint32_t net_len = 0;
    std::memcpy(&net_len, serialized.data(), 4);
    assert(ntohl(net_len) == 11);
    assert(serialized.substr(4, 11) == "hello world");

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

    assert(serialized.size() == 4 + 4);
    std::uint32_t net_len = 0;
    std::memcpy(&net_len, serialized.data(), 4);
    assert(ntohl(net_len) == 0);

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized, offset, deserialized);
    assert(ok);
    assert(offset == serialized.size());
    assert(deserialized.payload.empty());

    std::cout << "[PASS] test_serialize_empty_payload\n";
}

void test_crc32_known_vector()
{
    const std::string input = "123456789";

    assert(kafka::crc32(input.data(), input.size()) == 0xCBF43926u);

    std::cout << "[PASS] test_crc32_known_vector\n";
}

void test_deserialize_partial_header()
{
    kafka::Record record{"test payload"};
    std::string serialized = kafka::serialize_record(record);

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized.substr(0, 2), offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    std::cout << "[PASS] test_deserialize_partial_header\n";
}

void test_deserialize_partial_payload()
{
    kafka::Record record{"test payload"};
    std::string serialized = kafka::serialize_record(record);

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized.substr(0, 8), offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    std::cout << "[PASS] test_deserialize_partial_payload\n";
}

void test_deserialize_partial_crc()
{
    kafka::Record record{"test payload"};
    std::string serialized = kafka::serialize_record(record);

    serialized.resize(serialized.size() - 2);

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized, offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    std::cout << "[PASS] test_deserialize_partial_crc\n";
}

void test_deserialize_crc_corruption()
{
    kafka::Record record{"test payload"};
    std::string serialized = kafka::serialize_record(record);

    serialized[5] ^= 0x01;

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized, offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    std::cout << "[PASS] test_deserialize_crc_corruption\n";
}

void test_deserialize_length_corruption()
{
    kafka::Record record{"payload with multiple plausible lengths"};
    std::string serialized = kafka::serialize_record(record);

    std::uint32_t plausible_smaller_length = htonl(static_cast<std::uint32_t>(record.payload.size() - 1));
    std::memcpy(serialized.data(), &plausible_smaller_length, sizeof(plausible_smaller_length));

    std::size_t offset = 0;
    kafka::Record deserialized;
    bool ok = kafka::deserialize_record(serialized, offset, deserialized);
    assert(!ok);
    assert(offset == 0);

    std::cout << "[PASS] test_deserialize_length_corruption\n";
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
    assert(kafka::get_log_path("orders", 0) == "data/orders/partition-0.log");
    assert(kafka::get_log_path("user-events", 3) == "data/user-events/partition-3.log");

    std::cout << "[PASS] test_log_path_generation\n";
}

int main()
{
    test_serialize_deserialize_basic();
    test_serialize_empty_payload();
    test_crc32_known_vector();
    test_deserialize_partial_header();
    test_deserialize_partial_payload();
    test_deserialize_partial_crc();
    test_deserialize_crc_corruption();
    test_deserialize_length_corruption();
    test_multiple_sequential_records();
    test_log_path_generation();

    std::cout << "All record serialization tests passed successfully!\n";
    return 0;
}
