#include "../src/protocol.hpp"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

void test_parse_valid_replicate()
{
    kafka::Request req = kafka::parse_request("REPLICATE orders 1 42 payload with spaces");

    assert(req.type == kafka::RequestType::REPLICATE);
    assert(req.topic == "orders");
    assert(req.partition == 1);
    assert(req.offset == 42);
    assert(req.payload == "payload with spaces");

    std::cout << "[PASS] test_parse_valid_replicate\n";
}

void test_parse_replicate_allows_partition_above_default_count()
{
    kafka::Request req = kafka::parse_request("REPLICATE orders 10 0 copied record");

    assert(req.type == kafka::RequestType::REPLICATE);
    assert(req.partition == 10);
    assert(req.offset == 0);
    assert(req.payload == "copied record");

    std::cout << "[PASS] test_parse_replicate_allows_partition_above_default_count\n";
}

void test_parse_invalid_replicate()
{
    assert(kafka::parse_request("REPLICATE").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders 0").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders 0 1").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders -1 1 payload").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders abc 1 payload").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders 0 -1 payload").type == kafka::RequestType::INVALID);
    assert(kafka::parse_request("REPLICATE orders 0 abc payload").type == kafka::RequestType::INVALID);

    std::cout << "[PASS] test_parse_invalid_replicate\n";
}

void test_parse_existing_requests_still_work()
{
    assert(kafka::parse_request("PING").type == kafka::RequestType::PING);
    assert(kafka::parse_request("PRODUCE orders 0 hello world").type == kafka::RequestType::PRODUCE);
    assert(kafka::parse_request("FETCH orders 0 0").type == kafka::RequestType::FETCH);
    assert(kafka::parse_request("JOIN group-1 consumer-1 orders").type == kafka::RequestType::JOIN);
    assert(kafka::parse_request("LEAVE group-1 consumer-1").type == kafka::RequestType::LEAVE);
    assert(kafka::parse_request("GROUP_POLL group-1 consumer-1").type == kafka::RequestType::GROUP_POLL);
    assert(kafka::parse_request("COMMIT group-1 consumer-1 orders 0 5").type == kafka::RequestType::COMMIT);

    std::cout << "[PASS] test_parse_existing_requests_still_work\n";
}

int main()
{
    test_parse_valid_replicate();
    test_parse_replicate_allows_partition_above_default_count();
    test_parse_invalid_replicate();
    test_parse_existing_requests_still_work();

    std::cout << "All protocol parse tests passed.\n";
    return 0;
}
