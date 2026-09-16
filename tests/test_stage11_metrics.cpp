#include "broker.hpp"
#include "metrics.hpp"
#include "server_mode.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace
{
    void test_server_mode_parsing()
    {
        int argc = 2;
        char *argv[] = {const_cast<char *>("./broker"), const_cast<char *>("--epoll")};
        std::vector<std::string> remaining;
        kafka::ServerMode mode = kafka::parse_server_mode(argc, argv, remaining);
        assert(mode == kafka::ServerMode::EPOLL);
        assert(remaining.empty());

        argc = 2;
        char *argv_default[] = {const_cast<char *>("./broker"), const_cast<char *>("--unknown")};
        remaining.clear();
        mode = kafka::parse_server_mode(argc, argv_default, remaining);
        assert(mode == kafka::ServerMode::TCP);
        assert(remaining.size() == 1);
        assert(remaining[0] == "--unknown");

        std::cout << "[PASS] test_server_mode_parsing\n";
    }

    void test_metrics_recording()
    {
        kafka::Metrics metrics;
        metrics.record_request(kafka::RequestType::PRODUCE, true, 128, std::chrono::microseconds(250));
        metrics.record_request(kafka::RequestType::FETCH, false, 64, std::chrono::microseconds(1000));
        metrics.record_request(kafka::RequestType::JOIN, true, 16, std::chrono::microseconds(400));
        metrics.record_request(kafka::RequestType::COMMIT, true, 32, std::chrono::microseconds(500));

        kafka::MetricsSnapshot snapshot = metrics.snapshot();
        assert(snapshot.total_requests == 4);
        assert(snapshot.successful_requests == 3);
        assert(snapshot.failed_requests == 1);
        assert(snapshot.produce_requests == 1);
        assert(snapshot.fetch_requests == 1);
        assert(snapshot.join_requests == 1);
        assert(snapshot.commit_requests == 1);
        assert(snapshot.total_bytes_processed == 240);
        assert(snapshot.max_latency_us >= 1000);

        std::string csv = metrics.to_csv_row();
        assert(!csv.empty());
        assert(csv.find("total_requests") != std::string::npos || csv.find("4") != std::string::npos);

        std::cout << "[PASS] test_metrics_recording\n";
    }

    void test_metrics_request_snapshot_excludes_itself()
    {
        const std::filesystem::path data_dir = "test_data_stage11_metrics_request";
        std::filesystem::remove_all(data_dir);

        kafka::Broker broker;
        broker.configure(kafka::BrokerRole::LEADER);
        broker.recover_from_disk(data_dir.string());

        kafka::Request produce;
        produce.type = kafka::RequestType::PRODUCE;
        produce.topic = "metrics-test";
        produce.partition = 0;
        produce.payload = "payload";
        assert(broker.handle_request(produce) == "OK");

        kafka::Request metrics_request;
        metrics_request.type = kafka::RequestType::METRICS;
        const std::string response = broker.handle_request(metrics_request);

        std::istringstream fields(response);
        std::string field;
        std::vector<std::string> values;
        while (std::getline(fields, field, ','))
        {
            values.push_back(field);
        }

        assert(values.size() == 13);
        assert(values[0] == "1");
        assert(values[1] == "1");
        assert(values[2] == "0");
        assert(values[3] == "1");
        assert(values[7] == "51");

        std::filesystem::remove_all(data_dir);
        std::cout << "[PASS] test_metrics_request_snapshot_excludes_itself\n";
    }
}

int main()
{
    test_server_mode_parsing();
    test_metrics_recording();
    test_metrics_request_snapshot_excludes_itself();
    std::cout << "All Stage 11 metric tests passed.\n";
    return 0;
}
