#include "broker.hpp"
#include "metrics.hpp"
#include "server_mode.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>
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
}

int main()
{
    test_server_mode_parsing();
    test_metrics_recording();
    std::cout << "All Stage 11 metric tests passed.\n";
    return 0;
}
