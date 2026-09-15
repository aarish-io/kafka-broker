#pragma once

#include "protocol.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace kafka
{
    struct MetricsSnapshot
    {
        std::uint64_t total_requests = 0;
        std::uint64_t successful_requests = 0;
        std::uint64_t failed_requests = 0;
        std::uint64_t produce_requests = 0;
        std::uint64_t fetch_requests = 0;
        std::uint64_t join_requests = 0;
        std::uint64_t commit_requests = 0;
        std::uint64_t total_bytes_processed = 0;
        std::uint64_t average_latency_us = 0;
        std::uint64_t p50_latency_us = 0;
        std::uint64_t p95_latency_us = 0;
        std::uint64_t p99_latency_us = 0;
        std::uint64_t max_latency_us = 0;
    };

    class Metrics
    {
    public:
        void record_request(RequestType type,
                            bool success,
                            std::uint64_t bytes_processed,
                            std::chrono::steady_clock::duration elapsed);

        MetricsSnapshot snapshot() const;

        std::string to_csv_header() const;
        std::string to_csv_row() const;

        void reset();

    private:
        static std::uint64_t percentile_value(const std::vector<std::uint64_t> &values,
                                              double percentile);

        std::atomic<std::uint64_t> total_requests_{0};
        std::atomic<std::uint64_t> successful_requests_{0};
        std::atomic<std::uint64_t> failed_requests_{0};
        std::atomic<std::uint64_t> produce_requests_{0};
        std::atomic<std::uint64_t> fetch_requests_{0};
        std::atomic<std::uint64_t> join_requests_{0};
        std::atomic<std::uint64_t> commit_requests_{0};
        std::atomic<std::uint64_t> total_bytes_processed_{0};

        mutable std::mutex latency_mutex_;
        std::vector<std::uint64_t> latency_samples_us_;
    };

} // namespace kafka
