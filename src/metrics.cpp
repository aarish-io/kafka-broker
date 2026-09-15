#include "metrics.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace kafka
{
    namespace
    {
        std::uint64_t to_us(std::chrono::steady_clock::duration elapsed)
        {
            return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
        }
    }

    void Metrics::record_request(RequestType type,
                                 bool success,
                                 std::uint64_t bytes_processed,
                                 std::chrono::steady_clock::duration elapsed)
    {
        const std::uint64_t elapsed_us = to_us(elapsed);

        total_requests_.fetch_add(1, std::memory_order_relaxed);
        if (success)
        {
            successful_requests_.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            failed_requests_.fetch_add(1, std::memory_order_relaxed);
        }

        switch (type)
        {
        case RequestType::PRODUCE:
            produce_requests_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RequestType::FETCH:
            fetch_requests_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RequestType::JOIN:
            join_requests_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RequestType::COMMIT:
            commit_requests_.fetch_add(1, std::memory_order_relaxed);
            break;
        default:
            break;
        }

        total_bytes_processed_.fetch_add(bytes_processed, std::memory_order_relaxed);

        std::lock_guard<std::mutex> lock(latency_mutex_);
        latency_samples_us_.push_back(elapsed_us);
    }

    std::uint64_t Metrics::percentile_value(const std::vector<std::uint64_t> &values, double percentile)
    {
        if (values.empty())
        {
            return 0;
        }

        std::vector<std::uint64_t> sorted = values;
        std::sort(sorted.begin(), sorted.end());

        if (sorted.size() == 1)
        {
            return sorted.front();
        }

        const double rank = percentile * (static_cast<double>(sorted.size()) - 1.0);
        const std::size_t lower_index = static_cast<std::size_t>(std::floor(rank));
        const std::size_t upper_index = static_cast<std::size_t>(std::ceil(rank));
        const double weight = rank - static_cast<double>(lower_index);

        const std::uint64_t lower_value = sorted[lower_index];
        const std::uint64_t upper_value = sorted[upper_index < sorted.size() ? upper_index : sorted.size() - 1];
        return static_cast<std::uint64_t>(lower_value + (upper_value - lower_value) * weight);
    }

    MetricsSnapshot Metrics::snapshot() const
    {
        MetricsSnapshot snapshot;
        snapshot.total_requests = total_requests_.load(std::memory_order_relaxed);
        snapshot.successful_requests = successful_requests_.load(std::memory_order_relaxed);
        snapshot.failed_requests = failed_requests_.load(std::memory_order_relaxed);
        snapshot.produce_requests = produce_requests_.load(std::memory_order_relaxed);
        snapshot.fetch_requests = fetch_requests_.load(std::memory_order_relaxed);
        snapshot.join_requests = join_requests_.load(std::memory_order_relaxed);
        snapshot.commit_requests = commit_requests_.load(std::memory_order_relaxed);
        snapshot.total_bytes_processed = total_bytes_processed_.load(std::memory_order_relaxed);

        std::vector<std::uint64_t> latencies;
        {
            std::lock_guard<std::mutex> lock(latency_mutex_);
            latencies = latency_samples_us_;
        }

        if (!latencies.empty())
        {
            snapshot.max_latency_us = *std::max_element(latencies.begin(), latencies.end());
            snapshot.average_latency_us = static_cast<std::uint64_t>(
                std::accumulate(latencies.begin(), latencies.end(), 0ULL) / static_cast<double>(latencies.size()));
            snapshot.p50_latency_us = percentile_value(latencies, 0.50);
            snapshot.p95_latency_us = percentile_value(latencies, 0.95);
            snapshot.p99_latency_us = percentile_value(latencies, 0.99);
        }

        return snapshot;
    }

    std::string Metrics::to_csv_header() const
    {
        return "total_requests,successful_requests,failed_requests,produce_requests,fetch_requests,join_requests,commit_requests,total_bytes_processed,avg_latency_us,p50_latency_us,p95_latency_us,p99_latency_us,max_latency_us";
    }

    std::string Metrics::to_csv_row() const
    {
        MetricsSnapshot snapshot = this->snapshot();
        return std::to_string(snapshot.total_requests) + "," +
               std::to_string(snapshot.successful_requests) + "," +
               std::to_string(snapshot.failed_requests) + "," +
               std::to_string(snapshot.produce_requests) + "," +
               std::to_string(snapshot.fetch_requests) + "," +
               std::to_string(snapshot.join_requests) + "," +
               std::to_string(snapshot.commit_requests) + "," +
               std::to_string(snapshot.total_bytes_processed) + "," +
               std::to_string(snapshot.average_latency_us) + "," +
               std::to_string(snapshot.p50_latency_us) + "," +
               std::to_string(snapshot.p95_latency_us) + "," +
               std::to_string(snapshot.p99_latency_us) + "," +
               std::to_string(snapshot.max_latency_us);
    }

    void Metrics::reset()
    {
        total_requests_.store(0, std::memory_order_relaxed);
        successful_requests_.store(0, std::memory_order_relaxed);
        failed_requests_.store(0, std::memory_order_relaxed);
        produce_requests_.store(0, std::memory_order_relaxed);
        fetch_requests_.store(0, std::memory_order_relaxed);
        join_requests_.store(0, std::memory_order_relaxed);
        commit_requests_.store(0, std::memory_order_relaxed);
        total_bytes_processed_.store(0, std::memory_order_relaxed);

        std::lock_guard<std::mutex> lock(latency_mutex_);
        latency_samples_us_.clear();
    }

} // namespace kafka
