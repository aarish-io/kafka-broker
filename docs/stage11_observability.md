# Stage 11 observability and benchmarking

This repository keeps the existing broker design intact while adding a lightweight observability layer for benchmarking analysis.

## Startup server mode

The broker CLI supports either of the following forms:

- `./build/kafka-broker <port> <data-directory> <leader|follower> [follower-port]`
- `./build/kafka-broker --epoll <port> <data-directory> <leader|follower> [follower-port]`

The default mode remains `TcpServer`, and `--epoll` switches to the alternative `EpollServer` implementation. Both modes share the same broker logic and storage model.

At startup the broker prints a clear banner:

- `server mode: tcp`
- `server mode: epoll`

## Metrics

The broker now records lightweight server-side metrics in memory. The counters are atomic and the latency samples are protected by a small mutex, which keeps the normal locking architecture unchanged while allowing thread-safe measurements.

The metrics include:

- total requests processed
- successful requests
- failed/error requests
- PRODUCE requests
- FETCH requests
- JOIN requests
- COMMIT requests
- total processed bytes
- request latency samples in microseconds

The latency measurement is the time spent inside server-side request handling, from the moment the broker begins processing a parsed request until it has produced the response string. This is intentionally not end-to-end network latency because it excludes the client socket round-trip and only measures the server-side processing interval.

## Metrics export

The broker exposes a snapshot object via `Broker::get_metrics_snapshot()`. Metrics can be serialized to CSV using the `Metrics::to_csv_header()` and `Metrics::to_csv_row()` helpers, and the live broker exposes the current row through:

```text
METRICS
```

The response has the same 13-column order used by `Metrics::to_csv_header()`:

```text
total_requests,successful_requests,failed_requests,produce_requests,fetch_requests,join_requests,commit_requests,total_bytes_processed,avg_latency_us,p50_latency_us,p95_latency_us,p99_latency_us,max_latency_us
```

## Benchmark workload

A simple workload driver is available in `benchmarks/benchmark_driver.py`.

Example usage:

```bash
python3 benchmarks/benchmark_driver.py \
  --mode tcp \
  --broker-port 9092 \
  --producers 1 \
  --messages 100 \
  --payload-bytes 128 \
  --topic benchmark \
  --partition 0 \
  --output benchmark-results/benchmark.csv
```

The script emits machine-readable CSV.

## CSV result location

Generated results should be stored under `benchmark-results/` so they stay out of the code tree. The script defaults to:

- `benchmark-results/benchmark.csv`

## Throughput formula

The benchmark reports:

- `msg_per_sec = completed_messages / elapsed_seconds`
- `bytes_per_sec = completed_bytes / elapsed_seconds`

The measured interval covers only the actual workload execution and intentionally does not include broker startup or recovery time unless the caller explicitly chooses to do so.

## Controlled TCP versus epoll experiment

The controlled experiment runner is `benchmarks/run_tcp_vs_epoll.py`. It compares the existing `TcpServer` and `EpollServer` implementations while varying concurrent producer count.

Each run uses:

- payload size: 1024 bytes
- messages per producer: 1000
- topic: `benchmark`
- partition: `0`
- producer counts: `1`, `2`, `4`, `8`, `16`
- repetitions: 3 per server mode and producer count

The default matrix contains 30 runs. TCP starts with the normal broker command; epoll starts with the existing `--epoll` option. Every run uses a fresh temporary broker data directory, waits for the broker socket to accept connections, and stores its raw result in the consolidated CSV:

```bash
python3 benchmarks/run_tcp_vs_epoll.py
```

Results are stored in `benchmark-results/tcp_vs_epoll_concurrency.csv`. The runner preserves each individual benchmark row and adds a `repetition` column; it does not average or interpret the measurements. For a small sanity run, use overrides such as:

```bash
python3 benchmarks/run_tcp_vs_epoll.py \
  --producers 1 \
  --messages 3 \
  --repetitions 1 \
  --output /tmp/tcp_vs_epoll_sanity.csv
```

The runner terminates the exact broker child process after each run, including failed and interrupted runs where practical. It reports the configuration of any failed run and does not write a substitute result row.

## Plotting support

The existing raw TCP versus epoll experiment can be analyzed without rerunning it:

```bash
python3 benchmarks/analyze_tcp_vs_epoll.py \
  benchmark-results/tcp_vs_epoll_concurrency.csv
```

The analysis reads the raw CSV only. It validates the expected two modes, five producer counts, and three repetitions per configuration. For each mode and producer count it calculates the arithmetic mean and sample standard deviation for:

- messages/sec
- bytes/sec
- p50 server-side latency in microseconds
- p95 server-side latency in microseconds
- p99 server-side latency in microseconds

The three repetitions are not weighted or recomputed from total durations; each run contributes one value to the corresponding mean. The aggregate summary is written to `benchmark-results/tcp_vs_epoll_analysis/tcp_vs_epoll_concurrency_summary.csv`. Four graphs are written alongside it:

- `throughput_vs_concurrency.png`
- `p50_latency_vs_concurrency.png`
- `p95_latency_vs_concurrency.png`
- `p99_latency_vs_concurrency.png`

## Existing measured results

Source files:

- Raw runs: `benchmark-results/tcp_vs_epoll_concurrency.csv`
- Aggregated summary: `benchmark-results/tcp_vs_epoll_analysis/tcp_vs_epoll_concurrency_summary.csv`

### Measured facts

The existing dataset contains 30 raw runs:

- modes: `tcp`, `epoll`
- producer counts: `1`, `2`, `4`, `8`, `16`
- repetitions: `3`
- messages per producer: `1000`
- payload size: `1024` bytes
- topic: `benchmark`
- partition: `0`

The aggregate summary currently records:

| Mode | Producers | Mean msg/sec | Stddev msg/sec | Mean p50 us | Mean p95 us | Mean p99 us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tcp | 1 | 2103.25 | 166.54 | 104.00 | 150.00 | 203.33 |
| tcp | 2 | 4258.43 | 139.93 | 108.67 | 209.33 | 291.33 |
| tcp | 4 | 3319.22 | 906.58 | 184.00 | 530.33 | 754.33 |
| tcp | 8 | 1523.82 | 192.75 | 126.00 | 307.33 | 472.33 |
| tcp | 16 | 1667.32 | 49.31 | 119.67 | 271.33 | 408.33 |
| epoll | 1 | 2523.09 | 327.67 | 99.67 | 160.33 | 260.00 |
| epoll | 2 | 5449.58 | 498.52 | 74.00 | 125.00 | 206.67 |
| epoll | 4 | 2739.81 | 470.50 | 95.67 | 198.00 | 298.33 |
| epoll | 8 | 1425.46 | 341.31 | 116.33 | 207.33 | 311.33 |
| epoll | 16 | 1329.21 | 129.57 | 110.67 | 220.33 | 321.33 |

### Interpretation

Within this dataset only:

- Mean epoll throughput is higher at producer counts 1 and 2.
- Mean TCP throughput is higher at producer counts 4, 8, and 16.
- Mean p50 latency is lower for epoll at all five measured producer counts.
- Mean p95 latency is lower for TCP at 1 producer and lower for epoll at 2, 4, 8, and 16 producers.
- Mean p99 latency is lower for TCP at 1 producer and lower for epoll at 2, 4, 8, and 16 producers.
- Repeatability varies by configuration; for example, TCP at 4 producers has a much larger throughput standard deviation than TCP at 16 producers in this dataset.

These observations do not establish a universal winner, and they do not explain why the measured differences occurred.

The earlier `benchmarks/plot_benchmarks.py` helper remains available for its existing CSV plotting behavior; the Stage 11.3.3 script is the reproducible analysis entry point for the consolidated concurrency experiment.

## Limitations

- Consumer lag benchmarking is intentionally minimal because the project does not add persistent consumer-offset storage or a redesigned consumer-group model.
- The benchmark driver is intentionally simple and is for controlled local validation rather than final performance conclusions.
- No final performance claims are made in this documentation; the next stage should run the actual experiments and compare the measured numbers.
