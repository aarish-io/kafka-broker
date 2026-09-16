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

The broker exposes a snapshot object via `Broker::get_metrics_snapshot()`. It can be serialized to CSV using the `Metrics::to_csv_header()` and `Metrics::to_csv_row()` helpers.

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

The measured observations from the existing 30-row dataset are limited to this experiment. Mean epoll throughput is higher at producer counts 1 and 2, while mean TCP throughput is higher at 4, 8, and 16. Mean p50 latency is lower for epoll at all five measured producer counts. Mean p95 latency is lower for TCP at 1 producer and lower for epoll at 2, 4, 8, and 16 producers. Mean p99 latency is lower for TCP at 1 producer and lower for epoll at 2, 4, 8, and 16 producers. These observations do not establish a universal winner, and they do not explain why the measured differences occurred.

The earlier `benchmarks/plot_benchmarks.py` helper remains available for its existing CSV plotting behavior; the Stage 11.3.3 script is the reproducible analysis entry point for the consolidated concurrency experiment.

## Limitations

- Consumer lag benchmarking is intentionally minimal because the project does not add persistent consumer-offset storage or a redesigned consumer-group model.
- The benchmark driver is intentionally simple and is for controlled local validation rather than final performance conclusions.
- No final performance claims are made in this documentation; the next stage should run the actual experiments and compare the measured numbers.
