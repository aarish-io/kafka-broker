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

## Plotting support

A very small CSV consumer lives in `benchmarks/plot_benchmarks.py`. It validates that the result file exists and prints the CSV content in a basic readable form. This keeps plotting support lightweight and avoids adding external plotting dependencies.

## Limitations

- Consumer lag benchmarking is intentionally minimal because the project does not add persistent consumer-offset storage or a redesigned consumer-group model.
- The benchmark driver is intentionally simple and is for controlled local validation rather than final performance conclusions.
- No final performance claims are made in this documentation; the next stage should run the actual experiments and compare the measured numbers.
