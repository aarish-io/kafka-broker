#!/usr/bin/env python3

import argparse
import csv
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Run a small broker workload benchmark and output CSV.")
    parser.add_argument("--broker-host", default="127.0.0.1")
    parser.add_argument("--broker-port", type=int, default=9092)
    parser.add_argument("--mode", choices=["tcp", "epoll"], default="tcp")
    parser.add_argument("--producers", type=int, default=1)
    parser.add_argument("--messages", type=int, default=100)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--topic", default="benchmark")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--output", default="benchmark-results/benchmark.csv")
    parser.add_argument("--broker-binary", default="./build/kafka-broker")
    parser.add_argument("--data-dir", default="benchmark-data")
    parser.add_argument("--wait-seconds", type=float, default=0.5)
    return parser.parse_args()


def ensure_output_dir(path: str):
    directory = Path(path).parent
    if directory and not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)


def send_message(host: str, port: int, topic: str, partition: int, payload: str):
    request = f"PRODUCE {topic} {partition} {payload}".encode("utf-8")

    response = send_request(host, port, request)
    if response != "OK":
        raise RuntimeError(f"Unexpected broker response: {response!r}")
    return response


def send_request(host: str, port: int, request: bytes):

    with socket.create_connection((host, port), timeout=5.0) as sock:
        sock.sendall(len(request).to_bytes(4, byteorder="big"))
        sock.sendall(request)

        header = sock.recv(4)
        if len(header) != 4:
            raise RuntimeError("short response header")

        response_length = int.from_bytes(header, byteorder="big")
        chunks = []
        remaining = response_length
        while remaining > 0:
            chunk = sock.recv(min(4096, remaining))
            if not chunk:
                raise RuntimeError("short response payload")
            chunks.append(chunk)
            remaining -= len(chunk)

        response = b"".join(chunks).decode("utf-8", "replace").strip()
        return response


def fetch_metrics(host: str, port: int):
    response = send_request(host, port, b"METRICS")
    values = response.split(",")
    if len(values) != 13:
        raise RuntimeError(f"Unexpected metrics response: {response!r}")
    return values


def run_producer(args, producer_index):
    payload = "x" * args.payload_bytes
    for _ in range(args.messages):
        send_message(args.broker_host, args.broker_port, args.topic, args.partition, payload)


def main():
    args = parse_args()
    ensure_output_dir(args.output)

    total_messages = args.producers * args.messages
    total_bytes = total_messages * args.payload_bytes

    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.producers) as executor:
        futures = [executor.submit(run_producer, args, index) for index in range(args.producers)]
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                raise RuntimeError(f"producer failed: {exc}") from exc
    elapsed = time.perf_counter() - start_time
    metrics_values = fetch_metrics(args.broker_host, args.broker_port)

    rows = [[
        args.mode,
        str(args.producers),
        str(args.messages),
        str(args.payload_bytes),
        str(total_messages),
        f"{elapsed:.6f}",
        f"{(total_messages / elapsed) if elapsed > 0 else 0.0:.6f}",
        f"{(total_bytes / elapsed) if elapsed > 0 else 0.0:.6f}",
        *metrics_values,
    ]]

    with open(args.output, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "mode",
            "producers",
            "messages",
            "payload_bytes",
            "total_messages",
            "total_seconds",
            "msg_per_sec",
            "bytes_per_sec",
            "total_requests",
            "successful_requests",
            "failed_requests",
            "produce_requests",
            "fetch_requests",
            "join_requests",
            "commit_requests",
            "total_bytes_processed",
            "avg_latency_us",
            "p50_latency_us",
            "p95_latency_us",
            "p99_latency_us",
            "max_latency_us",
        ])
        writer.writerows(rows)

    print(
        f"mode,{args.mode},producers,{args.producers},messages,{args.messages},"
        f"payload_bytes,{args.payload_bytes},total_messages,{total_messages},"
        f"total_seconds,{elapsed:.6f},msg_per_sec,{(total_messages / elapsed) if elapsed > 0 else 0.0:.6f},"
        f"bytes_per_sec,{(total_bytes / elapsed) if elapsed > 0 else 0.0:.6f}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"benchmark failed: {exc}", file=sys.stderr)
        sys.exit(1)
