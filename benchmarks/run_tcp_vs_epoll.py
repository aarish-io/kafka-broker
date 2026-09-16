#!/usr/bin/env python3

import argparse
import csv
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DRIVER = ROOT / "benchmarks" / "benchmark_driver.py"
CSV_FIELDS = [
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
    "repetition",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the controlled TCP versus epoll concurrency experiment."
    )
    parser.add_argument(
        "--broker-binary",
        type=Path,
        default=ROOT / "build" / "kafka-broker",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "benchmark-results" / "tcp_vs_epoll_concurrency.csv",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=("tcp", "epoll"),
        default=["tcp", "epoll"],
    )
    parser.add_argument("--producers", nargs="+", type=int, default=[1, 2, 4, 8, 16])
    parser.add_argument("--messages", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--startup-timeout", type=float, default=10.0)
    return parser.parse_args()


def choose_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until_ready(process, host, port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def terminate_process(process):
    if process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def read_log_tail(log_path, limit=2000):
    try:
        content = log_path.read_text(errors="replace")
    except OSError:
        return ""
    return content[-limit:].strip()


def run_one(args, mode, producers, repetition, row_writer):
    port = choose_port()
    with tempfile.TemporaryDirectory(prefix=f"stage11_{mode}_{producers}_{repetition}_") as data_dir:
        data_path = Path(data_dir)
        run_output = data_path / "benchmark.csv"
        broker_log_path = data_path / "broker.log"
        broker_command = [
            str(args.broker_binary),
            *( ["--epoll"] if mode == "epoll" else [] ),
            str(port),
            str(data_path / "broker-data"),
            "follower",
        ]

        with broker_log_path.open("w") as broker_log:
            broker = subprocess.Popen(
                broker_command,
                cwd=ROOT,
                stdout=broker_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            benchmark = None
            try:
                if not wait_until_ready(broker, "127.0.0.1", port, args.startup_timeout):
                    broker_status = broker.poll()
                    log_tail = read_log_tail(broker_log_path)
                    raise RuntimeError(
                        f"broker did not become ready (return code: {broker_status})"
                        + (f"\n{log_tail}" if log_tail else "")
                    )

                benchmark_command = [
                    sys.executable,
                    str(DRIVER),
                    "--broker-host",
                    "127.0.0.1",
                    "--broker-port",
                    str(port),
                    "--mode",
                    mode,
                    "--producers",
                    str(producers),
                    "--messages",
                    str(args.messages),
                    "--payload-bytes",
                    str(args.payload_bytes),
                    "--topic",
                    "benchmark",
                    "--partition",
                    "0",
                    "--output",
                    str(run_output),
                ]
                benchmark = subprocess.Popen(
                    benchmark_command,
                    cwd=ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
                )
                stdout, stderr = benchmark.communicate()
                if benchmark.returncode != 0:
                    details = stderr.strip() or stdout.strip() or "no benchmark error output"
                    raise RuntimeError(
                        f"benchmark failed with return code {benchmark.returncode}: {details}"
                    )

                with run_output.open(newline="") as result_file:
                    rows = list(csv.DictReader(result_file))
                if len(rows) != 1:
                    raise RuntimeError(f"expected one benchmark row, found {len(rows)}")

                row = rows[0]
                missing_fields = [field for field in CSV_FIELDS[:-1] if field not in row]
                if missing_fields:
                    raise RuntimeError(f"benchmark CSV is missing fields: {', '.join(missing_fields)}")
                if row["mode"] != mode or int(row["producers"]) != producers:
                    raise RuntimeError("benchmark CSV metadata does not match the requested configuration")

                row["repetition"] = str(repetition)
                row_writer.writerow(row)
                return stdout.strip()
            except KeyboardInterrupt:
                if benchmark is not None:
                    terminate_process(benchmark)
                raise
            finally:
                terminate_process(broker)


def main():
    args = parse_args()
    if args.messages <= 0 or args.payload_bytes <= 0 or args.repetitions <= 0:
        raise ValueError("messages, payload-bytes, and repetitions must be positive")
    if any(producers <= 0 for producers in args.producers):
        raise ValueError("producer counts must be positive")
    if not args.broker_binary.is_file():
        raise FileNotFoundError(f"broker binary not found: {args.broker_binary}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    total_runs = len(args.modes) * len(args.producers) * args.repetitions
    print("Experiment: TCP vs Epoll concurrency")
    print(f"payload: {args.payload_bytes} bytes")
    print(f"messages/producer: {args.messages}")
    print(f"producers: {','.join(str(value) for value in args.producers)}")
    print(f"repetitions: {args.repetitions}")
    print(f"runs: {total_runs}")

    with args.output.open("w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        output_file.flush()

        run_number = 0
        try:
            for mode in args.modes:
                for producers in args.producers:
                    for repetition in range(1, args.repetitions + 1):
                        run_number += 1
                        print(
                            f"[{run_number}/{total_runs}] {mode} "
                            f"producers={producers} repetition={repetition}",
                            flush=True,
                        )
                        try:
                            run_one(args, mode, producers, repetition, writer)
                        except Exception as error:
                            print(
                                "Experiment failed: "
                                f"mode={mode} producers={producers} repetition={repetition}: {error}",
                                file=sys.stderr,
                            )
                            raise SystemExit(1) from error
                        output_file.flush()
        except KeyboardInterrupt:
            print("Experiment interrupted; completed rows remain in the output CSV.", file=sys.stderr)
            raise
        except Exception as error:
            print(f"Experiment failed at run [{run_number}/{total_runs}]: {error}", file=sys.stderr)
            raise SystemExit(1) from error

    print(f"Stored {total_runs} raw benchmark rows in {args.output}")


if __name__ == "__main__":
    main()
