#!/usr/bin/env python3

import argparse
import csv
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    print("matplotlib is required to generate graphs. Install it with: pip install matplotlib", file=sys.stderr)
    raise SystemExit(1)


def load_rows(csv_path: Path):
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV file is empty: {csv_path}")

    return rows


def make_throughput_vs_concurrency(rows, output_dir: Path):
    modes = {}
    for row in rows:
        mode = row.get("mode")
        if not mode:
            continue
        modes.setdefault(mode, []).append(row)

    if not modes:
        return

    figure, axis = plt.subplots(figsize=(8, 5))
    for mode, entries in sorted(modes.items()):
        entries = sorted(entries, key=lambda entry: int(entry["producers"]))
        axis.plot([int(entry["producers"]) for entry in entries],
                  [float(entry["msg_per_sec"]) for entry in entries],
                  marker="o",
                  label=mode)

    axis.set_xlabel("producer concurrency")
    axis.set_ylabel("messages/sec")
    axis.set_title("throughput vs producer concurrency")
    axis.grid(True, linestyle="--", alpha=0.5)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_dir / "throughput_vs_concurrency.png", dpi=180)
    plt.close(figure)


def make_throughput_vs_payload(rows, output_dir: Path):
    modes = {}
    for row in rows:
        mode = row.get("mode")
        if not mode:
            continue
        modes.setdefault(mode, []).append(row)

    if not modes:
        return

    figure, axis = plt.subplots(figsize=(8, 5))
    for mode, entries in sorted(modes.items()):
        entries = sorted(entries, key=lambda entry: int(entry["payload_bytes"]))
        axis.plot([int(entry["payload_bytes"]) for entry in entries],
                  [float(entry["msg_per_sec"]) for entry in entries],
                  marker="o",
                  label=mode)

    axis.set_xlabel("payload bytes")
    axis.set_ylabel("messages/sec")
    axis.set_title("throughput vs payload size")
    axis.grid(True, linestyle="--", alpha=0.5)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_dir / "throughput_vs_payload.png", dpi=180)
    plt.close(figure)


def make_latency_vs_concurrency(rows, output_dir: Path):
    latency_columns = ["p50_latency_us", "p95_latency_us", "p99_latency_us"]
    if not all(column in rows[0] for column in latency_columns):
        return

    modes = {}
    for row in rows:
        mode = row.get("mode")
        if not mode:
            continue
        modes.setdefault(mode, []).append(row)

    if not modes:
        return

    figure, axis = plt.subplots(figsize=(8, 5))
    for mode, entries in sorted(modes.items()):
        entries = sorted(entries, key=lambda entry: int(entry["producers"]))
        for column, label in (("p50_latency_us", "p50"), ("p95_latency_us", "p95"), ("p99_latency_us", "p99")):
            axis.plot([int(entry["producers"]) for entry in entries],
                      [float(entry[column]) for entry in entries],
                      marker="o",
                      label=f"{mode} {label}")

    axis.set_xlabel("producer concurrency")
    axis.set_ylabel("latency (us)")
    axis.set_title("latency vs producer concurrency")
    axis.grid(True, linestyle="--", alpha=0.5)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_dir / "latency_vs_concurrency.png", dpi=180)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark graphs from CSV results.")
    parser.add_argument("csv_path")
    parser.add_argument("--output-dir", default="benchmark-results/graphs")
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"missing CSV result file: {csv_path}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(csv_path)
    make_throughput_vs_concurrency(rows, output_dir)
    make_throughput_vs_payload(rows, output_dir)
    make_latency_vs_concurrency(rows, output_dir)

    print(f"generated graphs in {output_dir}")
    for child in sorted(output_dir.iterdir()):
        if child.suffix == ".png":
            print(child.name)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"plotting failed: {exc}", file=sys.stderr)
        sys.exit(1)
