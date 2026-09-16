#!/usr/bin/env python3

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    raise SystemExit("matplotlib is required to generate graphs. Install it with: pip install matplotlib")


EXPECTED_MODES = ("tcp", "epoll")
EXPECTED_PRODUCERS = (1, 2, 4, 8, 16)
REQUIRED_COLUMNS = (
    "mode",
    "producers",
    "repetition",
    "msg_per_sec",
    "bytes_per_sec",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
)
SUMMARY_COLUMNS = [
    "mode",
    "producers",
    "repetitions",
    "mean_msg_per_sec",
    "stddev_msg_per_sec",
    "mean_bytes_per_sec",
    "stddev_bytes_per_sec",
    "mean_p50_latency_us",
    "stddev_p50_latency_us",
    "mean_p95_latency_us",
    "stddev_p95_latency_us",
    "mean_p99_latency_us",
    "stddev_p99_latency_us",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze existing TCP versus epoll benchmark CSV results."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default="benchmark-results/tcp_vs_epoll_concurrency.csv",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmark-results/tcp_vs_epoll_analysis",
    )
    return parser.parse_args()


def load_rows(csv_path: Path):
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {csv_path}")
        missing = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
        if missing:
            raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV is empty: {csv_path}")
    return rows


def group_rows(rows):
    groups = defaultdict(list)
    for row in rows:
        mode = row["mode"]
        producers = int(row["producers"])
        if mode not in EXPECTED_MODES:
            raise ValueError(f"unexpected mode: {mode}")
        if producers not in EXPECTED_PRODUCERS:
            raise ValueError(f"unexpected producer count: {producers}")
        groups[(mode, producers)].append(row)

    expected_keys = {(mode, producers) for mode in EXPECTED_MODES for producers in EXPECTED_PRODUCERS}
    if set(groups) != expected_keys:
        missing = sorted(expected_keys - set(groups))
        extra = sorted(set(groups) - expected_keys)
        details = []
        if missing:
            details.append(f"missing configurations: {missing}")
        if extra:
            details.append(f"unexpected configurations: {extra}")
        raise ValueError("; ".join(details))

    for key, entries in groups.items():
        repetitions = sorted(int(entry["repetition"]) for entry in entries)
        if repetitions != [1, 2, 3]:
            raise ValueError(f"{key} must contain repetitions 1, 2, and 3; found {repetitions}")
    return groups


def mean_and_stddev(entries, column):
    values = [float(entry[column]) for entry in entries]
    mean = statistics.mean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else 0.0
    return mean, stddev


def aggregate(groups):
    summary = []
    for mode in EXPECTED_MODES:
        for producers in EXPECTED_PRODUCERS:
            entries = groups[(mode, producers)]
            row = {
                "mode": mode,
                "producers": producers,
                "repetitions": len(entries),
            }
            for source, mean_name, stddev_name in (
                ("msg_per_sec", "mean_msg_per_sec", "stddev_msg_per_sec"),
                ("bytes_per_sec", "mean_bytes_per_sec", "stddev_bytes_per_sec"),
                ("p50_latency_us", "mean_p50_latency_us", "stddev_p50_latency_us"),
                ("p95_latency_us", "mean_p95_latency_us", "stddev_p95_latency_us"),
                ("p99_latency_us", "mean_p99_latency_us", "stddev_p99_latency_us"),
            ):
                mean, stddev = mean_and_stddev(entries, source)
                row[mean_name] = f"{mean:.6f}"
                row[stddev_name] = f"{stddev:.6f}"
            summary.append(row)
    return summary


def write_summary(summary, output_path: Path):
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(summary)


def plot_metric(summary, mean_column, ylabel, title, filename, output_dir):
    figure, axis = plt.subplots(figsize=(8, 5))
    for mode in EXPECTED_MODES:
        entries = [row for row in summary if row["mode"] == mode]
        axis.plot(
            [int(row["producers"]) for row in entries],
            [float(row[mean_column]) for row in entries],
            marker="o",
            label=mode,
        )
    axis.set_xlabel("producer concurrency")
    axis.set_ylabel(ylabel)
    axis.set_title(title)
    axis.set_xticks(EXPECTED_PRODUCERS)
    axis.grid(True, linestyle="--", alpha=0.5)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_dir / filename, dpi=180)
    plt.close(figure)


def main():
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"missing raw benchmark CSV: {csv_path}")

    rows = load_rows(csv_path)
    groups = group_rows(rows)
    summary = aggregate(groups)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_summary(summary, output_dir / "tcp_vs_epoll_concurrency_summary.csv")

    plot_metric(
        summary,
        "mean_msg_per_sec",
        "messages/sec",
        "mean throughput vs producer concurrency",
        "throughput_vs_concurrency.png",
        output_dir,
    )
    plot_metric(
        summary,
        "mean_p50_latency_us",
        "p50 latency (us)",
        "mean p50 latency vs producer concurrency",
        "p50_latency_vs_concurrency.png",
        output_dir,
    )
    plot_metric(
        summary,
        "mean_p95_latency_us",
        "p95 latency (us)",
        "mean p95 latency vs producer concurrency",
        "p95_latency_vs_concurrency.png",
        output_dir,
    )
    plot_metric(
        summary,
        "mean_p99_latency_us",
        "p99 latency (us)",
        "mean p99 latency vs producer concurrency",
        "p99_latency_vs_concurrency.png",
        output_dir,
    )

    print(f"analyzed {len(rows)} raw rows from {csv_path}")
    print(f"wrote aggregate summary and graphs to {output_dir}")
    print(output_dir / "tcp_vs_epoll_concurrency_summary.csv")
    for filename in (
        "throughput_vs_concurrency.png",
        "p50_latency_vs_concurrency.png",
        "p95_latency_vs_concurrency.png",
        "p99_latency_vs_concurrency.png",
    ):
        print(output_dir / filename)


if __name__ == "__main__":
    main()
