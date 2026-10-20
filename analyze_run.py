"""
Summarize one experiment run.

Combines the per-message latency logs written by worker.py with the
controller's lag/worker-count CSV to produce the metrics promised in the
literature review: p50/p95/p99 latency, throughput, a resource-cost
metric (worker-seconds) and lag statistics.

Usage:
    python analyze_run.py <run_name> <controller_csv> [--trim SECONDS]

Example:
    python analyze_run.py reactive_1 reactive_scaling_log.csv --trim 240

<run_name> must match the RUN_NAME the run was started with, e.g.
    env RUN_NAME=reactive_1 python controller_reactive.py

--trim ignores controller rows after that many seconds, so every strategy
is judged over the same window (the last rows of a run can be teardown
noise once the producer has been stopped).

Each call also appends one line to run_summary.csv, so repeated runs of
each strategy can be averaged later.
"""

import argparse
import csv
import glob
import math
import os
import statistics

SUMMARY_FILE = "run_summary.csv"


def percentile(sorted_values, q):
    """Nearest-rank percentile: the smallest value with at least q% of the data at or below it."""
    rank = max(1, math.ceil(q / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def load_latencies(run_name):
    files = glob.glob(os.path.join("latency_logs", run_name, "worker_*.csv"))
    if not files:
        raise SystemExit(f"No latency files found in latency_logs/{run_name}/")
    e2e, queue, processed_at = [], [], []
    for path in files:
        with open(path) as f:
            for row in csv.DictReader(f):
                e2e.append(float(row["e2e_latency"]))
                queue.append(float(row["queue_latency"]))
                processed_at.append(float(row["processed_at"]))
    return files, e2e, queue, processed_at


def load_controller(path, trim):
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if trim is not None:
        rows = [r for r in rows if float(r["elapsed_seconds"]) <= trim]
    if len(rows) < 2:
        raise SystemExit("Not enough controller rows after trimming.")
    elapsed = [float(r["elapsed_seconds"]) for r in rows]
    lag = [int(r["total_lag"]) for r in rows]
    workers = [int(r["num_workers"]) for r in rows]
    interval = statistics.median(b - a for a, b in zip(elapsed, elapsed[1:]))
    return elapsed, lag, workers, interval


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_name")
    parser.add_argument("controller_csv")
    parser.add_argument("--trim", type=float, default=None)
    args = parser.parse_args()

    files, e2e, queue, processed_at = load_latencies(args.run_name)
    elapsed, lag, workers, interval = load_controller(args.controller_csv, args.trim)

    e2e_sorted = sorted(e2e)
    n = len(e2e_sorted)
    span = max(processed_at) - min(processed_at)
    throughput = n / span if span > 0 else float("nan")

    summary = {
        "run": args.run_name,
        "messages": n,
        "e2e_mean_s": round(statistics.mean(e2e_sorted), 3),
        "e2e_p50_s": round(percentile(e2e_sorted, 50), 3),
        "e2e_p95_s": round(percentile(e2e_sorted, 95), 3),
        "e2e_p99_s": round(percentile(e2e_sorted, 99), 3),
        "e2e_max_s": round(e2e_sorted[-1], 3),
        "throughput_msg_per_s": round(throughput, 2),
        "avg_workers": round(statistics.mean(workers), 2),
        "worker_seconds": round(sum(workers) * interval, 1),
        "avg_lag": round(statistics.mean(lag), 1),
        "max_lag": max(lag),
    }

    print(f"\nRun: {args.run_name}   ({len(files)} worker log file(s), {n} messages)")
    print(f"Controller window: {elapsed[0]:.1f}s to {elapsed[-1]:.1f}s, poll interval ~{interval:.1f}s")
    for key, value in summary.items():
        if key != "run":
            print(f"  {key:<22} {value}")

    new_file = not os.path.exists(SUMMARY_FILE)
    with open(SUMMARY_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        if new_file:
            writer.writeheader()
        writer.writerow(summary)
    print(f"\nAppended to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
