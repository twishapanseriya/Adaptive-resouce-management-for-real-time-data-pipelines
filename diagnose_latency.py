"""
Diagnose why the adaptive strategies show much higher latency than static.

Works on the per-message logs already written by worker.py (latency_logs/<run>/),
so no new experiments are needed.

Usage:
    python diagnose_latency.py static_1 reactive_1 predictive_1 ...
    python diagnose_latency.py reactive_1 --timeline        # latency per 10 s window

Options:
    --warmup SECONDS   exclude messages produced in the first SECONDS of a run (default 30)
    --gap SECONDS      system-wide pause threshold (default 1.0)
    --pgap SECONDS     per-partition stall threshold (default 5.0)
    --slow SECONDS     latency above which a message counts as "slow" (default 5.0)

What it measures (each maps to one explanation):

  1. Start-up cost      p95 with and without the first --warmup seconds.
                        If p95 barely changes, start-up is not the cause.

  2. Group-wide pauses  stretches where NO worker processed anything (> --gap s).
                        A rebalance that pauses the whole group would show up here.

  3. Partition stalls   stretches where one PARTITION was not processed for > --pgap s
                        while the other partitions kept moving. This is what happens if
                        a stopped worker's partitions wait for Kafka to notice the worker
                        is gone (the session timeout, often 10 s) before reassigning them.
                        Also reported: the share of slow messages that were waiting
                        through such a stall.

  4. Scale events       number of worker-count decreases/increases in the controller log
                        (results_<run>.csv, if present), to compare against the counts above.

Reading the results is explained in the printed summary table.
"""

import argparse
import csv
import glob
import math
import os
import statistics
from collections import defaultdict


def pct(sorted_vals, q):
    """Nearest-rank percentile."""
    if not sorted_vals:
        return float("nan")
    rank = max(1, math.ceil(q / 100 * len(sorted_vals)))
    return sorted_vals[rank - 1]


def load_run(run):
    files = glob.glob(os.path.join("latency_logs", run, "worker_*.csv"))
    if not files:
        return None
    rows = []
    for path in files:
        with open(path) as f:
            for r in csv.DictReader(f):
                rows.append(
                    (float(r["processed_at"]), float(r["produced_at"]),
                     int(r["partition"]), float(r["e2e_latency"]))
                )
    rows.sort()
    return rows  # (processed_at, produced_at, partition, e2e_latency)


def lat_summary(lats):
    s = sorted(lats)
    return {
        "n": len(s),
        "mean": statistics.mean(s) if s else float("nan"),
        "p50": pct(s, 50), "p95": pct(s, 95), "p99": pct(s, 99),
    }


def find_gaps(times, threshold):
    """times must be sorted. Returns [(gap_start, gap_end)] for gaps longer than threshold."""
    return [(a, b) for a, b in zip(times, times[1:]) if b - a > threshold]


def scale_events(run, pattern):
    path = pattern.format(run=run)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        workers = [int(r["num_workers"]) for r in csv.DictReader(f)]
    downs = sum(1 for a, b in zip(workers, workers[1:]) if b < a)
    ups = sum(1 for a, b in zip(workers, workers[1:]) if b > a)
    return ups, downs


def diagnose(run, args):
    rows = load_run(run)
    if rows is None:
        print(f"\n== {run}: no latency logs found in latency_logs/{run}/ -- skipped")
        return None

    t0 = min(r[1] for r in rows)  # first produced_at seen by any worker
    all_lat = [r[3] for r in rows]
    steady = [r for r in rows if r[1] >= t0 + args.warmup]
    steady_lat = [r[3] for r in steady]

    a = lat_summary(all_lat)
    b = lat_summary(steady_lat)

    # group-wide pauses
    all_times = sorted(r[0] for r in rows)
    sys_gaps = find_gaps(all_times, args.gap)
    sys_total = sum(e - s for s, e in sys_gaps)
    sys_longest = max((e - s for s, e in sys_gaps), default=0.0)

    # per-partition stalls
    by_part = defaultdict(list)
    for r in rows:
        by_part[r[2]].append(r)
    stalls = {}  # partition -> [(start, end)]
    stall_lengths = []
    for p, rs in by_part.items():
        times = sorted(x[0] for x in rs)
        g = find_gaps(times, args.pgap)
        stalls[p] = g
        stall_lengths += [e - s for s, e in g]

    # slow messages that were waiting through a stall
    slow = [r for r in rows if r[3] > args.slow]
    in_stall = 0
    for processed, produced, part, _ in slow:
        for s, e in stalls.get(part, []):
            if produced < e <= processed:
                in_stall += 1
                break
    share = (100.0 * in_stall / len(slow)) if slow else float("nan")

    ev = scale_events(run, args.csv_pattern)

    print(f"\n== {run}   ({a['n']} messages)")
    print(f"  all messages          mean {a['mean']:.2f}  p50 {a['p50']:.2f}  p95 {a['p95']:.2f}  p99 {a['p99']:.2f}")
    print(f"  excluding first {args.warmup:g}s  mean {b['mean']:.2f}  p50 {b['p50']:.2f}  p95 {b['p95']:.2f}  p99 {b['p99']:.2f}"
          f"   ({a['n'] - b['n']} msgs removed)")
    print(f"  group-wide pauses >{args.gap:g}s : {len(sys_gaps)}  (total {sys_total:.1f}s, longest {sys_longest:.1f}s)")
    if stall_lengths:
        lens = ", ".join(f"{x:.1f}" for x in sorted(stall_lengths, reverse=True)[:8])
        print(f"  partition stalls >{args.pgap:g}s: {len(stall_lengths)}  longest {max(stall_lengths):.1f}s  (top lengths: {lens})")
    else:
        print(f"  partition stalls >{args.pgap:g}s: 0")
    print(f"  slow msgs (>{args.slow:g}s): {len(slow)}   of which waiting through a stall: {share:.0f}%"
          if slow else f"  slow msgs (>{args.slow:g}s): 0")
    if ev is not None:
        print(f"  scale events in controller log: {ev[0]} up, {ev[1]} down")

    if args.timeline:
        print("  latency by 10 s window (by time produced, from run start):")
        bins = defaultdict(list)
        for _, produced, _, lat in rows:
            bins[int((produced - t0) // 10)].append(lat)
        for k in sorted(bins):
            v = bins[k]
            bar = "#" * min(60, int(statistics.mean(v) * 4))
            print(f"    {k*10:4d}-{k*10+10:<4d}s  n={len(v):4d}  mean {statistics.mean(v):6.2f}  {bar}")

    return {
        "run": run, "p95_all": a["p95"], "p95_steady": b["p95"],
        "sys_pauses": len(sys_gaps), "stalls": len(stall_lengths),
        "longest_stall": max(stall_lengths) if stall_lengths else 0.0,
        "share": share, "downs": ev[1] if ev else None, "ups": ev[0] if ev else None,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--warmup", type=float, default=30.0)
    ap.add_argument("--gap", type=float, default=1.0)
    ap.add_argument("--pgap", type=float, default=5.0)
    ap.add_argument("--slow", type=float, default=5.0)
    ap.add_argument("--timeline", action="store_true")
    ap.add_argument("--csv-pattern", default="results_{run}.csv",
                    help="controller CSV for each run (default results_{run}.csv)")
    args = ap.parse_args()

    results = [r for r in (diagnose(run, args) for run in args.runs) if r]
    if not results:
        return

    print("\n\nSUMMARY (paste this table back if you want help reading it)")
    head = f"{'run':<14}{'p95 all':>9}{'p95 -warm':>10}{'pauses':>8}{'stalls':>8}{'longest':>9}{'%slow in stall':>16}{'ups':>5}{'downs':>7}"
    print(head)
    print("-" * len(head))
    for r in results:
        share = "-" if math.isnan(r["share"]) else f"{r['share']:.0f}%"
        ups = "-" if r["ups"] is None else r["ups"]
        downs = "-" if r["downs"] is None else r["downs"]
        print(f"{r['run']:<14}{r['p95_all']:>9.2f}{r['p95_steady']:>10.2f}{r['sys_pauses']:>8}{r['stalls']:>8}"
              f"{r['longest_stall']:>8.1f}s{share:>16}{ups:>5}{downs:>7}")

    print("""
How to read it:
  - p95 -warm much lower than p95 all  -> start-up cost explains a lot (explanation 2)
  - 'pauses' high on adaptive runs, ~0 on static  -> group-wide rebalance pauses (explanation 1)
  - 'stalls' ~ 'downs', 'longest' near 10 s, and a high '%slow in stall'
        -> stopped workers' partitions waiting for Kafka to notice they are gone
  - none of the above         -> the delay is ordinary queueing during bursts (ramp-up too slow)
""")


if __name__ == "__main__":
    main()
