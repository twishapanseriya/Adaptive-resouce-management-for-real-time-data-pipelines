"""
Report the lag at every scale-down event in one or more controller CSVs.

Usage:
    python3 check_scaling.py results_predictive_1.csv results_predictive_2.csv ...

A worker count that drops between row i and row i+1 means the controller
released a worker at poll i, so the lag in row i is the lag it saw when it
decided. This shows which version of a controller produced a log:
  - fixed predictive:   every scale-down happened at lag < 25
  - reactive (v2):      every scale-down at lag < 25, and some at 5 or above
  - reactive (old, 5):  scale-downs only at lag < 5, or none at all
"""

import csv
import sys

for path in sys.argv[1:]:
    with open(path) as f:
        rows = list(csv.DictReader(f))
    downs = [
        (float(a["elapsed_seconds"]), int(a["total_lag"]))
        for a, b in zip(rows, rows[1:])
        if int(b["num_workers"]) < int(a["num_workers"])
    ]
    lags = [lag for _, lag in downs]
    print(f"{path}: {len(downs)} scale-down event(s)")
    if lags:
        print(f"   lag at scale-down: min={min(lags)}  max={max(lags)}")
        print(f"   at lag >= 25: {sum(l >= 25 for l in lags)}   at lag >= 5: {sum(l >= 5 for l in lags)}")
