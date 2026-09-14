"""
Lightweight predictive scaling controller (strategy #3).

This directly tests RQ2 from your literature review: can a lightweight,
statistically-driven forecast (a simple moving average -- no LSTM, no RL,
no training data) anticipate load changes better than purely reactive
threshold scaling?

Key difference from controller_reactive.py:
  - Reactive: watches LAG (how much backlog has already built up), and
    only scales once that backlog crosses a threshold. It waits for the
    problem to already be happening.
  - Predictive: watches the ARRIVAL RATE (how fast messages are coming
    in right now), forecasts the near-future rate as a moving average of
    recent observations, and sizes the worker pool for that forecast
    directly -- ideally scaling up just as a burst starts, rather than
    after a backlog has already formed.

PER_WORKER_CAPACITY below is a simplification: it's estimated from
worker.py's known simulated processing time, rather than learned from
live measurements. A production system would measure this empirically;
here it's fixed so the comparison stays simple and reproducible.
"""

import csv
import math
import subprocess
import sys
import time
from collections import deque

from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
GROUP_ID = "workers"

MIN_WORKERS = 1
MAX_WORKERS = 3

POLL_INTERVAL_SECONDS = 5
RATE_HISTORY_WINDOW = 2   # how many recent rate samples to average for the forecast
                          # (kept small so short bursts aren't smoothed away)

# ~12.5 msgs/sec, based on worker.py's PROCESSING_TIME_SECONDS = 0.08
PER_WORKER_CAPACITY = 1 / 0.08

LOG_FILE = "predictive_scaling_log.csv"


def get_total_produced(meta_consumer, topic_partitions):
    """Total messages ever produced across all partitions (sum of end offsets)."""
    end_offsets = meta_consumer.end_offsets(topic_partitions)
    return sum(end_offsets.values())


def get_total_lag(admin_client, meta_consumer, topic_partitions):
    """Only used for logging/comparison -- the predictive strategy does not
    use lag to make its scaling decision, unlike the reactive controller."""
    end_offsets = meta_consumer.end_offsets(topic_partitions)
    committed = admin_client.list_consumer_group_offsets(GROUP_ID)
    total_lag = 0
    for tp in topic_partitions:
        end_offset = end_offsets.get(tp, 0)
        committed_entry = committed.get(tp)
        committed_offset = committed_entry.offset if committed_entry else 0
        total_lag += max(end_offset - committed_offset, 0)
    return total_lag


def spawn_worker():
    return subprocess.Popen([sys.executable, "worker.py"])


def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    meta_consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)

    partitions = meta_consumer.partitions_for_topic(TOPIC)
    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]

    workers = [spawn_worker() for _ in range(MIN_WORKERS)]
    print(f"Predictive controller started with {len(workers)} worker(s).")

    with open(LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow(
            ["elapsed_seconds", "observed_rate", "forecast_rate", "total_lag", "num_workers"]
        )

    rate_history = deque(maxlen=RATE_HISTORY_WINDOW)
    last_total_produced = get_total_produced(meta_consumer, topic_partitions)
    start_time = time.time()

    try:
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)

            total_produced = get_total_produced(meta_consumer, topic_partitions)
            observed_rate = (total_produced - last_total_produced) / POLL_INTERVAL_SECONDS
            last_total_produced = total_produced
            rate_history.append(observed_rate)

            # The "predictive" step: forecast next window's rate as the
            # moving average of recent observed rates.
            forecast_rate = sum(rate_history) / len(rate_history)

            lag = get_total_lag(admin_client, meta_consumer, topic_partitions)
            elapsed = time.time() - start_time

            required_workers = max(
                MIN_WORKERS,
                min(MAX_WORKERS, math.ceil(forecast_rate / PER_WORKER_CAPACITY)),
            )

            print(
                f"[{elapsed:6.1f}s] observed={observed_rate:5.1f}/s  "
                f"forecast={forecast_rate:5.1f}/s  lag={lag}  "
                f"workers={len(workers)} -> target={required_workers}"
            )

            with open(LOG_FILE, "a", newline="") as f:
                csv.writer(f).writerow(
                    [f"{elapsed:.1f}", f"{observed_rate:.1f}", f"{forecast_rate:.1f}", lag, len(workers)]
                )

            while len(workers) < required_workers:
                print("  -> Forecast rising: scaling UP (+1 worker)")
                workers.append(spawn_worker())

            while len(workers) > required_workers:
                print("  -> Forecast falling: scaling DOWN (-1 worker)")
                workers.pop().terminate()

    except KeyboardInterrupt:
        print("\nShutting down controller and all workers...")
        for w in workers:
            w.terminate()


if __name__ == "__main__":
    main()
