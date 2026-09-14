"""
Static allocation baseline (strategy #1).

This is the control group for your comparison: a FIXED number of workers,
started once and never adjusted, regardless of load. No scaling decision
happens here at all -- this file only exists so the static strategy logs
lag/worker-count data in the exact same format as the reactive and
predictive controllers, making the three directly comparable in your
report's graphs.
"""

import csv
import subprocess
import sys
import time

from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
GROUP_ID = "workers"

FIXED_WORKER_COUNT = 3   # matches MAX_WORKERS in the other two controllers,
                         # so all three strategies have access to the same
                         # ceiling -- static just never scales below it
POLL_INTERVAL_SECONDS = 5

LOG_FILE = "static_scaling_log.csv"


def get_total_lag(admin_client, meta_consumer, topic_partitions):
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

    workers = [spawn_worker() for _ in range(FIXED_WORKER_COUNT)]
    print(f"Static baseline started with a FIXED {len(workers)} worker(s). No scaling will occur.")

    with open(LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow(["elapsed_seconds", "total_lag", "num_workers"])

    start_time = time.time()

    try:
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)
            lag = get_total_lag(admin_client, meta_consumer, topic_partitions)
            elapsed = time.time() - start_time

            print(f"[{elapsed:6.1f}s] lag={lag}  workers={len(workers)} (fixed)")

            with open(LOG_FILE, "a", newline="") as f:
                csv.writer(f).writerow([f"{elapsed:.1f}", lag, len(workers)])

    except KeyboardInterrupt:
        print("\nShutting down static baseline and all workers...")
        for w in workers:
            w.terminate()


if __name__ == "__main__":
    main()
