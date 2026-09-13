"""
Reactive threshold-based scaling controller.

This is strategy #2 of your three-way comparison (static / reactive /
predictive). It replaces manually opening worker.py in multiple tabs:
instead, it measures how far behind the workers are (consumer lag) and
automatically spawns or kills worker processes based on a threshold --
the same idea Kubernetes' Horizontal Pod Autoscaler uses, just applied
directly to Kafka consumer lag instead of CPU percentage.

Run this INSTEAD of manually starting worker.py yourself. Keep the
producer running in its own tab as before.
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

MIN_WORKERS = 1
MAX_WORKERS = 3          # capped at partition count -- more workers than
                         # partitions would just sit idle
SCALE_UP_LAG_THRESHOLD = 40    # total pending messages that triggers scale-up
SCALE_DOWN_LAG_THRESHOLD = 5   # total pending messages below which we scale down
POLL_INTERVAL_SECONDS = 5

LOG_FILE = "reactive_scaling_log.csv"


def get_total_lag(admin_client, meta_consumer, topic_partitions):
    """Total pending messages across all partitions for this consumer group."""
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
    # No group_id here -- this consumer is only used to read topic metadata
    # and end offsets, so it must NOT join the "workers" group itself.
    meta_consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)

    partitions = meta_consumer.partitions_for_topic(TOPIC)
    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]

    workers = [spawn_worker() for _ in range(MIN_WORKERS)]
    print(f"Controller started with {len(workers)} worker(s).")

    with open(LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow(["elapsed_seconds", "total_lag", "num_workers"])

    start_time = time.time()

    try:
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)
            lag = get_total_lag(admin_client, meta_consumer, topic_partitions)
            elapsed = time.time() - start_time

            print(f"[{elapsed:6.1f}s] lag={lag}  workers={len(workers)}")

            with open(LOG_FILE, "a", newline="") as f:
                csv.writer(f).writerow([f"{elapsed:.1f}", lag, len(workers)])

            if lag > SCALE_UP_LAG_THRESHOLD and len(workers) < MAX_WORKERS:
                print("  -> Lag high: scaling UP (+1 worker)")
                workers.append(spawn_worker())

            elif lag < SCALE_DOWN_LAG_THRESHOLD and len(workers) > MIN_WORKERS:
                print("  -> Lag low: scaling DOWN (-1 worker)")
                worker_to_stop = workers.pop()
                worker_to_stop.terminate()

    except KeyboardInterrupt:
        print("\nShutting down controller and all workers...")
        for w in workers:
            w.terminate()


if __name__ == "__main__":
    main()
