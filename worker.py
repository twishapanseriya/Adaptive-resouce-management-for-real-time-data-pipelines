"""
Worker for the adaptive resource management project.

Each running instance of this script is one "worker" that joins the same
Kafka consumer group. Kafka automatically splits the topic's partitions
across however many workers are currently running and connected to that
group -- this is the exact mechanism your adaptive controller will exploit
later: starting/stopping instances of this script IS the scaling action.

For now (the static-allocation baseline), you just start a fixed number
of these manually in separate terminals and leave them running.
"""

import json
import os
import signal
import time
import statistics
from collections import deque

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
GROUP_ID = "workers"

# Simulated processing cost per message (seconds). Represents CPU work
# your real pipeline would do -- e.g. parsing, transforming, scoring.
PROCESSING_TIME_SECONDS = 0.08

# How many recent message latencies to keep for computing p95/p99
WINDOW_SIZE = 200

# How often (in processed messages) to print a stats summary
REPORT_EVERY = 50

# Every message's latency is also written to a per-worker CSV file, so
# p50/p95/p99 can be computed per strategy after a run. Set RUN_NAME in
# the environment to keep runs separate, for example:
#   env RUN_NAME=reactive_1 python controller_reactive.py
# Workers started by a controller inherit it. Use a NEW name for every run.
RUN_NAME = os.environ.get("RUN_NAME", "default")
LOG_DIR = os.path.join("latency_logs", RUN_NAME)


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",  # only process new messages, not old backlog
    )

    os.makedirs(LOG_DIR, exist_ok=True)
    # buffering=1 flushes every line: controllers stop workers with
    # terminate(), which would lose any buffered rows otherwise.
    log_file = open(
        os.path.join(LOG_DIR, f"worker_{os.getpid()}.csv"), "w", buffering=1
    )
    log_file.write("processed_at,produced_at,partition,queue_latency,e2e_latency\n")

    print(f"Worker started. Joining group '{GROUP_ID}' on topic '{TOPIC}'.")
    print("Watch for partition assignment messages below.")

    latencies = deque(maxlen=WINDOW_SIZE)
    processed_count = 0

    # Controllers stop workers with terminate(), which sends SIGTERM. Without
    # a handler the process dies instantly and never leaves its consumer
    # group, so Kafka only notices after its session timeout and that
    # worker's partitions sit unprocessed in the meantime. Turning SIGTERM
    # into the same exit path as Ctrl+C makes the worker call
    # consumer.close(), which leaves the group immediately.
    def _graceful_exit(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _graceful_exit)

    try:
        for message in consumer:
            event = message.value

            # Latency = time between when producer created the event and
            # now, when this worker is finally getting to process it.
            latency = time.time() - event["produced_at"]
            latencies.append(latency)

            # Simulate the actual processing work (CPU-bound task stand-in)
            time.sleep(PROCESSING_TIME_SECONDS)

            processed_at = time.time()
            # queue_latency: wait before processing started (what the
            # console stats already show). e2e_latency: includes the
            # processing time itself, i.e. the user-visible delay.
            log_file.write(
                f"{processed_at:.4f},{event['produced_at']:.4f},"
                f"{message.partition},{latency:.4f},"
                f"{processed_at - event['produced_at']:.4f}\n"
            )

            processed_count += 1

            if processed_count % REPORT_EVERY == 0:
                sorted_latencies = sorted(latencies)
                p50 = sorted_latencies[int(len(sorted_latencies) * 0.50)]
                p95 = sorted_latencies[int(len(sorted_latencies) * 0.95) - 1]
                avg = statistics.mean(sorted_latencies)
                print(
                    f"[partition {message.partition}] processed={processed_count}  "
                    f"avg_latency={avg:.3f}s  p50={p50:.3f}s  p95={p95:.3f}s"
                )

    except KeyboardInterrupt:
        print("\nStopping worker.")
    finally:
        log_file.close()
        consumer.close()


if __name__ == "__main__":
    main()
