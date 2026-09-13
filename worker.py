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


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",  # only process new messages, not old backlog
    )

    print(f"Worker started. Joining group '{GROUP_ID}' on topic '{TOPIC}'.")
    print("Watch for partition assignment messages below.")

    latencies = deque(maxlen=WINDOW_SIZE)
    processed_count = 0

    try:
        for message in consumer:
            event = message.value

            # Latency = time between when producer created the event and
            # now, when this worker is finally getting to process it.
            latency = time.time() - event["produced_at"]
            latencies.append(latency)

            # Simulate the actual processing work (CPU-bound task stand-in)
            time.sleep(PROCESSING_TIME_SECONDS)

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
        consumer.close()


if __name__ == "__main__":
    main()
