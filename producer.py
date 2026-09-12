"""
Synthetic workload generator for the adaptive resource management project.

Emits events to a Kafka topic at a variable rate: a steady baseline rate,
with periodic bursts to simulate real-world load spikes (e.g. a traffic
surge, a sensor event storm). This bursty pattern is what your static,
reactive, and predictive scaling strategies will be tested against.
"""

import json
import random
import time

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"

BASE_RATE_PER_SEC = 5      # normal/quiet-period message rate
BURST_MULTIPLIER = 8       # how much the rate spikes during a burst
CYCLE_SECONDS = 30         # length of one full quiet+burst cycle
BURST_DURATION_SECONDS = 5 # how long the burst lasts within each cycle


def current_rate(elapsed_seconds: float) -> float:
    """Return the target messages-per-second for this point in time."""
    position_in_cycle = elapsed_seconds % CYCLE_SECONDS
    if position_in_cycle < BURST_DURATION_SECONDS:
        return BASE_RATE_PER_SEC * BURST_MULTIPLIER
    return BASE_RATE_PER_SEC


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing to topic '{TOPIC}' at {BOOTSTRAP_SERVERS}. Ctrl+C to stop.")

    start_time = time.time()
    message_id = 0

    try:
        while True:
            elapsed = time.time() - start_time
            rate = current_rate(elapsed)
            interval = 1.0 / rate

            event = {
                "id": message_id,
                "produced_at": time.time(),
                "value": round(random.random(), 4),
            }
            producer.send(TOPIC, event)

            if message_id % 20 == 0:
                print(f"[{elapsed:6.1f}s] rate={rate:5.1f}/s  sent id={message_id}")

            message_id += 1
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
