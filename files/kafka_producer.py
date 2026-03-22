"""
producers/kafka_producer.py
Publishes generated events to Apache Kafka topic 'user-events'.

HOW TO RUN:
    pip install kafka-python
    python kafka_producer.py

REQUIRES:
    Kafka running on localhost:9092
    Topic 'user-events' created (or auto-create enabled)
"""

import json
import time
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.event_generator import generate_event, stream_events

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[WARNING] kafka-python not installed. Run: pip install kafka-python")


# ── Config ────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "user-events")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "10"))


def create_producer() -> "KafkaProducer":
    """
    Create and return a Kafka producer with JSON serialization.
    Retries connection up to 3 times.
    """
    for attempt in range(1, 4):
        try:
            producer = KafkaProducer(
                bootstrap_servers = KAFKA_BROKERS,
                value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
                key_serializer    = lambda k: k.encode("utf-8") if k else None,
                # Reliability settings
                acks              = "all",       # wait for all replicas
                retries           = 3,
                retry_backoff_ms  = 300,
                # Performance settings
                batch_size        = 16384,       # 16KB batch
                linger_ms         = 10,          # wait 10ms to fill batch
                compression_type  = "gzip",
            )
            print(f"[Producer] Connected to Kafka at {KAFKA_BROKERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[Producer] Attempt {attempt}/3 — Kafka not reachable at {KAFKA_BROKERS}")
            if attempt < 3:
                time.sleep(2)
    raise ConnectionError(f"Cannot connect to Kafka at {KAFKA_BROKERS}")


def on_send_success(record_metadata):
    """Callback when message is successfully sent."""
    pass  # quiet by default — uncomment for verbose mode
    # print(f"[Producer] Sent → topic={record_metadata.topic} "
    #       f"partition={record_metadata.partition} "
    #       f"offset={record_metadata.offset}")


def on_send_error(exc):
    """Callback when message fails to send."""
    print(f"[Producer] ERROR sending message: {exc}")


def run_producer(events_per_second: float = EVENTS_PER_SEC):
    """
    Main producer loop.
    Streams events to Kafka indefinitely.
    """
    if not KAFKA_AVAILABLE:
        print("[Producer] Simulating production (Kafka not installed)")
        print("[Producer] Events would be published to:", KAFKA_TOPIC)
        _simulate_producer(events_per_second)
        return

    producer = create_producer()

    print(f"[Producer] Publishing to topic '{KAFKA_TOPIC}' "
          f"at {events_per_second} events/sec")
    print("[Producer] Press Ctrl+C to stop\n")

    sent_count = 0
    error_count = 0
    start_time = time.time()

    try:
        interval = 1.0 / events_per_second
        while True:
            event = generate_event()

            # Use user_id as partition key — ensures same user
            # always goes to same partition (ordering guarantee)
            producer.send(
                KAFKA_TOPIC,
                key   = event["user_id"],
                value = event,
            ).add_callback(on_send_success).add_errback(on_send_error)

            sent_count += 1

            # Print stats every 500 events
            if sent_count % 500 == 0:
                elapsed = time.time() - start_time
                rate    = sent_count / elapsed
                print(f"[Producer] Sent: {sent_count:,} | "
                      f"Errors: {error_count} | "
                      f"Rate: {rate:.1f}/sec | "
                      f"Time: {elapsed:.0f}s")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[Producer] Stopping... flushing remaining messages")
    finally:
        producer.flush()   # wait for all messages to be sent
        producer.close()
        elapsed = time.time() - start_time
        print(f"[Producer] Done. Sent {sent_count:,} events in {elapsed:.1f}s")


def _simulate_producer(events_per_second: float):
    """Simulate producer output when Kafka is not available."""
    interval    = 1.0 / events_per_second
    count       = 0
    start_time  = time.time()

    print("[Producer] Simulating events (no Kafka):")
    try:
        while True:
            event = generate_event()
            count += 1
            if count % 20 == 0:
                elapsed = time.time() - start_time
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                      f"event #{count}: {event['event_type']} | "
                      f"user={event['user_id']} | "
                      f"page={event['page']}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n[Producer] Stopped after {count} events")


if __name__ == "__main__":
    run_producer()
