"""
producers/event_generator.py
Simulates a realistic live stream of user events.
Generates random events that mimic real web/app traffic patterns.
"""

import uuid
import random
import time
from datetime import datetime, timezone


# ── Configuration ─────────────────────────────────────────────
EVENT_TYPES = [
    "page_view",      # 50% — most common
    "click",          # 20%
    "search",         # 15%
    "purchase",       # 10%
    "add_to_cart",    #  5%
]

EVENT_WEIGHTS = [50, 20, 15, 10, 5]

PAGES = [
    "/home", "/search", "/product/shoes",
    "/product/laptop", "/cart", "/checkout",
    "/profile", "/orders", "/category/electronics",
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [55, 35, 10]

# Simulate ~1000 active users
USER_POOL = [f"u_{random.randint(10000, 99999)}" for _ in range(1000)]


def generate_event() -> dict:
    """
    Generate one realistic user event.
    Returns a dict matching the schema.
    """
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    page       = random.choice(PAGES)
    device     = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
    user_id    = random.choice(USER_POOL)

    event = {
        "event_id":   str(uuid.uuid4()),
        "user_id":    user_id,
        "event_type": event_type,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "page":       page,
        "session_id": f"s_{random.randint(10000, 99999)}",
        "device":     device,
        "amount":     None,
    }

    # Only purchase events have amount
    if event_type == "purchase":
        event["amount"] = round(random.uniform(9.99, 999.99), 2)

    return event


def generate_burst(n: int = 100) -> list:
    """Generate a burst of N events (simulates traffic spike)."""
    return [generate_event() for _ in range(n)]


def stream_events(events_per_second: float = 10.0, burst_every: int = 30):
    """
    Infinite generator that yields events at the given rate.
    Occasionally generates traffic bursts to test anomaly detection.

    Args:
        events_per_second: Normal event rate
        burst_every: Trigger a burst every N seconds
    """
    interval   = 1.0 / events_per_second
    start_time = time.time()

    print(f"[Generator] Starting event stream at {events_per_second} events/sec")
    print(f"[Generator] Bursts every {burst_every} seconds")

    event_count = 0
    while True:
        elapsed = time.time() - start_time

        # Simulate periodic traffic bursts
        if int(elapsed) % burst_every == 0 and int(elapsed) > 0:
            burst = generate_burst(n=50)
            for e in burst:
                yield e
                event_count += 1
        else:
            yield generate_event()
            event_count += 1

        if event_count % 100 == 0:
            print(f"[Generator] {event_count} events generated | "
                  f"elapsed: {elapsed:.1f}s")

        time.sleep(interval)


if __name__ == "__main__":
    # Quick test — generate 5 events and print them
    import json
    print("Sample events:\n")
    for i in range(5):
        event = generate_event()
        print(json.dumps(event, indent=2))
        print()
