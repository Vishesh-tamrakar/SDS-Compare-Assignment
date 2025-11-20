import sys
import time
import uuid
import json
import random
from kafka import KafkaProducer

# -------------------------------------------------------------
#  helper: produce one YSB-style event with correct timestamps
# -------------------------------------------------------------
def generate_event(producer_id):
    producer_timestamp_ns = time.time_ns()  # high precision
    event_time_ms = producer_timestamp_ns // 1_000_000  # OPTION B3

    return {
        "producer_id": str(producer_id),
        "user_id": str(uuid.uuid4()),
        "page_id": str(uuid.uuid4()),
        "ad_id": str(uuid.uuid4()),
        "ad_type": random.choice([
            "banner", "modal", "sponsored-search", "mail", "mobile"
        ]),
        "event_type": random.choice(["view", "click", "purchase"]),
        "event_time_ms": event_time_ms,
        "producer_timestamp_ns": producer_timestamp_ns,
        "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}"
    }


# -------------------------------------------------------------
#  SPP mode — steady Poisson process
# -------------------------------------------------------------
def run_spp(producer, producer_id, throughput, seconds):
    events_sent = 0
    lam = throughput

    for _ in range(seconds):
        start = time.time()
        for _ in range(throughput):
            event = generate_event(producer_id)
            producer.send("ad_events_topic", value=event)
            events_sent += 1
        # Maintain ~1 second pacing
        elapsed = time.time() - start
        if elapsed < 1:
            time.sleep(1 - elapsed)

    return events_sent


# -------------------------------------------------------------
#  BURSTY mode — deterministic high/low switching
# -------------------------------------------------------------
def run_bursty(producer, producer_id, throughput, seconds):
    events_sent = 0
    half = max(1, seconds // 2)

    # HIGH rate
    for _ in range(half):
        for _ in range(throughput * 2):
            event = generate_event(producer_id)
            producer.send("ad_events_topic", value=event)
            events_sent += 1
        time.sleep(1)

    # LOW rate
    for _ in range(seconds - half):
        for _ in range(max(1, throughput // 4)):
            event = generate_event(producer_id)
            producer.send("ad_events_topic", value=event)
            events_sent += 1
        time.sleep(1)

    return events_sent


# -------------------------------------------------------------
#  MAIN
# -------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python producer.py <producer_id> <mode> <throughput> <seconds>")
        print("Modes available: spp, burst")
        sys.exit(1)

    producer_id = sys.argv[1]
    mode = sys.argv[2].lower()
    throughput = int(sys.argv[3])
    seconds = int(sys.argv[4])

    producer = KafkaProducer(
        bootstrap_servers=["localhost:29092"],   # Windows external port
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=32 * 1024,
    )

    print(f"Producer {producer_id} starting: mode={mode}, throughput={throughput}, seconds={seconds}")

    if mode == "spp":
        events_sent = run_spp(producer, producer_id, throughput, seconds)
    elif mode == "burst":
        events_sent = run_bursty(producer, producer_id, throughput, seconds)
    else:
        print("Invalid mode. Use: spp OR burst")
        sys.exit(1)

    producer.flush()
    print(f"Producer {producer_id} finished. Sent {events_sent} events.")
