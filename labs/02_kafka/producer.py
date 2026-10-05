"""Lab 02 — simulated contact-centre event producer.

Each interaction (a call or chat) emits: interaction_started -> agent_joined -> message* -> interaction_ended.
Events are keyed by interaction_id, so every event of one interaction lands on the same partition (ordering).

Run (laptop, venv active, docker compose up):
    python labs/02_kafka/producer.py                      # 5 events/sec forever, Ctrl+C to stop
    python labs/02_kafka/producer.py --rate 50 --count 2000
    python labs/02_kafka/producer.py --poison 0.02        # 2% malformed messages (for the dead-letter part)
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "cx.interactions"
TENANTS = ["acme_bank", "globex_telco", "initech_insurance"]
MESSAGES = [
    "Hi, I can't log in to my account",
    "My card was charged twice, please call me on +91 98765 43210",
    "Can you update my email to priya.sharma@example.com?",
    "I want to cancel my subscription",
    "The bot didn't understand my question",
    "Thanks, that solved it!",
]


def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY FAILED key={msg.key()}: {err}")


def interaction_events():
    """Yield the events of one interaction in order."""
    iid = str(uuid.uuid4())
    tenant = random.choice(TENANTS)
    channel = random.choice(["voice", "chat", "chat", "email"])
    agent = f"agent_{random.randint(1, 25)}"
    base = {"interaction_id": iid, "tenant_id": tenant, "channel": channel}
    yield {**base, "event_type": "interaction_started"}
    yield {**base, "event_type": "agent_joined", "agent_id": agent}
    for _ in range(random.randint(1, 4)):
        yield {**base, "event_type": "message", "agent_id": agent,
               "speaker": random.choice(["customer", "agent", "bot"]),
               "text": random.choice(MESSAGES)}
    yield {**base, "event_type": "interaction_ended", "agent_id": agent,
           "handle_secs": random.randint(60, 1200), "escalated": random.random() < 0.1,
           "csat": random.choice([None, 1, 2, 3, 4, 5])}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, default=5, help="events per second")
    ap.add_argument("--count", type=int, default=0, help="stop after N events (0 = forever)")
    ap.add_argument("--poison", type=float, default=0.0, help="fraction of malformed messages")
    args = ap.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,     # no duplicates on retry; implies acks=all
        "acks": "all",
        "linger.ms": 20,                # small batching for throughput
        "compression.type": "lz4",
    })

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            for event in interaction_events():
                event["event_id"] = str(uuid.uuid4())
                event["ts"] = datetime.now(timezone.utc).isoformat()
                value = json.dumps(event)
                if random.random() < args.poison:
                    value = value[: len(value) // 2]          # truncated JSON = poison message
                producer.produce(TOPIC, key=event["interaction_id"], value=value,
                                 on_delivery=delivery_report)
                producer.poll(0)
                sent += 1
                if sent % 100 == 0:
                    print(f"sent {sent} events")
                time.sleep(1 / args.rate)
                if args.count and sent >= args.count:
                    break
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        print(f"done, sent {sent} events")


if __name__ == "__main__":
    main()
