"""Lab 02 — consumer with manual commits and a dead-letter topic.

Run several copies with the same --group to see partitions shared out (and rebalanced when one stops):
    python labs/02_kafka/consumer.py --group analytics
    python labs/02_kafka/consumer.py --group analytics --slow 0.5    # simulate a slow consumer -> watch lag grow
    python labs/02_kafka/consumer.py --group audit --from-beginning  # a second, independent group
"""
import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Producer

TOPIC = "cx.interactions"
DLQ_TOPIC = "cx.interactions.dlq"
REQUIRED = {"event_id", "interaction_id", "tenant_id", "event_type", "ts"}


def validate(raw: bytes) -> dict:
    event = json.loads(raw)                      # raises on malformed JSON
    missing = REQUIRED - event.keys()
    if missing:
        raise ValueError(f"missing fields: {sorted(missing)}")
    return event


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="analytics")
    ap.add_argument("--slow", type=float, default=0.0, help="seconds of fake work per message")
    ap.add_argument("--from-beginning", action="store_true")
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "enable.auto.commit": False,                 # we commit only after processing -> at-least-once
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "isolation.level": "read_committed",
    })
    dlq = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})

    def on_assign(c, partitions):
        print("ASSIGNED partitions:", [p.partition for p in partitions])

    def on_revoke(c, partitions):
        print("REVOKED partitions:", [p.partition for p in partitions])

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    counts, processed = Counter(), 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("ERROR:", msg.error())
                continue
            try:
                event = validate(msg.value())
                counts[(event["tenant_id"], event["event_type"])] += 1
                if args.slow:
                    time.sleep(args.slow)
            except (ValueError, json.JSONDecodeError) as exc:
                # poison message: park it with context, alert, keep going
                dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(), headers={
                    "error": str(exc), "source_partition": str(msg.partition()),
                    "source_offset": str(msg.offset())})
                dlq.poll(0)
                print(f"-> DLQ p{msg.partition()} o{msg.offset()}: {exc}")
            consumer.commit(message=msg, asynchronous=False)
            processed += 1
            if processed % 50 == 0:
                print(f"p{msg.partition()} o{msg.offset()} processed={processed} top={counts.most_common(3)}")
    except KeyboardInterrupt:
        pass
    finally:
        dlq.flush(5)
        consumer.close()        # leaves the group cleanly -> fast rebalance


if __name__ == "__main__":
    main()
