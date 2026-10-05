# Lab 02 — Kafka (Tue, ~2 h)

Topic `cx.interactions` (3 partitions by default). Kafka UI: http://localhost:8081

## Part A — topics, partitions, produce and consume (40 min)
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic cx.interactions --partitions 6 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cx.interactions
python labs/02_kafka/producer.py --rate 10
python labs/02_kafka/consumer.py --group analytics --from-beginning      # second terminal
```
- In Kafka UI open the topic → Messages. Confirm all events of one interaction_id sit on ONE partition.
- Explain to yourself: why key by interaction_id and not tenant_id? (hot keys: one big tenant would overload one partition.)

## Part B — consumer groups, rebalancing, lag (40 min)
1. Start 3 consumers in the same group → each gets 2 partitions (watch ASSIGNED lines).
2. Stop one (Ctrl+C) → watch REVOKED/ASSIGNED: the rebalance.
3. Start a 7th consumer on 6 partitions → one sits idle. (Why? max useful consumers = partitions.)
4. Run one consumer with `--slow 0.5` and the producer at `--rate 20` → watch lag grow in Kafka UI → Consumers.
   Or: `docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group analytics`
5. Fix the lag: add consumers. Then explain what you'd do when you're already at 1 consumer per partition.
6. Reset offsets to replay: `kafka-consumer-groups.sh ... --group analytics --topic cx.interactions --reset-offsets --to-earliest --execute` (consumers stopped first).

## Part C — reliability: exactly-once settings, dead-letter topic (40 min)
1. `python labs/02_kafka/producer.py --poison 0.05` → see `-> DLQ` lines; inspect `cx.interactions.dlq` in the UI (check the headers).
2. Read the producer config: `enable.idempotence`, `acks=all`. Write 3 lines in this README on what each protects against.
3. Consumer commits AFTER processing → at-least-once. Kill the consumer mid-run with `kill -9` and restart: some messages are processed twice. How would you make the downstream write idempotent? (upsert on event_id)
4. Stretch: use `producer.init_transactions()` / `begin_transaction()` / `send_offsets_to_transaction()` for a read-process-write exactly-once pipeline.

## Interview talk track
"I simulated contact-centre events keyed by interaction_id so per-interaction order is kept, ran consumer groups to see rebalancing and lag, and added a dead-letter topic with error headers so a single bad message never blocks a partition."
