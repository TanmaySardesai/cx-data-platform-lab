# Lab 03 — Spark (Wed Parts A and B, ~1.5 h)

## Part A — batch: joins, skew, plans (45 min)
```bash
docker exec -it spark-iceberg spark-submit /home/iceberg/labs/03_spark/batch_skew.py
```
While it runs, open http://localhost:4040 → Stages. In the skewed join, one task runs far longer than the rest (the "acme_bank" partition). Compare the 4 timings it prints.

Write down in your own words:
1. Why the broadcast join was fastest, and when it would be a bad idea (small side not small).
2. What AQE changed at runtime (look at the SQL tab: "skew" / coalesced partitions).
3. Why `coalesce(4)` before writing gave 4-ish files, and when you'd use `repartition` instead.

## Part B — Structured Streaming from Kafka (45 min)
Terminal 1: `python labs/02_kafka/producer.py --rate 20 --poison 0.02`
Terminal 2: `bash labs/03_spark/run_streaming.sh`

Then:
1. Every 30 s the console prints per-tenant counts per minute. Note how late rows update earlier windows until the watermark passes.
2. Query the silver table while it streams:
   `docker exec -it spark-iceberg spark-sql -e "SELECT tenant_id, count(*), sum(int(has_pii_redacted)) FROM demo.cx.events_silver GROUP BY 1"`
   and check that no phone number or email appears: `... -e "SELECT text FROM demo.cx.events_silver WHERE text LIKE '%@%' OR text LIKE '%+91%' LIMIT 5"` (should return nothing)
3. Ctrl+C the stream, restart it → it resumes from the checkpoint. Explain: offsets in the checkpoint + atomic Iceberg commit = no duplicates.
4. Raise `--rate 200` and watch `maxOffsetsPerTrigger` cap each batch (Spark UI → Structured Streaming tab: input rate vs processing rate).

## Talk track
"Spark Structured Streaming reads Kafka, drops malformed events, redacts phone numbers and emails before anything is stored, and writes to an Iceberg table partitioned by day and tenant. Checkpoints plus Iceberg's atomic commits give effectively exactly-once results, and the watermark bounds state for the windowed aggregates."
