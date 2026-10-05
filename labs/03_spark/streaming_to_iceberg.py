"""Lab 03 Part B — Structured Streaming: Kafka -> parse -> redact PII -> Iceberg (silver) + live aggregates.

Run (producer running on your laptop in another terminal):
    bash labs/03_spark/run_streaming.sh
Stop with Ctrl+C, restart, and confirm it resumes from the checkpoint (no duplicates, no gaps).
"""
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("lab03-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("interaction_id", T.StringType()),
    T.StructField("tenant_id", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("agent_id", T.StringType()),
    T.StructField("speaker", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("handle_secs", T.IntegerType()),
    T.StructField("escalated", T.BooleanType()),
    T.StructField("csat", T.IntegerType()),
    T.StructField("ts", T.StringType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "cx.interactions")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 5000)          # back-pressure: cap each micro-batch
       .load())

parsed = (raw.select(F.col("key").cast("string").alias("kafka_key"),
                     F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                     F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("kafka_partition", "kafka_offset", "e.*")
          .where(F.col("event_id").isNotNull())        # malformed JSON parses to nulls -> dropped here
          .withColumn("event_ts", F.to_timestamp("ts")))

PHONE = r"\+?\d[\d\s-]{8,}\d"
EMAIL = r"[\w.+-]+@[\w-]+\.[\w.]+"
silver = (parsed
          .withColumn("text", F.regexp_replace(F.regexp_replace("text", PHONE, "<PHONE>"), EMAIL, "<EMAIL>"))
          .withColumn("has_pii_redacted", F.col("text").contains("<PHONE>") | F.col("text").contains("<EMAIL>"))
          .drop("ts"))

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.cx")
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.cx.events_silver (
  kafka_partition INT, kafka_offset BIGINT, event_id STRING, interaction_id STRING, tenant_id STRING,
  channel STRING, event_type STRING, agent_id STRING, speaker STRING, text STRING,
  handle_secs INT, escalated BOOLEAN, csat INT, event_ts TIMESTAMP, has_pii_redacted BOOLEAN)
USING iceberg
PARTITIONED BY (days(event_ts), tenant_id)
""")

to_iceberg = (silver.writeStream.format("iceberg")
              .outputMode("append")
              .trigger(processingTime="30 seconds")
              .option("checkpointLocation", "/tmp/checkpoints/events_silver")
              .toTable("demo.cx.events_silver"))

# Live per-tenant counts in 1-minute windows; the watermark bounds late data and state size
per_minute = (silver.withWatermark("event_ts", "2 minutes")
              .groupBy(F.window("event_ts", "1 minute"), "tenant_id")
              .agg(F.count("*").alias("events"),
                   F.sum(F.col("escalated").cast("int")).alias("escalations")))

to_console = (per_minute.writeStream.format("console")
              .outputMode("update")
              .option("truncate", False)
              .trigger(processingTime="30 seconds")
              .option("checkpointLocation", "/tmp/checkpoints/per_minute")
              .start())

spark.streams.awaitAnyTermination()
