"""Lab 03 Part A — Spark batch: joins, skew, query plans, file counts.

Run inside the Spark container:
    docker exec -it spark-iceberg spark-submit /home/iceberg/labs/03_spark/batch_skew.py
Open http://localhost:4040 while it runs (Jobs, Stages -> look at task duration spread, SQL tab -> plans).
"""
import time

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab03-batch-skew").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 5M calls, 90% of them from ONE tenant -> classic skew
calls = (spark.range(5_000_000)
         .withColumn("tenant_id", F.when(F.rand(1) < 0.9, F.lit("acme_bank"))
                     .otherwise(F.concat(F.lit("tenant_"), (F.rand(2) * 500).cast("int"))))
         .withColumn("handle_secs", (F.rand(3) * 1200).cast("int"))
         .withColumn("call_date", F.date_add(F.lit("2026-09-01"), (F.rand(4) * 30).cast("int"))))

tenants = (calls.select("tenant_id").distinct()
           .withColumn("region", F.when(F.col("tenant_id") == "acme_bank", "IN").otherwise("EU")))


def timed(label, df):
    t = time.time()
    n = df.count()
    print(f"{label:<45} rows={n:<10} {time.time() - t:6.1f}s")


# 1) Plain shuffle join with AQE OFF -> one giant task for acme_bank
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
skewed = calls.join(tenants, "tenant_id")
timed("shuffle join, AQE off (skewed)", skewed)
skewed.explain()   # look for SortMergeJoin + Exchange hashpartitioning(tenant_id)

# 2) Broadcast join: the small side is copied to every executor, no shuffle of the big side
bcast = calls.join(F.broadcast(tenants), "tenant_id")
timed("broadcast join", bcast)
bcast.explain()    # look for BroadcastHashJoin

# 3) AQE on: Spark splits skewed partitions at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
timed("shuffle join, AQE skew handling on", calls.join(tenants, "tenant_id"))

# 4) Salting by hand (what you do when AQE isn't enough, e.g. skewed aggregations)
SALT = 16
salted_calls = calls.withColumn("salt", (F.rand(5) * SALT).cast("int"))
salted_tenants = tenants.crossJoin(spark.range(SALT).withColumnRenamed("id", "salt"))
timed("salted join", salted_calls.join(salted_tenants, ["tenant_id", "salt"]))

# 5) Output file counts: repartition vs coalesce
agg = calls.groupBy("tenant_id", "call_date").agg(F.count("*").alias("calls"),
                                                 F.avg("handle_secs").alias("avg_handle_secs"))
print("partitions after groupBy:", agg.rdd.getNumPartitions())
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.cx")
agg.coalesce(4).writeTo("demo.cx.daily_tenant_stats").createOrReplace()
print(spark.sql("SELECT count(*) AS data_files FROM demo.cx.daily_tenant_stats.files").collect())

spark.stop()
