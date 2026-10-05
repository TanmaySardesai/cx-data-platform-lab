"""Copies one silver row, puts a raw email in it (event_id='bad-row'), and appends it -> the DQ task should fail.
Run:   docker exec -it spark-iceberg spark-submit /home/iceberg/labs/05_airflow/inject_bad_row.py
Undo:  docker exec -it spark-iceberg spark-sql -e "DELETE FROM demo.cx.events_silver WHERE event_id = 'bad-row'"
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("inject-bad-row").getOrCreate()
row = (spark.table("demo.cx.events_silver").limit(1)
       .withColumn("event_id", F.lit("bad-row"))
       .withColumn("text", F.lit("please email me at a.b@example.com"))
       .withColumn("event_ts", F.current_timestamp()))
row.writeTo("demo.cx.events_silver").append()
print("inserted bad row")
