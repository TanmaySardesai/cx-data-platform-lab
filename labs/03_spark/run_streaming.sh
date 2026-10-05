#!/usr/bin/env bash
# Finds the container's Spark + Scala versions and adds the matching Kafka connector.
set -euo pipefail
JAR=$(docker exec spark-iceberg bash -c 'ls /opt/spark/jars | grep -E "^spark-core_2\.1[0-9]-" | head -1')
SCALA=$(echo "$JAR" | sed -E 's/spark-core_(2\.1[0-9])-.*/\1/')
SPARK=$(echo "$JAR" | sed -E 's/spark-core_2\.1[0-9]-(.*)\.jar/\1/')
echo "Spark $SPARK, Scala $SCALA"
docker exec -it spark-iceberg spark-submit \
  --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA}:${SPARK}" \
  /home/iceberg/labs/03_spark/streaming_to_iceberg.py
