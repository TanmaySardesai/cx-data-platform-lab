# Lab 04 — Iceberg (Wed Part C, ~45 min)

Run the statements in `iceberg_ops.sql` one at a time in `docker exec -it spark-iceberg spark-sql`.
Open the S3 file browser (http://localhost:8889/buckets/warehouse/) alongside: see `data/` (Parquet files) and `metadata/` (JSON + Avro manifests) grow.

What to understand (and say in the interview):
1. **Snapshots**: each commit writes new metadata pointing at a set of files. Readers see either the old or the new snapshot, never half — that's the ACID part.
2. **Time travel / rollback**: old snapshots stay readable until expired → audit and quick recovery.
3. **Schema evolution**: columns tracked by ID, so add/rename/drop doesn't rewrite data.
4. **Hidden partitioning**: users filter on `event_ts`, Iceberg prunes by `days(event_ts)` without anyone knowing the partition column. Partition spec can change over time.
5. **Small files**: streaming creates many; `rewrite_data_files` compacts them. Schedule it (e.g. Airflow, lab 05).
6. **Cost & governance**: one copy of data readable by Spark, Trino, Athena, Snowflake (Iceberg tables) → no duplicate copies; snapshot expiry controls storage cost.

Stretch: in the Snowflake lab, read about Snowflake's external Iceberg tables and note how it would read this same table from S3.
