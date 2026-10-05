-- Lab 04 — Iceberg operations. Needs demo.cx.events_silver from lab 03 Part B (let the stream run a few minutes first).
-- Run step by step in:  docker exec -it spark-iceberg spark-sql
-- (or all at once:      docker exec -it spark-iceberg spark-sql -f /home/iceberg/labs/04_iceberg/iceberg_ops.sql)

-- 1. Metadata tables: every commit is a snapshot
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM demo.cx.events_silver.snapshots ORDER BY committed_at;

SELECT partition, record_count, file_count FROM demo.cx.events_silver.partitions;
SELECT count(*) AS data_files, round(avg(file_size_in_bytes) / 1024) AS avg_kb FROM demo.cx.events_silver.files;
-- Streaming every 30 s => many SMALL files. Note the avg size; you'll fix it in step 5.

-- 2. Time travel (replace the id with the FIRST snapshot_id from step 1)
-- SELECT count(*) FROM demo.cx.events_silver VERSION AS OF <snapshot_id>;
SELECT count(*) AS rows_now FROM demo.cx.events_silver;

-- 3. Schema evolution: no rewrite, old files just read the new column as NULL
ALTER TABLE demo.cx.events_silver ADD COLUMN sentiment_score DOUBLE;
ALTER TABLE demo.cx.events_silver RENAME COLUMN csat TO csat_score;
DESCRIBE TABLE demo.cx.events_silver;
-- !! Rename breaks the streaming job's column name: stop the stream before this step, or rename it back:
ALTER TABLE demo.cx.events_silver RENAME COLUMN csat_score TO csat;

-- 4. Partition evolution: new data uses the new spec, old data keeps the old one
ALTER TABLE demo.cx.events_silver ADD PARTITION FIELD channel;
SELECT spec_id, count(*) FROM demo.cx.events_silver.files GROUP BY spec_id;

-- 5. Maintenance: compact small files, expire old snapshots, clean orphans
CALL demo.system.rewrite_data_files(table => 'cx.events_silver',
       options => map('target-file-size-bytes', '134217728', 'min-input-files', '2'));
SELECT count(*) AS data_files_after FROM demo.cx.events_silver.files;

CALL demo.system.expire_snapshots(table => 'cx.events_silver',
       older_than => current_timestamp(), retain_last => 5);
CALL demo.system.remove_orphan_files(table => 'cx.events_silver');

-- 6. Row-level change (Iceberg v2 merge-on-read / copy-on-write): GDPR-style delete for one interaction
-- DELETE FROM demo.cx.events_silver WHERE interaction_id = '<pick one>';

-- 7. Rollback: undo a bad write
-- CALL demo.system.rollback_to_snapshot('cx.events_silver', <snapshot_id>);
