# Lab 06 — Snowflake (Thu Parts B and C, ~60 min)

Open Snowsight → Projects → Worksheets → paste `snowflake_lab.sql`. Run one block at a time (Ctrl/Cmd+Enter runs the statement under the cursor).

## Part B — load and CDC
Generator-based load, then Streams + Tasks with an incremental MERGE. After each step, open **Query Profile** (Query details → Profile) at least once: learn to read it, it's a common interview follow-up.

## Part C — governance, performance, cost
Masking policy on phone numbers, pruning and clustering, time travel and cloning, query history and credit usage, resource monitor.

## Be ready to explain
- Storage vs compute separation; scale **up** (bigger warehouse, faster heavy queries) vs **out** (multi-cluster, more concurrent users).
- Why auto-suspend 60 s, separate warehouses per workload, and resource monitors.
- Streams = change tracking offsets; Tasks = scheduled SQL; when you'd use Dynamic Tables instead.
- Micro-partitions and pruning; when clustering is worth the credits.
- How Snowflake fits with the Iceberg lake from lab 04 (Snowflake-managed or external Iceberg tables over S3).

**Cost safety**: the warehouse is XSMALL, auto-suspends after 60 s, and the resource monitor stops it at 5 credits. Trial credits cover this lab many times over.
