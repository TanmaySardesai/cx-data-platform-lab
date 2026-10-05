-- Lab 06 — Snowflake (Thu Parts B and C, ~60 min). Run in a Snowsight worksheet, top to bottom, one block at a time.
-- Trial: choose ENTERPRISE edition (needed for masking policies and multi-cluster warehouses).

------------------------------------------------------------------ Part B: setup, load, CDC
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS LAB_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- Cost guardrail: suspend the warehouse after 5 credits a month
CREATE OR REPLACE RESOURCE MONITOR LAB_MONITOR WITH CREDIT_QUOTA = 5
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE LAB_WH SET RESOURCE_MONITOR = LAB_MONITOR;

CREATE DATABASE IF NOT EXISTS CX_LAB;
CREATE SCHEMA IF NOT EXISTS CX_LAB.RAW;
CREATE SCHEMA IF NOT EXISTS CX_LAB.GOLD;
USE WAREHOUSE LAB_WH;
USE SCHEMA CX_LAB.RAW;

-- 2 million synthetic interactions (no files needed)
CREATE OR REPLACE TABLE INTERACTIONS AS
SELECT
  SEQ8()                                                        AS interaction_id,
  GET(ARRAY_CONSTRUCT('acme_bank','globex_telco','initech_insurance'), UNIFORM(0, 2, RANDOM()))::STRING AS tenant_id,
  GET(ARRAY_CONSTRUCT('voice','chat','email'), UNIFORM(0, 2, RANDOM()))::STRING AS channel,
  'agent_' || UNIFORM(1, 200, RANDOM())                         AS agent_id,
  DATEADD(second, UNIFORM(0, 60*60*24*90, RANDOM()), '2026-07-01'::TIMESTAMP_NTZ) AS started_at,
  UNIFORM(60, 1200, RANDOM())                                   AS handle_secs,
  UNIFORM(1, 100, RANDOM()) <= 10                               AS escalated,
  '+91 9' || UNIFORM(100000000, 999999999, RANDOM())            AS customer_phone
FROM TABLE(GENERATOR(ROWCOUNT => 2000000));

-- CDC with Streams + Tasks: the stream records changes, the task processes only new rows
CREATE OR REPLACE STREAM INTERACTIONS_STREAM ON TABLE INTERACTIONS;

CREATE OR REPLACE TABLE CX_LAB.GOLD.DAILY_KPIS (
  kpi_date DATE, tenant_id STRING, interactions NUMBER, avg_handle_secs FLOAT, escalation_rate FLOAT);

CREATE OR REPLACE TASK REFRESH_DAILY_KPIS
  WAREHOUSE = LAB_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CX_LAB.RAW.INTERACTIONS_STREAM')
AS
MERGE INTO CX_LAB.GOLD.DAILY_KPIS t
USING (
  SELECT started_at::DATE AS kpi_date, tenant_id, COUNT(*) AS n,
         SUM(handle_secs) AS total_secs, SUM(IFF(escalated, 1, 0)) AS esc
  FROM CX_LAB.RAW.INTERACTIONS_STREAM
  WHERE METADATA$ACTION = 'INSERT'
  GROUP BY 1, 2
) s
ON t.kpi_date = s.kpi_date AND t.tenant_id = s.tenant_id
WHEN MATCHED THEN UPDATE SET
  avg_handle_secs = (t.avg_handle_secs * t.interactions + s.total_secs) / (t.interactions + s.n),
  escalation_rate = (t.escalation_rate * t.interactions + s.esc) / (t.interactions + s.n),
  interactions    = t.interactions + s.n
WHEN NOT MATCHED THEN INSERT VALUES (s.kpi_date, s.tenant_id, s.n, s.total_secs / s.n, s.esc / s.n);

-- First load: insert new rows so the stream has data, then run the task by hand
INSERT INTO INTERACTIONS
SELECT interaction_id + 10000000, tenant_id, channel, agent_id, DATEADD(day, 1, started_at),
       handle_secs, escalated, customer_phone
FROM INTERACTIONS SAMPLE (1);
SELECT COUNT(*) FROM INTERACTIONS_STREAM;          -- ~20k pending changes
EXECUTE TASK REFRESH_DAILY_KPIS;
-- wait ~30 s, then:
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY scheduled_time DESC LIMIT 5;
SELECT * FROM CX_LAB.GOLD.DAILY_KPIS ORDER BY kpi_date DESC LIMIT 20;
SELECT COUNT(*) FROM INTERACTIONS_STREAM;          -- 0: the stream offset moved forward
-- ALTER TASK REFRESH_DAILY_KPIS RESUME;           -- only if you want it scheduled; SUSPEND it after!

------------------------------------------------------------------ Part C: governance, performance, cost
-- PII: mask phone numbers for everyone except a privileged role
CREATE OR REPLACE MASKING POLICY PHONE_MASK AS (val STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val ELSE CONCAT(REPEAT('*', LENGTH(val) - 4), RIGHT(val, 4)) END;
ALTER TABLE INTERACTIONS MODIFY COLUMN customer_phone SET MASKING POLICY PHONE_MASK;
CREATE ROLE IF NOT EXISTS ANALYST;
GRANT USAGE ON WAREHOUSE LAB_WH TO ROLE ANALYST;
GRANT USAGE ON DATABASE CX_LAB TO ROLE ANALYST;
GRANT USAGE ON SCHEMA CX_LAB.RAW TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CX_LAB.RAW TO ROLE ANALYST;
SET me = CURRENT_USER();
GRANT ROLE ANALYST TO USER IDENTIFIER($me);
USE ROLE ANALYST;
SELECT customer_phone FROM CX_LAB.RAW.INTERACTIONS LIMIT 5;  -- masked
USE ROLE ACCOUNTADMIN;

-- Pruning: check "Partitions scanned vs total" in the Query Profile of each query
ALTER SESSION SET USE_CACHED_RESULT = FALSE;               -- so the comparison is honest
SELECT tenant_id, COUNT(*) FROM INTERACTIONS
WHERE started_at BETWEEN '2026-08-01' AND '2026-08-02' GROUP BY 1;
ALTER TABLE INTERACTIONS CLUSTER BY (started_at::DATE);
SELECT SYSTEM$CLUSTERING_INFORMATION('INTERACTIONS', '(started_at::DATE)');
-- Re-run the query above after a few minutes and compare partitions scanned.
-- Note: automatic clustering costs credits. Fine for big, frequently filtered tables; wasteful on small ones.

-- Time travel and zero-copy clone
DELETE FROM INTERACTIONS WHERE tenant_id = 'acme_bank';    -- "oops"
SELECT COUNT(*) FROM INTERACTIONS AT(OFFSET => -60);       -- the table 60 s ago
CREATE OR REPLACE TABLE INTERACTIONS_RESTORED CLONE INTERACTIONS AT(OFFSET => -60);
CREATE OR REPLACE DATABASE CX_LAB_DEV CLONE CX_LAB;        -- instant dev copy, no extra storage until changed

-- Cost visibility (INFORMATION_SCHEMA is real-time; ACCOUNT_USAGE lags up to ~45 min)
SELECT query_id, warehouse_size, total_elapsed_time / 1000 AS secs, bytes_scanned, LEFT(query_text, 80) AS q
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 100))
ORDER BY total_elapsed_time DESC LIMIT 10;

SELECT warehouse_name, SUM(credits_used) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP()) GROUP BY 1;

-- Tag queries so cost can be charged back per team or pipeline
ALTER SESSION SET QUERY_TAG = 'team=cx-platform;pipeline=daily_kpis';

-- Clean up when finished with the lab
ALTER TASK REFRESH_DAILY_KPIS SUSPEND;
ALTER WAREHOUSE LAB_WH SUSPEND;
