"""Lab 05 — daily CX pipeline: silver events -> gold KPIs, data quality, Iceberg maintenance.

Idempotent by design: each run only touches its own day ({{ ds }}) and uses MERGE, so re-runs and
backfills never double-count. Spark SQL runs inside the spark-iceberg container via `docker exec`.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


def spark_sql(sql: str) -> str:
    """Build a shell command that runs Spark SQL in the container. SQL may contain Jinja like {{ ds }}."""
    one_line = " ".join(sql.split()).replace('"', '\\"')
    return f'docker exec spark-iceberg spark-sql -S -e "{one_line}"'


CREATE_GOLD = """
CREATE TABLE IF NOT EXISTS demo.cx.daily_tenant_kpis (
  kpi_date DATE, tenant_id STRING, interactions BIGINT, avg_handle_secs DOUBLE,
  escalation_rate DOUBLE, avg_csat DOUBLE, loaded_at TIMESTAMP)
USING iceberg PARTITIONED BY (kpi_date)
"""

MERGE_GOLD = """
MERGE INTO demo.cx.daily_tenant_kpis t
USING (
  SELECT DATE '{{ ds }}' AS kpi_date, tenant_id,
         count(DISTINCT interaction_id) AS interactions,
         avg(CASE WHEN event_type = 'interaction_ended' THEN handle_secs END) AS avg_handle_secs,
         avg(CASE WHEN event_type = 'interaction_ended' THEN int(escalated) END) AS escalation_rate,
         avg(csat) AS avg_csat,
         current_timestamp() AS loaded_at
  FROM demo.cx.events_silver
  WHERE event_ts >= TIMESTAMP '{{ ds }} 00:00:00' AND event_ts < TIMESTAMP '{{ next_ds }} 00:00:00'
  GROUP BY tenant_id
) s
ON t.kpi_date = s.kpi_date AND t.tenant_id = s.tenant_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

# Fails the task (non-zero exit) if any raw phone number or email reached the silver layer for the day
PII_CHECK = """
SELECT assert_true(count(*) = 0, 'PII found in events_silver')
FROM demo.cx.events_silver
WHERE event_ts >= TIMESTAMP '{{ ds }} 00:00:00' AND event_ts < TIMESTAMP '{{ next_ds }} 00:00:00'
  AND (text RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]+' OR text RLIKE '[0-9]{5}[ -]?[0-9]{5}')
"""

COMPACT = "CALL demo.system.rewrite_data_files(table => 'cx.events_silver', options => map('min-input-files', '2'))"
EXPIRE = ("CALL demo.system.expire_snapshots(table => 'cx.events_silver', "
          "older_than => TIMESTAMP '{{ ds }} 00:00:00', retain_last => 10)")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="cx_daily_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,          # protects the cluster during backfills
    default_args=default_args,
    tags=["cx", "iceberg", "lab"],
    doc_md=__doc__,
) as dag:
    create_gold = BashOperator(task_id="create_gold_table", bash_command=spark_sql(CREATE_GOLD))
    pii_check = BashOperator(task_id="dq_no_pii_in_silver", bash_command=spark_sql(PII_CHECK))
    merge_gold = BashOperator(task_id="merge_daily_kpis", bash_command=spark_sql(MERGE_GOLD))
    row_check = BashOperator(
        task_id="report_gold_row_count",
        bash_command=spark_sql("SELECT count(*) FROM demo.cx.daily_tenant_kpis WHERE kpi_date = DATE '{{ ds }}'"),
    )
    compact = BashOperator(task_id="compact_silver_files", bash_command=spark_sql(COMPACT))
    expire = BashOperator(task_id="expire_old_snapshots", bash_command=spark_sql(EXPIRE))

    create_gold >> pii_check >> merge_gold >> row_check
    merge_gold >> compact >> expire
