# Lab 05 — Airflow (Thu Part A, ~60 min)

## Install (Mac, Linux or WSL — not native Windows)
```bash
python -m venv .venv-airflow && source .venv-airflow/bin/activate
AIRFLOW_VERSION=2.10.5
PY=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
export AIRFLOW_HOME=$(pwd)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/labs/05_airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone          # prints the admin password; UI at http://localhost:8080
```
(Port clash? The Spark master UI also uses 8080 — run `airflow standalone` with `AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8090`.)

## Part A — run it, break it, backfill it
1. In the UI, unpause `cx_daily_pipeline`, trigger a run. Open Graph view → logs of each task.
2. **Idempotency test**: clear and re-run `merge_daily_kpis` for the same day 3 times. Check counts don't change:
   `docker exec -it spark-iceberg spark-sql -e "SELECT * FROM demo.cx.daily_tenant_kpis ORDER BY kpi_date"`
3. **Backfill**: `airflow dags backfill -s 2026-10-01 -e 2026-10-05 cx_daily_pipeline` — `max_active_runs=1` keeps it to one day at a time.
4. **Break the DQ check**: add a row with a raw email to silver, re-run today's DAG → `dq_no_pii_in_silver` fails and `merge_daily_kpis` never runs:
   `docker exec -it spark-iceberg spark-submit /home/iceberg/labs/05_airflow/inject_bad_row.py`
   Then remove it (`docker exec -it spark-iceberg spark-sql -e "DELETE FROM demo.cx.events_silver WHERE event_id = 'bad-row'"`) and clear the failed task.

## Be ready to explain
- Why MERGE on (kpi_date, tenant_id) makes the DAG idempotent; why `{{ ds }}`/`{{ next_ds }}` instead of `now()`.
- Retries with exponential backoff; `max_active_runs`; `catchup=False` vs backfill.
- Sensors vs deferrable operators; why no heavy code at DAG top level.
- In production: KubernetesPodOperator / Spark on EMR instead of `docker exec`; Airflow 3 changes (task SDK, `from airflow.sdk import dag, task`, DAG versioning).
