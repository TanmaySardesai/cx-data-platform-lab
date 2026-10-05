# cx-data-platform-lab

A local, end-to-end contact-centre (CX) data platform for learning and demonstrating streaming, lakehouse, orchestration, governance and AI-assistant evaluation.

```
Simulated interactions ─► Kafka ─► Spark Structured Streaming ─► Iceberg on S3 (SeaweedFS)
  (calls, chats, bots)    keyed by    parse · drop bad events ·      silver: events, partitioned by day + tenant
                          interaction  redact phone/email             gold: daily KPIs per tenant (Airflow MERGE)
                                                                          │
                                         Snowflake (CDC with Streams/Tasks, masking, cost controls)
AI assistant ─► golden set ─► evaluation harness ─► quality gate in GitHub Actions
```

| Lab | Topic | What you do |
| --- | --- | --- |
| [00](labs/00_setup) | Setup | Docker stack: Kafka, Spark + Iceberg, Iceberg REST catalog, SeaweedFS (S3) |
| [01](labs/01_sql) | SQL | 10 drills on contact-centre data in DuckDB (window functions, sessions, SCD2) |
| [02](labs/02_kafka) | Kafka | Keyed producer, consumer groups, lag, dead-letter topic, idempotent producer |
| [03](labs/03_spark) | Spark | Skewed joins + AQE + salting; streaming from Kafka with PII redaction into Iceberg |
| [04](labs/04_iceberg) | Iceberg | Snapshots, time travel, schema and partition evolution, compaction |
| [05](labs/05_airflow) | Airflow | Idempotent daily DAG with MERGE, data-quality gates, backfills, maintenance |
| [06](labs/06_snowflake) | Snowflake | Streams + Tasks, masking policy, clustering, time travel, cost monitoring |
| [07](labs/07_ai_eval) | AI evaluation | Golden set, metrics, version comparison, LLM-as-judge, quality gate |
| [08](labs/08_cicd) | CI/CD | GitHub Actions: lint, tests, AI quality gate, DAG validation |
| [09](labs/09_aws) | AWS | S3 + least-privilege IAM, S3-triggered Lambda, Kinesis, CloudWatch alarm |
| [10](labs/10_k8s) | Kubernetes | Deployments and services on kind; Spark Operator running a SparkApplication |

## Quick start
```bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
python labs/02_kafka/producer.py --rate 20
```

## What I'd change for production
- Amazon MSK or Kinesis instead of local Kafka; EMR on EKS or EMR Serverless for Spark; S3 + Glue/REST catalog for Iceberg.
- Schema Registry (Avro/Protobuf) with compatibility rules instead of free-form JSON.
- PII detection with a proper service (not regex), tokenisation for reversible cases, per-tenant encryption keys.
- Terraform for all infrastructure, OIDC roles for CI, data contracts and lineage (OpenLineage) across jobs.
