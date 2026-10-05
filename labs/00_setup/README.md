# Lab 00 — Setup (Mon, ~45 min)

## 1. Tools
- Docker Desktop (Settings → Resources → Memory ≥ 6 GB). Windows: use WSL2 and run everything inside Ubuntu.
- Python 3.11+, Git, VS Code, and Claude Code or Cursor.

## 2. Repo
```bash
# unzip the kit, then:
cd cx-data-platform-lab
git init && git add . && git commit -m "Lab kit: local CX data platform skeleton"
# create an empty repo called cx-data-platform-lab on GitHub, then:
git remote add origin https://github.com/<you>/cx-data-platform-lab.git
git branch -M main && git push -u origin main
```

## 3. Python env
```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 4. Start the platform
```bash
docker compose up -d
docker compose ps                  # all services "running" (mc stays running too)
```
Check: Kafka UI http://localhost:8081 · Jupyter http://localhost:8888 · S3 file browser http://localhost:8889/buckets/ (bucket "warehouse" exists; `docker logs mc` should say "bucket ready").

Smoke test Spark + Iceberg:
```bash
docker exec -it spark-iceberg spark-sql -e "CREATE NAMESPACE IF NOT EXISTS demo.cx; SHOW NAMESPACES IN demo;"
```

## 5. Accounts (do tonight, they take a while)
- AWS free tier → FIRST create a budget: Billing → Budgets → $5 monthly with email alert.
- Snowflake 30-day trial (Enterprise edition, AWS, region Mumbai or Singapore).
- Optional: Anthropic API key for the LLM judge in lab 07 (the lab also works without it).

## Talk track (what you can now say)
"I run a local platform with Kafka, Spark, an Iceberg REST catalog and SeaweedFS standing in for S3, all in Docker Compose, so I can test pipelines end to end before they touch AWS."
