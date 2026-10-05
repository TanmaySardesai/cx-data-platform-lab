# Lab 01 — SQL drills (Mon Part B + Thu warm-up, ~60 min)

```bash
python labs/01_sql/sql_drills.py                          # builds cx.duckdb, prints 10 exercises
# write answers in labs/01_sql/my_answers.sql (separate statements with ;)
python labs/01_sql/sql_drills.py --run labs/01_sql/my_answers.sql
python labs/01_sql/sql_drills.py --solutions              # compare
```

Rules for practice: write each query **without autocomplete or AI first** (interviews are often in a plain editor), say out loud what each CTE does, then compare.

For every query, also answer: "How would this perform on 1 billion rows in Snowflake?"
(Hints: partition pruning on call_ts, clustering, avoiding the CROSS JOIN in #6 at scale, QUALIFY vs subquery.)
