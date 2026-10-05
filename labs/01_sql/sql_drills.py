"""Lab 01 — SQL drills on contact-centre data, using DuckDB (no server needed).

Run:   python labs/01_sql/sql_drills.py            -> creates cx.duckdb with sample data, prints the exercises
Then:  write each answer yourself in labs/01_sql/my_answers.sql, run it with:
       python labs/01_sql/sql_drills.py --run labs/01_sql/my_answers.sql
Check: python labs/01_sql/sql_drills.py --solutions   (only after trying!)
"""
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

DB = Path(__file__).with_name("cx.duckdb")
random.seed(42)

EXERCISES = [
    "1. Top 3 most recent calls per agent (calls table). Use a window function + QUALIFY.",
    "2. Each agent's average handle time and their rank within their team (DENSE_RANK).",
    "3. Sessionise web_events: new session when a customer is inactive > 30 minutes. Output customer_id, session_id, start, end, events.",
    "4. 7-day rolling average of calls per day.",
    "5. Customers who called 3+ times within any 7-day window (repeat callers = bad first-contact resolution).",
    "6. Agents with NO calls on a given day (anti-join between agents x days and calls).",
    "7. Gaps and islands: for each agent, find streaks of consecutive working days.",
    "8. Deduplicate calls_raw keeping the latest ingested row per call_id.",
    "9. Percent of calls per channel per day that were escalated, with a day-over-day change (LAG).",
    "10. Build an SCD Type 2 view of agent_changes: valid_from, valid_to, is_current.",
]


def build(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE OR REPLACE TABLE agents(agent_id INT, team VARCHAR)")
    con.executemany("INSERT INTO agents VALUES (?, ?)", [(a, f"team_{a % 3}") for a in range(1, 13)])

    start = datetime(2026, 9, 1)
    rows = []
    for call_id in range(1, 3001):
        ts = start + timedelta(minutes=random.randint(0, 30 * 24 * 60))
        rows.append((
            call_id,
            random.randint(1, 12),
            random.randint(1, 400),
            ts,
            random.randint(60, 1200),
            random.choice(["voice", "chat", "email"]),
            random.random() < 0.12,
        ))
    con.execute("""CREATE OR REPLACE TABLE calls(call_id INT, agent_id INT, customer_id INT,
                   call_ts TIMESTAMP, handle_secs INT, channel VARCHAR, escalated BOOLEAN)""")
    con.executemany("INSERT INTO calls VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

    # duplicates with different ingestion times
    con.execute("""CREATE OR REPLACE TABLE calls_raw AS
                   SELECT *, call_ts + INTERVAL 5 MINUTE AS ingested_at FROM calls
                   UNION ALL
                   SELECT *, call_ts + INTERVAL 50 MINUTE AS ingested_at FROM calls WHERE call_id % 7 = 0""")

    ev = []
    for cust in range(1, 60):
        t = start + timedelta(hours=random.randint(0, 600))
        for _ in range(random.randint(5, 25)):
            t += timedelta(minutes=random.choice([1, 2, 5, 8, 45, 90]))
            ev.append((cust, t, random.choice(["view_faq", "open_chat", "search", "rate_bot"])))
    con.execute("CREATE OR REPLACE TABLE web_events(customer_id INT, ts TIMESTAMP, event VARCHAR)")
    con.executemany("INSERT INTO web_events VALUES (?, ?, ?)", ev)

    con.execute("CREATE OR REPLACE TABLE agent_changes(agent_id INT, team VARCHAR, changed_at TIMESTAMP)")
    con.executemany("INSERT INTO agent_changes VALUES (?, ?, ?)", [
        (1, "team_0", datetime(2025, 1, 1)), (1, "team_2", datetime(2026, 3, 1)),
        (2, "team_1", datetime(2025, 6, 1)), (3, "team_0", datetime(2025, 2, 1)),
        (3, "team_1", datetime(2025, 9, 1)), (3, "team_2", datetime(2026, 8, 1)),
    ])


def run_file(con, path: str) -> None:
    for stmt in [s.strip() for s in Path(path).read_text().split(";") if s.strip()]:
        print("\n>>>", stmt.splitlines()[0][:100])
        print(con.sql(stmt).limit(10))


if __name__ == "__main__":
    con = duckdb.connect(str(DB))
    if not con.sql("SELECT count(*) FROM information_schema.tables WHERE table_name='calls'").fetchone()[0]:
        build(con)
        print(f"Built sample data in {DB}")
    if "--run" in sys.argv:
        run_file(con, sys.argv[sys.argv.index("--run") + 1])
    elif "--solutions" in sys.argv:
        run_file(con, str(Path(__file__).with_name("solutions.sql")))
    else:
        print("Tables: agents, calls, calls_raw, web_events, agent_changes\n")
        print("\n".join(EXERCISES))
