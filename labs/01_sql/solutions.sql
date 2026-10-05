-- Lab 01 solutions (DuckDB dialect). Try each one yourself first.

-- 1. Top 3 most recent calls per agent
SELECT agent_id, call_id, call_ts
FROM calls
QUALIFY ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY call_ts DESC) <= 3
ORDER BY agent_id, call_ts DESC;

-- 2. Average handle time and rank within team
SELECT a.team, c.agent_id,
       ROUND(AVG(c.handle_secs), 1) AS avg_handle_secs,
       DENSE_RANK() OVER (PARTITION BY a.team ORDER BY AVG(c.handle_secs)) AS rank_in_team
FROM calls c JOIN agents a USING (agent_id)
GROUP BY a.team, c.agent_id
ORDER BY a.team, rank_in_team;

-- 3. Sessionise: new session after 30 minutes of inactivity
WITH flagged AS (
  SELECT *,
         CASE WHEN LAG(ts) OVER (PARTITION BY customer_id ORDER BY ts) IS NULL
                OR ts - LAG(ts) OVER (PARTITION BY customer_id ORDER BY ts) > INTERVAL 30 MINUTE
              THEN 1 ELSE 0 END AS new_session
  FROM web_events),
numbered AS (
  SELECT *, SUM(new_session) OVER (PARTITION BY customer_id ORDER BY ts
                                   ROWS UNBOUNDED PRECEDING) AS session_id
  FROM flagged)
SELECT customer_id, session_id, MIN(ts) AS session_start, MAX(ts) AS session_end, COUNT(*) AS events
FROM numbered
GROUP BY customer_id, session_id
ORDER BY customer_id, session_id;

-- 4. 7-day rolling average of calls per day
WITH daily AS (SELECT CAST(call_ts AS DATE) AS day, COUNT(*) AS n FROM calls GROUP BY 1)
SELECT day, n,
       ROUND(AVG(n) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily ORDER BY day;

-- 5. Repeat callers: 3+ calls within any 7-day window
SELECT DISTINCT customer_id
FROM (
  SELECT customer_id,
         COUNT(*) OVER (PARTITION BY customer_id ORDER BY call_ts
                        RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS calls_in_7d
  FROM calls)
WHERE calls_in_7d >= 3
ORDER BY customer_id;

-- 6. Agent-days with no calls (anti-join)
WITH days AS (SELECT DISTINCT CAST(call_ts AS DATE) AS day FROM calls),
grid AS (SELECT a.agent_id, d.day FROM agents a CROSS JOIN days d)
SELECT g.agent_id, g.day
FROM grid g
LEFT JOIN calls c ON c.agent_id = g.agent_id AND CAST(c.call_ts AS DATE) = g.day
WHERE c.call_id IS NULL
ORDER BY g.day, g.agent_id;

-- 7. Gaps and islands: streaks of consecutive working days per agent
WITH worked AS (SELECT DISTINCT agent_id, CAST(call_ts AS DATE) AS day FROM calls),
grouped AS (
  SELECT *, day - CAST(ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY day) AS INTEGER) AS grp
  FROM worked)
SELECT agent_id, MIN(day) AS streak_start, MAX(day) AS streak_end, COUNT(*) AS streak_days
FROM grouped
GROUP BY agent_id, grp
ORDER BY agent_id, streak_start;

-- 8. Deduplicate: keep the latest ingested row per call_id
SELECT *
FROM calls_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY call_id ORDER BY ingested_at DESC) = 1;

-- 9. Escalation % per channel per day, with day-over-day change
WITH daily AS (
  SELECT CAST(call_ts AS DATE) AS day, channel,
         ROUND(100.0 * AVG(CASE WHEN escalated THEN 1 ELSE 0 END), 1) AS escalation_pct
  FROM calls GROUP BY 1, 2)
SELECT *, escalation_pct - LAG(escalation_pct) OVER (PARTITION BY channel ORDER BY day) AS change_vs_prev_day
FROM daily ORDER BY channel, day;

-- 10. SCD Type 2 from a change log
SELECT agent_id, team,
       changed_at AS valid_from,
       LEAD(changed_at) OVER (PARTITION BY agent_id ORDER BY changed_at) AS valid_to,
       LEAD(changed_at) OVER (PARTITION BY agent_id ORDER BY changed_at) IS NULL AS is_current
FROM agent_changes
ORDER BY agent_id, valid_from;
