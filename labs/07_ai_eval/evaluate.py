"""Evaluation harness for the support bot (Lab 07).

    python labs/07_ai_eval/evaluate.py --version v1
    python labs/07_ai_eval/evaluate.py --compare v1 v2                 # side by side + regressions
    python labs/07_ai_eval/evaluate.py --version v2 --gate "retrieval_hit_rate>=0.9,answer_accuracy>=0.85"
    python labs/07_ai_eval/evaluate.py --version v2 --judge            # adds an LLM-as-judge score (needs ANTHROPIC_API_KEY)

--gate exits with code 1 when a metric misses its threshold: that is what blocks a bad release in CI.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from bot import KB, VERSIONS  # noqa: E402

HERE = Path(__file__).parent
ARTICLES = {a["id"]: a for a in KB}


def load_golden(path: Path = HERE / "golden_set.jsonl") -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def score_item(item: dict, reply) -> dict:
    text = reply.text.lower()
    return {
        "id": item["id"],
        "question": item["question"],
        "retrieval_hit": reply.article_id == item["expected_article"],
        "answer_correct": all(m.lower() in text for m in item["must_include"]),
        "escalation_correct": reply.escalated == item["should_escalate"],
        "fallback": reply.article_id is None,
        "article_id": reply.article_id,
    }


def aggregate(rows: list[dict]) -> dict:
    n = len(rows)
    agg = {
        "n": n,
        "retrieval_hit_rate": sum(r["retrieval_hit"] for r in rows) / n,
        "answer_accuracy": sum(r["answer_correct"] for r in rows) / n,
        "escalation_accuracy": sum(r["escalation_correct"] for r in rows) / n,
        "fallback_rate": sum(r["fallback"] for r in rows) / n,
        "p95_latency_ms": sorted(r["latency_ms"] for r in rows)[max(int(0.95 * n) - 1, 0)],
    }
    judged = [r["judge_score"] for r in rows if r.get("judge_score") is not None]
    if judged:
        agg["judge_avg_1to5"] = sum(judged) / len(judged)
    return {k: round(v, 3) if isinstance(v, float) else v for k, v in agg.items()}


def llm_judge(item: dict, reply_text: str) -> int | None:
    """Score 1-5 for correctness + groundedness against the reference article. Returns None if unavailable."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    import anthropic  # optional dependency

    reference = ARTICLES[item["expected_article"]]["answer"]
    prompt = (
        "You grade a bank support bot. Score its reply from 1 (wrong or unsupported) to 5 (correct, "
        "complete, and fully supported by the reference). Penalise any claim not in the reference.\n\n"
        f"Customer question: {item['question']}\nReference answer: {reference}\nBot reply: {reply_text}\n\n"
        'Respond with JSON only: {"score": <1-5>, "reason": "<one sentence>"}'
    )
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=os.environ.get("JUDGE_MODEL", "claude-sonnet-4-5"),
        max_tokens=200,
        temperature=0,                 # judges should be as deterministic as possible
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return int(json.loads(msg.content[0].text)["score"])
    except (ValueError, KeyError, json.JSONDecodeError):
        return None


def run(version: str, judge: bool = False) -> tuple[dict, list[dict]]:
    answer = VERSIONS[version]
    rows = []
    for item in load_golden():
        t0 = time.perf_counter()
        reply = answer(item["question"])
        row = score_item(item, reply)
        row["latency_ms"] = round((time.perf_counter() - t0) * 1000, 3)
        if judge:
            row["judge_score"] = llm_judge(item, reply.text)
        rows.append(row)
    summary = aggregate(rows)
    out = HERE / "results"
    out.mkdir(exist_ok=True)
    (out / f"{version}.json").write_text(json.dumps({"summary": summary, "rows": rows}, indent=2))
    return summary, rows


def check_gate(summary: dict, gate: str) -> list[str]:
    """gate like 'retrieval_hit_rate>=0.9,answer_accuracy>=0.85' -> list of failures."""
    failures = []
    for rule in filter(None, (r.strip() for r in gate.split(","))):
        metric, threshold = rule.split(">=")
        value = summary.get(metric.strip())
        if value is None or value < float(threshold):
            failures.append(f"{metric.strip()}={value} < {threshold}")
    return failures


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default="v2", choices=VERSIONS)
    ap.add_argument("--compare", nargs=2, metavar=("BASE", "CANDIDATE"))
    ap.add_argument("--gate", default="")
    ap.add_argument("--judge", action="store_true")
    args = ap.parse_args()

    if args.compare:
        base, cand = args.compare
        (s1, r1), (s2, r2) = run(base, args.judge), run(cand, args.judge)
        print(f"{'metric':<22}{base:>8}{cand:>8}")
        for k in s1:
            print(f"{k:<22}{s1[k]:>8}{s2[k]:>8}")
        regressions = [b["id"] for b, c in zip(r1, r2) if b["answer_correct"] and not c["answer_correct"]]
        print("\nRegressions (correct in base, wrong in candidate):", regressions or "none")
        print("Still failing in candidate:", [c["id"] for c in r2 if not c["answer_correct"]] or "none")
        return 0

    summary, rows = run(args.version, args.judge)
    print(json.dumps(summary, indent=2))
    failing = [r for r in rows if not (r["answer_correct"] and r["escalation_correct"])]
    for r in failing:
        print(f"FAIL {r['id']}: {r['question']!r} -> {r['article_id']}")
    if args.gate:
        failures = check_gate(summary, args.gate)
        if failures:
            print("QUALITY GATE FAILED:", "; ".join(failures))
            return 1
        print("Quality gate passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
