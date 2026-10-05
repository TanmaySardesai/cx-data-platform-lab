# Lab 07 — Evaluating an AI assistant (Fri Parts A and B, ~75 min)

This mirrors the JD line "self-serve tooling for creating, evaluating, and improving AI assistants".

Files: `knowledge_base.json` (10 support articles) · `golden_set.jsonl` (20 questions with expected article, required facts, and whether a human should take over) · `bot.py` (two bot versions) · `evaluate.py` (the harness) · `tests/`.

## Part A — the harness (40 min)
```bash
python labs/07_ai_eval/evaluate.py --version v1
python labs/07_ai_eval/evaluate.py --compare v1 v2
```
You'll see a real trade-off: **v2 escalates correctly (fraud, complaints) but regresses on q04 and still fails q15.**
That's exactly what a regression report is for.

**Exercise:** fix `answer_v2` so q04 and q15 pass without breaking anything else. Hints: stop-words like "in" make "log in" match too easily; "statement" in q04 pulls it to the wrong article — weight multi-word keyword matches higher. Re-run `--compare` after each change.
Then add 5 new golden questions of your own (including one the bot SHOULD fall back on), and see what breaks.

## Part B — LLM as judge (35 min)
```bash
export ANTHROPIC_API_KEY=...            # optional
python labs/07_ai_eval/evaluate.py --version v2 --judge
```
No API key? Skip running it, but read `llm_judge()` and be ready to explain:
- rubric + reference answer → scores **groundedness**, not just "sounds good";
- temperature 0, a pinned judge model version, JSON output;
- calibrate: hand-label 20 replies, check how often the judge agrees, before trusting it;
- known biases: favours longer answers and its own style; position bias in A/B comparisons.

## Metrics to know for a real contact-centre bot
Task success / resolution · containment (resolved without a human) · correct escalation · groundedness / hallucination rate · fallback rate · p95 latency · cost per conversation · CSAT after bot conversations.

## Talk track
"I built an evaluation harness for a support assistant: a versioned golden set, deterministic metrics for retrieval, answer correctness and escalation, an optional LLM judge, and a quality gate that fails CI when a new version regresses. Comparing two versions showed one fixed escalation but regressed two answers, which is exactly the kind of thing you want caught before release."
