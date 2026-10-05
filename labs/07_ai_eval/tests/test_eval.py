"""Unit tests for the evaluation harness. Run: pytest labs/07_ai_eval -q"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bot import BotReply, answer_v2
from evaluate import aggregate, check_gate, load_golden, score_item

ITEM = {"id": "t1", "question": "q", "expected_article": "kb_fraud",
        "must_include": ["24 hours"], "should_escalate": True}


def test_golden_set_is_well_formed():
    items = load_golden()
    assert len(items) >= 20
    assert len({i["id"] for i in items}) == len(items), "duplicate ids"
    for i in items:
        assert {"question", "expected_article", "must_include", "should_escalate"} <= i.keys()


def test_score_item_all_correct():
    reply = BotReply("A specialist calls within 24 hours.", "kb_fraud", escalated=True)
    row = score_item(ITEM, reply)
    assert row["retrieval_hit"] and row["answer_correct"] and row["escalation_correct"]


def test_score_item_wrong_article_and_missing_escalation():
    reply = BotReply("Reset your password.", "kb_login", escalated=False)
    row = score_item(ITEM, reply)
    assert not row["retrieval_hit"] and not row["answer_correct"] and not row["escalation_correct"]


def test_aggregate_rates():
    rows = [
        {"retrieval_hit": True, "answer_correct": True, "escalation_correct": True, "fallback": False, "latency_ms": 1},
        {"retrieval_hit": False, "answer_correct": False, "escalation_correct": True, "fallback": True, "latency_ms": 3},
    ]
    s = aggregate(rows)
    assert s["retrieval_hit_rate"] == 0.5 and s["fallback_rate"] == 0.5 and s["escalation_accuracy"] == 1.0


def test_gate_fails_below_threshold():
    assert check_gate({"answer_accuracy": 0.7}, "answer_accuracy>=0.8") == ["answer_accuracy=0.7 < 0.8"]
    assert check_gate({"answer_accuracy": 0.9}, "answer_accuracy>=0.8") == []


def test_fraud_is_escalated_by_v2():
    assert answer_v2("I think this is fraud on my card").escalated
