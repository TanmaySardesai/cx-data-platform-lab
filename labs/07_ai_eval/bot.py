"""A tiny, deterministic support bot with two versions, standing in for a real AI assistant.

v1: matches the question against article keywords with plain substring checks.
v2: normalises text, scores keyword *word overlap*, and escalates fraud/complaints to a human.

The point of the lab is the evaluation harness, not the bot: swap `answer()` for a call to a real
LLM/RAG assistant later and the harness stays the same.
"""
import json
import re
from dataclasses import dataclass
from pathlib import Path

KB = json.loads(Path(__file__).with_name("knowledge_base.json").read_text())
ESCALATE_ARTICLES = {"kb_fraud", "kb_complaint"}
FALLBACK = "Sorry, I didn't understand. Could you rephrase?"


@dataclass
class BotReply:
    text: str
    article_id: str | None
    escalated: bool


def _words(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def answer_v1(question: str) -> BotReply:
    q = question.lower()
    for article in KB:
        if any(k in q for k in article["keywords"]):
            return BotReply(article["answer"], article["id"], escalated=False)
    return BotReply(FALLBACK, None, escalated=False)


def answer_v2(question: str) -> BotReply:
    q_words = _words(question)
    best, best_score = None, 0.0
    for article in KB:
        score = 0.0
        for k in article["keywords"]:
            k_words = _words(k)
            overlap = len(q_words & k_words) / len(k_words)
            score = max(score, overlap)
        score += 0.25 * len(q_words & _words(article["title"])) / max(len(_words(article["title"])), 1)
        if score > best_score:
            best, best_score = article, score
    if best is None or best_score < 0.5:
        return BotReply(FALLBACK, None, escalated=True)   # unsure -> hand to a human
    return BotReply(best["answer"], best["id"], escalated=best["id"] in ESCALATE_ARTICLES)


VERSIONS = {"v1": answer_v1, "v2": answer_v2}
