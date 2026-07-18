"""/ask pipeline: question -> deterministic facts -> Gemini -> grounded answer.

Context is assembled fresh per question (cheap — it's all local SQL) and
kept compact on purpose: a facts block plus one line per recent journal
day. Instruction pins the model to the provided data; anything it can't
support with a number or a quoted journal item, it must say it can't answer.
"""

from __future__ import annotations

from .llm import generate
from .stats import build_facts, recent_journal_lines

_MAX_QUESTION_CHARS = 500
_JOURNAL_DETAIL_DAYS = 14

_INSTRUCTION = (
    "You are the analysis layer of a personal time-tracking system. The "
    "owner logs every hour (category + activity tag) and keeps a daily "
    "journal. Below is DATA computed deterministically from those records, "
    "then the owner's QUESTION.\n"
    "Rules:\n"
    "- Ground every claim in the data; cite the actual numbers or quoted "
    "journal items you rely on.\n"
    "- Never invent or extrapolate figures. If the data cannot answer the "
    "question, say exactly what is missing.\n"
    "- Be direct and specific; no flattery, no generic self-help filler.\n"
    "- At most one concrete, data-backed suggestion when relevant.\n"
    "- Plain text only (Telegram), under 180 words."
)


def build_prompt(question: str) -> str:
    parts = [
        _INSTRUCTION,
        "",
        "=== DATA ===",
        build_facts(30),
        "",
        f"recent journal, one line per day (last {_JOURNAL_DETAIL_DAYS}d):",
        *(recent_journal_lines(_JOURNAL_DETAIL_DAYS) or ["(no journal days ingested)"]),
        "",
        "=== QUESTION ===",
        question.strip()[:_MAX_QUESTION_CHARS],
    ]
    return "\n".join(parts)


def answer(question: str) -> str:
    """Blocking. Handlers call this via ``asyncio.to_thread``."""
    return generate(build_prompt(question), temperature=0.4, max_output_tokens=800)
