"""Gemini fallback extractor.

Called only when the rules parser returns a thin record — i.e. the template
has drifted past what the deterministic path recognises. Token budget is
deliberately tiny: one short instruction + the note body, structured-output
mode (``responseSchema``) so the reply is pure JSON with zero prose overhead,
temperature 0 for determinism. At ~1 call/day worst case this never brushes
the free-tier limits.
"""

from __future__ import annotations

import json
from typing import Any

from ..ai.llm import LlmError, generate
from ..logger import get_logger
from .schema import DayRecord, GEMINI_RESPONSE_SCHEMA

log = get_logger(__name__)

_MAX_NOTE_CHARS = 8_000  # a daily note is ~1-2 KB; this is a safety cap, not a budget

_INSTRUCTION = (
    "Extract structured data from this daily journal note (markdown).\n"
    "planned: every activity the author intended to do that day.\n"
    "completed: the subset actually done. Struck-through (~~text~~) or "
    "checked ([x]) items ARE completed; plain items are planned only.\n"
    "good / upset: the day's listed positives / negatives.\n"
    "food: meals or food notes as {meal, item} pairs.\n"
    "mood / energy: only if explicitly present as a 1-5 number, else null.\n"
    "free_text: one short line for anything not captured above (may be empty).\n"
    "Use the author's own words. Do not invent content."
)


class ExtractorError(RuntimeError):
    """Raised when Gemini extraction fails after all retries."""


def extract(note_md: str, date: str) -> DayRecord:
    """Extract a DayRecord from raw note markdown via Gemini."""
    try:
        text = generate(
            f"{_INSTRUCTION}\n\n{note_md[:_MAX_NOTE_CHARS]}",
            response_schema=GEMINI_RESPONSE_SCHEMA,
            temperature=0,
        )
        return _to_record(json.loads(text), date)
    except LlmError as e:
        raise ExtractorError(str(e)) from e
    except (KeyError, IndexError, TypeError, ValueError) as e:
        raise ExtractorError(f"Unparseable Gemini response: {e}") from e


def _to_record(obj: dict[str, Any], date: str) -> DayRecord:
    """Map Gemini JSON onto DayRecord, folding the food pair-array to a dict."""
    food_pairs = obj.get("food") or []
    food = {
        str(p.get("meal", "")).strip().lower(): str(p.get("item", "")).strip()
        for p in food_pairs
        if isinstance(p, dict) and p.get("meal") and p.get("item")
    }

    def _score(key: str) -> int | None:
        v = obj.get(key)
        return v if isinstance(v, int) and 1 <= v <= 5 else None

    def _strs(key: str) -> list[str]:
        return [str(x).strip() for x in (obj.get(key) or []) if str(x).strip()]

    return DayRecord(
        date=date,
        mood=_score("mood"),
        energy=_score("energy"),
        planned=_strs("planned"),
        completed=_strs("completed"),
        good=_strs("good"),
        upset=_strs("upset"),
        food=food,
        free_text=str(obj.get("free_text") or "").strip(),
    )
