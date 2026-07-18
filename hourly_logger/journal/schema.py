"""Canonical day schema.

Every journal note — whatever its template looks like — is normalised into a
:class:`DayRecord`. Template changes therefore never propagate past the
parsing layer: downstream consumers (stats, /ask, weekly review) only ever
see this shape. ``raw_md`` is stored separately in the DB, so the schema can
be extended later and all history re-extracted for free.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class DayRecord(BaseModel):
    """Structured view of one daily note."""

    date: str = ""  # YYYY-MM-DD
    mood: Optional[int] = Field(None, ge=1, le=5)
    energy: Optional[int] = Field(None, ge=1, le=5)
    planned: list[str] = Field(default_factory=list)    # everything listed for the day
    completed: list[str] = Field(default_factory=list)  # subset actually done
    good: list[str] = Field(default_factory=list)
    upset: list[str] = Field(default_factory=list)
    food: dict[str, str] = Field(default_factory=dict)  # meal -> what was eaten
    free_text: str = ""  # remainder not captured by the fields above

    def is_thin(self) -> bool:
        """True when extraction captured almost nothing.

        Used twice: (1) after the rules parser, to decide whether the Gemini
        fallback is worth a call; (2) after ingest, to raise the drift alarm
        so a silently-broken template never goes unnoticed.
        """
        return not (
            self.planned
            or self.good
            or self.upset
            or self.food
            or len(self.free_text.strip()) >= 40
        )


# Gemini structured-output schema (OpenAPI subset — no additionalProperties,
# so ``food`` travels as an array of {meal, item} pairs and is folded back
# into a dict by the extractor).
_STR = {"type": "string"}
_STR_ARR = {"type": "array", "items": _STR}
GEMINI_RESPONSE_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "mood": {"type": "integer", "nullable": True},
        "energy": {"type": "integer", "nullable": True},
        "planned": _STR_ARR,
        "completed": _STR_ARR,
        "good": _STR_ARR,
        "upset": _STR_ARR,
        "food": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"meal": _STR, "item": _STR},
                "required": ["meal", "item"],
            },
        },
        "free_text": _STR,
    },
    "required": ["planned", "completed", "good", "upset", "food", "free_text"],
}
