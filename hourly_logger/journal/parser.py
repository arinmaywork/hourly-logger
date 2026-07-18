"""Deterministic parser for the daily-note template. The zero-cost fast path.

Design rule: this parser is *keyword-tolerant, structure-strict*. Section
headings are matched by lowercase keyword (so "### Activities" vs
"## My activities" both work), but if the note is restructured beyond
recognition the result comes back thin and the caller falls through to the
Gemini extractor. It never raises on malformed input.

Conventions encoded here (confirmed by the author):
  * ``~~struck through~~`` list item  -> planned AND completed
  * ``- [x]`` checked checkbox        -> planned AND completed
  * plain / ``- [ ]`` item            -> planned only
"""

from __future__ import annotations

import re

from .schema import DayRecord

# Section classification: first keyword hit wins. Order matters — "good" must
# be checked before the food keywords ("What was good" contains no food word,
# but keep specific words first anyway).
_SECTION_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("good", ("good", "win", "grateful", "highlight")),
    ("upset", ("upset", "bad", "wrong", "annoy", "regret", "lowlight")),
    ("activities", ("activit", "task", "plan", "todo", "to-do")),
    ("food", ("food", "health", "meal", "diet", "eat")),
)

_FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n?", re.DOTALL)
_COMMENT_RE = re.compile(r"%%.*?%%", re.DOTALL)
_HEADING_RE = re.compile(r"^#{1,6}\s+(.*)$")
_BULLET_RE = re.compile(r"^\s*[-*+]\s+(.*)$")
_CHECKBOX_RE = re.compile(r"^\[( |x|X)\]\s*(.*)$")
_STRIKE_RE = re.compile(r"~~(.+?)~~")
_WIKILINK_RE = re.compile(r"\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]")
_MDLINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_SCORE_RE = re.compile(r"^(mood|energy)\s*[:=]\s*(\d)\s*(?:/\s*5)?\s*$", re.IGNORECASE)


def _clean(text: str) -> str:
    """Strip link syntax and emphasis, keeping the human-readable label."""
    text = _WIKILINK_RE.sub(lambda m: m.group(2) or m.group(1), text)
    text = _MDLINK_RE.sub(r"\1", text)
    text = text.replace("**", "").replace("__", "")
    return text.strip(" \t-–—")


def _classify_heading(title: str) -> str | None:
    low = title.lower()
    for section, keywords in _SECTION_KEYWORDS:
        if any(k in low for k in keywords):
            return section
    return None


def parse_note(note_md: str, date: str) -> DayRecord:
    """Parse one note into a DayRecord. Never raises; may return a thin record."""
    rec = DayRecord(date=date)
    body = note_md

    fm = _FRONTMATTER_RE.match(body)
    if fm:
        _scan_scores(fm.group(1), rec)
        body = body[fm.end():]
    body = _COMMENT_RE.sub("", body)

    section: str | None = None
    leftovers: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        heading = _HEADING_RE.match(line.strip())
        if heading:
            section = _classify_heading(_clean(heading.group(1)))
            continue
        if _scan_scores(line.strip(), rec):
            continue
        bullet = _BULLET_RE.match(line)
        item = bullet.group(1).strip() if bullet else None
        if section == "activities" and item is not None:
            _add_activity(item, rec)
        elif section in ("good", "upset") and item is not None:
            cleaned = _clean(_STRIKE_RE.sub(r"\1", item))
            if cleaned:
                getattr(rec, section).append(cleaned)
        elif section == "food" and item is not None:
            _add_food(item, rec)
        else:
            # Unrecognised sections and non-bullet prose are kept as signal.
            cleaned = _clean(_STRIKE_RE.sub(r"\1", line.strip()))
            if cleaned:
                leftovers.append(cleaned)

    rec.free_text = "\n".join(leftovers)
    return rec


def _scan_scores(text: str, rec: DayRecord) -> bool:
    """Capture ``mood: 4`` / ``energy: 3`` wherever they appear (frontmatter
    or body). Returns True if the line was consumed."""
    hit = False
    for line in text.splitlines():
        m = _SCORE_RE.match(line.strip())
        if m and 1 <= int(m.group(2)) <= 5:
            setattr(rec, m.group(1).lower(), int(m.group(2)))
            hit = True
    return hit


def _add_activity(item: str, rec: DayRecord) -> None:
    done = False
    struck = _STRIKE_RE.fullmatch(item.strip())
    if struck:  # struck through = crossed off = completed
        done, item = True, struck.group(1)
    else:
        cb = _CHECKBOX_RE.match(item.strip())
        if cb:
            done, item = cb.group(1).lower() == "x", cb.group(2)
    name = _clean(item)
    if not name:
        return
    rec.planned.append(name)
    if done:
        rec.completed.append(name)


def _add_food(item: str, rec: DayRecord) -> None:
    """``- Breakfast - nuts`` -> {"breakfast": "nuts"}. Separator may be
    ``-`` or ``:``. Non-matching bullets land in free_text via the caller?
    No — keep them here as unnamed food notes to avoid losing signal."""
    cleaned = _clean(_STRIKE_RE.sub(r"\1", item))
    if not cleaned:
        return
    m = re.match(r"^([^:–—-]+?)\s*[-:–—]\s+(.+)$", cleaned)
    if m:
        rec.food[m.group(1).strip().lower()] = m.group(2).strip()
    else:
        rec.food.setdefault("note", cleaned)
