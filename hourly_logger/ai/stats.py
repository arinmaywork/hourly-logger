"""Deterministic statistics — the ground truth under every AI answer.

Rule: Gemini never estimates a number. Everything quantitative is computed
here in SQL/Python and handed to the model as a pre-formatted facts block;
the model's job is interpretation only. That is what keeps /ask answers
grounded instead of horoscope-flavoured.

All windows are anchored on *log days* (LOG_DAY_START_HOUR) so figures match
the Weekly grid and existing /status figures the user already trusts.
"""

from __future__ import annotations

import datetime as dt
import json
from collections import Counter
from datetime import datetime, timezone

from ..config import settings
from ..database import db_connect, queue_category_hours_in_window
from ..dates import log_day_bounds, log_today


def _window_utc(days: int) -> tuple[datetime, datetime]:
    """UTC bounds covering the last *days* log-days up to now."""
    today = log_today(settings.tz)
    start_utc, _ = log_day_bounds(today - dt.timedelta(days=days - 1), settings.tz)
    return start_utc, datetime.now(timezone.utc)


def category_hours(days: int) -> dict[str, int]:
    start, end = _window_utc(days)
    return queue_category_hours_in_window(start, end)


def top_tags(days: int, limit: int = 6) -> list[tuple[str, int]]:
    """Most frequent activity tags (hours each) in the window."""
    start, end = _window_utc(days)
    from ..database import canonical_ts  # local import keeps module deps flat

    with db_connect() as conn:
        rows = conn.execute(
            """SELECT tag, COUNT(*) AS hrs FROM queue
               WHERE status='done' AND tag IS NOT NULL AND tag != ''
                 AND scheduled_ts >= ? AND scheduled_ts <= ?
               GROUP BY tag ORDER BY hrs DESC LIMIT ?""",
            (canonical_ts(start), canonical_ts(end), limit),
        ).fetchall()
    return [(r["tag"], r["hrs"]) for r in rows]


def journal_aggregates(days: int) -> dict:
    """Plan-completion + mood/energy aggregates from journal tables."""
    since = (log_today(settings.tz) - dt.timedelta(days=days - 1)).isoformat()
    with db_connect() as conn:
        day_n, mood_avg, energy_avg = conn.execute(
            """SELECT COUNT(*), ROUND(AVG(mood),1), ROUND(AVG(energy),1)
               FROM journal_days WHERE date >= ?""",
            (since,),
        ).fetchone()
        acts = conn.execute(
            """SELECT name, COUNT(*) AS planned, SUM(completed) AS done
               FROM journal_activities WHERE date >= ?
               GROUP BY name ORDER BY planned DESC LIMIT 12""",
            (since,),
        ).fetchall()
        totals = conn.execute(
            """SELECT COUNT(*), COALESCE(SUM(completed),0)
               FROM journal_activities WHERE date >= ?""",
            (since,),
        ).fetchone()
    planned_total, done_total = totals
    return {
        "journal_days": day_n,
        "mood_avg": mood_avg,
        "energy_avg": energy_avg,
        "completion_pct": round(100 * done_total / planned_total) if planned_total else None,
        "activities": [(a["name"], a["planned"], a["done"] or 0) for a in acts],
    }


def recurring_themes(days: int, limit: int = 5) -> tuple[list, list]:
    """Most repeated good / upset items across journal days (case-folded)."""
    since = (log_today(settings.tz) - dt.timedelta(days=days - 1)).isoformat()
    good: Counter = Counter()
    upset: Counter = Counter()
    with db_connect() as conn:
        for (pj,) in conn.execute(
            "SELECT parsed_json FROM journal_days WHERE date >= ?", (since,)
        ):
            rec = json.loads(pj)
            good.update(g.lower() for g in rec.get("good", []))
            upset.update(u.lower() for u in rec.get("upset", []))
    return good.most_common(limit), upset.most_common(limit)


def recent_journal_lines(days: int = 14) -> list[str]:
    """One compact line per recent journal day — the qualitative context."""
    since = (log_today(settings.tz) - dt.timedelta(days=days - 1)).isoformat()
    lines: list[str] = []
    with db_connect() as conn:
        for date, pj, mood, energy in conn.execute(
            """SELECT date, parsed_json, mood, energy FROM journal_days
               WHERE date >= ? ORDER BY date""",
            (since,),
        ):
            rec = json.loads(pj)
            planned, completed = rec.get("planned", []), set(rec.get("completed", []))
            missed = [p for p in planned if p not in completed]
            parts = [date]
            if mood or energy:
                parts.append(f"mood={mood or '?'} energy={energy or '?'}")
            if planned:
                parts.append(f"done {len(completed)}/{len(planned)}")
            if missed:
                parts.append("missed: " + ", ".join(missed[:4]))
            if rec.get("good"):
                parts.append("good: " + "; ".join(rec["good"][:3]))
            if rec.get("upset"):
                parts.append("upset: " + "; ".join(rec["upset"][:3]))
            lines.append(" | ".join(parts))
    return lines


def _fmt_hours(data: dict[str, int]) -> str:
    total = sum(data.values())
    if not total:
        return "no entries"
    parts = [
        f"{cat or 'uncategorised'} {hrs}h ({round(100 * hrs / total)}%)"
        for cat, hrs in sorted(data.items(), key=lambda kv: -kv[1])
    ]
    return f"{total}h total: " + ", ".join(parts)


def build_facts(days: int = 30) -> str:
    """The complete deterministic facts block handed to Gemini."""
    ja = journal_aggregates(days)
    good, upset = recurring_themes(days)
    sections = [
        f"TIME (hourly log, 1 row = 1 hour; day starts {settings.LOG_DAY_START_HOUR}:00 local)",
        f"last 7 days   — {_fmt_hours(category_hours(7))}",
        f"last {days} days  — {_fmt_hours(category_hours(days))}",
        f"year to date  — {_fmt_hours(category_hours((log_today(settings.tz) - dt.date(log_today(settings.tz).year, 1, 1)).days + 1))}",
        f"top activity tags (last {days}d): "
        + (", ".join(f"{t} {h}h" for t, h in top_tags(days)) or "none"),
        "",
        f"JOURNAL (last {days}d, {ja['journal_days']} days on record)",
        f"avg mood {ja['mood_avg'] or 'n/a'}, avg energy {ja['energy_avg'] or 'n/a'}, "
        f"plan-completion {str(ja['completion_pct']) + '%' if ja['completion_pct'] is not None else 'n/a'}",
        "per-activity done/planned: "
        + (", ".join(f"{n} {d}/{p}" for n, p, d in ja["activities"]) or "none"),
        "recurring good: " + (", ".join(f"{g}×{c}" for g, c in good) or "none"),
        "recurring upset: " + (", ".join(f"{u}×{c}" for u, c in upset) or "none"),
    ]
    return "\n".join(sections)
