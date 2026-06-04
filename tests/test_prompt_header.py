"""Rich hourly-prompt header — year progress + YTD category breakdown.

The header is built fresh on every prompt; these tests pin its
contract so a future refactor can't silently change the user-facing
shape (week count, bar width, category ordering, unlogged gap, etc.).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from hourly_logger.config import settings
from hourly_logger.database import (
    canonical_ts,
    db_init,
    queue_category_hours_in_window,
    queue_insert_done_row_sync,
)
from hourly_logger.handlers.flow import _bar, _build_year_header


# ── DB helper: per-category hour totals ────────────────────────────────────


def test_category_hours_aggregates_done_rows_by_category(tmp_db_path: str) -> None:
    """Done rows roll up; pending/skipped rows are excluded."""
    db_init()
    base = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        (0, "🟢 Creative"),
        (1, "🟢 Creative"),
        (2, "💎 Health"),
        (3, "🔘 Professional"),
    ]
    for h, cat in rows:
        sched = base.replace(hour=h)
        queue_insert_done_row_sync(sched, sched, cat, "T", "", sheets_synced=True)

    totals = queue_category_hours_in_window(base, base.replace(hour=23))
    assert totals == {"🟢 Creative": 2, "💎 Health": 1, "🔘 Professional": 1}


def test_category_hours_buckets_null_under_empty_string(tmp_db_path: str) -> None:
    """Legacy NULL-category rows shouldn't be silently dropped — they
    bucket under "" so the caller can choose how to surface them."""
    db_init()
    sched = datetime(2026, 4, 1, 5, 0, tzinfo=timezone.utc)
    # Insert with explicit None category by going through raw SQL.
    from hourly_logger.database import db_connect
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO queue (scheduled_ts, submitted_ts, status, "
            "category, tag, note, sheets_synced) "
            "VALUES (?,?,?,?,?,?,?)",
            (canonical_ts(sched), canonical_ts(sched), "done",
             None, "T", "", 1),
        )

    totals = queue_category_hours_in_window(sched, sched)
    assert totals == {"": 1}


def test_category_hours_respects_window_bounds(tmp_db_path: str) -> None:
    db_init()
    inside = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc)
    outside = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
    for ts in (inside, outside):
        queue_insert_done_row_sync(ts, ts, "🟢 Creative", "T", "", sheets_synced=True)
    totals = queue_category_hours_in_window(
        datetime(2026, 4, 1, tzinfo=timezone.utc),
        datetime(2026, 4, 30, 23, tzinfo=timezone.utc),
    )
    assert totals == {"🟢 Creative": 1}


# ── _bar helper ────────────────────────────────────────────────────────────


def test_bar_basic_proportions() -> None:
    # Default fill/empty switched to ▰/▱ for the aesthetic pass.
    assert _bar(0, 10) == "▱" * 10
    assert _bar(10, 10) == "▰" * 10
    assert _bar(3, 10) == "▰▰▰▱▱▱▱▱▱▱"


def test_bar_clamps_out_of_range() -> None:
    """Negative or oversize fill never produces a malformed bar."""
    assert _bar(-1, 10) == "▱" * 10
    assert _bar(99, 10) == "▰" * 10


def test_bar_accepts_custom_chars() -> None:
    """Caller can override fill/empty (kept open for future themes
    or for the year bar to diverge from the per-category bars)."""
    assert _bar(2, 5, fill="█", empty="░") == "██░░░"


# ── _build_year_header (integration) ───────────────────────────────────────


@pytest.fixture
def header_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set IST so the user-facing format matches production."""
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")


def test_header_contains_year_week_and_bar(
    tmp_db_path: str, header_env: None
) -> None:
    """The structural elements the user explicitly asked for:
    weeks-remaining count and the year-completion bar."""
    db_init()
    # Mid-year — June 3, 2026 in IST.
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    with patch(
        "hourly_logger.handlers.flow.queue_category_hours_in_window",
        return_value={"🟢 Creative": 100},
    ):
        out = _build_year_header(now)
    assert "2026" in out
    assert "weeks left" in out
    # Year bar — solid + empty blocks in a fixed-width span.
    assert "`" in out and "▰" in out and "▱" in out


def test_header_shows_every_category_in_canonical_order(
    tmp_db_path: str, header_env: None
) -> None:
    """All 5 category emoji-rows appear, in the same order the
    keyboard uses (CATEGORY_ORDER), so the eye-to-keyboard mapping
    stays stable. Each row is identified by its leading-emoji + bar
    pattern, since the descriptive name was dropped on purpose."""
    db_init()
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    fake = {
        "🟢 Creative": 100,
        "💎 Health": 50,
        "🔘 Professional": 200,
        "🟡 Social": 30,
        "⚪️ Other": 20,
    }
    with patch(
        "hourly_logger.handlers.flow.queue_category_hours_in_window",
        return_value=fake,
    ):
        out = _build_year_header(now)

    # Each per-category row starts with the emoji followed by a
    # backtick-wrapped bar — unique enough that .find() pins the row.
    creative = out.find("🟢 `")
    health = out.find("💎 `")
    prof = out.find("🔘 `")
    social = out.find("🟡 `")
    other = out.find("⚪️ `")
    assert -1 < creative < health < prof < social < other


def test_header_surfaces_unlogged_gap(
    tmp_db_path: str, header_env: None
) -> None:
    """Hours elapsed but unlogged appear as an explicit ⚫ row so the
    user can see at a glance how much of the year is unaccounted for."""
    db_init()
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    # Tiny logged total → big unlogged gap.
    with patch(
        "hourly_logger.handlers.flow.queue_category_hours_in_window",
        return_value={"🟢 Creative": 10},
    ):
        out = _build_year_header(now)
    # The unlogged row is identified by its ⚫ emoji + backtick-bar
    # prefix — the descriptive "Unlogged" label was dropped along
    # with the other category names.
    assert "⚫ `" in out


def test_header_omits_unlogged_when_fully_covered(
    tmp_db_path: str, header_env: None
) -> None:
    """If hours_logged ≥ hours_elapsed (e.g. backfill caught up
    perfectly), the Unlogged row is suppressed — no zero-share noise."""
    db_init()
    now_utc = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    # Pretend everything since year-start is already logged.
    # log_day_bounds(Jan 1, IST) = Jan 1 01:30 UTC (07:00 IST). Elapsed
    # at 08:00 UTC = (8 - 1.5) = 6.5h → clamped to 6h.
    with patch(
        "hourly_logger.handlers.flow.queue_category_hours_in_window",
        return_value={"🟢 Creative": 999},
    ):
        out = _build_year_header(now_utc)
    assert "⚫" not in out  # the unlogged-gap row is suppressed


def test_header_handles_first_hour_of_year_no_zero_division(
    tmp_db_path: str, header_env: None
) -> None:
    """The very first prompt of the year — hours_elapsed could be 0
    before clamping. Must not raise ZeroDivisionError."""
    db_init()
    # 1 second into the log-day on Jan 1.
    from hourly_logger.dates import log_day_bounds
    import datetime as _dt
    year_start_utc, _ = log_day_bounds(_dt.date(2026, 1, 1), settings.tz)
    now = year_start_utc + _dt.timedelta(seconds=1)
    with patch(
        "hourly_logger.handlers.flow.queue_category_hours_in_window",
        return_value={},
    ):
        out = _build_year_header(now)
    # Surviving anchor text after the wording trim — "h logged · "
    # is unique to the second line of the header.
    assert "h logged ·" in out


def test_header_weeks_remaining_decreases_through_year(
    tmp_db_path: str, header_env: None
) -> None:
    """Sanity check the weeks-left math: January should report ~52,
    December ~0."""
    db_init()
    jan = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    dec = datetime(2026, 12, 28, 12, 0, tzinfo=timezone.utc)
    with patch(
        "hourly_logger.handlers.flow.queue_category_hours_in_window",
        return_value={},
    ):
        jan_out = _build_year_header(jan)
        dec_out = _build_year_header(dec)

    import re
    jan_weeks = int(re.search(r"\*(\d+)\* weeks left", jan_out).group(1))
    dec_weeks = int(re.search(r"\*(\d+)\* weeks left", dec_out).group(1))
    assert jan_weeks >= 50
    assert dec_weeks <= 1
