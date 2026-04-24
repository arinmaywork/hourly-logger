"""``backfill_missed_prompts_sync`` — origin selection + max-hours cap.

Bug #4 follow-up. After a fresh DB reset the original backfill bailed out
with zero rows because there was no ``MAX(scheduled_ts)`` to extrapolate
from. ``SERVICE_START_DATE`` gives us a fallback origin so historical
hours can still be surfaced as ``pending`` (and then triaged via
``/missing``). ``BACKFILL_MAX_HOURS`` caps the total insert so a
mis-configured ``SERVICE_START_DATE`` from years ago can't pin the
process.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone

import pytest

from hourly_logger import database
from hourly_logger.config import settings
from hourly_logger.database import (
    backfill_missed_prompts_sync,
    canonical_ts,
    db_init,
    queue_add_prompt_sync,
    queue_count_pending,
)


def _now_utc() -> datetime:
    """A frozen "now" so the tests are deterministic across midnight."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)


# ── No origin available ────────────────────────────────────────────────────


def test_empty_db_without_service_start_returns_zero(tmp_db_path: str) -> None:
    db_init()
    assert backfill_missed_prompts_sync(now_utc=_now_utc()) == 0
    assert queue_count_pending() == 0


# ── SERVICE_START_DATE seeds a fresh DB ─────────────────────────────────────


def test_empty_db_with_service_start_seeds_from_that_date(
    tmp_db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SERVICE_START_DATE = 4 days before ``now`` → backfill the missing hours.

    With LOG_DAY_START_HOUR=7 and tz=UTC, the first inserted hour is
    2026-04-20T07:00:00Z; the last is 2026-04-24T12:00:00Z. That's
    24*4 + 6 = 102 hourly slots.
    """
    monkeypatch.setattr(settings, "SERVICE_START_DATE", dt.date(2026, 4, 20))
    db_init()
    inserted = backfill_missed_prompts_sync(now_utc=_now_utc())
    assert inserted == 102
    assert queue_count_pending() == 102

    # Spot-check the first and last hours actually landed.
    with database.db_connect() as conn:
        first = conn.execute(
            "SELECT MIN(scheduled_ts) FROM queue"
        ).fetchone()[0]
        last = conn.execute(
            "SELECT MAX(scheduled_ts) FROM queue"
        ).fetchone()[0]
    assert first == "2026-04-20T07:00:00Z"
    assert last == "2026-04-24T12:00:00Z"


# ── DB origin vs config origin ─────────────────────────────────────────────


def test_db_max_newer_than_service_start_uses_db_max(
    tmp_db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the DB already has rows past ``SERVICE_START_DATE`` we keep moving
    forward from the DB's max — we don't re-seed ancient history."""
    monkeypatch.setattr(settings, "SERVICE_START_DATE", dt.date(2026, 1, 1))
    db_init()
    # DB already has a row at 2026-04-24T08:00 → backfill should only
    # produce 09:00, 10:00, 11:00, 12:00 (4 rows).
    queue_add_prompt_sync(datetime(2026, 4, 24, 8, 0, tzinfo=timezone.utc))
    inserted = backfill_missed_prompts_sync(now_utc=_now_utc())
    assert inserted == 4
    assert queue_count_pending() == 5  # original + 4 new


def test_service_start_newer_than_db_max_uses_service_start(
    tmp_db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SERVICE_START_DATE *after* the DB's MAX overrides the DB origin —
    useful when the operator wants to ignore stale history."""
    monkeypatch.setattr(settings, "SERVICE_START_DATE", dt.date(2026, 4, 24))
    db_init()
    queue_add_prompt_sync(datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc))
    inserted = backfill_missed_prompts_sync(now_utc=_now_utc())
    # First inserted hour is 2026-04-24T07:00, last is 2026-04-24T12:00 → 6 rows.
    assert inserted == 6
    # Ancient row + 6 new = 7 pending.
    assert queue_count_pending() == 7


# ── BACKFILL_MAX_HOURS cap ─────────────────────────────────────────────────


def test_backfill_respects_max_hours_cap(
    tmp_db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``SERVICE_START_DATE`` years in the past would otherwise insert
    tens of thousands of rows — the cap keeps a single call bounded."""
    monkeypatch.setattr(settings, "SERVICE_START_DATE", dt.date(2024, 1, 1))
    monkeypatch.setattr(settings, "BACKFILL_MAX_HOURS", 50)
    db_init()
    inserted = backfill_missed_prompts_sync(now_utc=_now_utc())
    assert inserted == 50
    assert queue_count_pending() == 50


# ── Idempotency ────────────────────────────────────────────────────────────


def test_backfill_is_idempotent(
    tmp_db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "SERVICE_START_DATE", dt.date(2026, 4, 23))
    db_init()
    first = backfill_missed_prompts_sync(now_utc=_now_utc())
    second = backfill_missed_prompts_sync(now_utc=_now_utc())
    # First call inserts everything; second call inserts nothing because
    # the UNIQUE INDEX on scheduled_ts catches every duplicate.
    assert first > 0
    assert second == 0
    assert queue_count_pending() == first


# ── Canonical format invariant ─────────────────────────────────────────────


def test_backfilled_rows_use_canonical_format(
    tmp_db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "SERVICE_START_DATE", dt.date(2026, 4, 24))
    db_init()
    backfill_missed_prompts_sync(now_utc=_now_utc())
    with database.db_connect() as conn:
        rows = [r[0] for r in conn.execute("SELECT scheduled_ts FROM queue")]
    assert rows  # sanity — at least one row was inserted
    sample = datetime(2026, 4, 24, 7, 0, tzinfo=timezone.utc)
    assert canonical_ts(sample) in rows
    for ts in rows:
        # Canonical format is always "YYYY-MM-DDTHH:MM:SSZ".
        assert ts.endswith("Z")
        assert len(ts) == 20
