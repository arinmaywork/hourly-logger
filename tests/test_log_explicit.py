"""Explicit-timestamp ``/log`` form for backfilling historical hours.

Covers the parser (date-time peeling, rounding, format flexibility),
the database helper that finds an existing row by scheduled_ts, and
the end-to-end backfill paths (insert placeholder, update existing
pending, reject already-done, reject future).
"""

from __future__ import annotations

# The mocked ``background.spawn`` swallows the coroutine without awaiting
# it; that's intentional (we test the spawn was *called*, not what it
# does), so silence the standard "coroutine was never awaited" noise.
import warnings

warnings.filterwarnings(
    "ignore",
    message="coroutine '_background_sheets_sync' was never awaited",
)

import datetime as dt
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from hourly_logger.config import settings
from hourly_logger.database import (
    canonical_ts,
    db_init,
    queue_add_prompt_sync,
    queue_get_by_scheduled_ts,
    queue_insert_done_row_sync,
)
from hourly_logger.handlers import log as log_handler
from hourly_logger.handlers.log import (
    _round_to_utc_hour,
    _try_parse_explicit_ts,
    cmd_log,
)


# ── Parser: timestamp peeling ───────────────────────────────────────────────


def test_parse_iso_single_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """``2026-04-24T10:30 c Deep Work`` peels off the ISO timestamp."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    ts, rest = _try_parse_explicit_ts("2026-04-24T10:30 c Deep Work")
    assert ts == datetime(2026, 4, 24, 5, 0, tzinfo=timezone.utc)
    assert rest == "c Deep Work"


def test_parse_two_token_date_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """``2026-04-24 10:30 c Deep Work`` peels off date+time."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    ts, rest = _try_parse_explicit_ts("2026-04-24 10:30 c Deep Work")
    assert ts == datetime(2026, 4, 24, 5, 0, tzinfo=timezone.utc)
    assert rest == "c Deep Work"


def test_parse_round_to_nearest_utc_hour(monkeypatch: pytest.MonkeyPatch) -> None:
    """``10:00 IST`` (= 04:30 UTC) snaps UP to 05:00 UTC — same slot the
    Sheet displays as 10:30 IST. So copying a /gaps line works."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    ts, _ = _try_parse_explicit_ts("2026-04-24 10:00 c Foo")
    assert ts == datetime(2026, 4, 24, 5, 0, tzinfo=timezone.utc)


def test_parse_no_timestamp_falls_through() -> None:
    """No leading date → returns ``(None, original)`` so the live path
    handles it."""
    ts, rest = _try_parse_explicit_ts("c Deep Work,, note")
    assert ts is None
    assert rest == "c Deep Work,, note"


def test_parse_accepts_single_digit_hour(monkeypatch: pytest.MonkeyPatch) -> None:
    """``2026-04-01 4:00`` (single-digit hour, copy-pasted from /gaps)."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    ts, rest = _try_parse_explicit_ts("2026-04-01 4:00 h Sleep")
    # 04:00 IST = 22:30 UTC March 31 → snap to 23:00 UTC March 31.
    assert ts == datetime(2026, 3, 31, 23, 0, tzinfo=timezone.utc)
    assert rest == "h Sleep"


def test_parse_invalid_date_falls_through() -> None:
    """Garbage like ``2026-13-99`` doesn't crash — it falls through."""
    ts, rest = _try_parse_explicit_ts("2026-13-99 25:99 c Foo")
    assert ts is None
    assert rest == "2026-13-99 25:99 c Foo"


def test_round_to_utc_hour_half_up() -> None:
    """``_round_to_utc_hour`` is half-up at the 30-minute mark."""
    assert _round_to_utc_hour(
        datetime(2026, 4, 24, 4, 30, tzinfo=timezone.utc)
    ) == datetime(2026, 4, 24, 5, 0, tzinfo=timezone.utc)
    assert _round_to_utc_hour(
        datetime(2026, 4, 24, 4, 29, tzinfo=timezone.utc)
    ) == datetime(2026, 4, 24, 4, 0, tzinfo=timezone.utc)
    assert _round_to_utc_hour(
        datetime(2026, 4, 24, 4, 0, tzinfo=timezone.utc)
    ) == datetime(2026, 4, 24, 4, 0, tzinfo=timezone.utc)


# ── DB helper: queue_get_by_scheduled_ts ────────────────────────────────────


def test_get_by_scheduled_ts_finds_pending_row(tmp_db_path: str) -> None:
    db_init()
    ts = datetime(2026, 4, 24, 5, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(ts)
    row = queue_get_by_scheduled_ts(ts)
    assert row is not None
    assert row["status"] == "pending"
    assert row["scheduled_ts"] == "2026-04-24T05:00:00Z"


def test_get_by_scheduled_ts_returns_none_when_missing(tmp_db_path: str) -> None:
    db_init()
    ts = datetime(2026, 4, 24, 5, 0, tzinfo=timezone.utc)
    assert queue_get_by_scheduled_ts(ts) is None


# ── End-to-end backfill paths ──────────────────────────────────────────────


def _make_update(text: str) -> tuple[MagicMock, MagicMock]:
    """Build a minimal mock Update + Context that cmd_log will accept."""
    update = MagicMock()
    update.effective_chat.id = settings.CHAT_ID  # is_owner() reads this
    update.message = MagicMock()
    update.message.reply_text = AsyncMock()
    context = MagicMock()
    context.bot = MagicMock()
    context.args = text.split()
    return update, context


@pytest.mark.asyncio
async def test_backfill_inserts_placeholder_when_slot_missing(
    tmp_db_path: str, fresh_session: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bot was offline at the top of that hour → no row exists → backfill
    creates the pending placeholder, marks done, and surfaces a Sheet sync."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    db_init()

    # Choose a past hour so the future-rejection branch doesn't trip.
    one_hour_ago = (
        datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=2)
    )
    sheet_ts_label = one_hour_ago.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")

    update, context = _make_update(
        f"{sheet_ts_label.replace(' ', 'T')} c Backfilled"
    )

    with patch("hourly_logger.handlers.log.background.spawn"):
        await cmd_log(update, context)

    # The row exists, is done, and has the right metadata.
    row = queue_get_by_scheduled_ts(one_hour_ago)
    assert row is not None
    assert row["status"] == "done"
    assert row["category"] == "🟢 Creative"
    assert row["tag"] == "Backfilled"
    assert row["sheets_synced"] == 0  # background task hasn't run yet


@pytest.mark.asyncio
async def test_backfill_marks_existing_pending_row_done(
    tmp_db_path: str, fresh_session: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Slot exists as pending (e.g. backfill_missed_prompts caught it on
    the next tick) → mark it done in place, no second insert."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    db_init()

    sched = (
        datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=3)
    )
    queue_add_prompt_sync(sched)

    label = sched.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")
    update, context = _make_update(f"{label} h Sleep,, 7 hrs")

    with patch("hourly_logger.handlers.log.background.spawn"):
        await cmd_log(update, context)

    row = queue_get_by_scheduled_ts(sched)
    assert row is not None
    assert row["status"] == "done"
    assert row["category"] == "💎 Health"
    assert row["tag"] == "Sleep"
    assert row["note"] == "7 hrs"


@pytest.mark.asyncio
async def test_backfill_rejects_already_done_slot(
    tmp_db_path: str, fresh_session: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Slot already done → reject and suggest /edit (no overwrite)."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    db_init()

    sched = (
        datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=4)
    )
    queue_insert_done_row_sync(
        sched, sched, "🟢 Creative", "Original", "", sheets_synced=True,
    )

    label = sched.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")
    update, context = _make_update(f"{label} c Overwrite")

    with patch("hourly_logger.handlers.log.background.spawn") as spawn:
        await cmd_log(update, context)

    # Original row untouched.
    row = queue_get_by_scheduled_ts(sched)
    assert row["tag"] == "Original"
    spawn.assert_not_called()
    # User got an /edit nudge.
    msg = update.message.reply_text.await_args.args[0]
    assert "/edit" in msg


@pytest.mark.asyncio
async def test_backfill_rejects_future_slot(
    tmp_db_path: str, fresh_session: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Future hour → reject (no row insert, no Sheet write)."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    db_init()

    future = datetime.now(timezone.utc) + timedelta(hours=2)
    label = future.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")
    update, context = _make_update(f"{label} c Foo")

    with patch("hourly_logger.handlers.log.background.spawn") as spawn:
        await cmd_log(update, context)

    spawn.assert_not_called()
    msg = update.message.reply_text.await_args.args[0]
    assert "future" in msg.lower()


@pytest.mark.asyncio
async def test_backfill_works_even_when_session_not_idle(
    tmp_db_path: str, fresh_session: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mid-entry conflict only matters for the live path. Backfill
    targets a specific historical hour, doesn't touch live session state."""
    # ``tz`` is a computed property; patch the source field instead.
    monkeypatch.setattr(settings, "TIMEZONE", "Asia/Kolkata")
    db_init()

    # Force ``session.is_idle`` to False without touching the lock-protected
    # internals (the in-flight prompt object is a tagged dataclass we'd
    # have to fully construct).
    from hourly_logger import state
    monkeypatch.setattr(
        type(state.session), "is_idle", property(lambda self: False)
    )
    monkeypatch.setattr(
        type(log_handler.session), "is_idle", property(lambda self: False)
    )

    sched = (
        datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=5)
    )
    label = sched.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")
    update, context = _make_update(f"{label} p Tasks")

    with patch("hourly_logger.handlers.log.background.spawn"):
        await cmd_log(update, context)

    row = queue_get_by_scheduled_ts(sched)
    assert row is not None
    assert row["status"] == "done"
