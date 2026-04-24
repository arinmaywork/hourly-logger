"""Bug #4 — log-day arithmetic.

Verifies the helpers in :mod:`hourly_logger.dates` that translate between
clock time and the user's "log day" (the calendar column in the Weekly
grid). Hours before ``LOG_DAY_START_HOUR`` belong to the *previous*
calendar date.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from hourly_logger.config import settings
from hourly_logger.dates import (
    log_day_bounds,
    log_day_of,
    log_month_bounds,
    log_today,
    log_week_bounds,
)


UTC = ZoneInfo("UTC")
IST = ZoneInfo("Asia/Kolkata")  # UTC+5:30, no DST


# ── log_day_of ─────────────────────────────────────────────────────────────


def test_log_day_of_morning_belongs_to_same_date() -> None:
    # 9am — well after the 7am boundary → same date.
    assert log_day_of(datetime(2026, 4, 24, 9, 0, tzinfo=UTC)) == dt.date(2026, 4, 24)


def test_log_day_of_late_evening_belongs_to_same_date() -> None:
    # 11pm — same calendar date.
    assert log_day_of(datetime(2026, 4, 24, 23, 0, tzinfo=UTC)) == dt.date(2026, 4, 24)


def test_log_day_of_early_morning_belongs_to_previous_date() -> None:
    # 3am — before the 7am boundary → still yesterday's log day.
    assert log_day_of(datetime(2026, 4, 24, 3, 0, tzinfo=UTC)) == dt.date(2026, 4, 23)


def test_log_day_of_midnight_belongs_to_previous_date() -> None:
    assert log_day_of(datetime(2026, 4, 24, 0, 0, tzinfo=UTC)) == dt.date(2026, 4, 23)


def test_log_day_of_exactly_boundary_hour_belongs_to_same_date() -> None:
    # At exactly 7:00:00 the new log-day starts.
    assert log_day_of(datetime(2026, 4, 24, 7, 0, tzinfo=UTC)) == dt.date(2026, 4, 24)


def test_log_day_of_one_minute_before_boundary_belongs_to_previous() -> None:
    assert log_day_of(datetime(2026, 4, 24, 6, 59, tzinfo=UTC)) == dt.date(2026, 4, 23)


def test_log_day_of_respects_overridden_boundary_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "LOG_DAY_START_HOUR", 4)
    # Now 5am is "today", 3am is still "yesterday".
    assert log_day_of(datetime(2026, 4, 24, 5, 0, tzinfo=UTC)) == dt.date(2026, 4, 24)
    assert log_day_of(datetime(2026, 4, 24, 3, 0, tzinfo=UTC)) == dt.date(2026, 4, 23)


# ── log_day_bounds ─────────────────────────────────────────────────────────


def test_log_day_bounds_start_is_seven_am_utc() -> None:
    start, end = log_day_bounds(dt.date(2026, 4, 24), UTC)
    assert start == datetime(2026, 4, 24, 7, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 4, 25, 6, 59, 59, tzinfo=timezone.utc)


def test_log_day_bounds_returns_utc_aware_datetimes() -> None:
    start, end = log_day_bounds(dt.date(2026, 4, 24), IST)
    assert start.tzinfo is timezone.utc
    assert end.tzinfo is timezone.utc


def test_log_day_bounds_translates_local_tz_to_utc() -> None:
    # 7am IST = 01:30 UTC (IST is UTC+5:30, no DST).
    start, end = log_day_bounds(dt.date(2026, 4, 24), IST)
    assert start == datetime(2026, 4, 24, 1, 30, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 4, 25, 1, 29, 59, tzinfo=timezone.utc)


def test_log_day_bounds_respects_overridden_boundary_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "LOG_DAY_START_HOUR", 4)
    start, end = log_day_bounds(dt.date(2026, 4, 24), UTC)
    assert start == datetime(2026, 4, 24, 4, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 4, 25, 3, 59, 59, tzinfo=timezone.utc)


# ── log_week_bounds ────────────────────────────────────────────────────────


def test_log_week_bounds_anchors_on_monday() -> None:
    # 2026-04-22 is a Wednesday → week is Mon 2026-04-20 → next-Mon 2026-04-27.
    start, end = log_week_bounds(dt.date(2026, 4, 22), UTC)
    assert start == datetime(2026, 4, 20, 7, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 4, 27, 6, 59, 59, tzinfo=timezone.utc)


def test_log_week_bounds_for_a_monday_starts_that_day() -> None:
    start, _ = log_week_bounds(dt.date(2026, 4, 20), UTC)
    assert start.date() == dt.date(2026, 4, 20)


def test_log_week_bounds_for_a_sunday_uses_preceding_monday() -> None:
    start, end = log_week_bounds(dt.date(2026, 4, 26), UTC)
    assert start.date() == dt.date(2026, 4, 20)
    assert end.date() == dt.date(2026, 4, 27)


# ── log_month_bounds ───────────────────────────────────────────────────────


def test_log_month_bounds_spans_full_month() -> None:
    start, end = log_month_bounds(2026, 4, UTC)
    # April → starts 2026-04-01 7am, ends 2026-05-01 6:59:59am.
    assert start == datetime(2026, 4, 1, 7, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 1, 6, 59, 59, tzinfo=timezone.utc)


def test_log_month_bounds_february_leap_year() -> None:
    start, end = log_month_bounds(2024, 2, UTC)
    assert start == datetime(2024, 2, 1, 7, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2024, 3, 1, 6, 59, 59, tzinfo=timezone.utc)


def test_log_month_bounds_february_non_leap_year() -> None:
    start, end = log_month_bounds(2026, 2, UTC)
    assert start == datetime(2026, 2, 1, 7, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 3, 1, 6, 59, 59, tzinfo=timezone.utc)


def test_log_month_bounds_december_rolls_over() -> None:
    start, end = log_month_bounds(2026, 12, UTC)
    assert start == datetime(2026, 12, 1, 7, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2027, 1, 1, 6, 59, 59, tzinfo=timezone.utc)


# ── log_today ──────────────────────────────────────────────────────────────


def test_log_today_returns_a_date() -> None:
    today = log_today(UTC)
    assert isinstance(today, dt.date)
    # Sanity: the log-today is either today or yesterday in the same tz.
    now = datetime.now(UTC).date()
    assert today in (now, now - dt.timedelta(days=1))
