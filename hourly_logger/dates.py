"""Date-string parsing helpers.

Bug #10 fix: ``DD/MM`` and ``DD/MM/YYYY`` are now parsed with an explicit
``dayfirst=True`` policy, documented in user-facing error messages. Other
formats (``YYYY-MM-DD``, ``today``, ``yesterday``) keep working unchanged.
The ambiguous bare ``MM/DD`` form is rejected — users should pick one of
the supported formats.
"""

from __future__ import annotations

import calendar
import datetime as dt
from datetime import datetime, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from .config import settings


SUPPORTED_DATE_FORMATS_HUMAN = (
    "`YYYY-MM-DD` (e.g. `2026-03-28`)\n"
    "`DD/MM` (e.g. `28/03`) — day/month order\n"
    "`DD/MM/YYYY` (e.g. `28/03/2026`)\n"
    "`today` / `yesterday`"
)


def parse_user_date(arg: str, tz: ZoneInfo) -> Optional[dt.date]:
    """Parse a user-supplied date string, returning ``None`` if unrecognised.

    Strict order: ``YYYY-MM-DD`` → ``DD/MM/YYYY`` → ``DD/MM``. The
    ``DD/MM`` form falls back to the current year in ``tz``.
    """
    arg = (arg or "").strip().lower()
    if not arg:
        return None
    if arg == "today":
        return datetime.now(tz).date()
    if arg == "yesterday":
        return (datetime.now(tz) - dt.timedelta(days=1)).date()

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m"):
        try:
            parsed = datetime.strptime(arg, fmt)
        except ValueError:
            continue
        if fmt == "%d/%m":
            return parsed.replace(year=datetime.now(tz).year).date()
        return parsed.date()
    return None


# ── Log-day arithmetic (Bug #4) ─────────────────────────────────────────────
#
# A "log day" is the user's mental day as represented in the Weekly grid:
# it runs from LOG_DAY_START_HOUR (default 7am) of one calendar date to
# (LOG_DAY_START_HOUR - 1):59 of the next calendar date. So at 3am on
# 2026-04-24, the *log day* is still 2026-04-23.
#
# All date-bounded user-facing queries (status/weekly/monthly/edit-by-date,
# /skipall) must use these helpers so the totals shown match the colored
# cells in the grid.


def log_day_of(local_dt: datetime) -> dt.date:
    """Return the log-day (calendar date in the grid column) for a local datetime."""
    if local_dt.hour < settings.LOG_DAY_START_HOUR:
        return (local_dt - dt.timedelta(days=1)).date()
    return local_dt.date()


def log_day_bounds(
    log_date: dt.date, tz: ZoneInfo
) -> Tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` covering the log-day ``log_date``.

    ``start`` is ``log_date`` at LOG_DAY_START_HOUR local time. ``end`` is
    one second before ``log_date + 1`` at the same hour. Both are
    UTC-aware so they can be compared against canonical ``scheduled_ts``
    strings directly.
    """
    h = settings.LOG_DAY_START_HOUR
    start_local = datetime(log_date.year, log_date.month, log_date.day, h, 0, 0, tzinfo=tz)
    end_local = start_local + dt.timedelta(days=1) - dt.timedelta(seconds=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def log_week_bounds(
    any_date_in_week: dt.date, tz: ZoneInfo
) -> Tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` for the log-week containing ``any_date_in_week``.

    The week runs Monday LOG_DAY_START_HOUR → next Monday LOG_DAY_START_HOUR - 1s.
    """
    monday = any_date_in_week - dt.timedelta(days=any_date_in_week.weekday())
    sunday = monday + dt.timedelta(days=6)
    start, _ = log_day_bounds(monday, tz)
    _, end = log_day_bounds(sunday, tz)
    return start, end


def log_month_bounds(
    year: int, month: int, tz: ZoneInfo
) -> Tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` for the log-month ``(year, month)``."""
    last_day = calendar.monthrange(year, month)[1]
    first = dt.date(year, month, 1)
    last = dt.date(year, month, last_day)
    start, _ = log_day_bounds(first, tz)
    _, end = log_day_bounds(last, tz)
    return start, end


def log_today(tz: ZoneInfo) -> dt.date:
    """The current log-day in ``tz``."""
    return log_day_of(datetime.now(tz))


def parse_user_month(arg: str, tz: ZoneInfo) -> Optional[tuple[int, int]]:
    """Parse a month argument. Returns ``(year, month)`` or ``None``.

    Accepts ``YYYY-MM``, ``MM-YYYY``, or a bare ``M`` / ``MM`` (current year).
    """
    arg = (arg or "").strip()
    if not arg:
        now = datetime.now(tz)
        return now.year, now.month
    for fmt in ("%Y-%m", "%m-%Y"):
        try:
            d = datetime.strptime(arg, fmt)
            return d.year, d.month
        except ValueError:
            pass
    try:
        m = int(arg)
        if 1 <= m <= 12:
            return datetime.now(tz).year, m
    except ValueError:
        pass
    return None
