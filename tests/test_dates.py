"""Date parsing — Bug #10 (ambiguous DD/MM)."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from hourly_logger.dates import parse_user_date, parse_user_month


TZ = ZoneInfo("UTC")


def test_iso_date() -> None:
    assert parse_user_date("2026-03-28", TZ) == dt.date(2026, 3, 28)


def test_dd_mm_uses_current_year() -> None:
    parsed = parse_user_date("28/03", TZ)
    assert parsed is not None
    assert parsed.month == 3 and parsed.day == 28
    assert parsed.year == dt.datetime.now(TZ).year


def test_dd_mm_yyyy() -> None:
    assert parse_user_date("28/03/2026", TZ) == dt.date(2026, 3, 28)


def test_today_yesterday() -> None:
    today = parse_user_date("today", TZ)
    yesterday = parse_user_date("yesterday", TZ)
    assert today is not None and yesterday is not None
    assert (today - yesterday).days == 1


def test_unrecognised_returns_none() -> None:
    assert parse_user_date("not-a-date", TZ) is None
    assert parse_user_date("13/13/2026", TZ) is None  # invalid day/month


def test_empty_returns_none() -> None:
    assert parse_user_date("", TZ) is None


def test_parse_user_month_yyyymm() -> None:
    assert parse_user_month("2026-03", TZ) == (2026, 3)


def test_parse_user_month_bare_number_uses_current_year() -> None:
    assert parse_user_month("03", TZ) == (dt.datetime.now(TZ).year, 3)


def test_parse_user_month_invalid() -> None:
    assert parse_user_month("13", TZ) is None
    assert parse_user_month("nope", TZ) is None


def test_parse_user_month_empty_returns_now() -> None:
    now = dt.datetime.now(TZ)
    assert parse_user_month("", TZ) == (now.year, now.month)
