"""Read-only reporting commands: /status, /monthly, /weekly, /trend."""

from __future__ import annotations

import calendar
import datetime as dt
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ContextTypes

from .. import sheets
from ..colors import CATEGORY_ORDER
from ..config import settings
from ..database import queue_status_counts
from ..dates import (
    log_day_bounds,
    log_month_bounds,
    log_today,
    log_week_bounds,
    parse_user_date,
    parse_user_month,
)
from ..logger import get_logger
from ._common import is_owner


log = get_logger(__name__)


def format_breakdown(data: dict[str, int], total_done: int) -> str:
    if total_done == 0:
        return "_No entries yet._"

    bar_width = 10
    uncategorised = data.get("_uncategorised", 0)
    cat_data = {k: v for k, v in data.items() if k != "_uncategorised"}

    all_items: dict[str, int] = dict(cat_data)
    if uncategorised:
        all_items["⚠️ Uncategorised"] = uncategorised

    exact = {cat: cnt / total_done * 100 for cat, cnt in all_items.items()}
    floored = {cat: int(p) for cat, p in exact.items()}
    deficit = 100 - sum(floored.values())
    for cat in sorted(exact, key=lambda c: -(exact[c] % 1))[:deficit]:
        floored[cat] += 1

    lines: list[str] = []
    for cat, count in cat_data.items():
        pct = floored[cat]
        filled = round(pct / 100 * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        lines.append(f"{cat}\n  `{bar}` {pct}% ({count}h)")

    if uncategorised:
        pct = floored["⚠️ Uncategorised"]
        lines.append(f"_⚠️ {uncategorised}h unmatched colour ({pct}%)_")

    return "\n".join(lines)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    counts = queue_status_counts()

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(settings.tz)

    # Bug #4: anchor week + year on the log-day boundary, not midnight,
    # so the totals shown match the colored cells in the Weekly grid.
    today_log = log_today(settings.tz)
    week_start_utc, _ = log_week_bounds(today_log, settings.tz)
    monday_log = today_log - dt.timedelta(days=today_log.weekday())
    year_start_utc, _ = log_day_bounds(dt.date(today_log.year, 1, 1), settings.tz)
    week_start_local = week_start_utc.astimezone(settings.tz)
    year_start_local = year_start_utc.astimezone(settings.tz)

    week_data, total_week = await sheets.log_breakdown(week_start_local, now_local)
    year_data, total_year = await sheets.log_breakdown(year_start_local, now_local)

    week_label = (
        f"Mon {monday_log.day} {monday_log.strftime('%b')} — now"
        f" ({total_week}h)"
    )
    year_label = f"1 Jan {today_log.year} — now ({total_year}h)"

    await update.message.reply_text(
        f"📊 *Queue Status*\n"
        f"• Pending:   `{counts['pending']}`\n"
        f"• Completed: `{counts['done']}`\n"
        f"• Skipped:   `{counts['skipped']}`\n"
        f"• Unsynced:  `{counts['unsynced']}`\n\n"
        f"📅 *This Week* — _{week_label}_\n"
        f"{format_breakdown(week_data, total_week)}\n\n"
        f"📆 *This Year* — _{year_label}_\n"
        f"{format_breakdown(year_data, total_year)}",
        parse_mode="Markdown",
    )


async def cmd_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    now_local = datetime.now(timezone.utc).astimezone(settings.tz)
    arg = " ".join(context.args).strip() if context.args else ""
    parsed = parse_user_month(arg, settings.tz)
    if parsed is None:
        await update.message.reply_text(
            "⚠️ Unrecognised month. Try:\n"
            "`/monthly` — current month\n"
            "`/monthly 2026-03` — March 2026\n"
            "`/monthly 03` — month 3 of this year",
            parse_mode="Markdown",
        )
        return
    year, month = parsed

    # Bug #4: month bounds now hug the log-day boundaries, so the month's
    # totals match the cells the user can see in the Weekly grid.
    start_utc, end_utc = log_month_bounds(year, month, settings.tz)
    since_local = start_utc.astimezone(settings.tz)
    until_local = end_utc.astimezone(settings.tz)
    today_log = log_today(settings.tz)
    if year == today_log.year and month == today_log.month:
        until_local = now_local

    data, total = await sheets.log_breakdown(since_local, until_local)
    month_name = since_local.strftime("%B %Y")
    until_label = "now" if until_local == now_local else until_local.strftime("%d %b")
    header = f"📅 *{month_name}* — _1–{until_label} ({total}h)_"

    await update.message.reply_text(
        f"{header}\n\n{format_breakdown(data, total)}",
        parse_mode="Markdown",
    )


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    now_local = datetime.now(timezone.utc).astimezone(settings.tz)
    arg = " ".join(context.args).strip() if context.args else ""
    target_date = parse_user_date(arg, settings.tz) if arg else now_local.date()
    if target_date is None:
        await update.message.reply_text(
            "⚠️ Unrecognised date. Try:\n"
            "`/weekly` — current week\n"
            "`/weekly 2026-03-28`\n"
            "`/weekly 28/03`",
            parse_mode="Markdown",
        )
        return

    mon = target_date - dt.timedelta(days=target_date.weekday())
    sun = mon + dt.timedelta(days=6)
    # Bug #4: log-week bounds (Mon 7am → next-Mon 6:59am) instead of
    # midnight-to-midnight, matching the grid columns.
    start_utc, end_utc = log_week_bounds(target_date, settings.tz)
    since_local = start_utc.astimezone(settings.tz)
    until_local = end_utc.astimezone(settings.tz)
    today_log = log_today(settings.tz)
    is_current = mon <= today_log <= sun
    if is_current:
        until_local = now_local

    data, total = await sheets.log_breakdown(since_local, until_local)
    until_label = "now" if is_current else sun.strftime("%d %b")
    header = (
        f"📅 *Week of {mon.strftime('%d %b %Y')}* "
        f"— _{mon.strftime('%d %b')}–{until_label} ({total}h)_"
    )
    await update.message.reply_text(
        f"{header}\n\n{format_breakdown(data, total)}",
        parse_mode="Markdown",
    )


async def cmd_trend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    now_local = datetime.now(timezone.utc).astimezone(settings.tz)
    args = context.args if context.args else []
    mode = args[0].lower() if args else "monthly"
    if mode not in ("monthly", "weekly"):
        await update.message.reply_text(
            "Usage: `/trend monthly [YYYY]` or `/trend weekly [YYYY-MM]`",
            parse_mode="Markdown",
        )
        return

    cat_icon = {c: c.split()[0] for c in CATEGORY_ORDER}
    periods: list[tuple[str, datetime, datetime, bool]] = []

    today_log = log_today(settings.tz)
    if mode == "monthly":
        year_arg = int(args[1]) if len(args) > 1 and args[1].isdigit() else today_log.year
        for m in range(1, 13):
            start_utc, end_utc = log_month_bounds(year_arg, m, settings.tz)
            first = start_utc.astimezone(settings.tz)
            until = end_utc.astimezone(settings.tz)
            if first > now_local:
                break
            current = (year_arg == today_log.year and m == today_log.month)
            if current:
                until = now_local
            periods.append((first.strftime("%b %Y"), first, until, current))
        title = f"📈 *Monthly Trend — {year_arg}*"
    else:  # weekly
        if len(args) > 1:
            try:
                ref = datetime.strptime(args[1], "%Y-%m")
                ref_year, ref_month = ref.year, ref.month
            except ValueError:
                await update.message.reply_text(
                    "⚠️ Use `/trend weekly YYYY-MM` e.g. `/trend weekly 2026-03`",
                    parse_mode="Markdown",
                )
                return
        else:
            ref_year, ref_month = today_log.year, today_log.month

        first_of_month = dt.date(ref_year, ref_month, 1)
        last_day_num = calendar.monthrange(ref_year, ref_month)[1]
        last_of_month = dt.date(ref_year, ref_month, last_day_num)
        mon = first_of_month - dt.timedelta(days=first_of_month.weekday())

        while mon <= last_of_month:
            sun = mon + dt.timedelta(days=6)
            start_utc, end_utc = log_week_bounds(mon, settings.tz)
            since = start_utc.astimezone(settings.tz)
            until = end_utc.astimezone(settings.tz)
            current = (mon <= today_log <= sun)
            if current:
                until = now_local
            label = f"{mon.strftime('%d %b')}–{sun.strftime('%d %b')}"
            periods.append((label, since, until, current))
            mon += dt.timedelta(days=7)

        title = f"📈 *Weekly Trend — {datetime(ref_year, ref_month, 1).strftime('%B %Y')}*"

    if not periods:
        await update.message.reply_text("No periods to show.")
        return

    await update.message.reply_text(f"⏳ Loading trend data for {len(periods)} periods…")
    entries = await sheets.log_raw(periods[0][1], periods[-1][2])

    def count_period(since: datetime, until: datetime) -> tuple[dict[str, int], int]:
        """Return (per-category counts, uncategorised count). Rows whose
        category is blank or doesn't match CATEGORY_ORDER are surfaced as
        'uncategorised' so the period total matches /auditlog instead of
        silently swallowing the discrepancy (see /trend → 743 vs Sheet 744)."""
        counts: dict[str, int] = {}
        uncat = 0
        for sched_dt, cat in entries:
            if not (since <= sched_dt <= until):
                continue
            if cat in CATEGORY_ORDER:
                counts[cat] = counts.get(cat, 0) + 1
            else:
                uncat += 1
        return counts, uncat

    lines = [title, ""]
    any_uncat = False
    for label, since, until, is_current in periods:
        counts, uncat = count_period(since, until)
        total = sum(counts.values()) + uncat
        if total == 0:
            continue
        marker = "✦" if is_current else " "
        cat_parts = " ".join(f"{cat_icon[c]}{counts.get(c, 0)}" for c in CATEGORY_ORDER)
        if uncat:
            cat_parts += f" ⚠️{uncat}"
            any_uncat = True
        lines.append(f"`{marker}{label:<14}` {cat_parts}  *{total}h*")

    lines += [
        "",
        "_" + " · ".join(f"{cat_icon[c]} {c.split()[-1]}" for c in CATEGORY_ORDER) + "_",
    ]
    if any_uncat:
        lines.append("_⚠️ = uncategorised rows in the Sheet — try /fixcats_")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
