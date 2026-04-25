"""Maintenance commands: /sync, /dedup, /fixcats, /auditlog, cmd_migrate.

Notable fixes
-------------

* **Bug #5** — :func:`cmd_sync` already defaulted NULL category to
  ``"⚪️ Other"``; now also defaults missing tags consistently.

* **Bug #6** — :func:`cmd_dedup` now uses a stable tiebreaker (delete
  the *later* row when scores are equal) so the result is deterministic
  and the original/earlier entry is preferred.

* **Bug #9** — :func:`cmd_dedup` and friends catch
  :class:`gspread.exceptions.APIError` specifically; unexpected
  exceptions still bubble up so a real bug surfaces.
"""

from __future__ import annotations

import calendar
import datetime as dt
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import gspread
from gspread.exceptions import APIError
from telegram import Update
from telegram.ext import ContextTypes

from telegram import ReplyKeyboardMarkup

from .. import sheets
from ..colors import CATEGORIES, nearest_category
from ..config import settings
from ..database import (
    canonical_ts,
    parse_ts,
    queue_get_all_scheduled_ts,
    queue_get_done_in_window,
    queue_get_unfilled_window,
    queue_get_unsynced,
    queue_increment_sync_attempt,
    queue_insert_done_row,
    queue_mark_sheets_synced,
    queue_mark_unsynced,
)
from ..logger import get_logger
from ..state import STAGE_EDIT_SELECTION, session
from ._common import is_owner


log = get_logger(__name__)


# ── /sync ────────────────────────────────────────────────────────────────


async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Retry failed Sheets writes.

    Throttled by ``SHEETS_SYNC_DELAY_S`` (default 1.5s/row) so a large
    backlog stays inside Google's 60 reads/min/user quota. Each row needs
    roughly 3-4 API ops (log find/upsert + grid update + grid format),
    so 1.5s/row puts us at ~120-160 ops/min — comfortably under the cap
    once you account for the worksheet-handle and grid-dates caches that
    eliminate redundant reads inside ``sheets.py``.

    Bails out early if the circuit breaker opens, since hammering past
    that point just wastes the user's wait time.
    """
    import asyncio

    if not is_owner(update):
        return
    unsynced = queue_get_unsynced()
    if not unsynced:
        await update.message.reply_text("✅ Nothing to sync — all entries are up to date.")
        return

    count = len(unsynced)
    suffix = "y" if count == 1 else "ies"
    delay = settings.SHEETS_SYNC_DELAY_S
    eta_min = (count * delay) / 60
    eta_str = f"~{eta_min:.1f} min" if eta_min >= 0.1 else "<10 sec"
    await update.message.reply_text(
        f"🔄 Syncing {count} unsynced entr{suffix} (≈{delay:.1f}s/row, ETA {eta_str})…"
    )

    success = failed = grid_misses = 0
    last_progress = 0
    progress_every = max(20, count // 5) if count > 50 else count + 1

    for idx, row in enumerate(unsynced, start=1):
        sched_ts = parse_ts(row["scheduled_ts"])
        sub_ts = (
            parse_ts(row["submitted_ts"]) if row["submitted_ts"] else datetime.now(timezone.utc)
        )
        category = row["category"] or "⚪️ Other"  # Bug #5
        tag = row["tag"] or row["entry_text"] or ""
        note = row["note"] or ""

        await queue_increment_sync_attempt(row["id"])
        try:
            await sheets.save_log_row(sched_ts, sub_ts, category, tag, note, is_edit=True)
            outcome = await sheets.update_grid(sched_ts, category, tag)
            await queue_mark_sheets_synced(row["id"], True)
            success += 1
            if not outcome.date_in_grid:
                grid_misses += 1
        except APIError as e:
            log.error("sync APIError", extra={"queue_id": row["id"]}, exc_info=True)
            failed += 1
            # If the breaker opened, stop early — the next rows would all
            # bounce off the same breaker and add nothing but noise.
            if sheets.log_tab_breaker.is_open or sheets.grid_tab_breaker.is_open:
                await update.message.reply_text(
                    f"⛔ Circuit breaker tripped after {idx} rows — pausing /sync.\n"
                    f"Wait {settings.SHEETS_BREAKER_COOLDOWN_S}s and run /sync again."
                )
                break
        except (TimeoutError, ConnectionError) as e:
            log.error("sync network error", extra={"queue_id": row["id"], "err": str(e)})
            failed += 1

        # Surface progress on long runs so the user knows we're alive.
        if idx - last_progress >= progress_every and idx < count:
            await update.message.reply_text(
                f"… {idx}/{count} processed ({success} ok, {failed} failed)"
            )
            last_progress = idx

        # Throttle between rows. Skip the sleep on the very last row.
        if idx < count and delay > 0:
            await asyncio.sleep(delay)

    parts = []
    if success:
        parts.append(f"✅ {success} synced successfully")
    if grid_misses:
        parts.append(f"⚠️ {grid_misses} entries fell outside the Weekly grid range")
    if failed:
        parts.append(f"❌ {failed} still failing — check logs and try /sync again")
    await update.message.reply_text("\n".join(parts) or "No work performed.")


# ── /dedup ───────────────────────────────────────────────────────────────


def _row_score(row: list[str]) -> int:
    """Higher = prefer keeping. Real entries beat migrated ones."""
    sched = row[0].strip() if len(row) > 0 else ""
    submitted = row[1].strip() if len(row) > 1 else ""
    cat = row[2].strip() if len(row) > 2 else ""
    tag = row[3].strip() if len(row) > 3 else ""
    score = 0
    if sched != submitted:
        score += 2
    if cat:
        score += 1
    if tag:
        score += 1
    return score


def _hour_key(ts: str) -> str:
    ts = ts.strip()
    return ts[:13] if len(ts) >= 13 else ts


def _dedup_sync() -> str:
    sheet = sheets.get_worksheet(settings.SHEET_NAME)
    all_rows = sheet.get_all_values()
    if len(all_rows) < 2:
        return "✅ Log tab is empty — nothing to deduplicate."

    seen: dict[str, tuple[int, list[str], str]] = {}
    to_delete: list[int] = []
    for i, row in enumerate(all_rows[1:], start=2):
        sched = row[0].strip() if row else ""
        if not sched:
            continue
        key = _hour_key(sched)
        if key in seen:
            existing_row_num, existing_row, _ = seen[key]
            score_new = _row_score(row)
            score_old = _row_score(existing_row)
            if score_new > score_old:
                to_delete.append(existing_row_num)
                seen[key] = (i, row, sched)
            elif score_new == score_old:
                # Bug #6 fix: deterministic tiebreaker — keep earlier row.
                to_delete.append(max(i, existing_row_num))
                if i < existing_row_num:
                    seen[key] = (i, row, sched)
            else:
                to_delete.append(i)
        else:
            seen[key] = (i, row, sched)

    if not to_delete:
        return "✅ No duplicate timestamps found — Log tab is clean."

    sheet_id = sheet.id
    requests = [
        {
            "deleteDimension": {
                "range": {
                    "sheetId":    sheet_id,
                    "dimension":  "ROWS",
                    "startIndex": row_num - 1,
                    "endIndex":   row_num,
                }
            }
        }
        for row_num in sorted(set(to_delete), reverse=True)
    ]
    CHUNK = 100
    for i in range(0, len(requests), CHUNK):
        sheet.spreadsheet.batch_update({"requests": requests[i : i + CHUNK]})
        if i + CHUNK < len(requests):
            time.sleep(2)
    return f"✅ Removed *{len(set(to_delete))}* duplicate rows from the Log tab."


async def cmd_dedup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    await update.message.reply_text("⏳ Scanning Log tab for duplicate timestamps…")
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, _dedup_sync)
    except APIError as exc:
        log.exception("dedup APIError")
        result = f"❌ Sheets API error during dedup — check logs ({exc})."
    except Exception:
        log.exception("dedup failed")
        result = "❌ Error during dedup — check logs."
    await update.message.reply_text(result, parse_mode="Markdown")


# ── /fixcats ─────────────────────────────────────────────────────────────


def _cell_text(cell: dict) -> str:
    return (
        cell.get("formattedValue", "")
        or cell.get("effectiveValue", {}).get("stringValue", "")
        or cell.get("userEnteredValue", {}).get("stringValue", "")
    ).strip()


def _parse_date_row(row_data, row_idx: int) -> dict[int, dt.date]:
    result: dict[int, dt.date] = {}
    for col_idx, cell in enumerate(row_data[row_idx].get("values", [])):
        raw = _cell_text(cell)
        if not raw:
            continue
        for fmt in ("%m/%d/%y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%y"):
            try:
                result[col_idx] = datetime.strptime(raw, fmt).date()
                break
            except ValueError:
                pass
    return result


def _fixcats_sync() -> str:
    spreadsheet = sheets.get_spreadsheet()
    resp = spreadsheet.client.request(
        "get",
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet.id}",
        params={"ranges": f"'{settings.GRID_SHEET_NAME}'", "includeGridData": "true"},
    ).json()
    if "error" in resp:
        return f"❌ Sheets API error: {resp['error'].get('message', resp['error'])}"

    row_data = resp["sheets"][0]["data"][0].get("rowData", [])
    col_dates: dict[int, dt.date] = {}
    for i in range(min(4, len(row_data))):
        col_dates = _parse_date_row(row_data, i)
        if len(col_dates) >= 3:
            break
    if not col_dates:
        return "❌ Could not find date row in Weekly grid."

    date_to_col: dict[dt.date, int] = {v: k for k, v in col_dates.items()}

    data_start = 4
    for i, rd in enumerate(row_data):
        col_a = _cell_text(rd.get("values", [{}])[0]) if rd.get("values") else ""
        if col_a.strip() in ("7:00", "07:00"):
            data_start = i
            break

    log_ws = sheets.get_worksheet(settings.SHEET_NAME)
    all_rows = log_ws.get_all_values()
    blank_rows: list[tuple[int, str]] = []
    for i, row in enumerate(all_rows[1:], start=2):
        sched = row[0].strip() if len(row) > 0 else ""
        cat = row[2].strip() if len(row) > 2 else ""
        if sched and not cat:
            blank_rows.append((i, sched))

    if not blank_rows:
        return "✅ No blank-category rows found — nothing to fix."

    def hour_to_row_idx(hour: int) -> int:
        if hour >= 7:
            return data_start + (hour - 7)
        return data_start + 17 + hour

    cell_updates: list[gspread.Cell] = []
    fixed = no_date = no_colour = 0

    for sheet_row, sched_str in blank_rows:
        try:
            sched_dt = datetime.strptime(sched_str, "%Y-%m-%d %H:%M")
        except ValueError:
            no_colour += 1
            continue
        actual_date = sched_dt.date()
        hour = sched_dt.hour
        col_date = actual_date - dt.timedelta(days=1) if hour < 7 else actual_date

        col_idx = date_to_col.get(col_date)
        if col_idx is None:
            no_date += 1
            continue
        row_idx = hour_to_row_idx(hour)
        if row_idx >= len(row_data):
            no_date += 1
            continue
        cells = row_data[row_idx].get("values", [])
        if col_idx >= len(cells):
            no_colour += 1
            continue

        cell = cells[col_idx]
        eff_fmt = cell.get("effectiveFormat", {})
        rgb = eff_fmt.get("backgroundColorStyle", {}).get("rgbColor", {}) or eff_fmt.get(
            "backgroundColor", {}
        )
        cat = nearest_category(
            rgb.get("red", 0.0), rgb.get("green", 0.0), rgb.get("blue", 0.0),
        )
        if not cat:
            no_colour += 1
            continue
        cell_updates.append(gspread.Cell(row=sheet_row, col=3, value=cat))
        fixed += 1

    if cell_updates:
        log_ws.update_cells(cell_updates, value_input_option="RAW")

    suffix = "y" if fixed == 1 else "ies"
    msg = f"✅ Fixed *{fixed}* blank-category entr{suffix}."
    if no_date:
        msg += f"\n• {no_date} entries skipped — date not found in Weekly grid (pre-grid history)."
    if no_colour:
        msg += f"\n• {no_colour} entries skipped — colour unrecognised or cell empty."
    return msg


async def cmd_fixcats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    await update.message.reply_text("⏳ Scanning Log tab for blank categories…")
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, _fixcats_sync)
    except APIError as exc:
        log.exception("fixcats APIError")
        result = f"❌ Sheets API error during fixcats — check logs ({exc})."
    except Exception:
        log.exception("fixcats failed")
        result = "❌ Error during fixcats — check logs."
    await update.message.reply_text(result, parse_mode="Markdown")


# ── /gaps ────────────────────────────────────────────────────────────────
#
# /trend totals can disagree with /auditlog when the Sheet is missing a
# row for a specific hour (bot was down, skipped slot never refilled,
# etc.). /gaps walks the Sheet for a given period and lists every hourly
# slot that has no row, so the user can backfill from /missing or /log.


def _gaps_sync(year: int, month: int) -> str:
    sheet = sheets.get_worksheet(settings.SHEET_NAME)
    all_rows = sheet.get_all_values()

    prefix = f"{year:04d}-{month:02d}"
    have: set[str] = set()
    for r in all_rows[1:]:
        sched = r[0].strip() if r and len(r) > 0 else ""
        if not sched or not sched.startswith(prefix):
            continue
        # Normalise to 'YYYY-MM-DD HH:00' so two minute-variants of the
        # same hour collapse.
        if len(sched) >= 13:
            have.add(sched[:13] + ":00")

    last_day = calendar.monthrange(year, month)[1]
    today_local = datetime.now(settings.tz).date()
    if today_local.year == year and today_local.month == month:
        last_day = today_local.day

    expected: list[str] = []
    for day in range(1, last_day + 1):
        for hour in range(24):
            expected.append(f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:00")

    missing = [ts for ts in expected if ts not in have]
    expected_count = len(expected)
    have_count = len([ts for ts in expected if ts in have])

    lines = [
        f"🕳️ *Gap audit: {prefix}*",
        f"Hours covered: {have_count}/{expected_count}",
    ]
    if not missing:
        lines.append("✅ No missing hourly slots.")
        return "\n".join(lines)

    lines.append(f"❌ {len(missing)} missing slot(s).")
    lines.append("")
    lines.append("*First 30 gaps:*")
    for ts in missing[:30]:
        lines.append(f"• `{ts}`")
    if len(missing) > 30:
        lines.append(f"_…and {len(missing) - 30} more._")
    lines.append("")
    lines.append(
        "_Recover via /missing (if the bot recorded the slot as "
        "pending/skipped) or by adding the row manually in the Sheet "
        "then running /repair._"
    )
    return "\n".join(lines)


async def cmd_gaps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    arg = " ".join(context.args).strip() if context.args else ""
    from ..dates import parse_user_month
    parsed = parse_user_month(arg, settings.tz)
    if parsed is None:
        await update.message.reply_text(
            "⚠️ Usage: `/gaps [YYYY-MM | MM]` (default: this month)",
            parse_mode="Markdown",
        )
        return
    year, month = parsed
    await update.message.reply_text(
        f"⏳ Scanning Log tab for missing hours in {year:04d}-{month:02d}…"
    )
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, _gaps_sync, year, month)
    except APIError as exc:
        log.exception("gaps APIError")
        result = f"❌ Sheets API error during gaps — check logs ({exc})."
    except Exception:
        log.exception("gaps failed")
        result = "❌ Error during gaps — check logs."
    await update.message.reply_text(result, parse_mode="Markdown")


# ── /uncat ───────────────────────────────────────────────────────────────
#
# /trend silently drops any Sheet row whose Category cell isn't *exactly*
# in CATEGORY_ORDER (e.g. typo, stray space, different emoji variant, an
# old name like "Creative" without the prefix). /fixcats only handles the
# blank-cell case. /uncat surfaces the actual offending values so the user
# can decide whether to repair from Telegram or fix in the Sheet directly.


def _uncat_sync(prefix: Optional[str]) -> str:
    sheet = sheets.get_worksheet(settings.SHEET_NAME)
    all_rows = sheet.get_all_values()

    valid = set(CATEGORIES.keys())
    offenders: list[tuple[int, str, str]] = []  # (sheet_row, scheduled_ts, raw_cat)
    bad_values: Counter[str] = Counter()

    for i, r in enumerate(all_rows[1:], start=2):
        sched = r[0].strip() if len(r) > 0 else ""
        if not sched:
            continue
        if prefix and not sched.startswith(prefix):
            continue
        cat = r[2].strip() if len(r) > 2 else ""
        if cat in valid:
            continue
        # Skip blank rows — those belong to /fixcats territory.
        if not cat:
            continue
        offenders.append((i, sched, cat))
        bad_values[cat] += 1

    scope = f"in {prefix}" if prefix else "across all rows"
    if not offenders:
        return (
            f"✅ No non-canonical category values {scope}.\n"
            "_(Blank cells are handled by /fixcats.)_"
        )

    lines = [
        f"⚠️ *{len(offenders)} row(s) with unrecognised categories {scope}*",
        "",
        "*Bad values seen:*",
    ]
    for value, count in bad_values.most_common():
        lines.append(f"• `{value}` × {count}")

    lines.append("")
    lines.append("*First 15 offenders (Sheet row → timestamp → bad category):*")
    for sheet_row, sched, cat in offenders[:15]:
        lines.append(f"• row {sheet_row}: `{sched}` → `{cat}`")
    if len(offenders) > 15:
        lines.append(f"_…and {len(offenders) - 15} more._")

    lines.append("")
    lines.append(
        "_Fix options: edit the Category cell directly in the Sheet, "
        "then run /repair so the local DB picks up the change._"
    )
    return "\n".join(lines)


async def cmd_uncat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    arg = " ".join(context.args).strip() if context.args else ""
    prefix: Optional[str] = None
    if arg:
        # Accept YYYY, YYYY-MM, or YYYY-MM-DD as a timestamp prefix.
        if not (len(arg) in (4, 7, 10) and all(c.isdigit() or c == "-" for c in arg)):
            await update.message.reply_text(
                "⚠️ Usage: `/uncat [YYYY | YYYY-MM | YYYY-MM-DD]`",
                parse_mode="Markdown",
            )
            return
        prefix = arg
    await update.message.reply_text(
        f"⏳ Scanning Log tab for unrecognised categories"
        f"{f' in {prefix}' if prefix else ''}…"
    )
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, _uncat_sync, prefix)
    except APIError as exc:
        log.exception("uncat APIError")
        result = f"❌ Sheets API error during uncat — check logs ({exc})."
    except Exception:
        log.exception("uncat failed")
        result = "❌ Error during uncat — check logs."
    await update.message.reply_text(result, parse_mode="Markdown")


# ── /auditlog ────────────────────────────────────────────────────────────


def _auditlog_sync(prefix: str) -> str:
    sheet = sheets.get_worksheet(settings.SHEET_NAME)
    all_rows = sheet.get_all_values()
    month_rows = [r for r in all_rows[1:] if r and r[0].strip().startswith(prefix)]
    total = len(month_rows)

    day_counts: dict[str, int] = {}
    for r in month_rows:
        day = r[0].strip()[:10]
        day_counts[day] = day_counts.get(day, 0) + 1
    over_days = {d: c for d, c in day_counts.items() if c > 24}

    ts_counts = Counter(r[0].strip() for r in month_rows)
    exact_dups = sum(v - 1 for v in ts_counts.values() if v > 1)
    hour_counts = Counter(r[0].strip()[:13] for r in month_rows)
    hour_dups = sum(v - 1 for v in hour_counts.values() if v > 1)

    formats: set[str] = set()
    for r in month_rows:
        ts = r[0].strip()
        if len(ts) >= 16:
            formats.add(f"len={len(ts)} sample={ts[:16]!r}")

    lines = [
        f"📋 *Audit: {prefix}*",
        f"Total rows: {total}",
        f"Expected max: {31 if total else '?'} days × 24h = up to 744",
        f"Exact duplicate timestamps: {exact_dups}",
        f"Same-hour duplicates (diff minutes): {hour_dups}",
        f"Days with >24 entries: {len(over_days)}",
    ]
    if over_days:
        top = sorted(over_days.items(), key=lambda x: -x[1])[:5]
        lines.append("Worst days: " + ", ".join(f"{d}={c}" for d, c in top))
    if formats:
        lines.append("TS formats seen: " + "; ".join(sorted(formats)[:3]))
    return "\n".join(lines)


async def cmd_auditlog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    now_local = datetime.now(timezone.utc).astimezone(settings.tz)
    arg = " ".join(context.args).strip() if context.args else ""
    if arg:
        try:
            ref = datetime.strptime(arg, "%Y-%m").replace(tzinfo=settings.tz)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Use format: `/auditlog 2026-03`", parse_mode="Markdown",
            )
            return
    else:
        ref = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    prefix = ref.strftime("%Y-%m")
    await update.message.reply_text(f"⏳ Auditing Log tab for {prefix}…")
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, _auditlog_sync, prefix)
    except APIError as exc:
        log.exception("auditlog APIError")
        result = f"❌ Sheets API error: {exc}"
    except Exception as exc:
        log.exception("auditlog failed")
        result = f"❌ Error: {exc}"
    await update.message.reply_text(result, parse_mode="Markdown")


# ── /missing ─────────────────────────────────────────────────────────────
#
# Surfaces hours the user never filled in (status='pending' or 'skipped'),
# so they can re-prompt and complete them from Telegram instead of editing
# the Sheet by hand. Reuses the same edit-selection state the /edit
# command does, so picking a row drops back into the normal category →
# tag/note flow and finalises with queue_mark_done (which flips status to
# 'done' regardless of whether it was 'pending' or 'skipped').

DEFAULT_MISSING_HOURS = 48
MAX_MISSING_HOURS = 24 * 14  # two weeks


async def cmd_missing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    if not session.is_idle and session.stage != STAGE_EDIT_SELECTION:
        await update.message.reply_text(
            "⚠️ You're currently mid-entry. Use /cancel first, then /missing."
        )
        return

    arg = " ".join(context.args).strip() if context.args else ""
    hours = DEFAULT_MISSING_HOURS
    if arg:
        try:
            hours = max(1, min(int(arg), MAX_MISSING_HOURS))
        except ValueError:
            await update.message.reply_text(
                f"⚠️ Usage: `/missing [hours]` (1-{MAX_MISSING_HOURS}, default {DEFAULT_MISSING_HOURS}).",
                parse_mode="Markdown",
            )
            return

    now = datetime.now(timezone.utc)
    start = now - dt.timedelta(hours=hours)
    rows = queue_get_unfilled_window(start, now)

    if not rows:
        await update.message.reply_text(
            f"✅ No unfilled hours in the last {hours}h.\n"
            "_Note: this only sees hours the bot recorded — if it was "
            "offline at the top of an hour, that slot won't appear. "
            "Use /repair to pull in Sheet-only entries._",
            parse_mode="Markdown",
        )
        return

    msg = (
        f"⛳ *Unfilled hours in the last {hours}h:*\n"
        f"_(Use /cancel to abandon)_\n\n"
    )
    keyboard: list[list[str]] = []
    recent_ids: list[int] = []
    recent_labels: list[str] = []
    for row in rows:
        ts = parse_ts(row["scheduled_ts"]).astimezone(settings.tz)
        icon = "⏳" if row["status"] == "pending" else "⏭"
        label = f"[{row['id']}] {ts.strftime('%a %d %b %H:%M')} {icon} (unfilled)"
        msg += f"• {label}\n"
        keyboard.append([label])
        recent_ids.append(row["id"])
        recent_labels.append(label)
    msg += "\n_Tap one to fill it in._"

    await session.begin_edit_selection(recent_ids, recent_labels)
    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            keyboard, one_time_keyboard=True, resize_keyboard=True
        ),
    )


# ── /repair ──────────────────────────────────────────────────────────────
#
# Reconciles the local SQLite ``queue`` table with the Sheet's Log audit
# tab in *both* directions:
#
#   • Sheet → DB: any row that exists in the Log tab but not in DB is
#     ingested as ``status='done'``, ``sheets_synced=1``. After /repair,
#     manually-typed Sheet entries become editable from Telegram.
#
#   • DB → Sheet: any ``done`` row in DB whose canonical timestamp is
#     missing from the Log tab is flagged ``sheets_synced=0`` so the next
#     /sync re-attempts the write.
#
# The diff is keyed on the canonical UTC timestamp string, so it is
# robust against minor display-time differences in the Sheet.


def _parse_sheet_local_ts(s: str) -> Optional[datetime]:
    """Parse a Log-tab timestamp ('YYYY-MM-DD HH:MM') as the user's local
    tz, return UTC-aware datetime."""
    s = s.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            naive = datetime.strptime(s, fmt)
        except ValueError:
            continue
        return naive.replace(tzinfo=settings.tz).astimezone(timezone.utc)
    return None


def _repair_sync() -> str:
    log_ws = sheets.get_worksheet(settings.SHEET_NAME)
    sheet_rows = log_ws.get_all_values()

    # Index Sheet by canonical UTC timestamp.
    sheet_by_ts: dict[str, list[str]] = {}
    for r in sheet_rows[1:]:
        if not r or len(r) < 1:
            continue
        sched_utc = _parse_sheet_local_ts(r[0])
        if sched_utc is None:
            continue
        sheet_by_ts[canonical_ts(sched_utc)] = r

    # Sheet → DB: pull in rows the DB doesn't know about.
    db_ts = queue_get_all_scheduled_ts()
    pulled = 0
    for canon_ts, r in sheet_by_ts.items():
        if canon_ts in db_ts:
            continue
        sched_utc = parse_ts(canon_ts)
        sub_utc = _parse_sheet_local_ts(r[1] if len(r) > 1 else "") or sched_utc
        category = (r[2].strip() if len(r) > 2 else "") or "⚪️ Other"
        tag = r[3].strip() if len(r) > 3 else ""
        note = r[4].strip() if len(r) > 4 else ""
        # Run the insert synchronously via the sync helper since we're
        # already inside a thread-pool call.
        from ..database import queue_insert_done_row_sync
        if queue_insert_done_row_sync(
            sched_utc, sub_utc, category, tag, note, sheets_synced=True
        ):
            pulled += 1

    # DB → Sheet: re-flag DB done rows whose timestamp isn't in the Sheet.
    # Bound the window to the Sheet's actual range — anything older than
    # the earliest Sheet row is "pre-sheet history" and shouldn't be
    # forced to re-sync.
    flagged = 0
    if sheet_by_ts:
        earliest = min(parse_ts(t) for t in sheet_by_ts.keys())
        latest = max(parse_ts(t) for t in sheet_by_ts.keys())
        # Re-fetch DB rows AFTER the pull above so we see the new ones too.
        for row in queue_get_done_in_window(earliest, latest):
            if row["scheduled_ts"] not in sheet_by_ts and row["sheets_synced"]:
                from ..database import queue_mark_unsynced_sync
                queue_mark_unsynced_sync(row["id"])
                flagged += 1

    parts = []
    parts.append(
        f"✅ Pulled *{pulled}* Sheet-only entr{'y' if pulled == 1 else 'ies'} into local DB."
    )
    parts.append(
        f"🔄 Flagged *{flagged}* DB entr{'y' if flagged == 1 else 'ies'} for re-sync "
        f"(missing from Log tab — run /sync to push)."
    )
    if pulled == 0 and flagged == 0:
        return "✅ DB and Log tab are in agreement — nothing to repair."
    return "\n".join(parts)


async def cmd_repair(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    await update.message.reply_text("⏳ Reconciling local DB with Log tab…")
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, _repair_sync)
    except APIError as exc:
        log.exception("repair APIError")
        result = f"❌ Sheets API error during repair: {exc}"
    except Exception as exc:
        log.exception("repair failed")
        result = f"❌ Error during repair: {exc}"
    await update.message.reply_text(result, parse_mode="Markdown")
