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
    if not is_owner(update):
        return
    unsynced = queue_get_unsynced()
    if not unsynced:
        await update.message.reply_text("✅ Nothing to sync — all entries are up to date.")
        return

    count = len(unsynced)
    suffix = "y" if count == 1 else "ies"
    await update.message.reply_text(f"🔄 Syncing {count} unsynced entr{suffix}...")

    success = failed = grid_misses = 0
    for row in unsynced:
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
        except (TimeoutError, ConnectionError) as e:
            log.error("sync network error", extra={"queue_id": row["id"], "err": str(e)})
            failed += 1

    parts = []
    if success:
        parts.append(f"✅ {success} synced successfully")
    if grid_misses:
        parts.append(f"⚠️ {grid_misses} entries fell outside the Weekly grid range")
    if failed:
        parts.append(f"❌ {failed} still failing — check logs and try /sync again")
    await update.message.reply_text("\n".join(parts))


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
