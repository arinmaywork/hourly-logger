import os
import sqlite3
import logging
import asyncio
import json
import time
import datetime as dt
from datetime import datetime, timezone
from functools import partial
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError, NetworkError
from dotenv import load_dotenv

# ─── Config ───────────────────────────────────────────────────────────────────

load_dotenv()

TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
CHAT_ID         = int(os.environ["CHAT_ID"])
SPREADSHEET_ID  = os.environ["SPREADSHEET_ID"]
TZ              = ZoneInfo(os.getenv("TIMEZONE", "UTC"))
DB_PATH         = os.getenv("DB_PATH", "queue.db")
SHEET_NAME      = os.getenv("SHEET_NAME", "Log")           # Fix: was hardcoded
GRID_SHEET_NAME = os.getenv("GRID_SHEET_NAME", "Weekly")
CREDS_FILE      = os.getenv("CREDS_FILE", "credentials.json")  # Fix: was hardcoded
TAG_MAX_LEN     = 60
NOTE_MAX_LEN    = 500
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CATEGORIES = {
    "🟢 Creative":     {"color": {"red": 0.0, "green": 1.0, "blue": 0.0}},
    "💎 Health":       {"color": {"red": 0.0, "green": 1.0, "blue": 1.0}},
    "🔘 Professional": {"color": {"red": 0.8, "green": 0.8, "blue": 0.8}},  # #CCCCCC
    "🟡 Social":       {"color": {"red": 1.0, "green": 1.0, "blue": 0.0}},
    "⚪️ Other":        {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}},
}

# Single-letter (and word) shortcuts used by the /log quick-entry command
CATEGORY_SHORTCUTS: dict[str, str] = {
    "c": "🟢 Creative",    "cr": "🟢 Creative",    "creative": "🟢 Creative",
    "h": "💎 Health",      "he": "💎 Health",      "health":   "💎 Health",
    "p": "🔘 Professional","pr": "🔘 Professional","prof":     "🔘 Professional",
                                                    "professional": "🔘 Professional",
    "s": "🟡 Social",      "so": "🟡 Social",      "social":   "🟡 Social",
    "o": "⚪️ Other",       "ot": "⚪️ Other",       "other":    "⚪️ Other",
}

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def escape_md(text: str) -> str:
    """Escape Telegram Markdown v1 special characters in user-provided text."""
    for ch in ("*", "_", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


# ─── Database ─────────────────────────────────────────────────────────────────

def db_connect():
    dirname = os.path.dirname(DB_PATH)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                scheduled_ts  TEXT NOT NULL,
                submitted_ts  TEXT,
                category      TEXT,
                entry_text    TEXT,
                status        TEXT NOT NULL DEFAULT 'pending'
                              CHECK(status IN ('pending','done','skipped')),
                tag           TEXT,
                note          TEXT,
                sheets_synced INTEGER NOT NULL DEFAULT 1
            )
        """)
        # Migrations: safely add new columns to existing databases
        for col_sql in [
            "category TEXT",
            "tag TEXT",
            "note TEXT",
            "sheets_synced INTEGER NOT NULL DEFAULT 1",
        ]:
            try:
                conn.execute(f"ALTER TABLE queue ADD COLUMN {col_sql}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON queue(status)")
        conn.commit()
    log.info("Database initialised at %s", DB_PATH)


def queue_add_prompt(scheduled_ts: datetime):
    """Insert a new pending prompt. INSERT OR IGNORE prevents duplicate hours."""
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO queue (scheduled_ts) VALUES (?)",
            (scheduled_ts.isoformat(),),
        )
        conn.commit()


def queue_get_oldest_pending():
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM queue WHERE status='pending' ORDER BY scheduled_ts ASC LIMIT 1"
        ).fetchone()


def queue_count_pending() -> int:
    with db_connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM queue WHERE status='pending'"
        ).fetchone()[0]


def queue_get_recent_done(limit: int = 5):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM queue WHERE status='done' ORDER BY scheduled_ts DESC LIMIT ?",
            (limit,),
        ).fetchall()


def queue_get_by_id(row_id: int):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM queue WHERE id=?", (row_id,)
        ).fetchone()


def queue_mark_done(
    row_id: int,
    category: str,
    tag: str,
    note: str,
    submitted_ts: datetime,
    sheets_synced: bool = False,
):
    """Mark entry done, storing tag and note in both separate columns and combined entry_text."""
    combined = f"{tag} | {note}" if note else tag
    with db_connect() as conn:
        conn.execute(
            """UPDATE queue
               SET status='done', category=?, tag=?, note=?, entry_text=?,
                   submitted_ts=?, sheets_synced=?
               WHERE id=?""",
            (category, tag, note, combined, submitted_ts.isoformat(), int(sheets_synced), row_id),
        )
        conn.commit()


def queue_mark_skipped(row_id: int):
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET status='skipped', submitted_ts=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), row_id),
        )
        conn.commit()


def queue_mark_sheets_synced(row_id: int, synced: bool):
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET sheets_synced=? WHERE id=?",
            (int(synced), row_id),
        )
        conn.commit()


def queue_get_unsynced():
    """Return all done entries whose last Sheets write failed."""
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM queue WHERE status='done' AND sheets_synced=0"
        ).fetchall()


def _ts_param(ts: datetime) -> str:
    """Return a timestamp string normalised to bare UTC for SQLite strftime comparison.

    Strips the '+00:00' offset so it matches entries stored either with or without
    the timezone suffix.  All stored timestamps are UTC so the offset is irrelevant.
    """
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def queue_category_breakdown(since_ts: datetime) -> tuple[dict[str, int], int]:
    """Return (breakdown, total_done) for done entries on or after since_ts.

    breakdown   — {category: count} for entries with a non-NULL category, sorted by count desc.
    total_done  — true count of ALL done entries in the period, including uncategorised ones.
                  Using breakdown sum would silently undercount entries with NULL category.

    Uses strftime() on both sides of the comparison so entries stored with OR without
    the '+00:00' timezone suffix are handled consistently.
    """
    param = _ts_param(since_ts)
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT category, COUNT(*) as cnt
               FROM queue
               WHERE status='done'
                 AND strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?
               GROUP BY category
               ORDER BY cnt DESC""",
            (param,),
        ).fetchall()
        total_done = conn.execute(
            """SELECT COUNT(*) FROM queue
               WHERE status='done'
                 AND strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?""",
            (param,),
        ).fetchone()[0]
    breakdown = {row["category"]: row["cnt"] for row in rows if row["category"]}
    return breakdown, total_done


def queue_daily_counts_week(week_start_utc: datetime, now_utc: datetime) -> list[tuple[str, int]]:
    """Return a list of (day_label, done_count) for each day Mon→today (local TZ).

    Each day runs from 00:00 local time to 23:59:59 local time.
    Uses strftime() for consistent comparison regardless of stored tz suffix.
    """
    results = []
    with db_connect() as conn:
        # Iterate calendar days from week_start_local to today
        day_local = week_start_utc.astimezone(TZ).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        today_local = now_utc.astimezone(TZ).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        while day_local <= today_local:
            day_start_utc = day_local.astimezone(timezone.utc)
            day_end_local = day_local.replace(hour=23, minute=59, second=59)
            day_end_utc   = day_end_local.astimezone(timezone.utc)
            count = conn.execute(
                """SELECT COUNT(*) FROM queue
                   WHERE status='done'
                     AND strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?
                     AND strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) <= ?""",
                (_ts_param(day_start_utc), _ts_param(day_end_utc)),
            ).fetchone()[0]
            results.append((day_local.strftime("%a %-d %b"), count))
            day_local += dt.timedelta(days=1)
    return results


def backfill_missed_prompts():
    with db_connect() as conn:
        last = conn.execute("SELECT MAX(scheduled_ts) FROM queue").fetchone()[0]
    if not last:
        return
    # Fix: use .astimezone() instead of .replace() for correct conversion
    last_dt = datetime.fromisoformat(last).astimezone(timezone.utc)
    now     = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    current = last_dt + dt.timedelta(hours=1)
    count   = 0
    while current <= now:
        queue_add_prompt(current)  # INSERT OR IGNORE prevents duplicates at startup
        current += dt.timedelta(hours=1)
        count   += 1
    if count:
        log.info("Backfilled %d missed prompt(s).", count)


# ─── Google Sheets ────────────────────────────────────────────────────────────
# Fix: cache the client/spreadsheet object to avoid re-authing on every call.
# Fix: gspread.authorize() is deprecated; use gspread.Client(auth=creds) instead.
# Fix: all sync gspread calls are run via asyncio.run_in_executor so they never
#      block the event loop. time.sleep() inside these sync functions is fine
#      because they execute in a worker thread, not on the event loop thread.

_gspread_spreadsheet = None


def _get_spreadsheet():
    """Return a cached gspread Spreadsheet, re-authenticating if needed."""
    global _gspread_spreadsheet
    if _gspread_spreadsheet is not None:
        return _gspread_spreadsheet

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info  = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)

    client = gspread.Client(auth=creds)  # replaces deprecated gspread.authorize()
    _gspread_spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return _gspread_spreadsheet


def _reset_spreadsheet_cache():
    global _gspread_spreadsheet
    _gspread_spreadsheet = None


def _get_sheet_sync(name: str | None = None):
    """Synchronous worksheet fetch. Always call via run_in_executor from async code."""
    sheet_name = name or SHEET_NAME
    try:
        return _get_spreadsheet().worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        if sheet_name == SHEET_NAME:
            ws = _get_spreadsheet().add_worksheet(title=SHEET_NAME, rows=5000, cols=6)
            # Write headers only on first creation — not on every append
            ws.append_row(
                ["Scheduled Time", "Submitted Time", "Category", "Tag", "Note", "Lag (minutes)"],
                value_input_option="USER_ENTERED",
            )
            return ws
        raise
    except Exception:
        _reset_spreadsheet_cache()
        raise


def _sheets_save_row_sync(
    scheduled_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str = "",
    is_edit: bool = False,
):
    """Synchronous implementation. Runs in a thread pool — time.sleep() here is safe."""
    lag       = round((submitted_ts - scheduled_ts).total_seconds() / 60, 1)
    sched_str = scheduled_ts.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
    sub_str   = submitted_ts.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
    row       = [sched_str, sub_str, category, tag, note, lag]

    for attempt in range(5):
        try:
            sheet = _get_sheet_sync()
            if is_edit:
                try:
                    cell = sheet.find(sched_str, in_column=1)
                    if cell:
                        sheet.update(
                            f"A{cell.row}:F{cell.row}", [row],
                            value_input_option="USER_ENTERED",
                        )
                        log.info("Row %d updated in Log: %s", cell.row, row)
                        return
                except gspread.exceptions.CellNotFound:
                    pass  # Fall through to append if not found

            # Fix: header check removed — headers are only written at sheet creation
            sheet.append_row(row, value_input_option="USER_ENTERED")
            log.info("Row appended to Log: %s", row)
            return

        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < 4:
                wait = 2 ** attempt * 5
                log.warning("Rate limited (Log). Retrying in %ds...", wait)
                time.sleep(wait)  # Safe: runs in thread pool, not event loop
            else:
                _reset_spreadsheet_cache()
                raise
        except Exception as e:
            # Fix: generic errors now also get backoff delay, not an instant tight loop
            wait = 2 ** attempt
            log.error("Sheets log write failed (attempt %d): %s", attempt + 1, e)
            _reset_spreadsheet_cache()
            if attempt < 4:
                time.sleep(wait)
            else:
                raise


def _sheets_update_grid_sync(scheduled_ts: datetime, category: str, tag: str):
    """Synchronous implementation. Runs in a thread pool — time.sleep() here is safe."""
    local_dt = scheduled_ts.astimezone(TZ)
    hour     = local_dt.hour

    if hour < 7:
        effective_dt = local_dt - dt.timedelta(days=1)
    else:
        effective_dt = local_dt

    # Fix: use integer format instead of Linux-only %-m/%-d strftime codes
    date_str = f"{effective_dt.month}/{effective_dt.day}/{str(effective_dt.year)[2:]}"

    # Hour → row mapping: 7→5 … 23→21, 0→22 … 6→28
    row = (hour - 2) if hour >= 7 else (hour + 22)

    for attempt in range(5):
        try:
            grid      = _get_sheet_sync(GRID_SHEET_NAME)
            dates_row = grid.row_values(2)
            col       = -1
            for i, d in enumerate(dates_row):
                if d.strip() == date_str:
                    col = i + 1
                    break

            if col == -1:
                log.warning("Date %s not found in grid row 2. Skipping grid update.", date_str)
                return

            cell_addr = gspread.utils.rowcol_to_a1(row, col)
            grid.update(cell_addr, [[tag]])

            color = CATEGORIES.get(category, {}).get("color")
            if color:
                grid.format(cell_addr, {
                    "backgroundColor": color,
                    "horizontalAlignment": "CENTER",
                    "textFormat": {"bold": True},
                })

            log.info("Grid updated at %s (Row %d, Col %d) for %s", cell_addr, row, col, category)
            return

        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < 4:
                wait = 2 ** attempt * 5
                log.warning("Rate limited (Grid). Retrying in %ds...", wait)
                time.sleep(wait)  # Safe: runs in thread pool
            else:
                _reset_spreadsheet_cache()
                raise
        except Exception as e:
            wait = 2 ** attempt
            log.error("Grid write failed (attempt %d): %s", attempt + 1, e)
            _reset_spreadsheet_cache()
            if attempt < 4:
                time.sleep(wait)
            else:
                raise


def _sheets_log_breakdown_sync(
    since_local: datetime,
    until_local: datetime,
) -> tuple[dict[str, int], int]:
    """Read category breakdown from the Log tab.

    Log tab columns (row 1 = header):
      A: Scheduled Time  ("YYYY-MM-DD HH:MM", local TZ)
      B: Submitted Time
      C: Category
      D: Tag
      E: Note
      F: Lag (minutes)

    Returns (breakdown, total):
      breakdown — {category: count}, sorted by count desc, no None/empty keys
      total     — total logged entries in the period (including uncategorised)
    """
    since_str = since_local.strftime("%Y-%m-%d %H:%M")
    until_str = until_local.strftime("%Y-%m-%d %H:%M")

    sheet = _get_sheet_sync(SHEET_NAME)
    # Fetch only columns A and C to minimise data transfer
    all_rows = sheet.get("A:C", value_render_option="FORMATTED_VALUE")

    breakdown: dict[str, int] = {}
    total = 0

    for row in all_rows[1:]:   # skip header row
        if not row:
            continue
        sched = row[0].strip() if len(row) > 0 else ""
        cat   = row[2].strip() if len(row) > 2 else ""

        if not sched:
            continue
        # Simple string comparison works because format is "YYYY-MM-DD HH:MM"
        if not (since_str <= sched <= until_str):
            continue

        total += 1
        if cat:
            breakdown[cat] = breakdown.get(cat, 0) + 1
        else:
            breakdown.setdefault("_uncategorised", 0)
            breakdown["_uncategorised"] += 1

    uncategorised = breakdown.pop("_uncategorised", 0)
    breakdown = dict(sorted(breakdown.items(), key=lambda x: -x[1]))
    if uncategorised:
        breakdown["_uncategorised"] = uncategorised

    return breakdown, total


async def sheets_log_breakdown(
    since_local: datetime,
    until_local: datetime,
) -> tuple[dict[str, int], int]:
    """Non-blocking async wrapper around _sheets_log_breakdown_sync."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        partial(_sheets_log_breakdown_sync, since_local, until_local),
    )


async def sheets_save_row(
    scheduled_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str = "",
    is_edit: bool = False,
):
    """Non-blocking async wrapper — runs sync gspread calls in a thread pool."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        partial(_sheets_save_row_sync, scheduled_ts, submitted_ts, category, tag, note, is_edit),
    )


async def sheets_update_grid(scheduled_ts: datetime, category: str, tag: str):
    """Non-blocking async wrapper — runs sync gspread calls in a thread pool."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        partial(_sheets_update_grid_sync, scheduled_ts, category, tag),
    )


# ─── Bot State ────────────────────────────────────────────────────────────────

current_prompt: dict = {}
_scheduler: AsyncIOScheduler | None = None


def _is_owner(update: Update) -> bool:
    return update.effective_chat.id == CHAT_ID


async def send_prompt(bot, queue_row, is_edit: bool = False):
    global current_prompt
    scheduled     = datetime.fromisoformat(queue_row["scheduled_ts"]).astimezone(TZ)
    pending_count = queue_count_pending()
    header = ""
    if is_edit:
        header = f"🛠 *Editing Entry* — `{scheduled.strftime('%a %b %d, %H:%M')}`\n\n"
    elif pending_count > 1:
        header = f"⏳ *{pending_count} entries queued* — answering oldest first\n\n"

    msg = (
        f"{header}"
        f"📝 *Hourly Log* — `{scheduled.strftime('%a %b %d, %H:%M')}`\n\n"
        f"*Step 1/2:* Select a category:\n"
        f"_or skip the keyboard:_ `/log c Deep Work | note`\n"
        f"`c` Creative · `h` Health · `p` Professional · `s` Social · `o` Other\n\n"
        f"/skip · /cancel · /status · /edit · /sync"
    )
    keyboard     = [[cat] for cat in CATEGORIES.keys()]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=msg,
            parse_mode="Markdown",
            reply_markup=reply_markup,
        )
        current_prompt = {
            "queue_id":     queue_row["id"],
            "scheduled_ts": queue_row["scheduled_ts"],
            "stage":        "category",
            "category":     None,
            "tag":          None,
            "is_edit":      is_edit,
        }
    except (NetworkError, TelegramError) as e:
        log.error("Failed to send prompt: %s", e)


# ─── Handlers ─────────────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_prompt
    if not _is_owner(update):
        return
    text = update.message.text.strip()
    if not text:
        return

    if not current_prompt:
        pending = queue_get_oldest_pending()
        if pending:
            await send_prompt(context.bot, pending)
        else:
            await update.message.reply_text(
                "✅ No pending entries right now. I'll prompt you at the next hour!"
            )
        return

    # ── Stage: edit_selection ─────────────────────────────────────────────
    if current_prompt.get("stage") == "edit_selection":
        if text not in current_prompt.get("recent_labels", []):
            await update.message.reply_text("Please select a valid entry from the list.")
            return
        idx    = current_prompt["recent_labels"].index(text)
        row_id = current_prompt["recent_ids"][idx]
        row    = queue_get_by_id(row_id)
        if row:
            await send_prompt(context.bot, row, is_edit=True)
        else:
            await update.message.reply_text("Error finding that entry.")
            current_prompt = {}
        return

    # ── Stage 1: Waiting for category ─────────────────────────────────────
    if current_prompt.get("stage") == "category":
        if text not in CATEGORIES:
            await update.message.reply_text("Please select a valid category from the menu.")
            return
        current_prompt["category"] = text
        current_prompt["stage"]    = "tag_note"
        await update.message.reply_text(
            f"📂 *{escape_md(text)}*\n\n"
            f"*Step 2/2:* Tag? _(add `| note` for context — optional)_\n"
            f"`Deep Work` or `Deep Work | focused on Q1`",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # ── Stage 2: Waiting for tag (+ optional inline note) ─────────────────
    # Format: "Tag" or "Tag,, Note" (or legacy "Tag | Note").
    if current_prompt.get("stage") == "tag_note":
        if ",," in text:
            tag, note = text.split(",,", 1)
            tag  = tag.strip()
            note = note.strip()
        elif " | " in text:
            tag, note = text.split(" | ", 1)
            tag  = tag.strip()
            note = note.strip()
        else:
            tag  = text.strip()
            note = ""

        if len(tag) > TAG_MAX_LEN:
            await update.message.reply_text(
                f"⚠️ Tag is too long (max {TAG_MAX_LEN} chars). Please shorten it."
            )
            return
        if len(note) > NOTE_MAX_LEN:
            await update.message.reply_text(
                f"⚠️ Note is too long (max {NOTE_MAX_LEN} chars). Please shorten it."
            )
            return

        category = current_prompt["category"]
        now      = datetime.now(timezone.utc)
        queue_id = current_prompt["queue_id"]
        sched_ts = datetime.fromisoformat(current_prompt["scheduled_ts"]).astimezone(timezone.utc)
        is_edit  = current_prompt.get("is_edit", False)

        # Mark done locally first (sheets_synced=False until confirmed)
        queue_mark_done(queue_id, category, tag, note, now, sheets_synced=False)

        try:
            await sheets_save_row(sched_ts, now, category, tag, note, is_edit=is_edit)
            await sheets_update_grid(sched_ts, category, tag)
            queue_mark_sheets_synced(queue_id, True)
            status_text = "Updated" if is_edit else "Logged"
            note_line   = f"\n• Note: {escape_md(note)}" if note else ""
            await update.message.reply_text(
                f"✅ *{status_text}!*\n"
                f"• Category: {escape_md(category)}\n"
                f"• Tag: {escape_md(tag)}{note_line}",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error("Sheets write failed: %s", e)
            await update.message.reply_text(
                "⚠️ Saved locally but Google Sheets write failed. Use /sync to retry."
            )

        current_prompt = {}

        next_pending = queue_get_oldest_pending()
        if next_pending:
            await update.message.reply_text(
                f"➡️ {queue_count_pending()} more to go — here's the next one:"
            )
            await send_prompt(context.bot, next_pending)
        else:
            await update.message.reply_text(
                "🎉 All caught up! I'll ping you again next hour."
            )


async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Quick one-line entry: /log <category> <tag> [,, note]
    Bypasses the multi-step flow entirely — fastest way to log an hour.
    Use ',,' (three commas) to separate tag from note; legacy ' | ' also works.

    Category shortcuts (case-insensitive):
      c / cr / creative       → 🟢 Creative
      h / he / health         → 💎 Health
      p / pr / prof / ...     → 🔘 Professional
      s / so / social         → 🟡 Social
      o / ot / other          → ⚪️ Other

    Examples:
      /log c Deep Work
      /log h Sleep,, 7 hrs feel good
      /log p Tasks,, quarterly review
    """
    global current_prompt
    if not _is_owner(update):
        return

    args_text = " ".join(context.args).strip() if context.args else ""
    if not args_text:
        shortcuts = "`c` `h` `p` `s` `o`"
        await update.message.reply_text(
            f"⚡ *Quick Log* — log an hour in one message\n\n"
            f"*Usage:* `/log <category> <tag> [,, note]`\n\n"
            f"*Category shortcuts:* {shortcuts}\n"
            f"_(Creative, Health, Professional, Social, Other)_\n\n"
            f"*Examples:*\n"
            f"• `/log c Deep Work`\n"
            f"• `/log h Sleep,, 7 hrs feel rested`\n"
            f"• `/log p Tasks,, quarterly review`",
            parse_mode="Markdown",
        )
        return

    if current_prompt:
        await update.message.reply_text(
            "⚠️ You're mid-entry. Use /cancel first, then /log."
        )
        return

    # Parse: first token = category shortcut, rest = tag [| note]
    parts    = args_text.split(None, 1)
    shortcut = parts[0].lower()
    rest     = parts[1].strip() if len(parts) > 1 else ""

    category = CATEGORY_SHORTCUTS.get(shortcut)
    if not category:
        valid = " | ".join(f"`{k}`" for k in ["c", "h", "p", "s", "o"])
        await update.message.reply_text(
            f"❓ Unknown category `{escape_md(shortcut)}`.\n"
            f"Valid shortcuts: {valid}",
            parse_mode="Markdown",
        )
        return

    if not rest:
        await update.message.reply_text("Please add a tag after the category, e.g. `/log c Deep Work`", parse_mode="Markdown")
        return

    # Split "Tag,, Note" (or legacy "Tag | Note") or just "Tag"
    if ",," in rest:
        tag, note = rest.split(",,", 1)
        tag  = tag.strip()
        note = note.strip()
    elif " | " in rest:
        tag, note = rest.split(" | ", 1)
        tag  = tag.strip()
        note = note.strip()
    else:
        tag  = rest
        note = ""

    if len(tag) > TAG_MAX_LEN:
        await update.message.reply_text(f"⚠️ Tag too long (max {TAG_MAX_LEN} chars).")
        return
    if len(note) > NOTE_MAX_LEN:
        await update.message.reply_text(f"⚠️ Note too long (max {NOTE_MAX_LEN} chars).")
        return

    pending = queue_get_oldest_pending()
    if not pending:
        await update.message.reply_text("✅ No pending entries right now.")
        return

    now      = datetime.now(timezone.utc)
    queue_id = pending["id"]
    sched_ts = datetime.fromisoformat(pending["scheduled_ts"]).astimezone(timezone.utc)

    queue_mark_done(queue_id, category, tag, note, now, sheets_synced=False)
    try:
        await sheets_save_row(sched_ts, now, category, tag, note)
        await sheets_update_grid(sched_ts, category, tag)
        queue_mark_sheets_synced(queue_id, True)
        note_line = f"\n• Note: {escape_md(note)}" if note else ""
        await update.message.reply_text(
            f"⚡ *Logged!*\n"
            f"• Category: {escape_md(category)}\n"
            f"• Tag: {escape_md(tag)}{note_line}",
            parse_mode="Markdown",
        )
    except Exception as e:
        log.error("Sheets write failed in /log: %s", e)
        await update.message.reply_text(
            "⚠️ Saved locally but Sheets write failed. Use /sync to retry."
        )

    next_pending = queue_get_oldest_pending()
    if next_pending:
        await update.message.reply_text(
            f"➡️ {queue_count_pending()} more to go — here's the next one:"
        )
        await send_prompt(context.bot, next_pending)
    else:
        await update.message.reply_text("🎉 All caught up! I'll ping you again next hour.")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(
        "👋 *Hourly Logger is active!*\n\n"
        "I'll message you every hour for your log entry.\n\n"
        "*Entry modes:*\n"
        "• *Guided* — tap category, then type `Tag | Note` _(2 steps)_\n"
        "• *Quick* — `/log c Deep Work | note` _(1 message, no prompts)_\n\n"
        "*Commands:*\n"
        "• /log `<cat> <tag> [| note]` — instant one-line entry\n"
        "• /status — queue stats\n"
        "• /edit — edit recent entries\n"
        "• /skip — skip current prompt\n"
        "• /cancel — abandon flow without skipping\n"
        "• /sync — retry failed Sheets writes",
        parse_mode="Markdown",
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Abandon the current multi-step flow without marking the entry as skipped."""
    global current_prompt
    if not _is_owner(update):
        return
    if not current_prompt:
        await update.message.reply_text("Nothing to cancel right now.")
        return

    was_edit_selection = current_prompt.get("stage") == "edit_selection"
    current_prompt = {}

    if was_edit_selection:
        await update.message.reply_text(
            "🚫 Edit cancelled.",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await update.message.reply_text(
            "🚫 Cancelled. The current prompt remains pending and will be shown again.",
            reply_markup=ReplyKeyboardRemove(),
        )

    # In both cases, surface the next pending entry so the user isn't left stranded
    next_pending = queue_get_oldest_pending()
    if next_pending:
        await send_prompt(context.bot, next_pending)


async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_prompt
    if not _is_owner(update):
        return

    if not current_prompt:
        await update.message.reply_text("Nothing to skip right now.")
        return

    # Skip the entire entry from any stage — tag+note are now entered together
    # so there is no partial-save opportunity mid-flow.
    queue_mark_skipped(current_prompt["queue_id"])
    await update.message.reply_text("⏭ Skipped.", reply_markup=ReplyKeyboardRemove())

    current_prompt = {}
    next_pending = queue_get_oldest_pending()
    if next_pending:
        await send_prompt(context.bot, next_pending)


async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_prompt
    if not _is_owner(update):
        return

    # Fix: guard against silently discarding an in-progress entry
    if current_prompt and current_prompt.get("stage") != "edit_selection":
        await update.message.reply_text(
            "⚠️ You're currently mid-entry. Use /cancel to abandon it first, "
            "then /edit to pick a past entry."
        )
        return

    recent = queue_get_recent_done(5)
    if not recent:
        await update.message.reply_text("No entries found to edit.")
        return

    msg           = "✏️ *Select an entry to edit:*\n_(Use /cancel to go back)_\n\n"
    keyboard      = []
    recent_ids    = []
    recent_labels = []

    for row in recent:
        ts    = datetime.fromisoformat(row["scheduled_ts"]).astimezone(TZ)
        text  = row["entry_text"] or "(no text)"
        # Fix: embed row ID in label to guarantee uniqueness even with similar entries
        label = f"[{row['id']}] {ts.strftime('%a %H:%M')} — {text[:18]}"
        msg  += f"• {label}\n"
        keyboard.append([label])
        recent_ids.append(row["id"])
        recent_labels.append(label)

    current_prompt = {
        "stage":          "edit_selection",
        "recent_ids":     recent_ids,
        "recent_labels":  recent_labels,
    }

    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    pending = queue_count_pending()
    with db_connect() as conn:
        done     = conn.execute("SELECT COUNT(*) FROM queue WHERE status='done'").fetchone()[0]
        skipped  = conn.execute("SELECT COUNT(*) FROM queue WHERE status='skipped'").fetchone()[0]
        unsynced = conn.execute(
            "SELECT COUNT(*) FROM queue WHERE status='done' AND sheets_synced=0"
        ).fetchone()[0]

    now_utc   = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(TZ)

    # Weekly date range — Monday 00:00 local time to today
    week_start_local = (now_local - dt.timedelta(days=now_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Yearly date range — 1 Jan 00:00 local time to today
    year_start_local = now_local.replace(
        month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )

    # Fetch both breakdowns from the Log tab
    week_data, total_week = await sheets_log_breakdown(week_start_local, now_local)
    year_data, total_year = await sheets_log_breakdown(year_start_local, now_local)

    def format_breakdown(data: dict, total_done: int) -> str:
        """Render a bar-chart breakdown.

        data may contain a '_uncategorised' sentinel key for cells with content
        but no matching category colour.  Percentages are relative to total_done
        and use the largest-remainder method to guarantee they sum to 100%.
        """
        if total_done == 0:
            return "_No entries yet._"

        bar_width     = 10
        uncategorised = data.get("_uncategorised", 0)
        cat_data      = {k: v for k, v in data.items() if k != "_uncategorised"}

        all_items: dict[str, int] = dict(cat_data)
        if uncategorised:
            all_items["⚠️ Uncategorised"] = uncategorised

        # Largest-remainder rounding relative to total_done
        exact   = {cat: cnt / total_done * 100 for cat, cnt in all_items.items()}
        floored = {cat: int(p) for cat, p in exact.items()}
        deficit = 100 - sum(floored.values())
        for cat in sorted(exact, key=lambda c: -(exact[c] % 1))[:deficit]:
            floored[cat] += 1

        lines = []
        for cat, count in cat_data.items():
            pct    = floored[cat]
            filled = round(pct / 100 * bar_width)
            bar    = "█" * filled + "░" * (bar_width - filled)
            lines.append(f"{cat}\n  `{bar}` {pct}% ({count}h)")

        if uncategorised:
            pct = floored["⚠️ Uncategorised"]
            lines.append(f"_⚠️ {uncategorised}h unmatched colour ({pct}%)_")

        return "\n".join(lines)

    week_label = (
        f"Mon {week_start_local.day} {week_start_local.strftime('%b')} — now"
        f" ({total_week}h)"
    )
    year_label = f"1 Jan {now_local.year} — now ({total_year}h)"

    await update.message.reply_text(
        f"📊 *Queue Status*\n"
        f"• Pending:   `{pending}`\n"
        f"• Completed: `{done}`\n"
        f"• Skipped:   `{skipped}`\n"
        f"• Unsynced:  `{unsynced}`\n\n"
        f"📅 *This Week* — _{week_label}_\n"
        f"{format_breakdown(week_data, total_week)}\n\n"
        f"📆 *This Year* — _{year_label}_\n"
        f"{format_breakdown(year_data, total_year)}",
        parse_mode="Markdown",
    )


async def cmd_migrate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """One-time migration: copy entries from Weekly grid → Log tab.

    Safe to run multiple times — skips entries already in the Log tab.
    """
    if not _is_owner(update):
        return

    await update.message.reply_text(
        "⏳ Starting migration from Weekly grid → Log tab.\n"
        "This may take a minute…"
    )

    def _migrate_sync() -> str:
        """Synchronous migration — runs in thread pool."""
        # ── Colour → category map ────────────────────────────────────────────
        # Nearest-colour matching — tolerates Google Sheets palette variations.
        # Uses Euclidean distance in RGB (0-1) space; threshold 0.25 keeps
        # categories well-separated (min distance between any two is ~0.35).
        # White (1,1,1) maps to "⚪️ Other"; empty cells are filtered by tag check.
        def _nearest_cat(r: float, g: float, b: float) -> str:
            best, best_d = "", float("inf")
            for cat_name, info in CATEGORIES.items():
                c = info["color"]
                d = ((r - c["red"])**2 + (g - c["green"])**2 + (b - c["blue"])**2) ** 0.5
                if d < best_d:
                    best_d, best = d, cat_name
            return best if best_d <= 0.25 else ""

        def row_to_hour(row_1based: int) -> int:
            return row_1based - 22 if row_1based >= 22 else row_1based + 2

        spreadsheet = _get_spreadsheet()

        # ── 1. Fetch Weekly grid with full formatting ─────────────────────────
        resp = spreadsheet.client.request(
            "get",
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet.id}",
            params={
                "ranges":          f"'{GRID_SHEET_NAME}'",
                "includeGridData": "true",
            },
        ).json()

        if "error" in resp:
            return f"❌ Sheets API error: {resp['error'].get('message', resp['error'])}"

        row_data = resp["sheets"][0]["data"][0].get("rowData", [])
        if len(row_data) < 2:
            return "❌ Weekly grid has fewer than 2 rows — nothing to migrate."

        # ── 2. Find the date row — scan first 4 rows ─────────────────────────
        # Date cells in Sheets are stored as serial numbers (not text), so
        # effectiveValue.stringValue is empty.  formattedValue gives the
        # display string (e.g. "1/1/26") regardless of underlying cell type.
        def _cell_text(cell: dict) -> str:
            return (
                cell.get("formattedValue", "")
                or cell.get("effectiveValue", {}).get("stringValue", "")
                or cell.get("userEnteredValue", {}).get("stringValue", "")
            ).strip()

        def _parse_date_row(row_idx: int) -> dict[int, dt.date]:
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

        col_dates: dict[int, dt.date] = {}
        date_row_idx = -1
        for _i in range(min(4, len(row_data))):
            col_dates = _parse_date_row(_i)
            if len(col_dates) >= 3:   # need at least 3 date columns to be confident
                date_row_idx = _i
                break

        if not col_dates:
            return "❌ No date columns found in the first 4 rows of Weekly grid."

        # ── 3. Load existing Log entries ──────────────────────────────────────
        log_ws   = _get_sheet_sync(SHEET_NAME)
        existing = {
            row[0].strip()
            for row in log_ws.get_all_values()[1:]
            if row and row[0].strip()
        }

        # ── 4. Find data start row — look for "7:00" in column A ─────────────
        # The sheet has fixed header rows (Week No, Date, Day, Spent) before
        # the 24 hour-slot rows start at row 5 (0-based index 4).
        # Scan column A for the "7:00" label to locate the exact start row.
        data_start = 4   # safe default (row 5)
        for _i, rd in enumerate(row_data):
            col_a = _cell_text(rd.get("values", [{}])[0]) if rd.get("values") else ""
            if col_a.strip() in ("7:00", "07:00"):
                data_start = _i
                break

        new_rows: list[list] = []
        unmatched = 0

        for row_idx in range(data_start, data_start + 24):
            if row_idx >= len(row_data):
                break
            hour  = row_to_hour(row_idx + 1)
            cells = row_data[row_idx].get("values", [])

            for col_idx, col_date in col_dates.items():
                if col_idx >= len(cells):
                    continue
                cell = cells[col_idx]

                tag = _cell_text(cell)
                if not tag:
                    continue

                actual_date = col_date + dt.timedelta(days=1) if hour < 7 else col_date
                sched_local = datetime(
                    actual_date.year, actual_date.month, actual_date.day,
                    hour, 0, 0, tzinfo=TZ,
                )
                sched_str = sched_local.strftime("%Y-%m-%d %H:%M")

                if sched_str in existing:
                    continue

                # Skip future dates — only migrate up to today
                if actual_date > dt.date.today():
                    continue

                # Prefer backgroundColorStyle.rgbColor (current API field);
                # fall back to backgroundColor (deprecated but still populated).
                # IMPORTANT: Google Sheets API omits colour channels whose value
                # is 0.0 (treating absence as zero).  Default must be 0.0, not
                # 1.0, otherwise colours like #ffff00 (blue=0) or #03ff00
                # (blue=0) are misread as white and mapped to "Other".
                eff_fmt   = cell.get("effectiveFormat", {})
                rgb       = eff_fmt.get("backgroundColorStyle", {}).get("rgbColor", {})
                if not rgb:
                    rgb   = eff_fmt.get("backgroundColor", {})
                cat = _nearest_cat(
                    rgb.get("red",   0.0),
                    rgb.get("green", 0.0),
                    rgb.get("blue",  0.0),
                )
                if not cat:
                    unmatched += 1

                new_rows.append([sched_str, sched_str, cat, tag, "", 0])
                existing.add(sched_str)

        if not new_rows:
            return "✅ Nothing to migrate — Log tab is already up to date."

        new_rows.sort(key=lambda r: r[0])

        # Append in batches of 500
        for i in range(0, len(new_rows), 500):
            batch = new_rows[i : i + 500]
            log_ws.append_rows(batch, value_input_option="USER_ENTERED")
            time.sleep(1)  # avoid rate limits

        msg = f"✅ Migrated *{len(new_rows)}* entries from Weekly → Log tab."
        if unmatched:
            msg += f"\n⚠️ {unmatched} cells had unrecognised colours (copied without category)."
        return msg

    loop   = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _migrate_sync)
    await update.message.reply_text(result, parse_mode="Markdown")


async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Retry all done entries whose Sheets write previously failed."""
    if not _is_owner(update):
        return

    unsynced = queue_get_unsynced()
    if not unsynced:
        await update.message.reply_text("✅ Nothing to sync — all entries are up to date.")
        return

    count = len(unsynced)
    await update.message.reply_text(
        f"🔄 Syncing {count} unsynced entr{'y' if count == 1 else 'ies'}..."
    )

    success = 0
    failed  = 0
    for row in unsynced:
        sched_ts = datetime.fromisoformat(row["scheduled_ts"]).astimezone(timezone.utc)
        sub_ts   = (
            datetime.fromisoformat(row["submitted_ts"]).astimezone(timezone.utc)
            if row["submitted_ts"]
            else datetime.now(timezone.utc)
        )
        category = row["category"] or "⚪️ Other"
        tag      = row["tag"] or row["entry_text"] or ""
        note     = row["note"] or ""

        try:
            await sheets_save_row(sched_ts, sub_ts, category, tag, note, is_edit=True)
            await sheets_update_grid(sched_ts, category, tag)
            queue_mark_sheets_synced(row["id"], True)
            success += 1
        except Exception as e:
            log.error("Sync failed for row %d: %s", row["id"], e)
            failed += 1

    parts = []
    if success:
        parts.append(f"✅ {success} synced successfully")
    if failed:
        parts.append(f"❌ {failed} still failing — check logs and try /sync again")
    await update.message.reply_text("\n".join(parts))


# ─── Scheduler ────────────────────────────────────────────────────────────────

async def hourly_job(bot):
    global current_prompt
    now       = datetime.now(timezone.utc)
    scheduled = now.replace(minute=0, second=0, microsecond=0)
    queue_add_prompt(scheduled)  # INSERT OR IGNORE prevents duplicate on startup race
    log.info("Hourly job fired. Scheduled TS: %s", scheduled.isoformat())
    if not current_prompt:
        pending = queue_get_oldest_pending()
        if pending:
            await send_prompt(bot, pending)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global _scheduler
    db_init()
    backfill_missed_prompts()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("log",    cmd_log))
    app.add_handler(CommandHandler("skip",   cmd_skip))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("edit",   cmd_edit))
    app.add_handler(CommandHandler("sync",    cmd_sync))
    # /migrate intentionally not registered — one-time migration completed Mar 2026.
    # The cmd_migrate function is retained for reference only.
    # app.add_handler(CommandHandler("migrate", cmd_migrate))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(application: Application):
        global _scheduler
        _scheduler = AsyncIOScheduler(timezone="UTC")
        _scheduler.add_job(
            hourly_job,
            trigger="cron",
            minute=0,
            args=[application.bot],
        )
        _scheduler.start()
        log.info("Scheduler started.")

    async def on_shutdown(application: Application):
        global _scheduler
        if _scheduler and _scheduler.running:
            _scheduler.shutdown(wait=False)
            log.info("Scheduler stopped.")

    app.post_init = on_startup
    app.post_stop = on_shutdown  # Fix: graceful scheduler shutdown

    log.info("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
