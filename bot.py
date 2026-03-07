import os
import sqlite3
import logging
import asyncio
import json
import datetime as dt
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update
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

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID        = int(os.environ["CHAT_ID"])
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
TZ             = ZoneInfo(os.getenv("TIMEZONE", "UTC"))
DB_PATH        = os.getenv("DB_PATH", "queue.db")
SHEET_NAME     = "Log"
CREDS_FILE     = "credentials.json"
SCOPES         = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


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
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                scheduled_ts TEXT NOT NULL,
                submitted_ts TEXT,
                entry_text   TEXT,
                status       TEXT NOT NULL DEFAULT 'pending'
                             CHECK(status IN ('pending','done','skipped'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON queue(status)")
        conn.commit()
    log.info("Database initialised at %s", DB_PATH)


def queue_add_prompt(scheduled_ts: datetime):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO queue (scheduled_ts) VALUES (?)",
            (scheduled_ts.isoformat(),)
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


def queue_mark_done(row_id: int, text: str, submitted_ts: datetime):
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET status='done', entry_text=?, submitted_ts=? WHERE id=?",
            (text, submitted_ts.isoformat(), row_id),
        )
        conn.commit()


def queue_mark_skipped(row_id: int):
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET status='skipped', submitted_ts=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), row_id),
        )
        conn.commit()


def backfill_missed_prompts():
    with db_connect() as conn:
        last = conn.execute(
            "SELECT MAX(scheduled_ts) FROM queue"
        ).fetchone()[0]
    if not last:
        return
    last_dt = datetime.fromisoformat(last).replace(tzinfo=timezone.utc)
    now     = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    current = last_dt + dt.timedelta(hours=1)
    count   = 0
    while current <= now:
        queue_add_prompt(current)
        current += dt.timedelta(hours=1)
        count   += 1
    if count:
        log.info("Backfilled %d missed prompt(s).", count)


# ─── Google Sheets ────────────────────────────────────────────────────────────

def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=SCOPES
        )
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)

    client      = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        return spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=5000, cols=5)
        ws.append_row(
            ["Scheduled Time", "Submitted Time", "Tag", "Note", "Lag (minutes)"],
            value_input_option="USER_ENTERED",
        )
        return ws


def sheets_append_row(scheduled_ts: datetime, submitted_ts: datetime, tag: str, note: str = ""):
    import time
    lag = round((submitted_ts - scheduled_ts).total_seconds() / 60, 1)
    row = [
        scheduled_ts.astimezone(TZ).strftime("%Y-%m-%d %H:%M"),
        submitted_ts.astimezone(TZ).strftime("%Y-%m-%d %H:%M"),
        tag,
        note,
        lag,
    ]
    for attempt in range(5):
        try:
            sheet = get_sheet()
            headers = sheet.row_values(1)
            if "Note" not in headers:
                sheet.update("A1:E1", [["Scheduled Time", "Submitted Time", "Tag", "Note", "Lag (minutes)"]])
            sheet.append_row(row, value_input_option="USER_ENTERED")
            log.info("Row appended: %s", row)
            return
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < 4:
                wait = 2 ** attempt * 5
                log.warning("Rate limited. Retrying in %ds...", wait)
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            log.error("Sheets write failed (attempt %d): %s", attempt + 1, e)
            if attempt == 4:
                raise


# ─── Bot State ────────────────────────────────────────────────────────────────

# stages: "tag" → waiting for short tag, "note" → waiting for note
current_prompt: dict = {}


async def send_prompt(bot, queue_row):
    global current_prompt
    scheduled     = datetime.fromisoformat(queue_row["scheduled_ts"]).astimezone(TZ)
    pending_count = queue_count_pending()
    header = (
        f"⏳ *{pending_count} entries queued* — answering oldest first\n\n"
        if pending_count > 1 else ""
    )
    msg = (
        f"{header}"
        f"📝 *Hourly Log* — `{scheduled.strftime('%a %b %d, %H:%M')}`\n\n"
        f"*Step 1/2:* What's your activity tag?\n"
        f"_(e.g. Tasks, AI Tool, Sleep, Exercise)_"
    )
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
        current_prompt = {
            "queue_id":     queue_row["id"],
            "scheduled_ts": queue_row["scheduled_ts"],
            "stage":        "tag",
            "tag":          None,
        }
    except (NetworkError, TelegramError) as e:
        log.error("Failed to send prompt: %s", e)


# ─── Handlers ─────────────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_prompt
    if update.effective_chat.id != CHAT_ID:
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

    # ── Stage 1: Waiting for tag ───────────────────────────────────────────
    if current_prompt.get("stage") == "tag":
        current_prompt["tag"]   = text
        current_prompt["stage"] = "note"
        await update.message.reply_text(
            f"✏️ Tag: *{text}*\n\n"
            f"*Step 2/2:* Add a note for this hour?\n"
            f"_(Type anything or /skip to leave blank)_",
            parse_mode="Markdown",
        )
        return

    # ── Stage 2: Waiting for note ──────────────────────────────────────────
    if current_prompt.get("stage") == "note":
        tag      = current_prompt["tag"]
        note     = text
        now      = datetime.now(timezone.utc)
        queue_id = current_prompt["queue_id"]
        sched_ts = datetime.fromisoformat(current_prompt["scheduled_ts"])

        combined = f"{tag} | {note}"
        queue_mark_done(queue_id, combined, now)

        try:
            sheets_append_row(sched_ts, now, tag, note)
            await update.message.reply_text(
                f"✅ *Logged!*\n• Tag: {tag}\n• Note: {note}",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error("Sheets write failed: %s", e)
            await update.message.reply_text(
                "⚠️ Saved locally but Google Sheets write failed. Use /sync to retry."
            )

        current_prompt = {}
        await asyncio.sleep(0.5)

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


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != CHAT_ID:
        return
    await update.message.reply_text(
        "👋 *Hourly Logger is active!*\n\n"
        "I'll message you every hour for your log entry.\n\n"
        "Commands:\n"
        "• /status — see queue stats\n"
        "• /skip — skip current prompt\n"
        "• /sync — retry failed Sheets writes",
        parse_mode="Markdown",
    )


async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_prompt
    if update.effective_chat.id != CHAT_ID:
        return

    if not current_prompt:
        await update.message.reply_text("Nothing to skip right now.")
        return

    # On note stage — save tag only, skip note
    if current_prompt.get("stage") == "note":
        tag      = current_prompt["tag"]
        now      = datetime.now(timezone.utc)
        queue_id = current_prompt["queue_id"]
        sched_ts = datetime.fromisoformat(current_prompt["scheduled_ts"])
        queue_mark_done(queue_id, tag, now)
        try:
            sheets_append_row(sched_ts, now, tag, note="")
            await update.message.reply_text(
                f"✅ *Logged without note!*\n• Tag: {tag}",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error("Sheets write failed: %s", e)
            await update.message.reply_text("⚠️ Saved locally, Sheets write failed.")
    else:
        # Skip entire prompt
        queue_mark_skipped(current_prompt["queue_id"])
        await update.message.reply_text("⏭ Skipped.")

    current_prompt = {}
    next_pending = queue_get_oldest_pending()
    if next_pending:
        await send_prompt(context.bot, next_pending)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != CHAT_ID:
        return
    pending = queue_count_pending()
    with db_connect() as conn:
        done    = conn.execute("SELECT COUNT(*) FROM queue WHERE status='done'").fetchone()[0]
        skipped = conn.execute("SELECT COUNT(*) FROM queue WHERE status='skipped'").fetchone()[0]
    await update.message.reply_text(
        f"📊 *Queue Status*\n"
        f"• Pending: `{pending}`\n"
        f"• Completed: `{done}`\n"
        f"• Skipped: `{skipped}`",
        parse_mode="Markdown",
    )


async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != CHAT_ID:
        return
    await update.message.reply_text("ℹ️ Manual sync triggered. Check logs for details.")


# ─── Scheduler ────────────────────────────────────────────────────────────────

async def hourly_job(bot):
    global current_prompt
    now       = datetime.now(timezone.utc)
    scheduled = now.replace(minute=0, second=0, microsecond=0)
    queue_add_prompt(scheduled)
    log.info("Hourly job fired. Scheduled TS: %s", scheduled.isoformat())
    if not current_prompt:
        pending = queue_get_oldest_pending()
        if pending:
            await send_prompt(bot, pending)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    db_init()
    backfill_missed_prompts()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("skip",   cmd_skip))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("sync",   cmd_sync))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(application: Application):
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(
            hourly_job,
            trigger="cron",
            minute=0,
            args=[application.bot],
        )
        scheduler.start()
        log.info("Scheduler started.")

    app.post_init = on_startup

    log.info("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()