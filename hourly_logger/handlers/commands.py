"""Basic owner-facing commands: /start, /skip, /skipall, /cancel."""

from __future__ import annotations

from telegram import ReplyKeyboardRemove, Update
from telegram.ext import ContextTypes

from ..config import settings
from ..database import (
    queue_count_pending,
    queue_get_oldest_pending,
    queue_mark_skipped,
    queue_skipall_older_than,
)
from ..logger import get_logger
from ..state import session
from . import flow
from ._common import is_owner


log = get_logger(__name__)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    await update.message.reply_text(
        "👋 *Hourly Logger is active!*\n\n"
        "I'll message you every hour for your log entry.\n\n"
        "*Entry modes:*\n"
        "• *Guided* — tap category, then type `Tag,, Note` _(2 steps)_\n"
        "• *Quick* — `/log c Deep Work,, note` _(1 message, no prompts)_\n\n"
        "*Commands:*\n"
        "• /log `<cat> <tag> [,, note]` — instant one-line entry\n"
        "• /status — queue stats\n"
        "• /edit `[N|YYYY-MM-DD]` — edit recent entries (default 5)\n"
        "• /missing `[hours]` — fill in unfilled hours (default 48h)\n"
        "• /skip — skip current prompt\n"
        "• /cancel — abandon flow without skipping\n"
        "• /sync — retry failed Sheets writes\n"
        "• /repair — reconcile local DB with Log tab",
        parse_mode="Markdown",
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    if session.is_idle:
        await update.message.reply_text("Nothing to cancel right now.")
        return
    was_edit_selection = session.stage == "edit_selection"
    await session.clear()

    pending = queue_count_pending()
    pending_note = ""
    if pending:
        suffix = "y" if pending == 1 else "ies"
        pending_note = (
            f"\n_{pending} pending entr{suffix} waiting — send any message to "
            "resume, or /edit to fix a past entry._"
        )

    if was_edit_selection:
        await update.message.reply_text(
            f"🚫 Edit cancelled.{pending_note}",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            f"🚫 Cancelled. The current prompt remains pending.{pending_note}",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown",
        )


async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    if session.is_idle:
        await update.message.reply_text("Nothing to skip right now.")
        return
    queue_id = session.prompt.queue_id
    await queue_mark_skipped(queue_id)
    await session.clear()
    await update.message.reply_text("⏭ Skipped.", reply_markup=ReplyKeyboardRemove())
    pending = queue_get_oldest_pending()
    if pending:
        await flow.send_prompt(context.bot, pending)


async def cmd_skipall(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Skip all pending entries older than the start of today's log-day.

    Bug #4: at 3am the user's *log day* is still yesterday — so "today"
    starts at LOG_DAY_START_HOUR (default 7am), not at midnight. Without
    this, /skipall at 3am would treat 0-2:59 as "today" (still pending)
    even though they're really last-night's entries.
    """
    if not is_owner(update):
        return
    from ..dates import log_day_bounds, log_today
    today_log = log_today(settings.tz)
    today_start_utc, _ = log_day_bounds(today_log, settings.tz)

    skipped = await queue_skipall_older_than(today_start_utc)
    await session.clear()
    remaining = queue_count_pending()

    suffix = "y" if skipped == 1 else "ies"
    msg = (
        f"⏭ Skipped *{skipped}* pending entr{suffix} from before today's log-day "
        f"(starts {settings.LOG_DAY_START_HOUR:02d}:00)."
    )
    if remaining:
        rsuffix = "y" if remaining == 1 else "ies"
        msg += f"\n_{remaining} entr{rsuffix} from today still pending._"
    else:
        msg += "\n_No pending entries remaining._"
    await update.message.reply_text(
        msg, reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown",
    )
