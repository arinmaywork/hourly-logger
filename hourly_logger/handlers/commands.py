"""Basic owner-facing commands: /start, /help, /skip, /skipall, /cancel."""

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


HELP_TEXT = (
    "📖 *Hourly Logger — Commands*\n\n"
    "*Logging entries*\n"
    "• /log `<cat> <tag> [,, note]` — instant one-line entry\n"
    "   _e.g._ `/log c Deep Work,, refactored the bot`\n"
    "• Reply to a prompt — guided 2-step (category → `Tag,, Note`)\n"
    "• /skip — skip the current prompt\n"
    "• /skipall — skip every pending entry from before today's log-day\n"
    "• /cancel — abandon the current flow without skipping\n\n"
    "*Reports*\n"
    "• /status — queue + this week + this year breakdown\n"
    "• /weekly `[date]` — week containing date (default: this week)\n"
    "   _accepts_ `YYYY-MM-DD`, `DD/MM`, `today`, `yesterday`\n"
    "• /monthly `[YYYY-MM | MM]` — month breakdown (default: this month)\n"
    "• /trend `monthly [YYYY]` — monthly totals across a year\n"
    "• /trend `weekly [YYYY-MM]` — weekly totals across a month\n\n"
    "*Editing & recovery*\n"
    "• /edit `[N|date]` — pick from N recent entries to fix\n"
    "   _default 5, max 50; date forms same as /weekly_\n"
    "• /missing `[hours]` — list unfilled (pending/skipped) hours so you can\n"
    "   fill them in from Telegram _(default 48h, max 336h)_\n\n"
    "*Sheets sync & repair*\n"
    "• /sync — retry failed Google Sheets writes\n"
    "• /repair — reconcile local DB ↔ Log tab in both directions\n"
    "   _pulls Sheet-only rows into DB; re-flags DB rows missing from Sheet_\n"
    "• /dedup — remove duplicate-timestamp rows from the Log tab\n"
    "• /fixcats — re-derive blank categories from the Weekly grid colour\n"
    "• /uncat `[YYYY | YYYY-MM | YYYY-MM-DD]` — list rows whose category\n"
    "   isn't in the canonical set _(non-blank typos /fixcats can't catch)_\n"
    "• /auditlog `[YYYY-MM]` — health audit of the Log tab for a month\n\n"
    "*Misc*\n"
    "• /start — welcome message\n"
    "• /help — this list"
)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    await update.message.reply_text(
        "👋 *Hourly Logger is active!*\n\n"
        "I'll message you every hour for your log entry.\n\n"
        "*Entry modes:*\n"
        "• *Guided* — tap category, then type `Tag,, Note` _(2 steps)_\n"
        "• *Quick* — `/log c Deep Work,, note` _(1 message, no prompts)_\n\n"
        "Send /help for the full command reference.",
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
