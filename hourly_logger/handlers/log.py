"""``/log`` quick-entry command."""

from __future__ import annotations

from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ContextTypes

from .. import background
from ..colors import CATEGORY_SHORTCUTS
from ..config import settings
from ..database import (
    parse_ts,
    queue_count_pending,
    queue_get_oldest_pending,
    queue_mark_done,
)
from ..logger import get_logger
from ..state import session
from . import flow
from ._common import escape_md, is_owner


log = get_logger(__name__)


def _split_tag_note(rest: str) -> tuple[str, str]:
    if ",," in rest:
        tag, note = rest.split(",,", 1)
    elif " | " in rest:
        tag, note = rest.split(" | ", 1)
    else:
        return rest.strip(), ""
    return tag.strip(), note.strip()


async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    args_text = " ".join(context.args).strip() if context.args else ""
    if not args_text:
        await update.message.reply_text(
            "⚡ *Quick Log* — log an hour in one message\n\n"
            "*Usage:* `/log <category> <tag> [,, note]`\n\n"
            "*Category shortcuts:* `c` `h` `p` `s` `o`\n"
            "_(Creative, Health, Professional, Social, Other)_\n\n"
            "*Examples:*\n"
            "• `/log c Deep Work`\n"
            "• `/log h Sleep,, 7 hrs feel rested`\n"
            "• `/log p Tasks,, quarterly review`",
            parse_mode="Markdown",
        )
        return
    if not session.is_idle:
        await update.message.reply_text(
            "⚠️ You're mid-entry. Use /cancel first, then /log."
        )
        return

    parts = args_text.split(None, 1)
    shortcut = parts[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""

    category = CATEGORY_SHORTCUTS.get(shortcut)
    if not category:
        await update.message.reply_text(
            f"❓ Unknown category `{escape_md(shortcut)}`.\n"
            f"Valid shortcuts: `c` `h` `p` `s` `o`",
            parse_mode="Markdown",
        )
        return
    if not rest:
        await update.message.reply_text(
            "Please add a tag after the category, e.g. `/log c Deep Work`",
            parse_mode="Markdown",
        )
        return

    tag, note = _split_tag_note(rest)
    if len(tag) > settings.TAG_MAX_LEN:
        await update.message.reply_text(f"⚠️ Tag too long (max {settings.TAG_MAX_LEN} chars).")
        return
    if len(note) > settings.NOTE_MAX_LEN:
        await update.message.reply_text(f"⚠️ Note too long (max {settings.NOTE_MAX_LEN} chars).")
        return

    pending = queue_get_oldest_pending()
    if not pending:
        await update.message.reply_text("✅ No pending entries right now.")
        return

    queue_id = pending["id"]
    sched_ts = parse_ts(pending["scheduled_ts"])
    now = datetime.now(timezone.utc)

    await queue_mark_done(queue_id, category, tag, note, now, sheets_synced=False)

    note_line = f"\n• Note: {escape_md(note)}" if note else ""
    await update.message.reply_text(
        f"⚡ *Logged!*\n"
        f"• Category: {escape_md(category)}\n"
        f"• Tag: {escape_md(tag)}{note_line}",
        parse_mode="Markdown",
    )

    next_pending = queue_get_oldest_pending()
    if next_pending:
        await update.message.reply_text(
            f"➡️ {queue_count_pending()} more to go — here's the next one:"
        )
        await flow.send_prompt(context.bot, next_pending)
    else:
        await update.message.reply_text("🎉 All caught up! I'll ping you again next hour.")

    background.spawn(
        flow._background_sheets_sync(
            context.bot, queue_id, sched_ts, now, category, tag, note, False,
        ),
        name=f"sync:log:{queue_id}",
    )
