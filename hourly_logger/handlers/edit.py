"""``/edit`` command — list and select a past entry to revise.

Bug #5 fix: legacy NULL-category rows render with ``?`` and re-prompt
into a clean state where the user picks a category fresh.

Bug #10 fix: date parsing routed through :func:`hourly_logger.dates.parse_user_date`
with a documented error message listing the supported formats.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import ContextTypes

from ..colors import category_emoji
from ..config import settings
from ..database import (
    parse_ts,
    queue_get_by_date,
    queue_get_recent_done,
)
from ..dates import SUPPORTED_DATE_FORMATS_HUMAN, parse_user_date
from ..logger import get_logger
from ..state import session
from ._common import is_owner


log = get_logger(__name__)


async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    if not session.is_idle and session.stage != "edit_selection":
        await update.message.reply_text(
            "⚠️ You're currently mid-entry. Use /cancel to abandon it first, "
            "then /edit to pick a past entry."
        )
        return

    arg = " ".join(context.args).strip() if context.args else ""
    target_date = None
    count: Optional[int] = None
    if arg:
        # `/edit 20` → last 20 entries; `/edit 28/03/2026` → that date.
        if arg.isdigit():
            count = max(1, min(int(arg), 50))
        else:
            target_date = parse_user_date(arg, settings.tz)
            if target_date is None:
                await update.message.reply_text(
                    f"⚠️ Unrecognised arg. Pass a count (e.g. `20`) or a date.\n"
                    f"Supported date formats:\n{SUPPORTED_DATE_FORMATS_HUMAN}",
                    parse_mode="Markdown",
                )
                return

    if target_date:
        rows = queue_get_by_date(target_date, settings.tz)
        title = (
            f"✏️ *Entries for {target_date.strftime('%a %d %b %Y')}:*\n"
            "_(Use /cancel to go back)_\n\n"
        )
    else:
        n = count if count is not None else 5
        rows = queue_get_recent_done(n)
        title = (
            f"✏️ *Last {n} entries — pick one to edit:*\n"
            "_(Use /cancel to go back. Tip: `/edit 20` for more, "
            "`/edit YYYY-MM-DD` for a specific day, `/missing` for unfilled hours.)_\n\n"
        )

    if not rows:
        msg = (
            f"No entries found for {target_date.strftime('%d %b %Y')}."
            if target_date else "No entries found to edit."
        )
        await update.message.reply_text(msg)
        return

    msg = title
    keyboard: list[list[str]] = []
    recent_ids: list[int] = []
    recent_labels: list[str] = []

    for row in rows:
        ts = parse_ts(row["scheduled_ts"]).astimezone(settings.tz)
        text = row["entry_text"] or "(no text)"
        if row["status"] == "skipped":
            cat_icon = "⏭"
            text = "(skipped)"
        else:
            cat_icon = category_emoji(row["category"])  # Bug #5: NULL → '?'
        label = f"[{row['id']}] {ts.strftime('%a %H:%M')} {cat_icon} — {text[:18]}"
        msg += f"• {label}\n"
        keyboard.append([label])
        recent_ids.append(row["id"])
        recent_labels.append(label)

    await session.begin_edit_selection(recent_ids, recent_labels)
    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )
