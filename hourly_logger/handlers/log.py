"""``/log`` quick-entry command.

Two flavours:

1. **Implicit / live entry**: ``/log <category> <tag> [,, note]`` fills
   the oldest *pending* row. This is the original behaviour, used for
   keeping up with hourly prompts in real time.

2. **Explicit / backfill**: ``/log <YYYY-MM-DD> <HH:MM> <category>
   <tag> [,, note]`` (or the single-token ISO form
   ``/log <YYYY-MM-DDTHH:MM> ...``) targets a specific historical hour.
   Useful for the Sheet-only gaps that ``/gaps`` surfaces — bot was
   offline at the top of that hour, so no DB row exists, and the
   implicit flow would skip past them. The explicit form inserts the
   missing row on the fly and writes through to the Sheet.

The user types the time in their configured ``TIMEZONE`` (display
timezone). We round to the nearest top-of-UTC-hour so both ``10:00``
(what /gaps prints) and ``10:30`` (what the Sheet displays for an IST
user) collapse to the same canonical slot — see ``_round_to_utc_hour``.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from telegram import Update
from telegram.ext import ContextTypes

from .. import background
from ..colors import CATEGORY_SHORTCUTS
from ..config import settings
from ..database import (
    parse_ts,
    queue_add_prompt,
    queue_count_pending,
    queue_get_by_scheduled_ts,
    queue_get_oldest_pending,
    queue_mark_done,
)
from ..logger import get_logger
from ..state import session
from . import flow
from ._common import escape_md, is_owner


log = get_logger(__name__)

# Matchers for the optional [date time] / [datetime] prefix.
# Date: YYYY-MM-DD (allows single-digit month/day so /gaps copy-paste
# round-trips even if the user forgets a leading zero).
_DATE_RE = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$")
# Time: H:MM or HH:MM, 0-23 hours, 0-59 minutes (lax — datetime() will
# raise if values overflow, caught by the parse helper).
_TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
# Single-token ISO form: YYYY-MM-DDTHH:MM (what /missing prints).
_ISO_RE = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{2}$")


def _split_tag_note(rest: str) -> tuple[str, str]:
    if ",," in rest:
        tag, note = rest.split(",,", 1)
    elif " | " in rest:
        tag, note = rest.split(" | ", 1)
    else:
        return rest.strip(), ""
    return tag.strip(), note.strip()


def _round_to_utc_hour(dt: datetime) -> datetime:
    """Round a UTC datetime to the nearest top-of-hour (half-up).

    The bot only fires at top-of-hour UTC, so any user-typed minute
    has to collapse onto one of those slots. Half-up so a user typing
    "10:00 IST" (= 04:30 UTC) lands on 05:00 UTC — same slot the Sheet
    shows as "10:30 IST" — instead of the empty 04:00 UTC bucket.
    """
    return (dt + timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)


def _try_parse_explicit_ts(args_text: str) -> tuple[Optional[datetime], str]:
    """Peel a leading timestamp off ``args_text`` and return ``(ts, rest)``.

    Accepted forms (first one that matches wins):

    * ``YYYY-MM-DDTHH:MM`` — single token (what /missing emits).
    * ``YYYY-MM-DD HH:MM`` — two tokens (what /gaps emits).

    On success the timestamp is interpreted in ``settings.tz``, then
    converted to UTC and snapped to the nearest top-of-hour. On no-match
    or parse failure, returns ``(None, args_text)`` and the caller falls
    back to the implicit "oldest pending" path.
    """
    parts = args_text.split(None, 2)
    if not parts:
        return None, args_text

    # ── Single-token ISO form ──────────────────────────────────────────
    if _ISO_RE.match(parts[0]):
        try:
            naive = datetime.strptime(parts[0], "%Y-%m-%dT%H:%M")
        except ValueError:
            return None, args_text
        local = naive.replace(tzinfo=settings.tz)
        rest = " ".join(parts[1:])
        return _round_to_utc_hour(local.astimezone(timezone.utc)), rest

    # ── Two-token date + time ──────────────────────────────────────────
    if len(parts) >= 2 and _DATE_RE.match(parts[0]) and _TIME_RE.match(parts[1]):
        try:
            naive = datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M")
        except ValueError:
            return None, args_text
        local = naive.replace(tzinfo=settings.tz)
        rest = parts[2] if len(parts) >= 3 else ""
        return _round_to_utc_hour(local.astimezone(timezone.utc)), rest

    return None, args_text


def _format_local(ts_utc: datetime) -> str:
    """Render a UTC timestamp as ``YYYY-MM-DD HH:MM`` in the display tz."""
    local = ts_utc.astimezone(settings.tz)
    return local.strftime("%Y-%m-%d %H:%M")


async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    args_text = " ".join(context.args).strip() if context.args else ""
    if not args_text:
        await update.message.reply_text(
            "⚡ *Quick Log* — log an hour in one message\n\n"
            "*Live entry* (fills the oldest pending hour):\n"
            "`/log <category> <tag> [,, note]`\n\n"
            "*Backfill* (target a specific historical hour):\n"
            "`/log <YYYY-MM-DD> <HH:MM> <category> <tag> [,, note]`\n"
            "`/log <YYYY-MM-DDTHH:MM> <category> <tag> [,, note]`\n\n"
            "*Category shortcuts:* `c` `h` `p` `s` `o`\n"
            "_(Creative, Health, Professional, Social, Other)_\n\n"
            "*Examples:*\n"
            "• `/log c Deep Work`\n"
            "• `/log h Sleep,, 7 hrs feel rested`\n"
            "• `/log 2026-04-24 10:30 c Deep Work,, backfilled gap`\n"
            "• `/log 2026-04-01T04:00 h Sleep`",
            parse_mode="Markdown",
        )
        return

    explicit_ts, args_text = _try_parse_explicit_ts(args_text)

    if not session.is_idle and explicit_ts is None:
        # Mid-entry conflict only matters for the implicit/live flow,
        # which would hijack the in-progress prompt. Backfilling a
        # historical hour doesn't touch the live session state.
        await update.message.reply_text(
            "⚠️ You're mid-entry. Use /cancel first, then /log."
        )
        return

    parts = args_text.split(None, 1)
    if not parts:
        await update.message.reply_text(
            "⚠️ Need a category after the timestamp. "
            "Try: `/log 2026-04-24 10:30 c Deep Work`",
            parse_mode="Markdown",
        )
        return
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

    if explicit_ts is not None:
        await _log_explicit(
            update, context, explicit_ts, category, tag, note,
        )
        return

    # ── Implicit / live path: fill the oldest pending row ──────────────
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


async def _log_explicit(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    sched_ts: datetime,
    category: str,
    tag: str,
    note: str,
) -> None:
    """Backfill a specific historical hour identified by ``sched_ts``."""
    now = datetime.now(timezone.utc)
    label = _format_local(sched_ts)

    if sched_ts > now:
        await update.message.reply_text(
            f"⚠️ Can't log a future hour ({label}). "
            "Wait for the prompt or use the live form."
        )
        return

    existing = queue_get_by_scheduled_ts(sched_ts)
    if existing is not None and existing["status"] == "done":
        await update.message.reply_text(
            f"⚠️ {label} is already logged. Use `/edit` to change it.",
            parse_mode="Markdown",
        )
        return

    if existing is None:
        # Bot was offline at the top of this hour, so the queue has no
        # row for it. Insert a pending placeholder so we can route
        # through the same queue_mark_done path the live flow uses.
        await queue_add_prompt(sched_ts)
        existing = queue_get_by_scheduled_ts(sched_ts)
        if existing is None:
            await update.message.reply_text(
                "❌ Failed to insert backfill placeholder — check logs."
            )
            return

    queue_id = existing["id"]
    await queue_mark_done(queue_id, category, tag, note, now, sheets_synced=False)

    note_line = f"\n• Note: {escape_md(note)}" if note else ""
    await update.message.reply_text(
        f"⚡ *Backfilled {escape_md(label)}*\n"
        f"• Category: {escape_md(category)}\n"
        f"• Tag: {escape_md(tag)}{note_line}\n"
        f"_Sheet write running in background…_",
        parse_mode="Markdown",
    )

    background.spawn(
        flow._background_sheets_sync(
            context.bot, queue_id, sched_ts, now, category, tag, note, False,
        ),
        name=f"sync:log_backfill:{queue_id}",
    )
