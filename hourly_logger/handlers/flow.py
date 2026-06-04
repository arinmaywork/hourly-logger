"""Multi-step entry flow: prompt → category → tag/note → done.

Critical fixes implemented here:

* **Bug #2** — :func:`send_prompt` calls ``session.try_begin_prompt``
  which is a no-op if a prompt is already in progress, and the hourly
  scheduler short-circuits via :attr:`session.is_idle`. Edits force the
  state, but only via an explicit user command.

* **Bug #3** — every fire-and-forget Sheets sync goes through
  :func:`background.spawn`, which catches uncaught exceptions and DMs
  the owner via the global notifier installed in ``bot.py``.

* **Bug #5** — every read of ``row["category"]`` falls back to
  ``"⚪️ Other"`` so a NULL value can no longer crash the edit flow.

* **Bug #7** — the background sync inspects the
  :class:`GridUpdateOutcome`; if the date sat outside the Weekly grid
  range, the user gets an inline warning so they know.
"""

from __future__ import annotations

import calendar
import datetime as dt
from datetime import datetime, timezone
from typing import Optional

from telegram import (
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.error import NetworkError, TelegramError
from telegram.ext import ContextTypes

from .. import background, sheets
from ..colors import CATEGORIES, CATEGORY_ORDER
from ..config import settings
from ..database import (
    canonical_ts,
    parse_ts,
    queue_category_hours_in_window,
    queue_count_pending,
    queue_get_by_id,
    queue_get_oldest_pending,
    queue_increment_sync_attempt,
    queue_mark_done,
    queue_mark_sheets_synced,
)
from ..logger import get_logger, request_context
from ..state import (
    STAGE_CATEGORY,
    STAGE_EDIT_SELECTION,
    STAGE_TAG_NOTE,
    session,
)
from ._common import escape_md, is_owner


log = get_logger(__name__)

DEFAULT_CATEGORY = "⚪️ Other"  # Bug #5 fallback for legacy NULL rows.


# ── Year-progress rich header ──────────────────────────────────────────────


def _bar(filled: int, width: int) -> str:
    """Solid-block progress bar of fixed width."""
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def _build_year_header(now_utc: datetime) -> str:
    """Year-progress + YTD category breakdown shown above every hourly prompt.

    Two stacked visualisations:

    1. *Year bar* — ratio of elapsed days to total days in the year,
       with a "N weeks left" count for the at-a-glance time-pressure
       signal the user asked for.
    2. *Category bars* — share of *elapsed* hours each category has
       absorbed year-to-date, plus an explicit ``Unlogged`` row so the
       gap between elapsed-and-logged is visible. Pulled from the DB
       (not Sheets) so it's cheap to compute on every prompt and never
       blocks on a Sheets quota hiccup.

    Layout is locked to a 10-char bar wrapped in backticks so the
    columns line up on every Telegram client regardless of emoji
    width — emoji widths vary, but the leading emoji + bar segment
    is a self-contained monospace span.
    """
    from ..dates import log_day_bounds  # local — avoids handler-load cycle

    now_local = now_utc.astimezone(settings.tz)
    today_local = now_local.date()
    year = today_local.year

    # Anchor on the log-day boundary so this matches /status.
    year_start_utc, _ = log_day_bounds(dt.date(year, 1, 1), settings.tz)
    year_end_local = dt.date(year, 12, 31)
    days_in_year = 366 if calendar.isleap(year) else 365
    day_of_year = today_local.timetuple().tm_yday
    days_remaining = (year_end_local - today_local).days
    weeks_remaining = days_remaining // 7

    year_bar_w = 24
    year_bar = _bar(round(day_of_year / days_in_year * year_bar_w), year_bar_w)
    year_pct = round(day_of_year / days_in_year * 100)

    # Hour math — elapsed since year-start (clamped to 1 to avoid /0
    # in the very first hour of January), and total hours in the year.
    hours_elapsed = max(1, int((now_utc - year_start_utc).total_seconds() // 3600))
    hours_total = days_in_year * 24
    hours_remaining = max(0, hours_total - hours_elapsed)

    by_cat = queue_category_hours_in_window(year_start_utc, now_utc)
    total_logged = sum(by_cat.values())

    cat_bar_w = 10
    lines: list[str] = []
    for cat in CATEGORY_ORDER:
        h = by_cat.get(cat, 0)
        share = h / hours_elapsed
        bar = _bar(round(share * cat_bar_w), cat_bar_w)
        # Leading emoji-only label: the user reads icons faster than
        # repeated category names. Single-backtick monospace span
        # keeps the bar column rigid; trailing stats stay proportional.
        emoji = cat.split()[0]
        lines.append(f"{emoji} `{bar}` {h:,}h · {share*100:4.1f}%")

    unlogged = max(0, hours_elapsed - total_logged)
    if unlogged:
        share = unlogged / hours_elapsed
        bar = _bar(round(share * cat_bar_w), cat_bar_w)
        lines.append(f"⚫ `{bar}` {unlogged:,}h · {share*100:4.1f}%")

    return (
        f"🗓 *{year}* — *{weeks_remaining}* weeks left\n"
        f"`{year_bar}` {year_pct}%\n\n"
        f"⏱ {total_logged:,}h logged · {hours_remaining:,}h remaining\n"
        + "\n".join(lines)
        + "\n\n"
    )


# ── send_prompt ─────────────────────────────────────────────────────────────


async def send_prompt(bot, queue_row, *, is_edit: bool = False) -> bool:
    """Issue a category-selection prompt for ``queue_row``.

    Returns True if the prompt was sent. Returns False (and sends nothing)
    if the user is already mid-entry — the caller can either retry later
    or simply queue the row.
    """
    claimed = await session.try_begin_prompt(queue_row, is_edit=is_edit)
    if not claimed:
        log.info(
            "send_prompt skipped — user mid-entry",
            extra={"queue_id": queue_row["id"]},
        )
        return False

    scheduled = parse_ts(queue_row["scheduled_ts"]).astimezone(settings.tz)
    pending = queue_count_pending()
    header = ""
    if is_edit:
        header = f"🛠 *Editing Entry* — `{scheduled.strftime('%a %b %d, %H:%M')}`\n\n"
    elif pending > 1:
        header = f"⏳ *{pending} entries queued* — answering oldest first\n\n"

    # Rich year-progress + category breakdown sits above the live
    # prompt only — edits are about one specific past hour, the
    # year-wide context would just be noise.
    year_header = ""
    if not is_edit and settings.PROMPT_RICH_HEADER:
        try:
            year_header = _build_year_header(datetime.now(timezone.utc))
        except Exception as e:  # noqa: BLE001 — never let stats kill the prompt
            log.warning("rich header failed; falling back to lean", extra={"err": str(e)})

    msg = (
        f"{year_header}"
        f"{header}"
        f"📝 *Hourly Log* — `{scheduled.strftime('%a %b %d, %H:%M')}`\n\n"
        f"*Step 1/2:* Select a category:\n\n"
        f"/help · /cancel · /status · /edit · /trend"
    )
    keyboard = [[cat] for cat in CATEGORIES.keys()]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    try:
        await bot.send_message(
            chat_id=settings.CHAT_ID,
            text=msg,
            parse_mode="Markdown",
            reply_markup=reply_markup,
        )
        return True
    except (NetworkError, TelegramError) as e:
        log.error("failed to send prompt", extra={"err": str(e)})
        # Roll back the claim so the next attempt can re-issue.
        await session.clear()
        return False


# ── Background sync wrapper ─────────────────────────────────────────────────


async def _background_sheets_sync(
    bot,
    queue_id: int,
    sched_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str,
    is_edit: bool,
) -> None:
    """Run the dual-write to Sheets and update sync state.

    Bug #7: surface "date outside grid range" to the user inline.
    Bug #3: any uncaught exception escapes — :mod:`background` catches it
    and the global notifier DMs the owner so /sync can be used.
    """
    attempt = await queue_increment_sync_attempt(queue_id)
    await sheets.save_log_row(sched_ts, submitted_ts, category, tag, note, is_edit=is_edit)
    outcome = await sheets.update_grid(sched_ts, category, tag)
    await queue_mark_sheets_synced(queue_id, True)

    if not outcome.date_in_grid:
        try:
            await bot.send_message(
                chat_id=settings.CHAT_ID,
                text=(
                    "⚠️ Logged to the audit tab, but the entry's date sits "
                    "outside the Weekly grid range — no grid cell was filled."
                ),
            )
        except (NetworkError, TelegramError):
            pass

    log.info(
        "background sync ok",
        extra={"queue_id": queue_id, "attempt": attempt, "is_edit": is_edit},
    )


# ── Message handler ─────────────────────────────────────────────────────────


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    msg = update.message
    if msg is None or msg.text is None:
        return
    text = msg.text.strip()
    if not text:
        return

    with request_context(handler="message"):
        if session.is_idle:
            pending = queue_get_oldest_pending()
            if pending:
                await send_prompt(context.bot, pending)
            else:
                await msg.reply_text(
                    "✅ No pending entries right now. I'll prompt you at the next hour!"
                )
            return

        stage = session.stage
        if stage == STAGE_EDIT_SELECTION:
            await _handle_edit_selection(update, context, text)
            return
        if stage == STAGE_CATEGORY:
            await _handle_category(update, context, text)
            return
        if stage == STAGE_TAG_NOTE:
            await _handle_tag_note(update, context, text)
            return


async def _handle_edit_selection(update, context, text: str) -> None:
    async with session.transaction() as state:
        if state is None or state.stage != STAGE_EDIT_SELECTION:
            return
        if text not in state.recent_labels:
            await update.message.reply_text("Please select a valid entry from the list.")
            return
        idx = state.recent_labels.index(text)
        row_id = state.recent_ids[idx]
    row = queue_get_by_id(row_id)
    if not row:
        await update.message.reply_text("Error finding that entry.")
        await session.clear()
        return
    await send_prompt(context.bot, row, is_edit=True)


async def _handle_category(update, context, text: str) -> None:
    if text not in CATEGORIES:
        await update.message.reply_text("Please select a valid category from the menu.")
        return
    await session.advance_to_tag_note(text)
    await update.message.reply_text(
        f"📂 *{escape_md(text)}*\n\n"
        f"*Step 2/2:* Tag? _(add `,, note` for context — optional)_\n"
        f"`Deep Work` or `Deep Work,, focused on Q1`",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )


def _split_tag_note(text: str) -> tuple[str, str]:
    """Accept ``Tag,, Note`` or legacy ``Tag | Note`` or just ``Tag``."""
    if ",," in text:
        tag, note = text.split(",,", 1)
    elif " | " in text:
        tag, note = text.split(" | ", 1)
    else:
        return text.strip(), ""
    return tag.strip(), note.strip()


async def _handle_tag_note(update, context, text: str) -> None:
    tag, note = _split_tag_note(text)
    if len(tag) > settings.TAG_MAX_LEN:
        await update.message.reply_text(
            f"⚠️ Tag is too long (max {settings.TAG_MAX_LEN} chars). Please shorten it."
        )
        return
    if len(note) > settings.NOTE_MAX_LEN:
        await update.message.reply_text(
            f"⚠️ Note is too long (max {settings.NOTE_MAX_LEN} chars). Please shorten it."
        )
        return

    # Snapshot state under the lock, then release it — we don't want the
    # background task or scheduler waiting on this handler.
    snapshot: Optional[dict[str, object]] = None
    async with session.transaction() as state:
        if state is None or state.stage != STAGE_TAG_NOTE:
            return
        snapshot = {
            "queue_id": state.queue_id,
            "category": state.category or DEFAULT_CATEGORY,  # Bug #5
            "scheduled_ts": state.scheduled_ts,
            "is_edit": state.is_edit,
        }
    assert snapshot is not None
    await session.clear()

    queue_id = int(str(snapshot["queue_id"]))
    category = str(snapshot["category"])
    sched_ts = parse_ts(str(snapshot["scheduled_ts"]))
    is_edit = bool(snapshot["is_edit"])
    now = datetime.now(timezone.utc)

    await queue_mark_done(queue_id, category, tag, note, now, sheets_synced=False)

    note_line = f"\n• Note: {escape_md(note)}" if note else ""
    status_text = "Updated" if is_edit else "Logged"
    await update.message.reply_text(
        f"✅ *{status_text}!*\n"
        f"• Category: {escape_md(category)}\n"
        f"• Tag: {escape_md(tag)}{note_line}",
        parse_mode="Markdown",
    )

    next_pending = queue_get_oldest_pending()
    if next_pending:
        await update.message.reply_text(
            f"➡️ {queue_count_pending()} more to go — here's the next one:"
        )
        await send_prompt(context.bot, next_pending)
    else:
        await update.message.reply_text("🎉 All caught up! I'll ping you again next hour.")

    background.spawn(
        _background_sheets_sync(
            context.bot, queue_id, sched_ts, now, category, tag, note, is_edit,
        ),
        name=f"sync:{queue_id}",
    )
