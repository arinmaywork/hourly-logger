"""APScheduler integration: hourly job + lifecycle hooks."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .database import backfill_missed_prompts, queue_get_oldest_pending
from .logger import get_logger, request_context
from .state import session


log = get_logger(__name__)
_scheduler: Optional[AsyncIOScheduler] = None


async def hourly_job(bot) -> None:
    """Insert the current hour (and any missed hours) and (re-)prompt the user.

    Uses ``backfill_missed_prompts`` rather than a single
    ``queue_add_prompt(now)`` so the scheduler is self-healing: if a
    previous tick was dropped — APScheduler does this silently when the
    event loop is busy past ``misfire_grace_time`` (e.g. mid-Sheets
    call) — the next tick that *does* run will insert every missed
    hour up to ``now``. Reproduced in production: user got the 1:30am
    IST prompt and then jumped straight to 3:30am IST because the
    21:00 UTC tick was dropped while the bot was finishing a Sheets
    upsert. Backfill is idempotent (INSERT OR IGNORE on a UNIQUE
    index), so calling it every tick is cheap and safe.
    """
    with request_context(handler="hourly_job"):
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        inserted = await backfill_missed_prompts()
        log.info("hourly tick", extra={"sched": now.isoformat(), "backfilled": inserted})

        # Bug #2: ``send_prompt`` is now a no-op via ``try_begin_prompt``
        # if the user is mid-entry, so this check is belt-and-braces. We
        # still skip explicitly to avoid the chat traffic.
        if not session.is_idle:
            log.info("hourly skip — user mid-entry")
            return

        pending = queue_get_oldest_pending()
        if pending:
            from .handlers import flow  # local import avoids circulars
            await flow.send_prompt(bot, pending)


def start_scheduler(bot) -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(
        hourly_job,
        trigger="cron",
        minute=0,
        args=[bot],
        # Defaults are misfire_grace_time=1, coalesce=False — under load
        # the cron tick fails to schedule within 1s and APScheduler drops
        # it silently, leaving the user with no prompt for that hour.
        # 600s grace covers any plausible Sheets/Telegram round-trip;
        # coalesce=True means a long backlog of missed ticks (e.g. after
        # an outage) only fires once on recovery instead of N times in a
        # tight burst. The hourly_job's backfill loop still picks up
        # every individual missed hour.
        misfire_grace_time=600,
        coalesce=True,
    )
    _scheduler.start()
    log.info("scheduler started")
    return _scheduler


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        log.info("scheduler stopped")
    _scheduler = None
