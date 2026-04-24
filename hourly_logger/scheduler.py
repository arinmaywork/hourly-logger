"""APScheduler integration: hourly job + lifecycle hooks."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .database import queue_add_prompt, queue_get_oldest_pending
from .logger import get_logger, request_context
from .state import session


log = get_logger(__name__)
_scheduler: Optional[AsyncIOScheduler] = None


async def hourly_job(bot) -> None:
    """Insert the current hour and (re-)prompt the user if they're idle."""
    with request_context(handler="hourly_job"):
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        await queue_add_prompt(now)
        log.info("hourly tick", extra={"sched": now.isoformat()})

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
