"""Scheduler self-healing: dropped APScheduler ticks must not silently
swallow hours.

The original ``hourly_job`` called ``queue_add_prompt(now)`` for the
current hour only. APScheduler's default ``misfire_grace_time=1`` will
silently drop a cron tick when the asyncio event loop is busy past
the grace window (e.g. mid-Sheets-call). When that happened, the
missed hour was never inserted into the queue, ``/missing`` couldn't
see the gap, and the user got no prompt for that hour ever.

The fix: ``hourly_job`` now calls ``backfill_missed_prompts()`` which
inserts every missed hour from ``MAX(scheduled_ts)`` to ``now``,
idempotently. So even when a tick *is* dropped, the next tick that
runs catches every gap.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from hourly_logger import database, scheduler
from hourly_logger.database import (
    db_init,
    queue_add_prompt_sync,
    queue_count_pending,
)


@pytest.mark.asyncio
async def test_hourly_job_backfills_missed_ticks(
    tmp_db_path: str, fresh_session: object
) -> None:
    """Simulate a dropped tick: seed an old MAX(scheduled_ts), run
    ``hourly_job``, and confirm every missed hour up to now appears.

    Time is anchored relative to *real* ``datetime.now`` rather than
    mocked, because backfill reads the clock inside the database
    module — patching scheduler-local symbols isn't enough. We seed
    with a 2-hour-old timestamp; backfill must produce at least the
    intermediate hour and the current one.
    """
    db_init()
    real_now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    seed = real_now - dt.timedelta(hours=2)
    queue_add_prompt_sync(seed)

    with patch("hourly_logger.scheduler.queue_get_oldest_pending", return_value=None):
        await scheduler.hourly_job(bot=AsyncMock())

    with database.db_connect() as conn:
        rows = sorted(r[0] for r in conn.execute("SELECT scheduled_ts FROM queue"))

    # Three rows: seed (T-2h), the missed T-1h, and the current T.
    assert len(rows) == 3, rows
    assert rows[0] == seed.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert rows[1] == (seed + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert rows[2] == real_now.strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.mark.asyncio
async def test_hourly_job_inserts_current_hour_when_no_drops(
    tmp_db_path: str, fresh_session: object
) -> None:
    """Happy path: previous tick fired normally → one new row this tick."""
    db_init()
    real_now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    queue_add_prompt_sync(real_now - dt.timedelta(hours=1))

    with patch("hourly_logger.scheduler.queue_get_oldest_pending", return_value=None):
        await scheduler.hourly_job(bot=AsyncMock())

    assert queue_count_pending() == 2  # previous + current


@pytest.mark.asyncio
async def test_start_scheduler_sets_grace_and_coalesce(
    tmp_db_path: str, fresh_session: object
) -> None:
    """The cron job must declare a generous misfire grace window AND
    coalesce — both protections against tick-dropping under load.

    Async test so APScheduler can grab a running event loop; otherwise
    ``AsyncIOScheduler.start()`` raises ``no running event loop``.
    """
    db_init()
    sched = scheduler.start_scheduler(bot=AsyncMock())
    try:
        jobs = sched.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        # Both guards present — dropped-tick regression catcher.
        assert job.misfire_grace_time and job.misfire_grace_time >= 60
        assert job.coalesce is True
    finally:
        scheduler.stop_scheduler()
