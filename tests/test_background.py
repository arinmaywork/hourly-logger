"""Background task tracker — Bug #3 + Improvement #3."""

from __future__ import annotations

import asyncio

import pytest

from hourly_logger.background import _Tracker


@pytest.mark.asyncio
async def test_successful_task_is_cleaned_up() -> None:
    t = _Tracker()
    done = asyncio.Event()

    async def work() -> None:
        await asyncio.sleep(0)
        done.set()

    task = t.spawn(work(), name="ok")
    await task
    assert done.is_set()
    # Allow done callbacks to drain.
    await asyncio.sleep(0)
    assert t.pending_count() == 0


@pytest.mark.asyncio
async def test_failing_task_invokes_notifier() -> None:
    """The killer property: a fire-and-forget task that raises must
    cause the registered notifier to be invoked. This is exactly the
    behaviour Bug #3 was missing in the original code."""
    t = _Tracker()
    notifications: list[tuple[str, BaseException]] = []

    async def notifier(name: str, exc: BaseException) -> None:
        notifications.append((name, exc))

    t.set_notifier(notifier)

    async def boom() -> None:
        raise RuntimeError("kaboom")

    task = t.spawn(boom(), name="boom-task")
    # Wait for the failing task to finish without raising in our caller.
    with pytest.raises(RuntimeError):
        await task
    # The done-callback schedules the notifier as a tracked sub-task; let it run.
    await asyncio.sleep(0.05)
    await t.shutdown(timeout=1.0)
    assert len(notifications) == 1
    name, exc = notifications[0]
    assert name == "boom-task"
    assert isinstance(exc, RuntimeError)


@pytest.mark.asyncio
async def test_shutdown_drains_in_flight_tasks() -> None:
    t = _Tracker()
    done = asyncio.Event()

    async def slow() -> None:
        await asyncio.sleep(0.05)
        done.set()

    t.spawn(slow(), name="slow")
    await t.shutdown(timeout=1.0)
    assert done.is_set()
