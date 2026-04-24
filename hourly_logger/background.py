"""Tracked background tasks.

Bug #3 fix: ``asyncio.create_task(coro)`` swallows exceptions. Replace
every fire-and-forget call site with :func:`spawn`, which:

  1. Adds the task to a module-level set so the GC cannot kill it
     mid-flight (Python's "stored task" warning).
  2. Attaches a done-callback that *forwards* any uncaught exception to
     a user-provided notifier — the bot can then DM the owner so they
     know to run ``/sync``.
  3. Records the task name + start time so :func:`shutdown` can wait for
     in-flight syncs to drain on graceful exit.

Improvement #3: the registry also exposes :func:`pending_count` for
``/status`` and :func:`shutdown` for ``post_stop``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine, Optional

from .logger import get_logger


log = get_logger(__name__)

# Type alias: an async callable that takes the failing task name + the
# exception. Returning the awaitable lets the notifier DM the owner.
Notifier = Callable[[str, BaseException], Coroutine[Any, Any, None]]


class _Tracker:
    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[object]] = set()
        self._started_at: dict[asyncio.Task[object], float] = {}
        self._notifier: Optional[Notifier] = None

    def set_notifier(self, notifier: Notifier) -> None:
        """Install the function called when a tracked task raises."""
        self._notifier = notifier

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str
    ) -> asyncio.Task[Any]:
        """Schedule a tracked background task."""
        task: asyncio.Task[Any] = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        self._started_at[task] = time.monotonic()
        task.add_done_callback(self._on_done)
        return task

    def pending_count(self) -> int:
        return sum(1 for t in self._tasks if not t.done())

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Await all in-flight tasks (best effort) before the loop closes."""
        pending = [t for t in self._tasks if not t.done()]
        if not pending:
            return
        log.info("draining background tasks", extra={"count": len(pending)})
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning("background drain timed out", extra={"after_s": timeout})

    # ── Internals ───────────────────────────────────────────────────────

    def _on_done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        elapsed = time.monotonic() - self._started_at.pop(task, time.monotonic())
        if task.cancelled():
            log.info("bg task cancelled", extra={"task_name": task.get_name(), "elapsed_s": elapsed})
            return
        exc = task.exception()
        if exc is None:
            log.info("bg task ok", extra={"task_name": task.get_name(), "elapsed_s": round(elapsed, 2)})
            return
        log.error(
            "bg task failed",
            extra={"task_name": task.get_name(), "elapsed_s": round(elapsed, 2)},
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        if self._notifier is not None:
            # Schedule the notifier as another tracked task so its own
            # failure also surfaces (it must not throw, but defensive).
            try:
                asyncio.get_running_loop()
                self.spawn(
                    self._notifier(task.get_name(), exc),
                    name=f"notify:{task.get_name()}",
                )
            except RuntimeError:
                # Loop closed (e.g. during shutdown). Nothing to do.
                pass


tracker = _Tracker()


# Convenience proxies — handlers should import these directly.

def spawn(coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task[Any]:
    return tracker.spawn(coro, name=name)


def set_notifier(notifier: Notifier) -> None:
    tracker.set_notifier(notifier)


def pending_count() -> int:
    return tracker.pending_count()


async def shutdown(timeout: float = 30.0) -> None:
    await tracker.shutdown(timeout=timeout)
