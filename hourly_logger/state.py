"""Bot session state — replaces the global ``current_prompt`` dict.

Bug #2 fix: every read/write of the in-flight entry goes through this
class, which is guarded by an ``asyncio.Lock``. The hourly scheduler
callback now calls :meth:`try_begin_prompt` which is a no-op if the user
is mid-entry, so the in-progress state can never be silently overwritten.

Improvement #2: replaces three module-level globals with one cohesive
object. Tests can construct a fresh ``BotSession`` and inject it.
"""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional


# Stage labels — string-literal constants for readability and grep-ability.
STAGE_CATEGORY = "category"
STAGE_TAG_NOTE = "tag_note"
STAGE_EDIT_SELECTION = "edit_selection"


@dataclass
class PromptState:
    """In-memory state for the current multi-step entry."""

    queue_id: int
    scheduled_ts: str
    stage: str
    is_edit: bool = False
    category: Optional[str] = None
    tag: Optional[str] = None
    # Used only for STAGE_EDIT_SELECTION.
    recent_ids: list[int] = field(default_factory=list)
    recent_labels: list[str] = field(default_factory=list)


class BotSession:
    """Owner-scoped state. One instance per bot process.

    All mutations happen under :attr:`_lock`. Reads outside the lock are
    safe because Python attribute access on a single object is atomic and
    we treat the object as read-mostly between writes.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._prompt: Optional[PromptState] = None

    # ── Read API (lock-free, atomic-enough for our purposes) ────────────
    @property
    def is_idle(self) -> bool:
        return self._prompt is None

    @property
    def prompt(self) -> Optional[PromptState]:
        return self._prompt

    @property
    def stage(self) -> Optional[str]:
        return self._prompt.stage if self._prompt else None

    # ── Write API ───────────────────────────────────────────────────────

    async def clear(self) -> None:
        async with self._lock:
            self._prompt = None

    async def try_begin_prompt(
        self,
        queue_row: sqlite3.Row,
        is_edit: bool = False,
    ) -> bool:
        """Bug #2 fix: only start a new prompt when truly idle.

        Returns ``True`` if we claimed the slot, ``False`` if a prompt
        was already in progress and we left it untouched. Callers must
        not send the chat message until they have ``True``.

        For edits the call is *forced* (an explicit user action), so we
        clear any existing state first.
        """
        async with self._lock:
            if is_edit:
                self._prompt = self._build_initial(queue_row, is_edit=True)
                return True
            if self._prompt is not None:
                return False
            self._prompt = self._build_initial(queue_row, is_edit=False)
            return True

    async def advance_to_tag_note(self, category: str) -> None:
        async with self._lock:
            if self._prompt is None:
                return
            self._prompt.category = category
            self._prompt.stage = STAGE_TAG_NOTE

    async def begin_edit_selection(
        self,
        recent_ids: list[int],
        recent_labels: list[str],
    ) -> None:
        async with self._lock:
            self._prompt = PromptState(
                queue_id=0,
                scheduled_ts="",
                stage=STAGE_EDIT_SELECTION,
                recent_ids=recent_ids,
                recent_labels=recent_labels,
            )

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Optional[PromptState]]:
        """Borrow exclusive access to the state for atomic read-modify-write.

        Useful when a handler needs both consistency *and* to mutate the
        state based on what it reads (e.g. validating the user's reply
        against the recorded recent_labels).
        """
        async with self._lock:
            yield self._prompt

    # ── Internals ───────────────────────────────────────────────────────

    @staticmethod
    def _build_initial(queue_row: sqlite3.Row, is_edit: bool) -> PromptState:
        return PromptState(
            queue_id=queue_row["id"],
            scheduled_ts=queue_row["scheduled_ts"],
            stage=STAGE_CATEGORY,
            is_edit=is_edit,
        )


# Module-level singleton for the running bot. Tests can construct their own.
session = BotSession()
