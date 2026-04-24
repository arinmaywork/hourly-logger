"""BotSession concurrency — proves Bug #2 cannot recur.

The original bug: while the user was mid-entry, the hourly scheduler
fired ``send_prompt`` which overwrote ``current_prompt`` and lost the
in-flight state. With :meth:`BotSession.try_begin_prompt` returning
``False`` when busy, the scheduler is now safely a no-op.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from hourly_logger.state import (
    STAGE_CATEGORY,
    STAGE_TAG_NOTE,
    BotSession,
)


class _RowStub:
    """Mimic the sqlite3.Row interface enough for BotSession."""

    def __init__(self, qid: int, ts: str) -> None:
        self._d = {"id": qid, "scheduled_ts": ts}

    def __getitem__(self, key: str) -> Any:
        return self._d[key]


async def _begin(s: BotSession, qid: int) -> bool:
    return await s.try_begin_prompt(_RowStub(qid, "2026-04-24T12:00:00Z"))


@pytest.mark.asyncio
async def test_try_begin_prompt_is_idle_then_busy() -> None:
    s = BotSession()
    assert s.is_idle
    assert await _begin(s, qid=1) is True
    assert not s.is_idle
    # Second call must not overwrite — Bug #2 guard.
    assert await _begin(s, qid=2) is False
    assert s.prompt is not None
    assert s.prompt.queue_id == 1


@pytest.mark.asyncio
async def test_concurrent_try_begin_prompt_only_one_wins() -> None:
    s = BotSession()
    results = await asyncio.gather(*[_begin(s, qid=i) for i in range(20)])
    assert results.count(True) == 1
    assert results.count(False) == 19


@pytest.mark.asyncio
async def test_advance_to_tag_note_updates_state() -> None:
    s = BotSession()
    await _begin(s, qid=42)
    assert s.stage == STAGE_CATEGORY
    await s.advance_to_tag_note("🟢 Creative")
    assert s.stage == STAGE_TAG_NOTE
    assert s.prompt is not None
    assert s.prompt.category == "🟢 Creative"


@pytest.mark.asyncio
async def test_clear_resets_to_idle() -> None:
    s = BotSession()
    await _begin(s, qid=1)
    await s.clear()
    assert s.is_idle


@pytest.mark.asyncio
async def test_edit_force_overrides_existing_state() -> None:
    s = BotSession()
    await _begin(s, qid=1)
    # An explicit /edit must claim the slot even when something else is in flight.
    forced = await s.try_begin_prompt(
        _RowStub(99, "2026-04-24T13:00:00Z"), is_edit=True,
    )
    assert forced is True
    assert s.prompt is not None
    assert s.prompt.queue_id == 99
    assert s.prompt.is_edit is True
