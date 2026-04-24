"""Shared pytest fixtures.

Most tests need three things:
  1. A temporary SQLite path (so we never touch the real ``queue.db``).
  2. The ``settings`` singleton patched to a known minimal config.
  3. A fresh ``BotSession`` per test so state doesn't leak.

We set the required env vars BEFORE importing :mod:`hourly_logger.config`,
since pydantic-settings reads the environment at construction time.
"""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
from pathlib import Path
from typing import Iterator

import pytest


# ── Bootstrap: provide required env vars before any import ─────────────────
# This runs at collection time, well before any test imports the package.
os.environ.setdefault("TELEGRAM_TOKEN", "test-token-1234567890")
os.environ.setdefault("CHAT_ID", "1")
os.environ.setdefault("SPREADSHEET_ID", "test-spreadsheet-id-abcdef")
os.environ.setdefault("TIMEZONE", "UTC")
os.environ.setdefault("LOG_LEVEL", "WARNING")  # keep test output clean

# Ensure the project root is importable.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture
def tmp_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Point the ``settings.DB_PATH`` at a fresh sqlite file for the test.

    We monkeypatch the *attribute* on the cached singleton; the helper
    functions in :mod:`database` read ``settings.DB_PATH`` on every call,
    so the override takes effect immediately.
    """
    from hourly_logger.config import settings

    db_file = tmp_path / "queue.db"
    monkeypatch.setattr(settings, "DB_PATH", str(db_file))
    yield str(db_file)


@pytest.fixture
def fresh_session() -> Iterator["object"]:
    """Reset the global :data:`hourly_logger.state.session` between tests."""
    from hourly_logger import state
    importlib.reload(state)
    yield state.session
