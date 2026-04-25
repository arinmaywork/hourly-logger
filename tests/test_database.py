"""Database layer: schema migrations, lock-protected writes, canonical timestamps.

Covers the three database-related fixes:

* Bug #1 — concurrent writes do not corrupt each other (smoke test).
* Bug #11 — every stored ``scheduled_ts`` matches :data:`CANONICAL_FMT`.
* Improvement #4 — duplicate ``scheduled_ts`` rows are impossible after migration.
* Improvement #6 — ``schema_migrations`` table records every applied version.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from hourly_logger import database
from hourly_logger.database import (
    CANONICAL_FMT,
    canonical_ts,
    db_init,
    queue_add_prompt,
    queue_add_prompt_sync,
    queue_count_pending,
    queue_get_oldest_pending,
    queue_mark_done_sync,
    queue_status_counts,
    queue_skipall_older_than_sync,
)


def test_canonical_ts_format_is_stable() -> None:
    ts = datetime(2026, 4, 24, 12, 30, 0, tzinfo=timezone.utc)
    assert canonical_ts(ts) == "2026-04-24T12:30:00Z"
    # Always normalises to UTC.
    from zoneinfo import ZoneInfo
    ist = datetime(2026, 4, 24, 18, 0, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
    assert canonical_ts(ist) == "2026-04-24T12:30:00Z"
    # Naive datetimes are assumed UTC.
    naive = datetime(2026, 4, 24, 12, 30, 0)
    assert canonical_ts(naive) == "2026-04-24T12:30:00Z"


def test_db_init_creates_schema_and_records_migrations(tmp_db_path: str) -> None:
    db_init()
    with database.db_connect() as conn:
        applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
        # Every migration we declared should have been recorded.
        assert {v for v, _ in database.MIGRATIONS} <= applied

        # Required tables / columns exist.
        cols = {row[1] for row in conn.execute("PRAGMA table_info(queue)")}
        for required in ("id", "scheduled_ts", "category", "tag", "note", "sheets_synced", "sync_attempts"):
            assert required in cols

        # Improvement #4: unique index on scheduled_ts.
        indexes = [row[1] for row in conn.execute("PRAGMA index_list(queue)")]
        assert "idx_scheduled_ts_unique" in indexes


def test_db_init_is_idempotent(tmp_db_path: str) -> None:
    db_init()
    db_init()  # second call must be a no-op (no duplicate migration rows).
    with database.db_connect() as conn:
        rows = conn.execute("SELECT version, COUNT(*) FROM schema_migrations GROUP BY version").fetchall()
        for _, n in rows:
            assert n == 1


def test_unique_index_blocks_duplicate_scheduled_ts(tmp_db_path: str) -> None:
    db_init()
    ts = datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(ts)
    # INSERT OR IGNORE quietly drops the second insert.
    queue_add_prompt_sync(ts)
    assert queue_count_pending() == 1
    # Direct insert (without OR IGNORE) must raise.
    with database.db_connect() as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO queue (scheduled_ts) VALUES (?)", (canonical_ts(ts),))


def test_stored_timestamp_matches_canonical_format(tmp_db_path: str) -> None:
    db_init()
    ts = datetime(2026, 4, 24, 9, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(ts)
    row = queue_get_oldest_pending()
    assert row is not None
    # Bug #11: every row's scheduled_ts is the canonical UTC string.
    parsed = datetime.strptime(row["scheduled_ts"], CANONICAL_FMT)
    assert parsed == ts.replace(tzinfo=None)


def test_status_counts_include_unsynced(tmp_db_path: str) -> None:
    db_init()
    ts = datetime(2026, 4, 24, 9, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(ts)
    row = queue_get_oldest_pending()
    assert row is not None
    queue_mark_done_sync(
        row["id"], "🟢 Creative", "Deep Work", "", datetime.now(timezone.utc),
        sheets_synced=False,
    )
    counts = queue_status_counts()
    assert counts == {"pending": 0, "done": 1, "skipped": 0, "unsynced": 1}


def test_skipall_older_than_skips_only_old_pending(tmp_db_path: str) -> None:
    db_init()
    old = datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc)
    today = datetime(2026, 4, 24, 8, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(old)
    queue_add_prompt_sync(today)
    boundary = datetime(2026, 4, 24, 0, 0, 0, tzinfo=timezone.utc)
    n = queue_skipall_older_than_sync(boundary)
    assert n == 1
    counts = queue_status_counts()
    assert counts["pending"] == 1
    assert counts["skipped"] == 1


# ── Migration v5: legacy +00:00 → canonical Z ───────────────────────────────


def _apply_migrations_through(conn: sqlite3.Connection, max_version: int) -> None:
    """Run schema migrations 1..max_version (exclusive of v5+) so we can
    seed legacy data and then explicitly invoke v5."""
    database._ensure_migrations_table(conn)
    for version, migrate in database.MIGRATIONS:
        if version > max_version:
            break
        conn.execute("BEGIN")
        migrate(conn)
        database._record(conn, version)
        conn.execute("COMMIT")


def test_migration_v5_rewrites_legacy_plus0000_to_canonical_z(tmp_db_path: str) -> None:
    """A legacy ``+00:00`` row with no canonical-Z twin must be rewritten in place."""
    with database.db_connect() as conn:
        _apply_migrations_through(conn, 4)
        # Insert a legacy-format row directly (bypassing canonical_ts()).
        conn.execute(
            "INSERT INTO queue (scheduled_ts, status, sheets_synced) "
            "VALUES (?, 'done', 1)",
            ("2026-04-01T04:30:00+00:00",),
        )
        conn.commit()

        database._migration_v5(conn)

        rows = conn.execute("SELECT scheduled_ts FROM queue").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "2026-04-01T04:30:00Z"


def test_migration_v5_drops_legacy_when_canonical_twin_exists(tmp_db_path: str) -> None:
    """When both legacy ``+00:00`` and canonical ``Z`` rows exist for the
    same instant (production state after Bug #11 + /repair), the legacy
    row must be dropped without raising IntegrityError."""
    with database.db_connect() as conn:
        _apply_migrations_through(conn, 4)
        conn.execute(
            "INSERT INTO queue (scheduled_ts, status, sheets_synced, category) "
            "VALUES (?, 'done', 1, 'legacy')",
            ("2026-04-01T04:30:00+00:00",),
        )
        conn.execute(
            "INSERT INTO queue (scheduled_ts, status, sheets_synced, category) "
            "VALUES (?, 'done', 1, 'canonical')",
            ("2026-04-01T04:30:00Z",),
        )
        conn.commit()

        database._migration_v5(conn)

        rows = conn.execute(
            "SELECT scheduled_ts, category FROM queue"
        ).fetchall()
        # Only the canonical-Z row survives; the legacy +00:00 is dropped.
        assert [(r[0], r[1]) for r in rows] == [("2026-04-01T04:30:00Z", "canonical")]


def test_migration_v5_is_idempotent_on_already_canonical_rows(tmp_db_path: str) -> None:
    """Re-running v5 on already-canonical rows must be a no-op."""
    with database.db_connect() as conn:
        _apply_migrations_through(conn, 4)
        conn.execute(
            "INSERT INTO queue (scheduled_ts, status) VALUES (?, 'done')",
            ("2026-04-01T04:30:00Z",),
        )
        conn.commit()

        database._migration_v5(conn)
        database._migration_v5(conn)  # second call must not change anything

        rows = conn.execute("SELECT scheduled_ts FROM queue").fetchall()
        assert [r[0] for r in rows] == ["2026-04-01T04:30:00Z"]


# ── Bug #1: concurrency smoke test ──────────────────────────────────────────


def test_concurrent_writes_do_not_corrupt(tmp_db_path: str) -> None:
    """Hammering the queue from many tasks at once must end up coherent.

    With the old single-connection / no-WAL layout this would
    intermittently raise ``database is locked``. Under the new layout
    (WAL + asyncio lock + per-call connections) every insert succeeds
    and the final count matches the number of distinct hours we tried
    to insert (INSERT OR IGNORE collapses true duplicates).
    """
    db_init()

    async def runner() -> None:
        base = datetime(2026, 4, 24, 0, 0, 0, tzinfo=timezone.utc)
        # 50 distinct hours, each inserted twice from concurrent tasks.
        tasks = []
        for i in range(50):
            ts = base + timedelta(hours=i)
            tasks.append(queue_add_prompt(ts))
            tasks.append(queue_add_prompt(ts))
        await asyncio.gather(*tasks)

    asyncio.run(runner())
    assert queue_count_pending() == 50
