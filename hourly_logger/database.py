"""SQLite layer with WAL, async-safe locking, and numbered migrations.

What this module fixes / introduces
-----------------------------------

* **Bug #1 (SQLite concurrency)** — every connection opens with
  ``PRAGMA journal_mode=WAL`` and ``PRAGMA busy_timeout``. All *write*
  paths funnel through ``_with_write_lock`` which serialises mutations on a
  single ``asyncio.Lock``. Reads stay lock-free.

* **Bug #11 (timestamp consistency)** — :func:`canonical_ts` produces a
  single canonical UTC string ("YYYY-MM-DDTHH:MM:SSZ") used for *every*
  ``scheduled_ts`` write and every range query parameter, so
  ``strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?`` comparisons are
  guaranteed apples-to-apples.

* **Improvement #4 (UNIQUE index on ``scheduled_ts``)** — created in
  migration v3. Future duplicates are impossible at the DB level;
  ``/dedup`` becomes a maintenance tool for legacy rows only.

* **Improvement #6 (numbered schema migrations)** — explicit
  ``MIGRATIONS`` list. Each runs at most once and records itself in the
  ``schema_migrations`` table. No more silent ``try/except ALTER TABLE``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Optional, cast
from zoneinfo import ZoneInfo

from .config import settings
from .logger import get_logger


log = get_logger(__name__)

# Single asyncio lock guards all writes. Reads run unlocked (WAL allows
# concurrent readers). We deliberately *don't* lock reads — that would
# serialise the whole bot for no benefit.
_write_lock = asyncio.Lock()


# ── Timestamp canonicalisation ──────────────────────────────────────────────

CANONICAL_FMT = "%Y-%m-%dT%H:%M:%SZ"


def canonical_ts(ts: datetime) -> str:
    """Return the canonical UTC string used in every DB write.

    Bug #11 fix: one format everywhere ("...Z" suffix, no microseconds).
    Existing legacy rows (with or without ``+00:00``) remain readable
    because every comparison uses ``strftime('%Y-%m-%dT%H:%M:%S', col)``.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime(CANONICAL_FMT)


def ts_param(ts: datetime) -> str:
    """Return a ``WHERE strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?`` parameter.

    Mirrors the strftime mask exactly — drops the ``Z``/``+00:00`` suffix.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def parse_ts(s: str) -> datetime:
    """Parse any timestamp variant we have ever stored, return a UTC datetime."""
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


# ── Connection management ───────────────────────────────────────────────────


@contextmanager
def db_connect() -> Iterator[sqlite3.Connection]:
    """Open a short-lived connection with safe pragmas applied.

    A new connection per call is fine for SQLite + WAL: the OS file handle
    is cheap and connections cannot be safely shared across asyncio tasks
    or threads. Writes are serialised by ``_write_lock`` at the call site.
    """
    dirname = os.path.dirname(settings.DB_PATH)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    conn = sqlite3.connect(
        settings.DB_PATH,
        timeout=30.0,             # block briefly if another writer holds the lock
        isolation_level=None,     # autocommit mode; we manage transactions manually
        check_same_thread=False,  # safe — we serialise via _write_lock
    )
    conn.row_factory = sqlite3.Row
    # WAL = concurrent readers + a single writer. The killer feature here.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
    finally:
        conn.close()


async def _run_blocking(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run a blocking DB callable in the default executor."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ── Migrations ──────────────────────────────────────────────────────────────


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations (
               version INTEGER PRIMARY KEY,
               applied_at TEXT NOT NULL
           )"""
    )


def _applied(conn: sqlite3.Connection) -> set[int]:
    return {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}


def _record(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        (version, canonical_ts(datetime.now(timezone.utc))),
    )


def _migration_v1(conn: sqlite3.Connection) -> None:
    """Initial schema. Mirrors the original CREATE TABLE so existing DBs
    are 'caught up' to v1 by the no-op IF NOT EXISTS."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS queue (
               id            INTEGER PRIMARY KEY AUTOINCREMENT,
               scheduled_ts  TEXT NOT NULL,
               submitted_ts  TEXT,
               category      TEXT,
               entry_text    TEXT,
               status        TEXT NOT NULL DEFAULT 'pending'
                             CHECK(status IN ('pending','done','skipped')),
               tag           TEXT,
               note          TEXT,
               sheets_synced INTEGER NOT NULL DEFAULT 1
           )"""
    )


def _migration_v2(conn: sqlite3.Connection) -> None:
    """Idempotently add columns missing from very old installations.

    Replaces the original ``try/except sqlite3.OperationalError: pass`` pattern
    with an explicit column-existence check.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(queue)")}
    for name, ddl in [
        ("category",      "ALTER TABLE queue ADD COLUMN category TEXT"),
        ("tag",           "ALTER TABLE queue ADD COLUMN tag TEXT"),
        ("note",          "ALTER TABLE queue ADD COLUMN note TEXT"),
        ("sheets_synced", "ALTER TABLE queue ADD COLUMN sheets_synced INTEGER NOT NULL DEFAULT 1"),
    ]:
        if name not in cols:
            conn.execute(ddl)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON queue(status)")


def _migration_v3(conn: sqlite3.Connection) -> None:
    """Improvement #4: prevent duplicate scheduled_ts at the DB level.

    There is one historical caveat — older installations *might* contain
    duplicate ``scheduled_ts`` rows (one ``done`` and one ``pending``).
    We resolve those before adding the unique index so the migration never
    fails on a real-world DB. Resolution rule: keep the most informative
    row (status priority done > skipped > pending).
    """
    status_priority = "CASE status WHEN 'done' THEN 3 WHEN 'skipped' THEN 2 ELSE 1 END"
    conn.execute(
        f"""DELETE FROM queue
            WHERE id NOT IN (
                SELECT id FROM queue q1
                WHERE id = (
                    SELECT q2.id FROM queue q2
                    WHERE q2.scheduled_ts = q1.scheduled_ts
                    ORDER BY {status_priority} DESC, q2.id DESC
                    LIMIT 1
                )
            )"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduled_ts_unique ON queue(scheduled_ts)"
    )


def _migration_v4(conn: sqlite3.Connection) -> None:
    """Add a retry counter so the circuit breaker can tell the user when
    a row has exhausted its budget (improvement #9)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(queue)")}
    if "sync_attempts" not in cols:
        conn.execute("ALTER TABLE queue ADD COLUMN sync_attempts INTEGER NOT NULL DEFAULT 0")


def _migration_v5(conn: sqlite3.Connection) -> None:
    """Normalise legacy scheduled_ts ``+00:00`` suffixes to canonical ``Z``.

    Bug #11 fixed canonical_ts() to always emit ``...Z`` going forward,
    but legacy rows written before that change still carry ``+00:00``
    or ``+00:00:00`` suffixes. Range queries normalise both sides via
    ``strftime``, but raw-string set comparisons (notably /repair) do
    not — leading to /repair flagging hundreds of already-synced rows
    as "missing from Sheet" forever.

    Two failure modes to handle:

    1. **Plain UPDATE**: legacy row has no canonical-Z twin. Just rewrite
       the suffix.
    2. **UPDATE collides on UNIQUE(scheduled_ts)**: a canonical-Z row
       already exists for the same instant. This happened in production
       when /repair re-pulled rows from the Sheet AFTER Bug #11 was fixed
       — the new insert went in with ``Z`` suffix, the original ``+00:00``
       row stayed put, and SQLite treated them as distinct. Both rows
       refer to the same Sheet row (same ts, both ``sheets_synced=1``),
       so the canonical-Z row is the round-tripped-from-Sheet view and
       the legacy row is redundant. Drop the legacy row.

    Idempotent: only touches rows whose suffix differs from canonical.
    """
    rows = conn.execute(
        "SELECT id, scheduled_ts, submitted_ts FROM queue"
    ).fetchall()
    fixed = 0
    dropped = 0
    for rid, sched, sub in rows:
        new_sched = sched
        new_sub = sub
        # Reuse parse_ts/canonical_ts for both columns. Skip rows whose
        # suffix already matches to keep the migration cheap on re-runs.
        try:
            canon_sched = canonical_ts(parse_ts(sched))
            if canon_sched != sched:
                new_sched = canon_sched
        except Exception:
            pass
        if sub:
            try:
                canon_sub = canonical_ts(parse_ts(sub))
                if canon_sub != sub:
                    new_sub = canon_sub
            except Exception:
                pass
        if new_sched == sched and new_sub == sub:
            continue
        try:
            conn.execute(
                "UPDATE queue SET scheduled_ts=?, submitted_ts=? WHERE id=?",
                (new_sched, new_sub, rid),
            )
            fixed += 1
        except sqlite3.IntegrityError:
            # The canonical-Z twin already exists. Keep it (it's the
            # round-tripped-from-Sheet view) and drop the legacy row.
            conn.execute("DELETE FROM queue WHERE id=?", (rid,))
            dropped += 1
    log.info(
        "v5 normalised legacy timestamps",
        extra={"rewritten": fixed, "dropped_dupes": dropped},
    )


MIGRATIONS: list[tuple[int, Callable[[sqlite3.Connection], None]]] = [
    (1, _migration_v1),
    (2, _migration_v2),
    (3, _migration_v3),
    (4, _migration_v4),
    (5, _migration_v5),
]


def db_init() -> None:
    """Run any pending migrations. Safe to call on every boot."""
    with db_connect() as conn:
        _ensure_migrations_table(conn)
        applied = _applied(conn)
        for version, migrate in MIGRATIONS:
            if version in applied:
                continue
            log.info("applying migration", extra={"version": version})
            try:
                conn.execute("BEGIN")
                migrate(conn)
                _record(conn, version)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
    log.info("database ready", extra={"path": settings.DB_PATH})


# ── Queue ops (sync) ────────────────────────────────────────────────────────
#
# Each function exists in two flavours:
#   * ``*_sync`` — blocking, safe to call from sync code (migrations, tests).
#   * the bare name — async wrapper that takes the write lock when needed.


def queue_add_prompt_sync(scheduled_ts: datetime) -> None:
    """Insert a pending prompt, ignore if a row for that hour already exists."""
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO queue (scheduled_ts) VALUES (?)",
            (canonical_ts(scheduled_ts),),
        )


async def queue_add_prompt(scheduled_ts: datetime) -> None:
    async with _write_lock:
        await _run_blocking(queue_add_prompt_sync, scheduled_ts)


def queue_get_oldest_pending() -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM queue WHERE status='pending' "
            "ORDER BY scheduled_ts ASC LIMIT 1"
        ).fetchone()
        return cast(Optional[sqlite3.Row], row)


def queue_count_pending() -> int:
    with db_connect() as conn:
        n = conn.execute("SELECT COUNT(*) FROM queue WHERE status='pending'").fetchone()[0]
        return cast(int, n)


def queue_get_recent_done(limit: int = 5) -> list[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM queue WHERE status IN ('done','skipped') "
            "ORDER BY scheduled_ts DESC LIMIT ?",
            (limit,),
        ).fetchall()


def queue_get_by_date(date: dt.date, tz: ZoneInfo) -> list[sqlite3.Row]:
    """Return done/skipped entries whose scheduled time falls on the
    log-day ``date`` (Bug #4: hours 0-6 belong to the *previous*
    calendar date in the grid, so this query now uses log-day bounds
    rather than calendar-day midnight-to-midnight)."""
    from .dates import log_day_bounds  # local import: dates imports config
    start_utc, end_utc = log_day_bounds(date, tz)
    with db_connect() as conn:
        return conn.execute(
            """SELECT * FROM queue
               WHERE status IN ('done','skipped')
                 AND scheduled_ts >= ?
                 AND scheduled_ts <= ?
               ORDER BY scheduled_ts ASC""",
            (canonical_ts(start_utc), canonical_ts(end_utc)),
        ).fetchall()


def queue_get_by_id(row_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        row = conn.execute("SELECT * FROM queue WHERE id=?", (row_id,)).fetchone()
        return cast(Optional[sqlite3.Row], row)


def queue_get_by_scheduled_ts(scheduled_ts: datetime) -> Optional[sqlite3.Row]:
    """Return the queue row whose scheduled_ts matches, or ``None``.

    Used by the explicit-timestamp form of ``/log`` to detect whether
    the slot already exists (pending → mark done; done → reject; missing
    → insert placeholder then mark done). Comparison is on the canonical
    string form so legacy ``+00:00`` rows don't sneak through — though
    after migration v5 those are all rewritten to ``Z`` anyway.
    """
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM queue WHERE scheduled_ts=?",
            (canonical_ts(scheduled_ts),),
        ).fetchone()
        return cast(Optional[sqlite3.Row], row)


def queue_get_unfilled_window(
    start_utc: datetime, end_utc: datetime
) -> list[sqlite3.Row]:
    """Return rows in ``[start_utc, end_utc]`` whose status is ``pending`` or
    ``skipped`` — i.e. hours that the user never filled in.

    Used by ``/missing`` so the user can see and re-prompt forgotten or
    auto-skipped slots from Telegram instead of editing the Sheet by hand.
    """
    with db_connect() as conn:
        return conn.execute(
            """SELECT * FROM queue
               WHERE status IN ('pending','skipped')
                 AND scheduled_ts >= ?
                 AND scheduled_ts <= ?
               ORDER BY scheduled_ts ASC""",
            (canonical_ts(start_utc), canonical_ts(end_utc)),
        ).fetchall()


def queue_get_done_in_window(
    start_utc: datetime, end_utc: datetime
) -> list[sqlite3.Row]:
    """Return ``done`` rows in ``[start_utc, end_utc]``. Used by /repair to
    diff DB ↔ Sheets."""
    with db_connect() as conn:
        return conn.execute(
            """SELECT * FROM queue
               WHERE status='done'
                 AND scheduled_ts >= ?
                 AND scheduled_ts <= ?
               ORDER BY scheduled_ts ASC""",
            (canonical_ts(start_utc), canonical_ts(end_utc)),
        ).fetchall()


def queue_get_all_scheduled_ts() -> set[str]:
    """Return the set of all scheduled_ts values in the queue (canonical
    UTC strings). Used by /repair to detect rows present in the Sheet but
    missing from local DB."""
    with db_connect() as conn:
        rows = conn.execute("SELECT scheduled_ts FROM queue").fetchall()
    return {r[0] for r in rows}


def queue_insert_done_row_sync(
    scheduled_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str,
    sheets_synced: bool,
) -> bool:
    """Insert a fully-formed ``done`` row. Used by /repair to ingest a
    Sheet-only entry into the local DB. Returns True if the row was
    inserted, False if a conflicting scheduled_ts already existed (the
    UNIQUE index from migration v3 enforces one row per hour)."""
    with db_connect() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO queue
               (scheduled_ts, submitted_ts, category, tag, note, status, sheets_synced)
               VALUES (?, ?, ?, ?, ?, 'done', ?)""",
            (
                canonical_ts(scheduled_ts),
                canonical_ts(submitted_ts),
                category,
                tag,
                note,
                1 if sheets_synced else 0,
            ),
        )
        return bool(cur.rowcount)


async def queue_insert_done_row(
    scheduled_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str,
    sheets_synced: bool,
) -> bool:
    async with _write_lock:
        return cast(bool, await _run_blocking(
            queue_insert_done_row_sync,
            scheduled_ts, submitted_ts, category, tag, note, sheets_synced,
        ))


def queue_mark_unsynced_sync(row_id: int) -> None:
    """Flip a ``done`` row back to ``sheets_synced=0`` so the next /sync
    re-attempts it. Used by /repair when a DB row has no matching Sheet
    row."""
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET sheets_synced=0 WHERE id=? AND status='done'",
            (row_id,),
        )


async def queue_mark_unsynced(row_id: int) -> None:
    async with _write_lock:
        await _run_blocking(queue_mark_unsynced_sync, row_id)


def queue_mark_done_sync(
    row_id: int,
    category: str,
    tag: str,
    note: str,
    submitted_ts: datetime,
    sheets_synced: bool = False,
) -> None:
    combined = f"{tag} | {note}" if note else tag
    with db_connect() as conn:
        conn.execute(
            """UPDATE queue
               SET status='done', category=?, tag=?, note=?, entry_text=?,
                   submitted_ts=?, sheets_synced=?, sync_attempts=0
               WHERE id=?""",
            (category, tag, note, combined, canonical_ts(submitted_ts),
             int(sheets_synced), row_id),
        )


async def queue_mark_done(
    row_id: int,
    category: str,
    tag: str,
    note: str,
    submitted_ts: datetime,
    sheets_synced: bool = False,
) -> None:
    async with _write_lock:
        await _run_blocking(
            queue_mark_done_sync, row_id, category, tag, note, submitted_ts, sheets_synced,
        )


def queue_mark_skipped_sync(row_id: int) -> None:
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET status='skipped', submitted_ts=? WHERE id=?",
            (canonical_ts(datetime.now(timezone.utc)), row_id),
        )


async def queue_mark_skipped(row_id: int) -> None:
    async with _write_lock:
        await _run_blocking(queue_mark_skipped_sync, row_id)


def queue_mark_sheets_synced_sync(row_id: int, synced: bool) -> None:
    with db_connect() as conn:
        conn.execute(
            "UPDATE queue SET sheets_synced=? WHERE id=?",
            (int(synced), row_id),
        )


async def queue_mark_sheets_synced(row_id: int, synced: bool) -> None:
    async with _write_lock:
        await _run_blocking(queue_mark_sheets_synced_sync, row_id, synced)


def queue_increment_sync_attempt_sync(row_id: int) -> int:
    """Atomically bump and return the new attempt count."""
    with db_connect() as conn:
        conn.execute("UPDATE queue SET sync_attempts = sync_attempts + 1 WHERE id=?", (row_id,))
        cur = conn.execute("SELECT sync_attempts FROM queue WHERE id=?", (row_id,)).fetchone()
        return cur[0] if cur else 0


async def queue_increment_sync_attempt(row_id: int) -> int:
    async with _write_lock:
        return cast(int, await _run_blocking(queue_increment_sync_attempt_sync, row_id))


def queue_get_unsynced() -> list[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM queue WHERE status='done' AND sheets_synced=0 ORDER BY scheduled_ts ASC"
        ).fetchall()


def queue_skipall_older_than_sync(boundary_local_midnight_utc: datetime) -> int:
    with db_connect() as conn:
        cur = conn.execute(
            "UPDATE queue SET status='skipped', submitted_ts=? "
            "WHERE status='pending' AND scheduled_ts < ?",
            (canonical_ts(datetime.now(timezone.utc)),
             canonical_ts(boundary_local_midnight_utc)),
        )
        return cur.rowcount


async def queue_skipall_older_than(boundary_local_midnight_utc: datetime) -> int:
    async with _write_lock:
        return cast(int, await _run_blocking(queue_skipall_older_than_sync, boundary_local_midnight_utc))


def queue_status_counts() -> dict[str, int]:
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT status, COUNT(*) FROM queue GROUP BY status"""
        ).fetchall()
        unsynced = conn.execute(
            "SELECT COUNT(*) FROM queue WHERE status='done' AND sheets_synced=0"
        ).fetchone()[0]
    counts = {"pending": 0, "done": 0, "skipped": 0, "unsynced": unsynced}
    for status, n in rows:
        counts[status] = n
    return counts


def queue_category_breakdown(since_ts: datetime) -> tuple[dict[str, int], int]:
    """Return ``(breakdown, total_done)`` for done entries on or after
    ``since_ts``.

    ``breakdown`` excludes NULL-category rows; ``total_done`` does not, so
    callers can detect uncategorised drift.
    """
    param = ts_param(since_ts)
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT category, COUNT(*) AS cnt FROM queue
               WHERE status='done'
                 AND strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?
               GROUP BY category ORDER BY cnt DESC""",
            (param,),
        ).fetchall()
        total_done = conn.execute(
            """SELECT COUNT(*) FROM queue
               WHERE status='done'
                 AND strftime('%Y-%m-%dT%H:%M:%S', scheduled_ts) >= ?""",
            (param,),
        ).fetchone()[0]
    breakdown = {row["category"]: row["cnt"] for row in rows if row["category"]}
    return breakdown, total_done


def backfill_missed_prompts_sync(now_utc: Optional[datetime] = None) -> int:
    """Insert one pending row per missed hour up to ``now_utc``.

    Origin selection:

      * If ``SERVICE_START_DATE`` is set, the origin is the later of the
        DB's ``MAX(scheduled_ts)`` and ``SERVICE_START_DATE`` at
        ``LOG_DAY_START_HOUR`` local time. So a freshly reset DB on a
        long-running service still gets the historical hours backfilled
        as ``pending`` (where they then surface in /missing).
      * If ``SERVICE_START_DATE`` is unset and the DB is empty, returns 0
        (the original behaviour — nothing to extrapolate from).

    The total insert count is hard-capped by ``BACKFILL_MAX_HOURS`` so a
    misconfigured ``SERVICE_START_DATE`` from years ago can't pin the
    process inserting hundreds of thousands of rows.

    Safe to call repeatedly — every insert is ``INSERT OR IGNORE`` and
    the UNIQUE INDEX on ``scheduled_ts`` enforces one row per hour.
    """
    now_utc = (now_utc or datetime.now(timezone.utc)).replace(
        minute=0, second=0, microsecond=0
    )
    with db_connect() as conn:
        last = conn.execute("SELECT MAX(scheduled_ts) FROM queue").fetchone()[0]

    # Local import — avoids a circular at module load time.
    from .dates import log_day_bounds

    db_origin: Optional[datetime] = parse_ts(last) if last else None
    cfg_origin: Optional[datetime] = None
    if settings.SERVICE_START_DATE is not None:
        cfg_origin, _ = log_day_bounds(settings.SERVICE_START_DATE, settings.tz)
        # Service-start gives us the *first* hour to insert; subtract one
        # hour so the loop's ``current = origin + 1h`` lands on it.
        cfg_origin -= dt.timedelta(hours=1)

    if db_origin is None and cfg_origin is None:
        return 0
    if db_origin is None:
        origin = cfg_origin
    elif cfg_origin is None:
        origin = db_origin
    else:
        origin = max(db_origin, cfg_origin)
    assert origin is not None  # for the type checker

    current = origin + dt.timedelta(hours=1)
    inserted = 0
    cap = settings.BACKFILL_MAX_HOURS
    while current <= now_utc and inserted < cap:
        with db_connect() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO queue (scheduled_ts) VALUES (?)",
                (canonical_ts(current),),
            )
            inserted += cur.rowcount
        current += dt.timedelta(hours=1)
    if inserted:
        log.info(
            "backfilled missed prompts",
            extra={"count": inserted, "capped": inserted >= cap},
        )
    return inserted


async def backfill_missed_prompts() -> int:
    async with _write_lock:
        return cast(int, await _run_blocking(backfill_missed_prompts_sync))
