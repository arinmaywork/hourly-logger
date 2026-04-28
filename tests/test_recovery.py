"""Recovery helpers — /missing and /repair backing logic.

Covers the gap surfaced by the user: entries that get auto-skipped or
manually edited in the Sheet need to be visible and fixable from
Telegram. These tests exercise the database primitives that back the
new ``cmd_missing`` and ``cmd_repair`` handlers.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hourly_logger.database import (
    canonical_ts,
    db_init,
    db_connect,
    queue_add_prompt_sync,
    queue_get_all_scheduled_ts,
    queue_get_done_in_window,
    queue_get_unfilled_window,
    queue_insert_done_row_sync,
    queue_mark_done_sync,
    queue_mark_skipped_sync,
    queue_mark_unsynced_sync,
    queue_materialize_window_sync,
)


# ── /missing backing helper ─────────────────────────────────────────────────


def test_unfilled_window_returns_pending_and_skipped(tmp_db_path: str) -> None:
    db_init()
    base = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    # Hours 10-13.
    for h in range(4):
        queue_add_prompt_sync(base + timedelta(hours=h))
    # Mark 11 done, 12 skipped — leaving 10 + 13 pending and 12 skipped.
    rows_before = queue_get_unfilled_window(base, base + timedelta(hours=4))
    ids = {r["scheduled_ts"]: r["id"] for r in rows_before}
    queue_mark_done_sync(
        ids[canonical_ts(base + timedelta(hours=1))],
        "🟢 Creative", "Tag", "", base + timedelta(hours=1, minutes=15), False,
    )
    queue_mark_skipped_sync(ids[canonical_ts(base + timedelta(hours=2))])

    rows = queue_get_unfilled_window(base, base + timedelta(hours=4))
    statuses = sorted(r["status"] for r in rows)
    # 10 pending, 12 skipped, 13 pending.
    assert statuses == ["pending", "pending", "skipped"]


def test_unfilled_window_excludes_done_rows(tmp_db_path: str) -> None:
    db_init()
    ts = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(ts)
    row = queue_get_unfilled_window(ts, ts)[0]
    queue_mark_done_sync(row["id"], "🟢 Creative", "Tag", "", ts + timedelta(minutes=10), False)

    assert queue_get_unfilled_window(ts, ts) == []


def test_unfilled_window_respects_bounds(tmp_db_path: str) -> None:
    db_init()
    earlier = datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc)
    inside = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    later = datetime(2026, 4, 25, 10, 0, 0, tzinfo=timezone.utc)
    for ts in (earlier, inside, later):
        queue_add_prompt_sync(ts)

    rows = queue_get_unfilled_window(
        datetime(2026, 4, 24, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 24, 23, 59, 59, tzinfo=timezone.utc),
    )
    assert len(rows) == 1
    assert rows[0]["scheduled_ts"] == canonical_ts(inside)


# ── Ghost-gap materialisation (the /missing fix) ───────────────────────────


def test_materialize_window_inserts_only_missing_hours(tmp_db_path: str) -> None:
    """Walk a window and insert pending placeholders for every hour that
    has no row yet — leaving existing rows (any status) untouched.

    Reproduces the user-visible bug: APScheduler dropped a tick at
    2026-04-27 14:00 UTC, so no row existed for that hour. /missing
    couldn't see it (status-driven query). After materialisation the
    ghost hour appears as pending and /missing surfaces it.
    """
    db_init()
    base = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    # Seed only 12, 13, 15, 16 — leaving 14:00 as a ghost gap.
    for h in (0, 1, 3, 4):
        queue_add_prompt_sync(base + timedelta(hours=h))
    # Mark 13 done so we can prove materialise doesn't disturb it.
    rows = queue_get_unfilled_window(base, base + timedelta(hours=4))
    done_id = next(
        r["id"] for r in rows
        if r["scheduled_ts"] == canonical_ts(base + timedelta(hours=1))
    )
    queue_mark_done_sync(
        done_id, "🟢 Creative", "Tag", "", base + timedelta(hours=1, minutes=10), False,
    )

    inserted = queue_materialize_window_sync(base, base + timedelta(hours=4))
    assert inserted == 1  # only the 14:00 ghost gap

    # All 5 hours now exist; the ghost is pending; the done row is still done.
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT scheduled_ts, status FROM queue ORDER BY scheduled_ts"
        ).fetchall()
    assert [r[0] for r in rows] == [
        canonical_ts(base + timedelta(hours=h)) for h in range(5)
    ]
    by_ts = {r[0]: r[1] for r in rows}
    assert by_ts[canonical_ts(base + timedelta(hours=1))] == "done"
    assert by_ts[canonical_ts(base + timedelta(hours=2))] == "pending"  # the ghost


def test_materialize_window_is_idempotent(tmp_db_path: str) -> None:
    """A second call inserts zero rows — UNIQUE INDEX guards re-runs."""
    db_init()
    base = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)

    first = queue_materialize_window_sync(base, base + timedelta(hours=2))
    assert first == 3  # 12, 13, 14
    second = queue_materialize_window_sync(base, base + timedelta(hours=2))
    assert second == 0


def test_materialize_window_snaps_partial_bounds(tmp_db_path: str) -> None:
    """Start with a non-zero minute → snap *up* to next top-of-hour.
    End with a non-zero minute → snap *down* (don't fabricate future).

    Catches a regression where /missing on Tue 28 Apr 09:43 IST
    (= 04:13 UTC) might otherwise insert a phantom 04:00 UTC row even
    though that hour hasn't fully elapsed in the user's experience.
    """
    db_init()
    # Window: 04:13 → 06:47 UTC.
    start = datetime(2026, 4, 28, 4, 13, tzinfo=timezone.utc)
    end = datetime(2026, 4, 28, 6, 47, tzinfo=timezone.utc)
    inserted = queue_materialize_window_sync(start, end)
    # Expect 05:00 and 06:00 only (start snapped up to 05:00, end snapped down to 06:00).
    assert inserted == 2
    with db_connect() as conn:
        rows = [r[0] for r in conn.execute(
            "SELECT scheduled_ts FROM queue ORDER BY scheduled_ts"
        )]
    assert rows == [
        canonical_ts(datetime(2026, 4, 28, 5, 0, tzinfo=timezone.utc)),
        canonical_ts(datetime(2026, 4, 28, 6, 0, tzinfo=timezone.utc)),
    ]


def test_materialize_window_empty_when_start_after_end(tmp_db_path: str) -> None:
    """Bounds in the wrong order are a no-op, not a crash."""
    db_init()
    base = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    assert queue_materialize_window_sync(base + timedelta(hours=2), base) == 0


# ── /repair backing helpers ─────────────────────────────────────────────────


def test_insert_done_row_is_idempotent_via_unique_index(tmp_db_path: str) -> None:
    db_init()
    sched = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    sub = sched + timedelta(minutes=12)

    # First insert wins.
    assert queue_insert_done_row_sync(
        sched, sub, "🟢 Creative", "Tag", "Note", sheets_synced=True
    )
    # Second insert at same scheduled_ts is a no-op (UNIQUE INDEX guard
    # — Improvement #4). Critically, /repair calls this for every Sheet
    # row, so a second /repair must not duplicate.
    assert not queue_insert_done_row_sync(
        sched, sub, "🟡 Social", "Different", "Other", sheets_synced=True
    )

    all_ts = queue_get_all_scheduled_ts()
    assert all_ts == {canonical_ts(sched)}


def test_get_all_scheduled_ts_returns_canonical_strings(tmp_db_path: str) -> None:
    db_init()
    a = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    b = datetime(2026, 4, 24, 11, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(a)
    queue_insert_done_row_sync(b, b, "🟢 Creative", "Tag", "", sheets_synced=True)

    assert queue_get_all_scheduled_ts() == {canonical_ts(a), canonical_ts(b)}


def test_done_in_window_returns_only_done(tmp_db_path: str) -> None:
    db_init()
    base = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    queue_add_prompt_sync(base)  # pending
    queue_insert_done_row_sync(
        base + timedelta(hours=1),
        base + timedelta(hours=1, minutes=10),
        "🟢 Creative", "Tag", "", sheets_synced=True,
    )

    rows = queue_get_done_in_window(base, base + timedelta(hours=2))
    assert len(rows) == 1
    assert rows[0]["status"] == "done"


def test_mark_unsynced_flips_done_row_and_ignores_pending(tmp_db_path: str) -> None:
    db_init()
    base = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    # done row, currently synced.
    queue_insert_done_row_sync(base, base, "🟢 Creative", "T", "", sheets_synced=True)
    done_id = next(iter(queue_get_done_in_window(base, base)))["id"]

    # Pending row — should NOT be touched by mark_unsynced.
    queue_add_prompt_sync(base + timedelta(hours=1))

    queue_mark_unsynced_sync(done_id)

    refreshed = queue_get_done_in_window(base, base)[0]
    assert refreshed["sheets_synced"] == 0
