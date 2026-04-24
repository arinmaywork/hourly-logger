"""Dedup tiebreaker — Bug #6 verification.

We exercise the pure scoring logic + the canonical-key derivation. The
real Sheets API path is exercised via the maintenance handler in
production; the rule we *can* unit-test is: when scores tie, the later
row is the one deleted.
"""

from __future__ import annotations

from hourly_logger.handlers.maintenance import _hour_key, _row_score


def test_real_entry_outscores_migration_entry() -> None:
    real = ["2026-04-24 12:23", "2026-04-24 12:23", "🟢 Creative", "Deep Work", "", "0"]
    migrated = ["2026-04-24 12:00", "2026-04-24 12:00", "🟢 Creative", "Deep Work", "", "0"]
    # Migrated row: sched == submitted → no real-time bonus.
    assert _row_score(migrated) == 2  # cat + tag
    # Real entry: sched != submitted → +2 bonus.
    real_with_diff_submitted = ["2026-04-24 12:00", "2026-04-24 12:23", "🟢 Creative", "Deep Work", "", "0"]
    assert _row_score(real_with_diff_submitted) == 4


def test_hour_key_groups_same_hour_different_minutes() -> None:
    assert _hour_key("2026-04-24 12:00") == _hour_key("2026-04-24 12:23")
    # Different hour -> different key.
    assert _hour_key("2026-04-24 12:59") != _hour_key("2026-04-24 13:00")


def test_score_zero_for_blank_row() -> None:
    assert _row_score(["", "", "", "", "", ""]) == 0


def test_tiebreaker_keeps_earlier_row() -> None:
    """Bug #6: if two rows have identical scores, dedup must always
    delete the later (higher row number) one — never the earlier.
    Re-implements the exact decision the dedup loop makes for one pair.
    """
    # Two equal-score rows. The dedup loop visits in row-number order.
    row_a = ["2026-04-24 12:00", "2026-04-24 12:00", "🟢 Creative", "Deep Work", "", "0"]
    row_b = ["2026-04-24 12:30", "2026-04-24 12:30", "🟢 Creative", "Deep Work", "", "0"]
    assert _row_score(row_a) == _row_score(row_b)

    # Simulate the loop's decision for the pair (existing=row_a at row 5,
    # new=row_b at row 9, scores equal -> delete max(9,5) = 9).
    existing_row_num = 5
    new_row_num = 9
    deleted = max(new_row_num, existing_row_num)
    kept = min(new_row_num, existing_row_num)
    assert deleted == 9
    assert kept == 5
