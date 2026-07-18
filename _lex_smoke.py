from datetime import datetime
from zoneinfo import ZoneInfo
from hourly_logger.sheets import _parse_sheet_sched

tz = ZoneInfo("Asia/Kolkata")

# The bug case: single-digit hour falls in the early-April-1 window
# that /trend's lex compare used to drop.
since = datetime(2026, 3, 1, 7, 0, tzinfo=tz)
until = datetime(2026, 4, 1, 6, 59, tzinfo=tz)

cases = [
    ("2026-04-01 4:00", True),     # in March's log-window (the bug)
    ("2026-04-01 04:00", True),
    ("2026-04-01 9:00", False),    # 9am > 6:59am, not in March window
    ("2026-04-01 09:00", False),
    ("2026-03-15 14:00", True),
    ("2026-02-28 23:00", False),   # before window
    ("Scheduled Time", None),
    ("", None),
]

for s, expected in cases:
    dt = _parse_sheet_sched(s, tz)
    if expected is None:
        ok = dt is None
        print(f"{'OK  ' if ok else 'FAIL'} parse({s!r:20s}) = None")
    else:
        in_window = dt is not None and since <= dt <= until
        ok = in_window == expected
        print(f"{'OK  ' if ok else 'FAIL'} parse({s!r:20s}) = {dt}, in_march_window={in_window} (expected {expected})")
