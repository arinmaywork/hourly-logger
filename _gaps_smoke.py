from hourly_logger.handlers.maintenance import _parse_sched_ymdh

cases = [
    ("2026-03-01 09:00", (2026, 3, 1, 9)),
    ("2026-03-01 9:00", (2026, 3, 1, 9)),
    ("2026-03-01 23:00", (2026, 3, 1, 23)),
    ("2026-03-15 0:00", (2026, 3, 15, 0)),
    ("2026-03-01T09:00:00", (2026, 3, 1, 9)),
    ("Scheduled Time", None),
    ("", None),
    ("garbage", None),
    ("2026/03/01 09:00", None),
]
for s, expected in cases:
    result = _parse_sched_ymdh(s)
    status = "OK  " if result == expected else "FAIL"
    print(f"{status} {repr(s):30s} -> {result} (expected {expected})")
