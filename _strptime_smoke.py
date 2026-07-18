from datetime import datetime
for s in ["2026-04-01 04:00", "2026-04-01 4:00", "2026-04-01 09:00", "2026-04-01 9:00", "2026-04-01 0:00"]:
    try:
        d = datetime.strptime(s, "%Y-%m-%d %H:%M")
        print(f"OK   {repr(s):28s} -> {d}")
    except ValueError as e:
        print(f"FAIL {repr(s):28s} -> {e}")
