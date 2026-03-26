"""One-time migration: copy entries from the Weekly grid tab into the Log tab.

Run once on the GCP VM:
    python3 migrate_weekly_to_log.py

What it does:
  - Reads every non-empty cell in the Weekly grid (rows 5–28, all date columns).
  - Derives (scheduled_time, category, tag) from the cell colour and text.
  - Skips any scheduled_time already present in column A of the Log tab.
  - Appends new rows to the Log tab in the same format the bot uses:
      [Scheduled Time, Submitted Time, Category, Tag, Note, Lag (minutes)]

Hour → row mapping (mirrors _sheets_update_grid_sync in bot.py):
  Row 5  = 07:00 … Row 21 = 23:00
  Row 22 = 00:00, Row 23 = 01:00 … Row 28 = 06:00
  Hours 00–06 are filed under the *previous* day's column (effective_dt trick),
  so we add one day back to get the actual calendar date.
"""

import os
import json
import datetime as dt
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_ID  = os.environ["SPREADSHEET_ID"]
CREDS_FILE      = os.getenv("CREDS_FILE", "credentials.json")
SHEET_NAME      = os.getenv("SHEET_NAME", "Log")
GRID_SHEET_NAME = os.getenv("GRID_SHEET_NAME", "Weekly")
TZ              = ZoneInfo(os.getenv("TIMEZONE", "UTC"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Category colour map (matches CATEGORIES in bot.py) ──────────────────────
# Keys are (red, green, blue) rounded to 2 dp; values are category names.
CATEGORIES = {
    "🟢 Creative":     {"red": 0.0,  "green": 1.0,  "blue": 0.0},
    "💎 Health":       {"red": 0.0,  "green": 1.0,  "blue": 1.0},
    "🔘 Professional": {"red": 0.8,  "green": 0.8,  "blue": 0.8},
    "🟡 Social":       {"red": 1.0,  "green": 1.0,  "blue": 0.0},
    "⚪️ Other":        {"red": 1.0,  "green": 1.0,  "blue": 1.0},
}

def _colour_key(r, g, b):
    return (round(r, 2), round(g, 2), round(b, 2))

COLOR_TO_CAT = {
    _colour_key(**v): k for k, v in CATEGORIES.items()
}

# Row index (0-based) → local hour
# Row 4 (0-based) = row 5 (1-based) = 07:00, …, row 20 = 23:00
# Row 21 = 00:00, row 22 = 01:00, …, row 27 = 06:00
def row_index_to_hour(row_idx: int) -> int:
    row_1based = row_idx + 1
    if row_1based >= 22:          # rows 22-28 → hours 00-06
        return row_1based - 22
    else:                         # rows 5-21  → hours 07-23
        return row_1based + 2


def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info  = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)

    client = gspread.Client(auth=creds)
    ss     = client.open_by_key(SPREADSHEET_ID)

    # ── 1. Fetch Weekly grid with cell formatting ────────────────────────────
    print("Fetching Weekly grid with formatting…")
    resp = client.request(
        "get",
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}",
        params={
            "ranges":          f"'{GRID_SHEET_NAME}'",
            "includeGridData": "true",
            "fields": (
                "sheets(data(rowData(values("
                "effectiveValue/stringValue,"
                "effectiveFormat/backgroundColor))))"
            ),
        },
    ).json()

    row_data = resp["sheets"][0]["data"][0].get("rowData", [])

    # ── 2. Parse dates from row 2 (index 1) ─────────────────────────────────
    date_cells = row_data[1]["values"] if len(row_data) > 1 else []
    col_dates: dict[int, datetime.date] = {}
    for col_idx, cell in enumerate(date_cells):
        raw = cell.get("effectiveValue", {}).get("stringValue", "")
        if not raw:
            continue
        try:
            col_dates[col_idx] = datetime.strptime(raw.strip(), "%m/%d/%y").date()
        except ValueError:
            pass

    if not col_dates:
        print("No date columns found in row 2. Aborting.")
        return

    print(f"Found {len(col_dates)} date columns: "
          f"{min(col_dates.values())} → {max(col_dates.values())}")

    # ── 3. Load existing Log entries to skip duplicates ──────────────────────
    print("Reading existing Log entries…")
    log_ws    = ss.worksheet(SHEET_NAME)
    log_rows  = log_ws.get_all_values()
    # Column A = Scheduled Time, format "YYYY-MM-DD HH:MM"
    existing_times: set[str] = {row[0].strip() for row in log_rows[1:] if row}

    print(f"  {len(existing_times)} existing entries in Log.")

    # ── 4. Walk data rows (indices 4–27 = rows 5–28) ────────────────────────
    DATA_ROW_START = 4   # 0-based index
    DATA_ROW_END   = 27

    new_rows: list[list] = []

    for row_idx in range(DATA_ROW_START, DATA_ROW_END + 1):
        if row_idx >= len(row_data):
            break
        hour  = row_index_to_hour(row_idx)
        cells = row_data[row_idx].get("values", [])

        for col_idx, col_date in col_dates.items():
            if col_idx >= len(cells):
                continue
            cell = cells[col_idx]

            tag = cell.get("effectiveValue", {}).get("stringValue", "").strip()
            if not tag:
                continue   # empty cell — no entry

            # Determine actual calendar date:
            # hours 00-06 are filed under the *previous* day's column
            if hour < 7:
                actual_date = col_date + dt.timedelta(days=1)
            else:
                actual_date = col_date

            sched_local = datetime(
                actual_date.year, actual_date.month, actual_date.day,
                hour, 0, 0, tzinfo=TZ
            )
            sched_str = sched_local.strftime("%Y-%m-%d %H:%M")

            if sched_str in existing_times:
                continue   # already in Log — skip

            # Determine category from cell background colour
            bg   = cell.get("effectiveFormat", {}).get("backgroundColor", {})
            r    = bg.get("red",   1.0)
            g    = bg.get("green", 1.0)
            b    = bg.get("blue",  1.0)
            cat  = COLOR_TO_CAT.get(_colour_key(r, g, b), "")

            new_rows.append([sched_str, sched_str, cat, tag, "", 0])
            existing_times.add(sched_str)   # prevent duplicates within this run

    if not new_rows:
        print("No new rows to migrate — Log is already up to date.")
        return

    # Sort by scheduled time before appending
    new_rows.sort(key=lambda r: r[0])

    print(f"Appending {len(new_rows)} new rows to Log tab…")
    # Append in batches of 500 to stay within API limits
    batch_size = 500
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]
        log_ws.append_rows(batch, value_input_option="USER_ENTERED")
        print(f"  Appended rows {i+1}–{i+len(batch)}")

    print(f"Done. {len(new_rows)} entries migrated from Weekly → Log.")


if __name__ == "__main__":
    main()
