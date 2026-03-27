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
# Google Sheets stores colours as 0–255 integers internally; the API returns
# them as floats 0.0–1.0.  We round to 2 decimal places for comparison.
CATEGORIES = {
    "🟢 Creative":     {"red": 0.0,  "green": 1.0,  "blue": 0.0},
    "💎 Health":       {"red": 0.0,  "green": 1.0,  "blue": 1.0},
    "🔘 Professional": {"red": 0.8,  "green": 0.8,  "blue": 0.8},
    "🟡 Social":       {"red": 1.0,  "green": 1.0,  "blue": 0.0},
    "⚪️ Other":        {"red": 1.0,  "green": 1.0,  "blue": 1.0},
}

def _colour_key(r, g, b):
    return (round(float(r), 2), round(float(g), 2), round(float(b), 2))

COLOR_TO_CAT = {
    _colour_key(v["red"], v["green"], v["blue"]): k for k, v in CATEGORIES.items()
}

# Row (1-based) → local hour
def row_1based_to_hour(row: int) -> int:
    if row >= 22:          # rows 22-28 → hours 00-06
        return row - 22
    else:                  # rows 5-21  → hours 07-23
        return row + 2


def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info  = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)

    client = gspread.Client(auth=creds)
    ss     = client.open_by_key(SPREADSHEET_ID)

    # ── 1. Fetch Weekly grid with full cell data + formatting ────────────────
    # No 'fields' filter — fetch everything so we don't miss any fields.
    print(f"Fetching '{GRID_SHEET_NAME}' sheet with full grid data…")
    resp = client.request(
        "get",
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}",
        params={
            "ranges":          f"'{GRID_SHEET_NAME}'",
            "includeGridData": "true",
        },
    ).json()

    if "error" in resp:
        print(f"ERROR from Sheets API: {resp['error']}")
        return

    sheets = resp.get("sheets", [])
    if not sheets:
        print("No sheet data returned. Check GRID_SHEET_NAME in .env")
        return

    row_data = sheets[0]["data"][0].get("rowData", [])
    print(f"  {len(row_data)} rows returned from grid.")

    # ── 2. Parse dates from row 2 (index 1) ─────────────────────────────────
    if len(row_data) < 2:
        print("Grid has fewer than 2 rows. Nothing to migrate.")
        return

    date_cells = row_data[1].get("values", [])
    col_dates: dict[int, dt.date] = {}
    for col_idx, cell in enumerate(date_cells):
        # Try effectiveValue first, then userEnteredValue
        raw = (
            cell.get("effectiveValue", {}).get("stringValue", "")
            or cell.get("userEnteredValue", {}).get("stringValue", "")
        ).strip()
        if not raw:
            continue
        try:
            col_dates[col_idx] = datetime.strptime(raw, "%m/%d/%y").date()
        except ValueError:
            try:
                # Fallback: try other common date formats
                col_dates[col_idx] = datetime.strptime(raw, "%Y-%m-%d").date()
            except ValueError:
                pass  # skip unrecognised date formats

    if not col_dates:
        print("No date columns found in row 2. Check grid structure.")
        print("  Sample row-2 values:", [
            c.get("effectiveValue", {}) or c.get("userEnteredValue", {})
            for c in date_cells[:5]
        ])
        return

    print(f"  Found {len(col_dates)} date columns: "
          f"{min(col_dates.values())} → {max(col_dates.values())}")

    # ── 3. Load existing Log entries to skip duplicates ──────────────────────
    print(f"Reading existing entries from '{SHEET_NAME}' tab…")
    log_ws   = ss.worksheet(SHEET_NAME)
    log_rows = log_ws.get_all_values()
    existing: set[str] = {row[0].strip() for row in log_rows[1:] if row and row[0].strip()}
    print(f"  {len(existing)} existing entries in Log.")

    # ── 4. Walk data rows (1-based rows 5–28 = 0-based indices 4–27) ─────────
    DATA_ROWS = range(4, 28)   # 0-based indices

    new_rows: list[list] = []
    skipped_empty   = 0
    skipped_dup     = 0
    unmatched_colour = 0

    for row_idx in DATA_ROWS:
        if row_idx >= len(row_data):
            break
        hour  = row_1based_to_hour(row_idx + 1)
        cells = row_data[row_idx].get("values", [])

        for col_idx, col_date in col_dates.items():
            if col_idx >= len(cells):
                skipped_empty += 1
                continue
            cell = cells[col_idx]

            # Get tag text
            tag = (
                cell.get("effectiveValue", {}).get("stringValue", "")
                or cell.get("userEnteredValue", {}).get("stringValue", "")
            ).strip()
            if not tag:
                skipped_empty += 1
                continue

            # Actual calendar date:
            # hours 00-06 are filed under the previous day's column
            actual_date = col_date + dt.timedelta(days=1) if hour < 7 else col_date
            sched_local = datetime(
                actual_date.year, actual_date.month, actual_date.day,
                hour, 0, 0, tzinfo=TZ,
            )
            sched_str = sched_local.strftime("%Y-%m-%d %H:%M")

            if sched_str in existing:
                skipped_dup += 1
                continue

            # Map background colour → category
            bg  = cell.get("effectiveFormat", {}).get("backgroundColor", {})
            r   = bg.get("red",   1.0)
            g   = bg.get("green", 1.0)
            b   = bg.get("blue",  1.0)
            cat = COLOR_TO_CAT.get(_colour_key(r, g, b), "")
            if not cat:
                unmatched_colour += 1
                print(f"  Unmatched colour RGB({r:.3f},{g:.3f},{b:.3f}) "
                      f"at {sched_str} tag='{tag}' — will migrate without category")

            new_rows.append([sched_str, sched_str, cat, tag, "", 0])
            existing.add(sched_str)

    print(f"\nScan complete:")
    print(f"  New rows to add : {len(new_rows)}")
    print(f"  Skipped (empty) : {skipped_empty}")
    print(f"  Skipped (dup)   : {skipped_dup}")
    print(f"  Colour mismatches: {unmatched_colour}")

    if not new_rows:
        print("Nothing to migrate.")
        return

    # Sort by scheduled time before appending
    new_rows.sort(key=lambda r: r[0])

    # Append in batches of 500
    batch_size = 500
    print(f"\nAppending {len(new_rows)} rows to Log tab…")
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]
        log_ws.append_rows(batch, value_input_option="USER_ENTERED")
        print(f"  Appended rows {i+1}–{i+len(batch)}")

    print(f"\nDone. {len(new_rows)} entries migrated from Weekly → Log.")


if __name__ == "__main__":
    main()
