"""One-time migration: copy entries from the Weekly grid tab into the Log tab.

Run once on the GCP VM::

    python3 migrate_weekly_to_log.py

What it does
------------

* Reads every non-empty cell in the Weekly grid (rows 5–28, all date
  columns).
* Derives ``(scheduled_time, category, tag)`` from the cell colour and text.
* Skips any ``scheduled_time`` already present in column A of the Log tab.
* Appends new rows to the Log tab in the same format the bot uses.

This script now reuses the production colour-matching threshold (improved
in Bug #8) and the centralised :mod:`hourly_logger.sheets` client, so
behaviour is consistent with the live bot.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime

from hourly_logger.colors import nearest_category
from hourly_logger.config import settings
from hourly_logger.logger import configure_logging, get_logger
from hourly_logger.sheets import get_spreadsheet, get_worksheet


configure_logging()
log = get_logger(__name__)


def row_1based_to_hour(row: int) -> int:
    if row >= 22:
        return row - 22
    return row + 2


def main() -> None:
    ss = get_spreadsheet()

    log.info("fetching grid", extra={"sheet": settings.GRID_SHEET_NAME})
    resp = ss.client.request(
        "get",
        f"https://sheets.googleapis.com/v4/spreadsheets/{settings.SPREADSHEET_ID}",
        params={
            "ranges":          f"'{settings.GRID_SHEET_NAME}'",
            "includeGridData": "true",
        },
    ).json()

    if "error" in resp:
        log.error("sheets api error", extra={"error": resp["error"]})
        return

    sheets_data = resp.get("sheets", [])
    if not sheets_data:
        log.error("no sheet data returned — check GRID_SHEET_NAME")
        return

    row_data = sheets_data[0]["data"][0].get("rowData", [])
    log.info("grid loaded", extra={"rows": len(row_data)})
    if len(row_data) < 2:
        log.error("grid has fewer than 2 rows — nothing to migrate")
        return

    # Date row
    date_cells = row_data[1].get("values", [])
    col_dates: dict[int, dt.date] = {}
    for col_idx, cell in enumerate(date_cells):
        raw = (
            cell.get("formattedValue", "")
            or cell.get("effectiveValue", {}).get("stringValue", "")
            or cell.get("userEnteredValue", {}).get("stringValue", "")
        ).strip()
        if not raw:
            continue
        for fmt in ("%m/%d/%y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%y"):
            try:
                col_dates[col_idx] = datetime.strptime(raw, fmt).date()
                break
            except ValueError:
                pass

    if not col_dates:
        log.error("no date columns found in row 2")
        return

    log.info(
        "date columns parsed",
        extra={
            "count": len(col_dates),
            "min": min(col_dates.values()).isoformat(),
            "max": max(col_dates.values()).isoformat(),
        },
    )

    log_ws = get_worksheet(settings.SHEET_NAME)
    existing: set[str] = {
        row[0].strip()
        for row in log_ws.get_all_values()[1:]
        if row and row[0].strip()
    }
    log.info("existing log entries", extra={"count": len(existing)})

    DATA_ROWS = range(4, 28)
    new_rows: list[list[object]] = []
    skipped_empty = skipped_dup = unmatched_colour = 0

    for row_idx in DATA_ROWS:
        if row_idx >= len(row_data):
            break
        hour = row_1based_to_hour(row_idx + 1)
        cells = row_data[row_idx].get("values", [])

        for col_idx, col_date in col_dates.items():
            if col_idx >= len(cells):
                skipped_empty += 1
                continue
            cell = cells[col_idx]

            tag = (
                cell.get("formattedValue", "")
                or cell.get("effectiveValue", {}).get("stringValue", "")
                or cell.get("userEnteredValue", {}).get("stringValue", "")
            ).strip()
            if not tag:
                skipped_empty += 1
                continue

            actual_date = col_date + dt.timedelta(days=1) if hour < 7 else col_date
            sched_local = datetime(
                actual_date.year, actual_date.month, actual_date.day,
                hour, 0, 0, tzinfo=settings.tz,
            )
            sched_str = sched_local.strftime("%Y-%m-%d %H:%M")
            if sched_str in existing:
                skipped_dup += 1
                continue

            eff_fmt = cell.get("effectiveFormat", {})
            bg = eff_fmt.get("backgroundColorStyle", {}).get("rgbColor", {}) or eff_fmt.get(
                "backgroundColor", {}
            )
            cat = nearest_category(
                bg.get("red",   0.0),
                bg.get("green", 0.0),
                bg.get("blue",  0.0),
            )
            if not cat:
                unmatched_colour += 1
                log.warning(
                    "unmatched colour",
                    extra={
                        "rgb": (bg.get("red", 0.0), bg.get("green", 0.0), bg.get("blue", 0.0)),
                        "sched": sched_str,
                        "tag": tag,
                    },
                )

            new_rows.append([sched_str, sched_str, cat, tag, "", 0])
            existing.add(sched_str)

    log.info(
        "scan complete",
        extra={
            "to_add": len(new_rows),
            "skipped_empty": skipped_empty,
            "skipped_dup": skipped_dup,
            "unmatched_colour": unmatched_colour,
        },
    )

    if not new_rows:
        log.info("nothing to migrate")
        return

    new_rows.sort(key=lambda r: r[0])  # type: ignore[no-any-return]
    batch_size = 500
    log.info("appending rows", extra={"total": len(new_rows)})
    import time
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]
        log_ws.append_rows(batch, value_input_option="USER_ENTERED")
        log.info("batch appended", extra={"from": i + 1, "to": i + len(batch)})
        time.sleep(1)

    log.info("done", extra={"migrated": len(new_rows)})


if __name__ == "__main__":
    main()
