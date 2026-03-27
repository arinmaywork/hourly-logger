# 🕰 Hourly Logger Bot

A robust, asynchronous Telegram bot for high-fidelity activity tracking. It pings you every hour, records your activity into a SQLite queue, and syncs entries to two Google Sheets tabs: a visual **Weekly** grid and a raw **Log** audit trail.

---

## 🚀 Overview

The **Hourly Logger** is a personal productivity tool that asks you what you did each hour of the day. It is resilient to downtime, supports category-based breakdowns, and maintains a complete historical record.

### Core Features
- **Deterministic Scheduling**: Fires exactly at the start of every hour (HH:00).
- **Missed Prompt Backfilling**: On restart, detects any missed hours and prompts you to fill them in. Duplicate-safe — restarting exactly on the hour never creates double entries.
- **Three-Step Logging Workflow**: Category → Activity Tag (≤60 chars) → Note (optional, ≤500 chars).
- **Quick-Log Shortcut**: `/log c Deep Work` enters an hour in one message, bypassing the multi-step flow.
- **Edit Feature**: Correct any of the last 5 entries with `/edit`.
- **Dual-Layer Storage**: SQLite (local queue) + Google Sheets (Weekly grid + Log tab).
- **Resilient Sync**: Exponential backoff on Sheets API rate limits and network errors; retry on demand via `/sync`.
- **Non-Blocking Architecture**: All Sheets API calls run in a thread pool executor — the asyncio event loop is never blocked.

---

## 🕹 Commands

| Command | Description |
| :--- | :--- |
| `/start` | Initialise the bot and show available commands. |
| `/log <cat> <tag> [,, note]` | **Quick log**: enter an hour in one message using a category shortcut (`c` `h` `p` `s` `o`). Example: `/log c Deep Work,, focused session`. (Legacy ` \| ` separator also works.) |
| `/status` | Queue stats (Pending / Done / Skipped / Unsynced) plus a bar-chart breakdown of hours by category for **this week** (Mon → now) and **this year** (1 Jan → now), read from the Log tab. |
| `/edit` | List the 5 most recent entries. Select one to restart the 3-step flow for that hour, updating both SQLite and Sheets. |
| `/skip` | During the **note** step: saves the entry without a note. During **category** or **tag** step: marks the slot as skipped. |
| `/cancel` | Abandons the current in-progress entry without marking it skipped. The prompt remains pending. Also exits edit-selection mode. |
| `/sync` | Retries all entries whose Sheets write previously failed. Reports success/failure counts. |
| `/fixcats` | Patches blank-category rows in the Log tab by re-reading their background colour from the Weekly grid. Run once after the initial migration to fix the 322 entries that were copied without a category. |

### Category Shortcuts (for `/log`)

| Shortcut(s) | Category |
| :--- | :--- |
| `c` `cr` `creative` | 🟢 Creative |
| `h` `he` `health` | 💎 Health |
| `p` `pr` `prof` `professional` | 🔘 Professional |
| `s` `so` `social` | 🟡 Social |
| `o` `ot` `other` | ⚪️ Other |

---

## 📊 Category & Colour Mapping

These are the exact background colours used in the Weekly grid. The bot uses nearest-colour (Euclidean RGB) matching, so minor palette variations are tolerated.

| Category | Hex | RGB (0–1) | Intent |
| :--- | :--- | :--- | :--- |
| 🟢 Creative | `#03ff00` | (0.012, 1.0, 0.0) | Learning, Building, Designing |
| 💎 Health | `#02ffff` | (0.008, 1.0, 1.0) | Sleep, Exercise, Meals |
| 🔘 Professional | `#cccccc` | (0.8, 0.8, 0.8) | Deep Work, Tasks, Meetings |
| 🟡 Social | `#ffff00` | (1.0, 1.0, 0.0) | Calls, Family, Hanging out |
| ⚪️ Other | `#ffffff` | (1.0, 1.0, 1.0) | Miscellaneous, Chores |

> **Note on Google Sheets API colour encoding**: The Sheets API v4 omits colour channels whose value is `0.0` from the JSON response. The bot always defaults missing channels to `0.0` (not `1.0`) to handle this correctly — a critical detail for colours like Social (`#ffff00`, blue=0) and Creative (`#03ff00`, blue=0).

---

## 🗂 Google Sheets Structure

### Weekly Tab (Visual Grid)

| Row | Content |
| :--- | :--- |
| 1 | Week number |
| 2 | Dates (one column per day, format `m/d/yy`) |
| 3 | Day label (Mon, Tue, …) |
| 4 | "Spent" header |
| 5–21 | Hour slots **7:00 – 23:00** |
| 22–28 | Hour slots **0:00 – 6:00** (night hours, belong to the *previous* calendar day) |
| 30+ | Category summary rows |

**Day-start convention**: The logger treats 7:00 AM as the start of a new day. Hours 0:00–6:00 are filed under the *previous* calendar date's column (they are the tail end of that day's cycle).

### Log Tab (Audit Trail)

Append-only record of every submitted entry. This is the source of truth for `/status` breakdowns.

| Column | Content |
| :--- | :--- |
| A | Scheduled Time (`YYYY-MM-DD HH:MM`) |
| B | Submitted Time |
| C | Category |
| D | Tag |
| E | Note |
| F | Lag (minutes between scheduled and submitted) |

---

## 🏗 Technical Architecture

- **Language**: Python 3.10+
- **Framework**: `python-telegram-bot` v22+ with `asyncio`
- **Scheduler**: `APScheduler` (AsyncIOScheduler) with `cron` triggers. Shuts down via `post_stop` lifecycle hook.
- **Database**: `sqlite3` — table `queue` with columns `id`, `scheduled_ts`, `submitted_ts`, `category`, `tag`, `note`, `entry_text`, `status` (`pending`/`done`/`skipped`), `sheets_synced`.
- **Sheets client**: `gspread` v6, authenticated via a service-account `credentials.json` (or `GOOGLE_CREDENTIALS_JSON` env var). Client and spreadsheet object are cached at module level. All calls run via `asyncio.run_in_executor`.
- **State Machine**: `current_prompt` global dict tracks multi-step input. Stages: `category` → `tag` → `note`, plus `edit_selection` for the `/edit` flow.
- **`/status` breakdowns**: Read directly from the Log tab (column A = timestamp, column C = category) using string-range filtering on the `YYYY-MM-DD HH:MM` format. Never queries SQLite for hourly breakdowns.

---

## ☁️ Deployment: Railway

The bot is deployed on [Railway](https://railway.app/). The persistent database lives at `/data/queue.db` (a Railway volume).

### Environment Variables

Set these in the Railway service's **Variables** panel:

```env
TELEGRAM_TOKEN=your_token_here
CHAT_ID=your_personal_telegram_id
SPREADSHEET_ID=your_google_sheet_id
TIMEZONE=Asia/Kolkata
GRID_SHEET_NAME=Weekly

# Optional overrides (defaults shown)
SHEET_NAME=Log
CREDS_FILE=credentials.json
DB_PATH=/data/queue.db

# Recommended: inline credentials to avoid managing a file
GOOGLE_CREDENTIALS_JSON='{"type": "service_account", ...}'
```

### Deploying Updates

The Railway service is connected to this GitHub repository. Push to `main` to trigger an automatic redeploy:

```bash
git add .
git commit -m "your message"
git push origin main
```

### Local Development

```bash
git clone https://github.com/arinmaywork/hourly-logger.git
cd hourly-logger
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your values
python bot.py
```

---

## 🔄 Historical Data Migration (Completed — Mar 2026)

All historical entries from the Weekly grid were migrated into the Log tab in March 2026 using the `/migrate` bot command. The command is **intentionally disabled** (handler not registered) to prevent accidental re-runs.

### What the migration did
- Read every coloured hour-slot cell from the Weekly grid tab via the Sheets API v4 (`includeGridData: true`).
- Mapped background colour → category using nearest-colour Euclidean matching.
- Appended each entry to the Log tab in `YYYY-MM-DD HH:MM` format, skipping duplicates and future dates.

### Key technical notes
- Date cells in Sheets are stored as serial numbers — `formattedValue` (not `effectiveValue`) must be used to read the display string.
- The data rows start at the row labelled `7:00` in column A (rows before it are the Week No / Date / Day / Spent headers).
- Colour is read from `effectiveFormat.backgroundColorStyle.rgbColor` first, falling back to the deprecated `effectiveFormat.backgroundColor`. Missing channels default to `0.0` (the Sheets API omits zero-valued channels from the response).
- The `cmd_migrate` function is retained in `bot.py` for reference; `migrate_weekly_to_log.py` is the equivalent standalone script.

---

## 📋 Prerequisites

1. **Telegram Bot**: Create one via [@BotFather](https://t.me/botfather) and obtain your `TELEGRAM_TOKEN`.
2. **Google Service Account**:
   - Create a project in [Google Cloud Console](https://console.cloud.google.com/).
   - Enable **Google Sheets API** and **Google Drive API**.
   - Create a service account, download `credentials.json`, and share your spreadsheet with the service-account email (Editor role).
3. **Google Sheet Structure**:
   - A tab named **Log** (columns A–F as described above; row 1 = header).
   - A tab named **Weekly** (or your `GRID_SHEET_NAME`) with the visual grid structure described above.
