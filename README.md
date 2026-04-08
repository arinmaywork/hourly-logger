# 🕰 Hourly Logger Bot

A robust, asynchronous Telegram bot for high-fidelity activity tracking. It pings you every hour, records your activity into a SQLite queue, and syncs entries to two Google Sheets tabs: a visual **Weekly** grid and a raw **Log** audit trail.

---

## 🚀 Overview

The **Hourly Logger** is a personal productivity tool that asks you what you did each hour of the day. It is resilient to downtime, supports category-based breakdowns, and maintains a complete historical record.

### Core Features
- **Deterministic Scheduling**: Fires exactly at the start of every hour (HH:00).
- **Deterministic Scheduling**: Fires exactly at the start of every hour (HH:00). If a prompt is ignored, it is re-sent every subsequent hour until answered (as long as the user hasn't started filling it in).
- **Missed Prompt Backfilling**: On restart, detects any missed hours and prompts you to fill them in. Duplicate-safe — restarting exactly on the hour never creates double entries.
- **Three-Step Logging Workflow**: Category → Activity Tag (≤60 chars) → Note (optional, ≤500 chars).
- **Quick-Log Shortcut**: `/log c Deep Work` enters an hour in one message, bypassing the multi-step flow. Use `,,` to separate tag from note: `/log h Sleep,, 7 hrs`.
- **Edit Feature**: Correct any past entry with `/edit`. Pass a date to browse a specific day (`/edit today`, `/edit 2026-03-28`, `/edit 28/03`). Each entry in the list shows its category emoji, time, and tag so you can identify it at a glance.
- **Mid-Queue Edit**: Use `/cancel` at any time to pause the pending queue without losing entries — you are then free to `/edit` a past entry. Send any message afterwards to resume the queue.
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
| `/monthly` | Category breakdown for the **current month**. |
| `/monthly 2026-03` \| `/monthly 03` | Category breakdown for a specific month (YYYY-MM or bare month number). |
| `/weekly` | Category breakdown for the **current week** (Mon–Sun). |
| `/weekly 2026-03-28` \| `/weekly 28/03` | Category breakdown for the week containing the given date. Also accepts `today` and `yesterday`. |
| `/trend monthly` | Hours per category for **every month of the current year**, in one view. |
| `/trend monthly 2025` | Same, for a specific year. |
| `/trend weekly` | Hours per category for **every week of the current month**. |
| `/trend weekly 2026-03` | Same, for a specific month (YYYY-MM). |
| `/edit` | List the 5 most recent entries for quick selection. Each item shows `[id] Day HH:MM 🟢 — tag` so you can identify it at a glance. |
| `/edit today` \| `/edit yesterday` | List all entries for today or yesterday. |
| `/edit YYYY-MM-DD` | List all entries for a specific date (e.g. `/edit 2026-03-28`). |
| `/edit DD/MM` \| `/edit DD/MM/YYYY` | List all entries by day/month (e.g. `/edit 28/03`). |
| `/skip` | During the **note** step: saves the entry without a note. During **category** or **tag** step: marks the slot as skipped. |
| `/skipall` | Skips all pending entries **older than today** in one command. Useful after the bot was offline for a period and a large backlog of old prompts has accumulated. Today's pending entries are left intact. |
| `/cancel` | Pauses the current flow without skipping or losing the entry. Shows how many entries are still pending and hints to use `/edit` or send any message to resume. Does **not** auto-surface the next pending entry, so you can go straight to `/edit`. |
| `/sync` | Retries all entries whose Sheets write previously failed. Reports success/failure counts. |
| `/fixcats` | Patches blank-category rows in the Log tab by re-reading their background colour from the Weekly grid. Run once after the initial migration to fix entries that were copied without a category. |
| `/dedup` | Removes duplicate hour-slot rows from the Log tab. Handles both exact timestamp duplicates and same-hour duplicates with different minutes (e.g. migration entry at `HH:00` vs real bot entry at `HH:23`). Keeps the real bot entry; uses a single batch API call so it completes in seconds even for hundreds of rows. Safe to run at any time. |
| `/auditlog` | Diagnoses the Log tab for a given month. Reports total rows, exact vs same-hour duplicate counts, days with >24 entries, and timestamp format samples. Usage: `/auditlog` (current month) or `/auditlog 2026-03`. |

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
- **Sheets client**: `gspread` v6. Authentication priority: (1) `GOOGLE_CREDENTIALS_JSON` env var (full service-account JSON), (2) `credentials.json` file, (3) **Application Default Credentials** (ADC) — used automatically when running on a GCP VM with the correct instance scopes. Client and spreadsheet object are cached at module level. All calls run via `asyncio.run_in_executor`.
- **Non-blocking entry flow**: After an entry is saved to SQLite, the confirmation message and the next pending prompt are sent immediately. The two Sheets writes (Log tab + Weekly grid) are then fired as a background `asyncio.create_task`. If a background write fails, `sheets_synced` stays `False` and `/sync` retries it — data is never lost. This eliminates the per-entry delay when working through a backlog.
- **State Machine**: `current_prompt` global dict tracks multi-step input. Stages: `category` → `tag_note` → (done), plus `edit_selection` for the `/edit` flow. `/cancel` clears `current_prompt` without auto-surfacing the next pending entry, enabling mid-queue edits.
- **`/status` breakdowns**: Read directly from the Log tab (column A = timestamp, column C = category) using string-range filtering on the `YYYY-MM-DD HH:MM` format. Never queries SQLite for hourly breakdowns.
- **Dedup strategy**: `/dedup` normalises timestamps to `YYYY-MM-DD HH` (hour precision) before comparing, catching both exact duplicates and migration/bot pairs that share the same hour but differ in minutes. All row deletions are batched into a single `spreadsheet.batch_update()` call (chunked at 100 requests) to avoid write-quota limits.

---

## ☁️ Deployment: Google Cloud Platform (Always Free)

The bot runs on a GCP **e2-micro** Compute Engine VM, which is part of Google Cloud's [Always Free tier](https://cloud.google.com/free) — genuinely free forever, no expiry. Credit card required at sign-up for identity verification only.

**Always Free limits (e2-micro):**
- 1 e2-micro VM (2 vCPU burst, 1 GB RAM) — free for the full month in `us-west1`, `us-central1`, or `us-east1`
- 30 GB standard persistent disk
- 1 GB outbound network per month

### 1. Create the VM

In the [GCP Console](https://console.cloud.google.com/):

- **Navigation** → Compute Engine → VM Instances → **Create Instance**
- **Name**: `hourly-logger`
- **Region**: `us-central1` (Iowa) — must be a free-tier region
- **Machine type**: `e2-micro`
- **Boot disk**: Ubuntu 22.04 LTS, 30 GB Standard persistent disk
- **Firewall**: allow HTTPS traffic (the bot only makes outbound calls, no inbound ports needed)
- **Access scopes** → Select "Set access for each API" or use the custom scope approach below
- Click **Create**

> **Important — VM OAuth scopes**: The VM's service account must have access to the Google Sheets and Drive APIs. If you see `[403]: Request had insufficient authentication scopes` in the logs, run the following from **Cloud Shell** (not the VM's SSH terminal):
>
> ```bash
> gcloud compute instances stop hourly-logger --zone=us-central1-a --project=YOUR_PROJECT
> gcloud compute instances set-service-account hourly-logger \
>   --zone=us-central1-a \
>   --project=YOUR_PROJECT \
>   --scopes=cloud-platform,https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/spreadsheets
> gcloud compute instances start hourly-logger --zone=us-central1-a --project=YOUR_PROJECT
> ```

### 2. SSH into the VM

From the VM Instances page, click **SSH** next to your VM, or use:

```bash
gcloud compute ssh hourly-logger --zone=us-central1-a
```

### 3. Install dependencies

```bash
sudo apt update && sudo apt install -y python3-pip python3-venv git
git clone https://github.com/arinmaywork/hourly-logger.git
cd hourly-logger
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file:

```bash
nano .env
```

Paste and fill in your values:

```env
TELEGRAM_TOKEN=your_token_here
CHAT_ID=your_personal_telegram_id
SPREADSHEET_ID=your_google_sheet_id
TIMEZONE=Asia/Kolkata
GRID_SHEET_NAME=Weekly

# Optional overrides (defaults shown)
SHEET_NAME=Log
CREDS_FILE=credentials.json
DB_PATH=/home/ubuntu/hourly-logger/data/queue.db

# Google Sheets authentication (choose one approach — see below)
# GOOGLE_CREDENTIALS_JSON={"type":"service_account","project_id":"...","private_key":"..."}
```

**Google Sheets authentication options (in priority order):**

1. **`GOOGLE_CREDENTIALS_JSON` env var** — paste the full service-account JSON as a single line. Best for portability across environments.
2. **`credentials.json` file** — place the service-account key file in the project directory.
3. **Application Default Credentials (ADC)** — if neither of the above is set, the bot uses the VM's own identity automatically. This is the recommended approach on GCP VMs (no key file needed), as long as the VM was created with the correct OAuth scopes (see step 1 note above). Share the spreadsheet with the VM's service account email (`PROJECT_NUMBER-compute@developer.gserviceaccount.com`).

> **Note**: Some GCP organisations enforce the policy `iam.disableServiceAccountKeyCreation`, which blocks downloading service-account JSON keys. In this case, use ADC (option 3).

Create the data directory for the SQLite DB:

```bash
mkdir -p /home/ubuntu/hourly-logger/data
```

### 5. Run as a systemd service (24/7, auto-restart)

```bash
sudo nano /etc/systemd/system/hourly-logger.service
```

Paste (replace `ubuntu` with your actual username if different):

```ini
[Unit]
Description=Hourly Logger Telegram Bot
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/hourly-logger
ExecStart=/home/ubuntu/hourly-logger/venv/bin/python bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable hourly-logger
sudo systemctl start hourly-logger
sudo systemctl status hourly-logger   # should show "active (running)"
```

### 6. Deploying updates

After pushing to GitHub, SSH into the VM and run:

```bash
cd ~/hourly-logger
git pull
sudo systemctl restart hourly-logger
```

### 7. Useful commands

```bash
# View live logs
sudo journalctl -u hourly-logger -f

# Check status
sudo systemctl status hourly-logger

# Stop / restart
sudo systemctl stop hourly-logger
sudo systemctl restart hourly-logger
```

### Migrating from Railway

If you have an existing Railway deployment, export the SQLite database before shutting it down:

1. In Railway, open a shell on the service and run: `cp /data/queue.db /tmp/queue.db`
2. Use `railway run cat /tmp/queue.db > queue.db` or the Railway CLI to download it
3. Upload it to your GCP VM: `gcloud compute scp queue.db hourly-logger:~/hourly-logger/data/queue.db --zone=us-central1-a`

If you can't recover the DB, the bot will start fresh — historical data is safe in the Google Sheets Log tab.

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

## 🛠 Troubleshooting

### Bot not responding / 409 Conflict in logs
`telegram.error.Conflict: terminated by other getUpdates request` means two bot instances are polling simultaneously. Only one instance may run at a time.

- Check for a stale deployment on Railway (or any other platform) and delete it.
- Check for duplicate processes on the VM: `ps aux | grep bot.py`. If more than one appears, run `sudo pkill -f bot.py && sudo systemctl start hourly-logger`.

### Google Sheets write failure: 403 insufficient scopes
The VM's OAuth token doesn't include Drive/Sheets. Run from **Cloud Shell** (not the VM terminal):
```bash
gcloud compute instances stop INSTANCE --zone=ZONE --project=PROJECT
gcloud compute instances set-service-account INSTANCE \
  --zone=ZONE --project=PROJECT \
  --scopes=cloud-platform,https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/spreadsheets
gcloud compute instances start INSTANCE --zone=ZONE --project=PROJECT
```

### Large backlog of old pending prompts after downtime
If the bot was offline for hours or days, `backfill_missed_prompts()` will queue all the missed hours on restart. Use `/skipall` to dismiss everything before today in one command, then fill in today's entries normally.

### Monthly totals exceed maximum hours (duplicate data)
Run `/auditlog YYYY-MM` to count rows and identify the duplication pattern, then run `/dedup` to clean them up. The most common cause is the one-time historical migration creating entries at `HH:00` while the real-time bot also logged entries at `HH:MM` for the same hour.

### DeprecationWarning: worksheet.update() argument order
A non-fatal warning from gspread when writing to the Weekly grid. Does not affect functionality. Will be resolved in a future dependency update.

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
2. **Google Cloud Setup**:
   - In [Google Cloud Console](https://console.cloud.google.com/), enable **Google Sheets API** and **Google Drive API** for your project (Cloud Shell: `gcloud services enable sheets.googleapis.com drive.googleapis.com`).
   - **On GCP VM (recommended)**: Share your spreadsheet with the VM's default compute service account — `PROJECT_NUMBER-compute@developer.gserviceaccount.com` — as Editor. No key file needed; the bot uses ADC automatically.
   - **Other environments**: Create a service account, download `credentials.json` (or copy its JSON into `GOOGLE_CREDENTIALS_JSON`), and share the spreadsheet with the service-account email as Editor.
3. **Google Sheet Structure**:
   - A tab named **Log** (columns A–F as described above; row 1 = header).
   - A tab named **Weekly** (or your `GRID_SHEET_NAME`) with the visual grid structure described above.
