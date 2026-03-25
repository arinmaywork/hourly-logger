# 🕰 Hourly Logger Bot

A robust, asynchronous Telegram bot designed for high-fidelity activity tracking. It uses a persistent SQLite queue to ensure no hourly slots are missed, even during downtime, and synchronizes logs to a visual Google Sheets grid and a raw data backup.

---

## 🚀 Overview
The **Hourly Logger** is a personal productivity tool that pings you every hour to record your activities. It is built to be resilient, category-aware, and highly visual.

### Core Features
- **Deterministic Scheduling**: Pings exactly at the start of every hour (HH:00).
- **Missed Prompt Backfilling**: Automatically detects downtime and prompts you to fill in missed hours upon restart. Duplicate-safe — restarting at the exact turn of an hour will never create double entries.
- **Three-Step Logging Workflow**:
  1. **Category**: Choose from predefined categories (Creative, Health, Professional, Social, Other).
  2. **Activity Tag**: Provide a short label (e.g., "Deep Work", "Exercise") — max 60 characters.
  3. **Note (Optional)**: Add detailed context or `/skip` to leave blank — max 500 characters.
- **Edit Feature**: Correct mistakes by modifying any of the last 5 entries using the `/edit` command. Use `/cancel` at the selection screen to return to normal entry mode.
- **Dual-Layer Storage**:
    - **SQLite (`queue.db`)**: Local persistence for reliable state management. Each entry stores `tag` and `note` in separate columns alongside a combined `entry_text` field, and tracks whether the Google Sheets write succeeded (`sheets_synced`).
    - **Google Sheets**:
        - **Visual Grid**: A "Weekly" tracker that maps hours to rows and dates to columns, applying category-specific background colors.
        - **Day Start Logic**: The grid follows a 7:00 AM day-start convention. Hours from 7:00 AM to 11:59 PM map to the current calendar date's column. Hours from 12:00 AM to 6:59 AM map to the *previous* calendar date's column, as they are considered part of the previous day's cycle.
        - **Row Mapping**:
            - 7:00 AM - 11:00 PM: Rows 5 - 21
            - 12:00 AM - 6:00 AM: Rows 22 - 28
        - **Raw Log**: An append-only audit trail of every entry with precise timestamps and lag calculations.
- **Resilient Sync**: Exponential backoff for Google Sheets API rate limits (429) and generic network errors. Failed writes are flagged in the database and retried on demand via `/sync`.
- **Non-Blocking Architecture**: All Google Sheets API calls run in a thread pool executor, keeping the asyncio event loop free to process Telegram updates at all times.

---

## 🕹 Usage & Commands

| Command | Description |
| :--- | :--- |
| `/start` | Initialise the bot and view available commands. |
| `/status` | Displays queue statistics: Pending, Completed, Skipped, and Unsynced. |
| `/edit` | Lists the 5 most recent entries. Select one to restart the 3-step logging flow for that hour, updating both SQLite and Google Sheets. |
| `/skip` | On the **note** step: saves the entry without a note. On the **category** or **tag** step: skips the entire entry, marking it as skipped in the database. |
| `/cancel` | Abandons the current in-progress flow without marking anything as skipped. The prompt remains pending and will be shown again. Also exits edit-selection mode and returns to normal entry mode. |
| `/sync` | Retries all entries whose Google Sheets write previously failed. Reports success/failure counts. |

---

## 🛠 Technical Architecture

- **Language**: Python 3.10+
- **Framework**: `python-telegram-bot` (v22+) using `asyncio`.
- **Scheduler**: `APScheduler` (AsyncIOScheduler) with `cron` triggers. Shuts down cleanly via `post_stop` lifecycle hook.
- **Database**: `sqlite3` with a `queue` table managing `pending`, `done`, and `skipped` states. Columns: `id`, `scheduled_ts`, `submitted_ts`, `category`, `tag`, `note`, `entry_text`, `status`, `sheets_synced`.
- **Integrations**: `gspread` (v6) for Google Sheets API v4 interaction. The authenticated client and spreadsheet object are cached at module level to avoid repeated round-trips. All gspread calls run via `asyncio.run_in_executor` so they never block the event loop.
- **State Machine**: A global `current_prompt` dictionary tracks the user's progress through the multi-step input. Stages: `category` → `tag` → `note`, plus `edit_selection` for the `/edit` flow.

---

## 📋 Prerequisites
1. **Telegram Bot**: Create one via [@BotFather](https://t.me/botfather) and get your `TELEGRAM_TOKEN`.
2. **Google Service Account**:
   - Create a project in [Google Cloud Console](https://console.cloud.google.com/).
   - Enable **Google Sheets API** and **Google Drive API**.
   - Create a **Service Account**, download the `credentials.json`, and share your Google Sheet with the service account email.
3. **Google Sheet Structure**:
   - A tab named **"Log"** (created automatically if missing).
   - A tab named **"Weekly"** (or your `GRID_SHEET_NAME`) with dates in Row 2 (format: `m/d/yy`).

---

## ☁️ Deployment: Google Cloud Platform (Always Free)
To run this 100% free forever, use a Google Cloud **Compute Engine** VM.

### 1. Create the VM
- **Machine Type**: `e2-micro` (This is part of the "Always Free" tier).
- **Region**: `us-central1`, `us-west1`, or `us-east1`.
- **OS**: Ubuntu 22.04 LTS.
- **Disk**: 30GB Balanced Persistent Disk.

### 2. Initial Server Setup
SSH into your VM and run:
```bash
sudo apt update && sudo apt install -y python3-pip python3-venv git
git clone https://github.com/arinmaywork/hourly-logger.git
cd hourly-logger
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the project root:
```env
TELEGRAM_TOKEN=your_token_here
CHAT_ID=your_personal_telegram_id
SPREADSHEET_ID=your_google_sheet_id
TIMEZONE=Asia/Kolkata
GRID_SHEET_NAME=Weekly

# Optional overrides (defaults shown)
SHEET_NAME=Log
CREDS_FILE=credentials.json
DB_PATH=queue.db

# Recommended: inline credentials instead of a file
GOOGLE_CREDENTIALS_JSON='{"type": "service_account", ...}'
```

### 4. Background Persistence (systemd)
To ensure the bot runs 24/7 and restarts on crashes, create a service file:
`sudo nano /etc/systemd/system/hourly-logger.service`

Paste the following (adjust `/home/username/` to your path):
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

**Enable and Start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable hourly-logger
sudo systemctl start hourly-logger
```

### 5. Future Updates
To update the code after a `git push`:
```bash
cd ~/hourly-logger
git pull
sudo systemctl restart hourly-logger
```

---

## 📊 Category Mapping
| Category | Color | Intent |
| :--- | :--- | :--- |
| 🟢 Creative | Green | Learning, Building, Designing |
| 💎 Health | Cyan | Sleep, Exercise, Meals |
| 🔘 Professional | Grey | Deep Work, Tasks, Meetings |
| 🟡 Social | Yellow | Calls, Family, Hanging out |
| ⚪️ Other | White | Miscellaneous, Chores |
