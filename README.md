# 🕰 Hourly Logger Bot

A robust, asynchronous Telegram bot designed for high-fidelity activity tracking. It uses a persistent SQLite queue to ensure no hourly slots are missed, even during downtime, and synchronizes logs to a visual Google Sheets grid and a raw data backup.

---

## 🚀 Overview
The **Hourly Logger** is a personal productivity tool that pings you every hour to record your activities. It is built to be resilient, category-aware, and highly visual.

### Core Features
- **Deterministic Scheduling**: Pings exactly at the start of every hour (HH:00).
- **Missed Prompt Backfilling**: Automatically detects downtime and prompts you to fill in missed hours upon restart.
- **Three-Step Logging Workflow**:
  1. **Category**: Choose from predefined categories (Creative, Health, Professional, Social, Other).
  2. **Activity Tag**: Provide a short label (e.g., "Deep Work", "Exercise").
  3. **Note (Optional)**: Add detailed context or `/skip` to leave blank.
- **Edit Feature**: Correct mistakes by modifying any of the last 5 entries using the `/edit` command.
- **Dual-Layer Storage**:
    - **SQLite (`queue.db`)**: Local persistence for reliable state management.
    - **Google Sheets**:
        - **Visual Grid**: A "Weekly" tracker that maps hours to rows and dates to columns, applying category-specific background colors.
        - **Day Start Logic**: The grid follows a 7:00 AM day-start convention. Hours from 7:00 AM to 11:59 PM map to the current calendar date's column. Hours from 12:00 AM to 6:59 AM map to the *previous* calendar date's column, as they are considered part of the previous day's cycle.
        - **Row Mapping**:
            - 7:00 AM - 11:00 PM: Rows 5 - 21
            - 12:00 AM - 6:00 AM: Rows 22 - 28
        - **Raw Log**: An append-only audit trail of every entry with precise timestamps and lag calculations.
- **Resilient Sync**: Includes exponential backoff for Google Sheets API rate limits.

---

## 🕹 Usage & Commands
Interact with the bot using these commands:
- `/start`: Initialise the bot and view current configuration.
- `/edit`: Lists the 5 most recent entries. Selecting one will restart the 3-step logging process for that specific hour and update both the SQLite database and Google Sheets.
- `/status`: Displays statistics for the current queue (Pending, Completed, Skipped).
- `/skip`: Skips the current active prompt.
- `/sync`: Manual trigger to retry any failed Google Sheets writes.

---

## 🛠 Technical Architecture (Context for AI)
- **Language**: Python 3.10+
- **Framework**: `python-telegram-bot` (v22+) using `asyncio`.
- **Scheduler**: `APScheduler` (AsyncIOScheduler) with `cron` triggers.
- **Database**: `sqlite3` with a `queue` table managing `pending`, `done`, and `skipped` states.
- **Integrations**: `gspread` for Google Sheets API v4 interaction.
- **State Machine**: A global `current_prompt` dictionary tracks the user's progress through the multi-step input.

---

## 📋 Prerequisites
1. **Telegram Bot**: Create one via [@BotFather](https://t.me/botfather) and get your `TELEGRAM_TOKEN`.
2. **Google Service Account**:
   - Create a project in [Google Cloud Console](https://console.cloud.google.com/).
   - Enable **Google Sheets API** and **Google Drive API**.
   - Create a **Service Account**, download the `credentials.json`, and share your Google Sheet with the service account email.
3. **Google Sheet Structure**:
   - A tab named **"Log"** (created automatically).
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
