#!/usr/bin/env python3
"""Cron entry point: pull the Obsidian vault and ingest daily notes.

Usage (on the VM, from the repo root):
    python -m scripts.ingest_journal [--since-days N] [--force] [--no-pull] [--quiet]

Suggested crontab (02:00 local, after the day's journaling is done):
    0 2 * * * cd /path/to/hourly-logger && ./venv/bin/python -m scripts.ingest_journal >> journal_ingest.log 2>&1

Exit codes: 0 = clean, 1 = ran with alarms (pull failure / thin days /
extractor errors — also pushed to Telegram), 2 = could not run at all.
"""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since-days", type=int, default=None, help="Override look-back window")
    ap.add_argument("--force", action="store_true", help="Re-ingest even if note is unchanged")
    ap.add_argument("--no-pull", action="store_true", help="Skip git pull (local testing)")
    ap.add_argument("--quiet", action="store_true", help="No Telegram alarm on problems")
    args = ap.parse_args()

    from hourly_logger.database import db_init
    from hourly_logger.journal.ingest import ingest, send_telegram

    try:
        db_init()
        summary = ingest(since_days=args.since_days, force=args.force, pull=not args.no_pull)
    except Exception as e:  # noqa: BLE001 — cron: report, don't traceback-spam
        print(f"ingest failed to run: {e}", file=sys.stderr)
        if not args.quiet:
            send_telegram(f"⚠️ Journal ingest failed to run: {e}")
        return 2

    counts = summary.counts() or {"nothing found": 0}
    print(
        "journal ingest: "
        + ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
        + ("" if summary.pull_ok else f" | PULL FAILED: {summary.pull_msg}")
    )
    if summary.alarms:
        text = "⚠️ Journal ingest issues:\n" + "\n".join(f"• {a}" for a in summary.alarms)
        print(text, file=sys.stderr)
        if not args.quiet:
            send_telegram(text)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
