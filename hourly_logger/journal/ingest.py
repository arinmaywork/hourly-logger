"""Journal ingest pipeline: vault -> parse -> SQLite.

Flow per note (named ``YYYY-MM-DD.md``, anywhere under the vault):
  1. sha256 the raw markdown; if it matches the stored hash, skip (free).
  2. Rules parser. If the result is thin and Gemini is configured, fall back.
  3. Upsert ``journal_days`` (+ denormalised ``journal_activities``).
  4. Anything still thin, or any extractor failure, is surfaced to the
     caller so the cron script can raise a Telegram drift alarm — a broken
     template must never fail silently.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from ..config import settings
from ..database import canonical_ts, db_connect
from ..logger import get_logger
from .extractor import ExtractorError, extract
from .parser import parse_note
from .schema import DayRecord

log = get_logger(__name__)

_NOTE_NAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.md$")


@dataclass
class IngestResult:
    date: str
    action: str  # 'inserted' | 'updated' | 'unchanged' | 'error'
    extractor: str = "rules"
    thin: bool = False
    error: Optional[str] = None


@dataclass
class IngestSummary:
    results: list[IngestResult] = field(default_factory=list)
    pull_ok: bool = True
    pull_msg: str = ""

    def counts(self) -> dict[str, int]:
        c: dict[str, int] = {}
        for r in self.results:
            c[r.action] = c.get(r.action, 0) + 1
        return c

    @property
    def alarms(self) -> list[str]:
        """Human-readable problems worth a Telegram ping."""
        out = [f"vault git pull failed: {self.pull_msg}"] if not self.pull_ok else []
        for r in self.results:
            if r.error:
                out.append(f"{r.date}: {r.error}")
            elif r.thin and r.action in ("inserted", "updated"):
                out.append(
                    f"{r.date}: journal parsed nearly empty (template change?)"
                )
        return out


def vault_dir() -> Path:
    if not settings.VAULT_DIR:
        raise RuntimeError("VAULT_DIR is not configured")
    return Path(settings.VAULT_DIR)


def git_pull() -> tuple[bool, str]:
    """``git pull --ff-only`` in the vault clone. Never raises."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(vault_dir()), "pull", "--ff-only"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        msg = (proc.stdout + proc.stderr).strip()[-300:]
        return proc.returncode == 0, msg
    except Exception as e:  # noqa: BLE001 — cron must degrade, not crash
        return False, str(e)


def discover_notes(since: dt.date) -> dict[str, Path]:
    """Map date-string -> note path for all daily notes dated >= *since*.

    Notes are found by filename pattern anywhere under the vault (or
    JOURNAL_SUBDIR if set), so moving the daily-notes folder never breaks
    ingestion. On duplicate filenames the most recently modified wins.
    """
    root = vault_dir() / settings.JOURNAL_SUBDIR if settings.JOURNAL_SUBDIR else vault_dir()
    found: dict[str, Path] = {}
    for path in root.rglob("*.md"):
        m = _NOTE_NAME_RE.match(path.name)
        if not m:
            continue
        try:
            date = dt.date.fromisoformat(m.group(1))
        except ValueError:
            continue
        if date < since:
            continue
        key = m.group(1)
        if key not in found or path.stat().st_mtime > found[key].stat().st_mtime:
            found[key] = path
    return found


def ingest_note(path: Path, date: str, force: bool = False) -> IngestResult:
    """Ingest a single note. Returns the outcome; never raises."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        return IngestResult(date=date, action="error", error=f"unreadable: {e}")
    note_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    with db_connect() as conn:
        row = conn.execute(
            "SELECT note_hash FROM journal_days WHERE date=?", (date,)
        ).fetchone()
        if row and row["note_hash"] == note_hash and not force:
            return IngestResult(date=date, action="unchanged")

    rec, extractor_used, error = _parse_with_fallback(raw, date)
    _upsert(date, raw, note_hash, rec, extractor_used)
    return IngestResult(
        date=date,
        action="updated" if row else "inserted",
        extractor=extractor_used,
        thin=rec.is_thin(),
        error=error,
    )


def _parse_with_fallback(raw: str, date: str) -> tuple[DayRecord, str, Optional[str]]:
    rec = parse_note(raw, date)
    if not rec.is_thin() or not settings.GEMINI_API_KEY:
        return rec, "rules", None
    try:
        gem = extract(raw, date)
        # Scores are cheap to carry over if only the rules path found them.
        gem.mood = gem.mood or rec.mood
        gem.energy = gem.energy or rec.energy
        return gem, "gemini", None
    except ExtractorError as e:
        log.warning("gemini fallback failed", extra={"date": date, "err": str(e)})
        return rec, "rules", f"gemini fallback failed ({e})"


def _upsert(date: str, raw: str, note_hash: str, rec: DayRecord, extractor: str) -> None:
    now = canonical_ts(datetime.now(timezone.utc))
    with db_connect() as conn:
        conn.execute("BEGIN")
        try:
            conn.execute(
                """INSERT INTO journal_days
                       (date, raw_md, note_hash, mood, energy, parsed_json,
                        extractor, thin, ingested_ts)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(date) DO UPDATE SET
                       raw_md=excluded.raw_md, note_hash=excluded.note_hash,
                       mood=excluded.mood, energy=excluded.energy,
                       parsed_json=excluded.parsed_json,
                       extractor=excluded.extractor, thin=excluded.thin,
                       ingested_ts=excluded.ingested_ts""",
                (
                    date,
                    raw,
                    note_hash,
                    rec.mood,
                    rec.energy,
                    rec.model_dump_json(),
                    extractor,
                    int(rec.is_thin()),
                    now,
                ),
            )
            conn.execute("DELETE FROM journal_activities WHERE date=?", (date,))
            done = set(rec.completed)
            conn.executemany(
                "INSERT OR REPLACE INTO journal_activities (date, name, completed) VALUES (?,?,?)",
                [(date, name, int(name in done)) for name in dict.fromkeys(rec.planned)],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def ingest(since_days: Optional[int] = None, force: bool = False, pull: bool = True) -> IngestSummary:
    """Full run: optional git pull, then ingest every note in the window."""
    summary = IngestSummary()
    if pull:
        summary.pull_ok, summary.pull_msg = git_pull()
    since = dt.date.today() - dt.timedelta(days=since_days or settings.JOURNAL_INGEST_DAYS)
    for date, path in sorted(discover_notes(since).items()):
        summary.results.append(ingest_note(path, date, force=force))
    return summary


def send_telegram(text: str) -> bool:
    """Fire-and-forget owner notification. Never raises."""
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{settings.TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": settings.CHAT_ID, "text": text[:4000]},
            timeout=30,
        )
        return resp.status_code == 200
    except requests.RequestException as e:
        log.warning("telegram alarm failed", extra={"err": str(e)})
        return False
