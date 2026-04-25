"""Google Sheets client, retry policy, and circuit breaker.

What this module fixes / introduces
-----------------------------------

* **Bug #7 (off-grid date silent no-op)** — :func:`update_grid` returns a
  :class:`GridUpdateOutcome` with ``date_in_grid=False`` so the caller can
  warn the user inline instead of swallowing the failure.

* **Bug #9 (bare ``except``)** — every retry path catches the specific
  ``gspread.exceptions.APIError``, ``WorksheetNotFound``, and the network
  errors we care about. A *truly unexpected* exception bubbles up so a real
  bug surfaces instead of looping silently.

* **Improvement #9 (circuit breaker + bounded retry)** — a simple
  three-state breaker (closed → open → half-open). After
  ``SHEETS_BREAKER_THRESHOLD`` consecutive failures the breaker opens and
  rejects calls fast for ``SHEETS_BREAKER_COOLDOWN_S`` seconds. The first
  call after cooldown is "half-open" — success closes the breaker, failure
  re-opens it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any, Callable, Optional, TypeVar

import google.auth
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

from .colors import CATEGORIES
from .config import settings, SCOPES
from .logger import get_logger


log = get_logger(__name__)
T = TypeVar("T")


# ── Auth + client cache ─────────────────────────────────────────────────────

# Cache the spreadsheet object — auth is the slow part.
_spreadsheet: Optional[gspread.Spreadsheet] = None
# Cache worksheet handles by name. gspread's ``spreadsheet.worksheet(name)``
# calls ``fetch_sheet_metadata()`` on every invocation, which is a Sheets
# API READ. With /sync churning through hundreds of rows that's the
# single largest contributor to 429 quota errors. Worksheet IDs/titles do
# not change inside a process lifetime, so caching the handle is safe;
# the cache is reset alongside the spreadsheet on hard failure so a
# transient outage doesn't pin a stale handle.
_worksheet_cache: dict[str, gspread.Worksheet] = {}
_cache_lock = threading.Lock()


def _build_credentials() -> Any:
    """Resolve credentials by the documented priority: inline JSON → file → ADC."""
    if settings.has_inline_creds:
        info = json.loads(settings.GOOGLE_CREDENTIALS_JSON or "")
        return Credentials.from_service_account_info(info, scopes=list(SCOPES))
    if settings.creds_file_exists:
        return Credentials.from_service_account_file(settings.CREDS_FILE, scopes=list(SCOPES))
    creds, _ = google.auth.default(scopes=list(SCOPES))
    return creds


def get_spreadsheet() -> gspread.Spreadsheet:
    """Return a cached gspread Spreadsheet. Re-auths only on cache reset."""
    global _spreadsheet
    with _cache_lock:
        if _spreadsheet is not None:
            return _spreadsheet
        creds = _build_credentials()
        client = gspread.Client(auth=creds)
        _spreadsheet = client.open_by_key(settings.SPREADSHEET_ID)
        return _spreadsheet


def reset_spreadsheet_cache() -> None:
    global _spreadsheet
    with _cache_lock:
        _spreadsheet = None
        _worksheet_cache.clear()
        # Also drop any cached grid dates-row mapping — the new spreadsheet
        # handle may point at a refreshed grid.
        _grid_dates_cache["row"] = None
        _grid_dates_cache["fetched_at"] = 0.0


def get_worksheet(name: Optional[str] = None) -> gspread.Worksheet:
    """Return a worksheet handle, cached by name to avoid repeated metadata
    fetches. Auto-creates the Log tab with header row on first use."""
    sheet_name = name or settings.SHEET_NAME
    with _cache_lock:
        cached = _worksheet_cache.get(sheet_name)
        if cached is not None:
            return cached
    try:
        ws = get_spreadsheet().worksheet(sheet_name)
    except WorksheetNotFound:
        if sheet_name == settings.SHEET_NAME:
            ws = get_spreadsheet().add_worksheet(title=sheet_name, rows=5000, cols=6)
            ws.append_row(
                ["Scheduled Time", "Submitted Time", "Category", "Tag", "Note", "Lag (minutes)"],
                value_input_option="USER_ENTERED",
            )
        else:
            raise
    with _cache_lock:
        _worksheet_cache[sheet_name] = ws
    return ws


# Process-wide cache of the grid's "dates row" (row 2 of the Weekly tab).
# /sync iterates many rows in the same time window — re-fetching this
# 50-cell row per row was burning a Sheets read every single time. We cache
# for SHEETS_GRID_DATES_TTL_S seconds; it's invalidated on cache reset and
# on circuit-breaker failure so the next call refreshes naturally.
_grid_dates_cache: dict[str, Any] = {"row": None, "fetched_at": 0.0}


def _grid_dates_row(grid: gspread.Worksheet) -> list[str]:
    """Return ``grid.row_values(2)``, cached for SHEETS_GRID_DATES_TTL_S."""
    now = time.monotonic()
    cached = _grid_dates_cache.get("row")
    fetched_at = _grid_dates_cache.get("fetched_at", 0.0)
    if cached is not None and (now - fetched_at) < settings.SHEETS_GRID_DATES_TTL_S:
        return cached  # type: ignore[no-any-return]
    fresh = grid.row_values(2)
    _grid_dates_cache["row"] = fresh
    _grid_dates_cache["fetched_at"] = now
    return fresh


# ── Circuit breaker ─────────────────────────────────────────────────────────


class CircuitOpenError(RuntimeError):
    """Raised when the breaker rejects a call without attempting it."""


@dataclass
class _BreakerState:
    consecutive_failures: int = 0
    opened_at: float = 0.0
    open: bool = False


class CircuitBreaker:
    """Thread-safe, threshold-based circuit breaker.

    The breaker is consulted from sync code that runs inside the Sheets
    executor; that's why the lock is :class:`threading.Lock` not asyncio.
    """

    def __init__(self, threshold: int, cooldown_s: int) -> None:
        self.threshold = threshold
        self.cooldown_s = cooldown_s
        self._state = _BreakerState()
        self._lock = threading.Lock()

    def before(self) -> None:
        """Raise :class:`CircuitOpenError` if calls are currently rejected."""
        with self._lock:
            if not self._state.open:
                return
            if time.monotonic() - self._state.opened_at >= self.cooldown_s:
                # Move to half-open — let exactly one call through.
                log.info("breaker half-open, allowing probe")
                self._state.open = False
                return
            raise CircuitOpenError(
                f"sheets breaker open ({self._state.consecutive_failures} failures, "
                f"cooldown {self.cooldown_s}s)"
            )

    def on_success(self) -> None:
        with self._lock:
            if self._state.consecutive_failures or self._state.open:
                log.info("breaker reset", extra={
                    "after_failures": self._state.consecutive_failures,
                })
            self._state = _BreakerState()

    def on_failure(self) -> None:
        with self._lock:
            self._state.consecutive_failures += 1
            if self._state.consecutive_failures >= self.threshold:
                self._state.open = True
                self._state.opened_at = time.monotonic()
                log.warning("breaker opened", extra={
                    "threshold": self.threshold,
                    "cooldown_s": self.cooldown_s,
                })

    @property
    def is_open(self) -> bool:
        return self._state.open


# Two breakers, one per worksheet, so a flaky Log write doesn't block grid
# updates and vice versa.
log_tab_breaker = CircuitBreaker(
    threshold=settings.SHEETS_BREAKER_THRESHOLD,
    cooldown_s=settings.SHEETS_BREAKER_COOLDOWN_S,
)
grid_tab_breaker = CircuitBreaker(
    threshold=settings.SHEETS_BREAKER_THRESHOLD,
    cooldown_s=settings.SHEETS_BREAKER_COOLDOWN_S,
)


# ── Retry helper ────────────────────────────────────────────────────────────


# Errors that justify a retry. NetworkError-y stuff plus 429.
_RETRYABLE_HTTP = (500, 502, 503, 504, 429)


def _with_retry(
    fn: Callable[[], T],
    *,
    breaker: CircuitBreaker,
    label: str,
) -> T:
    """Run ``fn`` under exponential backoff, respecting the breaker.

    Bug #9: only known-retryable conditions retry. Unknown exceptions
    raise immediately so logic bugs do not get hidden in a retry loop.
    """
    breaker.before()
    last_exc: Optional[BaseException] = None
    for attempt in range(settings.SHEETS_MAX_RETRIES):
        try:
            result = fn()
            breaker.on_success()
            return result
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            last_exc = e
            if status in _RETRYABLE_HTTP and attempt < settings.SHEETS_MAX_RETRIES - 1:
                wait = (2 ** attempt) * (5 if status == 429 else 1)
                log.warning(
                    "sheets retryable error",
                    extra={"label": label, "status": status, "attempt": attempt + 1, "wait_s": wait},
                )
                time.sleep(wait)
                continue
            reset_spreadsheet_cache()
            breaker.on_failure()
            raise
        except (TimeoutError, ConnectionError) as e:
            last_exc = e
            if attempt < settings.SHEETS_MAX_RETRIES - 1:
                wait = 2 ** attempt
                log.warning(
                    "sheets network error",
                    extra={"label": label, "attempt": attempt + 1, "wait_s": wait},
                    exc_info=True,
                )
                time.sleep(wait)
                continue
            reset_spreadsheet_cache()
            breaker.on_failure()
            raise
    # Loop exhausted (extremely defensive — the loop returns or raises above).
    breaker.on_failure()
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry loop exited without result")


# ── Sync writers ────────────────────────────────────────────────────────────


def _save_log_row_sync(
    scheduled_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str = "",
    is_edit: bool = False,
) -> None:
    lag = round((submitted_ts - scheduled_ts).total_seconds() / 60, 1)
    sched_str = scheduled_ts.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")
    sub_str = submitted_ts.astimezone(settings.tz).strftime("%Y-%m-%d %H:%M")
    row = [sched_str, sub_str, category, tag, note, lag]

    def _do() -> None:
        sheet = get_worksheet()
        # Upsert by scheduled timestamp.
        cell = sheet.find(sched_str, in_column=1) if is_edit else sheet.find(sched_str, in_column=1)
        if cell:
            sheet.update(
                f"A{cell.row}:F{cell.row}",
                [row],
                value_input_option="USER_ENTERED",
            )
            log.info("log row upserted", extra={"sheet_row": cell.row, "sched": sched_str})
            return
        sheet.append_row(row, value_input_option="USER_ENTERED")
        log.info("log row appended", extra={"sched": sched_str})

    _with_retry(_do, breaker=log_tab_breaker, label="log_save")


@dataclass
class GridUpdateOutcome:
    """Bug #7 fix: surface "date not in grid" to callers explicitly."""

    date_in_grid: bool
    cell_addr: Optional[str] = None


def _update_grid_sync(scheduled_ts: datetime, category: str, tag: str) -> GridUpdateOutcome:
    local_dt = scheduled_ts.astimezone(settings.tz)
    hour = local_dt.hour

    # Hours 0–6 belong to the previous calendar day's column.
    effective_dt = local_dt - dt.timedelta(days=1) if hour < 7 else local_dt
    date_str = f"{effective_dt.month}/{effective_dt.day}/{str(effective_dt.year)[2:]}"

    # Hour → row mapping: 7→5 … 23→21, 0→22 … 6→28
    row = (hour - 2) if hour >= 7 else (hour + 22)

    outcome = GridUpdateOutcome(date_in_grid=True)

    def _do() -> None:
        nonlocal outcome
        grid = get_worksheet(settings.GRID_SHEET_NAME)
        # Cached for SHEETS_GRID_DATES_TTL_S — avoids burning a Sheets
        # read per row during /sync of a large backlog.
        dates_row = _grid_dates_row(grid)
        col = -1
        for i, d in enumerate(dates_row):
            if d.strip() == date_str:
                col = i + 1
                break
        if col == -1:
            log.warning("grid date not found", extra={"date_str": date_str})
            outcome = GridUpdateOutcome(date_in_grid=False)
            return
        cell_addr = gspread.utils.rowcol_to_a1(row, col)
        grid.update(cell_addr, [[tag]])
        color = CATEGORIES.get(category, {}).get("color")
        if color:
            grid.format(cell_addr, {
                "backgroundColor": color,
                "horizontalAlignment": "CENTER",
                "textFormat": {"bold": True},
            })
        outcome = GridUpdateOutcome(date_in_grid=True, cell_addr=cell_addr)
        log.info("grid updated", extra={"cell": cell_addr, "category": category})

    _with_retry(_do, breaker=grid_tab_breaker, label="grid_update")
    return outcome


def _log_breakdown_sync(
    since_local: datetime, until_local: datetime,
) -> tuple[dict[str, int], int]:
    since_str = since_local.strftime("%Y-%m-%d %H:%M")
    until_str = until_local.strftime("%Y-%m-%d %H:%M")

    def _do() -> tuple[dict[str, int], int]:
        sheet = get_worksheet(settings.SHEET_NAME)
        all_rows = sheet.get("A:C", value_render_option="FORMATTED_VALUE")
        breakdown: dict[str, int] = {}
        total = 0
        for row in all_rows[1:]:
            if not row:
                continue
            sched = row[0].strip() if len(row) > 0 else ""
            cat = row[2].strip() if len(row) > 2 else ""
            if not sched or not (since_str <= sched <= until_str):
                continue
            total += 1
            if cat:
                breakdown[cat] = breakdown.get(cat, 0) + 1
            else:
                breakdown["_uncategorised"] = breakdown.get("_uncategorised", 0) + 1
        uncat = breakdown.pop("_uncategorised", 0)
        breakdown = dict(sorted(breakdown.items(), key=lambda x: -x[1]))
        if uncat:
            breakdown["_uncategorised"] = uncat
        return breakdown, total

    return _with_retry(_do, breaker=log_tab_breaker, label="log_breakdown")


def _log_raw_sync(
    since_local: datetime, until_local: datetime,
) -> list[tuple[datetime, str]]:
    since_str = since_local.strftime("%Y-%m-%d %H:%M")
    until_str = until_local.strftime("%Y-%m-%d %H:%M")

    def _do() -> list[tuple[datetime, str]]:
        sheet = get_worksheet(settings.SHEET_NAME)
        all_rows = sheet.get("A:C", value_render_option="FORMATTED_VALUE")
        results: list[tuple[datetime, str]] = []
        for row in all_rows[1:]:
            if not row:
                continue
            sched = row[0].strip() if len(row) > 0 else ""
            cat = row[2].strip() if len(row) > 2 else ""
            if not sched or not (since_str <= sched <= until_str):
                continue
            try:
                sched_dt = datetime.strptime(sched, "%Y-%m-%d %H:%M").replace(
                    tzinfo=settings.tz
                )
                results.append((sched_dt, cat))
            except ValueError:
                continue
        return results

    return _with_retry(_do, breaker=log_tab_breaker, label="log_raw")


# ── Async wrappers ──────────────────────────────────────────────────────────


async def _to_thread(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(fn, *args, **kwargs))


async def save_log_row(
    scheduled_ts: datetime,
    submitted_ts: datetime,
    category: str,
    tag: str,
    note: str = "",
    is_edit: bool = False,
) -> None:
    await _to_thread(
        _save_log_row_sync, scheduled_ts, submitted_ts, category, tag, note, is_edit,
    )


async def update_grid(
    scheduled_ts: datetime, category: str, tag: str,
) -> GridUpdateOutcome:
    return await _to_thread(_update_grid_sync, scheduled_ts, category, tag)


async def log_breakdown(
    since_local: datetime, until_local: datetime,
) -> tuple[dict[str, int], int]:
    return await _to_thread(_log_breakdown_sync, since_local, until_local)


async def log_raw(
    since_local: datetime, until_local: datetime,
) -> list[tuple[datetime, str]]:
    return await _to_thread(_log_raw_sync, since_local, until_local)
