"""Typed configuration loaded from .env / environment.

Bug #12 fix: missing required env vars now produce a clean startup error
(``pydantic.ValidationError`` listing every missing/invalid field) instead of
a cryptic ``KeyError`` at first use.

Improvement #5: Pydantic ``BaseSettings`` with type checking, defaults, and
validation in one place. Import ``settings`` anywhere — it is constructed
once on first import and cached.
"""

from __future__ import annotations

import datetime as dt
import os
from functools import lru_cache
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)


class Settings(BaseSettings):
    """All runtime configuration. Values come from environment or a .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )

    # ── Required ────────────────────────────────────────────────────────────
    TELEGRAM_TOKEN: str = Field(..., min_length=10, description="Telegram bot token")
    CHAT_ID: int = Field(..., description="Owner's Telegram numeric chat id")
    SPREADSHEET_ID: str = Field(..., min_length=10, description="Google Sheets ID")

    # ── Sheets / file paths ────────────────────────────────────────────────
    TIMEZONE: str = Field("UTC", description="IANA tz, e.g. Asia/Kolkata")
    DB_PATH: str = Field("queue.db", description="Path to SQLite database file")
    SHEET_NAME: str = Field("Log", description="Audit-log worksheet name")
    GRID_SHEET_NAME: str = Field("Weekly", description="Visual grid worksheet name")
    CREDS_FILE: str = Field("credentials.json", description="Service-account JSON file path")
    GOOGLE_CREDENTIALS_JSON: Optional[str] = Field(
        None, description="Inline service-account JSON (overrides CREDS_FILE)"
    )

    # ── Limits ─────────────────────────────────────────────────────────────
    TAG_MAX_LEN: int = Field(60, ge=1, le=500)
    NOTE_MAX_LEN: int = Field(500, ge=1, le=10_000)

    # ── Sheets retry / circuit-breaker tuning ──────────────────────────────
    SHEETS_MAX_RETRIES: int = Field(5, ge=1, le=10)
    SHEETS_BREAKER_THRESHOLD: int = Field(
        3, ge=1, description="Consecutive failures before the breaker opens"
    )
    SHEETS_BREAKER_COOLDOWN_S: int = Field(
        60, ge=5, description="Seconds the breaker stays open before half-open retry"
    )
    SHEETS_GRID_DATES_TTL_S: int = Field(
        300,
        ge=0,
        description=(
            "TTL (seconds) for the cached Weekly grid dates row. /sync of "
            "a large backlog re-uses this cache to avoid burning a Sheets "
            "read per row. 0 disables the cache."
        ),
    )
    SHEETS_SYNC_DELAY_S: float = Field(
        1.5,
        ge=0.0,
        le=30.0,
        description=(
            "Seconds to sleep between rows in /sync. Each row needs ~3-4 "
            "Sheets API ops; with the default 60 reads/min user quota this "
            "throttle keeps a multi-hundred-row sync inside the budget. "
            "Lower it for paid quotas, raise it if you keep hitting 429s."
        ),
    )

    # ── Color matching ─────────────────────────────────────────────────────
    COLOR_MATCH_THRESHOLD: float = Field(
        0.35,
        gt=0.0,
        le=1.5,
        description=(
            "Bug #8 fix: bumped from 0.25 to 0.35 so the gray pair "
            "(#cccccc vs #ffffff, distance ≈0.346) still matches."
        ),
    )

    # ── Day-boundary (Bug #4) ──────────────────────────────────────────────
    # The Weekly grid groups hours by "log day" — hours 7am-onwards belong to
    # the calendar date, hours 0:00-6:59 belong to the *previous* date. Every
    # date-bounded query (status, /weekly, /monthly, /skipall, /edit by date)
    # must use this origin or its results won't match the grid the user sees.
    LOG_DAY_START_HOUR: int = Field(
        7,
        ge=0,
        le=23,
        description=(
            "Bug #4 fix: hour at which a 'log day' starts in the user's "
            "local tz. Hours before this belong to the previous calendar "
            "date in the grid. Default 7 matches the original Weekly tab."
        ),
    )

    # ── Backfill origin ────────────────────────────────────────────────────
    SERVICE_START_DATE: Optional[dt.date] = Field(
        None,
        description=(
            "When set, startup backfill seeds the queue from this date "
            "(at LOG_DAY_START_HOUR local time) instead of bailing out on "
            "an empty DB. Useful after a fresh deploy or DB reset so missed "
            "hours can be filled via /missing."
        ),
    )
    BACKFILL_MAX_HOURS: int = Field(
        24 * 90,
        ge=1,
        description="Hard cap on how many hourly rows the backfill will insert in one call.",
    )

    # ── Logging ────────────────────────────────────────────────────────────
    LOG_LEVEL: str = Field("INFO")
    LOG_JSON: bool = Field(
        False,
        description="Improvement #10: set true on the GCP VM for structured JSON logs",
    )

    # ── Validators ─────────────────────────────────────────────────────────
    @field_validator("TIMEZONE")
    @classmethod
    def _check_tz(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError as e:
            raise ValueError(f"Unknown IANA timezone: {v!r}") from e
        return v

    @field_validator("LOG_LEVEL")
    @classmethod
    def _check_log_level(cls, v: str) -> str:
        v = v.upper()
        if v not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"Invalid LOG_LEVEL: {v}")
        return v

    # ── Convenience properties (not env vars) ──────────────────────────────
    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.TIMEZONE)

    @property
    def has_inline_creds(self) -> bool:
        v = (self.GOOGLE_CREDENTIALS_JSON or "").strip()
        return v.startswith('{"type"') and "..." not in v

    @property
    def creds_file_exists(self) -> bool:
        return bool(self.CREDS_FILE) and os.path.exists(self.CREDS_FILE)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached singleton. Cached so the .env is parsed exactly once."""
    return Settings()  # type: ignore[call-arg]


# Module-level convenience handle. Imports as ``from .config import settings``.
settings: Settings = get_settings()
