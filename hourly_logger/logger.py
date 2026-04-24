"""Structured logging with a per-request context id.

Improvement #10: emit JSON when ``LOG_JSON=true`` so GCP Cloud Logging can
parse fields directly. Falls back to a pretty human format otherwise so local
dev stays readable.

Usage::

    from hourly_logger.logger import get_logger, request_context

    log = get_logger(__name__)

    async def handle_message(...):
        with request_context(user_id=update.effective_user.id):
            log.info("entry_logged", extra={"queue_id": qid, "lag_min": lag})

The ``extra`` dict is merged into the JSON payload; in human mode it is
appended as ``key=value`` pairs.
"""

from __future__ import annotations

import contextvars
import datetime as dt
import json
import logging
import sys
import uuid
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from .config import settings


# ── Context variable: request id propagated across async tasks ──────────────
_request_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)
_request_meta: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "request_meta", default={}
)


# Standard LogRecord attributes — anything else in record.__dict__ is "extra".
_RESERVED = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName",
}


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per record. Stable field order for grep-ability."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        rid = _request_id.get()
        if rid:
            payload["request_id"] = rid
        meta = _request_meta.get()
        if meta:
            payload["context"] = meta
        for k, v in record.__dict__.items():
            if k not in _RESERVED and not k.startswith("_"):
                payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class HumanFormatter(logging.Formatter):
    """Pretty single-line format with extras appended as ``k=v``."""

    DATE_FMT = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = []
        rid = _request_id.get()
        if rid:
            extras.append(f"rid={rid}")
        for k, v in record.__dict__.items():
            if k not in _RESERVED and not k.startswith("_"):
                extras.append(f"{k}={v}")
        if extras:
            base = f"{base} | {' '.join(extras)}"
        return base


_configured = False


def configure_logging() -> None:
    """Idempotent root-logger setup. Call once on bot start."""
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler(stream=sys.stdout)
    if settings.LOG_JSON:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            HumanFormatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt=HumanFormatter.DATE_FMT,
            )
        )
    root = logging.getLogger()
    # Replace any handlers (e.g. from a basicConfig elsewhere).
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.setLevel(getattr(logging, settings.LOG_LEVEL))

    # Telegram lib is chatty at INFO — clamp it a notch unless the user
    # explicitly asked for DEBUG.
    if settings.LOG_LEVEL != "DEBUG":
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("apscheduler").setLevel(logging.WARNING)
        logging.getLogger("telegram").setLevel(logging.INFO)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger. Calls ``configure_logging()`` lazily."""
    if not _configured:
        configure_logging()
    return logging.getLogger(name)


@contextmanager
def request_context(**meta: Any) -> Iterator[str]:
    """Bind a fresh request id (and optional metadata) to the current task.

    All log lines emitted inside the ``with`` block carry the request id, so
    one user interaction can be traced end-to-end across handlers, the
    background sync task, and Sheets retries.
    """
    rid = uuid.uuid4().hex[:8]
    rid_token = _request_id.set(rid)
    meta_token = _request_meta.set(meta)
    try:
        yield rid
    finally:
        _request_id.reset(rid_token)
        _request_meta.reset(meta_token)
