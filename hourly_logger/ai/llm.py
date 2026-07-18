"""Single Gemini entry point for the whole codebase.

Every Gemini call (journal extraction fallback, /ask, weekly review) goes
through :func:`generate`, so retry policy, timeouts, and token discipline
live in exactly one place. Free-tier friendly: temperature and output caps
are explicit per call-site, 429/5xx retried with exponential backoff,
non-retryable errors (bad key, bad schema) fail fast without burning quota.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import requests

from ..config import settings
from ..logger import get_logger

log = get_logger(__name__)

_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_RETRYABLE = (429, 500, 502, 503, 504)


class LlmError(RuntimeError):
    """Raised when a Gemini call fails after all retries."""


def generate(
    prompt: str,
    *,
    response_schema: Optional[dict[str, Any]] = None,
    temperature: float = 0.4,
    max_output_tokens: int = 1024,
    thinking_budget: int = 0,
) -> str:
    """One prompt in, text out. ``response_schema`` switches on structured
    JSON output (the returned string is then guaranteed-parseable JSON).

    ``thinking_budget=0`` disables Gemini 2.5's hidden reasoning tokens,
    which otherwise count against ``maxOutputTokens`` and can silently
    truncate the visible answer mid-sentence.
    """
    if not settings.GEMINI_API_KEY:
        raise LlmError("GEMINI_API_KEY is not configured")

    gen_config: dict[str, Any] = {
        "temperature": temperature,
        "maxOutputTokens": max_output_tokens,
        "thinkingConfig": {"thinkingBudget": thinking_budget},
    }
    if response_schema is not None:
        gen_config["responseMimeType"] = "application/json"
        gen_config["responseSchema"] = response_schema

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": gen_config,
    }
    data = _post_with_retries(payload)
    try:
        return str(data["candidates"][0]["content"]["parts"][0]["text"])
    except (KeyError, IndexError, TypeError) as e:
        raise LlmError(f"Unexpected Gemini response shape: {e}") from e


def _post_with_retries(payload: dict[str, Any]) -> dict[str, Any]:
    url = _API_URL.format(model=settings.GEMINI_MODEL)
    last = "no attempt made"
    for attempt in range(1, settings.GEMINI_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                url,
                params={"key": settings.GEMINI_API_KEY},
                json=payload,
                timeout=settings.GEMINI_TIMEOUT_S,
            )
            if resp.status_code == 200:
                return resp.json()
            last = f"HTTP {resp.status_code}: {resp.text[:200]}"
            if resp.status_code not in _RETRYABLE:
                break
        except requests.RequestException as e:
            last = f"network error: {e}"
        if attempt < settings.GEMINI_MAX_RETRIES:
            time.sleep(min(2 ** attempt * 2, 30))
    raise LlmError(f"Gemini call failed: {last}")
