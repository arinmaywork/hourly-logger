"""Sheets retry / circuit breaker — Improvement #9."""

from __future__ import annotations

import time

import pytest

from hourly_logger.sheets import CircuitBreaker, CircuitOpenError


def test_breaker_starts_closed() -> None:
    b = CircuitBreaker(threshold=3, cooldown_s=60)
    b.before()  # does not raise
    assert not b.is_open


def test_breaker_opens_after_threshold_failures() -> None:
    b = CircuitBreaker(threshold=3, cooldown_s=60)
    for _ in range(3):
        b.on_failure()
    assert b.is_open
    with pytest.raises(CircuitOpenError):
        b.before()


def test_success_resets_failure_count() -> None:
    b = CircuitBreaker(threshold=3, cooldown_s=60)
    b.on_failure()
    b.on_failure()
    b.on_success()
    # Should be back to a fresh state — one more failure shouldn't open.
    b.on_failure()
    assert not b.is_open


def test_breaker_half_opens_after_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    b = CircuitBreaker(threshold=2, cooldown_s=10)
    b.on_failure()
    b.on_failure()
    assert b.is_open

    # Simulate time passing past the cooldown.
    real_monotonic = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 1000)
    # Probe is allowed through (half-open).
    b.before()


def test_breaker_reopens_on_post_cooldown_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    b = CircuitBreaker(threshold=2, cooldown_s=10)
    b.on_failure(); b.on_failure()
    assert b.is_open
    real_monotonic = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 1000)
    b.before()  # half-open probe
    b.on_failure()
    b.on_failure()
    assert b.is_open
