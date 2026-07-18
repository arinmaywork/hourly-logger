"""AI layer tests: deterministic stats, prompt assembly, and the llm client."""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timedelta, timezone

import pytest

from hourly_logger.journal.schema import DayRecord


@pytest.fixture
def seeded_db(tmp_db_path: str):
    """Fresh DB with 3 hourly entries + 2 journal days."""
    from hourly_logger.database import canonical_ts, db_connect, db_init
    from hourly_logger.journal.ingest import _upsert

    db_init()
    now = datetime.now(timezone.utc)
    rows = [
        (canonical_ts(now - timedelta(hours=2)), "🟢 Creative", "Deep Work"),
        (canonical_ts(now - timedelta(hours=26)), "🟢 Creative", "Deep Work"),
        (canonical_ts(now - timedelta(hours=27)), "💎 Health", "Sleep"),
    ]
    with db_connect() as conn:
        conn.executemany(
            """INSERT INTO queue (scheduled_ts, submitted_ts, category, tag, status)
               VALUES (?, ?, ?, ?, 'done')""",
            [(ts, ts, cat, tag) for ts, cat, tag in rows],
        )

    today = dt.date.today()
    for offset, mood, done in ((1, 3, ["Exercise"]), (0, 5, ["Exercise", "Writing"])):
        date = (today - dt.timedelta(days=offset)).isoformat()
        rec = DayRecord(
            date=date,
            mood=mood,
            planned=["Exercise", "Writing"],
            completed=done,
            good=["shipped feature"] if offset == 0 else [],
            upset=["slept late"],
        )
        _upsert(date, "raw", f"hash-{date}", rec, "rules")
    return tmp_db_path


class TestStats:
    def test_category_hours(self, seeded_db):
        from hourly_logger.ai.stats import category_hours

        assert category_hours(7) == {"🟢 Creative": 2, "💎 Health": 1}

    def test_top_tags(self, seeded_db):
        from hourly_logger.ai.stats import top_tags

        assert top_tags(7)[0] == ("Deep Work", 2)

    def test_journal_aggregates(self, seeded_db):
        from hourly_logger.ai.stats import journal_aggregates

        ja = journal_aggregates(7)
        assert ja["journal_days"] == 2
        assert ja["mood_avg"] == 4.0
        assert ja["completion_pct"] == 75  # 3 done of 4 planned
        assert ("Exercise", 2, 2) in ja["activities"]
        assert ("Writing", 2, 1) in ja["activities"]

    def test_recurring_themes_and_lines(self, seeded_db):
        from hourly_logger.ai.stats import recent_journal_lines, recurring_themes

        good, upset = recurring_themes(7)
        assert upset[0] == ("slept late", 2)
        lines = recent_journal_lines(7)
        assert len(lines) == 2
        assert "missed: Writing" in lines[0]
        assert "done 2/2" in lines[1]

    def test_build_facts_is_grounded(self, seeded_db):
        from hourly_logger.ai.stats import build_facts

        facts = build_facts(30)
        assert "3h total" in facts
        assert "Deep Work 2h" in facts
        assert "plan-completion 75%" in facts
        assert "slept late×2" in facts


class TestAsk:
    def test_build_prompt_contains_data_and_question(self, seeded_db):
        from hourly_logger.ai.ask import build_prompt

        prompt = build_prompt("  why am I tired?  " + "x" * 600)
        assert "=== DATA ===" in prompt
        assert "plan-completion 75%" in prompt
        assert "why am I tired?" in prompt
        assert "x" * 500 not in prompt  # question truncated

    def test_answer_routes_through_llm(self, seeded_db, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.ai import ask

        seen = {}

        def fake_generate(prompt, **kw):
            seen["prompt"], seen["kw"] = prompt, kw
            return "grounded answer"

        monkeypatch.setattr(ask, "generate", fake_generate)
        assert ask.answer("how are my weeks?") == "grounded answer"
        assert "=== QUESTION ===" in seen["prompt"]
        assert seen["kw"]["max_output_tokens"] == 800


class TestLlmClient:
    def test_retries_429_then_succeeds(self, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.ai import llm
        from hourly_logger.config import settings

        monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-key")
        monkeypatch.setattr(llm.time, "sleep", lambda s: None)
        calls = {"n": 0}

        class Resp:
            def __init__(self, code, body=None):
                self.status_code, self._body = code, body
                self.text = "quota"

            def json(self):
                return self._body

        ok = {"candidates": [{"content": {"parts": [{"text": "hi"}]}}]}

        def fake_post(*a, **k):
            calls["n"] += 1
            return Resp(429) if calls["n"] == 1 else Resp(200, ok)

        monkeypatch.setattr(llm.requests, "post", fake_post)
        assert llm.generate("prompt") == "hi"
        assert calls["n"] == 2

    def test_no_key_fails_without_network(self, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.ai import llm
        from hourly_logger.config import settings

        monkeypatch.setattr(settings, "GEMINI_API_KEY", None)

        def explode(*a, **k):  # any HTTP attempt is a test failure
            raise AssertionError("network call attempted without key")

        monkeypatch.setattr(llm.requests, "post", explode)
        with pytest.raises(llm.LlmError):
            llm.generate("prompt")
