"""Journal module tests: parser (against the real template), fallback logic,
ingest pipeline, and the Gemini extractor with a mocked HTTP layer."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from hourly_logger.journal.parser import parse_note
from hourly_logger.journal.schema import DayRecord

# Verbatim structure of the user's actual daily note (2026-07-04).
SAMPLE = """---
title: <% tp.file.title %>
alias:
tags: Dailynote/Journal
modified:
  - 2026-03-06 18:09
---
# 2026-07-04 -- [[2026-07-04]]
%%
Essentials - Exercise, Journaling, Jadoo, Bro Help, Deep work
Food Tracking & Cold bath
%%
## Summary
[ALL GOALS](ALL%20GOALS.md)
[[List of Projects]]
### Activities
- ~~Journal - Morning & Night~~
- Exercise
- ~~Home Cooking~~
- ~~Writing~~
- ~~Bro Help~~
- ~~Her Day~~
- ~~Vpn tool~~
- ~~cms tool~~
### Did anything for health? - [Food Routine](Food%20Routine.md)
- Breakfast - nuts
- Lunch - biryani
- Dinner - quinoa & eggs
- Cold Bath - no
### What was good about today
- Railway workshop
- ai tools
- talk with Surbhi
### What made you upset
- wated time in the morning
"""


class TestParser:
    def test_real_template(self):
        rec = parse_note(SAMPLE, "2026-07-04")
        assert rec.date == "2026-07-04"
        # struck = completed; Exercise was planned but NOT struck
        assert "Exercise" in rec.planned
        assert "Exercise" not in rec.completed
        assert "Journal - Morning & Night" in rec.completed
        assert "cms tool" in rec.completed
        assert len(rec.planned) == 8
        assert len(rec.completed) == 7
        assert rec.good == ["Railway workshop", "ai tools", "talk with Surbhi"]
        assert rec.upset == ["wated time in the morning"]
        assert rec.food["breakfast"] == "nuts"
        assert rec.food["cold bath"] == "no"
        assert not rec.is_thin()

    def test_obsidian_comment_block_ignored(self):
        rec = parse_note(SAMPLE, "2026-07-04")
        assert "Essentials" not in rec.free_text

    def test_frontmatter_and_inline_scores(self):
        note = "---\nmood: 4\n---\n### Activities\n- Exercise\nenergy: 2\n"
        rec = parse_note(note, "2026-01-01")
        assert rec.mood == 4 and rec.energy == 2

    def test_checkbox_variant_future_proof(self):
        note = "### Activities\n- [x] Exercise\n- [ ] Writing\n"
        rec = parse_note(note, "2026-01-01")
        assert rec.completed == ["Exercise"]
        assert rec.planned == ["Exercise", "Writing"]

    def test_renamed_heading_still_matches_keywords(self):
        note = "## My tasks today\n- ~~Gym~~\n## Highlights\n- shipped feature\n"
        rec = parse_note(note, "2026-01-01")
        assert rec.completed == ["Gym"]
        assert rec.good == ["shipped feature"]

    def test_wikilinks_and_mdlinks_cleaned(self):
        note = "### Activities\n- ~~[[Vpn tool|VPN]]~~\n- [Writing](w.md)\n"
        rec = parse_note(note, "2026-01-01")
        assert rec.completed == ["VPN"]
        assert "Writing" in rec.planned

    def test_unrecognised_structure_is_thin(self):
        rec = parse_note("# Title\nshort line\n", "2026-01-01")
        assert rec.is_thin()

    def test_prose_heavy_note_is_not_thin(self):
        rec = parse_note("Today was a long day. " * 5, "2026-01-01")
        assert not rec.is_thin()
        assert "long day" in rec.free_text

    def test_never_raises_on_garbage(self):
        for garbage in ("", "---\nbroken", "~~~~", "### \n- \n- ~~~~\n"):
            parse_note(garbage, "2026-01-01")  # must not raise


class TestIngest:
    @pytest.fixture
    def vault(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_db_path: str) -> Path:
        from hourly_logger.config import settings
        from hourly_logger.database import db_init

        vault = tmp_path / "vault"
        (vault / "Daily").mkdir(parents=True)
        monkeypatch.setattr(settings, "VAULT_DIR", str(vault))
        monkeypatch.setattr(settings, "GEMINI_API_KEY", None)
        db_init()
        return vault

    def _write_note(self, vault: Path, date: str, body: str = SAMPLE) -> Path:
        p = vault / "Daily" / f"{date}.md"
        p.write_text(body, encoding="utf-8")
        return p

    def test_insert_then_unchanged_then_updated(self, vault: Path):
        from hourly_logger.database import db_connect
        from hourly_logger.journal.ingest import ingest

        today = dt.date.today().isoformat()
        self._write_note(vault, today)

        s1 = ingest(pull=False)
        assert [r.action for r in s1.results] == ["inserted"]
        assert not s1.alarms

        s2 = ingest(pull=False)
        assert [r.action for r in s2.results] == ["unchanged"]

        # appended bullet falls under the last section ("What made you upset")
        self._write_note(vault, today, SAMPLE + "\n- slept too late")
        s3 = ingest(pull=False)
        assert [r.action for r in s3.results] == ["updated"]

        with db_connect() as conn:
            day = conn.execute("SELECT * FROM journal_days WHERE date=?", (today,)).fetchone()
            assert day["extractor"] == "rules" and day["thin"] == 0
            rec = DayRecord.model_validate_json(day["parsed_json"])
            assert rec.upset[-1] == "slept too late"
            acts = conn.execute(
                "SELECT name, completed FROM journal_activities WHERE date=? ORDER BY name",
                (today,),
            ).fetchall()
            assert {a["name"]: a["completed"] for a in acts}["Exercise"] == 0
            assert {a["name"]: a["completed"] for a in acts}["Writing"] == 1

    def test_thin_note_raises_alarm(self, vault: Path):
        from hourly_logger.journal.ingest import ingest

        today = dt.date.today().isoformat()
        self._write_note(vault, today, "# just a title\n")
        s = ingest(pull=False)
        assert s.results[0].thin
        assert any("template change" in a for a in s.alarms)

    def test_old_notes_outside_window_ignored(self, vault: Path):
        from hourly_logger.journal.ingest import ingest

        self._write_note(vault, "2020-01-01")
        assert ingest(since_days=7, pull=False).results == []

    def test_non_daily_files_ignored(self, vault: Path):
        from hourly_logger.journal.ingest import ingest

        (vault / "Daily" / "ALL GOALS.md").write_text("goals", encoding="utf-8")
        assert ingest(pull=False).results == []

    def test_thin_falls_back_to_gemini(self, vault: Path, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.config import settings
        from hourly_logger.journal import ingest as ing

        monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-key")
        called = {}

        def fake_extract(raw: str, date: str) -> DayRecord:
            called["date"] = date
            return DayRecord(date=date, good=["extracted by llm"])

        monkeypatch.setattr(ing, "extract", fake_extract)
        today = dt.date.today().isoformat()
        self._write_note(vault, today, "# totally new template\nshort\n")
        s = ing.ingest(pull=False)
        assert called["date"] == today
        assert s.results[0].extractor == "gemini"
        assert not s.results[0].thin and not s.alarms

    def test_gemini_failure_keeps_rules_result_and_alarms(
        self, vault: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from hourly_logger.config import settings
        from hourly_logger.journal import ingest as ing
        from hourly_logger.journal.extractor import ExtractorError

        monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-key")

        def boom(raw: str, date: str) -> DayRecord:
            raise ExtractorError("HTTP 429")

        monkeypatch.setattr(ing, "extract", boom)
        today = dt.date.today().isoformat()
        self._write_note(vault, today, "# new template\nshort\n")
        s = ing.ingest(pull=False)
        assert s.results[0].action == "inserted"  # rules result still stored
        assert any("gemini fallback failed" in a for a in s.alarms)


class TestExtractor:
    def _gemini_response(self, payload: dict) -> dict:
        return {
            "candidates": [
                {"content": {"parts": [{"text": json.dumps(payload)}]}}
            ]
        }

    def test_extract_maps_schema(self, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.ai import llm
        from hourly_logger.config import settings
        from hourly_logger.journal import extractor

        monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-key")
        sent = {}

        class FakeResp:
            status_code = 200

            def json(self_inner):
                return self._gemini_response(
                    {
                        "mood": 4,
                        "energy": 9,  # out of range -> dropped
                        "planned": ["Exercise", "Writing"],
                        "completed": ["Writing"],
                        "good": ["workshop"],
                        "upset": [],
                        "food": [{"meal": "Lunch", "item": "biryani"}],
                        "free_text": "",
                    }
                )

        def fake_post(url, params=None, json=None, timeout=None):
            sent["url"], sent["json"] = url, json
            return FakeResp()

        monkeypatch.setattr(llm.requests, "post", fake_post)
        rec = extractor.extract(SAMPLE, "2026-07-04")
        assert rec.mood == 4 and rec.energy is None
        assert rec.completed == ["Writing"]
        assert rec.food == {"lunch": "biryani"}
        assert settings.GEMINI_MODEL in sent["url"]
        assert sent["json"]["generationConfig"]["responseMimeType"] == "application/json"

    def test_non_retryable_error_fails_fast(self, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.ai import llm
        from hourly_logger.config import settings
        from hourly_logger.journal import extractor

        monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-key")
        calls = {"n": 0}

        class FakeResp:
            status_code = 400
            text = "bad request"

        def fake_post(*a, **k):
            calls["n"] += 1
            return FakeResp()

        monkeypatch.setattr(llm.requests, "post", fake_post)
        with pytest.raises(extractor.ExtractorError):
            extractor.extract("note", "2026-01-01")
        assert calls["n"] == 1  # 400 must not burn retries

    def test_no_key_refuses(self, monkeypatch: pytest.MonkeyPatch):
        from hourly_logger.config import settings
        from hourly_logger.journal import extractor

        monkeypatch.setattr(settings, "GEMINI_API_KEY", None)
        with pytest.raises(extractor.ExtractorError):
            extractor.extract("note", "2026-01-01")
