"""Colour matching — Bug #8 verification."""

from __future__ import annotations

from hourly_logger.colors import (
    CATEGORIES,
    CATEGORY_SHORTCUTS,
    category_emoji,
    nearest_category,
)


def test_exact_swatches_match() -> None:
    for name, info in CATEGORIES.items():
        c = info["color"]
        assert nearest_category(c["red"], c["green"], c["blue"]) == name


def test_gray_pair_matches_under_default_threshold() -> None:
    # Bug #8: this case used to fall outside the 0.25 threshold.
    # (0.6, 0.6, 0.6) sits at distance ~0.346 from Professional (0.8,0.8,0.8) —
    # outside the old 0.25 default but inside the new 0.35 default.
    assert nearest_category(0.6, 0.6, 0.6) == "🔘 Professional"


def test_far_color_returns_empty_string() -> None:
    # Saturated red is not one of the categories — should be unmatched
    # because the nearest neighbour (Other / white) is too far away.
    assert nearest_category(1.0, 0.0, 0.0) == ""


def test_zero_channels_default_to_creative_via_proximity() -> None:
    # Sheets API omits 0.0 channels. (0,1,0) = Creative.
    assert nearest_category(0.0, 1.0, 0.0) == "🟢 Creative"
    # Social uses (1,1,0) — blue=0, would be misread as Creative if defaults were wrong.
    assert nearest_category(1.0, 1.0, 0.0) == "🟡 Social"


def test_threshold_can_be_overridden() -> None:
    # With a tight threshold the gray pair fails again — proves the knob works.
    # (0.6, 0.6, 0.6) is ~0.346 from Professional, well outside 0.05.
    assert nearest_category(0.6, 0.6, 0.6, threshold=0.05) == ""


def test_category_emoji_handles_none() -> None:
    assert category_emoji(None) == "?"
    assert category_emoji("🟢 Creative") == "🟢"


def test_shortcut_table_covers_all_categories() -> None:
    distinct = set(CATEGORY_SHORTCUTS.values())
    assert distinct == set(CATEGORIES.keys())
