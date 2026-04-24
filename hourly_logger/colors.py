"""Category colour constants and matching.

Bug #8 fix: the matching threshold lives in :mod:`config` and defaults to
0.35 (was 0.25). The old value rejected the legitimate gray pair distance
of 0.346 between Professional ``#cccccc`` and Other ``#ffffff``.

This module is shared between the live grid writer, ``/migrate``, and
``/fixcats``, so there is exactly one place to tune colour behaviour.
"""

from __future__ import annotations

from typing import Optional

from .config import settings


# Category → display name → RGB (0..1). Order is significant: it is also the
# canonical display order for /trend and /status.
CATEGORIES: dict[str, dict[str, dict[str, float]]] = {
    "🟢 Creative":     {"color": {"red": 0.0, "green": 1.0, "blue": 0.0}},
    "💎 Health":       {"color": {"red": 0.0, "green": 1.0, "blue": 1.0}},
    "🔘 Professional": {"color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
    "🟡 Social":       {"color": {"red": 1.0, "green": 1.0, "blue": 0.0}},
    "⚪️ Other":        {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}},
}

CATEGORY_ORDER = list(CATEGORIES.keys())

# Quick-log shortcuts.
CATEGORY_SHORTCUTS: dict[str, str] = {
    "c": "🟢 Creative",    "cr": "🟢 Creative",    "creative": "🟢 Creative",
    "h": "💎 Health",      "he": "💎 Health",      "health":   "💎 Health",
    "p": "🔘 Professional","pr": "🔘 Professional","prof":     "🔘 Professional",
                                                    "professional": "🔘 Professional",
    "s": "🟡 Social",      "so": "🟡 Social",      "social":   "🟡 Social",
    "o": "⚪️ Other",       "ot": "⚪️ Other",       "other":    "⚪️ Other",
}


def nearest_category(
    r: float,
    g: float,
    b: float,
    threshold: Optional[float] = None,
) -> str:
    """Return the closest category name, or ``""`` if no swatch is within
    ``threshold`` Euclidean RGB distance.

    Bug #8: an empty cell still has all three channels = 0.0 (the API omits
    them). We treat *only* exact white as "Other"; everything else is
    matched on distance. The default threshold of 0.35 covers the gray pair.
    """
    threshold = threshold if threshold is not None else settings.COLOR_MATCH_THRESHOLD
    best_name = ""
    best_d = float("inf")
    for name, info in CATEGORIES.items():
        c = info["color"]
        d = ((r - c["red"]) ** 2 + (g - c["green"]) ** 2 + (b - c["blue"]) ** 2) ** 0.5
        if d < best_d:
            best_d, best_name = d, name
    return best_name if best_d <= threshold else ""


def category_emoji(name: Optional[str]) -> str:
    """Return the leading emoji of a category name, or ``?`` if unknown."""
    if not name:
        return "?"
    return name.split()[0]
