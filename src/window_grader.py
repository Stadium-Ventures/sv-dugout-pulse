"""
SV Dugout Pulse — Window Stats Grading

Assigns performance grades to aggregate window stats (7D/30D/Season).
Uses different thresholds than daily grades:
- Hitters graded on OPS
- Pitchers graded on ERA
"""

from .config import (
    WINDOW_HITTER_HOT_OPS,
    WINDOW_HITTER_SOLID_OPS,
    WINDOW_HITTER_QUIET_OPS,
    WINDOW_PITCHER_HOT_ERA,
    WINDOW_PITCHER_SOLID_ERA,
    WINDOW_PITCHER_QUIET_ERA,
)

# Grade labels with emojis
GRADE_HOT = "🔥 Hot"
GRADE_SOLID = "✅ Solid"
GRADE_QUIET = "😐 Steady"
GRADE_COLD = "🥶 Cold"
GRADE_INSUFFICIENT = "— Insufficient"


def grade_hitter_window(stats: dict, window: str) -> str:
    """
    Grade a hitter based on OPS over the time window.

    Thresholds:
    - Hot:   OPS >= 1.000
    - Solid: OPS >= 0.750
    - Quiet: OPS >= 0.550
    - Cold:  OPS < 0.550
    """
    ops = stats.get("ops", 0)

    if ops >= WINDOW_HITTER_HOT_OPS:
        return GRADE_HOT
    elif ops >= WINDOW_HITTER_SOLID_OPS:
        return GRADE_SOLID
    elif ops >= WINDOW_HITTER_QUIET_OPS:
        return GRADE_QUIET
    else:
        return GRADE_COLD


def grade_pitcher_window(stats: dict, window: str) -> str:
    """
    Grade a pitcher based on ERA over the time window.

    Thresholds:
    - Hot:   ERA <= 2.00
    - Solid: ERA <= 3.50
    - Quiet: ERA <= 5.00
    - Cold:  ERA > 5.00
    """
    era = stats.get("era", 99)

    if era <= WINDOW_PITCHER_HOT_ERA:
        return GRADE_HOT
    elif era <= WINDOW_PITCHER_SOLID_ERA:
        return GRADE_SOLID
    elif era <= WINDOW_PITCHER_QUIET_ERA:
        return GRADE_QUIET
    else:
        return GRADE_COLD


def get_grade_class(grade: str) -> str:
    """Return CSS class name for a grade."""
    if "Hot" in grade or "🔥" in grade:
        return "grade-hot"
    elif "Solid" in grade or "✅" in grade:
        return "grade-solid"
    elif "Steady" in grade or "😐" in grade:
        return "grade-quiet"
    elif "Cold" in grade or "🥶" in grade:
        return "grade-cold"
    else:
        return "grade-insufficient"
