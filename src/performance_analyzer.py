"""
SV Dugout Pulse — Performance Analyzer ("The Kent Filter")

Takes raw stats and assigns a performance_grade plus a social search URL.
"""

import logging
from urllib.parse import quote

from .config import (
    HITTER_GOOD_HITS,
    HITTER_STANDOUT_HITS,
    PITCHER_QS_IP,
    PITCHER_QS_MAX_ER,
    PITCHER_STANDOUT_KS,
    SLUMP_HITLESS_AB,
)

logger = logging.getLogger(__name__)

# Grade definitions (emoji + label)
GRADE_MILESTONE = "\U0001f48e Milestone"
GRADE_STANDOUT = "\U0001f525 Standout"
GRADE_GOOD = "\u2705 Good"
GRADE_ROUTINE = "\U0001f610 Routine"
GRADE_SOFT_FLAG = "\U0001f6a9 Soft Flag"
GRADE_SCHEDULED = "\U0001f552 Scheduled"
GRADE_NO_DATA = "\u2014 No Data"


class PerformanceAnalyzer:
    """Analyze a player's daily stats and assign a performance grade."""

    def analyze(self, player: dict, stats: dict) -> dict:
        """
        Returns a dict with:
          - performance_grade: emoji + label string
          - social_search_url: X search deep link
        """
        grade = self._grade(player, stats)
        search_url = self._build_social_url(player)

        return {
            "performance_grade": grade,
            "social_search_url": search_url,
        }

    # ----- Grading logic -----

    def _grade(self, player: dict, stats: dict) -> str:
        if stats.get("game_status") == "N/A":
            return GRADE_NO_DATA

        if stats.get("game_status") == "Scheduled":
            return GRADE_SCHEDULED

        # Milestone always takes priority
        if stats.get("is_debut"):
            return GRADE_MILESTONE
        if stats.get("milestone_label"):
            return GRADE_MILESTONE

        position = player.get("position", "Hitter")
        if position == "Pitcher" or stats.get("is_pitcher_line"):
            return self._grade_pitcher(stats)
        elif position == "Two-Way":
            # Grade whichever line they have today
            if stats.get("is_pitcher_line"):
                return self._grade_pitcher(stats)
            return self._grade_hitter(stats)
        else:
            return self._grade_hitter(stats)

    def _grade_hitter(self, stats: dict) -> str:
        hits = stats.get("hits", 0)
        ab = stats.get("at_bats", 0)
        hr = stats.get("home_runs", 0)
        rbi = stats.get("rbi", 0)
        bb = stats.get("walks", 0)
        k = stats.get("strikeouts", 0)
        tob = hits + bb  # times on base (approx, no HBP available)
        pa = ab + bb      # plate appearances (approx)

        # Standout: HR, 3+ hits, high-leverage RBI (3+),
        # elite plate discipline (3+ BB), or dominant OBP day (4+ times on base)
        if hr >= 1 or hits >= HITTER_STANDOUT_HITS or rbi >= 3 or bb >= 3 or tob >= 4:
            return GRADE_STANDOUT

        # Good: 2+ hits, productive on-base day (2+ TOB in 3+ PA), or 2+ RBI
        if hits >= HITTER_GOOD_HITS or (tob >= 2 and pa >= 3) or rbi >= 2:
            return GRADE_GOOD

        # Soft flag: hitless in 4+ AB, or 3+ strikeouts
        if (ab >= 4 and hits == 0) or (k >= 3 and pa >= 3):
            return GRADE_SOFT_FLAG

        # Routine: everything else
        return GRADE_ROUTINE

    def _grade_pitcher(self, stats: dict) -> str:
        ip = stats.get("ip", 0.0)
        er = stats.get("earned_runs", 0)
        k = stats.get("strikeouts", 0)
        bb = stats.get("walks_allowed", stats.get("bb", 0))
        saves = stats.get("saves", 0)
        qs = stats.get("quality_start", False)

        k_bb = k - bb
        bb_per_ip = bb / ip if ip > 0 else 0

        # Hard cap: 5+ ER is always a bad day
        if er >= 5:
            return GRADE_SOFT_FLAG

        # Save is always Standout
        if saves >= 1:
            return GRADE_STANDOUT

        # Standout: QS with decent command, or dominant K-BB with low ER
        if (qs and k_bb >= 2) or (k_bb >= 5 and bb_per_ip <= 1.0 and er <= 2):
            return GRADE_STANDOUT

        # Good: strong K-BB (3+ net) with controlled walks
        if k_bb >= 3 and bb_per_ip <= 1.0:
            return GRADE_GOOD

        # Good: clean outing with controlled walks
        if ip > 0 and er == 0 and bb_per_ip <= 1.0:
            return GRADE_GOOD

        # Good: 3+ solid IP with controlled walks
        if ip >= 3.0 and er <= 1 and bb_per_ip <= 1.0:
            return GRADE_GOOD

        # Soft flag: negative K-BB (more walks than Ks), or rough short outing
        if (k_bb < 0 and ip >= 2.0) or (ip < 4.0 and er >= 3):
            return GRADE_SOFT_FLAG

        return GRADE_ROUTINE

    # ----- Social search URL -----

    @staticmethod
    def _build_social_url(player: dict) -> str:
        name = player.get("player_name", "")
        team = player.get("team", "")
        level = player.get("level", "")
        # Pro: use last word ("Yankees", "Reds") to avoid city clutter
        # NCAA: use full school name ("Florida State", "South Carolina")
        if level == "Pro":
            team_keyword = team.split()[-1] if team else ""
        else:
            team_keyword = team
        query = f'"{name}" {team_keyword}'.strip()
        return f"https://x.com/search?q={quote(query)}&f=live"
