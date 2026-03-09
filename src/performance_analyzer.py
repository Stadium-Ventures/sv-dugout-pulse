"""
SV Dugout Pulse — Performance Analyzer ("The Kent Filter")

Takes raw stats and assigns a performance_grade plus a social search URL.
"""

import logging
from urllib.parse import quote

from .config import (
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
GRADE_SOFT_FLAG = "\U0001f6a9 Off Day"
GRADE_SCHEDULED = "\U0001f552 Scheduled"
GRADE_NO_DATA = "\u2014 No Data"


class PerformanceAnalyzer:
    """Analyze a player's daily stats and assign a performance grade."""

    def analyze(self, player: dict, stats: dict) -> dict:
        """
        Returns a dict with:
          - performance_grade: emoji + label string
          - grade_reason: plain-English one-liner explaining the grade
          - social_search_url: X search deep link
        """
        grade, reason = self._grade(player, stats)
        search_url = self._build_social_url(player)

        return {
            "performance_grade": grade,
            "grade_reason": reason,
            "social_search_url": search_url,
        }

    # ----- Grading logic -----

    def _grade(self, player: dict, stats: dict) -> tuple[str, str]:
        if stats.get("game_status") == "N/A":
            return GRADE_NO_DATA, ""

        if stats.get("game_status") == "Scheduled":
            return GRADE_SCHEDULED, ""

        # Milestone always takes priority
        if stats.get("is_debut"):
            return GRADE_MILESTONE, "First career appearance"
        if stats.get("milestone_label"):
            return GRADE_MILESTONE, stats["milestone_label"]

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

    def _grade_hitter(self, stats: dict) -> tuple[str, str]:
        hits = stats.get("hits", 0)
        ab = stats.get("at_bats", 0)
        hr = stats.get("home_runs", 0)
        rbi = stats.get("rbi", 0)
        bb = stats.get("walks", 0)
        k = stats.get("strikeouts", 0)
        sb = stats.get("stolen_bases", 0)
        doubles = stats.get("doubles", 0)
        triples = stats.get("triples", 0)
        hbp = stats.get("hit_by_pitch", 0)
        pa = ab + bb + hbp
        singles = max(hits - doubles - triples - hr, 0)
        tob = hits + bb + hbp

        # ── Game wOBA (standard linear weights) ──
        woba_num = (
            0.69 * (bb + hbp)
            + 0.87 * singles
            + 1.22 * doubles
            + 1.58 * triples
            + 2.01 * hr
        )
        woba = woba_num / pa if pa > 0 else 0.0

        # Bonuses: a little extra for SB, tiny extra for RBI
        game_score = woba + 0.10 * sb + 0.03 * rbi

        # Hard rule: 0-for-3+ is never Good or better
        hitless_cap = hits == 0 and ab >= 3

        # ── Off Day: hitless in 4+ AB ──
        if ab >= 4 and hits == 0:
            reasons = [f"Hitless in {ab} at-bats"]
            if k >= 3:
                reasons.append(f"{k} strikeouts")
            return GRADE_SOFT_FLAG, " with ".join(reasons)

        # ── Standout: dominant wOBA game (>= .650) ──
        if game_score >= 0.650 and not hitless_cap:
            return GRADE_STANDOUT, self._hitter_reason(stats, tob)

        # ── Good: solid wOBA game (>= .350) ──
        if game_score >= 0.350 and not hitless_cap:
            return GRADE_GOOD, self._hitter_reason(stats, tob)

        # ── Good: reached base early (small sample, game still going) ──
        if pa <= 2 and tob >= 1 and not hitless_cap:
            return GRADE_GOOD, "Reached base early"

        # ── Routine ──
        return GRADE_ROUTINE, f"{hits}-for-{ab}" if ab > 0 else ""

    @staticmethod
    def _hitter_reason(stats: dict, tob: int) -> str:
        """Build a human-readable reason string for Good/Standout grades."""
        hr = stats.get("home_runs", 0)
        hits = stats.get("hits", 0)
        doubles = stats.get("doubles", 0)
        triples = stats.get("triples", 0)
        sb = stats.get("stolen_bases", 0)
        rbi = stats.get("rbi", 0)
        bb = stats.get("walks", 0)

        reasons = []
        if hr >= 1:
            reasons.append(f"{hr} HR" if hr > 1 else "Home run")
        if hits >= 3:
            reasons.append(f"{hits} hits")
        elif doubles + triples >= 1 and not hr:
            xb_count = doubles + triples
            xb_type = "triple" if triples else "double"
            if xb_count > 1:
                reasons.append(f"{xb_count} extra-base hits")
            else:
                reasons.append(f"Extra-base hit ({xb_type})")
        if sb >= 2:
            reasons.append(f"{sb} stolen bases")
        if rbi >= 3:
            reasons.append(f"{rbi} RBI")
        if bb >= 3:
            reasons.append(f"{bb} walks")
        if not reasons:
            if tob >= 3:
                reasons.append(f"Reached base {tob} times")
            elif hits >= 2:
                reasons.append(f"{hits} hits")
            elif rbi >= 2:
                reasons.append(f"{rbi} RBI")
            elif sb >= 1:
                reasons.append("Stolen base")
            else:
                reasons.append("Productive game")
        return " + ".join(reasons)

    def _grade_pitcher(self, stats: dict) -> tuple[str, str]:
        ip = stats.get("ip", 0.0)
        er = stats.get("earned_runs", 0)
        k = stats.get("strikeouts", 0)
        bb = stats.get("walks_allowed", stats.get("bb", 0))
        saves = stats.get("saves", 0)
        qs = stats.get("quality_start", False)

        k_bb = k - bb
        bb_per_ip = bb / ip if ip > 0 else 0

        # Format IP for display (e.g., 6.0 → "6", 5.2 → "5.2")
        ip_str = f"{ip:.1f}".rstrip('0').rstrip('.') if ip > 0 else "0"

        # Hard cap: 5+ ER is always a bad day
        if er >= 5:
            return GRADE_SOFT_FLAG, f"{er} earned runs allowed in {ip_str} IP"

        # Save is always Standout
        if saves >= 1:
            return GRADE_STANDOUT, "Earned the save"

        # Standout: QS with decent command, or dominant K-BB with low ER
        if (qs and k_bb >= 2) or (k_bb >= 5 and bb_per_ip <= 1.0 and er <= 2):
            if qs:
                return GRADE_STANDOUT, f"Strong outing — {ip_str} IP, {k} K, {er} ER"
            return GRADE_STANDOUT, f"Dominant — {k} K vs {bb} BB with {er} ER"

        # Good: strong K-BB (3+ net) with controlled walks
        if k_bb >= 3 and bb_per_ip <= 1.0:
            return GRADE_GOOD, f"{k} strikeouts with good command"

        # Good: clean outing with controlled walks
        if ip > 0 and er == 0 and bb_per_ip <= 1.0:
            return GRADE_GOOD, f"Scoreless in {ip_str} IP"

        # Good: 3+ solid IP with controlled walks
        if ip >= 3.0 and er <= 1 and bb_per_ip <= 1.0:
            return GRADE_GOOD, f"Solid — {ip_str} IP, {er} ER"

        # Off Day: negative K-BB (more walks than Ks), or rough short outing
        if (k_bb < 0 and ip >= 2.0) or (ip < 4.0 and er >= 3):
            if k_bb < 0:
                return GRADE_SOFT_FLAG, f"More walks ({bb}) than strikeouts ({k})"
            return GRADE_SOFT_FLAG, f"{er} earned runs in {ip_str} IP"

        return GRADE_ROUTINE, f"{ip_str} IP, {er} ER" if ip > 0 else ""

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
