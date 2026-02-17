"""
SV Dugout Pulse — Stats Engine

Two ecosystems:
  1. Pro (MLB/MiLB) — via the MLB-StatsAPI library
  2. NCAA — fault-tolerant framework with pluggable school scrapers
"""

from __future__ import annotations

import abc
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests
import statsapi
from bs4 import BeautifulSoup

from .config import ROSTER_URL

logger = logging.getLogger(__name__)


# ===== Shared helpers =====

# School-name qualifiers that indicate a different school when they appear
# as a suffix ("Florida State") or prefix ("North Florida") to a base name.
_SUFFIX_QUALIFIERS = {
    "a&t", "state", "st", "st.", "central", "wilmington", "charlotte",
    "greensboro", "pembroke", "asheville", "a&m", "am", "at", "tech",
    "southern", "northern", "eastern", "western", "international",
    "atlantic", "pacific", "gulf", "upstate", "baptist", "christian",
    "lutheran", "wesleyan", "methodist", "valley", "polytechnic",
    "poly", "marymount", "of",
}
_PREFIX_QUALIFIERS = {
    "north", "south", "east", "west", "central", "se", "ne", "sw", "nw",
    "coastal", "fiu",
}


def _school_name_matches(team_lower: str, names: list[str], exact: bool) -> bool:
    """Match our team name against candidate name strings.

    *exact* mode: equality only.
    *substring* mode: ``team_lower in name``, but rejects false positives where
    the candidate is actually a different school, e.g.:
        - "florida" in "florida state"   → False (suffix qualifier "state")
        - "florida" in "north florida"   → False (prefix qualifier "north")
        - "florida" in "florida gators"  → True  (mascot, not qualifier)
        - "carolina" in "coastal carolina" → only if searching for "carolina"
    """
    for n in names:
        n_lower = n.lower()
        if not n_lower:
            continue
        if exact:
            if team_lower == n_lower:
                return True
        else:
            if team_lower not in n_lower:
                continue

            reject = False

            # Suffix guard: "florida" in "florida state" — check word after match
            if n_lower.startswith(team_lower) and len(n_lower) > len(team_lower):
                suffix = n_lower[len(team_lower):].strip()
                first_word = suffix.split()[0] if suffix else ""
                if first_word in _SUFFIX_QUALIFIERS:
                    reject = True

            # Prefix guard: "florida" in "north florida" — check word before match
            idx = n_lower.find(team_lower)
            if idx > 0:
                prefix = n_lower[:idx].strip()
                last_word = prefix.split()[-1] if prefix else ""
                if last_word in _PREFIX_QUALIFIERS:
                    reject = True

            if not reject:
                return True
    return False


# ===== Shared data structures =====

def empty_stats() -> dict:
    """Return a blank stats dict (no data available)."""
    return {
        "stats_summary": "No game data",
        "game_context": "",
        "game_status": "N/A",
        "game_time": None,  # Scheduled start time (e.g., "7:05 PM ET")
        "next_game": None,  # Dict with date, opponent, time for next game
        "game_date": None,  # ISO date string of the actual game (YYYY-MM-DD)
        "is_pitcher_line": False,
        # Raw fields used by the analyzer
        "hits": 0,
        "at_bats": 0,
        "home_runs": 0,
        "rbi": 0,
        "runs": 0,
        "stolen_bases": 0,
        "ip": 0.0,
        "earned_runs": 0,
        "strikeouts": 0,
        "walks_allowed": 0,
        "hits_allowed": 0,
        "saves": 0,
        "win": False,
        "loss": False,
        "quality_start": False,
        "is_debut": False,
        "milestone_label": None,
    }


# =========================================================================
# PRO (MLB / MiLB)
# =========================================================================

# Map common org display names to the franchise search names used by statsapi
TEAM_NAME_MAP = {
    "Athletics": "Oakland Athletics",
    "Unsigned": None,
}


class ProStatsFetcher:
    """Fetch game/stats data for MLB and MiLB players via MLB-StatsAPI."""

    def __init__(self):
        self._games_cache: dict[str, list] = {}
        self._player_cache: dict[str, int] = {}
        self._today = date.today()
        self._today_str = self._today.strftime("%m/%d/%Y")

    # ----- public API -----

    def fetch(self, player: dict) -> dict:
        """
        Given a normalized player dict, attempt to find today's game
        and return a stats dict. Also fetches next game info.
        """
        team = player.get("team", "")
        name = player.get("player_name", "")

        if not team or team == "Unsigned":
            logger.debug("Skipping %s — unsigned / no team", name)
            return empty_stats()

        try:
            player_id = self._lookup_player(name)
            if player_id is None:
                logger.info("Player not found in MLB lookup: %s", name)
                return empty_stats()

            game = self._find_todays_game(player_id, team)

            # Always try to get next game info
            next_game = self._find_next_game(player_id, team)

            if game is None:
                logger.debug("No game today for %s", name)
                result = empty_stats()
                result["next_game"] = next_game
                if next_game:
                    result["stats_summary"] = f"Next: {next_game['display']}"
                else:
                    result["stats_summary"] = "No game scheduled"
                return result

            result = self._extract_stats(player, player_id, game)
            result["next_game"] = next_game
            return result

        except Exception:
            logger.exception("Error fetching pro stats for %s", name)
            return empty_stats()

    # ----- internal helpers -----

    def _lookup_player(self, name: str) -> Optional[int]:
        """Search MLB for a player ID by name, with caching."""
        if name in self._player_cache:
            return self._player_cache[name]

        try:
            results = statsapi.lookup_player(name)
            if results:
                player_id = results[0]["id"]
                self._player_cache[name] = player_id
                return player_id
        except Exception:
            logger.exception("MLB player lookup failed for %s", name)
        return None

    def _find_todays_game(self, player_id: int, team: str = "") -> Optional[dict]:
        """Find a game today that involves the player's team."""
        try:
            schedule = statsapi.schedule(date=self._today_str)

            # Pre-filter schedule by team name to avoid unnecessary boxscore calls
            team_lower = team.lower() if team else ""
            if team_lower:
                candidates = [
                    g for g in schedule
                    if team_lower in g.get("home_name", "").lower()
                    or team_lower in g.get("away_name", "").lower()
                ]
            else:
                candidates = schedule

            for game in candidates:
                try:
                    boxscore = statsapi.boxscore_data(game["game_id"])
                except Exception:
                    logger.debug("Boxscore fetch failed for game %s — skipping", game["game_id"])
                    continue
                # Search both teams' rosters
                for side in ("home", "away"):
                    players = boxscore.get(f"{side}Batters", []) + boxscore.get(
                        f"{side}Pitchers", []
                    )
                    if player_id in players:
                        return {
                            "game_id": game["game_id"],
                            "boxscore": boxscore,
                            "schedule": game,
                            "side": side,
                        }
        except Exception:
            logger.exception("Error searching today's games for player %d", player_id)
        return None

    def _find_next_game(self, player_id: int, team: str) -> Optional[dict]:
        """Find the next scheduled game for a player's team."""
        try:
            # Look ahead up to 14 days (catches spring training start)
            for days_ahead in range(1, 15):
                future_date = self._today + timedelta(days=days_ahead)
                future_str = future_date.strftime("%m/%d/%Y")

                schedule = statsapi.schedule(date=future_str)
                for game in schedule:
                    # Check if this team is playing
                    home = game.get("home_name", "")
                    away = game.get("away_name", "")

                    # Match team name (partial match for flexibility)
                    team_lower = team.lower()
                    if team_lower in home.lower() or team_lower in away.lower():
                        # Determine opponent
                        if team_lower in home.lower():
                            opponent = away
                            home_away = "vs"
                        else:
                            opponent = home
                            home_away = "@"

                        # Parse game time
                        game_time = self._format_game_time(game.get("game_datetime", ""))

                        return {
                            "date": future_date.strftime("%a %m/%d"),
                            "date_full": future_date.isoformat(),
                            "opponent": opponent,
                            "home_away": home_away,
                            "time": game_time,
                            "display": f"{home_away} {opponent} - {future_date.strftime('%a %m/%d')} {game_time}" if game_time else f"{home_away} {opponent} - {future_date.strftime('%a %m/%d')}",
                        }

            return None
        except Exception:
            logger.debug("Error finding next game for %s", team)
            return None

    @staticmethod
    def _format_game_time(game_datetime_str: str) -> str:
        """Format game datetime string to readable time (e.g., '7:05 PM ET')."""
        if not game_datetime_str:
            return ""
        try:
            # MLB API returns ISO format like "2026-04-01T23:05:00Z"
            dt = datetime.fromisoformat(game_datetime_str.replace("Z", "+00:00"))
            # Convert to ET (UTC-4 or UTC-5 depending on DST, approximate with -4)
            from datetime import timezone
            et_offset = timezone(timedelta(hours=-4))
            dt_et = dt.astimezone(et_offset)
            return dt_et.strftime("%-I:%M %p ET").replace(" 0", " ")
        except Exception:
            return ""

    def _extract_stats(self, player: dict, player_id: int, game: dict) -> dict:
        """Pull the player's line from the boxscore."""
        result = empty_stats()
        sched = game["schedule"]
        box = game["boxscore"]

        # Game context
        status = sched.get("status", "Unknown")
        home = sched.get("home_name", "")
        away = sched.get("away_name", "")
        home_score = sched.get("home_score", 0)
        away_score = sched.get("away_score", 0)
        inning = sched.get("current_inning", "")

        # Get scheduled game time
        game_time = self._format_game_time(sched.get("game_datetime", ""))
        result["game_time"] = game_time

        # Populate game_date
        game_datetime = sched.get("game_datetime", "")
        result["game_date"] = game_datetime[:10] if game_datetime and len(game_datetime) >= 10 else self._today.isoformat()

        if status == "Final":
            result["game_context"] = f"{away} {away_score}, {home} {home_score} | Final"
            result["game_status"] = "Final"
        elif status in ("In Progress", "Live"):
            half = sched.get("inning_state", "")
            result["game_context"] = (
                f"{away} {away_score}, {home} {home_score} | {half} {inning}"
            )
            result["game_status"] = "Live"
        elif status in ("Scheduled", "Pre-Game", "Warmup"):
            # Game hasn't started yet - show scheduled time
            result["game_context"] = f"{away} vs {home} | {game_time}" if game_time else f"{away} vs {home}"
            result["game_status"] = "Scheduled"
            result["stats_summary"] = f"Game at {game_time}" if game_time else "Game today"
        else:
            result["game_context"] = f"{away} vs {home} | {status}"
            result["game_status"] = status

        # Find the player's stats line in the boxscore
        pid_str = f"ID{player_id}"
        position = player.get("position", "Hitter")

        if position == "Pitcher":
            result["is_pitcher_line"] = True
            pitchers = box.get(f"{game['side']}Pitchers", [])
            for entry in pitchers:
                if isinstance(entry, dict) and str(player_id) in str(
                    entry.get("personId", "")
                ):
                    result.update(self._parse_pitcher_line(entry))
                    break
        else:
            batters = box.get(f"{game['side']}Batters", [])
            for entry in batters:
                if isinstance(entry, dict) and str(player_id) in str(
                    entry.get("personId", "")
                ):
                    result.update(self._parse_batter_line(entry))
                    break

        return result

    @staticmethod
    def _parse_batter_line(entry: dict) -> dict:
        """Parse a batter's boxscore entry into our stats dict."""
        stats = entry.get("stats", {})
        h = int(stats.get("hits", 0))
        ab = int(stats.get("atBats", 0))
        hr = int(stats.get("homeRuns", 0))
        rbi = int(stats.get("rbi", 0))
        r = int(stats.get("runs", 0))
        sb = int(stats.get("stolenBases", 0))

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(f"{hr} HR" if hr > 1 else "HR")
        if rbi:
            parts.append(f"{rbi} RBI")
        if r:
            parts.append(f"{r} R")
        if sb:
            parts.append(f"{sb} SB")

        return {
            "stats_summary": ", ".join(parts),
            "hits": h,
            "at_bats": ab,
            "home_runs": hr,
            "rbi": rbi,
            "runs": r,
            "stolen_bases": sb,
        }

    @staticmethod
    def _parse_pitcher_line(entry: dict) -> dict:
        """Parse a pitcher's boxscore entry into our stats dict."""
        stats = entry.get("stats", {})
        ip_str = stats.get("inningsPitched", "0")
        ip = float(ip_str) if ip_str else 0.0
        er = int(stats.get("earnedRuns", 0))
        k = int(stats.get("strikeOuts", 0))
        bb = int(stats.get("baseOnBalls", 0))
        ha = int(stats.get("hits", 0))
        sv = int(stats.get("saves", 0))
        w = stats.get("wins", 0)
        l = stats.get("losses", 0)

        parts = [f"{ip_str} IP"]
        if ha:
            parts.append(f"{ha} H")
        parts.append(f"{er} ER")
        parts.append(f"{k} K")
        if bb:
            parts.append(f"{bb} BB")
        if sv:
            parts.append("SV")
        if w:
            parts.append("W")
        if l:
            parts.append("L")

        qs = ip >= 6.0 and er <= 3

        return {
            "stats_summary": ", ".join(parts),
            "is_pitcher_line": True,
            "ip": ip,
            "earned_runs": er,
            "strikeouts": k,
            "walks_allowed": bb,
            "hits_allowed": ha,
            "saves": sv,
            "win": bool(w),
            "loss": bool(l),
            "quality_start": qs,
        }


# =========================================================================
# NCAA — Fault-Tolerant Framework
# =========================================================================


class BaseSchoolScraper(abc.ABC):
    """
    Base class for school-specific NCAA stat scrapers.
    Subclass this and register in SCHOOL_SCRAPERS to add support for a school.
    """

    @abc.abstractmethod
    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        """
        Return a stats dict for the player, or None if unavailable.
        Must not raise — catch and log internally.
        """
        ...


class SidearmScraper(BaseSchoolScraper):
    """
    Scraper for schools using the Sidearm Sports platform.
    Many D1 programs use this (e.g., Florida, Texas, Alabama).
    """

    # Override per school — map school name to its Sidearm base URL
    SIDEARM_URLS: dict[str, str] = {
        # "Florida": "https://floridagators.com/sports/baseball/stats",
        # Add URLs as you discover them
    }

    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        base_url = self.SIDEARM_URLS.get(team)
        if not base_url:
            logger.debug("No Sidearm URL configured for %s", team)
            return None

        try:
            # Sidearm exposes a JSON schedule/stats feed at predictable paths
            resp = requests.get(f"{base_url}?format=json", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return self._find_player_in_feed(player_name, data)
        except Exception:
            logger.info("Sidearm fetch failed for %s @ %s", player_name, team)
            return None

    def _find_player_in_feed(self, player_name: str, data: dict) -> Optional[dict]:
        """Parse the Sidearm JSON feed for a specific player. Override as needed."""
        # Sidearm feed structures vary — this is a starting point
        logger.debug("Sidearm feed parsing not yet implemented for this school")
        return None


class StatBroadcastScraper(BaseSchoolScraper):
    """
    Scraper for schools using the StatBroadcast live stats platform.
    """

    STATBROADCAST_URLS: dict[str, str] = {
        # "Coastal Carolina": "https://statbroadcast.com/events/statmonitr.php?gid=ccu",
        # Add URLs as you discover them
    }

    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        url = self.STATBROADCAST_URLS.get(team)
        if not url:
            logger.debug("No StatBroadcast URL configured for %s", team)
            return None

        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return self._parse_statbroadcast(player_name, resp.text)
        except Exception:
            logger.info("StatBroadcast fetch failed for %s @ %s", player_name, team)
            return None

    def _parse_statbroadcast(self, player_name: str, html: str) -> Optional[dict]:
        logger.debug("StatBroadcast parsing not yet implemented")
        return None


class NCAAOrgScraper(BaseSchoolScraper):
    """
    Fallback scraper: stats.ncaa.org box scores.
    This is the least reliable but widest-coverage option.
    """

    BASE_URL = "https://stats.ncaa.org"

    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        try:
            # stats.ncaa.org requires team lookup -> schedule -> boxscore
            # This is a structural placeholder — the site changes frequently
            logger.info(
                "NCAA.org scraper called for %s @ %s — not yet fully implemented",
                player_name,
                team,
            )
            return None
        except Exception:
            logger.info("NCAA.org fetch failed for %s @ %s", player_name, team)
            return None


class NCAAComScraper(BaseSchoolScraper):
    """
    Scraper using the NCAA.com public data (via ncaa-api proxy).

    Flow: scoreboard → find game by team → fetch box score → find player.
    Covers all D1 programs with individual player box scores.
    """

    SCOREBOARD_URL = "https://ncaa-api.henrygd.me/scoreboard/baseball/d1"
    BOXSCORE_URL = "https://ncaa-api.henrygd.me/game"

    def __init__(self):
        self._scoreboard_cache: dict[str, dict] = {}
        self._today = date.today()

    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        try:
            game_info = self._find_game(team)
            if not game_info:
                logger.debug("No NCAA.com game found for %s", team)
                return None

            game_id = game_info["game_id"]
            box = self._get_boxscore(game_id)
            if not box:
                return None

            # Determine which team is ours
            is_home = game_info["team_side"] == "home"
            player_stats = self._find_player(player_name, is_home, box)

            result = self._build_context(game_info)
            if player_stats:
                result.update(player_stats)
            else:
                status = result.get("game_status", "")
                if status == "Live":
                    result["stats_summary"] = "In lineup — stats updating"
                elif status == "Final":
                    result["stats_summary"] = "DNP — game final"

            return result

        except Exception:
            logger.info("NCAAComScraper failed for %s @ %s", player_name, team)
            return None

    # ---- scoreboard / game lookup ----

    def _get_scoreboard(self, date_str: str) -> list:
        """Fetch NCAA scoreboard for a date (YYYY/MM/DD). Caches per date."""
        if date_str not in self._scoreboard_cache:
            url = f"{self.SCOREBOARD_URL}/{date_str}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            self._scoreboard_cache[date_str] = resp.json().get("games", [])
        return self._scoreboard_cache[date_str]

    def _find_game(self, team: str) -> Optional[dict]:
        """Search today then yesterday for a game matching the team."""
        team_lower = team.lower()
        today = self._today
        yesterday = today - timedelta(days=1)

        for check_date in (today, yesterday):
            date_str = check_date.strftime("%Y/%m/%d")
            games = self._get_scoreboard(date_str)
            is_yesterday = (check_date == yesterday)

            # Two passes: exact then substring (with school-qualifier guard)
            for exact in (True, False):
                for g in games:
                    game = g.get("game", {})
                    for side in ("home", "away"):
                        side_info = game.get(side, {})
                        names_dict = side_info.get("names", {})
                        # Use short/full for matching; seo uses hyphens that
                        # break qualifier logic so normalize it
                        seo = names_dict.get("seo", "").replace("-", " ")
                        names = [
                            names_dict.get("short", ""),
                            names_dict.get("full", ""),
                            seo,
                        ]
                        if self._team_matches(team_lower, names, exact):
                            # For yesterday, only include live or final
                            state = game.get("gameState", "")
                            if is_yesterday and state not in ("final", "live"):
                                continue

                            opp_side = "away" if side == "home" else "home"
                            opp_name = game.get(opp_side, {}).get("names", {}).get("short", "?")
                            home_name = game.get("home", {}).get("names", {}).get("short", "?")
                            away_name = game.get("away", {}).get("names", {}).get("short", "?")

                            return {
                                "game_id": game.get("gameID"),
                                "team_id": side_info.get("teamId"),
                                "team_side": side,
                                "opponent": opp_name,
                                "home_name": home_name,
                                "away_name": away_name,
                                "home_score": game.get("home", {}).get("score", "0"),
                                "away_score": game.get("away", {}).get("score", "0"),
                                "state": state,
                                "is_yesterday": is_yesterday,
                                "start_time": game.get("startTime", ""),
                                "start_date": game.get("startDate", ""),
                            }
        return None

    @staticmethod
    def _team_matches(team_lower: str, names: list[str], exact: bool) -> bool:
        """Match team name, guarding against school-name false positives.

        Uses the shared ``_school_name_matches`` helper.
        """
        return _school_name_matches(team_lower, names, exact)

    # ---- box score ----

    def _get_boxscore(self, game_id) -> Optional[dict]:
        url = f"{self.BOXSCORE_URL}/{game_id}/boxscore"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        return resp.json()

    def _find_player(self, player_name: str, is_home: bool, box: dict) -> Optional[dict]:
        """Find a player in the box score by last name."""
        player_last = player_name.split()[-1].lower()

        # Match our team in the box score by home/away
        # The teams array has isHome, and teamBoxscore order matches
        teams = box.get("teams", [])
        target_team_id = None
        for t in teams:
            if t.get("isHome") == is_home:
                target_team_id = t.get("teamId")
                break

        for tb in box.get("teamBoxscore", []):
            if target_team_id and str(tb.get("teamId")) != str(target_team_id):
                continue

            for ps in tb.get("playerStats", []):
                last_name = ps.get("lastName", "").lower()
                if player_last in last_name or last_name in player_last:
                    pitcher = ps.get("pitcherStats")
                    batter = ps.get("batterStats")
                    if pitcher:
                        result = self._parse_pitching(pitcher)
                        result["_player_found"] = True
                        return result
                    if batter:
                        result = self._parse_batting(batter)
                        result["_player_found"] = True
                        return result
        return None

    # ---- context / parsing ----

    def _build_context(self, game_info: dict) -> dict:
        """Build game context dict from NCAA.com game data."""
        result = empty_stats()
        state = game_info["state"]
        home = game_info["home_name"]
        away = game_info["away_name"]
        hs = game_info["home_score"]
        a_s = game_info["away_score"]

        if state == "final":
            result["game_context"] = f"{away} {a_s}, {home} {hs} | Final"
            result["game_status"] = "Final"
        elif state == "live":
            result["game_context"] = f"{away} {a_s}, {home} {hs} | Live"
            result["game_status"] = "Live"
        elif state == "pre":
            result["game_context"] = f"{away} vs {home}"
            result["game_status"] = "Scheduled"
            start = game_info.get("start_time", "")
            if start:
                result["game_time"] = start
                result["stats_summary"] = f"Game at {start}"
            else:
                result["stats_summary"] = "Game today"
        else:
            result["game_context"] = f"{away} vs {home} | {state}"
            result["game_status"] = state

        # Populate game_date
        start_date = game_info.get("start_date", "")
        if start_date and len(start_date) >= 10:
            result["game_date"] = start_date[:10]
        if game_info.get("is_yesterday"):
            result["is_yesterday"] = True

        return result

    @staticmethod
    def _parse_batting(bs: dict) -> dict:
        h = int(bs.get("hits", 0) or 0)
        ab = int(bs.get("atBats", 0) or 0)
        rbi = int(bs.get("runsBattedIn", 0) or 0)
        r = int(bs.get("runsScored", 0) or 0)
        bb = int(bs.get("walks", 0) or 0)
        k = int(bs.get("strikeouts", 0) or 0)
        # NCAA API doesn't split HR/SB in batterStats — check hittingSeason if needed
        hr = 0
        sb = 0

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(f"{hr} HR" if hr > 1 else "HR")
        if rbi:
            parts.append(f"{rbi} RBI")
        if r:
            parts.append(f"{r} R")
        if sb:
            parts.append(f"{sb} SB")
        if bb:
            parts.append(f"{bb} BB")

        return {
            "stats_summary": ", ".join(parts),
            "hits": h,
            "at_bats": ab,
            "home_runs": hr,
            "rbi": rbi,
            "runs": r,
            "stolen_bases": sb,
            "walks": bb,
        }

    @staticmethod
    def _parse_pitching(ps: dict) -> dict:
        ip_str = ps.get("inningsPitched", "0") or "0"
        ip = float(ip_str) if str(ip_str).replace(".", "").isdigit() else 0.0
        h = int(ps.get("hitsAllowed", 0) or 0)
        er = int(ps.get("earnedRunsAllowed", 0) or 0)
        k = int(ps.get("strikeouts", 0) or 0)
        bb = int(ps.get("walksAllowed", 0) or 0)

        parts = [f"{ip_str} IP"]
        if h:
            parts.append(f"{h} H")
        parts.append(f"{er} ER")
        parts.append(f"{k} K")
        if bb:
            parts.append(f"{bb} BB")

        qs = ip >= 6.0 and er <= 3
        return {
            "stats_summary": ", ".join(parts),
            "is_pitcher_line": True,
            "ip": ip,
            "earned_runs": er,
            "strikeouts": k,
            "walks_allowed": bb,
            "hits_allowed": h,
            "quality_start": qs,
        }


class D1BaseballScraper(BaseSchoolScraper):
    """
    Scraper for D1Baseball.com — covers all D1 programs.
    Has consistent box score structure across schools.
    Good for post-game summaries and live scoring.
    """

    BASE_URL = "https://d1baseball.com"

    # Map school names to their D1Baseball team slug
    # Example: "Florida" -> "florida-gators"
    TEAM_SLUGS: dict[str, str] = {
        # Power 5 + major programs — add more as needed
        "Alabama": "alabama-crimson-tide",
        "Arizona": "arizona-wildcats",
        "Arkansas": "arkansas-razorbacks",
        "Auburn": "auburn-tigers",
        "Clemson": "clemson-tigers",
        "Coastal Carolina": "coastal-carolina-chanticleers",
        "Dallas Baptist": "dallas-baptist-patriots",
        "Duke": "duke-blue-devils",
        "FIU": "fiu-panthers",
        "Florida": "florida-gators",
        "Florida State": "florida-state-seminoles",
        "Fordham": "fordham-rams",
        "Georgia Tech": "georgia-tech-yellow-jackets",
        "LSU": "lsu-tigers",
        "Mercer": "mercer-bears",
        "Miami": "miami-hurricanes",
        "Michigan": "michigan-wolverines",
        "Mississippi State": "mississippi-state-bulldogs",
        "North Carolina": "north-carolina-tar-heels",
        "Ohio State": "ohio-state-buckeyes",
        "Oklahoma": "oklahoma-sooners",
        "Oklahoma State": "oklahoma-state-cowboys",
        "Ole Miss": "ole-miss-rebels",
        "Oregon": "oregon-ducks",
        "Oregon State": "oregon-state-beavers",
        "Rutgers": "rutgers-scarlet-knights",
        "Sacramento State": "sacramento-state-hornets",
        "SE Louisiana": "southeastern-louisiana-lions",
        "South Carolina": "south-carolina-gamecocks",
        "Southern Miss": "southern-miss-golden-eagles",
        "Stanford": "stanford-cardinal",
        "Stony Brook": "stony-brook-seawolves",
        "TCU": "tcu-horned-frogs",
        "Tennessee": "tennessee-volunteers",
        "Texas": "texas-longhorns",
        "Texas A&M": "texas-am-aggies",
        "Texas Tech": "texas-tech-red-raiders",
        "UCF": "ucf-knights",
        "USF": "usf-bulls",
        "Vanderbilt": "vanderbilt-commodores",
        "Virginia": "virginia-cavaliers",
        "Virginia Tech": "virginia-tech-hokies",
        "Wake Forest": "wake-forest-demon-deacons",
    }

    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        slug = self.TEAM_SLUGS.get(team)
        if not slug:
            logger.debug("No D1Baseball slug configured for %s", team)
            return None

        try:
            # D1Baseball box scores are at /teams/{slug}/schedule
            # We look for today's game and parse the box score
            schedule_url = f"{self.BASE_URL}/teams/{slug}/schedule"
            resp = requests.get(schedule_url, timeout=15)
            resp.raise_for_status()

            return self._find_player_box_score(player_name, team, resp.text)

        except Exception:
            logger.info("D1Baseball fetch failed for %s @ %s", player_name, team)
            return None

    def _find_player_box_score(
        self, player_name: str, team: str, html: str
    ) -> Optional[dict]:
        """
        Parse D1Baseball schedule page to find today's (or yesterday's) game,
        then fetch and parse the box score for the player.
        """
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Find links to box scores (typically contain "/boxscore/" in href)
            # D1Baseball uses format: /games/{game-slug}/boxscore
            today = date.today()
            yesterday = today - timedelta(days=1)
            date_needles = [
                today.strftime("%m/%d"),
                today.strftime("%b %d"),
                yesterday.strftime("%m/%d"),
                yesterday.strftime("%b %d"),
            ]

            game_links = soup.select('a[href*="/boxscore"]')

            for link in game_links:
                row = link.find_parent("tr") or link.find_parent("div")
                if row:
                    row_text = row.get_text()
                    if any(needle in row_text for needle in date_needles):
                        box_url = self.BASE_URL + link.get("href", "")
                        return self._parse_box_score(player_name, box_url)

            logger.debug("No game found today on D1Baseball for %s", team)
            return None

        except Exception:
            logger.exception("Error parsing D1Baseball schedule for %s", team)
            return None

    def _parse_box_score(self, player_name: str, box_url: str) -> Optional[dict]:
        """Fetch and parse a D1Baseball box score page for a specific player."""
        try:
            resp = requests.get(box_url, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # D1Baseball box scores have tables with player stats
            # Look for the player's name in the batting or pitching tables
            tables = soup.select("table")

            for table in tables:
                rows = table.select("tr")
                for row in rows:
                    cells = row.select("td")
                    if not cells:
                        continue

                    # First cell typically contains player name
                    name_cell = cells[0].get_text(strip=True)

                    # Fuzzy match on player name (last name at minimum)
                    player_last = player_name.split()[-1].lower()
                    if player_last in name_cell.lower():
                        return self._extract_stats_from_row(cells, table)

            logger.debug("Player %s not found in box score at %s", player_name, box_url)
            return None

        except Exception:
            logger.exception("Error parsing D1Baseball box score at %s", box_url)
            return None

    def _extract_stats_from_row(self, cells: list, table) -> Optional[dict]:
        """Extract stats from a box score row. Determines if batting or pitching."""
        try:
            # Detect if this is a batting or pitching table by headers
            headers = [th.get_text(strip=True).upper() for th in table.select("th")]

            if "AB" in headers or "H" in headers:
                # Batting line
                return self._parse_batting_row(cells, headers)
            elif "IP" in headers or "ER" in headers:
                # Pitching line
                return self._parse_pitching_row(cells, headers)

            return None

        except Exception:
            logger.exception("Error extracting stats from row")
            return None

    def _parse_batting_row(self, cells: list, headers: list) -> dict:
        """Parse a batting stats row."""
        stats = {}
        cell_texts = [c.get_text(strip=True) for c in cells]

        # Build a mapping from header to value
        # Skip first column (player name), align with headers
        for i, header in enumerate(headers):
            if i < len(cell_texts):
                stats[header] = cell_texts[i]

        h = int(stats.get("H", 0) or 0)
        ab = int(stats.get("AB", 0) or 0)
        hr = int(stats.get("HR", 0) or 0)
        rbi = int(stats.get("RBI", 0) or 0)
        r = int(stats.get("R", 0) or 0)
        sb = int(stats.get("SB", 0) or 0)

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(f"{hr} HR" if hr > 1 else "HR")
        if rbi:
            parts.append(f"{rbi} RBI")
        if r:
            parts.append(f"{r} R")
        if sb:
            parts.append(f"{sb} SB")

        return {
            "stats_summary": ", ".join(parts),
            "game_status": "Final",
            "game_context": "",  # Would need to parse from page header
            "hits": h,
            "at_bats": ab,
            "home_runs": hr,
            "rbi": rbi,
            "runs": r,
            "stolen_bases": sb,
        }

    def _parse_pitching_row(self, cells: list, headers: list) -> dict:
        """Parse a pitching stats row."""
        stats = {}
        cell_texts = [c.get_text(strip=True) for c in cells]

        for i, header in enumerate(headers):
            if i < len(cell_texts):
                stats[header] = cell_texts[i]

        ip_str = stats.get("IP", "0") or "0"
        ip = float(ip_str) if ip_str.replace(".", "").isdigit() else 0.0
        er = int(stats.get("ER", 0) or 0)
        k = int(stats.get("K", stats.get("SO", 0)) or 0)
        bb = int(stats.get("BB", 0) or 0)
        ha = int(stats.get("H", 0) or 0)

        parts = [f"{ip_str} IP"]
        if ha:
            parts.append(f"{ha} H")
        parts.append(f"{er} ER")
        parts.append(f"{k} K")
        if bb:
            parts.append(f"{bb} BB")

        qs = ip >= 6.0 and er <= 3

        return {
            "stats_summary": ", ".join(parts),
            "game_status": "Final",
            "game_context": "",
            "is_pitcher_line": True,
            "ip": ip,
            "earned_runs": er,
            "strikeouts": k,
            "walks_allowed": bb,
            "hits_allowed": ha,
            "quality_start": qs,
        }


class ESPNScraper(BaseSchoolScraper):
    """
    Scraper using ESPN's public college baseball API.
    Works for all D1 programs with no per-school configuration.
    Returns live and final box score data as JSON (no HTML scraping).
    """

    SCOREBOARD_URL = (
        "https://site.api.espn.com/apis/site/v2/sports/baseball/"
        "college-baseball/scoreboard"
    )
    SUMMARY_URL = (
        "https://site.api.espn.com/apis/site/v2/sports/baseball/"
        "college-baseball/summary"
    )

    def __init__(self):
        self._scoreboard_cache: dict[str, dict] = {}  # date_str -> scoreboard JSON
        self._today = date.today()

    def fetch_stats(self, player_name: str, team: str) -> Optional[dict]:
        try:
            game_info = self._find_game(team)
            if not game_info:
                logger.debug("No ESPN game found today for %s", team)
                return None

            summary = self._get_summary(game_info["id"])
            if not summary:
                return None

            result = self._extract_game_context(game_info)
            player_stats = self._find_player(player_name, summary)
            if player_stats:
                player_stats["_player_found"] = True
                result.update(player_stats)
            else:
                # Game found but player not in boxscore — provide context-specific message
                status = result.get("game_status", "")
                if status == "Live":
                    result["stats_summary"] = "In lineup — stats updating"
                elif status == "Final":
                    result["stats_summary"] = "DNP — game final"
                # If Scheduled, _extract_game_context already set "Game at X:XX PM ET"

            return result
        except Exception:
            logger.info("ESPN fetch failed for %s @ %s", player_name, team)
            return None

    # ----- next game lookup -----

    def find_next_game(self, team: str) -> Optional[dict]:
        """Search ESPN scoreboards for the team's next game (up to 3 days ahead)."""
        team_lower = team.lower()
        for days_ahead in range(1, 4):
            future_date = self._today + timedelta(days=days_ahead)
            date_str = future_date.strftime("%Y%m%d")
            try:
                scoreboard = self._get_scoreboard(date_str)
                for event in scoreboard.get("events", []):
                    for comp in event.get("competitions", []):
                        for competitor in comp.get("competitors", []):
                            team_info = competitor.get("team", {})
                            names = [
                                team_info.get("displayName", ""),
                                team_info.get("shortDisplayName", ""),
                                team_info.get("location", ""),
                                team_info.get("name", ""),
                            ]
                            if self._team_matches(team_lower, names, exact=True) or \
                               self._team_matches(team_lower, names, exact=False):
                                # Determine opponent and home/away
                                home_comp = away_comp = None
                                for c in comp.get("competitors", []):
                                    if c.get("homeAway") == "home":
                                        home_comp = c
                                    else:
                                        away_comp = c

                                home_name = home_comp.get("team", {}).get("shortDisplayName", "") if home_comp else ""
                                away_name = away_comp.get("team", {}).get("shortDisplayName", "") if away_comp else ""

                                # Check if this team is the home team
                                is_home = False
                                if home_comp:
                                    home_names = [
                                        home_comp.get("team", {}).get("displayName", ""),
                                        home_comp.get("team", {}).get("location", ""),
                                    ]
                                    is_home = self._team_matches(team_lower, home_names, exact=True) or \
                                             self._team_matches(team_lower, home_names, exact=False)

                                if is_home:
                                    opponent = away_name
                                    home_away = "vs"
                                else:
                                    opponent = home_name
                                    home_away = "@"

                                game_time = self._format_espn_time(comp.get("date", event.get("date", "")))
                                display = f"{home_away} {opponent} — {future_date.strftime('%a %m/%d')}"
                                if game_time:
                                    display += f" {game_time}"

                                return {
                                    "date": future_date.strftime("%a %m/%d"),
                                    "date_full": future_date.isoformat(),
                                    "opponent": opponent,
                                    "home_away": home_away,
                                    "time": game_time,
                                    "display": display,
                                }
            except Exception:
                logger.debug("Error fetching ESPN scoreboard for %s", date_str)
                continue
        return None

    # ----- internal helpers -----

    @staticmethod
    def _team_matches(team_lower: str, names: list[str], exact: bool) -> bool:
        """Match team name, guarding against school-name false positives.

        Uses the shared ``_school_name_matches`` helper.
        """
        return _school_name_matches(team_lower, names, exact)

    def _get_scoreboard(self, date_str: str = None) -> dict:
        """Fetch ESPN scoreboard for a specific date (YYYYMMDD). Caches per date."""
        if date_str is None:
            date_str = self._today.strftime("%Y%m%d")
        if date_str not in self._scoreboard_cache:
            url = f"{self.SCOREBOARD_URL}?dates={date_str}&limit=200"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            self._scoreboard_cache[date_str] = resp.json()
        return self._scoreboard_cache[date_str]

    def _find_game(self, team: str) -> Optional[dict]:
        """Find today's game for the given team from the ESPN scoreboard."""
        today_str = self._today.strftime("%Y%m%d")
        yesterday_str = (self._today - timedelta(days=1)).strftime("%Y%m%d")
        team_lower = team.lower()

        # Check today's scoreboard first, then yesterday's for late-night spillover
        for sb_date in (today_str, yesterday_str):
            scoreboard = self._get_scoreboard(sb_date)
            # Two passes: exact match first, then substring fallback.
            for exact in (True, False):
                for event in scoreboard.get("events", []):
                    for comp in event.get("competitions", []):
                        # For yesterday's games, include In Progress and Final
                        if sb_date == yesterday_str:
                            status_desc = comp.get("status", {}).get("type", {}).get("description", "")
                            if "Progress" not in status_desc and "Final" not in status_desc:
                                continue

                        for competitor in comp.get("competitors", []):
                            team_info = competitor.get("team", {})
                            names = [
                                team_info.get("displayName", ""),
                                team_info.get("shortDisplayName", ""),
                                team_info.get("location", ""),
                                team_info.get("name", ""),
                            ]
                            if self._team_matches(team_lower, names, exact):
                                info = self._build_game_info(event, comp)
                                info["is_yesterday"] = (sb_date == yesterday_str)
                                return info
        return None

    def _build_game_info(self, event: dict, comp: dict) -> dict:
        """Extract game info from an ESPN event/competition."""
        status = comp.get("status", {})
        status_type = status.get("type", {})
        home_comp = away_comp = None
        for c in comp.get("competitors", []):
            if c.get("homeAway") == "home":
                home_comp = c
            else:
                away_comp = c

        return {
            "id": event.get("id"),
            "status": status_type.get("description", "Unknown"),
            "period": status.get("period", 0),
            "home_team": (
                home_comp.get("team", {}).get("shortDisplayName", "")
                if home_comp else ""
            ),
            "away_team": (
                away_comp.get("team", {}).get("shortDisplayName", "")
                if away_comp else ""
            ),
            "home_score": home_comp.get("score", "0") if home_comp else "0",
            "away_score": away_comp.get("score", "0") if away_comp else "0",
            "date": comp.get("date", event.get("date", "")),
        }

    def _get_summary(self, game_id: str) -> Optional[dict]:
        resp = requests.get(
            f"{self.SUMMARY_URL}?event={game_id}", timeout=15
        )
        resp.raise_for_status()
        return resp.json()

    def _extract_game_context(self, game_info: dict) -> dict:
        """Build game context dict from ESPN scoreboard data."""
        status = game_info["status"]
        home = game_info["home_team"]
        away = game_info["away_team"]
        hs = game_info["home_score"]
        a_s = game_info["away_score"]
        inning = game_info.get("period", 0)

        result = empty_stats()
        if status == "Final":
            result["game_context"] = f"{away} {a_s}, {home} {hs} | Final"
            result["game_status"] = "Final"
        elif "Progress" in status:
            result["game_context"] = f"{away} {a_s}, {home} {hs} | Inn {inning}"
            result["game_status"] = "Live"
        elif status in ("Scheduled", "Pre-Game"):
            game_time = self._format_espn_time(game_info.get("date", ""))
            result["game_context"] = f"{away} vs {home}"
            result["game_status"] = "Scheduled"
            if game_time:
                result["game_time"] = game_time
                result["stats_summary"] = f"Game at {game_time}"
            else:
                result["stats_summary"] = "Game today"
        else:
            result["game_context"] = f"{away} vs {home} | {status}"
            result["game_status"] = status

        # Populate game_date from ESPN event datetime
        event_date_str = game_info.get("date", "")
        if event_date_str and len(event_date_str) >= 10:
            result["game_date"] = event_date_str[:10]
        if game_info.get("is_yesterday"):
            result["is_yesterday"] = True

        return result

    @staticmethod
    def _format_espn_time(date_str: str) -> str:
        """Convert ESPN ISO date (e.g. '2026-02-13T18:00Z') to ET time string."""
        if not date_str:
            return ""
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            et_offset = timezone(timedelta(hours=-5))
            dt_et = dt.astimezone(et_offset)
            return dt_et.strftime("%-I:%M %p ET")
        except Exception:
            return ""

    def _find_player(self, player_name: str, summary: dict) -> Optional[dict]:
        """Find a player's stats in the ESPN summary boxscore."""
        boxscore = summary.get("boxscore", {})
        player_last = player_name.split()[-1].lower()

        # ESPN puts individual player stats under "players", not "teams"
        for player_group in boxscore.get("players", []):
            for stat_group in player_group.get("statistics", []):
                labels = [lb.upper() for lb in stat_group.get("labels", [])]
                is_pitching = "IP" in labels

                for athlete_entry in stat_group.get("athletes", []):
                    athlete = athlete_entry.get("athlete", {})
                    display_name = athlete.get("displayName", "")

                    if player_last in display_name.lower():
                        stat_values = athlete_entry.get("stats", [])
                        stat_map = {}
                        for i, label in enumerate(labels):
                            if i < len(stat_values):
                                stat_map[label] = stat_values[i]

                        if is_pitching:
                            return self._parse_pitching(stat_map)
                        return self._parse_batting(stat_map)
        return None

    @staticmethod
    def _parse_batting(sm: dict) -> dict:
        h = int(sm.get("H", 0) or 0)
        ab = int(sm.get("AB", 0) or 0)
        hr = int(sm.get("HR", 0) or 0)
        rbi = int(sm.get("RBI", 0) or 0)
        r = int(sm.get("R", 0) or 0)
        sb = int(sm.get("SB", 0) or 0)
        bb = int(sm.get("BB", 0) or 0)

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(f"{hr} HR" if hr > 1 else "HR")
        if rbi:
            parts.append(f"{rbi} RBI")
        if r:
            parts.append(f"{r} R")
        if sb:
            parts.append(f"{sb} SB")
        if bb:
            parts.append(f"{bb} BB")

        return {
            "stats_summary": ", ".join(parts),
            "hits": h,
            "at_bats": ab,
            "home_runs": hr,
            "rbi": rbi,
            "runs": r,
            "stolen_bases": sb,
            "walks": bb,
        }

    @staticmethod
    def _parse_pitching(sm: dict) -> dict:
        ip_str = sm.get("IP", "0") or "0"
        ip = float(ip_str) if ip_str.replace(".", "").isdigit() else 0.0
        h = int(sm.get("H", 0) or 0)
        er = int(sm.get("ER", 0) or 0)
        k = int(sm.get("K", sm.get("SO", 0)) or 0)
        bb = int(sm.get("BB", 0) or 0)

        parts = [f"{ip_str} IP"]
        if h:
            parts.append(f"{h} H")
        parts.append(f"{er} ER")
        parts.append(f"{k} K")
        if bb:
            parts.append(f"{bb} BB")

        qs = ip >= 6.0 and er <= 3
        return {
            "stats_summary": ", ".join(parts),
            "is_pitcher_line": True,
            "ip": ip,
            "earned_runs": er,
            "strikeouts": k,
            "walks_allowed": bb,
            "hits_allowed": h,
            "quality_start": qs,
        }


class NCAAStatsFetcher:
    """
    Fault-tolerant NCAA stats fetcher.
    Tries registered school scrapers in order, falls back gracefully.
    """

    def __init__(self):
        self._espn = ESPNScraper()
        self._ncaa_com = NCAAComScraper()
        self._sidearm = SidearmScraper()
        self._statbroadcast = StatBroadcastScraper()
        self._ncaa_org = NCAAOrgScraper()
        self._d1baseball = D1BaseballScraper()

        # Registry: school name -> list of scrapers to try in order.
        # Add school-specific overrides here.
        self._school_scrapers: dict[str, list[BaseSchoolScraper]] = {
            # Example:
            # "Coastal Carolina": [self._statbroadcast, self._ncaa_org],
        }

        # Default waterfall chain:
        #   1. ESPN — fast JSON API, all D1, good for game status/scores/live
        #   2. NCAA.com — JSON API with individual player box scores
        #   3. D1Baseball, Sidearm, StatBroadcast, NCAA.org — additional fallbacks
        self._default_chain: list[BaseSchoolScraper] = [
            self._espn,
            self._ncaa_com,
            self._d1baseball,
            self._sidearm,
            self._statbroadcast,
            self._ncaa_org,
        ]

    @staticmethod
    def _has_player_stats(result: dict) -> bool:
        """Return True if the result contains actual player stat lines.

        A result from a scraper that found the game but NOT the player's
        individual stats will have at_bats == 0 and ip == 0.  We use this
        to decide whether to accept the result or keep trying the next
        scraper in the waterfall.

        Scheduled games (no stats expected yet) are also accepted.
        A ``_player_found`` flag (set by scrapers that locate the player
        in a box score) is also accepted — covers 0-AB appearances like
        pinch runners or walk-only plate appearances.
        """
        if result.get("game_status") in ("Scheduled", "N/A"):
            return True  # no stats expected — accept as-is
        return (
            result.get("_player_found", False)
            or result.get("at_bats", 0) > 0
            or result.get("ip", 0) > 0
            or result.get("is_pitcher_line", False)
        )

    def fetch(self, player: dict) -> dict:
        """
        Attempt to fetch stats for an NCAA player using a waterfall approach.

        Tries each scraper in order.  If a scraper returns game context but
        no actual player stats (e.g. ESPN found the game but couldn't match
        the player in the boxscore), the context is saved and the next
        scraper is tried.  The first scraper that returns real stats wins.
        If none do, the best game-context result is returned so the UI can
        still show the game score / status.
        """
        name = player.get("player_name", "")
        team = player.get("team", "")

        scrapers = self._school_scrapers.get(team, self._default_chain)

        best_context = None  # game context from first scraper that found a game

        for scraper in scrapers:
            try:
                result = scraper.fetch_stats(name, team)
                if result is None:
                    continue

                # If this result has actual player stats, we're done
                if self._has_player_stats(result):
                    # Merge game context from earlier scraper if this one lacks it
                    if best_context and not result.get("game_context"):
                        result["game_context"] = best_context.get("game_context", "")
                        result["game_status"] = best_context.get("game_status", result.get("game_status", "N/A"))
                        result.setdefault("game_date", best_context.get("game_date"))
                        result.setdefault("is_yesterday", best_context.get("is_yesterday", False))
                    return result

                # No player stats — save game context and keep trying
                if best_context is None:
                    best_context = result
                    logger.info(
                        "%s found game for %s @ %s but no player stats — trying next scraper",
                        scraper.__class__.__name__, name, team,
                    )
            except Exception:
                logger.exception(
                    "Scraper %s crashed for %s @ %s",
                    scraper.__class__.__name__,
                    name,
                    team,
                )
                continue

        # If any scraper found the game but none found player stats, return
        # that context so the UI still shows the game info
        if best_context:
            logger.info("Waterfall exhausted for %s @ %s — returning game context only", name, team)
            return best_context

        # No game today — try to find next game via ESPN
        logger.info("No NCAA game found for %s @ %s — checking next game", name, team)
        result = empty_stats()
        try:
            next_game = self._espn.find_next_game(team)
            if next_game:
                result["next_game"] = next_game
                result["stats_summary"] = f"Next: {next_game['display']}"
            else:
                result["stats_summary"] = "No game scheduled"
        except Exception:
            logger.debug("Next game lookup failed for %s", team)
            result["stats_summary"] = "No game scheduled"
        return result


# =========================================================================
# Unified fetcher
# =========================================================================


class StatsFetcher:
    """
    Unified interface: routes a player to the correct fetcher based on level.
    """

    def __init__(self):
        self.pro = ProStatsFetcher()
        self.ncaa = NCAAStatsFetcher()

    def fetch(self, player: dict) -> dict:
        level = player.get("level", "")
        if level == "Pro":
            return self.pro.fetch(player)
        elif level == "NCAA":
            return self.ncaa.fetch(player)
        else:
            logger.warning("Unknown level '%s' for %s", level, player.get("player_name"))
            return empty_stats()
