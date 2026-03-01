"""
SV Dugout Pulse — Stats Engine

Two ecosystems:
  1. Pro (MLB/MiLB) — via the MLB-StatsAPI library
  2. NCAA — fault-tolerant framework with pluggable school scrapers
"""

from __future__ import annotations

import abc
import base64
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import statsapi
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import ROSTER_URL

logger = logging.getLogger(__name__)


def _make_http_session() -> requests.Session:
    """Create a shared HTTP session with connection pooling and retry logic."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503],
    )
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_http = _make_http_session()


# ===== Shared helpers =====

# Eastern Time zone — used for all "today" calculations so that games at
# 8-11 PM ET aren't incorrectly attributed to the next day when running
# on UTC-based servers (e.g. GitHub Actions at 1:30 AM UTC = 8:30 PM ET).
_ET = ZoneInfo("US/Eastern")

# The day flips at 4 AM ET so late-night West Coast finishes still show
# on the correct calendar day.
_DAY_FLIP_HOUR = 4


def _today_et() -> date:
    """Return today's date in Eastern Time, with a 4 AM ET day boundary."""
    now = datetime.now(_ET)
    if now.hour < _DAY_FLIP_HOUR:
        return (now - timedelta(days=1)).date()
    return now.date()


# School-name qualifiers that indicate a different school when they appear
# as a suffix ("Florida State") or prefix ("North Florida") to a base name.
_SUFFIX_QUALIFIERS = {
    "a&t", "state", "st", "st.", "central", "wilmington", "charlotte",
    "greensboro", "pembroke", "asheville", "a&m", "am", "at", "tech",
    "southern", "northern", "eastern", "western", "international",
    "atlantic", "pacific", "gulf", "upstate", "baptist", "christian",
    "lutheran", "wesleyan", "methodist", "valley", "polytechnic",
    "poly", "marymount", "of", "commonwealth",
    # City qualifiers — prevents "alabama" matching "Alabama Birmingham" (UAB),
    # "illinois" matching "Illinois Chicago" (UIC), etc.
    "birmingham", "chicago", "omaha", "huntsville",
}
_PREFIX_QUALIFIERS = {
    "north", "south", "east", "west", "central", "se", "ne", "sw", "nw",
    "northern", "southern", "eastern", "western",
    "coastal", "fiu",
}


# Common NCAA abbreviation expansions used by the NCAA.com API.
# Applied to both the search term and candidate names before matching.
_ABBREV_MAP = {
    "st.": "state",
    "miss.": "mississippi",
    "mich.": "michigan",
    "ill.": "illinois",
    "ky.": "kentucky",
    "tenn.": "tennessee",
    "colo.": "colorado",
    "caro.": "carolina",
    "ind.": "indiana",
    "fla.": "florida",
    "mo.": "missouri",
    "la.": "louisiana",
    "ark.": "arkansas",
    "ala.": "alabama",
    "ga.": "georgia",
    "so.": "southern",
    "u.": "university",
}


# Team name aliases — map common alternate names to canonical forms.
# Applied to the search term so "FIU" matches "Florida International", etc.
_TEAM_ALIASES = {
    "fiu": "florida international",
    "ole miss": "mississippi",
    "uconn": "connecticut",
    "umass": "massachusetts",
    "unlv": "nevada las vegas",
    "utsa": "texas san antonio",
    "utep": "texas el paso",
    "ucf": "central florida",
    "usf": "south florida",
    "uab": "alabama birmingham",
    "lsu": "louisiana state",
    "tcu": "texas christian",
    "smu": "southern methodist",
    "byu": "brigham young",
    "vcu": "virginia commonwealth",
    "ecu": "east carolina",
    "wku": "western kentucky",
    "eku": "eastern kentucky",
    "fau": "florida atlantic",
    "fgcu": "florida gulf coast",
    "njit": "new jersey institute of technology",
    "unc": "north carolina",
    "usc": "southern california",
    "pitt": "pittsburgh",
    "cal": "california",
    "miami (oh)": "miami ohio",
    "miami (fl)": "miami",
    "se louisiana": "southeastern louisiana",
    "saint josephs": "saint joseph",
}


def _expand_abbreviations(name: str) -> str:
    """Expand common NCAA abbreviations so names can match.

    e.g. "Florida St." → "Florida State", "Southern Miss." → "Southern Mississippi"
    """
    result = name
    for abbrev, full in _ABBREV_MAP.items():
        # Case-insensitive word replacement
        pattern = re.compile(re.escape(abbrev), re.IGNORECASE)
        result = pattern.sub(full, result)
    return result


def _school_name_matches(team_lower: str, names: list[str], exact: bool) -> bool:
    """Match our team name against candidate name strings.

    Expands common NCAA abbreviations (e.g. "St." → "State") before matching
    so that roster names like "Florida State" match API names like "Florida St."
    Also resolves team aliases (e.g. "FIU" → "Florida International").

    *exact* mode: equality only.
    *substring* mode: ``team_lower in name``, but rejects false positives where
    the candidate is actually a different school, e.g.:
        - "florida" in "florida state"   → False (suffix qualifier "state")
        - "florida" in "north florida"   → False (prefix qualifier "north")
        - "florida" in "florida gators"  → True  (mascot, not qualifier)
        - "carolina" in "coastal carolina" → only if searching for "carolina"
    """
    # Resolve aliases — e.g. "fiu" → "florida international"
    team_lower = _TEAM_ALIASES.get(team_lower, team_lower)
    # Also check if any candidate name is an alias
    resolved_names = [_TEAM_ALIASES.get(n.lower(), n) for n in names]

    # Expand abbreviations on the search term once
    team_expanded = _expand_abbreviations(team_lower).lower()

    # Check both original names and alias-resolved names
    all_names = list(dict.fromkeys(names + resolved_names))  # deduplicated, order preserved

    for n in all_names:
        n_lower = n.lower()
        if not n_lower:
            continue

        # Also try with abbreviations expanded
        n_expanded = _expand_abbreviations(n_lower).lower()

        if exact:
            if team_expanded == n_expanded or team_lower == n_lower:
                return True
        else:
            # Check both raw and expanded forms
            if team_lower not in n_lower and team_expanded not in n_expanded:
                continue

            reject = False

            # Use expanded forms for the qualifier guards
            check_team = team_expanded
            check_name = n_expanded

            # Suffix guard: "florida" in "florida state" — check word after match
            if check_name.startswith(check_team) and len(check_name) > len(check_team):
                suffix = check_name[len(check_team):].strip()
                first_word = suffix.split()[0] if suffix else ""
                if first_word in _SUFFIX_QUALIFIERS:
                    reject = True

            # Prefix guard: "florida" in "north florida" — check word before match
            idx = check_name.find(check_team)
            if idx > 0:
                prefix = check_name[:idx].strip()
                last_word = prefix.split()[-1] if prefix else ""
                if last_word in _PREFIX_QUALIFIERS:
                    reject = True

            if not reject:
                return True
    return False


def _normalize_last_name(name: str) -> str:
    """Strip suffixes like Jr, Sr, II, III, IV and lowercase."""
    name = name.strip().lower()
    # Remove trailing suffixes (with or without dots/commas)
    name = re.sub(r"[,\s]+(jr\.?|sr\.?|ii|iii|iv|v)$", "", name)
    return name.strip()


def _names_match(roster_name: str, box_name: str) -> bool:
    """Compare last names using multiple strategies.

    1. Exact match after normalization (suffix stripping)
    2. Substring containment
    3. Hyphenated name handling (either part matches)
    4. startswith for truncated names (NCAA API truncates to ~10-12 chars)
    """
    r = _normalize_last_name(roster_name)
    b = _normalize_last_name(box_name)

    if not r or not b:
        return False

    # Exact match
    if r == b:
        return True

    # Substring containment (current behavior)
    if r in b or b in r:
        return True

    # Hyphenated: "Smith-Jones" matches "Smith" or "Jones"
    for part in r.split("-"):
        part = part.strip()
        if part and (part == b or part in b or b in part):
            return True
    for part in b.split("-"):
        part = part.strip()
        if part and (part == r or part in r or r in part):
            return True

    # Truncation: NCAA API sometimes truncates names
    if len(b) >= 8 and (r.startswith(b) or b.startswith(r)):
        return True

    return False


def _fmt(n: int, label: str) -> str:
    """Format a batting stat: omit count when 1 (baseball convention).

    ``_fmt(1, "HR")`` → ``"HR"``
    ``_fmt(2, "RBI")`` → ``"2 RBI"``
    """
    return label if n == 1 else f"{n} {label}"


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
        self._team_info_cache: dict[int, dict] = {}  # team_id -> {name, sport_id, parent_id, parent_name}
        self._player_team_cache: dict[int, dict] = {}  # player_id -> team info
        self._next_game_cache: dict[str, Optional[dict]] = {}  # team_lower -> next game result
        self._today = _today_et()
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
            mlb_id = player.get("mlb_id")
            player_id = self._resolve_player_id(name, mlb_id)
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
                result["mlb_player_id"] = player_id
                if next_game:
                    result["stats_summary"] = f"Next: {next_game['display']}"
                else:
                    result["stats_summary"] = "No game scheduled"
                return result

            result = self._extract_stats(player, player_id, game)
            result["next_game"] = next_game
            result["mlb_player_id"] = player_id
            return result

        except Exception:
            logger.exception("Error fetching pro stats for %s", name)
            return empty_stats()

    def fetch_yesterday(self, player: dict) -> Optional[dict]:
        """Fetch yesterday's Final stats for a Pro player.

        Queries MLB/MiLB schedule for yesterday, finds the player's team's
        game, and extracts boxscore stats if the game is Final.
        """
        team = player.get("team", "")
        name = player.get("player_name", "")

        if not team or team == "Unsigned":
            return None

        try:
            mlb_id = player.get("mlb_id")
            player_id = self._resolve_player_id(name, mlb_id)
            if player_id is None:
                return None

            yesterday = self._today - timedelta(days=1)
            yesterday_str = yesterday.strftime("%m/%d/%Y")
            team_lower = team.lower()

            game = self._find_game_on_date(player_id, team_lower, yesterday_str)
            if game is None:
                return None

            status = game["schedule"].get("status", "")
            if status != "Final":
                return None

            result = self._extract_stats(player, player_id, game)
            result["is_yesterday"] = True
            result["game_date"] = yesterday.isoformat()
            result["mlb_player_id"] = player_id
            return result

        except Exception:
            logger.debug("Yesterday fetch failed for pro player %s", name)
            return None

    def _find_game_on_date(self, player_id: int, team_lower: str,
                           date_str: str) -> Optional[dict]:
        """Find a game on a specific date for a player's team."""
        # Search MLB schedule first
        try:
            schedule = statsapi.schedule(date=date_str, sportId=1)
            game = self._match_team_in_schedule(schedule, team_lower)
            if game:
                return game
        except Exception:
            pass

        # Search MiLB if player has a known team
        api_team_id = self._player_team_cache.get(player_id)
        if api_team_id:
            team_info = self._resolve_team(api_team_id)
            if team_info["sport_id"] != 1:
                try:
                    schedule = statsapi.schedule(
                        date=date_str,
                        team=api_team_id,
                        sportId=team_info["sport_id"],
                    )
                    game = self._match_team_in_schedule(
                        schedule, team_info["name"].lower(),
                    )
                    if game:
                        return game
                except Exception:
                    pass

        return None

    # ----- internal helpers -----

    # Search MLB, then AAA, AA, High-A, Single-A
    _SPORT_IDS = [1, 11, 12, 13, 14]

    def _resolve_player_id(self, name: str, mlb_id: Optional[int] = None) -> Optional[int]:
        """Resolve a player's MLB API ID.

        Uses the roster-provided mlb_id directly when available (skips the
        name search entirely).  Falls back to _lookup_player() for players
        without an ID in the sheet.
        """
        if mlb_id:
            # Cache by name so downstream code that only has the name still hits cache
            self._player_cache[name] = mlb_id
            # Eagerly resolve current team so schedule lookups work
            if mlb_id not in self._player_team_cache:
                try:
                    data = statsapi.lookup_player(str(mlb_id))
                    if data:
                        ct = data[0].get("currentTeam", {})
                        if isinstance(ct, dict) and ct.get("id"):
                            self._resolve_team(ct["id"])
                            self._player_team_cache[mlb_id] = ct["id"]
                except Exception:
                    logger.debug("Team resolve failed for mlb_id %d", mlb_id)
            return mlb_id
        return self._lookup_player(name)

    def _lookup_player(self, name: str) -> Optional[int]:
        """Search across all pro levels for a player ID, with caching."""
        if name in self._player_cache:
            return self._player_cache[name]

        try:
            for sport_id in self._SPORT_IDS:
                results = statsapi.lookup_player(name, sportId=sport_id)
                if results:
                    player_id = results[0]["id"]
                    self._player_cache[name] = player_id
                    # Cache the team ID from the lookup result
                    ct = results[0].get("currentTeam", {})
                    if isinstance(ct, dict) and ct.get("id"):
                        self._resolve_team(ct["id"])
                        self._player_team_cache[player_id] = ct["id"]
                    return player_id
        except Exception:
            logger.exception("Player lookup failed for %s", name)
        return None

    def _resolve_team(self, team_id: int) -> dict:
        """Fetch and cache team details (name, sport level, parent org)."""
        if team_id in self._team_info_cache:
            return self._team_info_cache[team_id]
        try:
            resp = _http.get(
                f"https://statsapi.mlb.com/api/v1/teams/{team_id}",
                timeout=10,
            )
            resp.raise_for_status()
            t = resp.json()["teams"][0]
            info = {
                "team_id": team_id,
                "name": t.get("name", ""),
                "sport_id": t.get("sport", {}).get("id", 1),
                "parent_id": t.get("parentOrgId"),
                "parent_name": t.get("parentOrgName", ""),
            }
            self._team_info_cache[team_id] = info
            return info
        except Exception:
            logger.debug("Failed to resolve team %d", team_id)
            return {"team_id": team_id, "name": "", "sport_id": 1, "parent_id": None, "parent_name": ""}

    def _get_schedule(self, sport_id: int, team_id: Optional[int] = None) -> list:
        """Fetch today's schedule for a given sport level, with caching."""
        cache_key = (sport_id, team_id or 0)
        if cache_key in self._games_cache:
            return self._games_cache[cache_key]

        try:
            if team_id:
                games = statsapi.schedule(date=self._today_str, team=team_id, sportId=sport_id)
            else:
                games = statsapi.schedule(date=self._today_str, sportId=sport_id)
            self._games_cache[cache_key] = games
            return games
        except Exception:
            logger.debug("Schedule fetch failed for sportId=%d, team=%s", sport_id, team_id)
            return []

    def _find_todays_game(self, player_id: int, team: str = "") -> Optional[dict]:
        """Find a game today for the player's team.

        Search strategy:
        1. MLB schedule by roster team name (handles spring training)
        2. MiLB schedule by player's actual team (handles regular season)
        3. Parent MLB team schedule as fallback
        """
        team_lower = team.lower() if team else ""

        try:
            # --- 1. Search MLB schedule by roster team name ---
            mlb_schedule = self._get_schedule(sport_id=1)
            game = self._match_team_in_schedule(mlb_schedule, team_lower)
            if game:
                return game

            # --- 2. Search player's actual MiLB team schedule ---
            api_team_id = self._player_team_cache.get(player_id)
            if api_team_id:
                team_info = self._resolve_team(api_team_id)
                if team_info["sport_id"] != 1:
                    milb_schedule = self._get_schedule(
                        sport_id=team_info["sport_id"],
                        team_id=api_team_id,
                    )
                    game = self._match_team_in_schedule(
                        milb_schedule, team_info["name"].lower(),
                    )
                    if game:
                        return game

                    # --- 3. Fallback: parent MLB team (spring training) ---
                    if team_info["parent_id"]:
                        parent_info = self._resolve_team(team_info["parent_id"])
                        game = self._match_team_in_schedule(
                            mlb_schedule, parent_info["name"].lower(),
                        )
                        if game:
                            return game

        except Exception:
            logger.exception("Error searching today's games for player %d", player_id)
        return None

    def _match_team_in_schedule(self, schedule: list, team_lower: str) -> Optional[dict]:
        """Find the first game in *schedule* matching *team_lower*."""
        if not team_lower:
            return None
        for game in schedule:
            home_match = team_lower in game.get("home_name", "").lower()
            away_match = team_lower in game.get("away_name", "").lower()
            if home_match or away_match:
                side = "home" if home_match else "away"
                status = game.get("status", "")
                boxscore = {}
                if status not in ("Scheduled",):
                    try:
                        boxscore = statsapi.boxscore_data(game["game_id"])
                    except Exception:
                        logger.debug("Boxscore fetch failed for game %s", game["game_id"])
                return {
                    "game_id": game["game_id"],
                    "boxscore": boxscore,
                    "schedule": game,
                    "side": side,
                }
        return None

    def _find_next_game(self, player_id: int, team: str) -> Optional[dict]:
        """Find the next scheduled game for a player's team.

        Searches MLB schedule by roster team name, then the player's actual
        MiLB team schedule if available.  Results are cached by team name
        so teammates share a single lookup.
        """
        team_lower = team.lower() if team else ""

        # Check cache first — two players on the same team share one lookup
        if team_lower and team_lower in self._next_game_cache:
            return self._next_game_cache[team_lower]

        # Collect team names + sport IDs to search
        search_targets = []
        if team_lower:
            search_targets.append((team_lower, 1))  # MLB schedule

        # Add MiLB team if known
        api_team_id = self._player_team_cache.get(player_id)
        if api_team_id:
            info = self._resolve_team(api_team_id)
            if info["sport_id"] != 1:
                search_targets.append((info["name"].lower(), info["sport_id"]))

        if not search_targets:
            return None

        try:
            for days_ahead in range(1, 15):
                future_date = self._today + timedelta(days=days_ahead)
                future_str = future_date.strftime("%m/%d/%Y")

                for search_name, sport_id in search_targets:
                    try:
                        schedule = statsapi.schedule(date=future_str, sportId=sport_id)
                    except Exception:
                        continue

                    for game in schedule:
                        home = game.get("home_name", "").lower()
                        away = game.get("away_name", "").lower()
                        if search_name in home or search_name in away:
                            if search_name in home:
                                opponent = game.get("away_name", "")
                                home_away = "vs"
                            else:
                                opponent = game.get("home_name", "")
                                home_away = "@"
                            game_time = self._format_game_time(game.get("game_datetime", ""))
                            result = {
                                "date": future_date.strftime("%a %m/%d"),
                                "date_full": future_date.isoformat(),
                                "opponent": opponent,
                                "home_away": home_away,
                                "time": game_time,
                                "display": f"{home_away} {opponent} - {future_date.strftime('%a %m/%d')} {game_time}" if game_time else f"{home_away} {opponent} - {future_date.strftime('%a %m/%d')}",
                            }
                            if team_lower:
                                self._next_game_cache[team_lower] = result
                            return result
            if team_lower:
                self._next_game_cache[team_lower] = None
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
            # Convert to ET with proper DST handling (EST in winter, EDT in summer)
            dt_et = dt.astimezone(ZoneInfo("America/New_York"))
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

        # MLB Gameday box score link
        game_id = game.get("game_id", "")
        if game_id:
            result["box_score_url"] = f"https://www.mlb.com/gameday/{game_id}"

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
        position = player.get("position", "Hitter")
        found_in_box = False

        if position == "Pitcher":
            result["is_pitcher_line"] = True
            pitchers = box.get(f"{game['side']}Pitchers", [])
            for entry in pitchers:
                if isinstance(entry, dict) and entry.get("personId") == player_id:
                    found_in_box = True
                    ip_val = entry.get("ip", "0")
                    if float(ip_val) if ip_val else 0:
                        result.update(self._parse_pitcher_line(entry))
                    else:
                        result["stats_summary"] = "On the mound"
                    break
        else:
            batters = box.get(f"{game['side']}Batters", [])
            for entry in batters:
                if isinstance(entry, dict) and entry.get("personId") == player_id:
                    found_in_box = True
                    ab = int(entry.get("ab", 0))
                    bb = int(entry.get("bb", 0))
                    if ab + bb > 0:
                        result.update(self._parse_batter_line(entry))
                    else:
                        # In lineup but no plate appearance yet
                        order_str = entry.get("battingOrder", "")
                        pos = entry.get("position", "")
                        is_sub = entry.get("substitution", False)
                        if is_sub:
                            result["stats_summary"] = f"Entered game ({pos})" if pos else "Entered game"
                        elif order_str and order_str.isdigit():
                            spot = int(order_str) // 100
                            result["stats_summary"] = f"In lineup — batting {self._ordinal(spot)} ({pos})" if pos else f"In lineup — batting {self._ordinal(spot)}"
                        else:
                            result["stats_summary"] = f"In lineup ({pos})" if pos else "In lineup"
                    break

        # Fallback for players not found in the boxscore at all
        if not found_in_box and result["stats_summary"] == "No game data":
            if result["game_status"] == "Live":
                if position == "Pitcher":
                    result["stats_summary"] = "Game in progress — not yet pitching"
                else:
                    result["stats_summary"] = "Game in progress — not in lineup"
            elif result["game_status"] == "Final":
                result["stats_summary"] = "DNP — game final"

        return result

    @staticmethod
    def _ordinal(n: int) -> str:
        """Return ordinal string: 1→'1st', 2→'2nd', 3→'3rd', 4→'4th', etc."""
        if 11 <= n % 100 <= 13:
            return f"{n}th"
        return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"

    @staticmethod
    def _parse_batter_line(entry: dict) -> dict:
        """Parse a batter's boxscore entry into our stats dict.

        statsapi.boxscore_data() returns flat dicts with short string keys:
            {"ab": "3", "h": "1", "hr": "0", "rbi": "1", "r": "0", "sb": "0", ...}
        """
        h = int(entry.get("h", 0))
        ab = int(entry.get("ab", 0))
        hr = int(entry.get("hr", 0))
        rbi = int(entry.get("rbi", 0))
        r = int(entry.get("r", 0))
        sb = int(entry.get("sb", 0))
        bb = int(entry.get("bb", 0))

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(_fmt(hr, "HR"))
        if rbi:
            parts.append(_fmt(rbi, "RBI"))
        if r:
            parts.append(_fmt(r, "R"))
        if sb:
            parts.append(_fmt(sb, "SB"))
        if bb:
            parts.append(_fmt(bb, "BB"))

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
    def _parse_pitcher_line(entry: dict) -> dict:
        """Parse a pitcher's boxscore entry into our stats dict.

        statsapi.boxscore_data() returns flat dicts with short string keys:
            {"ip": "6.0", "h": "4", "er": "2", "bb": "1", "k": "7", "hr": "0", ...}
        Win/loss/save info is in the "note" field: "(W, 1-0)", "(L, 0-1)", "(S, 2)".
        """
        ip_str = entry.get("ip", "0") or "0"
        ip = float(ip_str) if ip_str else 0.0
        er = int(entry.get("er", 0))
        k = int(entry.get("k", 0))
        bb = int(entry.get("bb", 0))
        ha = int(entry.get("h", 0))
        hr = int(entry.get("hr", 0))

        # Parse W/L/S from note field
        note = entry.get("note", "")
        w = "(W," in note or "(W)" in note
        l = "(L," in note or "(L)" in note
        sv = "(S," in note or "(S)" in note or "(SV," in note

        parts = [f"{ip_str} IP"]
        if ha:
            parts.append(f"{ha} H")
        parts.append(f"{er} ER")
        parts.append(f"{k} K")
        if bb:
            parts.append(f"{bb} BB")
        if hr:
            parts.append(f"{hr} HR")
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
            "saves": 1 if sv else 0,
            "win": w,
            "loss": l,
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
    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
        """
        Return a stats dict for the player, or None if unavailable.
        Must not raise — catch and log internally.

        If *yesterday_only* is True, only return Final results from yesterday.
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

    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
        base_url = self.SIDEARM_URLS.get(team)
        if not base_url:
            logger.debug("No Sidearm URL configured for %s", team)
            return None

        try:
            # Sidearm exposes a JSON schedule/stats feed at predictable paths
            resp = _http.get(f"{base_url}?format=json", timeout=15)
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

    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
        url = self.STATBROADCAST_URLS.get(team)
        if not url:
            logger.debug("No StatBroadcast URL configured for %s", team)
            return None

        try:
            resp = _http.get(url, timeout=15)
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

    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
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
        self._boxscore_cache: dict[str, dict] = {}
        self._today = _today_et()

    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
        try:
            all_games = self._find_all_games(team, yesterday_only=yesterday_only)
            if not all_games:
                logger.debug("No NCAA.com game found for %s (yesterday_only=%s)", team, yesterday_only)
                return None

            # Try each game — return the first where the player has stats
            # (handles doubleheaders where player appears in only one game)
            first_context = None
            for game_info in all_games:
                # Build context FIRST — even if boxscore fails we know a game exists
                result = self._build_context(game_info)
                if first_context is None:
                    first_context = result

                # Pre-game: boxscore won't exist yet — skip the fetch
                if game_info.get("state") == "pre":
                    continue

                game_id = game_info["game_id"]
                try:
                    box = self._get_boxscore(game_id)
                except Exception:
                    logger.debug("Boxscore fetch failed for game %s", game_id)
                    continue
                if not box:
                    continue

                is_home = game_info["team_side"] == "home"
                player_stats = self._find_player(player_name, is_home, box)

                if player_stats:
                    result.update(player_stats)
                    return result

            # Player not found in any game — use first game's context
            if first_context is not None:
                status = first_context.get("game_status", "")
                if status == "Live":
                    first_context["stats_summary"] = "Game in progress — not in lineup"
                elif status == "Final":
                    first_context["stats_summary"] = "DNP — game final"
                return first_context

            return None

        except Exception:
            logger.info("NCAAComScraper failed for %s @ %s", player_name, team)
            return None

    # ---- scoreboard / game lookup ----

    def _get_scoreboard(self, date_str: str) -> list:
        """Fetch NCAA scoreboard for a date (YYYY/MM/DD). Caches per date."""
        if date_str not in self._scoreboard_cache:
            url = f"{self.SCOREBOARD_URL}/{date_str}"
            resp = _http.get(url, timeout=15)
            resp.raise_for_status()
            self._scoreboard_cache[date_str] = resp.json().get("games", [])
        return self._scoreboard_cache[date_str]

    def _find_all_games(self, team: str, yesterday_only: bool = False) -> list[dict]:
        """Find ALL games for the given team (handles doubleheaders).

        Returns a list of game info dicts (may be empty).

        If *yesterday_only* is True, skip today and only return Final games
        from yesterday's scoreboard.
        """
        team_lower = team.lower()
        today = self._today
        yesterday = today - timedelta(days=1)

        if yesterday_only:
            dates_to_check = (yesterday,)
        else:
            dates_to_check = (today, yesterday)

        results = []
        seen_ids = set()

        for check_date in dates_to_check:
            date_str = check_date.strftime("%Y/%m/%d")
            games = self._get_scoreboard(date_str)
            is_yesterday = (check_date == yesterday)

            # Two passes: exact then substring (with school-qualifier guard)
            for exact in (True, False):
                for g in games:
                    game = g.get("game", {})
                    state = game.get("gameState", "")

                    if yesterday_only:
                        # Only accept final games
                        if state != "final":
                            continue
                    elif is_yesterday and state not in ("final", "live"):
                        # Normal mode: for yesterday, only include live or final
                        continue

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
                            game_id = game.get("gameID")
                            if game_id not in seen_ids:
                                seen_ids.add(game_id)
                                opp_side = "away" if side == "home" else "home"
                                opp_name = game.get(opp_side, {}).get("names", {}).get("short", "?")
                                home_name = game.get("home", {}).get("names", {}).get("short", "?")
                                away_name = game.get("away", {}).get("names", {}).get("short", "?")

                                results.append({
                                    "game_id": game_id,
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
                                })
        return results

    def _find_game(self, team: str, yesterday_only: bool = False) -> Optional[dict]:
        """Find a game for the given team (returns first match).

        Prefer _find_all_games when the caller needs doubleheader support.
        """
        games = self._find_all_games(team, yesterday_only=yesterday_only)
        return games[0] if games else None

    @staticmethod
    def _team_matches(team_lower: str, names: list[str], exact: bool) -> bool:
        """Match team name, guarding against school-name false positives.

        Uses the shared ``_school_name_matches`` helper.
        """
        return _school_name_matches(team_lower, names, exact)

    # ---- box score ----

    def _get_boxscore(self, game_id) -> Optional[dict]:
        gid = str(game_id)
        if gid in self._boxscore_cache:
            logger.debug("Boxscore cache hit for game %s", gid)
            return self._boxscore_cache[gid]
        url = f"{self.BOXSCORE_URL}/{game_id}/boxscore"
        resp = _http.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        self._boxscore_cache[gid] = data
        return data

    def _find_player(self, player_name: str, is_home: bool, box: dict) -> Optional[dict]:
        """Find a player in the box score by last name, with fuzzy matching."""
        name_parts = player_name.split()
        player_last = name_parts[-1]
        player_first = name_parts[0].lower() if len(name_parts) > 1 else ""

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

            # Collect all last-name matches for disambiguation
            candidates = []
            for ps in tb.get("playerStats", []):
                last_name = ps.get("lastName", "")
                if _names_match(player_last, last_name):
                    candidates.append(ps)

            # First-name disambiguation when multiple players share a last name
            if len(candidates) > 1 and player_first:
                narrowed = [
                    ps for ps in candidates
                    if ps.get("firstName", "").lower().startswith(player_first[:3])
                ]
                if narrowed:
                    candidates = narrowed

            for ps in candidates:
                pitcher = ps.get("pitcherStats")
                batter = ps.get("batterStats")

                # Two-way players may have both — prefer the one with actual stats
                if pitcher and batter:
                    p_ip = float(pitcher.get("inningsPitched", 0) or 0)
                    b_ab = int(batter.get("atBats", 0) or 0)
                    b_bb = int(batter.get("walks", 0) or 0)
                    if p_ip > 0:
                        result = self._parse_pitching(pitcher)
                        result["_player_found"] = True
                        return result
                    if b_ab > 0 or b_bb > 0:
                        result = self._parse_batting(batter)
                        result["_player_found"] = True
                        return result
                    # Neither has stats yet — mark as found (in lineup)
                    return {"_player_found": True}

                if pitcher:
                    result = self._parse_pitching(pitcher)
                    result["_player_found"] = True
                    return result
                if batter:
                    ab = int(batter.get("atBats", 0) or 0)
                    bb = int(batter.get("walks", 0) or 0)
                    # Skip players listed in box score with 0 AB and 0 BB
                    # — they appear on the roster but didn't actually play
                    if ab == 0 and bb == 0:
                        return None
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

        # Populate game_date (normalize to YYYY-MM-DD)
        start_date = game_info.get("start_date", "")
        if start_date:
            # Handle MM/DD/YYYY format from NCAA API
            if "/" in start_date:
                try:
                    result["game_date"] = datetime.strptime(
                        start_date.split()[0], "%m/%d/%Y"
                    ).date().isoformat()
                except ValueError:
                    pass
            elif len(start_date) >= 10:
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
        # NCAA per-game batterStats doesn't include HR or SB fields.
        # These are available in the season-level hittingSeason endpoint but
        # not broken out per game — this is a known NCAA API limitation.
        hr = 0
        sb = 0

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(_fmt(hr, "HR"))
        if rbi:
            parts.append(_fmt(rbi, "RBI"))
        if r:
            parts.append(_fmt(r, "R"))
        if sb:
            parts.append(_fmt(sb, "SB"))
        if bb:
            parts.append(_fmt(bb, "BB"))
        if k:
            parts.append(_fmt(k, "K"))

        return {
            "stats_summary": ", ".join(parts),
            "hits": h,
            "at_bats": ab,
            "home_runs": hr,
            "rbi": rbi,
            "runs": r,
            "stolen_bases": sb,
            "walks": bb,
            "strikeouts": k,
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

    Uses the D1Baseball dynamic scores API to discover games, then follows
    the box score link (typically to the school's Sidearm page) for player stats.
    No per-school configuration needed — works for every D1 game.
    """

    SCORES_URL = (
        "https://d1baseball.com/wp-content/plugins/integritive/dynamic-scores.php"
    )

    def __init__(self):
        self._scores_cache: dict[str, str] = {}  # date_str -> HTML content
        self._today = _today_et()

    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
        try:
            tiles = self._find_all_game_tiles(team, yesterday_only=yesterday_only)
            if not tiles:
                logger.debug("No D1Baseball game found for %s (yesterday_only=%s)", team, yesterday_only)
                return None

            first_context = None
            for tile_info in tiles:
                context = self._build_tile_context(tile_info)
                if first_context is None:
                    first_context = context

                box_url = tile_info.get("box_score_url")
                if not box_url:
                    continue

                # Skip sidearm stat parsing for Scheduled games — but check
                # for pre-game starting lineups (posted ~30-60 min before
                # first pitch).  The sidearm page may show stale data from
                # the previous game, so we validate the matchup first.
                tile_status = tile_info.get("status")

                if tile_status == "Cancelled":
                    # Skip cancelled games entirely — treat as no game found.
                    continue

                if tile_status == "Scheduled":
                    if self._check_pregame_lineup(
                        player_name, box_url,
                        tile_info["home_name"], tile_info["road_name"],
                    ):
                        context["stats_summary"] = "In starting lineup"
                        return context
                    continue

                # For Live/Final box scores: if this tile is from yesterday but
                # we already saved a today-game context, skip yesterday's stats
                # and fall through to return today's context.
                if tile_info.get("is_yesterday") and first_context and not first_context.get("is_yesterday"):
                    logger.debug(
                        "D1Baseball: skipping yesterday stats for %s — today's game context already found",
                        player_name,
                    )
                    break

                if "statbroadcast.com" in box_url or "statb.us" in box_url:
                    player_stats = self._parse_statbroadcast_box_score(player_name, box_url)
                else:
                    player_stats = self._parse_sidearm_box_score(player_name, box_url)
                if player_stats:
                    context.update(player_stats)
                    # StatBroadcast's inning is more current than the D1Baseball
                    # tile (which can lag by a half-inning or more).  Override
                    # game_context with the inning we read directly from the box
                    # score HTML so the two sources stay in sync.
                    sb_inning = context.pop("_sb_inning_label", None)
                    if sb_inning and context.get("game_status") == "Live":
                        away = tile_info["road_name"]
                        home = tile_info["home_name"]
                        a_s = tile_info["road_score"]
                        hs = tile_info["home_score"]
                        context["game_context"] = f"{away} {a_s}, {home} {hs} | {sb_inning}"
                    return context

            # Player not found in any game — return game context
            if first_context is not None:
                status = first_context.get("game_status", "")
                if status == "Live":
                    # Don't claim "in lineup" — we can't verify from D1Baseball
                    # alone when the box score is on StatBroadcast (not Sidearm).
                    # ESPN will provide the accurate lineup status downstream.
                    first_context["stats_summary"] = "Game in progress"
                elif status == "Final":
                    first_context["stats_summary"] = "DNP — game final"
                return first_context

            return None
        except Exception:
            logger.info("D1Baseball fetch failed for %s @ %s", player_name, team)
            return None

    # ---- scores API / game discovery ----

    def _get_scores(self, date_str: str) -> str:
        """Fetch D1Baseball scores HTML for a date (YYYYMMDD). Caches per date."""
        if date_str not in self._scores_cache:
            resp = _http.get(
                self.SCORES_URL,
                params={"date": date_str},
                headers={
                    "Referer": "https://d1baseball.com/scores/",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            self._scores_cache[date_str] = data.get("content", {}).get("d1-scores", "")
        return self._scores_cache[date_str]

    def _find_all_game_tiles(self, team: str, yesterday_only: bool = False) -> list[dict]:
        """Find all game tiles for a team from the D1Baseball scores page."""
        today_str = self._today.strftime("%Y%m%d")
        yesterday_str = (self._today - timedelta(days=1)).strftime("%Y%m%d")
        team_lower = team.lower()

        if yesterday_only:
            dates_to_check = [(yesterday_str, True)]
        else:
            dates_to_check = [(today_str, False), (yesterday_str, True)]

        results = []
        seen_keys = set()

        for date_str, is_yesterday in dates_to_check:
            html = self._get_scores(date_str)
            if not html:
                continue

            # Convert YYYYMMDD to ISO date for game_date
            iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            soup = BeautifulSoup(html, "html.parser")
            tiles = soup.select(".d1-score-tile")

            for tile in tiles:
                home_name = tile.get("data-home-name", "")
                road_name = tile.get("data-road-name", "")

                # Match team: exact first, then substring with qualifier guards
                names = [home_name, road_name]
                matched = (
                    _school_name_matches(team_lower, names, exact=True)
                    or _school_name_matches(team_lower, names, exact=False)
                )

                if not matched:
                    continue

                tile_key = tile.get("data-key", "")
                if tile_key in seen_keys:
                    continue
                seen_keys.add(tile_key)

                # Status filtering
                is_final = "status-final" in tile.get("class", [])
                is_live = "in-progress" in tile.get("class", [])

                if yesterday_only and not is_final:
                    continue
                if is_yesterday and not (is_final or is_live):
                    continue

                # Extract box score link
                box_link = tile.select_one(".box-score-links a")
                box_url = box_link.get("href", "") if box_link else ""

                # Extract scores
                teams = tile.select(".team")
                road_score = home_score = "0"
                if len(teams) >= 2:
                    road_score = self._extract_score(teams[0])
                    home_score = self._extract_score(teams[1])

                status = "Final" if is_final else "Live" if is_live else "Scheduled"

                # Extract game time / inning from .status-wrapper h5
                # Scheduled: "1:00 PM" | Live: "Top 3", "Bottom 7" | Final: "FINAL"
                # Cancelled: "CANCELLED" | Postponed: "POSTPONED"
                game_time = ""
                inning_label = ""
                status_h5 = tile.select_one(".status-wrapper h5")
                if status_h5:
                    h5_text = status_h5.get_text(strip=True).upper()
                    if h5_text in ("CANCELLED", "POSTPONED"):
                        status = "Cancelled"
                    elif status == "Scheduled" and re.search(r"\d+:\d+\s*(AM|PM)", h5_text, re.IGNORECASE):
                        game_time = status_h5.get_text(strip=True)
                    elif status == "Live":
                        # D1Baseball shows "Top 3", "Bottom 7", "Middle 5", etc.
                        inning_label = status_h5.get_text(strip=True)

                results.append({
                    "home_name": home_name,
                    "road_name": road_name,
                    "home_score": home_score,
                    "road_score": road_score,
                    "status": status,
                    "is_yesterday": is_yesterday,
                    "game_date": iso_date,
                    "box_score_url": box_url,
                    "tile_key": tile_key,
                    "game_time": game_time,
                    "inning_label": inning_label,
                })

        return results

    @staticmethod
    def _extract_score(team_div) -> str:
        """Extract the runs score from a D1Baseball tile team div."""
        runs = team_div.select_one(".score-runs")
        if runs:
            text = runs.get_text(strip=True)
            # Strip the "R" label if present
            return re.sub(r"[^\d]", "", text) or "0"
        return "0"

    def _build_tile_context(self, tile_info: dict) -> dict:
        """Build game context dict from a D1Baseball tile."""
        result = empty_stats()
        home = tile_info["home_name"]
        away = tile_info["road_name"]
        hs = tile_info["home_score"]
        a_s = tile_info["road_score"]
        status = tile_info["status"]

        if status == "Final":
            result["game_context"] = f"{away} {a_s}, {home} {hs} | Final"
            result["game_status"] = "Final"
        elif status == "Live":
            inning = tile_info.get("inning_label", "")
            live_label = inning if inning else "Live"
            result["game_context"] = f"{away} {a_s}, {home} {hs} | {live_label}"
            result["game_status"] = "Live"
        elif status == "Cancelled":
            result["game_context"] = f"{away} vs {home} | Cancelled"
            result["game_status"] = "Cancelled"
            result["stats_summary"] = "Game cancelled"
        else:
            game_time = tile_info.get("game_time", "")
            result["game_context"] = f"{away} vs {home}"
            result["game_status"] = "Scheduled"
            if game_time:
                result["game_time"] = game_time
                result["stats_summary"] = f"Game at {game_time}"
            else:
                result["stats_summary"] = "Game today"

        if tile_info.get("is_yesterday"):
            result["is_yesterday"] = True

        result["game_date"] = tile_info.get("game_date")

        # Only set box_score_url for Final/Live games.  For Scheduled games
        # the sidearm link points to a "summary" page that shows the latest
        # completed game (wrong game).  Leaving it blank lets the ESPN
        # fallback supply a correct preview URL.
        box_url = tile_info.get("box_score_url", "")
        if box_url and status != "Scheduled":
            result["box_score_url"] = box_url

        return result

    # ---- Pre-game lineup detection ----

    def _check_pregame_lineup(
        self, player_name: str, box_url: str, home_name: str, road_name: str,
    ) -> bool:
        """Check if a Scheduled game's sidearm page shows a pre-game lineup.

        The sidearm "summary" URL can show either stale data (previous game)
        or today's pre-game lineup.  We validate by checking that both team
        names from the D1Baseball tile appear on the page.  If the matchup
        matches, we look for the player's last name in table-row <th>
        elements (where Sidearm puts player names).
        """
        try:
            resp = _http.get(box_url, timeout=15)
            resp.raise_for_status()
            page_text = resp.text.lower()

            # Matchup validation — both teams must appear on the page
            if home_name.lower() not in page_text or road_name.lower() not in page_text:
                return False

            # Page shows today's game — check for player in lineup
            soup = BeautifulSoup(resp.text, "html.parser")
            player_last = player_name.split()[-1].lower()
            for th in soup.select("table tr th"):
                if player_last in th.get_text(strip=True).lower():
                    return True

            return False
        except Exception:
            logger.debug("Pre-game lineup check failed for %s", box_url)
            return False

    # ---- StatBroadcast box score parsing ----

    def _parse_statbroadcast_box_score(self, player_name: str, box_url: str) -> Optional[dict]:
        """Fetch and parse a StatBroadcast live stats page for a specific player.

        StatBroadcast is a JS app, but its webservice endpoint returns encoded HTML.
        Protocol: GET /interface/webservice/stats?data=base64(params) → ROT13+base64 → HTML.
        Player names are in LastName,FirstName format.
        """
        try:
            import codecs as _codecs
            m = re.search(r"[?&]id=(\d+)", box_url)
            if not m:
                # Handle statb.us short URLs by following redirect
                r0 = _http.get(box_url, timeout=10, allow_redirects=True)
                m = re.search(r"[?&]id=(\d+)", r0.url)
                if not m:
                    return None
                box_url = r0.url
            event_id = m.group(1)

            # Step 1: get event metadata (xmlfile path contains groupid)
            r1 = _http.get(
                f"https://stats.statbroadcast.com/interface/webservice/event/{event_id}",
                headers={"Referer": box_url},
                timeout=15,
            )
            r1.raise_for_status()
            event_xml = base64.b64decode(
                _codecs.encode(r1.text.strip(), "rot_13") + "=="
            ).decode("utf-8", errors="replace")
            xmlfile_m = re.search(r"<xmlfile><!\[CDATA\[([^\]]+)\]\]></xmlfile>", event_xml)
            if not xmlfile_m:
                return None
            xml_file = xmlfile_m.group(1)

            # Step 2: fetch box score HTML for home then visitor
            for team_side in ("H", "V"):
                data_str = (
                    f"event={event_id}&xml={xml_file}"
                    f"&xsl=baseball/sb.bsgame.views.box.xsl"
                    f'&params={{"team":"{team_side}"}}'
                    f"&sport=bsgame&filetime=-1&type=statmonitr&start=true"
                )
                encoded = base64.b64encode(data_str.encode()).decode()
                r2 = _http.get(
                    "https://stats.statbroadcast.com/interface/webservice/stats",
                    params={"data": encoded},
                    headers={"Referer": box_url, "X-Requested-With": "XMLHttpRequest"},
                    timeout=15,
                )
                r2.raise_for_status()
                html = base64.b64decode(
                    _codecs.encode(r2.text.strip(), "rot_13") + "=="
                ).decode("utf-8", errors="replace")
                result = self._parse_statbroadcast_html(player_name, html)
                if result:
                    inning_label = self._extract_sb_inning_from_html(html)
                    if inning_label:
                        result["_sb_inning_label"] = inning_label
                    return result

        except Exception:
            logger.debug("StatBroadcast parse failed for %s @ %s", player_name, box_url)
        return None

    @staticmethod
    def _parse_statbroadcast_html(player_name: str, html: str) -> Optional[dict]:
        """Parse a player's stats from StatBroadcast box score HTML.

        Full batting table cols:
          POS(0), #(1), PLAYER(2), AB(3), R(4), H(5), RBI(6), 2B(7), 3B(8), HR(9), BB(10), K(11)
        Full pitching table cols (no POS column):
          #(0), Player(1), Dec(2), IP(3), H(4), R(5), ER(6), BB(7), K(8)

        The page also contains a "TODAY" summary table (first header = "TODAY") which
        has a different column layout and should be skipped.
        """
        soup = BeautifulSoup(html, "html.parser")
        player_last = player_name.split()[-1].lower()
        player_first = player_name.split()[0].lower()[:3] if len(player_name.split()) > 1 else ""

        for table in soup.select("table"):
            header_row = table.select_one("tr")
            if not header_row:
                continue
            headers = [th.get_text(strip=True).upper() for th in header_row.select("th, td")]
            # Skip the "TODAY" summary table — it has a different column layout
            if headers and headers[0] == "TODAY":
                continue
            is_batting = "AB" in headers and "IP" not in headers
            is_pitching = "IP" in headers
            if not is_batting and not is_pitching:
                continue

            # Batting: name at cells[2] (POS, #, PLAYER, ...)
            # Pitching: name at cells[1] (no POS column: #, Player, ...)
            name_idx = 2 if is_batting else 1

            for row in table.select("tr")[1:]:
                cells = [c.get_text(strip=True) for c in row.select("td")]
                if len(cells) <= name_idx:
                    continue
                name_cell = cells[name_idx]  # "LastName,FirstName"
                if player_last not in name_cell.lower():
                    continue
                if "," in name_cell and player_first:
                    first_in_cell = name_cell.split(",", 1)[1].strip().lower()
                    if not first_in_cell.startswith(player_first):
                        continue

                if is_batting:
                    try:
                        ab  = int(cells[3])  if len(cells) > 3  and cells[3].isdigit()  else 0
                        r   = int(cells[4])  if len(cells) > 4  and cells[4].isdigit()  else 0
                        h   = int(cells[5])  if len(cells) > 5  and cells[5].isdigit()  else 0
                        rbi = int(cells[6])  if len(cells) > 6  and cells[6].isdigit()  else 0
                        dbl = int(cells[7])  if len(cells) > 7  and cells[7].isdigit()  else 0
                        hr  = int(cells[9])  if len(cells) > 9  and cells[9].isdigit()  else 0
                        bb  = int(cells[10]) if len(cells) > 10 and cells[10].isdigit() else 0
                        k   = int(cells[11]) if len(cells) > 11 and cells[11].isdigit() else 0
                    except (ValueError, IndexError):
                        continue
                    # Skip rows with no plate appearances — pitchers often appear
                    # in the batting table with 0 AB/BB before they bat (or never bat).
                    # Continuing lets the loop reach the pitching table instead.
                    if ab == 0 and bb == 0:
                        continue
                    parts = [f"{h}-{ab}"]
                    if hr:  parts.append(_fmt(hr,  "HR"))
                    if rbi: parts.append(_fmt(rbi, "RBI"))
                    if r:   parts.append(_fmt(r,   "R"))
                    if bb:  parts.append(_fmt(bb,  "BB"))
                    if dbl: parts.append(_fmt(dbl, "2B"))
                    return {"at_bats": ab, "hits": h, "runs": r, "rbi": rbi,
                            "walks": bb, "strikeouts": k, "home_runs": hr,
                            "stats_summary": ", ".join(parts), "_player_found": True}

                if is_pitching:
                    try:
                        ip_str = cells[3] if len(cells) > 3 else "0"
                        parts_ip = ip_str.split(".")
                        ip = int(parts_ip[0]) + (int(parts_ip[1]) / 3 if len(parts_ip) > 1 else 0)
                        h  = int(cells[4]) if len(cells) > 4 and cells[4].isdigit() else 0
                        er = int(cells[6]) if len(cells) > 6 and cells[6].isdigit() else 0
                        bb = int(cells[7]) if len(cells) > 7 and cells[7].isdigit() else 0
                        k  = int(cells[8]) if len(cells) > 8 and cells[8].isdigit() else 0
                    except (ValueError, IndexError):
                        continue
                    parts = [f"{ip_str} IP"]
                    if h:  parts.append(_fmt(h,  "H"))
                    if er: parts.append(_fmt(er, "ER"))
                    if k:  parts.append(_fmt(k,  "K"))
                    if bb: parts.append(_fmt(bb, "BB"))
                    return {"ip": ip, "hits_allowed": h, "earned_runs": er,
                            "walks": bb, "strikeouts": k,
                            "stats_summary": ", ".join(parts),
                            "_player_found": True, "is_pitcher_line": True}
        return None

    @staticmethod
    def _extract_sb_inning_from_html(html: str) -> Optional[str]:
        """Scan StatBroadcast box score HTML for a current inning label.

        The XSL-rendered HTML often contains text like 'Top 6', 'Bottom 4',
        'Middle 3', etc. in headers, status elements, or the linescore.
        Returns a normalised label (e.g. 'Top 6') or None if not found.
        """
        m = re.search(r"\b(Top|Bottom|Bot|Mid(?:dle)?|End)\s+(\d+)", html, re.IGNORECASE)
        if not m:
            return None
        half_map = {
            "top": "Top", "bottom": "Bottom", "bot": "Bottom",
            "mid": "Middle", "middle": "Middle", "end": "End",
        }
        half = half_map.get(m.group(1).lower(), m.group(1).capitalize())
        return f"{half} {m.group(2)}"

    # ---- Sidearm box score parsing ----

    def _parse_sidearm_box_score(self, player_name: str, box_url: str) -> Optional[dict]:
        """Fetch and parse a Sidearm box score page for a specific player.

        Sidearm pages are JavaScript-rendered so the HTML itself never contains
        the stats tables.  We instead use the static JSON API
        (static.sidearmstats.com) which returns full box score data.  The
        old HTML table parser is kept as a last-ditch fallback for any
        rare schools that serve pre-rendered HTML.
        """
        try:
            resp = _http.get(box_url, timeout=15)
            resp.raise_for_status()
            html = resp.text

            # Primary path: Sidearm static JSON API (works for all JS-rendered sites)
            # Pass the final URL (after redirects) so legacy boxscore.aspx links
            # can still have their sport extracted from the redirected URL path.
            result = self._parse_sidearm_stats_json(player_name, html, box_url, final_url=resp.url)
            if result:
                return result

            # Fallback: legacy HTML table parsing (rarely succeeds on modern Sidearm)
            result = self._find_player_in_sidearm(player_name, html)

            # Supplement HR count from scoring summary if the batting table
            # didn't have an HR column (many Sidearm layouts omit it)
            if result and not result.get("is_pitcher_line") and result.get("home_runs", 0) == 0:
                hr = self._count_hrs_from_summary(player_name, html)
                if hr > 0:
                    result["home_runs"] = hr
                    # Rebuild stats_summary with HR included
                    parts = [f"{result.get('hits', 0)}-{result.get('at_bats', 0)}"]
                    parts.append(_fmt(hr, "HR"))
                    if result.get("rbi", 0):
                        parts.append(_fmt(result["rbi"], "RBI"))
                    if result.get("runs", 0):
                        parts.append(_fmt(result["runs"], "R"))
                    if result.get("stolen_bases", 0):
                        parts.append(_fmt(result["stolen_bases"], "SB"))
                    if result.get("walks", 0):
                        parts.append(_fmt(result["walks"], "BB"))
                    result["stats_summary"] = ", ".join(parts)

            return result
        except Exception:
            logger.debug("Failed to fetch Sidearm box score at %s", box_url)
            return None

    def _parse_sidearm_stats_json(
        self, player_name: str, html: str, box_url: str, final_url: str = ""
    ) -> Optional[dict]:
        """Fetch player stats from the Sidearm static JSON API.

        Sidearm's Angular app loads stats from:
          http://static.sidearmstats.com/schools/{folder}/{sport}/game.json?detail=full

        The ``folder`` (e.g. ``"pacific"``) is embedded in the page HTML as
        ``window.livestats_foldername``.  The sport is extracted from the
        box_score_url path (e.g. ``/sidearmstats/baseball/summary``).
        """
        try:
            import re as _re

            # Extract folder name from the JS variable injected into every Sidearm page
            m = _re.search(
                r'window\.livestats_foldername\s*=\s*["\']([^"\']+)["\']', html
            )
            if not m:
                logger.debug("livestats_foldername not found in Sidearm HTML for %s", box_url)
                return None
            folder = m.group(1)

            # Extract sport from the URL path. Sidearm uses several URL formats:
            #   Modern Angular: /sidearmstats/baseball/summary
            #   New style:      /sports/baseball/stats/...
            #   Legacy ASP.NET: boxscore.aspx?id=XXXX  (no sport in URL — may
            #                   redirect to new-style URL, captured in final_url)
            # Fall back to "baseball" since this dashboard only monitors baseball.
            sport = None
            for candidate in (box_url, final_url):
                m2 = (
                    _re.search(r"/sidearmstats/([^/?#]+)/", candidate)
                    or _re.search(r"/sports/([^/?#]+)/stats/", candidate)
                )
                if m2:
                    sport = m2.group(1)
                    break
            if not sport:
                sport = "baseball"

            json_url = (
                f"http://static.sidearmstats.com/schools/{folder}/{sport}/game.json"
                "?detail=full"
            )
            jresp = _http.get(json_url, timeout=15)
            jresp.raise_for_status()
            data = jresp.json()

            stats = data.get("Stats", {})
            player_last = player_name.split()[-1].lower()

            for team_key in ("HomeTeam", "VisitingTeam"):
                team_stats = stats.get(team_key, {})
                pg = team_stats.get("PlayerGroups", {})

                # Batting
                batting = pg.get("Batting", {})
                for v in batting.get("Values", []):
                    if player_last in v.get("Name", "").lower():
                        return self._parse_sidearm_batting_json(v)

                # Pitching
                pitching = pg.get("Pitching", {})
                for v in pitching.get("Values", []):
                    if player_last in v.get("Name", "").lower():
                        return self._parse_sidearm_pitching_json(v)

            logger.debug("Player %s not found in Sidearm stats JSON for %s", player_name, box_url)
            return None
        except Exception:
            logger.debug("Sidearm stats JSON fetch failed for %s @ %s", player_name, box_url)
            return None

    @staticmethod
    def _parse_sidearm_batting_json(v: dict) -> Optional[dict]:
        """Parse batting stats from a Sidearm game.json PlayerGroups Batting Value."""
        try:
            ab  = int(v.get("AtBats", 0) or 0)
            h   = int(v.get("Hits", 0) or 0)
            r   = int(v.get("Runs", 0) or 0)
            rbi = int(v.get("RunsBattedIn", 0) or 0)
            hr  = int(v.get("HomeRuns", 0) or 0)
            bb  = int(v.get("Walks", 0) or 0)
            k   = int(v.get("Strikeouts", 0) or 0)
            sb  = int(v.get("StolenBases", 0) or 0)

            if ab == 0 and bb == 0:
                return None  # didn't actually play

            parts = [f"{h}-{ab}"]
            if hr:
                parts.append(_fmt(hr, "HR"))
            if rbi:
                parts.append(_fmt(rbi, "RBI"))
            if r:
                parts.append(_fmt(r, "R"))
            if sb:
                parts.append(_fmt(sb, "SB"))
            if bb:
                parts.append(_fmt(bb, "BB"))
            if k:
                parts.append(_fmt(k, "K"))

            return {
                "stats_summary": ", ".join(parts),
                "hits": h,
                "at_bats": ab,
                "home_runs": hr,
                "rbi": rbi,
                "runs": r,
                "stolen_bases": sb,
                "walks": bb,
                "_player_found": True,
            }
        except Exception:
            return None

    @staticmethod
    def _parse_sidearm_pitching_json(v: dict) -> Optional[dict]:
        """Parse pitching stats from a Sidearm game.json PlayerGroups Pitching Value."""
        try:
            ip_str = str(v.get("InningsPitched", "0") or "0")
            ip = float(ip_str) if ip_str.replace(".", "").isdigit() else 0.0
            h  = int(v.get("HitsAllowed", 0) or 0)
            er = int(v.get("EarnedRuns", 0) or 0)
            k  = int(v.get("Strikeouts", 0) or 0)
            bb = int(v.get("WalksAllowed", 0) or 0)

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
                "_player_found": True,
            }
        except Exception:
            return None

    @staticmethod
    def _count_hrs_from_summary(player_name: str, html: str) -> int:
        """Count home runs from the Sidearm scoring summary table.

        Sidearm scoring summaries contain entries like:
        "Bailey,Myles homered to left center (396 ft), RBI"

        Only searches visible text in <td> and <div> elements to avoid
        double-counting from embedded JavaScript data.
        """
        player_last = player_name.split()[-1].lower()
        soup = BeautifulSoup(html, "html.parser")

        # Remove script/style tags to avoid matching embedded JS data
        for tag in soup.select("script, style"):
            tag.decompose()

        text = soup.get_text()
        pattern = rf"{re.escape(player_last)},\s*\w+\s+homered"
        return len(re.findall(pattern, text, re.IGNORECASE))

    def _find_player_in_sidearm(self, player_name: str, html: str) -> Optional[dict]:
        """Find a player's stats in a Sidearm-format box score page.

        Sidearm puts player names in <th> elements within each row,
        with stat values in <td> elements.
        """
        try:
            soup = BeautifulSoup(html, "html.parser")
            player_last = player_name.split()[-1].lower()

            tables = soup.select("table")
            for table in tables:
                # Get column headers from the first row's <th> elements
                header_row = table.select_one("tr")
                if not header_row:
                    continue
                col_headers = [th.get_text(strip=True).upper() for th in header_row.select("th")]

                # Skip tables that aren't batting or pitching stat tables
                if "AB" not in col_headers and "IP" not in col_headers:
                    continue

                rows = table.select("tr")
                for row in rows:
                    # Player name is in a <th> within the row
                    row_th = row.select("th")
                    if not row_th:
                        continue
                    name_text = row_th[0].get_text(strip=True)

                    if player_last not in name_text.lower():
                        continue

                    cells = row.select("td")
                    if not cells:
                        continue

                    # Build stat map: col_headers[1:] align with td cells
                    # (col_headers[0] is "Player", rest are stat columns)
                    stat_headers = col_headers[1:]  # skip "Player"
                    # First td is typically Pos, rest are stats
                    cell_texts = [c.get_text(strip=True) for c in cells]

                    # Check if this is a batting or pitching table
                    if "AB" in col_headers:
                        return self._parse_sidearm_batting(stat_headers, cell_texts)
                    elif "IP" in col_headers:
                        return self._parse_sidearm_pitching(stat_headers, cell_texts)

            return None
        except Exception:
            logger.debug("Error parsing Sidearm box score for %s", player_name)
            return None

    @staticmethod
    def _parse_sidearm_batting(headers: list, values: list) -> Optional[dict]:
        """Parse batting stats from Sidearm header/value alignment."""
        stats = {}
        for i, header in enumerate(headers):
            if i < len(values):
                stats[header] = values[i]

        ab = int(stats.get("AB", 0) or 0)
        h = int(stats.get("H", 0) or 0)
        hr = int(stats.get("HR", 0) or 0)
        rbi = int(stats.get("RBI", 0) or 0)
        r = int(stats.get("R", 0) or 0)
        sb = int(stats.get("SB", 0) or 0)
        bb = int(stats.get("BB", 0) or 0)

        if ab == 0 and bb == 0:
            return None  # didn't actually play

        parts = [f"{h}-{ab}"]
        if hr:
            parts.append(_fmt(hr, "HR"))
        if rbi:
            parts.append(_fmt(rbi, "RBI"))
        if r:
            parts.append(_fmt(r, "R"))
        if sb:
            parts.append(_fmt(sb, "SB"))
        if bb:
            parts.append(_fmt(bb, "BB"))

        return {
            "stats_summary": ", ".join(parts),
            "hits": h,
            "at_bats": ab,
            "home_runs": hr,
            "rbi": rbi,
            "runs": r,
            "stolen_bases": sb,
            "walks": bb,
            "_player_found": True,
        }

    @staticmethod
    def _parse_sidearm_pitching(headers: list, values: list) -> Optional[dict]:
        """Parse pitching stats from Sidearm header/value alignment."""
        stats = {}
        for i, header in enumerate(headers):
            if i < len(values):
                stats[header] = values[i]

        ip_str = stats.get("IP", "0") or "0"
        ip = float(ip_str) if str(ip_str).replace(".", "").isdigit() else 0.0
        h = int(stats.get("H", 0) or 0)
        er = int(stats.get("ER", 0) or 0)
        k = int(stats.get("SO", 0) or stats.get("K", 0) or 0)
        bb = int(stats.get("BB", 0) or 0)

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
            "_player_found": True,
        }

    def _parse_box_score(self, player_name: str, box_url: str) -> Optional[dict]:
        """Fetch and parse a D1Baseball box score page for a specific player."""
        try:
            resp = _http.get(box_url, timeout=15)
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
        self._summary_cache: dict[str, dict] = {}  # game_id -> summary JSON
        self._today = _today_et()

    def fetch_stats(self, player_name: str, team: str, yesterday_only: bool = False) -> Optional[dict]:
        try:
            all_games = self._find_all_games(team, yesterday_only=yesterday_only)
            if not all_games:
                logger.info("No ESPN game found for %s (yesterday_only=%s)", team, yesterday_only)
                return None

            # Try each game — return the first where the player has stats
            # (handles doubleheaders where player appears in only one game)
            first_context = None
            for game_info in all_games:
                # Build context FIRST — even if summary fails we know a game exists
                result = self._extract_game_context(game_info)
                if first_context is None:
                    first_context = result

                try:
                    summary = self._get_summary(game_info["id"])
                except Exception:
                    logger.debug("ESPN summary fetch failed for game %s", game_info["id"])
                    continue
                if not summary:
                    continue

                player_stats = self._find_player(player_name, summary)
                if player_stats:
                    player_stats["_player_found"] = True
                    result.update(player_stats)
                    return result

            # Player not found in any game — use first game's context
            if first_context is not None:
                status = first_context.get("game_status", "")
                if status == "Live":
                    first_context["stats_summary"] = "Game in progress — not in lineup"
                elif status == "Final":
                    first_context["stats_summary"] = "DNP — game final"
                return first_context

            return None
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
            resp = _http.get(url, timeout=15)
            resp.raise_for_status()
            self._scoreboard_cache[date_str] = resp.json()
        return self._scoreboard_cache[date_str]

    def _find_all_games(self, team: str, yesterday_only: bool = False) -> list[dict]:
        """Find ALL games for the given team from the ESPN scoreboard.

        Returns a list of game info dicts (may be empty). Handles doubleheaders
        by collecting every matching game rather than returning the first one.

        If *yesterday_only* is True, skip today entirely and only return
        Final games from yesterday's scoreboard.
        """
        today_str = self._today.strftime("%Y%m%d")
        yesterday_str = (self._today - timedelta(days=1)).strftime("%Y%m%d")
        team_lower = team.lower()

        if yesterday_only:
            dates_to_check = (yesterday_str,)
        else:
            # Check today's scoreboard first, then yesterday's for late-night spillover
            dates_to_check = (today_str, yesterday_str)

        results = []
        seen_ids = set()

        for sb_date in dates_to_check:
            scoreboard = self._get_scoreboard(sb_date)
            is_yesterday = (sb_date == yesterday_str)
            # Two passes: exact match first, then substring fallback.
            for exact in (True, False):
                for event in scoreboard.get("events", []):
                    for comp in event.get("competitions", []):
                        status_desc = comp.get("status", {}).get("type", {}).get("description", "")

                        if yesterday_only:
                            # Only accept Final games
                            if "Final" not in status_desc:
                                continue
                        elif is_yesterday:
                            # Normal mode: for yesterday's games, include In Progress and Final
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
                                info["is_yesterday"] = is_yesterday
                                game_id = info.get("id", "")
                                if game_id not in seen_ids:
                                    seen_ids.add(game_id)
                                    results.append(info)
        return results

    def _find_game(self, team: str, yesterday_only: bool = False) -> Optional[dict]:
        """Find a game for the given team (returns first match).

        Prefer _find_all_games when the caller needs doubleheader support.
        """
        games = self._find_all_games(team, yesterday_only=yesterday_only)
        return games[0] if games else None

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
        if game_id in self._summary_cache:
            logger.debug("ESPN summary cache hit for game %s", game_id)
            return self._summary_cache[game_id]
        resp = _http.get(
            f"{self.SUMMARY_URL}?event={game_id}", timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        self._summary_cache[game_id] = data
        return data

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

        game_id = game_info.get("id", "")
        if game_id:
            result["box_score_url"] = f"https://www.espn.com/college-baseball/game/_/gameId/{game_id}"

        return result

    @staticmethod
    def _format_espn_time(date_str: str) -> str:
        """Convert ESPN ISO date (e.g. '2026-02-13T18:00Z') to ET time string."""
        if not date_str:
            return ""
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            dt_et = dt.astimezone(ZoneInfo("America/New_York"))
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
            parts.append(_fmt(hr, "HR"))
        if rbi:
            parts.append(_fmt(rbi, "RBI"))
        if r:
            parts.append(_fmt(r, "R"))
        if sb:
            parts.append(_fmt(sb, "SB"))
        if bb:
            parts.append(_fmt(bb, "BB"))

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
        #   1. D1Baseball — best school matching, links to live stats
        #   2. ESPN — fast JSON API, all D1, good for game status/scores/live
        #   3. NCAA.com — JSON API with individual player box scores
        #   4. Sidearm, StatBroadcast, NCAA.org — additional fallbacks
        self._default_chain: list[BaseSchoolScraper] = [
            self._d1baseball,
            self._espn,
            self._ncaa_com,
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

    def _waterfall_fetch(self, player: dict, yesterday_only: bool = False) -> Optional[dict]:
        """Internal waterfall: try each scraper in order.

        Returns the best result found, or None if no game was found at all.
        """
        name = player.get("player_name", "")
        team = player.get("team", "")

        scrapers = self._school_scrapers.get(team, self._default_chain)
        best_context = None

        for scraper in scrapers:
            try:
                result = scraper.fetch_stats(name, team, yesterday_only=yesterday_only)
                if result is None:
                    continue

                if self._has_player_stats(result):
                    # For Scheduled games missing game_time, save context and
                    # keep trying so ESPN/NCAA.com can supply the start time
                    if (result.get("game_status") == "Scheduled"
                            and not result.get("game_time")
                            and best_context is None):
                        best_context = result
                        logger.info(
                            "%s found scheduled game for %s @ %s but no game_time — trying next for time",
                            scraper.__class__.__name__, name, team,
                        )
                        continue

                    if best_context:
                        if not result.get("game_context"):
                            result["game_context"] = best_context.get("game_context", "")
                            result["game_status"] = best_context.get("game_status", result.get("game_status", "N/A"))
                        result.setdefault("game_date", best_context.get("game_date"))
                        result.setdefault("is_yesterday", best_context.get("is_yesterday", False))
                        # Prefer D1Baseball's URL (StatBroadcast / school box score)
                        # over ESPN/NCAA.com URLs
                        if best_context.get("box_score_url"):
                            result["box_score_url"] = best_context["box_score_url"]
                        # Merge game_time from best_context if this result lacks it
                        if not result.get("game_time") and best_context.get("game_time"):
                            result["game_time"] = best_context["game_time"]
                            if "Game at" in best_context.get("stats_summary", ""):
                                result["stats_summary"] = best_context["stats_summary"]
                    return result

                if best_context is None:
                    best_context = result
                    logger.info(
                        "%s found game for %s @ %s but no player stats — trying next scraper",
                        scraper.__class__.__name__, name, team,
                    )
                elif not best_context.get("game_time") and result.get("game_time"):
                    # Upgrade: this scraper has game_time that best_context lacks.
                    # Carry over any fields best_context had that the new result is missing.
                    result.setdefault("game_context", best_context.get("game_context", ""))
                    result.setdefault("game_date", best_context.get("game_date"))
                    result.setdefault("is_yesterday", best_context.get("is_yesterday", False))
                    # Preserve D1Baseball's URL (StatBroadcast) over ESPN's
                    if best_context.get("box_score_url"):
                        result["box_score_url"] = best_context["box_score_url"]
                    best_context = result
                elif best_context is not None and not self._has_player_stats(result):
                    # Allow a later scraper to supply a more specific lineup status
                    # when D1Baseball returned a generic "Game in progress" placeholder.
                    r_sum = result.get("stats_summary", "")
                    if (r_sum and r_sum != "No game data"
                            and best_context.get("stats_summary") == "Game in progress"):
                        best_context["stats_summary"] = r_sum
                    logger.info(
                        "%s upgraded game_time for %s @ %s",
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

        return best_context  # may be None

    def _find_todays_pregame(self, name: str, team: str) -> Optional[dict]:
        """Check for a game scheduled/live for today.

        Called when the waterfall already returned a yesterday result (or
        nothing) so we don't accidentally swallow yesterday's completed stats.
        Tries NCAA.com first (pre + live states), then falls back to
        D1Baseball scheduled/live tiles if NCAA.com is unavailable.
        Uses the same two-pass (exact then substring) strategy as other scrapers.
        """
        # --- NCAA.com pass ---
        try:
            today_str = _today_et().strftime("%Y/%m/%d")
            games = self._ncaa_com._get_scoreboard(today_str)
            team_lower = team.lower()
            for exact in (True, False):
                for g in games:
                    game = g.get("game", {})
                    game_state = game.get("gameState", "")
                    if game_state not in ("pre", "live"):
                        continue
                    for side in ("home", "away"):
                        side_info = game.get(side, {})
                        names_dict = side_info.get("names", {})
                        seo = names_dict.get("seo", "").replace("-", " ")
                        cand_names = [
                            names_dict.get("short", ""),
                            names_dict.get("full", ""),
                            seo,
                        ]
                        if not _school_name_matches(team_lower, cand_names, exact=exact):
                            continue
                        opp_side = "away" if side == "home" else "home"
                        opp_name = game.get(opp_side, {}).get("names", {}).get("short", "?")
                        home_name = game.get("home", {}).get("names", {}).get("short", "?")
                        away_name = game.get("away", {}).get("names", {}).get("short", "?")
                        game_info = {
                            "game_id": game.get("gameID"),
                            "team_side": side,
                            "opponent": opp_name,
                            "home_name": home_name,
                            "away_name": away_name,
                            "home_score": "0",
                            "away_score": "0",
                            "state": game_state,
                            "is_yesterday": False,
                            "start_time": game.get("startTime", ""),
                            "start_date": game.get("startDate", ""),
                        }
                        today_game = self._ncaa_com._build_context(game_info)
                        # Attach ESPN box score URL if available
                        try:
                            for eg in self._espn._find_all_games(team):
                                gid = eg.get("id", "")
                                if gid:
                                    today_game["box_score_url"] = (
                                        f"https://www.espn.com/college-baseball/game/_/gameId/{gid}"
                                    )
                                    break
                        except Exception:
                            pass
                        logger.info(
                            "Pre-game found for %s @ %s: %s vs %s",
                            name, team, away_name, home_name,
                        )
                        return today_game
        except Exception:
            logger.debug("NCAA.com _find_todays_pregame failed for %s @ %s — trying D1Baseball fallback", name, team)

        # --- D1Baseball fallback (used when NCAA.com is unavailable) ---
        try:
            tiles = self._d1baseball._find_all_game_tiles(team, yesterday_only=False)
            for tile_info in tiles:
                if tile_info.get("is_yesterday"):
                    continue
                if tile_info.get("status") == "Cancelled":
                    continue
                today_game = self._d1baseball._build_tile_context(tile_info)
                # Attach ESPN box score URL if available
                try:
                    for eg in self._espn._find_all_games(team):
                        gid = eg.get("id", "")
                        if gid:
                            today_game["box_score_url"] = (
                                f"https://www.espn.com/college-baseball/game/_/gameId/{gid}"
                            )
                            break
                except Exception:
                    pass
                logger.info(
                    "D1Baseball fallback pre-game found for %s @ %s",
                    name, team,
                )
                return today_game
        except Exception:
            logger.debug("D1Baseball _find_todays_pregame fallback failed for %s @ %s", name, team)

        return None

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

        result = self._waterfall_fetch(player)
        if result:
            # If no box_score_url yet (e.g. scheduled game from D1Baseball),
            # try ESPN for a game preview URL
            if not result.get("box_score_url"):
                try:
                    espn_games = self._espn._find_all_games(team)
                    for g in espn_games:
                        gid = g.get("id", "")
                        if gid:
                            result["box_score_url"] = f"https://www.espn.com/college-baseball/game/_/gameId/{gid}"
                            break
                except Exception:
                    pass

            # For yesterday-only results, check if there's a pre-game TODAY
            # before falling back to next-game lookup.  The waterfall stops
            # early when D1Baseball finds yesterday's stats, so NCAA.com
            # (which covers all D1 scheduled games) is never reached.
            if result.get("is_yesterday"):
                today_game = self._find_todays_pregame(name, team)
                if today_game:
                    return today_game
                if not result.get("next_game"):
                    try:
                        next_game = self._espn.find_next_game(team)
                        if next_game:
                            result["next_game"] = next_game
                    except Exception:
                        pass

            return result

        # No game today via waterfall — check NCAA.com for a pre-game first,
        # then fall back to next-game lookup.
        logger.info("No NCAA game found for %s @ %s — checking today + next game", name, team)
        today_game = self._find_todays_pregame(name, team)
        if today_game:
            return today_game
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

    def fetch_yesterday(self, player: dict) -> Optional[dict]:
        """Fetch only yesterday's Final stats for an NCAA player.

        Returns the result dict or None if no yesterday game was found.
        """
        return self._waterfall_fetch(player, yesterday_only=True)


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

    def fetch_yesterday(self, player: dict) -> Optional[dict]:
        """Fetch only yesterday's Final stats for a player.

        Returns the result dict or None if no yesterday game was found.
        """
        level = player.get("level", "")
        if level == "NCAA":
            return self.ncaa.fetch_yesterday(player)
        elif level == "Pro":
            return self.pro.fetch_yesterday(player)
        return None
