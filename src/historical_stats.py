"""
SV Dugout Pulse — Historical Stats Aggregator

Fetches and aggregates player statistics for Season (and Pro 7D) windows.
- Pro (MLB/MiLB): game logs via statsapi
- NCAA Season: scraped from D1Baseball team stats pages
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional

import requests
import statsapi
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import (
    NCAA_GAME_LOG_PATH,
    WINDOW_7D_PATH,
    WINDOW_SEASON_PATH,
    WINDOW_MIN_IP,
    WINDOW_MIN_PA,
)
from .window_grader import grade_hitter_window, grade_pitcher_window

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


# =============================================================================
# Shared utilities
# =============================================================================

def ip_to_outs(ip_val) -> int:
    """Convert innings pitched (e.g. '6.1') to total outs (e.g. 19).

    Handles both baseball notation ('5.1' = 5⅓ IP) and accidental
    float strings ('5.333333' = 5⅓ IP) by detecting fractional parts
    that aren't valid baseball thirds (0, 1, 2).
    """
    ip_str = str(ip_val)
    try:
        if "." in ip_str:
            parts = ip_str.split(".")
            whole = int(parts[0])
            frac_str = parts[1]
            # Valid baseball notation: fractional part is 0, 1, or 2
            if frac_str in ("0", "1", "2"):
                return whole * 3 + int(frac_str)
            # Float value (e.g. "5.333333"): convert via rounding
            frac = float(ip_val) - whole
            if frac < 0.16:
                outs = 0
            elif frac < 0.5:
                outs = 1
            else:
                outs = 2
            return whole * 3 + outs
        return int(float(ip_str)) * 3
    except (ValueError, IndexError):
        return 0


def outs_to_ip_display(outs: int) -> str:
    """Convert total outs back to IP display format (e.g. 19 -> '6.1')."""
    innings = outs // 3
    partial = outs % 3
    return f"{innings}.{partial}" if partial else str(innings)


# =============================================================================
# MLB Historical Stats (Pro 7D + Season)
# =============================================================================


class MLBHistoricalFetcher:
    """Fetch and aggregate MLB stats over date ranges using game logs."""

    _SPORT_IDS = [1, 11, 12, 13, 14]  # MLB, AAA, AA, High-A, A

    def __init__(self):
        self._player_cache: dict[str, int] = {}  # name -> player_id
        self._player_sport: dict[int, int] = {}   # player_id -> sport_id

    def fetch_window(
        self, player_name: str, team: str, position: str, start_date: date, end_date: date,
        mlb_id: Optional[int] = None,
    ) -> tuple[Optional[dict], list]:
        """
        Fetch aggregated stats for a player over the given date range.
        Returns (stats_dict, game_entries_list) — stats_dict is None if
        player not found or no games in range.
        """
        player_id = self._resolve_player_id(player_name, mlb_id)
        if player_id is None:
            logger.debug("MLB player not found: %s", player_name)
            return None, []

        try:
            # Get player's game log for the date range
            game_log = self._fetch_game_log(player_id, start_date, end_date, position)
            if not game_log:
                logger.debug("No games found for %s in range", player_name)
                return None, []

            # Build per-game entries for drill-down
            game_entries = self._build_game_entries(game_log, position)

            # Aggregate based on position
            if position == "Pitcher":
                return self._aggregate_pitcher_stats(game_log), game_entries
            elif position == "Two-Way":
                batter_result = self._aggregate_batter_stats(game_log)
                if batter_result and batter_result.get("pa", 0) > 0:
                    return batter_result, game_entries
                return self._aggregate_pitcher_stats(game_log), game_entries
            else:
                return self._aggregate_batter_stats(game_log), game_entries

        except Exception:
            logger.exception("Error fetching window stats for %s", player_name)
            return None, []

    def _build_game_entries(self, games: list[dict], position: str) -> list[dict]:
        """Build per-game drill-down entries from MLB API splits."""
        entries = []
        for game in games:
            stat = game.get("stat", {})
            game_date = game.get("date", "")
            opponent_info = game.get("opponent", {})
            opp_name = ""
            if isinstance(opponent_info, dict):
                opp_team = opponent_info.get("team", {})
                if isinstance(opp_team, dict):
                    opp_name = opp_team.get("name", "")
            is_home = game.get("isHome", False)
            opp_str = f"vs {opp_name}" if is_home else f"@ {opp_name}" if opp_name else ""

            # Determine pitcher vs hitter for this game
            ip_str = str(stat.get("inningsPitched", ""))
            if ip_str and ip_str != "0" and (position == "Pitcher" or position == "Two-Way"):
                entry_stats = {
                    "ip": ip_str,
                    "er": int(stat.get("earnedRuns", 0)),
                    "k": int(stat.get("strikeOuts", 0)),
                    "bb": int(stat.get("baseOnBalls", 0)),
                    "h": int(stat.get("hits", 0)),
                }
            else:
                entry_stats = {
                    "h": int(stat.get("hits", 0)),
                    "ab": int(stat.get("atBats", 0)),
                    "hr": int(stat.get("homeRuns", 0)),
                    "rbi": int(stat.get("rbi", 0)),
                    "r": int(stat.get("runs", 0)),
                    "bb": int(stat.get("baseOnBalls", 0)),
                    "k": int(stat.get("strikeOuts", 0)),
                    "sb": int(stat.get("stolenBases", 0)),
                }
            entries.append({"date": game_date, "opponent": opp_str, "stats": entry_stats})
        # Sort most recent first
        entries.sort(key=lambda g: g["date"], reverse=True)
        return entries

    def _resolve_player_id(self, name: str, mlb_id: Optional[int] = None) -> Optional[int]:
        """Resolve a player's MLB API ID.

        Uses the roster-provided mlb_id directly when available, falling
        back to the name-based search for players without one.
        """
        if mlb_id:
            self._player_cache[name] = mlb_id
            # Resolve sport level for the player so _fetch_game_log uses
            # the correct sportId
            if mlb_id not in self._player_sport:
                try:
                    data = statsapi.lookup_player(str(mlb_id))
                    if data:
                        ct = data[0].get("currentTeam", {})
                        if isinstance(ct, dict) and ct.get("id"):
                            resp = _http.get(
                                f"https://statsapi.mlb.com/api/v1/teams/{ct['id']}",
                                timeout=10,
                            )
                            t = resp.json()["teams"][0]
                            self._player_sport[mlb_id] = t.get("sport", {}).get("id", 1)
                        else:
                            self._player_sport[mlb_id] = 1
                except Exception:
                    self._player_sport[mlb_id] = 1
            return mlb_id
        return self._lookup_player(name)

    def _lookup_player(self, name: str) -> Optional[int]:
        """Search MLB/MiLB for a player ID by name, with caching."""
        if name in self._player_cache:
            return self._player_cache[name]

        try:
            for sport_id in self._SPORT_IDS:
                results = statsapi.lookup_player(name, sportId=sport_id)
                if results:
                    player_id = results[0]["id"]
                    self._player_cache[name] = player_id
                    # Track which sport level this player belongs to
                    ct = results[0].get("currentTeam", {})
                    if isinstance(ct, dict) and ct.get("id"):
                        try:
                            resp = _http.get(
                                f"https://statsapi.mlb.com/api/v1/teams/{ct['id']}",
                                timeout=10,
                            )
                            t = resp.json()["teams"][0]
                            self._player_sport[player_id] = t.get("sport", {}).get("id", sport_id)
                        except Exception:
                            self._player_sport[player_id] = sport_id
                    else:
                        self._player_sport[player_id] = sport_id
                    logger.debug("Found %s at sportId=%d (id=%d)", name, self._player_sport[player_id], player_id)
                    return player_id
        except Exception:
            logger.debug("MLB player lookup failed for %s", name)

        return None

    # Spring Training runs roughly Feb 15 – March 31
    _SPRING_TRAINING_START_MONTH = 2
    _SPRING_TRAINING_END_MONTH = 3

    def _fetch_game_log(
        self, player_id: int, start_date: date, end_date: date,
        position: str = "",
    ) -> list[dict]:
        """
        Fetch player's game-by-game stats for the date range.

        Uses the raw MLB Stats API which returns splits with date fields.
        Includes Spring Training (gameType=S) when the date range overlaps
        the Spring Training window.

        When position is known, only fetches the relevant stat group
        (hitting or pitching) to cut API calls roughly in half.
        """
        try:
            sport_id = self._player_sport.get(player_id, 1)
            season = end_date.year

            # Determine which game types to query
            game_types = ["R"]  # Regular season
            if (start_date.month <= self._SPRING_TRAINING_END_MONTH
                    or end_date.month <= self._SPRING_TRAINING_END_MONTH):
                game_types.append("S")  # Spring Training

            # Only fetch relevant stat groups based on position
            if position == "Pitcher":
                groups = ("pitching",)
            elif position in ("Hitter", ""):
                groups = ("hitting",)
            else:  # Two-Way or unknown
                groups = ("hitting", "pitching")

            all_splits = []
            for group in groups:
                for game_type in game_types:
                    splits = self._fetch_raw_game_log(
                        player_id, season, group, sport_id, game_type
                    )
                    all_splits.extend(splits)

            # Filter to date range
            games = []
            for split in all_splits:
                game_date_str = split.get("date", "")
                if game_date_str:
                    try:
                        game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
                        if start_date <= game_date <= end_date:
                            games.append(split)
                    except ValueError:
                        continue

            return games

        except Exception:
            logger.exception("Error fetching game log for player %d", player_id)
            return []

    @staticmethod
    def _fetch_raw_game_log(
        player_id: int, season: int, group: str, sport_id: int, game_type: str
    ) -> list[dict]:
        """Fetch game log splits from the raw MLB Stats API."""
        try:
            url = (
                f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
                f"?stats=gameLog&season={season}&group={group}"
                f"&sportId={sport_id}&gameType={game_type}"
            )
            resp = _http.get(url, timeout=10)
            if resp.status_code != 200:
                return []
            data = resp.json()
            for stat_group in data.get("stats", []):
                splits = stat_group.get("splits", [])
                if splits:
                    return splits
            return []
        except Exception:
            return []

    def _aggregate_batter_stats(self, games: list[dict]) -> dict:
        """Aggregate batting stats across multiple games."""
        totals = {
            "pa": 0, "ab": 0, "h": 0, "doubles": 0, "triples": 0,
            "hr": 0, "rbi": 0, "r": 0, "bb": 0, "k": 0, "sb": 0,
            "hbp": 0, "sf": 0,
        }

        for game in games:
            stat = game.get("stat", {})
            totals["ab"] += int(stat.get("atBats", 0))
            totals["h"] += int(stat.get("hits", 0))
            totals["doubles"] += int(stat.get("doubles", 0))
            totals["triples"] += int(stat.get("triples", 0))
            totals["hr"] += int(stat.get("homeRuns", 0))
            totals["rbi"] += int(stat.get("rbi", 0))
            totals["r"] += int(stat.get("runs", 0))
            totals["bb"] += int(stat.get("baseOnBalls", 0))
            totals["k"] += int(stat.get("strikeOuts", 0))
            totals["sb"] += int(stat.get("stolenBases", 0))
            totals["hbp"] += int(stat.get("hitByPitch", 0))
            totals["sf"] += int(stat.get("sacFlies", 0))

        totals["pa"] = totals["ab"] + totals["bb"] + totals["hbp"] + totals["sf"]

        avg = totals["h"] / totals["ab"] if totals["ab"] > 0 else 0
        obp = (
            (totals["h"] + totals["bb"] + totals["hbp"]) / totals["pa"]
            if totals["pa"] > 0 else 0
        )
        singles = totals["h"] - totals["doubles"] - totals["triples"] - totals["hr"]
        tb = singles + (2 * totals["doubles"]) + (3 * totals["triples"]) + (4 * totals["hr"])
        slg = tb / totals["ab"] if totals["ab"] > 0 else 0
        ops = obp + slg
        k_pct = totals["k"] / totals["pa"] if totals["pa"] > 0 else 0
        bb_pct = totals["bb"] / totals["pa"] if totals["pa"] > 0 else 0

        return {
            "games_played": len(games),
            "pa": totals["pa"], "ab": totals["ab"], "h": totals["h"],
            "hr": totals["hr"], "rbi": totals["rbi"], "r": totals["r"],
            "bb": totals["bb"], "k": totals["k"], "sb": totals["sb"],
            "avg": avg, "obp": obp, "slg": slg, "ops": ops,
            "k_pct": k_pct, "bb_pct": bb_pct,
            "is_pitcher": False,
        }

    def _aggregate_pitcher_stats(self, games: list[dict]) -> dict:
        """Aggregate pitching stats across multiple games."""
        totals = {
            "outs": 0, "h": 0, "r": 0, "er": 0,
            "bb": 0, "k": 0, "hr": 0, "w": 0, "l": 0, "sv": 0,
        }

        for game in games:
            stat = game.get("stat", {})
            ip_str = str(stat.get("inningsPitched", "0"))
            totals["outs"] += self._ip_to_outs(ip_str)
            totals["h"] += int(stat.get("hits", 0))
            totals["r"] += int(stat.get("runs", 0))
            totals["er"] += int(stat.get("earnedRuns", 0))
            totals["bb"] += int(stat.get("baseOnBalls", 0))
            totals["k"] += int(stat.get("strikeOuts", 0))
            totals["hr"] += int(stat.get("homeRuns", 0))
            totals["w"] += int(stat.get("wins", 0))
            totals["l"] += int(stat.get("losses", 0))
            totals["sv"] += int(stat.get("saves", 0))

        ip = totals["outs"] / 3
        era = (totals["er"] * 9) / ip if ip > 0 else 0
        whip = (totals["bb"] + totals["h"]) / ip if ip > 0 else 0
        k_per_9 = (totals["k"] * 9) / ip if ip > 0 else 0
        bb_per_9 = (totals["bb"] * 9) / ip if ip > 0 else 0
        bf = totals["outs"] + totals["h"] + totals["bb"]
        k_pct = totals["k"] / bf if bf > 0 else 0
        bb_pct = totals["bb"] / bf if bf > 0 else 0

        return {
            "games_played": len(games),
            "ip": ip,
            "ip_display": self._outs_to_ip_display(totals["outs"]),
            "h": totals["h"], "er": totals["er"], "bb": totals["bb"],
            "k": totals["k"], "hr": totals["hr"],
            "w": totals["w"], "l": totals["l"], "sv": totals["sv"],
            "era": era, "whip": whip,
            "k_per_9": k_per_9, "bb_per_9": bb_per_9,
            "k_pct": k_pct, "bb_pct": bb_pct,
            "is_pitcher": True,
        }

    _ip_to_outs = staticmethod(ip_to_outs)
    _outs_to_ip_display = staticmethod(outs_to_ip_display)


# =============================================================================
# D1Baseball Season Stats (NCAA)
# =============================================================================

# Map roster team names → D1Baseball URL slugs
D1B_SLUG = {
    "Alabama": "alabama",
    "Auburn": "auburn",
    "Clemson": "clemson",
    "Coastal Carolina": "coastcar",
    "Dallas Baptist": "dallasbapt",
    "Duke": "duke",
    "FIU": "flinternat",
    "Florida": "florida",
    "Florida State": "floridast",
    "Fordham": "fordham",
    "Georgia Tech": "gatech",
    "Mercer": "mercer",
    "Michigan": "michigan",
    "North Carolina": "unc",
    "Ohio State": "ohiost",
    "Rutgers": "rutgers",
    "Sacramento State": "sacstate",
    "SE Louisiana": "sela",
    "Saint Josephs": "stjosephs",
    "South Carolina": "scarolina",
    "Southern Miss": "smiss",
    "Texas": "texas",
    "Tulane": "tulane",
    "UCF": "ucf",
    "USF": "sflorida",
    "Vanderbilt": "vandy",
    "Virginia": "virginia",
    "Wake Forest": "wake",
}


class D1BaseballSeasonFetcher:
    """
    Fetch season statistics for NCAA players from D1Baseball team stats pages.

    One HTTP request per team returns all players' batting + pitching stats.
    Results are cached in memory per team so multiple players on the same
    team don't cause repeat requests.
    """

    STATS_URL = "https://d1baseball.com/team/{slug}/stats/"
    REQUEST_DELAY = 3.0  # seconds between requests
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self):
        # team_name -> {"batting": [row_dicts], "pitching": [row_dicts]}
        self._team_cache: dict[str, dict] = {}
        self._last_request: float = 0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self._last_request = time.time()

    @staticmethod
    def _normalize_last_name(name: str) -> str:
        """Strip accents, suffixes (Jr, Sr, II, III, IV), and lowercase."""
        nfkd = unicodedata.normalize("NFKD", name)
        name = "".join(c for c in nfkd if not unicodedata.combining(c))
        name = name.strip().lower()
        name = re.sub(r"[,\s]+(jr\.?|sr\.?|ii|iii|iv|v)$", "", name)
        return name.strip()

    @staticmethod
    def _safe_int(val: str) -> int:
        try:
            return int(val)
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _safe_float(val: str) -> float:
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    def get_season_stats(self, player_name: str, team: str, position: str) -> Optional[dict]:
        """
        Fetch current season stats for an NCAA player from D1Baseball.
        Returns a formatted stats dict or None if not found.
        """
        team_data = self._get_team_data(team)
        if team_data is None:
            return None

        if position == "Two-Way":
            result = self._find_batter(player_name, team_data["batting"])
            if result is None:
                result = self._find_pitcher(player_name, team_data["pitching"])
            return result
        elif position == "Pitcher":
            return self._find_pitcher(player_name, team_data["pitching"])
        else:
            return self._find_batter(player_name, team_data["batting"])

    def _get_team_data(self, team: str) -> Optional[dict]:
        """Fetch and cache parsed batting + pitching tables for a team."""
        if team in self._team_cache:
            return self._team_cache[team]

        slug = D1B_SLUG.get(team)
        if not slug:
            logger.warning("D1B: no slug mapping for team '%s'", team)
            return None

        try:
            self._rate_limit()
            url = self.STATS_URL.format(slug=slug)
            resp = _http.get(url, headers=self._HEADERS, timeout=15)
            if resp.status_code != 200:
                logger.warning("D1B: fetch failed for %s (%d)", team, resp.status_code)
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            batting = self._parse_table(soup, "batting-stats")
            pitching = self._parse_table(soup, "pitching-stats")

            data = {"batting": batting, "pitching": pitching}
            self._team_cache[team] = data
            logger.info("D1B: fetched %s — %d batters, %d pitchers",
                        team, len(batting), len(pitching))
            return data

        except Exception:
            logger.exception("D1B: error fetching team page for %s", team)
            return None

    @staticmethod
    def _parse_table(soup: BeautifulSoup, table_id: str) -> list[dict]:
        """Parse a D1Baseball stats table into a list of row dicts."""
        table = soup.find("table", id=table_id)
        if not table:
            return []

        thead = table.find("thead")
        if not thead:
            return []
        headers = [th.text.strip() for th in thead.find_all("th")]

        tbody = table.find("tbody")
        if not tbody:
            return []

        rows = []
        for tr in tbody.find_all("tr"):
            cells = [td.text.strip() for td in tr.find_all(["td", "th"])]
            if len(cells) >= len(headers):
                rows.append(dict(zip(headers, cells)))
        return rows

    def _match_player(self, player_name: str, rows: list[dict]) -> Optional[dict]:
        """
        Match a player by last name from the D1Baseball table rows.
        If multiple matches, prefer the one whose first-name initial matches.
        """
        parts = player_name.strip().split()
        if not parts:
            return None

        target_last = self._normalize_last_name(parts[-1])
        target_first_initial = parts[0][0].lower() if parts[0] else ""

        candidates = []
        for row in rows:
            d1b_name = row.get("Player", "")
            if not d1b_name:
                continue
            d1b_parts = d1b_name.strip().split()
            if not d1b_parts:
                continue
            d1b_last = self._normalize_last_name(d1b_parts[-1])
            if d1b_last == target_last:
                candidates.append((row, d1b_parts))

        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0][0]

        # Multiple matches — prefer first-name initial match
        for row, d1b_parts in candidates:
            if d1b_parts[0][0].lower() == target_first_initial:
                return row

        # Fallback to first match
        return candidates[0][0]

    def _find_batter(self, player_name: str, batting_rows: list[dict]) -> Optional[dict]:
        """Find a player in batting rows and build the hitter stats dict."""
        row = self._match_player(player_name, batting_rows)
        if row is None:
            logger.debug("D1B: batter not found — %s", player_name)
            return None

        gp = self._safe_int(row.get("GP", "0"))
        pa = self._safe_int(row.get("PA", "0"))
        ab = self._safe_int(row.get("AB", "0"))
        h = self._safe_int(row.get("H", "0"))
        hr = self._safe_int(row.get("HR", "0"))
        rbi = self._safe_int(row.get("RBI", "0"))
        r = self._safe_int(row.get("R", "0"))
        bb = self._safe_int(row.get("BB", "0"))
        k = self._safe_int(row.get("K", "0"))
        sb = self._safe_int(row.get("SB", "0"))

        avg = self._safe_float(row.get("BA", "0"))
        obp = self._safe_float(row.get("OBP", "0"))
        slg = self._safe_float(row.get("SLG", "0"))
        ops = self._safe_float(row.get("OPS", "0"))
        k_pct = k / pa if pa > 0 else 0
        bb_pct = bb / pa if pa > 0 else 0

        logger.info("D1B season: %s — %dG %dPA %dH %dHR .%03d/.%03d/.%03d",
                     player_name, gp, pa, h, hr,
                     int(avg * 1000), int(obp * 1000), int(slg * 1000))

        return {
            "games_played": gp,
            "pa": pa, "ab": ab, "h": h, "hr": hr,
            "rbi": rbi, "r": r, "bb": bb, "k": k, "sb": sb,
            "avg": avg, "obp": obp, "slg": slg, "ops": ops,
            "k_pct": k_pct, "bb_pct": bb_pct,
            "is_pitcher": False,
        }

    def _find_pitcher(self, player_name: str, pitching_rows: list[dict]) -> Optional[dict]:
        """Find a player in pitching rows and build the pitcher stats dict."""
        row = self._match_player(player_name, pitching_rows)
        if row is None:
            logger.debug("D1B: pitcher not found — %s", player_name)
            return None

        app = self._safe_int(row.get("APP", "0"))
        ip = self._safe_float(row.get("IP", "0"))
        ip_str = row.get("IP", "0")
        h = self._safe_int(row.get("H", "0"))
        er = self._safe_int(row.get("ER", "0"))
        bb = self._safe_int(row.get("BB", "0"))
        k = self._safe_int(row.get("K", "0"))
        w = self._safe_int(row.get("W", "0"))
        l = self._safe_int(row.get("L", "0"))
        sv = self._safe_int(row.get("SV", "0"))

        era = self._safe_float(row.get("ERA", "0"))
        # WHIP not in D1B pitching table — calculate from (H + BB) / IP
        whip = (h + bb) / ip if ip > 0 else 0.0
        k_per_9 = (k * 9) / ip if ip > 0 else 0
        bb_per_9 = (bb * 9) / ip if ip > 0 else 0
        outs = ip_to_outs(ip_str)
        bf = outs + h + bb
        k_pct = k / bf if bf > 0 else 0
        bb_pct = bb / bf if bf > 0 else 0

        logger.info("D1B season: %s — %dAPP %sIP %dK %.2f ERA %.2f WHIP",
                     player_name, app, ip_str, k, era, whip)

        return {
            "games_played": app,
            "ip": ip,
            "ip_display": outs_to_ip_display(outs),
            "h": h, "er": er, "bb": bb, "k": k, "hr": 0,
            "w": w, "l": l, "sv": sv,
            "era": era, "whip": whip,
            "k_per_9": k_per_9, "bb_per_9": bb_per_9,
            "k_pct": k_pct, "bb_pct": bb_pct,
            "is_pitcher": True,
        }


# =============================================================================
# NCAA Game Log Aggregator (7D from persisted per-game data)
# =============================================================================


class NCAAGameLogAggregator:
    """Aggregate NCAA per-game stats from the persisted game log file."""

    def __init__(self):
        self._log: dict[str, list] = {}
        if os.path.exists(NCAA_GAME_LOG_PATH):
            try:
                with open(NCAA_GAME_LOG_PATH) as f:
                    self._log = json.load(f)
            except Exception:
                logger.error("NCAA game log is corrupted — backing up and starting fresh")
                try:
                    backup = NCAA_GAME_LOG_PATH + ".corrupt"
                    import shutil
                    shutil.copy2(NCAA_GAME_LOG_PATH, backup)
                    logger.error("Corrupted log backed up to %s", backup)
                except Exception:
                    pass
        # Normalize dates and deduplicate
        for key, entries in self._log.items():
            seen = set()
            clean = []
            for e in entries:
                d = self._normalize_date(e.get("date", ""))
                e["date"] = d
                if d and d not in seen:
                    seen.add(d)
                    clean.append(e)
            self._log[key] = clean

    @staticmethod
    def _normalize_date(d: str) -> str:
        if "/" in d:
            try:
                return datetime.strptime(d, "%m/%d/%Y").strftime("%Y-%m-%d")
            except ValueError:
                return d
        return d

    _ip_to_outs = staticmethod(ip_to_outs)
    _outs_to_ip_display = staticmethod(outs_to_ip_display)

    def _is_pitcher_entry(self, stats: dict) -> bool:
        """Check if a game log entry is a pitcher line (has ip field)."""
        return "ip" in stats and str(stats.get("ip", "0")) not in ("0", "0.0", "")

    def get_window_stats(
        self, player_name: str, team: str, position: str,
        start_date: date, end_date: date,
    ) -> tuple[Optional[dict], list]:
        """
        Aggregate game log stats for an NCAA player in the given date range.
        Returns (stats_dict, game_entries_list).
        """
        key = f"{player_name}|{team}"
        entries = self._log.get(key, [])

        # Filter to date range
        in_range = []
        for e in entries:
            try:
                d = datetime.strptime(e["date"], "%Y-%m-%d").date()
                if start_date <= d <= end_date:
                    in_range.append(e)
            except (ValueError, KeyError):
                continue

        if not in_range:
            return None, []

        # Determine pitcher vs hitter
        # For Two-Way: check if entries have ip field
        is_pitcher = position == "Pitcher"
        if position == "Two-Way":
            pitcher_entries = [e for e in in_range if self._is_pitcher_entry(e.get("stats", {}))]
            is_pitcher = len(pitcher_entries) > len(in_range) - len(pitcher_entries)

        if is_pitcher:
            stats, game_entries = self._aggregate_pitcher(in_range)
        else:
            stats, game_entries = self._aggregate_hitter(in_range)

        return stats, game_entries

    def _aggregate_hitter(self, entries: list[dict]) -> tuple[dict, list]:
        totals = {
            "h": 0, "ab": 0, "hr": 0, "2b": 0, "3b": 0,
            "rbi": 0, "r": 0, "bb": 0, "hbp": 0, "k": 0, "sb": 0,
        }
        game_entries = []

        for e in entries:
            s = e.get("stats", {})
            totals["h"] += int(s.get("h", 0))
            totals["ab"] += int(s.get("ab", 0))
            totals["hr"] += int(s.get("hr", 0))
            totals["2b"] += int(s.get("2b", s.get("doubles", 0)))
            totals["3b"] += int(s.get("3b", s.get("triples", 0)))
            totals["rbi"] += int(s.get("rbi", 0))
            totals["r"] += int(s.get("r", 0))
            totals["bb"] += int(s.get("bb", 0))
            totals["hbp"] += int(s.get("hbp", 0))
            totals["k"] += int(s.get("k", 0))
            totals["sb"] += int(s.get("stolen_bases", s.get("sb", 0)))
            ge = {
                "date": e["date"],
                "opponent": e.get("opponent", ""),
                "stats": {
                    "h": int(s.get("h", 0)), "ab": int(s.get("ab", 0)),
                    "hr": int(s.get("hr", 0)), "rbi": int(s.get("rbi", 0)),
                    "r": int(s.get("r", 0)), "bb": int(s.get("bb", 0)),
                    "hbp": int(s.get("hbp", 0)),
                    "k": int(s.get("k", 0)), "sb": int(s.get("stolen_bases", s.get("sb", 0))),
                },
            }
            if e.get("box_score_url"):
                ge["box_score_url"] = e["box_score_url"]
            game_entries.append(ge)

        # Sort game entries most recent first
        game_entries.sort(key=lambda g: g["date"], reverse=True)

        hbp = totals["hbp"]
        sf = 0
        pa = totals["ab"] + totals["bb"] + hbp + sf
        avg = totals["h"] / totals["ab"] if totals["ab"] > 0 else 0
        obp = (totals["h"] + totals["bb"] + hbp) / pa if pa > 0 else 0
        singles = totals["h"] - totals["2b"] - totals["3b"] - totals["hr"]
        tb = singles + (2 * totals["2b"]) + (3 * totals["3b"]) + (4 * totals["hr"])
        slg = tb / totals["ab"] if totals["ab"] > 0 else 0
        ops = obp + slg
        k_pct = totals["k"] / pa if pa > 0 else 0
        bb_pct = totals["bb"] / pa if pa > 0 else 0

        stats = {
            "games_played": len(entries),
            "pa": pa, "ab": totals["ab"], "h": totals["h"],
            "hr": totals["hr"], "rbi": totals["rbi"], "r": totals["r"],
            "bb": totals["bb"], "k": totals["k"], "sb": totals["sb"],
            "avg": avg, "obp": obp, "slg": slg, "ops": ops,
            "k_pct": k_pct, "bb_pct": bb_pct,
            "is_pitcher": False,
        }
        return stats, game_entries

    def _aggregate_pitcher(self, entries: list[dict]) -> tuple[dict, list]:
        total_outs = 0
        totals = {"er": 0, "k": 0, "bb": 0, "h": 0}
        game_entries = []

        for e in entries:
            s = e.get("stats", {})
            total_outs += self._ip_to_outs(s.get("ip", "0"))
            totals["er"] += int(s.get("er", s.get("earned_runs", 0)))
            totals["k"] += int(s.get("k", s.get("strikeouts", 0)))
            totals["bb"] += int(s.get("bb", s.get("walks_allowed", 0)))
            totals["h"] += int(s.get("h", s.get("hits_allowed", 0)))
            ge = {
                "date": e["date"],
                "opponent": e.get("opponent", ""),
                "stats": {
                    "ip": str(s.get("ip", "0")),
                    "er": int(s.get("er", s.get("earned_runs", 0))),
                    "k": int(s.get("k", s.get("strikeouts", 0))),
                    "bb": int(s.get("bb", s.get("walks_allowed", 0))),
                    "h": int(s.get("h", s.get("hits_allowed", 0))),
                },
            }
            if e.get("box_score_url"):
                ge["box_score_url"] = e["box_score_url"]
            game_entries.append(ge)

        # Sort game entries most recent first
        game_entries.sort(key=lambda g: g["date"], reverse=True)

        ip = total_outs / 3
        era = (totals["er"] * 9) / ip if ip > 0 else 0
        whip = (totals["bb"] + totals["h"]) / ip if ip > 0 else 0
        k_per_9 = (totals["k"] * 9) / ip if ip > 0 else 0
        bb_per_9 = (totals["bb"] * 9) / ip if ip > 0 else 0
        bf = total_outs + totals["h"] + totals["bb"]
        k_pct = totals["k"] / bf if bf > 0 else 0
        bb_pct = totals["bb"] / bf if bf > 0 else 0

        stats = {
            "games_played": len(entries),
            "ip": ip,
            "ip_display": self._outs_to_ip_display(total_outs),
            "h": totals["h"], "er": totals["er"], "bb": totals["bb"],
            "k": totals["k"], "hr": 0,
            "w": 0, "l": 0, "sv": 0,
            "era": era, "whip": whip,
            "k_per_9": k_per_9, "bb_per_9": bb_per_9,
            "k_pct": k_pct, "bb_pct": bb_pct,
            "is_pitcher": True,
        }
        return stats, game_entries


# =============================================================================
# Window Stats Aggregator
# =============================================================================


class WindowStatsAggregator:
    """Orchestrate historical stats for all players across windows.

    Windows:
    - 7D: Pro (MLB game logs) + NCAA (persisted game log)
    - Season: Pro (MLB game logs) + NCAA (D1Baseball)
    """

    def __init__(self):
        self.mlb_fetcher = MLBHistoricalFetcher()
        self.d1b_fetcher = D1BaseballSeasonFetcher()
        self.ncaa_log = NCAAGameLogAggregator()
        self._today = date.today()
        self._season_start = date(self._today.year, 2, 1)
        if self._today < self._season_start:
            self._season_start = date(self._today.year - 1, 2, 1)

    def run_all_windows(self, players: list[dict]) -> dict[str, list]:
        """
        Aggregate stats for all players using concurrent fetching.
        Returns: {"7d": [...], "season": [...]}
        """
        start_7d = self._today - timedelta(days=7)

        def _process_player(player):
            name = player.get("player_name", "")
            level = player.get("level", "")
            logger.info("Processing windows for %s (%s)", name, level)

            entry_7d = self._build_window_entry(
                player, "7d", start_7d, self._today
            )
            entry_season = self._build_window_entry(
                player, "season", self._season_start, self._today
            )
            return entry_7d, entry_season

        results = {"7d": [], "season": []}

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(_process_player, p): p for p in players}
            for future in as_completed(futures):
                try:
                    entry_7d, entry_season = future.result()
                    if entry_7d:
                        results["7d"].append(entry_7d)
                    if entry_season:
                        results["season"].append(entry_season)
                except Exception:
                    player = futures[future]
                    logger.exception(
                        "Failed to process windows for %s",
                        player.get("player_name", "?"),
                    )

        return results

    def _build_window_entry(
        self, player: dict, window: str, start_date: date, end_date: date
    ) -> Optional[dict]:
        """Build a single window stats entry for a player."""
        name = player.get("player_name", "")
        team = player.get("team", "")
        level = player.get("level", "")
        position = player.get("position", "") or player.get("tags", {}).get("position", "Hitter")
        is_client = player.get("is_client", True)

        # Fetch stats based on level and window
        mlb_id = player.get("mlb_id")
        game_log_entries = []
        if level == "Pro":
            stats, game_log_entries = self.mlb_fetcher.fetch_window(
                name, team, position, start_date, end_date, mlb_id=mlb_id
            )
        elif level == "NCAA" and window == "7d":
            stats, game_log_entries = self.ncaa_log.get_window_stats(
                name, team, position, start_date, end_date
            )
        elif level == "NCAA" and window == "season":
            stats = self.d1b_fetcher.get_season_stats(name, team, position)
        else:
            stats = None

        if stats is None:
            stats = self._empty_stats(position)

        formatted = self._format_stats(stats, window, position)
        grade = self._calculate_grade(stats, window, position)

        result = {
            "player_name": name,
            "team": team,
            "level": level,
            "is_client": is_client,
            "tags": player.get("tags", {
                "position": position,
                "draft_class": player.get("draft_class", "N/A"),
                "roster_priority": player.get("roster_priority", 99),
            }),
            "window": window,
            "window_grade": grade,
            "stats": formatted,
            "games_played": stats.get("games_played", 0),
            "last_updated": datetime.utcnow().isoformat() + "Z",
        }

        # Include game log for 7D window drill-down
        if window == "7d" and game_log_entries:
            result["game_log"] = game_log_entries

        return result

    def _empty_stats(self, position: str) -> dict:
        if position == "Pitcher":
            return {
                "ip": 0, "k": 0, "bb": 0, "era": 0, "whip": 0,
                "k_per_9": 0, "bb_per_9": 0, "k_pct": 0, "bb_pct": 0,
                "is_pitcher": True, "games_played": 0,
            }
        return {
            "pa": 0, "ab": 0, "h": 0, "hr": 0,
            "avg": 0, "obp": 0, "slg": 0, "ops": 0,
            "k_pct": 0, "bb_pct": 0,
            "is_pitcher": False, "games_played": 0,
        }

    @staticmethod
    def _fmt_rate(val: float) -> str:
        """Format a rate stat: .455 for <1.000, 1.363 for >=1.000."""
        if val >= 1.0:
            return f"{val:.3f}"
        return f"{val:.3f}"[1:]  # strip leading "0" → ".455"

    def _format_stats(self, stats: dict, window: str, position: str) -> dict:
        is_pitcher = stats.get("is_pitcher", position == "Pitcher")

        if is_pitcher:
            ip = stats.get("ip", 0)
            min_ip = WINDOW_MIN_IP.get(window, 0.1)
            sparse = ip < min_ip

            return {
                "ip": stats.get("ip_display", "--") if not sparse else "--",
                "k": stats.get("k", 0) if not sparse else "--",
                "bb": stats.get("bb", 0) if not sparse else "--",
                "era": f"{stats.get('era', 0):.2f}" if not sparse else "--",
                "whip": f"{stats.get('whip', 0):.2f}" if not sparse else "--",
                "k_per_9": f"{stats.get('k_per_9', 0):.1f}" if not sparse else "--",
                "bb_per_9": f"{stats.get('bb_per_9', 0):.1f}" if not sparse else "--",
                "k_pct": f"{stats.get('k_pct', 0) * 100:.1f}%" if not sparse else "--",
                "bb_pct": f"{stats.get('bb_pct', 0) * 100:.1f}%" if not sparse else "--",
            }
        else:
            pa = stats.get("pa", 0)
            min_pa = WINDOW_MIN_PA.get(window, 1)
            sparse = pa < min_pa

            return {
                "pa": stats.get("pa", 0) if not sparse else "--",
                "ab": stats.get("ab", 0) if not sparse else "--",
                "h": stats.get("h", 0) if not sparse else "--",
                "hr": stats.get("hr", 0) if not sparse else "--",
                "bb": stats.get("bb", 0) if not sparse else "--",
                "k": stats.get("k", 0) if not sparse else "--",
                "rbi": stats.get("rbi", 0) if not sparse else "--",
                "r": stats.get("r", 0) if not sparse else "--",
                "sb": stats.get("sb", 0) if not sparse else "--",
                "avg": self._fmt_rate(stats.get("avg", 0)) if not sparse else "--",
                "obp": self._fmt_rate(stats.get("obp", 0)) if not sparse else "--",
                "slg": self._fmt_rate(stats.get("slg", 0)) if not sparse else "--",
                "ops": self._fmt_rate(stats.get("ops", 0)) if not sparse else "--",
                "k_pct": f"{stats.get('k_pct', 0) * 100:.1f}%" if not sparse else "--",
                "bb_pct": f"{stats.get('bb_pct', 0) * 100:.1f}%" if not sparse else "--",
            }

    def _calculate_grade(self, stats: dict, window: str, position: str) -> str:
        is_pitcher = stats.get("is_pitcher", position == "Pitcher")

        if is_pitcher:
            ip = stats.get("ip", 0)
            min_ip = WINDOW_MIN_IP.get(window, 0.1)
            if ip < min_ip:
                return "— Insufficient"
            return grade_pitcher_window(stats, window)
        else:
            pa = stats.get("pa", 0)
            min_pa = WINDOW_MIN_PA.get(window, 1)
            if pa < min_pa:
                return "— Insufficient"
            return grade_hitter_window(stats, window)


def write_window_json(data: list, path: str):
    """Write window stats to JSON file (atomic via temp file + rename)."""
    import tempfile
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise
    logger.info("Wrote %d entries to %s", len(data), path)
