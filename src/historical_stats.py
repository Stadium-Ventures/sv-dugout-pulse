"""
SV Dugout Pulse — Historical Stats Aggregator

Fetches and aggregates player statistics for Season (and Pro 7D) windows.
- Pro (MLB/MiLB): game logs via statsapi
- NCAA Season: scraped from Baseball Reference player pages
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests
import statsapi
from bs4 import BeautifulSoup

from .config import (
    BBREF_CACHE_PATH,
    WINDOW_7D_PATH,
    WINDOW_SEASON_PATH,
    WINDOW_MIN_IP,
    WINDOW_MIN_PA,
)
from .window_grader import grade_hitter_window, grade_pitcher_window

logger = logging.getLogger(__name__)


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
        self, player_name: str, team: str, position: str, start_date: date, end_date: date
    ) -> Optional[dict]:
        """
        Fetch aggregated stats for a player over the given date range.
        Returns None if player not found or no games in range.
        """
        player_id = self._lookup_player(player_name)
        if player_id is None:
            logger.debug("MLB player not found: %s", player_name)
            return None

        try:
            # Get player's game log for the date range
            game_log = self._fetch_game_log(player_id, start_date, end_date)
            if not game_log:
                logger.debug("No games found for %s in range", player_name)
                return None

            # Aggregate based on position
            if position == "Pitcher":
                return self._aggregate_pitcher_stats(game_log)
            elif position == "Two-Way":
                # Two-way players: try batting first, pitching as fallback
                batter_result = self._aggregate_batter_stats(game_log)
                if batter_result and batter_result.get("pa", 0) > 0:
                    return batter_result
                return self._aggregate_pitcher_stats(game_log)
            else:
                return self._aggregate_batter_stats(game_log)

        except Exception:
            logger.exception("Error fetching window stats for %s", player_name)
            return None

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
                            resp = requests.get(
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
        self, player_id: int, start_date: date, end_date: date
    ) -> list[dict]:
        """
        Fetch player's game-by-game stats for the date range.

        Uses the raw MLB Stats API which returns splits with date fields.
        Includes Spring Training (gameType=S) when the date range overlaps
        the Spring Training window.
        """
        try:
            sport_id = self._player_sport.get(player_id, 1)
            season = end_date.year

            # Determine which game types to query
            game_types = ["R"]  # Regular season
            if (start_date.month <= self._SPRING_TRAINING_END_MONTH
                    or end_date.month <= self._SPRING_TRAINING_END_MONTH):
                game_types.append("S")  # Spring Training

            all_splits = []
            for group in ("hitting", "pitching"):
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
            resp = requests.get(url, timeout=10)
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

        return {
            "games_played": len(games),
            "pa": totals["pa"], "ab": totals["ab"], "h": totals["h"],
            "hr": totals["hr"], "rbi": totals["rbi"], "r": totals["r"],
            "bb": totals["bb"], "k": totals["k"], "sb": totals["sb"],
            "avg": avg, "obp": obp, "slg": slg, "ops": ops,
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

        return {
            "games_played": len(games),
            "ip": ip,
            "ip_display": self._outs_to_ip_display(totals["outs"]),
            "h": totals["h"], "er": totals["er"], "bb": totals["bb"],
            "k": totals["k"], "hr": totals["hr"],
            "w": totals["w"], "l": totals["l"], "sv": totals["sv"],
            "era": era, "whip": whip,
            "is_pitcher": True,
        }

    @staticmethod
    def _ip_to_outs(ip_str: str) -> int:
        try:
            if "." in ip_str:
                parts = ip_str.split(".")
                return (int(parts[0]) * 3) + int(parts[1])
            return int(float(ip_str)) * 3
        except (ValueError, IndexError):
            return 0

    @staticmethod
    def _outs_to_ip_display(outs: int) -> str:
        innings = outs // 3
        partial = outs % 3
        return f"{innings}.{partial}" if partial else str(innings)


# =============================================================================
# Baseball Reference Season Stats (NCAA)
# =============================================================================


class BBRefSeasonFetcher:
    """
    Fetch complete season statistics for NCAA players from Baseball Reference.

    Searches by player name, caches the BBRef player ID, and scrapes the
    stats table for the current year.  Results are authoritative and include
    2B, 3B, HR, SB, HBP, SF, etc.  Accepts 1-3 day data lag.
    """

    SEARCH_URL = "https://www.baseball-reference.com/search/search.fcgi"
    PLAYER_URL = "https://www.baseball-reference.com/register/player.fcgi"
    REQUEST_DELAY = 3.0  # seconds between requests (be respectful)
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, cache_path: str = BBREF_CACHE_PATH):
        self._cache_path = cache_path
        self._id_cache: dict[str, str] = {}  # "Name|Team" -> bbref_id
        self._last_request: float = 0
        self._load_cache()

    def _load_cache(self):
        """Load cached BBRef player ID mappings from disk."""
        if os.path.exists(self._cache_path):
            try:
                with open(self._cache_path) as f:
                    self._id_cache = json.load(f)
                logger.info("Loaded %d BBRef ID mappings", len(self._id_cache))
            except Exception:
                self._id_cache = {}

    def _save_cache(self):
        """Persist BBRef ID mappings to disk."""
        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
        with open(self._cache_path, "w") as f:
            json.dump(self._id_cache, f, indent=2)

    def _rate_limit(self):
        """Ensure at least REQUEST_DELAY seconds between HTTP requests."""
        elapsed = time.time() - self._last_request
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self._last_request = time.time()

    @staticmethod
    def _player_key(name: str, team: str) -> str:
        return f"{name}|{team}"

    def get_season_stats(self, player_name: str, team: str, position: str) -> Optional[dict]:
        """
        Fetch current season stats for an NCAA player from BBRef.
        Returns a formatted stats dict or None if not found.
        """
        bbref_id = self._find_player_id(player_name, team)
        if bbref_id is None:
            return None

        try:
            self._rate_limit()
            resp = requests.get(
                f"{self.PLAYER_URL}?id={bbref_id}",
                headers=self._HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("BBRef fetch failed for %s: %d", player_name, resp.status_code)
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            if position == "Two-Way":
                # Two-way players: try batting first (primary), pitching as fallback
                result = self._parse_batting_table(soup, player_name)
                if result is None:
                    result = self._parse_pitching_table(soup, player_name)
                return result
            elif position == "Pitcher":
                return self._parse_pitching_table(soup, player_name)
            else:
                return self._parse_batting_table(soup, player_name)

        except Exception:
            logger.exception("Error fetching BBRef stats for %s", player_name)
            return None

    def _find_player_id(self, player_name: str, team: str) -> Optional[str]:
        """Find a player's BBRef ID by searching, with caching."""
        key = self._player_key(player_name, team)
        if key in self._id_cache:
            cached = self._id_cache[key]
            if cached == "NOT_FOUND":
                return None
            return cached

        try:
            self._rate_limit()
            resp = requests.get(
                self.SEARCH_URL,
                params={"search": player_name},
                headers=self._HEADERS,
                timeout=15,
                allow_redirects=True,
            )

            if resp.status_code != 200:
                return None

            # Direct redirect to player page = unique match
            if "player.fcgi?id=" in resp.url:
                bbref_id = resp.url.split("id=")[-1]
                self._id_cache[key] = bbref_id
                self._save_cache()
                logger.info("BBRef: %s -> %s (direct match)", player_name, bbref_id)
                return bbref_id

            # Search results page — disambiguate by team and year
            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("div", class_="search-item-name")

            current_year = str(date.today().year)
            team_lower = team.lower().replace("\xa0", " ")

            best_id = None
            for item in items:
                link = item.find("a")
                if not link:
                    continue
                href = link.get("href", "")
                text = item.text.strip()

                # Check if year range includes current year
                if current_year not in text:
                    continue

                # Extract ID from href
                match = re.search(r"id=([^&]+)", href)
                if match:
                    candidate_id = match.group(1)
                    # If only one result matches the year, use it
                    if best_id is None:
                        best_id = candidate_id
                    else:
                        # Multiple matches — need to check team on player page
                        best_id = self._disambiguate_by_team(
                            [best_id, candidate_id], team_lower, current_year
                        )
                        break

            if best_id:
                self._id_cache[key] = best_id
                self._save_cache()
                logger.info("BBRef: %s -> %s (search match)", player_name, best_id)
                return best_id

            # Cache negative result to avoid repeated searches
            self._id_cache[key] = "NOT_FOUND"
            self._save_cache()
            logger.info("BBRef: %s not found", player_name)
            return None

        except Exception:
            logger.exception("BBRef search failed for %s", player_name)
            return None

    def _disambiguate_by_team(
        self, candidate_ids: list[str], team_lower: str, year: str
    ) -> Optional[str]:
        """Check player pages to find the one on the right team."""
        for cid in candidate_ids:
            try:
                self._rate_limit()
                resp = requests.get(
                    f"{self.PLAYER_URL}?id={cid}",
                    headers=self._HEADERS,
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                table = soup.find("table", id="standard_batting") or soup.find("table", id="standard_pitching")
                if not table:
                    continue
                tbody = table.find("tbody")
                if not tbody:
                    continue
                for row in tbody.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 4:
                        continue
                    row_year = cells[0].text.strip()
                    row_team = cells[3].text.strip().replace("\xa0", " ").lower()
                    if row_year == year and team_lower in row_team:
                        return cid
            except Exception:
                continue
        return candidate_ids[0] if candidate_ids else None

    def _parse_batting_table(self, soup: BeautifulSoup, player_name: str) -> Optional[dict]:
        """Extract current-year batting stats from BBRef player page."""
        table = soup.find("table", id="standard_batting")
        if not table:
            return None

        thead = table.find("thead")
        if not thead:
            return None
        headers = [th.text.strip() for th in thead.find_all("th")]

        current_year = str(date.today().year)
        tbody = table.find("tbody")
        if not tbody:
            return None

        # Find the row for the current year at NCAA level
        for row in tbody.find_all("tr"):
            cells = [td.text.strip() for td in row.find_all(["td", "th"])]
            if len(cells) < len(headers):
                continue
            row_data = dict(zip(headers, cells))

            if row_data.get("Year") != current_year:
                continue
            if row_data.get("Lev") != "NCAA":
                continue

            return self._build_batter_result(row_data, player_name)

        return None

    def _parse_pitching_table(self, soup: BeautifulSoup, player_name: str) -> Optional[dict]:
        """Extract current-year pitching stats from BBRef player page."""
        table = soup.find("table", id="standard_pitching")
        if not table:
            return None

        thead = table.find("thead")
        if not thead:
            return None
        headers = [th.text.strip() for th in thead.find_all("th")]

        current_year = str(date.today().year)
        tbody = table.find("tbody")
        if not tbody:
            return None

        for row in tbody.find_all("tr"):
            cells = [td.text.strip() for td in row.find_all(["td", "th"])]
            if len(cells) < len(headers):
                continue
            row_data = dict(zip(headers, cells))

            if row_data.get("Year") != current_year:
                continue
            if row_data.get("Lev") != "NCAA":
                continue

            return self._build_pitcher_result(row_data, player_name)

        return None

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

    def _build_batter_result(self, row: dict, player_name: str) -> dict:
        """Build stats dict from a BBRef batting table row."""
        g = self._safe_int(row.get("G", "0"))
        pa = self._safe_int(row.get("PA", "0"))
        ab = self._safe_int(row.get("AB", "0"))
        h = self._safe_int(row.get("H", "0"))
        doubles = self._safe_int(row.get("2B", "0"))
        triples = self._safe_int(row.get("3B", "0"))
        hr = self._safe_int(row.get("HR", "0"))
        rbi = self._safe_int(row.get("RBI", "0"))
        r = self._safe_int(row.get("R", "0"))
        sb = self._safe_int(row.get("SB", "0"))
        bb = self._safe_int(row.get("BB", "0"))
        k = self._safe_int(row.get("SO", "0"))
        hbp = self._safe_int(row.get("HBP", "0"))

        avg = self._safe_float(row.get("BA", "0"))
        obp = self._safe_float(row.get("OBP", "0"))
        slg = self._safe_float(row.get("SLG", "0"))
        ops = self._safe_float(row.get("OPS", "0"))

        logger.info("BBRef season: %s — %dG %dPA %dH %dHR .%03d/.%03d/.%03d",
                     player_name, g, pa, h, hr,
                     int(avg * 1000), int(obp * 1000), int(slg * 1000))

        return {
            "games_played": g,
            "pa": pa, "ab": ab, "h": h, "hr": hr,
            "rbi": rbi, "r": r, "bb": bb, "k": k, "sb": sb,
            "avg": avg, "obp": obp, "slg": slg, "ops": ops,
            "is_pitcher": False,
        }

    def _build_pitcher_result(self, row: dict, player_name: str) -> dict:
        """Build stats dict from a BBRef pitching table row."""
        g = self._safe_int(row.get("G", "0"))
        ip_str = row.get("IP", "0")
        ip = self._safe_float(ip_str)
        er = self._safe_int(row.get("ER", "0"))
        h = self._safe_int(row.get("H", "0"))
        bb = self._safe_int(row.get("BB", "0"))
        k = self._safe_int(row.get("SO", "0"))
        hr = self._safe_int(row.get("HR", "0"))
        w = self._safe_int(row.get("W", "0"))
        l = self._safe_int(row.get("L", "0"))
        sv = self._safe_int(row.get("SV", "0"))

        era = self._safe_float(row.get("ERA", "0"))
        whip = self._safe_float(row.get("WHIP", "0"))

        logger.info("BBRef season: %s — %dG %sIP %dK %.2f ERA",
                     player_name, g, ip_str, k, era)

        return {
            "games_played": g,
            "ip": ip,
            "ip_display": ip_str,
            "h": h, "er": er, "bb": bb, "k": k, "hr": hr,
            "w": w, "l": l, "sv": sv,
            "era": era, "whip": whip,
            "is_pitcher": True,
        }


# =============================================================================
# Window Stats Aggregator
# =============================================================================


class WindowStatsAggregator:
    """Orchestrate historical stats for all players across windows.

    Windows:
    - 7D: Pro players only (MLB game logs)
    - Season: Pro (MLB game logs) + NCAA (Baseball Reference)
    """

    def __init__(self):
        self.mlb_fetcher = MLBHistoricalFetcher()
        self.bbref_fetcher = BBRefSeasonFetcher()
        self._today = date.today()
        self._season_start = date(self._today.year, 2, 1)
        if self._today < self._season_start:
            self._season_start = date(self._today.year - 1, 2, 1)

    def run_all_windows(self, players: list[dict]) -> dict[str, list]:
        """
        Aggregate stats for all players.
        Returns: {"7d": [...], "season": [...]}
        """
        results = {"7d": [], "season": []}

        for player in players:
            name = player.get("player_name", "")
            team = player.get("team", "")
            level = player.get("level", "")
            position = player.get("position", "") or player.get("tags", {}).get("position", "Hitter")

            logger.info("Processing windows for %s (%s)", name, level)

            # --- 7D: Pro only ---
            if level == "Pro":
                start_7d = self._today - timedelta(days=7)
                entry = self._build_window_entry(
                    player, "7d", start_7d, self._today
                )
                if entry:
                    results["7d"].append(entry)

            # --- Season: everyone ---
            entry = self._build_window_entry(
                player, "season", self._season_start, self._today
            )
            if entry:
                results["season"].append(entry)

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
        if level == "Pro":
            stats = self.mlb_fetcher.fetch_window(name, team, position, start_date, end_date)
        elif level == "NCAA" and window == "season":
            stats = self.bbref_fetcher.get_season_stats(name, team, position)
        else:
            stats = None

        if stats is None:
            stats = self._empty_stats(position)

        formatted = self._format_stats(stats, window, position)
        grade = self._calculate_grade(stats, window, position)

        return {
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

    def _empty_stats(self, position: str) -> dict:
        if position == "Pitcher":
            return {
                "ip": 0, "k": 0, "bb": 0, "era": 0, "whip": 0,
                "is_pitcher": True, "games_played": 0,
            }
        return {
            "pa": 0, "ab": 0, "h": 0, "hr": 0,
            "avg": 0, "obp": 0, "slg": 0, "ops": 0,
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
                "rbi": stats.get("rbi", 0) if not sparse else "--",
                "r": stats.get("r", 0) if not sparse else "--",
                "sb": stats.get("sb", 0) if not sparse else "--",
                "avg": self._fmt_rate(stats.get("avg", 0)) if not sparse else "--",
                "obp": self._fmt_rate(stats.get("obp", 0)) if not sparse else "--",
                "slg": self._fmt_rate(stats.get("slg", 0)) if not sparse else "--",
                "ops": self._fmt_rate(stats.get("ops", 0)) if not sparse else "--",
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
    """Write window stats to JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %d entries to %s", len(data), path)
