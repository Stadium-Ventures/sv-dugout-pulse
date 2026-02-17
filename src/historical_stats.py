"""
SV Dugout Pulse — Historical Stats Aggregator

Fetches and aggregates player statistics over time windows (7D/30D/Season).
- MLB: game logs via statsapi
- NCAA Season: cumulative stats fetched directly from NCAA.com (via proxy API)
- NCAA 7D/30D: daily cumulative snapshots → delta calculation
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests
import statsapi

from .config import (
    NCAA_BASELINES_PATH,
    NCAA_GAME_LOG_PATH,
    WINDOW_7D_PATH,
    WINDOW_30D_PATH,
    WINDOW_SEASON_PATH,
    WINDOW_MIN_IP,
    WINDOW_MIN_PA,
)
from .window_grader import grade_hitter_window, grade_pitcher_window

logger = logging.getLogger(__name__)


# =============================================================================
# MLB Historical Stats
# =============================================================================


class MLBHistoricalFetcher:
    """Fetch and aggregate MLB stats over date ranges using game logs."""

    def __init__(self):
        self._player_cache: dict[str, int] = {}  # name -> player_id

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
            is_pitcher = position == "Pitcher"
            if is_pitcher:
                return self._aggregate_pitcher_stats(game_log)
            else:
                return self._aggregate_batter_stats(game_log)

        except Exception:
            logger.exception("Error fetching window stats for %s", player_name)
            return None

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
            logger.debug("MLB player lookup failed for %s", name)

        return None

    def _fetch_game_log(
        self, player_id: int, start_date: date, end_date: date
    ) -> list[dict]:
        """
        Fetch player's game-by-game stats for the date range.
        Uses the stats API endpoint with gameLog type.
        """
        try:
            # Format dates for API
            start_str = start_date.strftime("%m/%d/%Y")
            end_str = end_date.strftime("%m/%d/%Y")

            # Try to get hitting stats
            hitting_log = []
            pitching_log = []

            try:
                hitting_data = statsapi.player_stat_data(
                    player_id,
                    group="hitting",
                    type="gameLog",
                    sportId=1,
                )
                if hitting_data and "stats" in hitting_data:
                    for stat_group in hitting_data["stats"]:
                        if stat_group.get("type", {}).get("displayName") == "gameLog":
                            hitting_log = stat_group.get("splits", [])
                            break
            except Exception:
                pass

            try:
                pitching_data = statsapi.player_stat_data(
                    player_id,
                    group="pitching",
                    type="gameLog",
                    sportId=1,
                )
                if pitching_data and "stats" in pitching_data:
                    for stat_group in pitching_data["stats"]:
                        if stat_group.get("type", {}).get("displayName") == "gameLog":
                            pitching_log = stat_group.get("splits", [])
                            break
            except Exception:
                pass

            # Filter to date range
            games = []
            for game in hitting_log + pitching_log:
                game_date_str = game.get("date", "")
                if game_date_str:
                    try:
                        game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
                        if start_date <= game_date <= end_date:
                            games.append(game)
                    except ValueError:
                        continue

            return games

        except Exception:
            logger.exception("Error fetching game log for player %d", player_id)
            return []

    def _aggregate_batter_stats(self, games: list[dict]) -> dict:
        """Aggregate batting stats across multiple games."""
        totals = {
            "pa": 0,
            "ab": 0,
            "h": 0,
            "doubles": 0,
            "triples": 0,
            "hr": 0,
            "rbi": 0,
            "r": 0,
            "bb": 0,
            "k": 0,
            "sb": 0,
            "hbp": 0,
            "sf": 0,
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

        # Calculate PA
        totals["pa"] = totals["ab"] + totals["bb"] + totals["hbp"] + totals["sf"]

        # Calculate slash line
        avg = totals["h"] / totals["ab"] if totals["ab"] > 0 else 0
        obp = (
            (totals["h"] + totals["bb"] + totals["hbp"]) / totals["pa"]
            if totals["pa"] > 0
            else 0
        )
        singles = totals["h"] - totals["doubles"] - totals["triples"] - totals["hr"]
        tb = singles + (2 * totals["doubles"]) + (3 * totals["triples"]) + (4 * totals["hr"])
        slg = tb / totals["ab"] if totals["ab"] > 0 else 0
        ops = obp + slg

        return {
            "games_played": len(games),
            "pa": totals["pa"],
            "ab": totals["ab"],
            "h": totals["h"],
            "hr": totals["hr"],
            "rbi": totals["rbi"],
            "r": totals["r"],
            "bb": totals["bb"],
            "k": totals["k"],
            "sb": totals["sb"],
            "avg": avg,
            "obp": obp,
            "slg": slg,
            "ops": ops,
            "is_pitcher": False,
        }

    def _aggregate_pitcher_stats(self, games: list[dict]) -> dict:
        """Aggregate pitching stats across multiple games."""
        totals = {
            "outs": 0,  # Track outs for proper IP calculation
            "h": 0,
            "r": 0,
            "er": 0,
            "bb": 0,
            "k": 0,
            "hr": 0,
            "w": 0,
            "l": 0,
            "sv": 0,
        }

        for game in games:
            stat = game.get("stat", {})
            # Convert IP to outs (6.1 IP = 19 outs)
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

        # Convert outs back to IP
        ip = totals["outs"] / 3

        # Calculate ratios
        era = (totals["er"] * 9) / ip if ip > 0 else 0
        whip = (totals["bb"] + totals["h"]) / ip if ip > 0 else 0

        return {
            "games_played": len(games),
            "ip": ip,
            "ip_display": self._outs_to_ip_display(totals["outs"]),
            "h": totals["h"],
            "er": totals["er"],
            "bb": totals["bb"],
            "k": totals["k"],
            "hr": totals["hr"],
            "w": totals["w"],
            "l": totals["l"],
            "sv": totals["sv"],
            "era": era,
            "whip": whip,
            "is_pitcher": True,
        }

    @staticmethod
    def _ip_to_outs(ip_str: str) -> int:
        """Convert IP string (e.g., '6.1') to total outs."""
        try:
            if "." in ip_str:
                parts = ip_str.split(".")
                innings = int(parts[0])
                partial = int(parts[1]) if len(parts) > 1 else 0
                return (innings * 3) + partial
            else:
                return int(float(ip_str)) * 3
        except (ValueError, IndexError):
            return 0

    @staticmethod
    def _outs_to_ip_display(outs: int) -> str:
        """Convert total outs to display IP (e.g., 19 outs -> '6.1')."""
        innings = outs // 3
        partial = outs % 3
        if partial == 0:
            return str(innings)
        return f"{innings}.{partial}"


# =============================================================================
# NCAA Baseline Management
# =============================================================================


class NCAABaselineManager:
    """
    Manage NCAA cumulative stat snapshots for delta calculation.

    NCAA sources provide cumulative season stats, not daily game logs.
    We store daily snapshots and calculate window stats as:
        window_stats = current_cumulative - baseline_from_N_days_ago
    """

    def __init__(self, baselines_path: str = NCAA_BASELINES_PATH):
        self.baselines_path = baselines_path
        self._baselines: dict = {}
        self._load_baselines()

    def _load_baselines(self):
        """Load existing baselines from disk."""
        if os.path.exists(self.baselines_path):
            try:
                with open(self.baselines_path) as f:
                    self._baselines = json.load(f)
            except Exception:
                logger.exception("Failed to load NCAA baselines")
                self._baselines = {}

    def _save_baselines(self):
        """Persist baselines to disk."""
        os.makedirs(os.path.dirname(self.baselines_path), exist_ok=True)
        with open(self.baselines_path, "w") as f:
            json.dump(self._baselines, f, indent=2)

    def _player_key(self, player_name: str, team: str) -> str:
        """Generate unique key for a player."""
        return f"{player_name}|{team}"

    def store_baseline(
        self, player_name: str, team: str, stats: dict, as_of_date: date
    ):
        """Store today's cumulative stats as a baseline snapshot."""
        key = self._player_key(player_name, team)
        date_str = as_of_date.isoformat()

        if key not in self._baselines:
            self._baselines[key] = {"snapshots": []}

        # Remove old snapshot for same date if exists
        self._baselines[key]["snapshots"] = [
            s for s in self._baselines[key]["snapshots"] if s["date"] != date_str
        ]

        # Add new snapshot
        self._baselines[key]["snapshots"].append({
            "date": date_str,
            "cumulative": stats,
        })

        # Keep only last 45 days of snapshots
        cutoff = (date.today() - timedelta(days=45)).isoformat()
        self._baselines[key]["snapshots"] = [
            s for s in self._baselines[key]["snapshots"] if s["date"] >= cutoff
        ]

        self._save_baselines()

    def get_baseline(
        self, player_name: str, team: str, days_ago: int
    ) -> Optional[dict]:
        """Retrieve baseline from N days ago for delta calculation."""
        key = self._player_key(player_name, team)
        if key not in self._baselines:
            return None

        target_date = (date.today() - timedelta(days=days_ago)).isoformat()
        snapshots = self._baselines[key].get("snapshots", [])

        # Find closest snapshot on or before target date
        best = None
        for snap in snapshots:
            if snap["date"] <= target_date:
                if best is None or snap["date"] > best["date"]:
                    best = snap

        return best["cumulative"] if best else None

    def calculate_window_stats(
        self, current: dict, baseline: Optional[dict], position: str
    ) -> Optional[dict]:
        """
        Calculate window stats as delta between current and baseline.
        Returns None if baseline is unavailable.
        """
        if baseline is None:
            return None

        is_pitcher = position == "Pitcher"

        if is_pitcher:
            # Calculate pitcher deltas
            ip_current = self._ip_to_outs(str(current.get("ip", 0)))
            ip_baseline = self._ip_to_outs(str(baseline.get("ip", 0)))
            outs = ip_current - ip_baseline
            ip = outs / 3

            er = current.get("er", 0) - baseline.get("er", 0)
            h = current.get("h", 0) - baseline.get("h", 0)
            bb = current.get("bb", 0) - baseline.get("bb", 0)
            k = current.get("k", 0) - baseline.get("k", 0)

            era = (er * 9) / ip if ip > 0 else 0
            whip = (bb + h) / ip if ip > 0 else 0

            return {
                "ip": ip,
                "ip_display": self._outs_to_ip_display(outs),
                "k": k,
                "bb": bb,
                "h": h,
                "er": er,
                "era": era,
                "whip": whip,
                "is_pitcher": True,
            }
        else:
            # Calculate hitter deltas
            pa = current.get("pa", 0) - baseline.get("pa", 0)
            ab = current.get("ab", 0) - baseline.get("ab", 0)
            h = current.get("h", 0) - baseline.get("h", 0)
            hr = current.get("hr", 0) - baseline.get("hr", 0)
            bb = current.get("bb", 0) - baseline.get("bb", 0)
            hbp = current.get("hbp", 0) - baseline.get("hbp", 0)
            doubles = current.get("doubles", 0) - baseline.get("doubles", 0)
            triples = current.get("triples", 0) - baseline.get("triples", 0)

            avg = h / ab if ab > 0 else 0
            obp = (h + bb + hbp) / pa if pa > 0 else 0
            singles = h - doubles - triples - hr
            tb = singles + (2 * doubles) + (3 * triples) + (4 * hr)
            slg = tb / ab if ab > 0 else 0
            ops = obp + slg

            return {
                "pa": pa,
                "ab": ab,
                "h": h,
                "hr": hr,
                "bb": bb,
                "avg": avg,
                "obp": obp,
                "slg": slg,
                "ops": ops,
                "is_pitcher": False,
            }

    @staticmethod
    def _ip_to_outs(ip_str: str) -> int:
        """Convert IP string to outs."""
        try:
            if "." in ip_str:
                parts = ip_str.split(".")
                return (int(parts[0]) * 3) + int(parts[1])
            return int(float(ip_str)) * 3
        except (ValueError, IndexError):
            return 0

    @staticmethod
    def _outs_to_ip_display(outs: int) -> str:
        """Convert outs to display IP."""
        innings = outs // 3
        partial = outs % 3
        return f"{innings}.{partial}" if partial else str(innings)


# =============================================================================
# NCAA Game Log Accumulator
# =============================================================================


class NCAAGameLogAggregator:
    """
    Aggregate NCAA player stats from accumulated ESPN boxscore game logs.

    Since no NCAA aggregate stats API exists, main.py appends per-game stats
    to ncaa_game_log.json during each live run. This class reads those logs
    and aggregates over a date window, identically to MLBHistoricalFetcher.

    Game log format:
        {"PlayerName|Team": [{"date": "2026-02-15", "stats": {h, ab, hr, ...}}, ...]}
    """

    def __init__(self, game_log_path: str = NCAA_GAME_LOG_PATH):
        self.game_log_path = game_log_path
        self._game_logs: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.game_log_path):
            try:
                with open(self.game_log_path) as f:
                    self._game_logs = json.load(f)
            except Exception:
                logger.exception("Failed to load NCAA game logs")
                self._game_logs = {}

    @staticmethod
    def _player_key(player_name: str, team: str) -> str:
        return f"{player_name}|{team}"

    def fetch_window(
        self, player_name: str, team: str, position: str,
        start_date: date, end_date: date
    ) -> Optional[dict]:
        """Aggregate game log stats within the given date window."""
        key = self._player_key(player_name, team)
        entries = self._game_logs.get(key, [])
        if not entries:
            return None

        # Filter to date range (handle both YYYY-MM-DD and MM/DD/YYYY formats)
        games_in_range = []
        for entry in entries:
            try:
                d = entry["date"]
                if "/" in d:
                    game_date = datetime.strptime(d, "%m/%d/%Y").date()
                else:
                    game_date = datetime.strptime(d, "%Y-%m-%d").date()
                if start_date <= game_date <= end_date:
                    games_in_range.append(entry["stats"])
            except (ValueError, KeyError):
                continue

        if not games_in_range:
            return None

        is_pitcher = position == "Pitcher"
        if is_pitcher:
            return self._aggregate_pitcher(games_in_range)
        return self._aggregate_batter(games_in_range)

    def _aggregate_batter(self, games: list[dict]) -> dict:
        totals = {
            "pa": 0, "ab": 0, "h": 0, "doubles": 0, "triples": 0,
            "hr": 0, "rbi": 0, "r": 0, "bb": 0, "k": 0, "sb": 0,
            "hbp": 0, "sf": 0,
        }
        for g in games:
            totals["ab"] += int(g.get("ab", 0))
            totals["h"] += int(g.get("h", 0))
            totals["doubles"] += int(g.get("doubles", 0))
            totals["triples"] += int(g.get("triples", 0))
            totals["hr"] += int(g.get("hr", 0))
            totals["rbi"] += int(g.get("rbi", 0))
            totals["r"] += int(g.get("r", 0))
            totals["bb"] += int(g.get("bb", 0))
            totals["k"] += int(g.get("k", 0))
            totals["sb"] += int(g.get("sb", 0))
            totals["hbp"] += int(g.get("hbp", 0))
            totals["sf"] += int(g.get("sf", 0))

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

    def _aggregate_pitcher(self, games: list[dict]) -> dict:
        totals = {"outs": 0, "h": 0, "er": 0, "bb": 0, "k": 0, "hr": 0}
        for g in games:
            ip_str = str(g.get("ip", "0"))
            totals["outs"] += self._ip_to_outs(ip_str)
            totals["h"] += int(g.get("h", g.get("hits_allowed", 0)))
            totals["er"] += int(g.get("er", g.get("earned_runs", 0)))
            totals["bb"] += int(g.get("bb", g.get("walks_allowed", 0)))
            totals["k"] += int(g.get("k", g.get("strikeouts", 0)))
            totals["hr"] += int(g.get("hr", 0))

        ip = totals["outs"] / 3
        era = (totals["er"] * 9) / ip if ip > 0 else 0
        whip = (totals["bb"] + totals["h"]) / ip if ip > 0 else 0

        return {
            "games_played": len(games),
            "ip": ip,
            "ip_display": self._outs_to_ip_display(totals["outs"]),
            "h": totals["h"], "er": totals["er"], "bb": totals["bb"],
            "k": totals["k"], "hr": totals["hr"],
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
# NCAA Game Log Backfiller (fetch boxscores for missed games)
# =============================================================================


class NCAAGameLogBackfiller:
    """
    Backfill NCAA game logs by fetching boxscores for all recent games
    involving roster teams.  Uses the NCAA.com proxy API (same as
    NCAAComScraper) to find games and extract per-player stats.

    This ensures that players who appeared in ANY game get season data,
    even if the cron didn't catch the game while live.
    """

    SCOREBOARD_URL = "https://ncaa-api.henrygd.me/scoreboard/baseball/d1"
    BOXSCORE_URL = "https://ncaa-api.henrygd.me/game"
    SEASON_START = date(2026, 2, 14)  # Opening day 2026

    def __init__(self, game_log_path: str = NCAA_GAME_LOG_PATH):
        self.game_log_path = game_log_path
        self._scoreboard_cache: dict[str, list] = {}
        self._boxscore_cache: dict[str, dict] = {}

    def backfill(self, players: list[dict]):
        """
        Backfill game logs for all NCAA roster players.
        Iterates over dates since season start, finds games for our teams,
        fetches boxscores, and stores per-game stats.
        """
        # Load existing game logs
        game_logs = {}
        if os.path.exists(self.game_log_path):
            try:
                with open(self.game_log_path) as f:
                    game_logs = json.load(f)
            except Exception:
                game_logs = {}

        # Build lookup: team -> list of (player_name, position)
        ncaa_players = [p for p in players if p.get("level") == "NCAA"]
        teams: dict[str, list[dict]] = {}
        for p in ncaa_players:
            team = p.get("team", "")
            if team not in teams:
                teams[team] = []
            teams[team].append(p)

        if not teams:
            return

        logger.info("Backfilling NCAA game logs for %d teams since %s", len(teams), self.SEASON_START)

        today = date.today()
        check_date = self.SEASON_START
        games_found = 0
        entries_added = 0

        while check_date <= today:
            date_str = check_date.strftime("%Y/%m/%d")
            games = self._get_scoreboard(date_str)

            for g in games:
                game = g.get("game", {})
                state = game.get("gameState", "")
                if state != "final":
                    continue

                game_id = game.get("gameID")
                if not game_id:
                    continue

                game_date = check_date.isoformat()

                # Check if any of our teams are in this game
                for side in ("home", "away"):
                    side_info = game.get(side, {})
                    names_dict = side_info.get("names", {})
                    short_name = names_dict.get("short", "")
                    full_name = names_dict.get("full", "")
                    seo_name = names_dict.get("seo", "").replace("-", " ")

                    # Match against our team names
                    matched_team = self._match_team(
                        [short_name, full_name, seo_name], teams
                    )
                    if not matched_team:
                        continue

                    games_found += 1
                    is_home = (side == "home")

                    # Fetch boxscore and extract stats for our players
                    box = self._get_boxscore(game_id)
                    if not box:
                        continue

                    for player in teams[matched_team]:
                        name = player["player_name"]
                        key = f"{name}|{matched_team}"
                        position = player.get("position", "") or player.get("tags", {}).get("position", "Hitter")

                        # Skip if we already have this game
                        if key in game_logs:
                            if any(e["date"] == game_date for e in game_logs[key]):
                                continue

                        stats = self._extract_player_stats(name, is_home, box)
                        if stats is None:
                            continue

                        if key not in game_logs:
                            game_logs[key] = []

                        game_logs[key].append({"date": game_date, "stats": stats})
                        entries_added += 1

            check_date += timedelta(days=1)
            time.sleep(0.1)

        # Save
        if entries_added > 0:
            os.makedirs(os.path.dirname(self.game_log_path), exist_ok=True)
            with open(self.game_log_path, "w") as f:
                json.dump(game_logs, f, indent=2, ensure_ascii=False)

        total_games = sum(len(v) for v in game_logs.values())
        logger.info(
            "Game log backfill: checked %d team-games, added %d entries. "
            "Total: %d players, %d game entries.",
            games_found, entries_added, len(game_logs), total_games,
        )

    def _get_scoreboard(self, date_str: str) -> list:
        if date_str in self._scoreboard_cache:
            return self._scoreboard_cache[date_str]
        try:
            resp = requests.get(f"{self.SCOREBOARD_URL}/{date_str}", timeout=15)
            resp.raise_for_status()
            data = resp.json().get("games", [])
            self._scoreboard_cache[date_str] = data
            return data
        except Exception:
            logger.debug("Scoreboard fetch failed for %s", date_str)
            self._scoreboard_cache[date_str] = []
            return []

    def _get_boxscore(self, game_id) -> Optional[dict]:
        gid = str(game_id)
        if gid in self._boxscore_cache:
            return self._boxscore_cache[gid]
        try:
            resp = requests.get(f"{self.BOXSCORE_URL}/{game_id}/boxscore", timeout=15)
            if resp.status_code != 200:
                self._boxscore_cache[gid] = None
                return None
            data = resp.json()
            self._boxscore_cache[gid] = data
            return data
        except Exception:
            self._boxscore_cache[gid] = None
            return None

    # Map roster team names to NCAA scoreboard short names
    TEAM_ALIAS = {
        "SE Louisiana": "Southeastern La.",
        "USF": "South Fla.",
        "Ohio State": "Ohio St.",
        "Southern Miss": "Southern Miss.",
        "Saint Josephs": "Saint Joseph's",
        "Florida State": "Florida St.",
        "Michigan State": "Michigan St.",
        "Mississippi State": "Mississippi St.",
        "Arizona State": "Arizona St.",
        "Sacramento State": "Sacramento St.",
        "Kennesaw State": "Kennesaw St.",
    }

    @classmethod
    def _match_team(cls, api_names: list[str], our_teams: dict[str, list]) -> Optional[str]:
        """Match NCAA API team names against our roster team names."""
        for api_name in api_names:
            api_lower = api_name.lower()
            for our_team in our_teams:
                our_lower = our_team.lower()
                # Exact match
                if api_lower == our_lower:
                    return our_team
                # Alias match
                alias = cls.TEAM_ALIAS.get(our_team, "")
                if alias and alias.lower() == api_lower:
                    return our_team
                # Common "State" -> "St." pattern
                if our_lower.replace(" state", " st.") == api_lower:
                    return our_team
        return None

    def _extract_player_stats(self, player_name: str, is_home: bool, box: dict) -> Optional[dict]:
        """Extract per-game stats for a specific player from a boxscore."""
        name_parts = player_name.split()
        player_last = name_parts[-1].lower()
        player_first = name_parts[0].lower() if len(name_parts) > 1 else ""

        teams = box.get("teams", [])
        target_team_id = None
        for t in teams:
            if t.get("isHome") == is_home:
                target_team_id = t.get("teamId")
                break

        for tb in box.get("teamBoxscore", []):
            if target_team_id and str(tb.get("teamId")) != str(target_team_id):
                continue

            candidates = []
            for ps in tb.get("playerStats", []):
                last = ps.get("lastName", "").lower()
                if last == player_last or (len(player_last) > 2 and last.startswith(player_last[:3])):
                    candidates.append(ps)

            if len(candidates) > 1 and player_first:
                narrowed = [ps for ps in candidates if ps.get("firstName", "").lower().startswith(player_first[:3])]
                if narrowed:
                    candidates = narrowed

            for ps in candidates:
                pitcher = ps.get("pitcherStats")
                batter = ps.get("batterStats")
                if pitcher:
                    ip_str = pitcher.get("inningsPitched", "0") or "0"
                    return {
                        "h": 0, "ab": 0, "hr": 0, "rbi": 0, "r": 0,
                        "bb": 0, "k": 0, "sb": 0, "hbp": 0, "sf": 0,
                        "doubles": 0, "triples": 0,
                        "ip": ip_str,
                        "earned_runs": int(pitcher.get("earnedRunsAllowed", 0) or 0),
                        "er": int(pitcher.get("earnedRunsAllowed", 0) or 0),
                        "strikeouts": int(pitcher.get("strikeouts", 0) or 0),
                        "walks_allowed": int(pitcher.get("walksAllowed", 0) or 0),
                        "hits_allowed": int(pitcher.get("hitsAllowed", 0) or 0),
                    }
                if batter:
                    ab = int(batter.get("atBats", 0) or 0)
                    bb = int(batter.get("walks", 0) or 0)
                    if ab == 0 and bb == 0:
                        return None  # didn't play
                    return {
                        "h": int(batter.get("hits", 0) or 0),
                        "ab": ab,
                        "hr": 0,
                        "rbi": int(batter.get("runsBattedIn", 0) or 0),
                        "r": int(batter.get("runsScored", 0) or 0),
                        "bb": bb,
                        "k": int(batter.get("strikeouts", 0) or 0),
                        "sb": 0, "hbp": 0, "sf": 0,
                        "doubles": 0, "triples": 0,
                    }
        return None


# =============================================================================
# NCAA Cumulative Season Stats (via NCAA.com API proxy)
# =============================================================================


class NCAASeasonStatsFetcher:
    """
    Fetch cumulative season statistics for NCAA players from NCAA.com
    via the ncaa-api.henrygd.me proxy.

    Pulls leaderboard pages for key stat categories and merges them into
    per-player cumulative stat dicts.  Results are cached for the lifetime
    of this object (one run).
    """

    BASE_URL = "https://ncaa-api.henrygd.me/stats/baseball/d1/current/individual"

    # Stat endpoints to fetch.  Key = stat_id, value = list of fields to extract.
    HITTER_ENDPOINTS = {
        200: {"fields": ["G", "AB", "H"], "rate": "BA", "rate_key": "avg_raw"},  # BA — widest coverage
        504: {"fields": ["BB", "HBP", "SF", "SH"], "rate": "PCT", "rate_key": "obp"},  # OBP
        321: {"fields": ["TB"], "rate": "SLG PCT", "rate_key": "slg"},  # SLG
        201: {"fields": ["HR"]},    # HR per game (has total HR)
        339: {"fields": ["K"]},     # Toughest to K (has total K)
        487: {"fields": ["RBI"]},   # RBI total
        485: {"fields": ["R"]},     # Runs total
        492: {"fields": ["SB"]},    # SB total
    }

    PITCHER_ENDPOINTS = {
        205: {"fields": ["App", "IP", "R", "ER"], "rate": "ERA", "rate_key": "era"},  # ERA
        596: {"fields": ["BB", "HA"], "rate": "WHIP", "rate_key": "whip"},             # WHIP
        207: {"fields": ["SO"]},   # K/9 (has SO total)
    }

    # Map roster team names → NCAA API abbreviated names where they differ.
    TEAM_NAME_MAP = {
        "Florida State": "Florida St.",
        "Ohio State": "Ohio St.",
        "USF": "South Fla.",
        "Southern Miss": "Southern Miss.",
        "SE Louisiana": "Southeastern La.",
        "Sacramento State": "Sacramento St.",
        "Saint Josephs": "Saint Joseph's",
        "Coastal Carolina": "Coastal Caro.",
        "NC State": "NC State",
        "Mississippi State": "Mississippi St.",
        "Michigan State": "Michigan St.",
        "Arizona State": "Arizona St.",
        "Oregon State": "Oregon St.",
        "Fresno State": "Fresno St.",
        "Penn State": "Penn St.",
        "Iowa State": "Iowa St.",
        "Kennesaw State": "Kennesaw St.",
        "Wichita State": "Wichita St.",
    }

    def __init__(self):
        self._hitter_cache: dict[str, dict] = {}   # "Name|Team" → stats
        self._pitcher_cache: dict[str, dict] = {}
        self._fetched = False

    def _ncaa_team_name(self, roster_team: str) -> str:
        """Convert roster team name to NCAA API team name."""
        return self.TEAM_NAME_MAP.get(roster_team, roster_team)

    @staticmethod
    def _player_key(name: str, team: str) -> str:
        return f"{name}|{team}"

    def _ensure_fetched(self):
        """Fetch all stat leaderboards once per run."""
        if self._fetched:
            return
        self._fetched = True

        logger.info("Fetching NCAA cumulative season stats from NCAA API...")

        # Fetch hitter stats
        for stat_id, meta in self.HITTER_ENDPOINTS.items():
            self._fetch_stat_pages(stat_id, meta, is_pitcher=False)

        # Fetch pitcher stats
        for stat_id, meta in self.PITCHER_ENDPOINTS.items():
            self._fetch_stat_pages(stat_id, meta, is_pitcher=True)

        logger.info(
            "NCAA season stats loaded: %d hitters, %d pitchers",
            len(self._hitter_cache), len(self._pitcher_cache),
        )

    def _fetch_stat_pages(self, stat_id: int, meta: dict, is_pitcher: bool):
        """Fetch all pages for a single stat endpoint and merge into cache."""
        cache = self._pitcher_cache if is_pitcher else self._hitter_cache
        page = 1
        while True:
            url = f"{self.BASE_URL}/{stat_id}?page={page}"
            try:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                logger.warning("NCAA API fetch failed: %s page %d", stat_id, page)
                break

            entries = data.get("data", [])
            if not entries:
                break

            for entry in entries:
                name = entry.get("Name", "")
                team = entry.get("Team", "")
                if not name or not team:
                    continue

                key = self._player_key(name, team)
                if key not in cache:
                    cache[key] = {"_name": name, "_team": team}

                # Extract fields
                for field in meta.get("fields", []):
                    if field in entry:
                        cache[key][field.lower()] = self._parse_num(entry[field])

                # Extract rate stat
                rate_field = meta.get("rate")
                rate_key = meta.get("rate_key")
                if rate_field and rate_key and rate_field in entry:
                    cache[key][rate_key] = self._parse_float(entry[rate_field])

            total_pages = data.get("pages", 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.15)  # be polite to the API

    def get_season_stats(self, player_name: str, team: str, position: str) -> Optional[dict]:
        """
        Return cumulative season stats for an NCAA player.
        Returns a dict compatible with the window stats format, or None.
        """
        self._ensure_fetched()

        ncaa_team = self._ncaa_team_name(team)
        is_pitcher = position == "Pitcher"
        cache = self._pitcher_cache if is_pitcher else self._hitter_cache

        # Try exact name + team match
        key = self._player_key(player_name, ncaa_team)
        stats = cache.get(key)

        # Fallback: try last-name match within the same team
        if stats is None:
            last_name = player_name.split()[-1].lower()
            for k, v in cache.items():
                if v["_team"] == ncaa_team and v["_name"].split()[-1].lower() == last_name:
                    stats = v
                    break

        if stats is None:
            return None

        if is_pitcher:
            return self._build_pitcher_result(stats)
        return self._build_hitter_result(stats)

    def get_cumulative_snapshot(self, player_name: str, team: str, position: str) -> Optional[dict]:
        """
        Return raw cumulative stats for baseline storage (7D/30D delta calculation).
        These are the raw counting stats, not formatted for display.
        """
        self._ensure_fetched()

        ncaa_team = self._ncaa_team_name(team)
        is_pitcher = position == "Pitcher"
        cache = self._pitcher_cache if is_pitcher else self._hitter_cache

        key = self._player_key(player_name, ncaa_team)
        stats = cache.get(key)

        if stats is None:
            last_name = player_name.split()[-1].lower()
            for k, v in cache.items():
                if v["_team"] == ncaa_team and v["_name"].split()[-1].lower() == last_name:
                    stats = v
                    break

        if stats is None:
            return None

        # Return raw counting stats for snapshot storage
        if is_pitcher:
            return {
                "ip": stats.get("ip", 0),
                "er": stats.get("er", 0),
                "h": stats.get("ha", 0),
                "bb": stats.get("bb", 0),
                "k": stats.get("so", 0),
            }
        return {
            "ab": stats.get("ab", 0),
            "h": stats.get("h", 0),
            "bb": stats.get("bb", 0),
            "hbp": stats.get("hbp", 0),
            "sf": stats.get("sf", 0),
            "hr": stats.get("hr", 0),
            "tb": stats.get("tb", 0),
            "k": stats.get("k", 0),
            "rbi": stats.get("rbi", 0),
            "r": stats.get("r", 0),
            "sb": stats.get("sb", 0),
            "pa": stats.get("ab", 0) + stats.get("bb", 0) + stats.get("hbp", 0) + stats.get("sf", 0),
        }

    def _build_hitter_result(self, stats: dict) -> dict:
        """Build a formatted hitter stats dict from cached NCAA data."""
        ab = stats.get("ab", 0)
        h = stats.get("h", 0)
        bb = stats.get("bb", 0)
        hbp = stats.get("hbp", 0)
        sf = stats.get("sf", 0)
        hr = stats.get("hr", 0)
        tb = stats.get("tb", 0)
        pa = ab + bb + hbp + sf

        avg = stats.get("avg_raw", h / ab if ab > 0 else 0)
        obp = stats.get("obp", (h + bb + hbp) / pa if pa > 0 else 0)
        slg = stats.get("slg", tb / ab if ab > 0 else 0)
        ops = obp + slg

        return {
            "games_played": stats.get("g", 0),
            "pa": pa,
            "ab": ab,
            "h": h,
            "hr": hr,
            "rbi": stats.get("rbi", 0),
            "r": stats.get("r", 0),
            "bb": bb,
            "k": stats.get("k", 0),
            "sb": stats.get("sb", 0),
            "avg": avg,
            "obp": obp,
            "slg": slg,
            "ops": ops,
            "is_pitcher": False,
        }

    def _build_pitcher_result(self, stats: dict) -> dict:
        """Build a formatted pitcher stats dict from cached NCAA data."""
        ip_raw = stats.get("ip", 0)
        # IP from NCAA API is a display string like "10.0" — convert to float
        ip_outs = self._ip_to_outs(str(ip_raw))
        ip = ip_outs / 3

        er = stats.get("er", 0)
        bb = stats.get("bb", 0)
        ha = stats.get("ha", 0)
        so = stats.get("so", 0)

        era = stats.get("era", (er * 9) / ip if ip > 0 else 0)
        whip = stats.get("whip", (bb + ha) / ip if ip > 0 else 0)

        return {
            "games_played": stats.get("app", 0),
            "ip": ip,
            "ip_display": self._outs_to_ip_display(ip_outs),
            "h": ha,
            "er": er,
            "bb": bb,
            "k": so,
            "era": era,
            "whip": whip,
            "is_pitcher": True,
        }

    @staticmethod
    def _parse_num(val) -> int:
        """Parse a string like '4' or '0' to int, tolerating non-numeric."""
        try:
            return int(val)
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _parse_float(val) -> float:
        """Parse a string like '.667' or '1.23' to float."""
        try:
            s = str(val).lstrip(".")
            return float(f"0.{s}") if val and str(val).startswith(".") else float(val)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _ip_to_outs(ip_str: str) -> int:
        try:
            if "." in str(ip_str):
                parts = str(ip_str).split(".")
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
# Window Stats Aggregator
# =============================================================================


class WindowStatsAggregator:
    """Orchestrate historical stats for all players across all windows."""

    WINDOWS = {
        "7d": 7,
        "30d": 30,
        "season": 365,  # Will be adjusted to season start
    }

    def __init__(self):
        self.mlb_fetcher = MLBHistoricalFetcher()
        self.ncaa_game_log = NCAAGameLogAggregator()
        self.ncaa_season = NCAASeasonStatsFetcher()
        self.ncaa_baselines = NCAABaselineManager()
        self.ncaa_backfiller = NCAAGameLogBackfiller()
        self._today = date.today()
        # Approximate season start (adjust based on actual season)
        self._season_start = date(self._today.year, 2, 1)  # Feb 1
        if self._today < self._season_start:
            self._season_start = date(self._today.year - 1, 2, 1)

    def run_all_windows(self, players: list[dict]) -> dict[str, list]:
        """
        Aggregate stats for all players across all time windows.
        Returns: {"7d": [...], "30d": [...], "season": [...]}
        """
        results = {"7d": [], "30d": [], "season": []}

        # Backfill game logs from boxscores for any games we missed
        self.ncaa_backfiller.backfill(players)
        # Reload game logs after backfill
        self.ncaa_game_log = NCAAGameLogAggregator()

        # Store today's cumulative snapshots for NCAA players (enables 7D/30D deltas)
        for player in players:
            if player.get("level") == "NCAA":
                name = player.get("player_name", "")
                team = player.get("team", "")
                position = player.get("position", "") or player.get("tags", {}).get("position", "Hitter")
                snapshot = self.ncaa_season.get_cumulative_snapshot(name, team, position)
                if snapshot:
                    self.ncaa_baselines.store_baseline(name, team, snapshot, self._today)

        for player in players:
            name = player.get("player_name", "")
            team = player.get("team", "")
            level = player.get("level", "")
            position = player.get("position", "") or player.get("tags", {}).get("position", "Hitter")

            logger.info("Processing windows for %s (%s)", name, level)

            for window_key, days in self.WINDOWS.items():
                if window_key == "season":
                    start_date = self._season_start
                else:
                    start_date = self._today - timedelta(days=days)

                entry = self._build_window_entry(
                    player, window_key, start_date, self._today
                )
                if entry:
                    results[window_key].append(entry)

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
        elif level == "NCAA":
            stats = self._fetch_ncaa_window(name, team, position, window)
        else:
            stats = None

        if stats is None:
            # Return entry with sparse data
            stats = self._empty_stats(position)

        # Format stats for display
        formatted = self._format_stats(stats, window, position)

        # Calculate grade
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

    def _fetch_ncaa_window(self, name: str, team: str, position: str, window: str) -> Optional[dict]:
        """Fetch NCAA stats for a given window.

        - Season: try NCAA API leaderboards first, fall back to game logs.
        - 7D/30D: compute delta from daily cumulative snapshots (baselines),
          fall back to game-log accumulation.
        """
        if window == "season":
            # Try NCAA API leaderboards first (widest stats)
            result = self.ncaa_season.get_season_stats(name, team, position)
            if result and result.get("games_played", 0) > 0:
                return result
            # Fall back to aggregated game logs
            return self.ncaa_game_log.fetch_window(
                name, team, position, self._season_start, self._today
            )

        # For 7D/30D: try baseline delta first, fall back to game logs
        current = self.ncaa_season.get_cumulative_snapshot(name, team, position)
        if current is not None:
            days = 7 if window == "7d" else 30
            baseline = self.ncaa_baselines.get_baseline(name, team, days)
            delta = self.ncaa_baselines.calculate_window_stats(current, baseline, position)
            if delta is not None:
                return delta

        # Fall back to accumulated game logs
        days = 7 if window == "7d" else 30
        start = self._today - timedelta(days=days)
        return self.ncaa_game_log.fetch_window(name, team, position, start, self._today)

    def _empty_stats(self, position: str) -> dict:
        """Return empty stats dict for sparse data."""
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
        """Format stats for display, using '--' for sparse data."""
        is_pitcher = stats.get("is_pitcher", position == "Pitcher")

        if is_pitcher:
            ip = stats.get("ip", 0)
            min_ip = WINDOW_MIN_IP.get(window, 2.0)
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
            min_pa = WINDOW_MIN_PA.get(window, 5)
            sparse = pa < min_pa

            return {
                "pa": stats.get("pa", 0) if not sparse else "--",
                "ab": stats.get("ab", 0) if not sparse else "--",
                "h": stats.get("h", 0) if not sparse else "--",
                "hr": stats.get("hr", 0) if not sparse else "--",
                "avg": self._fmt_rate(stats.get("avg", 0)) if not sparse else "--",
                "obp": self._fmt_rate(stats.get("obp", 0)) if not sparse else "--",
                "slg": self._fmt_rate(stats.get("slg", 0)) if not sparse else "--",
                "ops": self._fmt_rate(stats.get("ops", 0)) if not sparse else "--",
            }

    def _calculate_grade(self, stats: dict, window: str, position: str) -> str:
        """Calculate window grade based on stats."""
        is_pitcher = stats.get("is_pitcher", position == "Pitcher")

        if is_pitcher:
            ip = stats.get("ip", 0)
            min_ip = WINDOW_MIN_IP.get(window, 2.0)
            if ip < min_ip:
                return "— Insufficient"
            return grade_pitcher_window(stats, window)
        else:
            pa = stats.get("pa", 0)
            min_pa = WINDOW_MIN_PA.get(window, 5)
            if pa < min_pa:
                return "— Insufficient"
            return grade_hitter_window(stats, window)


def write_window_json(data: list, path: str):
    """Write window stats to JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %d entries to %s", len(data), path)
