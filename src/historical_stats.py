"""
SV Dugout Pulse — Historical Stats Aggregator

Fetches and aggregates player statistics over time windows (7D/30D/Season).
Handles both MLB (via API) and NCAA (via accumulated game logs from ESPN boxscores).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

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

        # Filter to date range
        games_in_range = []
        for entry in entries:
            try:
                game_date = datetime.strptime(entry["date"], "%Y-%m-%d").date()
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

        for player in players:
            name = player.get("player_name", "")
            team = player.get("team", "")
            level = player.get("level", "")
            position = player.get("position", "") or player.get("tags", {}).get("position", "Hitter")
            is_client = player.get("is_client", True)

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

        # Fetch stats based on level
        if level == "Pro":
            stats = self.mlb_fetcher.fetch_window(name, team, position, start_date, end_date)
        elif level == "NCAA":
            # NCAA uses accumulated game logs from ESPN boxscores
            stats = self.ncaa_game_log.fetch_window(name, team, position, start_date, end_date)
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
                "avg": f".{int(stats.get('avg', 0) * 1000):03d}"[1:] if not sparse else "--",
                "obp": f".{int(stats.get('obp', 0) * 1000):03d}"[1:] if not sparse else "--",
                "slg": f".{int(stats.get('slg', 0) * 1000):03d}"[1:] if not sparse else "--",
                "ops": f".{int(stats.get('ops', 0) * 1000):03d}"[1:] if not sparse else "--",
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
