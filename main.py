"""
SV Dugout Pulse — Main Orchestrator

Usage:
    python main.py                # Live mode (fetches roster + today's stats)
    python main.py --mock         # Load test data only (no API calls)
    python main.py --historical   # Aggregate historical stats (7D/30D/Season)
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

from src.alerts import check_and_send_alerts, reset_sent_alerts
from src.config import (
    NCAA_GAME_LOG_PATH,
    OUTPUT_PATH,
    ROSTER_URL,
    WINDOW_7D_PATH,
    WINDOW_30D_PATH,
    WINDOW_SEASON_PATH,
    YESTERDAY_PULSE_PATH,
)
from src.historical_stats import WindowStatsAggregator, write_window_json
from src.performance_analyzer import PerformanceAnalyzer
from src.roster_manager import get_all_players
from src.stats_engine import StatsFetcher

logger = logging.getLogger("pulse")


def build_pulse_entry(player: dict, stats: dict, analysis: dict) -> dict:
    """Assemble a single player's output record."""
    return {
        "player_name": player["player_name"],
        "team": player["team"],
        "level": player["level"],
        "stats_summary": stats.get("stats_summary", "No game data"),
        "game_context": stats.get("game_context", ""),
        "game_status": stats.get("game_status", "N/A"),
        "game_time": stats.get("game_time"),
        "game_date": stats.get("game_date"),
        "is_yesterday": stats.get("is_yesterday", False),
        "next_game": stats.get("next_game"),
        "performance_grade": analysis["performance_grade"],
        "social_search_url": analysis["social_search_url"],
        "is_client": player.get("is_client", True),
        "tags": {
            "draft_class": player.get("draft_class", ""),
            "position": player.get("position", ""),
            "roster_priority": player.get("roster_priority", 99),
        },
    }


def _store_ncaa_game_log(player: dict, stats: dict, game_logs: dict):
    """
    If an NCAA player has a Final game with actual stats, append to game_logs dict.
    Deduplicates by date so repeated cron runs don't double-count.
    """
    if player.get("level") != "NCAA":
        return
    if stats.get("game_status") != "Final":
        return
    # Must have actual stats (not just game context)
    if stats.get("at_bats", 0) == 0 and stats.get("ip", 0) == 0:
        return

    key = f"{player['player_name']}|{player['team']}"
    game_date_str = stats.get("game_date") or date.today().isoformat()

    if key not in game_logs:
        game_logs[key] = []

    # Deduplicate by date
    if any(entry["date"] == game_date_str for entry in game_logs[key]):
        return

    # Build stats entry
    entry_stats = {
        "h": stats.get("hits", 0),
        "ab": stats.get("at_bats", 0),
        "hr": stats.get("home_runs", 0),
        "rbi": stats.get("rbi", 0),
        "r": stats.get("runs", 0),
        "bb": stats.get("walks", 0),
        "k": stats.get("strikeouts", 0),
        "sb": stats.get("stolen_bases", 0),
        "hbp": 0,
        "sf": 0,
        "doubles": 0,
        "triples": 0,
    }

    # Pitcher fields
    if stats.get("is_pitcher_line"):
        entry_stats.update({
            "ip": str(stats.get("ip", 0)),
            "earned_runs": stats.get("earned_runs", 0),
            "er": stats.get("earned_runs", 0),
            "strikeouts": stats.get("strikeouts", 0),
            "k": stats.get("strikeouts", 0),
            "walks_allowed": stats.get("walks_allowed", 0),
            "bb": stats.get("walks_allowed", 0),
            "hits_allowed": stats.get("hits_allowed", 0),
        })

    game_logs[key].append({"date": game_date_str, "stats": entry_stats})


def _load_ncaa_game_logs() -> dict:
    """Load existing NCAA game logs from disk."""
    if os.path.exists(NCAA_GAME_LOG_PATH):
        try:
            with open(NCAA_GAME_LOG_PATH) as f:
                return json.load(f)
        except Exception:
            logger.warning("Failed to load NCAA game logs, starting fresh")
    return {}


def _save_ncaa_game_logs(game_logs: dict):
    """Persist NCAA game logs to disk."""
    os.makedirs(os.path.dirname(NCAA_GAME_LOG_PATH), exist_ok=True)
    with open(NCAA_GAME_LOG_PATH, "w") as f:
        json.dump(game_logs, f, indent=2, ensure_ascii=False)
    total_games = sum(len(v) for v in game_logs.values())
    logger.info("Saved NCAA game logs: %d players, %d total game entries", len(game_logs), total_games)


def _rotate_yesterday():
    """On first run of a new day, save previous day's Final results as yesterday.

    Uses ET (UTC-5) for day boundaries so late-night games ending at 11 PM ET
    are correctly attributed to their calendar day.
    """
    if not os.path.exists(OUTPUT_PATH):
        return
    try:
        with open(OUTPUT_PATH) as f:
            old = json.load(f)
        old_gen = old.get("generated_at", "")
        if not old_gen:
            return

        # Compare dates in ET
        ET = timezone(timedelta(hours=-5))
        old_dt = datetime.fromisoformat(old_gen).astimezone(ET)
        now_et = datetime.now(ET)

        if old_dt.date() >= now_et.date():
            return  # Same day — no rotation needed

        old_players = old.get("players", [])
        finals = [p for p in old_players if p.get("game_status") == "Final"]

        if not finals:
            return

        envelope = {
            "generated_at": old_gen,
            "source_date": old_dt.date().isoformat(),
            "players": finals,
        }
        os.makedirs(os.path.dirname(YESTERDAY_PULSE_PATH), exist_ok=True)
        with open(YESTERDAY_PULSE_PATH, "w") as f:
            json.dump(envelope, f, indent=2, ensure_ascii=False)
        logger.info(
            "Rotated %d Final entries to yesterday_pulse.json (from %s)",
            len(finals), old_dt.date(),
        )
    except Exception:
        logger.warning("Failed to rotate yesterday pulse data")


def _supplement_yesterday(pulse: list):
    """Add is_yesterday Final entries from the current run to yesterday file.

    Merges with existing entries (from rotation) so no data is lost.
    """
    new_entries = [
        p for p in pulse
        if p.get("is_yesterday") and p.get("game_status") == "Final"
    ]
    if not new_entries:
        return

    # Load existing yesterday file to merge
    existing = []
    if os.path.exists(YESTERDAY_PULSE_PATH):
        try:
            with open(YESTERDAY_PULSE_PATH) as f:
                data = json.load(f)
            existing = data.get("players", [])
        except Exception:
            pass

    # Merge: add new entries for players not already present
    existing_names = {p["player_name"] for p in existing}
    for entry in new_entries:
        if entry["player_name"] not in existing_names:
            existing.append(entry)

    envelope = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_date": (date.today() - timedelta(days=1)).isoformat(),
        "players": existing,
    }
    os.makedirs(os.path.dirname(YESTERDAY_PULSE_PATH), exist_ok=True)
    with open(YESTERDAY_PULSE_PATH, "w") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False)
    logger.info("Yesterday pulse: %d total entries", len(existing))


def run_live():
    """Full pipeline: fetch roster + recruits -> fetch stats -> grade -> alert -> write JSON."""
    logger.info("Starting live pulse run")

    # Rotate previous day's Final results to yesterday file before overwriting
    _rotate_yesterday()

    # Reset alert tracking for this run
    reset_sent_alerts()

    # 1. Roster (clients + recruits)
    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    clients = [p for p in all_players if p.get("is_client")]
    recruits = [p for p in all_players if not p.get("is_client")]
    logger.info("Loaded %d clients + %d recruits", len(clients), len(recruits))

    # 2. Stats + Analysis + Alerts
    fetcher = StatsFetcher()
    analyzer = PerformanceAnalyzer()
    pulse = []
    ncaa_game_logs = _load_ncaa_game_logs()

    for player in all_players:
        name = player["player_name"]
        is_client = player.get("is_client", True)
        try:
            stats = fetcher.fetch(player)
            analysis = analyzer.analyze(player, stats)
            entry = build_pulse_entry(player, stats, analysis)
            pulse.append(entry)

            # Accumulate NCAA game logs for window stats
            _store_ncaa_game_log(player, stats, ncaa_game_logs)

            # Only send Slack alerts for clients, not recruits
            if is_client:
                check_and_send_alerts(player, stats)

            logger.info(
                "%s%s | %s | %s",
                name,
                "" if is_client else " [following]",
                stats.get("stats_summary", "—"),
                analysis["performance_grade"],
            )
        except Exception:
            logger.exception("Failed to process %s — skipping", name)
            continue

    # 3. Write output
    write_output(pulse)
    _supplement_yesterday(pulse)
    _save_ncaa_game_logs(ncaa_game_logs)


def run_mock():
    """Load pre-generated test data (from generate_test_data.py)."""
    logger.info("Running in --mock mode")

    if not os.path.exists(OUTPUT_PATH):
        logger.error(
            "No test data found at %s — run generate_test_data.py first", OUTPUT_PATH
        )
        sys.exit(1)

    with open(OUTPUT_PATH) as f:
        raw = json.load(f)

    # Support both envelope format and legacy array format
    pulse = raw["players"] if isinstance(raw, dict) else raw

    logger.info("Loaded %d mock entries from %s", len(pulse), OUTPUT_PATH)
    # In mock mode we just validate the file exists and is loadable.
    # The dashboard reads data/current_pulse.json either way.
    print(f"Mock pulse loaded: {len(pulse)} players")
    for entry in pulse:
        print(
            f"  {entry['performance_grade']:15s} | {entry['player_name']:25s} | {entry['stats_summary']}"
        )


def write_output(pulse: list[dict]):
    """Write the pulse list to data/current_pulse.json with generated_at envelope."""
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    envelope = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "players": pulse,
    }
    with open(OUTPUT_PATH, "w") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %d entries to %s", len(pulse), OUTPUT_PATH)


def run_historical():
    """Aggregate historical stats for all time windows (7D/30D/Season)."""
    logger.info("Starting historical stats aggregation")

    # 1. Fetch roster
    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    logger.info("Loaded %d total players for historical aggregation", len(all_players))

    # 2. Aggregate stats across all windows
    aggregator = WindowStatsAggregator()
    window_data = aggregator.run_all_windows(all_players)

    # 3. Write separate JSON files
    write_window_json(window_data["7d"], WINDOW_7D_PATH)
    write_window_json(window_data["30d"], WINDOW_30D_PATH)
    write_window_json(window_data["season"], WINDOW_SEASON_PATH)

    logger.info(
        "Historical aggregation complete: 7D=%d, 30D=%d, Season=%d",
        len(window_data["7d"]),
        len(window_data["30d"]),
        len(window_data["season"]),
    )


def main():
    parser = argparse.ArgumentParser(description="SV Dugout Pulse")
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use pre-generated test data instead of live APIs",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Aggregate historical stats (7D/30D/Season) instead of live stats",
    )
    args = parser.parse_args()

    if args.mock:
        run_mock()
    elif args.historical:
        run_historical()
    else:
        run_live()


if __name__ == "__main__":
    main()
