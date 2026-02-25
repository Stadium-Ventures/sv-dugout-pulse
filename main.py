"""
SV Dugout Pulse — Main Orchestrator

Usage:
    python main.py                # Live mode (fetches roster + today's stats)
    python main.py --mock         # Load test data only (no API calls)
    python main.py --historical   # Aggregate historical stats (7D Pro + Season)
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from src.alerts import check_and_send_alerts, reset_sent_alerts
from src.config import (
    OUTPUT_PATH,
    WINDOW_7D_PATH,
    WINDOW_SEASON_PATH,
    YESTERDAY_PULSE_PATH,
)
from src.historical_stats import WindowStatsAggregator, write_window_json
from src.performance_analyzer import PerformanceAnalyzer
from src.roster_manager import get_all_players
from src.stats_engine import StatsFetcher

logger = logging.getLogger("pulse")

_ET = ZoneInfo("US/Eastern")
_DAY_FLIP_HOUR = 4  # Day flips at 4 AM ET


def _today_et() -> date:
    """Return today's date in ET with a 4 AM day boundary."""
    now = datetime.now(_ET)
    if now.hour < _DAY_FLIP_HOUR:
        return (now - timedelta(days=1)).date()
    return now.date()


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

        # Apply the same 4 AM ET game-day flip to the old file's timestamp so
        # we compare game days (not calendar days) consistently.
        old_dt = datetime.fromisoformat(old_gen).astimezone(_ET)
        old_game_day = (
            (old_dt - timedelta(days=1)).date()
            if old_dt.hour < _DAY_FLIP_HOUR
            else old_dt.date()
        )
        today = _today_et()

        if old_game_day >= today:
            return  # Same or future game day — no rotation needed

        old_players = old.get("players", [])
        # Yesterday's game day = today minus one game day
        yesterday_str = (today - timedelta(days=1)).isoformat()
        finals = [
            p for p in old_players
            if p.get("game_status") == "Final"
            and p.get("game_date") == yesterday_str
        ]

        if not finals:
            return

        envelope = {
            "generated_at": old_gen,
            "source_date": yesterday_str,
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
    """Add is_yesterday Final entries from the current run to yesterday file."""
    yesterday_str = (_today_et() - timedelta(days=1)).isoformat()
    new_entries = [
        p for p in pulse
        if p.get("is_yesterday") and p.get("game_status") == "Final"
        and p.get("game_date") == yesterday_str
    ]
    if not new_entries:
        return

    existing = []
    if os.path.exists(YESTERDAY_PULSE_PATH):
        try:
            with open(YESTERDAY_PULSE_PATH) as f:
                data = json.load(f)
            # Only keep entries that actually belong to yesterday — filter out
            # stale entries from earlier game days that may have accumulated.
            existing = [
                p for p in data.get("players", [])
                if p.get("game_date") == yesterday_str
            ]
        except Exception:
            pass

    existing_by_name = {p["player_name"]: p for p in existing}
    for entry in new_entries:
        old = existing_by_name.get(entry["player_name"])
        if old is None:
            existing.append(entry)
        elif "DNP" in old.get("stats_summary", "") and "DNP" not in entry.get("stats_summary", ""):
            existing[:] = [p for p in existing if p["player_name"] != entry["player_name"]]
            existing.append(entry)

    envelope = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_date": yesterday_str,
        "players": existing,
    }
    os.makedirs(os.path.dirname(YESTERDAY_PULSE_PATH), exist_ok=True)
    with open(YESTERDAY_PULSE_PATH, "w") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False)
    logger.info("Yesterday pulse: %d total entries", len(existing))


def _fetch_yesterday_pass(all_players: list, fetcher: StatsFetcher, analyzer: PerformanceAnalyzer):
    """Dedicated yesterday-only fetch pass."""
    yesterday_str = (_today_et() - timedelta(days=1)).isoformat()

    existing = []
    if os.path.exists(YESTERDAY_PULSE_PATH):
        try:
            with open(YESTERDAY_PULSE_PATH) as f:
                data = json.load(f)
            # Filter existing entries to only actual yesterday games
            existing = [
                p for p in data.get("players", [])
                if p.get("game_date") == yesterday_str
            ]
        except Exception:
            pass

    confirmed_names = {
        p["player_name"] for p in existing
        if "DNP" not in p.get("stats_summary", "")
    }
    existing_by_name = {p["player_name"]: p for p in existing}
    added = 0
    updated = 0

    for player in all_players:
        name = player["player_name"]
        if name in confirmed_names:
            continue

        try:
            stats = fetcher.fetch_yesterday(player)
            if stats is None or stats.get("game_status") != "Final":
                continue
            if stats.get("game_date") != yesterday_str:
                continue
            if "DNP" in stats.get("stats_summary", "") and name in existing_by_name:
                continue

            stats["is_yesterday"] = True
            analysis = analyzer.analyze(player, stats)
            entry = build_pulse_entry(player, stats, analysis)

            if name in existing_by_name:
                existing[:] = [p for p in existing if p["player_name"] != name]
                updated += 1
            else:
                added += 1

            existing.append(entry)
            confirmed_names.add(name)
        except Exception:
            logger.debug("Yesterday pass failed for %s — skipping", name)
            continue

    # Always write — even an empty list clears stale wrong-date entries that
    # _supplement_yesterday may have left behind.
    envelope = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_date": yesterday_str,
        "players": existing,
    }
    os.makedirs(os.path.dirname(YESTERDAY_PULSE_PATH), exist_ok=True)
    with open(YESTERDAY_PULSE_PATH, "w") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False)
    logger.info("Yesterday pulse: %d total entries (%d added, %d upgraded)", len(existing), added, updated)


def run_live():
    """Full pipeline: fetch roster + recruits -> fetch stats -> grade -> alert -> write JSON."""
    logger.info("Starting live pulse run")

    _rotate_yesterday()
    reset_sent_alerts()

    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    clients = [p for p in all_players if p.get("is_client")]
    recruits = [p for p in all_players if not p.get("is_client")]
    logger.info("Loaded %d clients + %d recruits", len(clients), len(recruits))

    fetcher = StatsFetcher()
    analyzer = PerformanceAnalyzer()
    pulse = []

    for player in all_players:
        name = player["player_name"]
        is_client = player.get("is_client", True)
        try:
            stats = fetcher.fetch(player)
            analysis = analyzer.analyze(player, stats)
            entry = build_pulse_entry(player, stats, analysis)
            pulse.append(entry)

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

    # Write output — exclude yesterday's games from the Today tab
    today_str = _today_et().isoformat()
    today_pulse = [
        p for p in pulse
        if not p.get("is_yesterday")
        and (not p.get("game_date") or p["game_date"] >= today_str)
    ]
    write_output(today_pulse)
    _supplement_yesterday(pulse)

    _fetch_yesterday_pass(all_players, fetcher, analyzer)


def run_mock():
    """Load pre-generated test data (from generate_test_data.py)."""
    logger.info("Running in --mock mode")

    if not os.path.exists(OUTPUT_PATH):
        logger.error("No test data found at %s — run generate_test_data.py first", OUTPUT_PATH)
        sys.exit(1)

    with open(OUTPUT_PATH) as f:
        raw = json.load(f)

    pulse = raw["players"] if isinstance(raw, dict) else raw

    logger.info("Loaded %d mock entries from %s", len(pulse), OUTPUT_PATH)
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
    """Aggregate historical stats: 7D (Pro only) + Season (everyone via BBRef/statsapi)."""
    logger.info("Starting historical stats aggregation")

    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    logger.info("Loaded %d total players for historical aggregation", len(all_players))

    aggregator = WindowStatsAggregator()
    window_data = aggregator.run_all_windows(all_players)

    write_window_json(window_data["7d"], WINDOW_7D_PATH)
    write_window_json(window_data["season"], WINDOW_SEASON_PATH)

    logger.info(
        "Historical aggregation complete: 7D=%d, Season=%d",
        len(window_data["7d"]),
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
        help="Aggregate historical stats (7D Pro + Season) instead of live stats",
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
