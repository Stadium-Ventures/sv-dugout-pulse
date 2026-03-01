"""
SV Dugout Pulse — Main Orchestrator

Usage:
    python main.py                # Live mode (fetches roster + today's stats)
    python main.py --mock         # Load test data only (no API calls)
    python main.py --historical   # Aggregate historical stats (7D Pro+NCAA + Season)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo

from src.alerts import check_and_send_alerts, reset_sent_alerts
from src.config import (
    NCAA_GAME_LOG_PATH,
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

# Pending NCAA game log entries — batched and flushed at end of run
_ncaa_log_pending: list[tuple] = []  # [(key, game_date, opponent, entry_stats), ...]
_ncaa_log_lock = threading.Lock()


def _today_et() -> date:
    """Return today's date in ET with a 4 AM day boundary."""
    now = datetime.now(_ET)
    if now.hour < _DAY_FLIP_HOUR:
        return (now - timedelta(days=1)).date()
    return now.date()


def _normalize_date(d: str) -> str:
    """Normalize MM/DD/YYYY → YYYY-MM-DD; pass through ISO dates unchanged."""
    if "/" in d:
        try:
            return datetime.strptime(d, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return d
    return d


def _extract_opponent(game_context: str, player_team: str) -> str:
    """Extract opponent from game context like 'Texas 5, Arkansas 3 | Final'."""
    if not game_context:
        return ""
    # Strip status suffix (e.g. " | Final", " | Top 5th")
    score_part = game_context.split("|")[0].strip()
    # Try to find teams: "TeamA N, TeamB N" or "TeamA N @ TeamB N"
    # Match patterns like "Texas 5, Arkansas 3" or "Texas 5 @ Arkansas 3"
    m = re.match(r"^(.+?)\s+\d+\s*[,@]\s*(.+?)\s+\d+", score_part)
    if m:
        team_a, team_b = m.group(1).strip(), m.group(2).strip()
        # The opponent is whichever team isn't ours
        if player_team.lower() in team_a.lower():
            return f"vs {team_b}"
        elif player_team.lower() in team_b.lower():
            return f"vs {team_a}"
        # Fallback: partial match
        if team_a.lower() in player_team.lower():
            return f"vs {team_b}"
        return f"vs {team_a}"
    return ""


def _append_to_ncaa_game_log(player: dict, stats: dict):
    """Queue a single NCAA game result for batched write."""
    if player.get("level") != "NCAA":
        return
    if stats.get("game_status") != "Final":
        return
    game_date = stats.get("game_date")
    if not game_date:
        return

    # Skip DNP players (game was Final but player didn't appear)
    summary = stats.get("stats_summary", "")
    if "DNP" in summary or "No game data" in summary:
        return

    game_date = _normalize_date(game_date)
    key = f"{player['player_name']}|{player['team']}"
    opponent = _extract_opponent(stats.get("game_context", ""), player["team"])

    # Determine if pitcher or hitter line.
    roster_pos = player.get("position", "Hitter")
    is_pitcher = stats.get("is_pitcher_line", False)
    if roster_pos == "Hitter":
        is_pitcher = False
    elif roster_pos == "Pitcher":
        is_pitcher = True
    # Two-Way: trust the scraper's is_pitcher_line flag
    if is_pitcher:
        entry_stats = {
            "ip": str(stats.get("ip", "0")),
            "er": int(stats.get("earned_runs", stats.get("er", 0))),
            "k": int(stats.get("strikeouts", stats.get("k", 0))),
            "bb": int(stats.get("walks_allowed", stats.get("bb", 0))),
            "h": int(stats.get("hits_allowed", stats.get("h", 0))),
        }
    else:
        entry_stats = {
            "h": int(stats.get("hits", stats.get("h", 0))),
            "ab": int(stats.get("at_bats", stats.get("ab", 0))),
            "hr": int(stats.get("home_runs", stats.get("hr", 0))),
            "2b": int(stats.get("doubles", stats.get("2b", 0))),
            "3b": int(stats.get("triples", stats.get("3b", 0))),
            "rbi": int(stats.get("rbi", 0)),
            "r": int(stats.get("runs", stats.get("r", 0))),
            "bb": int(stats.get("walks", stats.get("bb", 0))),
            "k": int(stats.get("strikeouts", stats.get("k", 0))),
            "sb": int(stats.get("stolen_bases", stats.get("sb", 0))),
        }

    # Skip if all stats are zero (likely DNP not caught above)
    if is_pitcher:
        if entry_stats.get("ip") in ("0", "0.0") and not any(entry_stats.get(k) for k in ("er", "k", "bb", "h")):
            return
    else:
        if entry_stats.get("ab", 0) == 0 and not any(entry_stats.get(k) for k in ("bb", "r", "sb")):
            return

    with _ncaa_log_lock:
        _ncaa_log_pending.append((key, game_date, opponent, entry_stats))
    logger.debug("NCAA game log: queued %s on %s", key, game_date)


def _flush_ncaa_game_log():
    """Flush all pending NCAA game log entries in a single read+write cycle."""
    if not _ncaa_log_pending:
        return

    # Load existing log once
    log = {}
    if os.path.exists(NCAA_GAME_LOG_PATH):
        try:
            with open(NCAA_GAME_LOG_PATH) as f:
                log = json.load(f)
        except Exception:
            log = {}

    added = 0
    for key, game_date, opponent, entry_stats in _ncaa_log_pending:
        entries = log.get(key, [])

        # Normalize existing dates and deduplicate
        seen = set()
        clean = []
        for e in entries:
            e["date"] = _normalize_date(e.get("date", ""))
            if e["date"] not in seen:
                seen.add(e["date"])
                clean.append(e)
        entries = clean

        # Only append if we don't already have this date
        if game_date not in seen:
            entries.append({"date": game_date, "opponent": opponent, "stats": entry_stats})
            added += 1
        log[key] = entries

    if added > 0:
        os.makedirs(os.path.dirname(NCAA_GAME_LOG_PATH), exist_ok=True)
        with open(NCAA_GAME_LOG_PATH, "w") as f:
            json.dump(log, f, indent=2, ensure_ascii=False)
        logger.info("NCAA game log: flushed %d new entries (%d queued)", added, len(_ncaa_log_pending))
    else:
        logger.debug("NCAA game log: no new entries to flush (%d queued, all dupes)", len(_ncaa_log_pending))

    _ncaa_log_pending.clear()


def _build_profile_url(player: dict, stats: dict) -> str | None:
    """Build a player profile URL — MLB.com for Pro, Baseball Cube search for NCAA."""
    level = player.get("level", "")
    name = player.get("player_name", "")
    if level == "Pro":
        mlb_id = stats.get("mlb_player_id")
        if mlb_id:
            return f"https://www.mlb.com/player/{mlb_id}"
    elif level == "NCAA" and name:
        q = quote(f'site:thebaseballcube.com "{name}"')
        return f"https://www.google.com/search?q={q}&btnI="
    return None


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
        "box_score_url": stats.get("box_score_url"),
        "player_profile_url": _build_profile_url(player, stats),
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

            _append_to_ncaa_game_log(player, stats)
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

    def _process_player(player):
        """Process a single player — safe for concurrent execution."""
        name = player["player_name"]
        is_client = player.get("is_client", True)
        try:
            stats = fetcher.fetch(player)
            _append_to_ncaa_game_log(player, stats)
            analysis = analyzer.analyze(player, stats)
            entry = build_pulse_entry(player, stats, analysis)

            logger.info(
                "%s%s | %s | %s",
                name,
                "" if is_client else " [following]",
                stats.get("stats_summary", "—"),
                analysis["performance_grade"],
            )
            # Return entry + alert data (alerts sent serially after pool)
            alert_data = (player, stats) if is_client else None
            return entry, alert_data
        except Exception:
            logger.exception("Failed to process %s — skipping", name)
            return None, None

    pulse = []
    alert_queue = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_process_player, p): p for p in all_players}
        for future in as_completed(futures):
            entry, alert_data = future.result()
            if entry:
                pulse.append(entry)
            if alert_data:
                alert_queue.append(alert_data)

    # Send alerts serially to avoid Slack rate limits
    for player, stats in alert_queue:
        check_and_send_alerts(player, stats)

    _flush_ncaa_game_log()

    # Write output — convert yesterday-only entries to N/A for Today tab
    today_str = _today_et().isoformat()
    today_pulse = []
    for p in pulse:
        is_stale = p.get("is_yesterday") or (
            p.get("game_date") and p["game_date"] < today_str
        )
        if is_stale:
            # Player's only game was from a previous day — show as "No game
            # today" on the Today tab but preserve next_game info.
            na = dict(p)
            na.update({
                "stats_summary": "No game data",
                "game_context": "",
                "game_status": "N/A",
                "game_time": None,
                "game_date": None,
                "is_yesterday": False,
                "box_score_url": None,
                "performance_grade": "— No Data",
            })
            today_pulse.append(na)
        elif p.get("game_status") == "Cancelled":
            # Game was cancelled — show in its own Cancelled section.
            na = dict(p)
            na.update({
                "stats_summary": "Game cancelled",
                "game_context": "",
                "game_status": "Cancelled",
                "game_time": None,
                "is_yesterday": False,
                "box_score_url": None,
                "performance_grade": "— No Data",
            })
            today_pulse.append(na)
        else:
            today_pulse.append(p)
    write_output(today_pulse)
    _supplement_yesterday(pulse)

    _fetch_yesterday_pass(all_players, fetcher, analyzer)
    _flush_ncaa_game_log()


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
    """Aggregate historical stats: 7D (Pro + NCAA) + Season (everyone)."""
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
