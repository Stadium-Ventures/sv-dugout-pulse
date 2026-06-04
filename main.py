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
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo

from src.alerts import check_and_send_alerts, reset_sent_alerts, save_sent_alerts
from src.config import (
    HS_GAME_LOG_PATH,
    NCAA_GAME_LOG_PATH,
    OUTPUT_PATH,
    PLAYER_HEALTH_HISTORY_PATH,
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


def _ip_float_to_display(ip_val) -> str:
    """Convert IP float (e.g. 5.333) back to baseball notation (e.g. '5.1').

    Also handles strings already in baseball notation ('5.1') by detecting
    fractional parts that are valid thirds (0, 1, 2) and passing through.
    """
    if isinstance(ip_val, str):
        # If already in baseball notation (fractional part is 0, 1, or 2), pass through
        if "." in ip_val:
            parts = ip_val.split(".")
            if len(parts) == 2 and parts[1] in ("0", "1", "2"):
                # Already baseball notation — clean up trailing .0
                return parts[0] if parts[1] == "0" else ip_val
        try:
            val = float(ip_val)
        except (ValueError, TypeError):
            return ip_val
    else:
        try:
            val = float(ip_val)
        except (ValueError, TypeError):
            return "0"

    if val == 0:
        return "0"

    whole = int(val)
    frac = val - whole
    # Map fractional part to outs: ~0.33 = 1 out, ~0.67 = 2 outs
    if frac < 0.16:
        outs = 0
    elif frac < 0.5:
        outs = 1
    else:
        outs = 2

    return f"{whole}.{outs}" if outs else str(whole)


def _today_et() -> date:
    """Return today's date in ET with a 4 AM day boundary."""
    now = datetime.now(_ET)
    if now.hour < _DAY_FLIP_HOUR:
        return (now - timedelta(days=1)).date()
    return now.date()


def _atomic_json_write(path: str, data, **kwargs):
    """Write JSON to *path* atomically via temp file + rename."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, **kwargs)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def _normalize_date(d: str) -> str:
    """Normalize MM/DD/YYYY → YYYY-MM-DD; pass through ISO dates unchanged."""
    if "/" in d:
        try:
            return datetime.strptime(d, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return d
    return d


def _extract_opponent(game_context: str, player_team: str) -> str:
    """Extract opponent from game context like 'Texas 5, Arkansas 3 | Final'.

    Also handles score-less contexts like 'Texas vs Arkansas | Scheduled'
    and MLB formats like 'New York Yankees 4, New York Mets 10 | Final'.
    """
    if not game_context:
        return ""
    # Strip status suffix (e.g. " | Final", " | Top 5th")
    score_part = game_context.split("|")[0].strip()

    # Pattern 1: "TeamA N, TeamB N" or "TeamA N @ TeamB N" (with scores)
    m = re.match(r"^(.+?)\s+\d+\s*[,@]\s*(.+?)\s+\d+", score_part)
    if not m:
        # Pattern 2: "TeamA vs TeamB" or "TeamA at TeamB" (no scores)
        m = re.match(r"^(.+?)\s+(?:vs\.?|at|@)\s+(.+?)$", score_part, re.IGNORECASE)
    if m:
        team_a, team_b = m.group(1).strip(), m.group(2).strip()
        # The opponent is whichever team isn't ours
        pt = player_team.lower()
        if pt in team_a.lower() or team_a.lower() in pt:
            return f"vs {team_b}"
        elif pt in team_b.lower() or team_b.lower() in pt:
            return f"vs {team_a}"
        return f"vs {team_a}"
    return ""


def _count_recent_starts(player: dict, game_date: str) -> int:
    """Count how many games a position player appeared in over the last 7 days.

    Uses existing game log files for NCAA/HS and the MLB Stats API for Pro.
    Only called for position players showing DNP / not-in-lineup, so the
    extra I/O is minimal.  Returns 0 on any error.
    """
    level = player.get("level", "")
    name = player.get("player_name", "")
    team = player.get("team", "")

    try:
        ref_date = date.fromisoformat(game_date) if game_date else _today_et()
    except (ValueError, TypeError):
        ref_date = _today_et()
    cutoff = (ref_date - timedelta(days=7)).isoformat()

    try:
        if level == "NCAA":
            return _count_from_game_log(NCAA_GAME_LOG_PATH, f"{name}|{team}", cutoff)
        if level == "HS":
            return _count_from_game_log(HS_GAME_LOG_PATH, name, cutoff)
        if level == "Pro":
            return _count_pro_recent(player, cutoff)
    except Exception:
        logger.debug("recent_starts lookup failed for %s", name)
    return 0


def _count_from_game_log(path: str, key: str, cutoff: str) -> int:
    """Count entries in a game-log JSON file where date >= cutoff."""
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        log = json.load(f)
    entries = log.get(key, [])
    return sum(1 for e in entries if (e.get("date") or "") >= cutoff)


def _count_pro_recent(player: dict, cutoff: str) -> int:
    """Count recent Pro game appearances via the MLB Stats API gameLog endpoint."""
    import requests as _requests  # local import to avoid circular / top-level dep

    mlb_id = player.get("mlb_id")
    if not mlb_id:
        return 0
    year = _today_et().year
    url = (
        f"https://statsapi.mlb.com/api/v1/people/{mlb_id}/stats"
        f"?stats=gameLog&group=hitting&season={year}"
    )
    resp = _requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    count = 0
    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            if (split.get("date") or "") >= cutoff:
                count += 1
    return count


def _append_to_ncaa_game_log(player: dict, stats: dict):
    """Queue a single NCAA game result for batched write.

    Live entries with real stats are also logged so a later run that
    can't reach the live box can carry these forward instead of falling
    back to "not in lineup" — the dedup logic at flush time keeps the
    fullest snapshot of any (date, opponent, game_number).
    """
    if player.get("level") != "NCAA":
        return
    status = stats.get("game_status")
    if status not in ("Final", "Live"):
        return
    game_date = stats.get("game_date")
    if not game_date:
        return

    # Skip DNP players (game was Final but player didn't appear)
    summary = stats.get("stats_summary", "")
    if "DNP" in summary or "No game data" in summary:
        return
    # During Live games, only log if we actually have stats — the
    # all-zero check below catches "in lineup, no PAs yet" cases.
    if status == "Live" and "not in lineup" in summary.lower():
        return

    game_date = _normalize_date(game_date)
    key = f"{player['player_name']}|{player['team']}"
    opponent = _extract_opponent(stats.get("game_context", ""), stats.get("api_current_team") or player.get("affiliate") or player["team"])

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
            "ip": _ip_float_to_display(stats.get("ip", 0)),
            "er": int(stats.get("earned_runs", stats.get("er", 0))),
            "k": int(stats.get("strikeouts", stats.get("k", 0))),
            "bb": int(stats.get("walks_allowed", stats.get("walks", stats.get("bb", 0)))),
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
            "hbp": int(stats.get("hit_by_pitch", stats.get("hbp", 0))),
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

    box_url = stats.get("box_score_url", "")
    game_number = stats.get("game_number") or 0
    is_pitcher_line = bool(is_pitcher)

    with _ncaa_log_lock:
        _ncaa_log_pending.append(
            (key, game_date, opponent, entry_stats, box_url, game_number, status, is_pitcher_line)
        )
    logger.debug("NCAA game log: queued %s on %s (gm %s, %s)", key, game_date, game_number, status)


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
            logger.error("NCAA game log corrupted during flush — starting fresh for this batch")
            log = {}

    # Pre-build seen sets for ALL keys at once (avoids re-reading during iteration).
    # Uses date|opponent composite key so both games of a doubleheader are logged.
    seen_by_key: dict[str, set] = {}
    for key, entries in log.items():
        clean = []
        seen = set()
        for e in entries:
            e["date"] = _normalize_date(e.get("date", ""))
            dedup = f"{e['date']}|{e.get('opponent', '')}"
            if dedup not in seen:
                seen.add(dedup)
                clean.append(e)
        log[key] = clean
        seen_by_key[key] = seen

    now_iso = datetime.now(timezone.utc).isoformat()

    added = 0
    updated = 0
    for key, game_date, opponent, entry_stats, box_url, game_number, status, is_pitcher_line in _ncaa_log_pending:
        seen = seen_by_key.get(key, set())

        entry = {
            "date": game_date,
            "opponent": opponent,
            "stats": entry_stats,
            "captured_at": now_iso,
            "captured_status": status,  # "Live" or "Final"
            "is_pitcher_line": is_pitcher_line,
        }
        if box_url:
            entry["box_score_url"] = box_url
        if game_number:
            entry["game_number"] = game_number

        # Include game_number in dedup key so doubleheaders aren't collapsed
        dedup = f"{game_date}|{opponent}|{game_number}" if game_number else f"{game_date}|{opponent}"
        if dedup not in seen:
            # New entry — append
            log.setdefault(key, []).append(entry)
            seen.add(dedup)
            seen_by_key[key] = seen
            added += 1
        else:
            # Existing entry — update if new data has more substance
            # (e.g. walks populated on a later fetch after box score fully loaded)
            for existing in log.get(key, []):
                existing_gn = existing.get("game_number", 0)
                if (existing.get("date") == game_date
                        and existing.get("opponent") == opponent
                        and existing_gn == game_number):
                    old_s = existing.get("stats", {})
                    # Count non-zero fields as a measure of data completeness
                    old_nonzero = sum(1 for v in old_s.values() if v and v != "0")
                    new_nonzero = sum(1 for v in entry_stats.values() if v and v != "0")
                    # Final always wins over Live for the same game (it's the
                    # authoritative source); for same-status comparisons, more
                    # non-zero fields wins.
                    old_status = existing.get("captured_status", "Final")
                    final_wins = (status == "Final" and old_status == "Live")
                    if final_wins or new_nonzero > old_nonzero:
                        existing["stats"] = entry_stats
                        existing["captured_at"] = now_iso
                        existing["captured_status"] = status
                        existing["is_pitcher_line"] = is_pitcher_line
                        updated += 1
                        logger.info("NCAA game log: updated %s on %s (non-zero fields %d→%d, %s→%s)",
                                    key, game_date, old_nonzero, new_nonzero, old_status, status)
                    # Backfill box_score_url if missing
                    if box_url and not existing.get("box_score_url"):
                        existing["box_score_url"] = box_url
                        if not updated:  # count as update for save trigger
                            updated += 1
                    break

    if added > 0 or updated > 0:
        _atomic_json_write(NCAA_GAME_LOG_PATH, log, indent=2, ensure_ascii=False)
        logger.info("NCAA game log: flushed %d new, %d updated (%d queued)", added, updated, len(_ncaa_log_pending))
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


# NCAA D1 single-game HR record is 4 (set multiple times). Anything above this is
# almost certainly a parser bug — clamp aggressively and log so we can debug.
_MAX_SINGLE_GAME_HR = 4

# Per-game sanity caps. If a stat exceeds these, the row is almost
# certainly a season aggregate leaking into a per-game field (parser
# bug). When more than one cap fires simultaneously, treat the entire
# stat line as unreliable: skip alerts + flag the entry for QA.
#
# Bounds are generous to avoid false positives on extra-inning outliers:
#   AB     >  9  — never seen legitimately at any level
#   hits   >  7  — MLB single-game record is 7 (Wilbert Robinson 1892);
#                  modern record is 6. NCAA's single-game record is 7.
#   K (batter) > 6  — NCAA single-game K record for a batter is 6.
#   RBI    > 12  — MLB single-game RBI record is 12.
#   walks  >  6  — NCAA outlier cap.
#   SB     >  6  — Otis Nixon's 6-SB game is the MLB max.
_PER_GAME_CAPS = {
    "at_bats": 9,
    "hits": 7,
    "strikeouts_batter": 6,  # batter-side K (pitcher Ks have their own cap)
    "rbi": 12,
    "walks": 6,
    "stolen_bases": 6,
    "doubles": 4,
    "triples": 3,
    "runs": 6,
}


def _patch_summary_stat(summary: str, label: str, old_val: int, new_val: int) -> str:
    """Replace an `{old_val} {label}` token in stats_summary with the clamped value.

    Used by _sanitize_stats so the human-readable summary stays consistent with the
    numeric fields after a clamp. Without this, the alert headline reads from
    stats.home_runs (clamped) while the body reads stats_summary (unclamped) and
    the two disagree — exactly the "4 HRs!" / "9 HR" Tiroly incident on 2026-05-11.
    """
    if not summary or old_val == new_val:
        return summary
    if new_val == 0:
        # Drop the segment entirely (with its leading comma+space if present)
        return re.sub(rf"(?:, )?{old_val} {label}(?=,|$)", "", summary)
    return summary.replace(f"{old_val} {label}", f"{new_val} {label}")


def _sanitize_stats(stats: dict) -> dict:
    """Clamp impossible stat values to prevent garbage data from reaching output.

    When a value is clamped, stats_summary is patched in-place so the rendered
    string stays consistent with the numeric fields. The parser builds
    stats_summary from raw values before this function runs, so any clamp here
    must also rewrite the summary or downstream consumers (Slack alerts, the
    diagnostics dashboard) will display the pre-clamp value.
    """
    summary = stats.get("stats_summary", "") or ""

    # Non-negative integer fields
    for key in ("hits", "at_bats", "home_runs", "rbi", "runs", "walks",
                "strikeouts", "doubles", "triples", "stolen_bases",
                "earned_runs", "walks_allowed", "hits_allowed", "saves"):
        val = stats.get(key)
        if val is not None:
            try:
                val = int(val)
            except (ValueError, TypeError):
                val = 0
            if val < 0:
                logger.warning("Negative %s=%d — clamping to 0", key, val)
                val = 0
            stats[key] = val

    # IP: non-negative, max 27 per game
    ip = stats.get("ip")
    if ip is not None:
        try:
            ip = float(ip)
        except (ValueError, TypeError):
            ip = 0.0
        if ip < 0:
            ip = 0.0
        elif ip > 27.0:
            logger.warning("IP=%s exceeds single-game max — clamping to 27", ip)
            ip = 27.0
        stats["ip"] = ip

    # Relational: hits <= at_bats, hr <= min(hits, NCAA single-game max).
    # When we clamp, also rewrite stats_summary so the human-facing string
    # matches the numeric fields.
    ab = stats.get("at_bats", 0)
    h = stats.get("hits", 0)
    hr = stats.get("home_runs", 0)

    if ab > 0 and h > ab:
        logger.warning("hits(%d) > at_bats(%d) — clamping hits", h, ab)
        # Batter summaries lead with "{h}-{ab}" — rebuild that segment.
        summary = re.sub(rf"^{h}-{ab}", f"{ab}-{ab}", summary)
        stats["hits"] = ab
        h = ab

    if hr > _MAX_SINGLE_GAME_HR:
        # Implausible — almost always a parser bug (wrong column / season total
        # leaking into a per-game field). Log loudly with full context so we
        # can find the root cause.
        logger.error(
            "Implausible home_runs=%d (max %d/game) for player=%r team=%r "
            "raw_stats=%s — clamping to %d",
            hr, _MAX_SINGLE_GAME_HR,
            stats.get("_player_name"), stats.get("_team"),
            {k: v for k, v in stats.items() if not k.startswith("_") and k != "stats_summary"},
            min(h, _MAX_SINGLE_GAME_HR),
        )
        new_hr = min(h, _MAX_SINGLE_GAME_HR) if h > 0 else 0
        summary = _patch_summary_stat(summary, "HR", hr, new_hr)
        stats["home_runs"] = new_hr
        hr = new_hr

    if h > 0 and hr > h:
        logger.warning("home_runs(%d) > hits(%d) — clamping HR", hr, h)
        summary = _patch_summary_stat(summary, "HR", hr, h)
        stats["home_runs"] = h

    # ---- Multi-stat sanity: detect a season-aggregate masquerading as a
    # single-game line. If ≥2 per-game caps are simultaneously busted, the
    # entire row is almost certainly garbage (the Kyle Jones / NCAA Regional
    # 2026-05-29 case had AB=210, hits=64, RBI=37, K=43 — every cap busted).
    # Flag _implausible so downstream consumers (alerts) skip the row
    # entirely, rather than firing "4 HR game!" on season totals.
    is_pitcher_line = stats.get("is_pitcher_line", False)
    caps_busted: list[str] = []
    for field, cap in _PER_GAME_CAPS.items():
        # Skip batter-K check when the row is the pitcher side; that's a
        # legitimate pitcher stat path with its own logic.
        if field == "strikeouts_batter":
            if is_pitcher_line:
                continue
            v = stats.get("strikeouts")
        else:
            v = stats.get(field)
        if v is None:
            continue
        try:
            v = int(v)
        except (ValueError, TypeError):
            continue
        if v > cap:
            caps_busted.append(f"{field}={v} (cap {cap})")

    if len(caps_busted) >= 2:
        logger.error(
            "Implausible single-game stat line for player=%r team=%r — caps busted: %s. "
            "Treating as garbage; downstream alerts will skip. raw=%s",
            stats.get("_player_name"), stats.get("_team"),
            ", ".join(caps_busted),
            {k: v for k, v in stats.items() if not k.startswith("_") and k != "stats_summary"},
        )
        stats["_implausible"] = True
        stats["_implausible_reason"] = ", ".join(caps_busted)

    if summary != stats.get("stats_summary"):
        stats["stats_summary"] = summary

    return stats


def build_pulse_entry(player: dict, stats: dict, analysis: dict) -> dict:
    """Assemble a single player's output record."""
    # If _sanitize_stats flagged the row as a season-aggregate leaking into
    # per-game (≥2 caps busted), don't render the garbage line on cards.
    # Surface as unavailable instead — a later pulse run from a different
    # source path can replace this with real data.
    if stats.get("_implausible"):
        summary = "Stats unavailable — source returned aggregate"
    else:
        summary = stats.get("stats_summary", "No game data")
    summary_lower = (summary or "").lower()
    if stats.get("game_status") == "Final" and ("in lineup" in summary_lower or "in starting" in summary_lower):
        summary = "Started — 0 PA"

    # Prefer the live MLB API team over the static Google Sheet affiliate.
    # This auto-detects promotions, demotions, and trades without waiting
    # for someone to manually update the roster spreadsheet.
    api_team = stats.get("api_current_team")
    sheet_team = player.get("affiliate") or player["team"]

    entry = {
        "player_name": player["player_name"],
        "team": api_team or sheet_team,
        "level": player["level"],
        "stats_summary": summary,
        "game_context": stats.get("game_context", ""),
        "game_status": stats.get("game_status", "N/A"),
        "game_time": stats.get("game_time"),
        "game_date": stats.get("game_date"),
        "is_yesterday": stats.get("is_yesterday", False),
        "next_game": stats.get("next_game"),
        "box_score_url": stats.get("box_score_url"),
        "player_profile_url": _build_profile_url(player, stats),
        "performance_grade": analysis["performance_grade"],
        "grade_reason": analysis.get("grade_reason", ""),
        "social_search_url": analysis["social_search_url"],
        "data_source": stats.get("data_source", ""),
        "fetch_diagnostic": stats.get("fetch_diagnostic"),
        "stats_captured_at": stats.get("stats_captured_at"),
        "stats_captured_status": stats.get("stats_captured_status"),
        "is_client": player.get("is_client", True),
        "tags": {
            "draft_class": player.get("draft_class", ""),
            "position": player.get("position", ""),
            "roster_priority": player.get("roster_priority", 99),
        },
    }
    gn = stats.get("game_number")
    if gn:
        entry["game_number"] = gn
    if stats.get("split_squad"):
        entry["split_squad"] = True
    return entry


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
        candidates = [
            p for p in old_players
            if p.get("game_date") == yesterday_str
            and p.get("game_status") in ("Final", "Live")
        ]

        if not candidates:
            return

        # Mark formerly-Live entries so the yesterday pass re-fetches them
        for p in candidates:
            if p.get("game_status") == "Live":
                p["_needs_refresh"] = True

        envelope = {
            "generated_at": old_gen,
            "source_date": yesterday_str,
            "players": candidates,
        }
        _atomic_json_write(YESTERDAY_PULSE_PATH, envelope, indent=2, ensure_ascii=False)
        live_count = sum(1 for p in candidates if p.get("_needs_refresh"))
        logger.info(
            "Rotated %d entries to yesterday_pulse.json (%d Final, %d Live needing refresh, from %s)",
            len(candidates), len(candidates) - live_count, live_count, old_dt.date(),
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
            logger.warning("Failed to read yesterday_pulse.json in _supplement_yesterday — starting fresh")

    # Dedup key: (player_name, game_number) so doubleheader games don't collide
    def _dedup_key(p):
        return (p["player_name"], p.get("game_number") or 0)

    existing_by_key = {_dedup_key(p): p for p in existing}
    for entry in new_entries:
        key = _dedup_key(entry)
        old = existing_by_key.get(key)
        if old is None:
            existing.append(entry)
            existing_by_key[key] = entry
        elif "DNP" in old.get("stats_summary", "") and "DNP" not in entry.get("stats_summary", ""):
            existing[:] = [p for p in existing if _dedup_key(p) != key]
            existing.append(entry)
            existing_by_key[key] = entry

    # Strip internal flags before writing
    for p in existing:
        p.pop("_needs_refresh", None)

    envelope = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_date": yesterday_str,
        "players": existing,
    }
    _atomic_json_write(YESTERDAY_PULSE_PATH, envelope, indent=2, ensure_ascii=False)
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
            logger.warning("Failed to read yesterday_pulse.json in _fetch_yesterday_pass — starting fresh")

    # Dedup key: (player_name, game_number) so doubleheader games don't collide
    def _dedup_key(p):
        return (p["player_name"], p.get("game_number") or 0)

    confirmed_keys = {
        _dedup_key(p) for p in existing
        if "DNP" not in p.get("stats_summary", "")
        and not p.get("_needs_refresh")
    }
    existing_by_key = {_dedup_key(p): p for p in existing}

    # Players with a confirmed single-game entry (game_number=0) are fully done.
    # Players with game_number > 0 may be missing another game from a
    # doubleheader, so they must be re-fetched.
    fully_confirmed_names = {
        k[0] for k in confirmed_keys if k[1] == 0
    }
    to_fetch = [p for p in all_players if p["player_name"] not in fully_confirmed_names]

    def _fetch_one(player):
        """Fetch + analyze a single player for yesterday — thread-safe.

        Returns a list of pulse entries (supports doubleheaders).
        """
        name = player["player_name"]
        try:
            all_stats = None
            for attempt in range(2):
                try:
                    all_stats = fetcher.fetch_all_yesterday(player)
                    break
                except Exception:
                    if attempt == 0:
                        time.sleep(1)
                    else:
                        raise
            if not all_stats:
                return []

            entries = []
            for stats in all_stats:
                stats = _sanitize_stats(stats)
                if stats.get("game_status") != "Final":
                    continue
                if stats.get("game_date") != yesterday_str:
                    continue
                if "DNP" in stats.get("stats_summary", "") and name in {k[0] for k in existing_by_key}:
                    continue

                _append_to_ncaa_game_log(player, stats)
                stats["is_yesterday"] = True
                analysis = analyzer.analyze(player, stats)
                entries.append(build_pulse_entry(player, stats, analysis))

            return entries
        except Exception:
            logger.debug("Yesterday pass failed for %s after retry — skipping", name)
            return []

    # Fan out fetches concurrently, then merge results sequentially
    results = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_fetch_one, p): p for p in to_fetch}
        for future in as_completed(futures):
            entries = future.result()
            if entries:
                results.extend(entries)

    added = 0
    updated = 0
    for entry in results:
        key = _dedup_key(entry)
        if key in existing_by_key:
            existing[:] = [p for p in existing if _dedup_key(p) != key]
            updated += 1
        else:
            added += 1
        existing.append(entry)
        existing_by_key[key] = entry

    # Repair pass: any entry still flagged Live with _needs_refresh is one
    # the post-game refetch failed to recover. Replace with a "stats
    # unavailable" placeholder marked Final so the UI surfaces it instead
    # of silently filtering it out.
    unavailable = 0
    for p in existing:
        if p.get("_needs_refresh") and p.get("game_status") == "Live":
            p["game_status"] = "Final"
            p["stats_summary"] = "Stats unavailable — couldn't reach box score"
            p["stats_unavailable"] = True
            p["performance_grade"] = "No Data"
            p["grade_reason"] = "Could not capture box score"
            # Preserve fetch_diagnostic so hover-detail still works on the card.
            unavailable += 1

    # Strip internal _needs_refresh flag before writing output
    for p in existing:
        p.pop("_needs_refresh", None)

    # Always write — even an empty list clears stale wrong-date entries that
    # _supplement_yesterday may have left behind.
    envelope = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_date": yesterday_str,
        "players": existing,
    }
    _atomic_json_write(YESTERDAY_PULSE_PATH, envelope, indent=2, ensure_ascii=False)
    logger.info(
        "Yesterday pulse: %d total entries (%d added, %d upgraded, %d marked unavailable)",
        len(existing), added, updated, unavailable,
    )


def _load_locked_finals(today_str: str) -> dict[str, list[dict]]:
    """Load Final entries from current_pulse.json to prevent re-fetching.

    Returns a dict keyed by 'player_name|team' -> list of locked pulse entries.
    Only NCAA entries are locked (Pro links are stable).  Entries must be
    Final, have a game_date matching today, and contain real stats.
    """
    locked: dict[str, list[dict]] = {}
    if not os.path.exists(OUTPUT_PATH):
        return locked
    try:
        with open(OUTPUT_PATH) as f:
            raw = json.load(f)
        players = raw.get("players", raw) if isinstance(raw, dict) else raw
        for p in players:
            if p.get("game_status") != "Final":
                continue
            if p.get("level") != "NCAA":
                continue
            if p.get("game_date") != today_str:
                continue
            summary = p.get("stats_summary", "")
            if summary in ("Did Not Play", "No game data", "Game cancelled", ""):
                continue
            key = f"{p['player_name']}|{p['team']}"
            gn = p.get("game_number") or 0
            existing_gns = {e.get("game_number") or 0 for e in locked.get(key, [])}
            if gn in existing_gns:
                continue  # Skip duplicate game_number for same player
            locked.setdefault(key, []).append(p)
        logger.info("Stats lock: %d NCAA players with Final stats locked for %s", len(locked), today_str)
    except Exception:
        logger.warning("Failed to load locked finals — will re-fetch all")
    return locked


# Status messages that indicate no real player stats were scraped.
_STATUS_ONLY_SUMMARIES = frozenset({
    "Game in progress", "Game in progress — not in lineup",
    "Game in progress — hasn't pitched",
    "Did Not Play", "No game data", "Game cancelled", "",
    "No game today",
})


def _entry_has_real_stats(entry: dict) -> bool:
    """Return True if a pulse entry contains actual player stats (not just a status message)."""
    summary = (entry.get("stats_summary") or "").strip()
    if summary in _STATUS_ONLY_SUMMARIES:
        return False
    if summary.startswith("Game at "):
        return False
    if summary.lower().startswith("in lineup") or summary.lower().startswith("in starting"):
        return False
    return True


def _load_live_stats_cache(today_str: str) -> dict[str, list[dict]]:
    """Load Live NCAA entries with real stats from the previous pulse run.

    When D1Baseball transiently fails and the waterfall falls back to ESPN
    (which rarely has NCAA box-score stats), we lose the player's stats for
    that cycle.  This cache lets us carry forward the previous good data
    instead of showing 'not in lineup'.

    Each cached entry has ``_prev_generated_at`` attached so a downstream
    carry-forward can stamp ``stats_captured_at`` honestly when the
    previous entry didn't already have one set.
    """
    cache: dict[str, list[dict]] = {}
    if not os.path.exists(OUTPUT_PATH):
        return cache
    try:
        with open(OUTPUT_PATH) as f:
            raw = json.load(f)
        players = raw.get("players", raw) if isinstance(raw, dict) else raw
        prev_generated_at = raw.get("generated_at") if isinstance(raw, dict) else None
        for p in players:
            if p.get("game_status") != "Live":
                continue
            if p.get("level") != "NCAA":
                continue
            if p.get("game_date") != today_str:
                continue
            if not _entry_has_real_stats(p):
                continue
            p["_prev_generated_at"] = prev_generated_at
            key = f"{p['player_name']}|{p['team']}"
            cache.setdefault(key, []).append(p)
        if cache:
            logger.info("Live stats cache: %d NCAA players with Live stats preserved", len(cache))
    except Exception:
        logger.warning("Failed to load live stats cache")
    return cache


def run_live():
    """Full pipeline: fetch roster + recruits -> fetch stats -> grade -> alert -> write JSON."""
    logger.info("Starting live pulse run")

    # Warn early if Slack webhook is missing (alerts will be silently skipped)
    from src.alerts import SLACK_WEBHOOK_URL
    if not SLACK_WEBHOOK_URL or "YOUR_WEBHOOK" in SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not configured — alerts will be skipped this run")

    _rotate_yesterday()
    reset_sent_alerts()

    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    # HS sheet refresh: download sheet and discover HS players not yet in roster
    try:
        from src.hs_stats import HSSheetParser, HSGameLog
        hs_parser = HSSheetParser()
        hs_parsed = hs_parser.parse_all()
        if hs_parsed:
            hs_log = HSGameLog()
            hs_log.update_from_sheet(hs_parsed)
            # Add HS players from sheet that aren't already in the roster
            roster_names = {p["player_name"] for p in all_players}
            sheet_names = hs_parser.get_all_player_names()
            new_hs = sheet_names - roster_names
            for name in sorted(new_hs):
                pos = hs_parser.get_position_for_player(name)
                all_players.append({
                    "player_name": name,
                    "team": "HS",
                    "level": "HS",
                    "position": pos,
                    "mlb_id": None,
                    "roster_priority": 99,
                    "draft_class": "",
                    "is_client": False,
                })
            if new_hs:
                logger.info("Discovered %d HS players from sheet: %s", len(new_hs), sorted(new_hs))
    except Exception:
        logger.exception("HS sheet refresh failed in run_live — continuing without HS data")

    clients = [p for p in all_players if p.get("is_client")]
    recruits = [p for p in all_players if not p.get("is_client")]
    logger.info("Loaded %d clients + %d recruits", len(clients), len(recruits))

    today_str = _today_et().isoformat()
    locked_finals = _load_locked_finals(today_str)
    live_stats_cache = _load_live_stats_cache(today_str)

    fetcher = StatsFetcher()
    analyzer = PerformanceAnalyzer()

    def _process_player(player):
        """Process a single player — safe for concurrent execution.

        Returns (entries_list, alert_data_list) to support doubleheaders.
        """
        name = player["player_name"]
        team = player["team"]
        is_client = player.get("is_client", True)

        # Stats lock: if we already captured Final stats for this NCAA
        # player today, carry them forward instead of re-fetching.
        # This prevents stale/reused live links from corrupting data.
        lock_key = f"{name}|{team}"
        locked = locked_finals.get(lock_key)
        if locked:
            locked_game_nums = {e.get("game_number", 1) for e in locked}
            logger.info("%s | %d locked Final game(s) (games %s) — carrying forward",
                        name, len(locked), locked_game_nums)
            return locked, []

        try:
            all_stats = None
            for attempt in range(2):
                try:
                    all_stats = fetcher.fetch_all(player)
                    break
                except Exception:
                    if attempt == 0:
                        time.sleep(1)
                    else:
                        raise
            if not all_stats:
                return [], []
            entries = []
            alert_data_list = []
            for stats in all_stats:
                stats = _sanitize_stats(stats)
                _append_to_ncaa_game_log(player, stats)
                analysis = analyzer.analyze(player, stats)
                entry = build_pulse_entry(player, stats, analysis)

                gn_label = f" Gm {stats.get('game_number', '')}" if stats.get("game_number") else ""
                logger.info(
                    "%s%s%s | %s | %s",
                    name,
                    "" if is_client else " [following]",
                    gn_label,
                    stats.get("stats_summary", "—"),
                    analysis["performance_grade"],
                )
                entries.append(entry)
                if is_client:
                    # Enrich stats with recent-start count for out-of-lineup alert
                    _summary_lc = (stats.get("stats_summary") or "").lower()
                    _pos = player.get("position", "Hitter")
                    if (_pos in ("Hitter", "Two-Way")
                            and stats.get("game_status") in ("Live", "Final")
                            and ("did not play" in _summary_lc
                                 or "not in lineup" in _summary_lc)):
                        stats["recent_starts"] = _count_recent_starts(
                            player, stats.get("game_date") or "")
                    alert_data_list.append((player, stats, analysis["performance_grade"]))

            # Live stats carry-forward: if every new entry for this NCAA
            # player lacks real stats but the previous run had them,
            # keep the old stats and just update game context/status.
            cached = live_stats_cache.get(lock_key)
            if (cached
                    and player.get("level") == "NCAA"
                    and entries
                    and all(not _entry_has_real_stats(e) for e in entries)
                    and any(_entry_has_real_stats(c) for c in cached)):
                carried = []
                for c in cached:
                    patched = dict(c)
                    # Use the freshest game context from the new fetch
                    fresh = entries[0]
                    if fresh.get("game_context"):
                        patched["game_context"] = fresh["game_context"]
                    if fresh.get("game_status"):
                        patched["game_status"] = fresh["game_status"]
                    # Stamp stats_captured_at so the dashboard can render
                    # "as of HH:MM". Prefer an existing value (chained carry-forward)
                    # over the previous run's generated_at (first carry).
                    if not patched.get("stats_captured_at"):
                        patched["stats_captured_at"] = patched.pop("_prev_generated_at", None)
                    else:
                        patched.pop("_prev_generated_at", None)
                    # Keep the new fetch's diagnostic so the trace shows
                    # *this* run's failure, not the cached run's success.
                    if fresh.get("fetch_diagnostic") is not None:
                        patched["fetch_diagnostic"] = fresh["fetch_diagnostic"]
                    carried.append(patched)
                logger.info(
                    "%s | Live stats carry-forward: kept previous %s (new fetch had no stats via %s)",
                    name,
                    cached[0].get("stats_summary", "?"),
                    entries[0].get("data_source", "?"),
                )
                return carried, []

            return entries, alert_data_list
        except Exception:
            logger.exception("Failed to process %s — skipping", name)
            return [], []

    pulse = []
    alert_queue = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_process_player, p): p for p in all_players}
        for future in as_completed(futures):
            entries, alert_data_list = future.result()
            if entries:
                pulse.extend(entries)
            if alert_data_list:
                alert_queue.extend(alert_data_list)

    # Deduplicate alert queue before sending.
    # False doubleheader detection can produce 2 entries for the same player+game
    # with different game_numbers, causing duplicate alerts.  Drop any entry whose
    # (player, game_date, stats fingerprint) was already seen in this run.
    seen_alert_fingerprints: set = set()
    deduped_alert_queue = []
    for _ap, _as, _ag in alert_queue:
        _fp = (
            _ap.get("player_name", ""),
            _as.get("game_date", ""),
            _as.get("home_runs", 0),
            _as.get("hits", 0),
            _as.get("at_bats", 0),
            _as.get("ip", 0.0),
        )
        if _fp not in seen_alert_fingerprints:
            seen_alert_fingerprints.add(_fp)
            deduped_alert_queue.append((_ap, _as, _ag))

    # Send alerts serially to avoid Slack rate limits
    for player, stats, grade in deduped_alert_queue:
        check_and_send_alerts(player, stats, grade=grade)
    save_sent_alerts()  # Persist all alert state in one write

    _flush_ncaa_game_log()

    # Deduplicate pulse entries.  Concurrent fetches + stats lock can
    # produce duplicate entries for the same player+game_number.  Keep the
    # first (which is typically the locked/carried-forward entry).
    seen_pulse_keys: set = set()
    deduped_pulse: list = []
    for p in pulse:
        pk = (p.get("player_name", ""), p.get("team", ""), p.get("game_number") or 0)
        if pk not in seen_pulse_keys:
            seen_pulse_keys.add(pk)
            deduped_pulse.append(p)
    if len(deduped_pulse) < len(pulse):
        logger.info("Pulse dedup: removed %d duplicate entries", len(pulse) - len(deduped_pulse))
    pulse = deduped_pulse

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

    # Summer-ball cards. Reads data/summer_ball_rosters.json (built 4x/day
    # by the summer_rosters workflow) and emits a card per matched player
    # with today's/yesterday's MLB-Stats-API summer-league game state.
    # Failures are non-fatal — the rest of the pulse should still ship.
    summer_entries: list = []
    try:
        from src.summer_pulse import build_summer_pulse_entries
        summer_entries = build_summer_pulse_entries()
        if summer_entries:
            logger.info("Appended %d summer-ball entries to pulse", len(summer_entries))
            today_pulse.extend(summer_entries)
    except Exception:
        logger.exception("summer_pulse: build failed, continuing without summer cards")

    write_output(today_pulse)
    _supplement_yesterday(pulse)

    # _supplement_yesterday rewrites yesterday_pulse.json from the current
    # pulse's Final entries (Pro/NCAA/HS only — it doesn't know about
    # Summer). Re-merge Summer is_yesterday cards back in here so the
    # Yesterday tab surfaces them. Without this, summer_pulse's earlier
    # write gets clobbered.
    if summer_entries:
        try:
            from src.summer_pulse import _merge_summer_into_yesterday_pulse
            _merge_summer_into_yesterday_pulse(summer_entries)
        except Exception:
            logger.exception("summer_pulse: post-supplement yesterday-merge failed")

    _fetch_yesterday_pass(all_players, fetcher, analyzer)
    _flush_ncaa_game_log()

    # Quick NCAA L7 refresh — game log was just updated, so re-aggregate
    # the NCAA portion of L7 window stats to include today's Final games.
    _refresh_ncaa_l7(all_players)

    # _refresh_ncaa_l7 rewrites window_7d.json from Pro+NCAA+HS only —
    # same clobber pattern as _supplement_yesterday. Re-merge Summer
    # window entries here so the 7 Days tab keeps them.
    if summer_entries:
        try:
            from src.summer_pulse import _load_placements
            from src.summer_pulse import _write_summer_window_entries
            placements = _load_placements()
            # auto_by_name not needed for non-MLB leagues; pass empty since
            # the rebuild only needs placement data for MLB-API queries
            # which already happened above. Falls back to BBRef/static for
            # other leagues — same as the first call.
            _write_summer_window_entries(placements, {})
        except Exception:
            logger.exception("summer_pulse: post-l7 window-merge failed")


def _refresh_ncaa_l7(all_players: list[dict]):
    """Re-aggregate NCAA + HS L7 window stats from the freshly-flushed game logs.

    Only touches NCAA/HS players and reads local game logs — no API calls.
    Pro entries are preserved from the last full historical run.
    """
    try:
        aggregator = WindowStatsAggregator()
        today = _today_et()
        start_7d = today - timedelta(days=7)

        # Load existing window data to preserve Pro entries
        existing = []
        if os.path.exists(WINDOW_7D_PATH):
            with open(WINDOW_7D_PATH) as f:
                raw = json.load(f)
                existing = raw.get("players", raw) if isinstance(raw, dict) else raw

        pro_entries = [e for e in existing if e.get("level") == "Pro"]

        # Re-aggregate NCAA entries from the fresh game log
        ncaa_entries = []
        for player in all_players:
            if player.get("level") != "NCAA":
                continue
            entry = aggregator._build_window_entry(player, "7d", start_7d, today)
            if entry:
                ncaa_entries.append(entry)

        # Re-aggregate HS entries from the fresh game log
        hs_entries = []
        for player in all_players:
            if player.get("level") != "HS":
                continue
            entry = aggregator._build_window_entry(player, "7d", start_7d, today)
            if entry:
                hs_entries.append(entry)

        write_window_json(pro_entries + ncaa_entries + hs_entries, WINDOW_7D_PATH)
        logger.info("L7 refresh: %d NCAA + %d HS entries updated", len(ncaa_entries), len(hs_entries))
    except Exception:
        logger.exception("NCAA L7 refresh failed — window_7d.json unchanged")


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


def _stable_sort_key(p: dict) -> tuple:
    """Return a sort key that is consistent across runs.

    ThreadPoolExecutor + as_completed produces non-deterministic order.
    If two concurrent runs write the same players in different order, git's
    line-level merge can include entries from BOTH runs (duplicate cards).
    Sorting by a stable key ensures identical line positions across runs,
    so git sees real conflicts and ``-X ours`` picks one version cleanly.
    """
    return (
        p.get("tags", {}).get("roster_priority", 99),
        p.get("player_name", ""),
        p.get("team", ""),
        p.get("game_number") or 0,
    )


_BLOCKED_OUTCOME_RE = re.compile(r"block|403|waf", re.IGNORECASE)


def _summarize_run_health(pulse: list[dict]) -> dict:
    """Aggregate per-player fetch_diagnostic into a run-level health summary.

    Returns a dict with counts of blocked / stale / no-data outcomes per
    source, plus client-only counts for higher-priority signal. The banner
    in the dashboard reads this; the rolling history page reads the same.
    """
    by_source: dict[str, dict[str, int]] = {}
    blocked_clients: list[str] = []
    carry_forward_clients: list[str] = []
    fallback_clients: list[str] = []
    total_clients = 0
    # A "today game" is a client whose game record is from a current run (not
    # the yesterday-rollover bucket) AND is either in progress or already
    # finalized. We use this to gate fallback-based warnings: at 2am with no
    # games anywhere, a high fallback ratio is structural, not a failure.
    clients_with_today_game = 0

    for p in pulse:
        is_client = bool(p.get("is_client"))
        if is_client:
            total_clients += 1
            if not p.get("is_yesterday") and p.get("game_status") in ("Live", "Final"):
                clients_with_today_game += 1
            if p.get("stats_captured_at"):
                carry_forward_clients.append(p.get("player_name", "?"))
            summary = (p.get("stats_summary") or "").lower()
            if (("not in lineup" in summary or "hasn't pitched" in summary
                    or summary == "game in progress" or summary.startswith("did not play"))
                    and not p.get("stats_captured_at")):
                fallback_clients.append(p.get("player_name", "?"))

        diag = p.get("fetch_diagnostic")
        if not diag:
            continue
        seen_blocked_for_player = False
        for entry in diag:
            src = entry.get("source") or "?"
            outcome = entry.get("outcome") or "?"
            bucket = by_source.setdefault(src, {})
            bucket[outcome] = bucket.get(outcome, 0) + 1
            if not seen_blocked_for_player and _BLOCKED_OUTCOME_RE.search(outcome):
                seen_blocked_for_player = True
                if is_client:
                    blocked_clients.append(p.get("player_name", "?"))

    blocked_sources = sorted({
        src for src, outcomes in by_source.items()
        if any(_BLOCKED_OUTCOME_RE.search(o) for o in outcomes)
    })
    blocked_total = sum(
        n for outcomes in by_source.values()
        for o, n in outcomes.items() if _BLOCKED_OUTCOME_RE.search(o)
    )

    severity = "ok"
    if blocked_clients:
        severity = "warning" if len(blocked_clients) < 5 else "critical"
    elif (fallback_clients and total_clients
            and len(fallback_clients) >= max(3, total_clients // 5)
            and clients_with_today_game > 0):
        # Only escalate fallback-based warnings when there are actually games
        # happening today. Overnight, every roster player without a scheduled
        # game looks like "fallback" and floods the dashboard with structural
        # noise.
        severity = "warning"

    # Per-proxy block stats from the SB residential pool — surfaces which
    # provider (Webshare vs IPRoyal) carried the run vs got blocked.  Lets
    # diagnostics.html show which residential proxy is healthy at a glance.
    try:
        from src.stats_engine import get_sb_proxy_stats
        proxy_pool = get_sb_proxy_stats()
    except Exception:
        proxy_pool = {}

    return {
        "severity": severity,
        "by_source": by_source,
        "blocked_sources": blocked_sources,
        "blocked_event_count": blocked_total,
        "blocked_clients": blocked_clients,
        "carry_forward_clients": carry_forward_clients,
        "fallback_clients": fallback_clients,
        "total_clients": total_clients,
        "proxy_pool": proxy_pool,
    }


_HEALTH_HISTORY_PATH = os.path.join(
    os.path.dirname(__file__), "data", "fetch_health_history.json"
)
_HEALTH_HISTORY_MAX_HOURS = 168  # 7 days — enough to look back on a Wed from a Mon


def _append_health_history(generated_at: str, health: dict) -> None:
    """Persist a slim run-level health snapshot for the diagnostics page."""
    try:
        history = []
        if os.path.exists(_HEALTH_HISTORY_PATH):
            with open(_HEALTH_HISTORY_PATH) as f:
                history = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=_HEALTH_HISTORY_MAX_HOURS)
        history = [
            h for h in history
            if h.get("generated_at") and datetime.fromisoformat(h["generated_at"]) >= cutoff
        ]
        history.append({
            "generated_at": generated_at,
            "severity": health.get("severity"),
            "blocked_sources": health.get("blocked_sources", []),
            "blocked_event_count": health.get("blocked_event_count", 0),
            "blocked_clients": health.get("blocked_clients", []),
            "carry_forward_clients": health.get("carry_forward_clients", []),
            "fallback_clients": health.get("fallback_clients", []),
            "total_clients": health.get("total_clients", 0),
            "by_source": health.get("by_source", {}),
            "proxy_pool": health.get("proxy_pool", {}),
        })
        _atomic_json_write(_HEALTH_HISTORY_PATH, history, indent=2, ensure_ascii=False)
    except Exception:
        logger.warning("Failed to append fetch health history — non-fatal")


_PLAYER_HEALTH_RETENTION_DAYS = 90
_BLOCKED_OUTCOME_RE = re.compile(r"block|403|waf", re.IGNORECASE)


def _update_player_health_history(pulse: list[dict]) -> None:
    """Snapshot per-player live-stats coverage for today (ET).

    Writes one entry per ET date; subsequent runs the same day overwrite the
    entry, so the last run of the day is the persisted snapshot. Capped at
    `_PLAYER_HEALTH_RETENTION_DAYS` rolling days. Failures are non-fatal.
    """
    try:
        et_now = datetime.now(ZoneInfo("America/New_York"))
        today_et = et_now.strftime("%Y-%m-%d")

        snapshot_players = []
        for p in pulse:
            game_status = (p.get("game_status") or "").strip()
            # Only count players who had a game today. "N/A" or empty = no game.
            if game_status in ("", "N/A"):
                continue
            data_source = (p.get("data_source") or "").strip()
            fd = p.get("fetch_diagnostic") or []
            outcomes = [(d.get("outcome") or "") for d in fd]
            sources_tried = sorted({(d.get("source") or "") for d in fd if d.get("source")})
            blocked = any(_BLOCKED_OUTCOME_RE.search(o) for o in outcomes)
            captured = bool(data_source) and not blocked
            snapshot_players.append({
                "name": p.get("player_name"),
                "team": p.get("team"),
                "level": p.get("level"),
                "tier": p.get("roster_priority"),
                "game_status": game_status,
                "captured": captured,
                "blocked": blocked,
                "source": data_source,
                "sources_tried": sources_tried,
            })

        snapshot = {
            "date": today_et,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "players": snapshot_players,
        }

        history = []
        if os.path.exists(PLAYER_HEALTH_HISTORY_PATH):
            try:
                with open(PLAYER_HEALTH_HISTORY_PATH) as f:
                    history = json.load(f)
            except Exception:
                history = []

        # Replace today's row if it exists, then cap retention.
        history = [h for h in history if h.get("date") != today_et]
        cutoff = (et_now.date() - timedelta(days=_PLAYER_HEALTH_RETENTION_DAYS)).isoformat()
        history = [h for h in history if (h.get("date") or "") >= cutoff]
        history.append(snapshot)
        history.sort(key=lambda h: h.get("date") or "")

        _atomic_json_write(PLAYER_HEALTH_HISTORY_PATH, history, indent=2, ensure_ascii=False)
    except Exception:
        logger.warning("Failed to update player health history — non-fatal")


def write_output(pulse: list[dict]):
    """Write the pulse list to data/current_pulse.json with generated_at envelope."""
    # Stable sort so concurrent runs produce identical line positions (see _stable_sort_key).
    pulse = sorted(pulse, key=_stable_sort_key)
    generated_at = datetime.now(timezone.utc).isoformat()
    health = _summarize_run_health(pulse)
    envelope = {
        "generated_at": generated_at,
        "players": pulse,
        "health": health,
    }
    _atomic_json_write(OUTPUT_PATH, envelope, indent=2, ensure_ascii=False)
    _append_health_history(generated_at, health)
    _update_player_health_history(pulse)
    if health["severity"] != "ok":
        logger.warning(
            "Run health: %s — blocked sources=%s, blocked clients=%d, fallback clients=%d",
            health["severity"], health["blocked_sources"],
            len(health["blocked_clients"]), len(health["fallback_clients"]),
        )
    logger.info("Wrote %d entries to %s", len(pulse), OUTPUT_PATH)


def _read_yesterday_capture_state() -> dict:
    """Snapshot which yesterday-game client entries have real stats.

    Returns {dedup_key: bool_captured}. Used to diff before/after backfill so
    we can post a Slack summary of which players were rescued from a previous
    Cloudflare/StatBroadcast block.
    """
    state: dict = {}
    if not os.path.exists(YESTERDAY_PULSE_PATH):
        return state
    try:
        with open(YESTERDAY_PULSE_PATH) as f:
            data = json.load(f)
        for p in data.get("players", []):
            if not p.get("is_client"):
                continue
            key = (p.get("player_name", "?"), p.get("game_number") or 0)
            state[key] = {
                "captured": _entry_has_real_stats(p),
                "team": p.get("team", "?"),
            }
    except Exception:
        logger.warning("Failed to read yesterday_pulse for capture snapshot")
    return state


def _slack_backfill_summary(pre: dict, post: dict) -> None:
    """Slack a one-line summary of what the overnight backfill rescued."""
    rescued = []
    for key, post_entry in post.items():
        pre_entry = pre.get(key)
        if not post_entry["captured"]:
            continue
        if pre_entry is None or not pre_entry["captured"]:
            rescued.append((key[0], post_entry["team"]))

    if not rescued:
        logger.info("Overnight backfill: nothing new rescued")
        return

    from src.alerts import send_slack_message
    sample = ", ".join(f"{n} ({t})" for n, t in rescued[:5])
    extra = f" +{len(rescued) - 5}" if len(rescued) > 5 else ""
    send_slack_message(
        f"🌙 Overnight backfill rescued *{len(rescued)}* client game(s): {sample}{extra}"
    )


_STUCK_CLIENT_DAYS = 3


def _check_stuck_clients(stuck_days: int = _STUCK_CLIENT_DAYS) -> None:
    """Slack-ping clients who've gone N days with no successful capture.

    Reads ``player_health_history.json`` (which only contains players with
    a non-N/A game_status, so absence from a date already means "no game
    that day"). For each client name, looks at the last *stuck_days* daily
    snapshots: if the player had games on at least 2 of those days and was
    never captured, they're stuck.

    Dedupe is per-ET-day via the existing sent_alerts mechanism, so you'll
    see at most one ping per stuck player per day. If they recover, the
    ping goes away naturally.
    """
    try:
        if not os.path.exists(PLAYER_HEALTH_HISTORY_PATH):
            return
        with open(PLAYER_HEALTH_HISTORY_PATH) as f:
            history = json.load(f)
    except Exception:
        logger.debug("Could not read player health history for stuck-client check")
        return

    if not history or len(history) < 2:
        return

    history.sort(key=lambda h: h.get("date") or "")
    recent = history[-stuck_days:]

    by_player: dict[str, list[tuple[str, dict]]] = {}
    for snap in recent:
        for p in snap.get("players", []):
            name = p.get("name")
            if not name:
                continue
            by_player.setdefault(name, []).append((snap.get("date"), p))

    today_et = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    from src.alerts import _already_sent, _mark_sent, save_sent_alerts, send_slack_message

    sent_count = 0
    for name, entries in by_player.items():
        # Require at least 2 game-days to call it "stuck" — a single missed
        # game is just one bad fetch, not a pattern.
        if len(entries) < 2:
            continue
        if any(p.get("captured") for _, p in entries):
            continue
        latest = entries[-1][1]
        # Only ping for clients (roster_priority < 99 — recruits sit at 99).
        tier = latest.get("tier")
        if tier is None or tier >= 99:
            continue
        if _already_sent(today_et, name, "stuck"):
            continue

        # Walk older history (before the stuck window) to find last capture date.
        last_seen = "—"
        for snap in history[:-stuck_days][::-1]:
            for p in snap.get("players", []):
                if p.get("name") == name and p.get("captured"):
                    last_seen = snap.get("date") or "—"
                    break
            if last_seen != "—":
                break

        team = latest.get("team", "?")
        sources_tried = latest.get("sources_tried") or []
        sources_str = f" Tried: {', '.join(sources_tried)}." if sources_tried else ""
        ok = send_slack_message(
            f"🔁 *{name}* ({team}) — stuck {len(entries)}/{stuck_days} days with no capture. "
            f"Last successful: {last_seen}.{sources_str}"
        )
        if ok:
            _mark_sent(today_et, name, "stuck")
            sent_count += 1

    if sent_count:
        save_sent_alerts()
        logger.info("Pinged %d stuck client(s)", sent_count)


def run_backfill():
    """Lean overnight pass to recover yesterday's blocked entries.

    Runs in the post-game dead zone (~2:45 AM – 10 AM ET) when StatBroadcast's
    Cloudflare WAF is least loaded — previously-blocked Final box scores have
    their best shot at coming through. Skips all today-game work, live alerts,
    and historical aggregation; only refreshes yesterday and the game log.
    """
    logger.info("Starting overnight backfill run")

    _rotate_yesterday()

    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    # HS sheet refresh — keeps the HS game log in step with the sheet so the
    # L7 recompute below sees fresh manual entries.
    try:
        from src.hs_stats import HSSheetParser, HSGameLog
        hs_parser = HSSheetParser()
        hs_parsed = hs_parser.parse_all()
        if hs_parsed:
            hs_log = HSGameLog()
            hs_log.update_from_sheet(hs_parsed)
    except Exception:
        logger.exception("HS sheet refresh failed in run_backfill — continuing")

    pre_state = _read_yesterday_capture_state()
    pre_uncaptured = sum(1 for v in pre_state.values() if not v["captured"])
    logger.info(
        "Backfill: %d client entries already captured, %d still uncaptured before retry",
        sum(1 for v in pre_state.values() if v["captured"]),
        pre_uncaptured,
    )

    fetcher = StatsFetcher()
    analyzer = PerformanceAnalyzer()
    _fetch_yesterday_pass(all_players, fetcher, analyzer)
    _flush_ncaa_game_log()
    _refresh_ncaa_l7(all_players)

    post_state = _read_yesterday_capture_state()
    _slack_backfill_summary(pre_state, post_state)

    # After the night's rescue attempts, surface clients who've been stuck
    # multiple days so they don't rot unseen. One ping per stuck player per
    # ET day; auto-clears once they recover.
    _check_stuck_clients()


def run_historical():
    """Aggregate historical stats: 7D (Pro + NCAA + HS) + Season (everyone)."""
    logger.info("Starting historical stats aggregation")

    _rotate_yesterday()

    all_players = get_all_players()
    if not all_players:
        logger.error("No players found — aborting")
        sys.exit(1)

    # HS sheet refresh before aggregation
    try:
        from src.hs_stats import HSSheetParser, HSGameLog
        hs_parser = HSSheetParser()
        hs_parsed = hs_parser.parse_all()
        if hs_parsed:
            hs_log = HSGameLog()
            hs_log.update_from_sheet(hs_parsed)
            # Discover HS players from sheet not in roster
            roster_names = {p["player_name"] for p in all_players}
            sheet_names = hs_parser.get_all_player_names()
            new_hs = sheet_names - roster_names
            for name in sorted(new_hs):
                pos = hs_parser.get_position_for_player(name)
                all_players.append({
                    "player_name": name,
                    "team": "HS",
                    "level": "HS",
                    "position": pos,
                    "mlb_id": None,
                    "roster_priority": 99,
                    "draft_class": "",
                    "is_client": False,
                })
            if new_hs:
                logger.info("Discovered %d HS players from sheet: %s", len(new_hs), sorted(new_hs))
    except Exception:
        logger.exception("HS sheet refresh failed in run_historical — continuing without HS data")

    logger.info("Loaded %d total players for historical aggregation", len(all_players))

    aggregator = WindowStatsAggregator()
    window_data = aggregator.run_all_windows(all_players)

    write_window_json(window_data["7d"], WINDOW_7D_PATH)

    # Preserve existing season data for NCAA players where D1B returned 403/empty.
    # D1Baseball rate-limits aggressively, so we don't want to wipe good data on a
    # transient failure.  We explicitly scope this to NCAA — for Pro players the
    # MLB Stats API is reliable, so an empty response means the player genuinely
    # has no games (e.g. a prospect who hasn't debuted yet), and we should let
    # the fresh empty entry replace the stale one instead of propping it up.
    season_data = window_data["season"]
    if os.path.exists(WINDOW_SEASON_PATH):
        try:
            with open(WINDOW_SEASON_PATH) as f:
                raw = json.load(f)
                existing = raw.get("players", raw) if isinstance(raw, dict) else raw
            existing_by_name = {e["player_name"]: e for e in existing}
            for i, entry in enumerate(season_data):
                if entry.get("level") != "NCAA":
                    continue
                stats = entry.get("stats", {})
                has_data = any(v != "--" for v in stats.values())
                if not has_data and entry["player_name"] in existing_by_name:
                    prev = existing_by_name[entry["player_name"]]
                    prev_has_data = any(v != "--" for v in prev.get("stats", {}).values())
                    if prev_has_data:
                        season_data[i] = prev
                        logger.info("Season: preserved existing data for %s (D1B fetch failed)", entry["player_name"])
        except Exception:
            logger.debug("Could not load existing season data for preservation")

    write_window_json(season_data, WINDOW_SEASON_PATH)

    logger.info(
        "Historical aggregation complete: 7D=%d, Season=%d",
        len(window_data["7d"]),
        len(season_data),
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
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Overnight retry of yesterday's blocked entries — no today-game work",
    )
    args = parser.parse_args()

    if args.mock:
        run_mock()
    elif args.historical:
        run_historical()
    elif args.backfill:
        run_backfill()
    else:
        run_live()


if __name__ == "__main__":
    main()
