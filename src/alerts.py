"""
SV Dugout Pulse — Slack Alerts

Sends notifications to Slack when trigger conditions are met.

Alert state is persisted to data/sent_alerts.json so that alerts are NOT
re-sent across cron runs (each 15-minute run is a separate process).
Keys include the game date, so stale entries auto-expire.
"""

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from src.stats_engine import _is_pitcher_pos
from .config import SENT_ALERTS_PATH

_ET = ZoneInfo("US/Eastern")
_DAY_FLIP_HOUR = 4


def _today_et() -> date:
    """Return today's ET game-day date (flips at 4 AM ET, not midnight)."""
    now = datetime.now(_ET)
    if now.hour < _DAY_FLIP_HOUR:
        return (now - timedelta(days=1)).date()
    return now.date()

logger = logging.getLogger(__name__)

# Webhook URL — MUST be set via environment variable (GitHub secret)
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# Feature flag: pitcher-removed Slack alerts are muted for now. Detection in
# stats_engine.py still populates stats["pitcher_removed"], so flipping this
# back to True re-enables notifications without any other code changes.
ALERT_PITCHER_REMOVED = False

# Persistent alert state loaded from disk: {"date|player:type": value, ...}
_sent_alerts: dict = {}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _load_sent_alerts():
    """Load persistent alert state from disk and prune entries older than yesterday."""
    global _sent_alerts

    if os.path.exists(SENT_ALERTS_PATH):
        try:
            with open(SENT_ALERTS_PATH) as f:
                _sent_alerts = json.load(f)
        except Exception:
            logger.warning("Failed to load sent_alerts.json — starting fresh")
            _sent_alerts = {}
    else:
        _sent_alerts = {}

    # Prune entries older than yesterday in ET game-day terms.
    # Must use ET (not UTC) so that entries from the previous calendar day
    # are not wrongly pruned after midnight UTC (e.g. 9 PM ET = 2 AM UTC next day).
    cutoff = (_today_et() - timedelta(days=1)).isoformat()
    before = len(_sent_alerts)
    _sent_alerts = {
        k: v for k, v in _sent_alerts.items()
        if k.split("|", 1)[0] >= cutoff
    }
    pruned = before - len(_sent_alerts)
    if pruned:
        logger.info("Pruned %d stale alert entries", pruned)
        _save_sent_alerts()

    logger.info("Loaded %d active alert entries", len(_sent_alerts))


def _save_sent_alerts():
    """Persist sent alerts to disk."""
    os.makedirs(os.path.dirname(SENT_ALERTS_PATH), exist_ok=True)
    with open(SENT_ALERTS_PATH, "w") as f:
        json.dump(_sent_alerts, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Dedup helpers
# ---------------------------------------------------------------------------

def _alert_key(game_date: str, player_name: str, alert_type: str,
               game_number: int = 0) -> str:
    """Key format: 'YYYY-MM-DD|PlayerName:type[:gmN]' — date prefix enables auto-pruning."""
    base = f"{game_date}|{player_name}:{alert_type}"
    if game_number:
        base += f":gm{game_number}"
    return base


_COOLDOWN_MINUTES = 20  # Suppress identical alerts within this window


def _already_sent(game_date: str, player_name: str, alert_type: str,
                  current_value=None, game_number: int = 0) -> bool:
    """Check if this alert was already sent (persists across runs).

    For value-aware alerts (like HR count), re-triggers if current_value
    exceeds the previously alerted value.

    Also checks a cooldown guard using the :ts timestamp key written by
    _mark_sent.  If the same alert (ignoring game_number) was sent within
    _COOLDOWN_MINUTES, treat it as already sent — this catches cross-run
    duplicates when a prior run's git push was skipped (rebase conflict).
    """
    key = _alert_key(game_date, player_name, alert_type, game_number)

    # Cooldown guard: check the game_number=0 timestamp key as a catch-all.
    # If ANY game_number variant of this alert was sent recently, skip.
    base_ts_key = _alert_key(game_date, player_name, alert_type, 0) + ":ts"
    ts_str = _sent_alerts.get(base_ts_key) or _sent_alerts.get(key + ":ts")
    if ts_str:
        try:
            sent_at = datetime.fromisoformat(ts_str)
            age_minutes = (datetime.now(ZoneInfo("UTC")) - sent_at).total_seconds() / 60
            if age_minutes < _COOLDOWN_MINUTES:
                return True
        except Exception:
            pass

    if key not in _sent_alerts:
        return False
    # Value-aware check: re-alert if the stat increased (e.g. 2nd HR)
    if current_value is not None:
        prev = _sent_alerts[key]
        if isinstance(prev, (int, float)):
            return current_value <= prev
    return True


def _mark_sent(game_date: str, player_name: str, alert_type: str,
               value=True, game_number: int = 0):
    """Mark an alert as sent (in-memory). Call save_sent_alerts() to persist.

    Stores the value AND a sent_at timestamp.  The timestamp enables a
    cooldown guard in _already_sent that catches cross-run duplicates even
    when the previous run's sent_alerts.json push was skipped (rebase conflict).
    """
    key = _alert_key(game_date, player_name, alert_type, game_number)
    _sent_alerts[key] = value
    # Also store a timestamp key so the cooldown check can compare wall-clock time
    _sent_alerts[key + ":ts"] = datetime.now(ZoneInfo("UTC")).isoformat()


# ---------------------------------------------------------------------------
# Slack messaging
# ---------------------------------------------------------------------------

def send_slack_message(text: str, blocks: Optional[list] = None) -> bool:
    """Send a message to the configured Slack webhook."""
    if not SLACK_WEBHOOK_URL or "YOUR_WEBHOOK" in SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook not configured — skipping alert")
        return False

    payload = {"text": text}
    if blocks:
        payload["blocks"] = blocks

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("Slack alert sent: %s", text[:80])
            return True
        else:
            logger.error("Slack webhook failed: %s %s", resp.status_code, resp.text)
            return False
    except Exception:
        logger.exception("Failed to send Slack alert")
        return False


# ---------------------------------------------------------------------------
# Alert logic
# ---------------------------------------------------------------------------

def check_and_send_alerts(player: dict, stats: dict, grade: str = ""):
    """
    Check if the player's stats trigger any alert conditions.
    Sends Slack notifications for:
    - Any player hits a home run (re-alerts on additional HRs)
    - Any pitcher enters the game
    - Pitcher strikeouts (value-aware: re-alerts at 5, 8, 10+)
    - Pitcher quality start (6+ IP, 3 or fewer ER)
    - Any hitter reaches base 3+ times
    - Standout game summary when game goes Final

    Only marks alerts as sent when Slack delivery succeeds.
    """
    name = player.get("player_name", "Unknown")
    team = player.get("team", "")
    tier = player.get("roster_priority", 99)
    position = player.get("position", "Hitter")
    game_context = stats.get("game_context", "")
    game_status = stats.get("game_status", "N/A")
    game_date = stats.get("game_date") or date.today().isoformat()
    game_number = stats.get("game_number") or 0
    split_squad = stats.get("split_squad", False)
    summary = stats.get("stats_summary", "")

    # Skip if no game data
    if game_status == "N/A":
        return

    # Skip if main.py's _sanitize_stats flagged this row as a likely
    # season-aggregate masquerading as a single-game line (Kyle Jones / NCAA
    # Regional 2026-05-29: AB=210, hits=64, RBI=37 — fired "MULTI-HR 4 HR!"
    # off season totals). The flag is set when ≥2 per-game caps are busted
    # simultaneously, which is a strong signal that the parser pulled the
    # wrong row.
    if stats.get("_implausible"):
        logger.warning(
            "Skipping alerts for %s — stats line flagged implausible (%s)",
            name, stats.get("_implausible_reason", "?"),
        )
        return

    # Clear sticky out-of-lineup flag when a player returns to the lineup.
    # This ensures we alert again if they go out a second time later.
    _ool_key = f"ool_active|{name}"
    _sum_lc = (summary or "").lower()
    if (game_status in ("Live", "Final")
            and "did not play" not in _sum_lc
            and "not in lineup" not in _sum_lc
            and _ool_key in _sent_alerts):
        del _sent_alerts[_ool_key]
        logger.info("Cleared out-of-lineup flag for %s (back in lineup)", name)

    tier_label = f"T{tier}" if tier <= 4 else "T?"
    gm_label = " (SS)" if split_squad else (f" (Gm {game_number})" if game_number else "")
    box_url = stats.get("box_score_url", "")
    box_link = f"\n<{box_url}|Box Score>" if box_url else ""

    # --- Alert: Home Run (any player, any tier) ---
    # Value-aware: re-alerts if HR count increases (e.g. 1→2)
    try:
        hr = int(stats.get("home_runs", 0))
    except (ValueError, TypeError):
        hr = 0
    if hr > 0 and not _already_sent(game_date, name, "hr", current_value=hr, game_number=game_number):
        if hr >= 2:
            emoji = "💣"
            hr_text = f"MULTI-HR GAME — {hr} HRs"
        else:
            emoji = "⚾"
            hr_text = "a HR"
        if send_slack_message(
            f"{emoji} *{name}* ({tier_label}) just hit {hr_text}{gm_label}!\n"
            f"_{team}_ — {summary} — {game_context}{box_link}"
        ):
            _mark_sent(game_date, name, "hr", value=hr, game_number=game_number)

    # --- Alert: Pitcher enters game (any pitcher, any tier) ---
    is_pitching = stats.get("is_pitcher_line", False) or _is_pitcher_pos(position)
    try:
        ip = float(stats.get("ip", 0.0))
    except (ValueError, TypeError):
        ip = 0.0

    if is_pitching and ip > 0 and not _already_sent(game_date, name, "entered", game_number=game_number):
        if send_slack_message(
            f"🔥 *{name}* ({tier_label}) is pitching{gm_label}!\n"
            f"_{team}_ — {summary} — {game_context}{box_link}"
        ):
            _mark_sent(game_date, name, "entered", game_number=game_number)

    # --- Alert: Pitcher removed from game ---
    # Muted via ALERT_PITCHER_REMOVED. Detection still runs upstream — flip
    # the flag to re-enable. Fires once per game when another pitcher enters
    # after ours. Gated on the "entered" alert having already fired so we
    # don't retroactively page on games the cron never observed live.
    entered_key = _alert_key(game_date, name, "entered", game_number)
    if (ALERT_PITCHER_REMOVED
            and is_pitching and stats.get("pitcher_removed")
            and entered_key in _sent_alerts
            and not _already_sent(game_date, name, "pitcher_removed", game_number=game_number)):
        if send_slack_message(
            f"*{name}* ({tier_label}) has been taken out of the game{gm_label}\n"
            f"_{team}_ — {summary} — {game_context}{box_link}"
        ):
            _mark_sent(game_date, name, "pitcher_removed", game_number=game_number)

    # --- Alert: Pitcher strikeouts (value-aware: re-alerts at 5, 8, 10) ---
    try:
        strikeouts = int(stats.get("strikeouts", 0))
    except (ValueError, TypeError):
        strikeouts = 0
    if is_pitching and strikeouts >= 5:
        if not _already_sent(game_date, name, "ks", current_value=strikeouts, game_number=game_number):
            if strikeouts >= 10:
                k_emoji = "🔥🎯"
                k_label = f"{strikeouts} K's — DOUBLE DIGITS"
            elif strikeouts >= 8:
                k_emoji = "💪🎯"
                k_label = f"{strikeouts} K's — dominant"
            else:
                k_emoji = "🎯"
                k_label = f"{strikeouts} K's"
            if send_slack_message(
                f"{k_emoji} *{name}* ({tier_label}) has {k_label}{gm_label}!\n"
                f"_{team}_ — {summary} — {game_context}{box_link}"
            ):
                _mark_sent(game_date, name, "ks", value=strikeouts, game_number=game_number)

    # --- Alert: Pitcher quality start (6+ IP, ≤3 ER) ---
    if is_pitching and stats.get("quality_start"):
        if not _already_sent(game_date, name, "qs", game_number=game_number):
            if send_slack_message(
                f"⭐ *{name}* ({tier_label}) — strong outing (6+ IP, 3 or fewer runs){gm_label}!\n"
                f"_{team}_ — {summary} — {game_context}{box_link}"
            ):
                _mark_sent(game_date, name, "qs", game_number=game_number)

    # --- Alert: Hitter reaches base 3+ times (all tiers) ---
    if position in ("Hitter", "Two-Way") and not stats.get("is_pitcher_line"):
        try:
            hits = int(stats.get("hits", 0))
        except (ValueError, TypeError):
            hits = 0
        try:
            walks = int(stats.get("walks", 0))
        except (ValueError, TypeError):
            walks = 0
        try:
            hbp = int(stats.get("hit_by_pitch", 0))
        except (ValueError, TypeError):
            hbp = 0
        times_on_base = hits + walks + hbp

        if times_on_base >= 3 or hits >= 3:
            if not _already_sent(game_date, name, "3ob", game_number=game_number):
                tob_count = max(times_on_base, hits)
                if send_slack_message(
                    f"💪 *{name}* ({tier_label}) has reached base "
                    f"{tob_count}+ times{gm_label}!\n"
                    f"_{team}_ — {summary} — {game_context}{box_link}"
                ):
                    _mark_sent(game_date, name, "3ob", game_number=game_number)

    # --- Alert: Position player pulled from game ---
    is_hitter = position in ("Hitter", "Two-Way") and not stats.get("is_pitcher_line")
    if is_hitter and "(pulled)" in summary:
        if not _already_sent(game_date, name, "pulled", game_number=game_number):
            if send_slack_message(
                f"*{name}* ({tier_label}) has been taken out of the game{gm_label}\n"
                f"_{team}_ — {summary} — {game_context}{box_link}"
            ):
                _mark_sent(game_date, name, "pulled", game_number=game_number)

    # --- Alert: Regular starter out of lineup / pregame scratch ---
    # Player appeared in 4+ of the last 7 games but is suddenly DNP today.
    # Tightened threshold from 3 to 4 (less false-positive) and only fires on
    # Final (so a day-off in a doubleheader doesn't double-alert). Sticky
    # until player returns to lineup.
    _REGULAR_STARTER_MIN = 4
    if is_hitter and game_status == "Final":
        summary_lower = (summary or "").lower()
        is_out = "did not play" in summary_lower or "not in lineup" in summary_lower
        recent_starts = stats.get("recent_starts", 0)
        if (is_out and recent_starts >= _REGULAR_STARTER_MIN
                and _ool_key not in _sent_alerts):
            if send_slack_message(
                f"👀 *{name}* ({tier_label}) is out of the lineup{gm_label}\n"
                f"_{team}_ — started {recent_starts} of last 7 games — "
                f"{game_context}{box_link}"
            ):
                _sent_alerts[_ool_key] = True  # sticky until player returns

    # --- Alert: Career first (hit / HR / start / save) ---
    _check_career_firsts(player, stats, name, team, tier_label, gm_label,
                         game_date, game_number, game_context, box_link)

    # --- Alert: Hot or cold streak (rolling window) ---
    if game_status == "Final":
        _check_streak(player, stats, name, team, tier_label, gm_label,
                      game_date, game_number, game_context, box_link)

    # --- Alert: Homecoming (player in their home state) ---
    if game_status in ("Scheduled", "Live", "Final"):
        _check_homecoming(player, stats, name, team, tier_label, gm_label,
                          game_date, game_number, game_context, box_link)

    # --- Alert: Standout game summary (when game goes Final) ---
    if game_status == "Final" and "Standout" in grade:
        if not _already_sent(game_date, name, "standout_recap", game_number=game_number):
            if send_slack_message(
                f"🌟 *{name}* ({tier_label}) — Standout performance{gm_label}!\n"
                f"_{team}_ — {summary} — {game_context}{box_link}"
            ):
                _mark_sent(game_date, name, "standout_recap", game_number=game_number)


# ---------------------------------------------------------------------------
# New Slack alert checks — career firsts, hot/cold streaks, homecoming games
# (added 2026-06-15 after Kent flagged the "Munroe pulled" alert as high-value
# and asked for more in the same spirit). All gated behind MLB person_id
# availability; quietly no-op for players without one.
# ---------------------------------------------------------------------------

_STATSAPI = "https://statsapi.mlb.com/api/v1"


def _mlb_get(path: str) -> Optional[dict]:
    """Tiny helper: fetch + parse JSON from MLB Stats API. Returns None on
    any failure — these alerts are non-essential so we never bubble errors."""
    try:
        resp = requests.get(f"{_STATSAPI}{path}", timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _mlb_id(player: dict, stats: dict) -> Optional[int]:
    """Pull the player's MLB person_id from whichever field has it."""
    for k in ("mlb_id", "mlb_player_id", "api_player_id"):
        v = player.get(k) or stats.get(k)
        if v:
            try:
                return int(v)
            except Exception:
                continue
    return None


def _check_career_firsts(player, stats, name, team, tier_label, gm_label,
                         game_date, game_number, game_context, box_link):
    """Alert on MLB career firsts — first hit, first HR, first start, first
    save, first MLB game played. Check after each game by comparing today's
    contribution to career totals.

    Sentinel: today_X == career_X means this game produced their only
    career X to date — i.e., the first one happened in this game.
    """
    if stats.get("game_status") != "Final":
        return
    mlb_id = _mlb_id(player, stats)
    if not mlb_id:
        return

    # Pull career hitting + pitching totals once.
    career_h = _mlb_get(f"/people/{mlb_id}/stats?stats=career&group=hitting,pitching")
    if not career_h:
        return

    career_hitting = {}
    career_pitching = {}
    for grp in career_h.get("stats", []) or []:
        kind = grp.get("group", {}).get("displayName", "")
        splits = grp.get("splits", []) or []
        if not splits:
            continue
        stat = splits[0].get("stat", {})
        if kind == "hitting":
            career_hitting = stat
        elif kind == "pitching":
            career_pitching = stat

    # Today's contribution — best available from stats dict.
    today_hits = int(stats.get("hits", 0) or 0)
    today_hr = int(stats.get("hr", stats.get("home_runs", 0)) or 0)
    today_rbi = int(stats.get("rbi", 0) or 0)
    today_starts = 1 if stats.get("is_starting_pitcher") else 0
    today_saves = int(stats.get("saves", 0) or 0)

    career_hits = int(career_hitting.get("hits", 0) or 0)
    career_hr = int(career_hitting.get("homeRuns", 0) or 0)
    career_starts = int(career_pitching.get("gamesStarted", 0) or 0)
    career_saves = int(career_pitching.get("saves", 0) or 0)
    career_games = int(career_hitting.get("gamesPlayed", 0)
                       or career_pitching.get("gamesPlayed", 0) or 0)

    def _emit(kind: str, message: str) -> None:
        if _already_sent(game_date, name, kind, game_number=game_number):
            return
        if send_slack_message(
            f"🥇 *{name}* ({tier_label}) — {message}{gm_label}\n"
            f"_{team}_ — {game_context}{box_link}"
        ):
            _mark_sent(game_date, name, kind, game_number=game_number)

    # MLB debut: career games == 1.
    if career_games == 1:
        _emit("debut", "MLB debut! 👏")
    # First MLB hit.
    if today_hits and career_hits == today_hits:
        _emit("first_hit", f"first MLB hit ({today_hits}-for-X)")
    # First MLB HR.
    if today_hr and career_hr == today_hr:
        _emit("first_hr", f"first MLB home run! ({today_hr})")
    # First MLB pitching start.
    if today_starts and career_starts == today_starts:
        _emit("first_start", "first MLB start")
    # First MLB save.
    if today_saves and career_saves == today_saves:
        _emit("first_save", "first MLB save")


def _check_streak(player, stats, name, team, tier_label, gm_label,
                  game_date, game_number, game_context, box_link):
    """Look at the player's last 5 game logs and alert on hot or cold patterns.

    Hot (hitter): 3+ multi-hit games in last 5 (sticky per player+streak-start).
    Cold (hitter): 0 hits across 5 consecutive games with PAs.
    Hot (pitcher): 3 consecutive starts of <=2 ER AND >=5 IP.
    """
    mlb_id = _mlb_id(player, stats)
    if not mlb_id:
        return
    log = _mlb_get(
        f"/people/{mlb_id}/stats?stats=gameLog&season={date.today().year}"
        f"&group=hitting,pitching&limit=10"
    )
    if not log:
        return
    hits_log = []
    pit_log = []
    for grp in log.get("stats", []) or []:
        kind = grp.get("group", {}).get("displayName", "")
        for sp in grp.get("splits", []) or []:
            s = sp.get("stat", {})
            row = {"date": sp.get("date", ""), **s}
            if kind == "hitting":
                hits_log.append(row)
            elif kind == "pitching":
                pit_log.append(row)

    hits_log.sort(key=lambda r: r.get("date", ""), reverse=True)
    pit_log.sort(key=lambda r: r.get("date", ""), reverse=True)

    # Hitter streaks.
    if hits_log:
        last5 = hits_log[:5]
        multi_hit = sum(1 for g in last5 if int(g.get("hits", 0) or 0) >= 2)
        with_pa = [g for g in last5 if int(g.get("plateAppearances", 0) or 0) >= 1]
        zero_for = (
            len(with_pa) >= 5
            and all(int(g.get("hits", 0) or 0) == 0 for g in with_pa)
        )
        if multi_hit >= 3:
            kind = f"hot_hit_{game_date}"
            if not _already_sent(game_date, name, kind, game_number=game_number):
                if send_slack_message(
                    f"🔥 *{name}* ({tier_label}) heating up{gm_label} — "
                    f"{multi_hit} multi-hit games in last 5\n"
                    f"_{team}_ — {game_context}{box_link}"
                ):
                    _mark_sent(game_date, name, kind, game_number=game_number)
        elif zero_for:
            kind = f"cold_hit_{game_date}"
            if not _already_sent(game_date, name, kind, game_number=game_number):
                if send_slack_message(
                    f"🥶 *{name}* ({tier_label}) cold{gm_label} — "
                    f"0 hits in last 5 games\n"
                    f"_{team}_ — {game_context}{box_link}"
                ):
                    _mark_sent(game_date, name, kind, game_number=game_number)

    # Pitcher hot streak.
    if pit_log:
        starts = [g for g in pit_log[:5] if int(g.get("gamesStarted", 0) or 0) >= 1]
        if len(starts) >= 3:
            good = sum(
                1 for g in starts[:3]
                if (int(g.get("earnedRuns", 0) or 0) <= 2
                    and float(g.get("inningsPitched", "0.0") or 0) >= 5)
            )
            if good == 3:
                kind = f"hot_pit_{game_date}"
                if not _already_sent(game_date, name, kind, game_number=game_number):
                    if send_slack_message(
                        f"🔥 *{name}* ({tier_label}) — 3 consecutive quality "
                        f"starts{gm_label}\n_{team}_ — {game_context}{box_link}"
                    ):
                        _mark_sent(game_date, name, kind, game_number=game_number)


def _check_homecoming(player, stats, name, team, tier_label, gm_label,
                     game_date, game_number, game_context, box_link):
    """Alert when player is playing a game in their birth state (often the
    only time per year they're in front of family). Dedup per player+state
    per season — fires once when they first arrive in-state.
    """
    mlb_id = _mlb_id(player, stats)
    if not mlb_id:
        return
    # Need today's game venue. Pull it from the schedule if not on stats.
    venue_state = stats.get("venue_state")
    if not venue_state:
        game_pk = stats.get("game_pk")
        if game_pk:
            data = _mlb_get(f"/game/{game_pk}/feed/live")
            if data:
                venue = (data.get("gameData", {}) or {}).get("venue", {}) or {}
                venue_state = (venue.get("location") or {}).get("state", "")
    if not venue_state:
        return

    # Player birth state — cached lookup via /people.
    person = _mlb_get(f"/people/{mlb_id}")
    if not person:
        return
    people = person.get("people", []) or []
    if not people:
        return
    birth_state = people[0].get("birthStateProvince", "")
    if not birth_state or birth_state.lower() != venue_state.lower():
        return

    season = date.today().year
    kind = f"homecoming_{season}_{venue_state}"
    if _already_sent(game_date, name, kind, game_number=game_number):
        return
    if send_slack_message(
        f"📍 *{name}* ({tier_label}) — homecoming game in {venue_state}{gm_label}\n"
        f"_{team}_ — {game_context}{box_link}"
    ):
        _mark_sent(game_date, name, kind, game_number=game_number)


def save_sent_alerts():
    """Persist alert state to disk. Call once after all alerts are processed."""
    _save_sent_alerts()


def reset_sent_alerts():
    """Load persistent alert state from disk (replaces the old in-memory clear)."""
    _load_sent_alerts()
