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

    # Promotion / level-change check runs first because we want to catch
    # call-ups on off-days too (player isn't in tonight's game because he
    # was just promoted and is traveling). Doesn't need a game.
    _check_promotion(player, stats, name, team)

    # Skip remaining alerts if no game data.
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


# Rolling-window wOBA thresholds for hitter streak alerts. Calibrated so
# the cold side only fires on genuinely worrying performance (not a 3-game
# slump). 5-game window with min 15 PAs filters out short looks.
#
# Reference points:
#   .450+ wOBA over a week = top of the league, "on fire"
#   .200- wOBA over 15+ PAs = struggling badly, ~replacement level or worse
#   League average wOBA is ~.320
_WOBA_HOT_THRESHOLD = 0.450
_WOBA_COLD_THRESHOLD = 0.200
_STREAK_MIN_PA = 15


def _woba(games: list[dict]) -> tuple[float, dict]:
    """Compute rolling wOBA across a list of MLB game-log splits.

    Returns (wOBA, totals_dict). Totals include ab/h/2b/3b/hr/bb/k/pa for
    rendering in the alert message. wOBA=0.0 when denominator is 0.

    Formula (standard FanGraphs weights):
      wOBA = (0.69·BB + 0.72·HBP + 0.89·1B + 1.27·2B + 1.62·3B + 2.10·HR)
             ÷ (AB + BB + SF + HBP)
    Using BB instead of uBB (intentional walks are tiny noise at this scale).
    """
    ab = h = bb = hbp = sf = sb_2 = sb_3 = sb_hr = k = pa = 0
    for g in games:
        ab += int(g.get("atBats", 0) or 0)
        h += int(g.get("hits", 0) or 0)
        bb += int(g.get("baseOnBalls", 0) or 0)
        hbp += int(g.get("hitByPitch", 0) or 0)
        sf += int(g.get("sacFlies", 0) or 0)
        sb_2 += int(g.get("doubles", 0) or 0)
        sb_3 += int(g.get("triples", 0) or 0)
        sb_hr += int(g.get("homeRuns", 0) or 0)
        k += int(g.get("strikeOuts", 0) or 0)
        pa += int(g.get("plateAppearances", 0) or 0)
    singles = h - sb_2 - sb_3 - sb_hr
    denom = ab + bb + sf + hbp
    if denom <= 0:
        return 0.0, {"ab": ab, "h": h, "hr": sb_hr, "bb": bb, "k": k, "pa": pa}
    numer = (
        0.69 * bb + 0.72 * hbp + 0.89 * singles
        + 1.27 * sb_2 + 1.62 * sb_3 + 2.10 * sb_hr
    )
    return round(numer / denom, 3), {
        "ab": ab, "h": h, "hr": sb_hr, "bb": bb, "k": k, "pa": pa,
        "2b": sb_2, "3b": sb_3,
    }


def _check_streak(player, stats, name, team, tier_label, gm_label,
                  game_date, game_number, game_context, box_link):
    """Rolling 5-game streak detector based on wOBA.

    Hot (hitter): 5-game wOBA >= .450 over 15+ PA — exceptional stretch.
    Cold (hitter): 5-game wOBA <= .200 over 15+ PA — genuinely bad; surface
       so someone can check in.
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

    # Hitter streaks via rolling 5-game wOBA.
    if hits_log:
        last5 = hits_log[:5]
        woba, totals = _woba(last5)
        if totals["pa"] >= _STREAK_MIN_PA:
            stat_line = (
                f"{totals['h']}-for-{totals['ab']}, "
                f"{totals['hr']} HR, {totals['k']} K"
            )
            if woba >= _WOBA_HOT_THRESHOLD:
                kind = f"hot_hit_{game_date}"
                if not _already_sent(game_date, name, kind, game_number=game_number):
                    if send_slack_message(
                        f"🔥 *{name}* ({tier_label}) on fire{gm_label} — "
                        f"*{woba:.3f} wOBA* over last 5 games "
                        f"({stat_line})\n_{team}_ — {game_context}{box_link}"
                    ):
                        _mark_sent(game_date, name, kind, game_number=game_number)
            elif woba <= _WOBA_COLD_THRESHOLD:
                kind = f"cold_hit_{game_date}"
                if not _already_sent(game_date, name, kind, game_number=game_number):
                    if send_slack_message(
                        f"🚨 *{name}* ({tier_label}) struggling{gm_label} — "
                        f"only *{woba:.3f} wOBA* over last 5 games "
                        f"({stat_line}). Worth a check-in.\n"
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


_TEAM_STATE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "_last_team_levels.json",
)

# MLB Stats API sport hierarchy (lower id = higher level).
# Source: statsapi.mlb.com /sports
_SPORT_LEVEL_NAME = {
    1: "MLB", 11: "AAA", 12: "AA", 13: "A+", 14: "A",
    15: "Short-Season A", 16: "Rookie", 17: "Winter",
    21: "Minors (rollup)", 22: "College", 23: "Independent",
}


def _load_team_state() -> dict:
    try:
        with open(_TEAM_STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_team_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_TEAM_STATE_PATH), exist_ok=True)
        with open(_TEAM_STATE_PATH, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except Exception:
        logger.exception("Failed to write team-state file")


def _check_promotion(player, stats, name, team):
    """Detect when a Pro client moves to a higher level (lower sportId).

    Tracks each client's current team_id + sport_id between pulses in
    data/_last_team_levels.json. On a downward sportId shift (e.g.,
    AA → AAA, AAA → MLB), fires a Slack alert once.

    Quietly no-ops for any player without an mlb_id or current team info.
    """
    mlb_id = _mlb_id(player, stats)
    if not mlb_id:
        return
    # Pull current team + sport. Try stats first, fall back to MLB API.
    sport_id = stats.get("api_sport_id") or stats.get("sport_id")
    team_id = stats.get("api_team_id") or stats.get("team_id")
    team_name = stats.get("api_current_team") or team
    if not (sport_id and team_id):
        data = _mlb_get(f"/people/{mlb_id}?hydrate=currentTeam")
        if not data:
            return
        people = data.get("people", []) or []
        if not people:
            return
        ct = people[0].get("currentTeam") or {}
        team_id = ct.get("id")
        team_name = ct.get("name", team_name)
        # Hydrated team carries sport.id one level deeper.
        team_data = _mlb_get(f"/teams/{team_id}") if team_id else None
        if team_data:
            teams = team_data.get("teams", []) or []
            if teams:
                sport_id = (teams[0].get("sport") or {}).get("id")
    if not (mlb_id and team_id and sport_id):
        return

    state = _load_team_state()
    key = str(mlb_id)
    prior = state.get(key) or {}
    prior_team_id = prior.get("team_id")
    prior_sport_id = prior.get("sport_id")

    # First time seeing this player — just record state, no alert.
    if not prior_team_id:
        state[key] = {"team_id": team_id, "sport_id": sport_id,
                      "team_name": team_name, "name": name}
        _save_team_state(state)
        return

    # No change.
    if prior_team_id == team_id:
        return

    # Team changed — compare sport levels. Lower sport_id = higher level.
    new_level = _SPORT_LEVEL_NAME.get(sport_id, f"sport {sport_id}")
    prior_level = _SPORT_LEVEL_NAME.get(prior_sport_id, f"sport {prior_sport_id}")
    prior_team = prior.get("team_name", "(prior team)")

    is_promotion = prior_sport_id and sport_id < prior_sport_id
    is_mlb_callup = sport_id == 1 and prior_sport_id != 1

    if is_mlb_callup:
        msg = (
            f"🎉 *{name}* called up to the *MAJOR LEAGUES* — "
            f"{prior_team} ({prior_level}) → *{team_name} (MLB)*"
        )
    elif is_promotion:
        msg = (
            f"⬆️ *{name}* promoted — "
            f"{prior_team} ({prior_level}) → {team_name} ({new_level})"
        )
    else:
        # Lateral move or demotion — log silently, update state, no Slack.
        state[key] = {"team_id": team_id, "sport_id": sport_id,
                      "team_name": team_name, "name": name}
        _save_team_state(state)
        return

    # Dedup: per-player per-promotion-destination so re-pulses don't re-fire.
    today = date.today().isoformat()
    if _already_sent(today, name, f"promo_{team_id}"):
        return
    if send_slack_message(msg):
        _mark_sent(today, name, f"promo_{team_id}")
        state[key] = {"team_id": team_id, "sport_id": sport_id,
                      "team_name": team_name, "name": name}
        _save_team_state(state)


def save_sent_alerts():
    """Persist alert state to disk. Call once after all alerts are processed."""
    _save_sent_alerts()


def reset_sent_alerts():
    """Load persistent alert state from disk (replaces the old in-memory clear)."""
    _load_sent_alerts()
