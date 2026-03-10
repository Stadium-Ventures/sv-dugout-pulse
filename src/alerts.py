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


def _already_sent(game_date: str, player_name: str, alert_type: str,
                  current_value=None, game_number: int = 0) -> bool:
    """Check if this alert was already sent (persists across runs).

    For value-aware alerts (like HR count), re-triggers if current_value
    exceeds the previously alerted value.
    """
    key = _alert_key(game_date, player_name, alert_type, game_number)
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
    """Mark an alert as sent (in-memory). Call save_sent_alerts() to persist."""
    key = _alert_key(game_date, player_name, alert_type, game_number)
    _sent_alerts[key] = value


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
    is_pitching = stats.get("is_pitcher_line", False) or position == "Pitcher"
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

    # --- Alert: Standout game summary (when game goes Final) ---
    if game_status == "Final" and "Standout" in grade:
        if not _already_sent(game_date, name, "standout_recap", game_number=game_number):
            if send_slack_message(
                f"🌟 *{name}* ({tier_label}) — Standout performance{gm_label}!\n"
                f"_{team}_ — {summary} — {game_context}{box_link}"
            ):
                _mark_sent(game_date, name, "standout_recap", game_number=game_number)


def save_sent_alerts():
    """Persist alert state to disk. Call once after all alerts are processed."""
    _save_sent_alerts()


def reset_sent_alerts():
    """Load persistent alert state from disk (replaces the old in-memory clear)."""
    _load_sent_alerts()
