"""Daily Slack summary of summer-ball activity for SV's NCAA clients.

Posts to Dugout Pulse at 9 AM ET each morning:
  - Yesterday's Finals (and DNPs) for clients in summer leagues
  - Today's scheduled games

Reads:
  - data/yesterday_pulse.json → yesterday's Final entries
  - data/current_pulse.json   → today's Scheduled / Live / Final entries

Both files already have the Summer entries we need; this script just
filters and formats for Slack.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CURRENT_PATH = _REPO_ROOT / "data" / "current_pulse.json"
_YESTERDAY_PATH = _REPO_ROOT / "data" / "yesterday_pulse.json"
_SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
_ET = timezone(timedelta(hours=-4))  # EDT for summer


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except Exception:
        return []
    if isinstance(data, dict):
        return data.get("players") or []
    if isinstance(data, list):
        return data
    return []


def _summer_clients(entries: list[dict]) -> list[dict]:
    return [
        e for e in entries
        if e.get("level") == "Summer" and e.get("is_client") is not False
    ]


def _format_yesterday_line(e: dict) -> str:
    name = e.get("player_name", "?")
    team = e.get("team", "")
    summary = e.get("stats_summary", "")
    if not summary or summary in ("No game today", "No games yet — season just opened"):
        return ""
    # Drop the trailing "(league)" piece from the team string for brevity.
    if "(" in team:
        team = team.split("(")[0].strip()
    return f"• *{name}* ({team}): {summary}"


def _format_today_line(e: dict) -> str:
    name = e.get("player_name", "?")
    team = e.get("team", "")
    if "(" in team:
        team = team.split("(")[0].strip()
    status = e.get("game_status", "")
    if status == "Scheduled":
        when = e.get("game_time") or "TBD"
        matchup = e.get("game_context") or ""
        return f"• *{name}* ({team}): {matchup} — {when}"
    if status in ("In Progress", "Live"):
        return f"• *{name}* ({team}): :red_circle: live — {e.get('stats_summary','')}"
    if status == "Final":
        return f"• *{name}* ({team}): Final — {e.get('stats_summary','')}"
    return ""


def build_message() -> str:
    yest = _summer_clients(_load(_YESTERDAY_PATH))
    today = _summer_clients(_load(_CURRENT_PATH))

    # Yesterday: finals only.
    yest_lines = []
    for e in sorted(yest, key=lambda x: x.get("player_name", "")):
        if e.get("game_status") not in ("Final", "In Progress"):
            continue
        line = _format_yesterday_line(e)
        if line:
            yest_lines.append(line)

    # Today: scheduled / live / final. Filter out is_yesterday entries
    # that summer_pulse mirrors into current_pulse.json (those belong in
    # the Yesterday section, not Today).
    today_lines = []
    for e in sorted(today, key=lambda x: x.get("player_name", "")):
        if e.get("is_yesterday"):
            continue
        if e.get("game_status") not in ("Scheduled", "In Progress", "Final", "Live"):
            continue
        line = _format_today_line(e)
        if line:
            today_lines.append(line)

    now_et = datetime.now(_ET)
    yest_date = (now_et - timedelta(days=1)).strftime("%b %-d")
    today_date = now_et.strftime("%b %-d")

    parts = [f":sunny: *Summer Ball — Daily Recap*  ({today_date})"]
    if yest_lines:
        parts.append(f"\n*Yesterday ({yest_date}):*")
        parts.extend(yest_lines)
    else:
        parts.append(f"\n*Yesterday ({yest_date}):*  _no client summer activity recorded_")

    if today_lines:
        parts.append(f"\n*Coming up today ({today_date}):*")
        parts.extend(today_lines)
    else:
        parts.append(f"\n*Coming up today ({today_date}):*  _no client summer games on the schedule_")

    parts.append("\n_<https://stadium-ventures.github.io/sv-dugout-pulse/|Open Dugout Pulse>_")
    return "\n".join(parts)


def main() -> int:
    if not _SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — printing message instead:")
        print(build_message())
        return 0
    text = build_message()
    try:
        resp = requests.post(
            _SLACK_WEBHOOK_URL,
            json={"text": text},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.error("Slack send failed: %s %s", resp.status_code, resp.text)
            return 1
        logger.info("Sent %d-char summer recap to Slack", len(text))
        return 0
    except Exception:
        logger.exception("Slack send errored")
        return 1


if __name__ == "__main__":
    sys.exit(main())
