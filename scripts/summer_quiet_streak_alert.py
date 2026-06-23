"""Slack alert when a Summer placement hasn't played in N days.

The "Did not appear" lines in the daily recap surface a one-off, but
multi-day quiet stretches go invisible — injury, scratch, bench, lost
job. This script tracks game-count changes day-over-day and posts when
a confirmed Summer client has been quiet for >= threshold days.

State file (data/_last_summer_games.json):
  { "Ben Tryon": {"games": 17, "last_change_date": "2026-06-22"}, ... }

Behavior:
- Read current games_played from data/window_season.json.
- Compare to stored games count.
- If games_played increased → update state with today's date as last_change.
- If unchanged → leave last_change as-is; compute days_since.
- Alert when days_since >= THRESHOLD and games_played > 0 (skip players
  who haven't played a game yet — different problem, different alert).
- Persist state file so the next run has the baseline.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SEASON_PATH = _REPO_ROOT / "data" / "window_season.json"
_STATE_PATH = _REPO_ROOT / "data" / "_last_summer_games.json"
_SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# Quiet for >= this many days fires an alert. 5 = "missed roughly two
# series of games" — enough to be meaningful, not a typical day off.
_THRESHOLD_DAYS = 5
# Once we alert, suppress for this many days before re-alerting on the
# same player. Avoids re-firing every day on the same multi-week stretch.
_REALERT_COOLDOWN_DAYS = 7
_ET = timezone(timedelta(hours=-4))


def _load_state() -> dict:
    if not _STATE_PATH.exists():
        return {}
    try:
        return json.loads(_STATE_PATH.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    _STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def _today_et_str() -> str:
    return datetime.now(_ET).date().isoformat()


def _days_between(iso_a: str, iso_b: str) -> int:
    try:
        a = date.fromisoformat(iso_a)
        b = date.fromisoformat(iso_b)
        return (b - a).days
    except Exception:
        return 0


def main() -> int:
    if not _SEASON_PATH.exists():
        logger.info("window_season.json missing — skipping")
        return 0
    try:
        season = json.loads(_SEASON_PATH.read_text())
    except Exception:
        logger.exception("Failed to read %s", _SEASON_PATH)
        return 0
    summer = [
        p for p in season
        if p.get("level") == "Summer" and p.get("is_client") is not False
    ]
    state = _load_state()
    today = _today_et_str()
    quiet: list[dict] = []
    new_state: dict = {}

    for p in summer:
        name = p.get("player_name")
        if not name:
            continue
        games = int(p.get("games_played") or 0)
        team = (p.get("team") or "").split("(")[0].strip()
        prior = state.get(name) or {}
        prior_games = int(prior.get("games") or 0)
        last_change = prior.get("last_change_date") or today
        last_alert = prior.get("last_alert_date")

        if games > prior_games:
            last_change = today

        new_state[name] = {
            "games": games,
            "last_change_date": last_change,
            "last_alert_date": last_alert,
        }

        if games <= 0:
            # Hasn't played at all — covered by the "no data" hedge in the
            # daily recap. Don't fire quiet-streak on these.
            continue
        days = _days_between(last_change, today)
        if days < _THRESHOLD_DAYS:
            continue
        # Cooldown
        if last_alert:
            since_alert = _days_between(last_alert, today)
            if since_alert < _REALERT_COOLDOWN_DAYS:
                continue
        new_state[name]["last_alert_date"] = today
        quiet.append({"name": name, "team": team, "days": days, "games": games})

    _save_state(new_state)

    if not quiet:
        logger.info("No quiet-streak alerts (%d tracked)", len(new_state))
        return 0

    lines = [":mute: *Quiet streak — clients who haven't appeared recently*"]
    for q in sorted(quiet, key=lambda x: -x["days"]):
        lines.append(
            f"• *{q['name']}* ({q['team']}): {q['days']} days since last game "
            f"(season total: {q['games']} G)"
        )
    lines.append(
        "\n_Worth a check — could be injury, scratch, role change, or just a "
        "long off-stretch._"
    )
    text = "\n".join(lines)

    if not _SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — would have posted:")
        print(text)
        return 0
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
        logger.info("Posted %d quiet-streak alert(s)", len(quiet))
        return 0
    except Exception:
        logger.exception("Slack send errored")
        return 1


if __name__ == "__main__":
    sys.exit(main())
