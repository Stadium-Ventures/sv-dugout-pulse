"""Slack alert when a summer-league roster count drops significantly.

Catches silent failure modes — e.g., 2026-06-12: CCBL dropped from 391
players to 0 because MLB Stats API cleared its 2026 roster pre-opener.
That looked broken on the dashboard but was an upstream state change;
either way we want to know about it.

Behavior:
- Reads data/summer_ball_rosters.json (current snapshot).
- Reads data/_last_league_counts.json (last-seen counts).
- For each league where prior >= 50 and current < prior * 0.5, posts a
  Slack alert.
- Also fires when a previously-"ok" league flips to "failed" status.
- Writes current counts back to the state file (next run's baseline).
- Stays silent when nothing regressed.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

from scripts._automation_notify import post_automation

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ROSTERS_PATH = _REPO_ROOT / "data" / "summer_ball_rosters.json"
_STATE_PATH = _REPO_ROOT / "data" / "_last_league_counts.json"

# Don't fire on day-over-day variance for tiny leagues — they're noisy.
_MIN_PRIOR_PLAYERS = 50
# Drop ratio that counts as a regression (current / prior).
_DROP_THRESHOLD = 0.5


def _load_state() -> dict:
    if not _STATE_PATH.exists():
        return {}
    try:
        return json.loads(_STATE_PATH.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    _STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def main() -> int:
    if not _ROSTERS_PATH.exists():
        logger.info("No summer_ball_rosters.json yet — skipping")
        return 0
    try:
        rosters = json.loads(_ROSTERS_PATH.read_text())
    except Exception:
        logger.exception("Failed to read %s", _ROSTERS_PATH)
        return 0

    health = rosters.get("league_health") or []
    prior_state = _load_state()
    regressions: list[dict] = []
    new_state: dict = {}

    for entry in health:
        league = entry.get("league")
        status = entry.get("status")
        count = int(entry.get("player_count") or 0)
        if not league or status == "not_implemented":
            continue
        new_state[league] = {"count": count, "status": status}
        prior = prior_state.get(league) or {}
        prior_count = int(prior.get("count") or 0)
        prior_status = prior.get("status")
        # Big roster drop on a previously-populated league.
        if prior_count >= _MIN_PRIOR_PLAYERS and count < prior_count * _DROP_THRESHOLD:
            regressions.append({
                "league": league,
                "prior": prior_count,
                "current": count,
                "kind": "roster_drop",
            })
        # Healthy → failed flip on a previously-populated league.
        elif (
            prior_status == "ok" and status == "failed"
            and prior_count >= _MIN_PRIOR_PLAYERS
        ):
            regressions.append({
                "league": league,
                "prior": prior_count,
                "current": count,
                "kind": "scrape_failed",
            })

    # Persist new state regardless of alert outcome (so a regression
    # resolving the next day doesn't keep firing).
    _save_state(new_state)

    if not regressions:
        logger.info("No regressions to report (%d leagues tracked)", len(new_state))
        return 0

    lines = [":rotating_light: *Summer-league roster regression detected*"]
    for r in regressions:
        if r["kind"] == "scrape_failed":
            lines.append(
                f"• *{r['league']}*: scrape now failing "
                f"(was returning {r['prior']} players)"
            )
        else:
            lines.append(
                f"• *{r['league']}*: {r['prior']} → {r['current']} players "
                f"({100 * (1 - r['current']/r['prior']):.0f}% drop)"
            )
    lines.append(
        "\n_Likely causes: upstream roster reset, source URL change, or our parser breaking. "
        "Check workflow logs + the league site._"
    )
    text = "\n".join(lines)
    return 0 if post_automation(text) else 1


if __name__ == "__main__":
    sys.exit(main())
