"""Daily Slack alert: any new placement-vs-auto-match conflicts?

The summer_ball roster refresh writes `placement_validation` into
data/summer_ball_rosters.json with three buckets:
  - agrees: auto-match team matches Kent's spreadsheet
  - conflicts: auto-match found a DIFFERENT team for that player
  - unmatched: in spreadsheet, not auto-matched (expected for Northwoods etc.)

This script posts to Slack only when `conflicts` is non-empty — so Kent
gets pinged when our auto-scrape disagrees with his spreadsheet and can
either update the sheet or know the auto-match is showing stale data.

Tracks last-seen conflict set in `data/_last_conflicts.json` so we don't
re-alert about the same conflict every day.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ROSTER_PATH = _REPO_ROOT / "data" / "summer_ball_rosters.json"
_SEEN_PATH = _REPO_ROOT / "data" / "_last_conflicts.json"
_SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def _load_seen() -> set:
    if not _SEEN_PATH.exists():
        return set()
    try:
        return set(json.loads(_SEEN_PATH.read_text()))
    except Exception:
        return set()


def _save_seen(keys: set) -> None:
    _SEEN_PATH.write_text(json.dumps(sorted(keys)))


def main() -> int:
    if not _ROSTER_PATH.exists():
        logger.info("No summer_ball_rosters.json yet — skipping")
        return 0
    try:
        data = json.loads(_ROSTER_PATH.read_text())
    except Exception:
        logger.exception("Failed to read %s", _ROSTER_PATH)
        return 0

    validation = data.get("placement_validation") or {}
    conflicts = validation.get("conflicts") or []
    if not conflicts:
        logger.info("No conflicts to report")
        return 0

    seen = _load_seen()
    # Use (player_name, auto_team) as the conflict key — if the auto-match
    # team changes for a player, that's a new conflict worth re-alerting.
    new_conflicts = [
        c for c in conflicts
        if f"{c.get('player_name')}|{c.get('auto_team')}" not in seen
    ]
    if not new_conflicts:
        logger.info("%d existing conflicts, no new ones — staying quiet", len(conflicts))
        return 0

    lines = [":warning: *Summer ball placement conflicts to review*"]
    lines.append(
        "_Our auto-scrape found different teams than your placement spreadsheet — "
        "likely stale rosters from prior years. Confirm the right team:_"
    )
    for c in new_conflicts:
        lines.append(
            f"• *{c.get('player_name')}* ({c.get('school','?')}): "
            f"sheet says *{c.get('placement_team','?')}* ({c.get('placement_league','?')}) — "
            f"auto-scrape found *{c.get('auto_team','?')}* ({c.get('auto_league','?')})"
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
        # Mark as seen so we don't re-alert tomorrow.
        seen.update(f"{c.get('player_name')}|{c.get('auto_team')}" for c in new_conflicts)
        _save_seen(seen)
        logger.info("Posted %d new conflict(s)", len(new_conflicts))
        return 0
    except Exception:
        logger.exception("Slack send errored")
        return 1


if __name__ == "__main__":
    sys.exit(main())
