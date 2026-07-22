"""Daily self-health-check → #sv-automation. Silent when healthy.

Runs once a day (see .github/workflows/cron_health_alert.yml, 22:00 UTC =
6 PM ET) and checks the freshness of every data product this app writes on a
schedule. If a scheduled job silently stops running — GitHub cron skip,
repeated timeout-cancellations (which do NOT trigger `if: failure()` alert
steps), a dead upstream — the stale file is the symptom this catches.

Checks:
  1. data/current_pulse.json   — live pulse; rebuilt every 15 min during game
                                 hours, so >12h old means the pipeline is dead
  2. data/bbref_stats.json     — BBRef refresh; 3 windows/day, >24h = missed,
                                 >48h = stale
  3. data/summer_ball_rosters.json — summer roster refresh; 4 runs/day, >36h
                                 old means the workflow is silently dying
                                 (e.g. the 2026-07 timeout-cancellation streak)

Posts ONE message to #sv-automation listing only the checks that failed,
following the SV message contract (what broke / how we know / what to do,
each finding tagged 🛠️ Code change vs 👤 Manual). Healthy runs post nothing.

Test mode: `python -m scripts.cron_health_alert --test` sends a clearly
labeled test post through the real code path, regardless of health.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts._automation_notify import post_automation

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DATA = _REPO_ROOT / "data"

_ACTIONS_URL = "https://github.com/Stadium-Ventures/sv-dugout-pulse/actions"


def _hours_since(iso_ts: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:
        return float("inf")


def _ago(hours: float) -> str:
    if hours == float("inf"):
        return "an unknown time ago (timestamp unreadable)"
    if hours < 1:
        return f"{int(hours * 60)} minutes ago"
    if hours < 48:
        return f"{int(hours)} hours ago"
    days = int(hours / 24)
    return f"{days} day{'s' if days != 1 else ''} ago"


def _file_age_hours(filename: str, ts_field: str) -> float:
    """Age of a data file's own generated-at stamp; inf if missing/unreadable."""
    path = _DATA / filename
    if not path.exists():
        return float("inf")
    try:
        data = json.loads(path.read_text())
    except Exception:
        return float("inf")
    return _hours_since(str(data.get(ts_field, "")))


def run_checks() -> list[str]:
    """Returns one contract-formatted finding string per failed check."""
    findings: list[str] = []

    # 1. Live pulse — the main product. Rebuilt every 15 min, 10 AM–2:45 AM ET.
    age = _file_age_hours("current_pulse.json", "generated_at")
    if age > 12:
        findings.append(
            ":red_circle: *We've stopped updating live player stats — the "
            "dashboard is showing old numbers.*\n"
            f"How we know: the last successful stats refresh was {_ago(age)}; "
            "during the season it updates every few minutes.\n"
            f"What to do: 👤 re-run the *Update pulse data* workflow at {_ACTIONS_URL} "
            "and check its log. 🛠️ If re-running fails the same way, it needs a "
            "code fix — open the failing run's log in Claude Code."
        )

    # 2. BBRef summer-ball stats — refreshed 9 AM / 12 PM / 5 PM ET.
    age = _file_age_hours("bbref_stats.json", "generated_at_utc")
    if age > 48:
        findings.append(
            ":rotating_light: *Summer-ball stat lines have gone stale.*\n"
            f"How we know: the Baseball-Reference refresh last succeeded {_ago(age)} "
            "— it normally lands three times a day.\n"
            f"What to do: 👤 re-run the *Refresh BBRef Stats* workflow at {_ACTIONS_URL}. "
            "🛠️ If it keeps failing, the scraper likely needs a code fix."
        )
    elif age > 24:
        findings.append(
            ":warning: *Yesterday's summer-ball stats may be missing.*\n"
            f"How we know: the Baseball-Reference refresh last succeeded {_ago(age)} "
            "— it normally lands three times a day.\n"
            f"What to do: 👤 re-run the *Refresh BBRef Stats* workflow at {_ACTIONS_URL}, "
            "or wait — the next scheduled window may self-resolve it."
        )

    # 3. Summer roster snapshot — refreshed 4×/day. Catches the failure mode
    #    where the workflow is repeatedly timeout-cancelled (cancelled runs
    #    never trigger `if: failure()` alerts, so staleness is the only tell).
    age = _file_age_hours("summer_ball_rosters.json", "generated_at")
    if age > 36:
        findings.append(
            ":warning: *Summer-league rosters have stopped refreshing, so team "
            "assignments on the dashboard may be out of date.*\n"
            f"How we know: the roster snapshot was last rebuilt {_ago(age)}; it "
            "normally refreshes several times a day.\n"
            f"What to do: 👤 check the *Summer Ball Roster Refresh* runs at {_ACTIONS_URL} "
            "— if they show 'cancelled', the job is timing out. 🛠️ That needs a code "
            "fix (speed the refresh up or raise its time limit)."
        )

    return findings


def main() -> int:
    if "--test" in sys.argv:
        ok = post_automation(
            ":test_tube: *Test post — please ignore.* Verifying the Dugout Pulse "
            "daily health check can reach #sv-automation. No action needed."
        )
        return 0 if ok else 1

    findings = run_checks()
    if not findings:
        logger.info("All checks healthy — staying silent.")
        return 0

    text = "\n\n".join(findings)
    logger.info("%d finding(s) — posting to #sv-automation", len(findings))
    return 0 if post_automation(text) else 1


if __name__ == "__main__":
    sys.exit(main())
