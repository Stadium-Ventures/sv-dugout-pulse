"""Daily Slack alert: did the BBRef refresh cron fire in the last 24 hours?

Reads `data/bbref_stats.json`'s `generated_at_utc` field — that's stamped
every time the refresh_bbref_stats workflow successfully completes.
Compares to wall clock and posts a plain-English status to #sv-automation
(the muted cross-product channel — moved off #dugout-pulse 2026-06-29).

Until Tom wires cron-job.org for refresh_bbref_stats.yml (queued for Mon
2026-06-09), GitHub-native cron sometimes silently skips. This catches
those misses.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from scripts._automation_notify import post_automation

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BBREF_STATS_PATH = _REPO_ROOT / "data" / "bbref_stats.json"


def _hours_ago(iso_ts: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:
        return 9999.0


def _format_time_ago(hours: float) -> str:
    if hours < 1: return f"{int(hours * 60)} min ago"
    if hours < 24: return f"{int(hours)} hours ago"
    days = int(hours / 24)
    return f"{days} day{'s' if days != 1 else ''} ago"


def main() -> int:
    if not _BBREF_STATS_PATH.exists():
        text = (
            ":warning: *BBRef cron health check*: stats file missing entirely. "
            "Workflow likely never ran. Manual trigger needed."
        )
    else:
        try:
            data = json.loads(_BBREF_STATS_PATH.read_text())
        except Exception:
            text = ":warning: *BBRef cron health check*: stats file unreadable."
        else:
            ts = data.get("generated_at_utc", "")
            hours = _hours_ago(ts)
            ago = _format_time_ago(hours)
            if hours <= 24:
                text = (
                    f":white_check_mark: *BBRef cron health check*: last successful "
                    f"refresh was {ago}. Good."
                )
            elif hours <= 48:
                text = (
                    f":warning: *BBRef cron health check*: last successful refresh "
                    f"was {ago} — looks like a cron miss. Manually trigger via "
                    f"GitHub Actions or wait for the next scheduled window."
                )
            else:
                text = (
                    f":rotating_light: *BBRef cron health check*: last successful "
                    f"refresh was {ago}. BBRef stats are stale. Manually trigger "
                    f"refresh_bbref_stats.yml in GitHub Actions."
                )

    return 0 if post_automation(text) else 1


if __name__ == "__main__":
    sys.exit(main())
