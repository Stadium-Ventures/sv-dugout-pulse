"""Shared season-over detection for the summer-ball alerts.

Kent, 2026-08-17: "Summer ball season over can we end these alerts?" —
the daily recap kept posting "no client summer activity" every morning
for a week-plus after every reachable league (Cape Cod, NECBL,
Appalachian, MLB Draft) had actually finished playing. Rather than a
one-time manual off-switch, detect it from the data so both this year's
tail and next year's start-of-season are automatic:

- Off when no tracked placement has logged a real (played) game in
  SEASON_IDLE_THRESHOLD_DAYS — covers the offseason and next year's
  pre-opener stretch before anyone's played yet.
- On again the first time a real game shows up in the log.

Used by summer_daily_slack.py (daily recap) and summer_quiet_streak_alert.py
(per-player quiet streak) — both go quiet with the season and come back
with it, no manual toggle either direction.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_GAME_LOG_PATH = _REPO_ROOT / "data" / "summer_game_log.json"
_ET = timezone(timedelta(hours=-4))

# Clubs play 5-6 games a week, so a full week-plus with zero real games
# logged across every tracked placement means the season itself has
# ended, not just one player's stretch. Matches the per-player bar in
# summer_quiet_streak_alert.py.
SEASON_IDLE_THRESHOLD_DAYS = 8


def _is_real_game(entry: dict) -> bool:
    summary = entry.get("stats_summary") or ""
    return bool(summary) and "Did not appear" not in summary and "No game" not in summary


def last_real_game_day(log_path: Path = _GAME_LOG_PATH) -> str | None:
    """Most recent date (ISO string) with at least one actually-played
    game anywhere in the log, or None if the log is missing/empty/all
    DNP placeholders."""
    try:
        log = json.loads(log_path.read_text())
    except Exception:
        return None
    if not isinstance(log, dict):
        return None
    for day in sorted(log, reverse=True):
        if any(_is_real_game(e) for e in (log[day] or [])):
            return day
    return None


def season_is_active(today: date | None = None, log_path: Path = _GAME_LOG_PATH) -> bool:
    """False once every tracked placement has gone quiet for
    SEASON_IDLE_THRESHOLD_DAYS+ — season's over (or hasn't started).
    True as soon as a real game reappears in the log."""
    last_day = last_real_game_day(log_path)
    if last_day is None:
        return False
    if today is None:
        today = datetime.now(_ET).date()
    try:
        last = date.fromisoformat(last_day)
    except ValueError:
        return False
    return (today - last).days < SEASON_IDLE_THRESHOLD_DAYS
