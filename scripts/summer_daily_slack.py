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
_SEASON_PATH = _REPO_ROOT / "data" / "window_season.json"
_SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
_ET = timezone(timedelta(hours=-4))  # EDT for summer


def _season_lookup() -> dict[str, str]:
    """Map player_name → short season-to-date summary, pulled from
    window_season.json. Empty string for placements with no stats yet."""
    raw = _load(_SEASON_PATH)
    out: dict[str, str] = {}
    for e in raw:
        if e.get("level") != "Summer":
            continue
        name = e.get("player_name", "")
        summary = e.get("stats_summary", "") or ""
        if not name or not summary or "No games yet" in summary:
            continue
        out[name] = _shorten_season(summary)
    return out


def _shorten_season(s: str) -> str:
    """Trim window_season.json's stats_summary to a recap-friendly clause.

    Hitters:  '18-59, 0 HR, 14 RBI · .305/.387/.458 (17 G)'
              → '17 G, .305/.387/.458, 0 HR, 14 RBI'
    Pitchers: '14.0 IP, 2 ER, 11 K, 3 BB · ERA 1.29 (3 G)'
              → '3 G, 14.0 IP, 1.29 ERA, 11 K'
    """
    import re
    g_match = re.search(r"\((\d+)\s*G\)", s)
    games = g_match.group(1) + " G" if g_match else ""
    # Pitcher: contains "IP"
    if " IP" in s:
        ip_m = re.search(r"([\d.]+)\s*IP", s)
        era_m = re.search(r"ERA\s*([\d.]+)", s)
        k_m = re.search(r"(\d+)\s*K", s)
        bits = [games] if games else []
        if ip_m: bits.append(f"{ip_m.group(1)} IP")
        if era_m: bits.append(f"{era_m.group(1)} ERA")
        if k_m: bits.append(f"{k_m.group(1)} K")
        return ", ".join(b for b in bits if b)
    # Hitter
    slash_m = re.search(r"(\.\d{3}/\.\d{3}/\.\d{3})", s)
    hr_m = re.search(r"(\d+)\s*HR", s)
    rbi_m = re.search(r"(\d+)\s*RBI", s)
    bits = [games] if games else []
    if slash_m: bits.append(slash_m.group(1))
    if hr_m: bits.append(f"{hr_m.group(1)} HR")
    if rbi_m: bits.append(f"{rbi_m.group(1)} RBI")
    return ", ".join(b for b in bits if b)


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


def _team_md(e: dict) -> str:
    """Format the team name as a Slack mrkdwn link when we have a league-site
    URL, plain text otherwise. Slack link syntax: <url|label>."""
    team = e.get("team", "")
    if "(" in team:
        team = team.split("(")[0].strip()
    url = ((e.get("tags") or {}).get("league_site_url") or "").strip()
    if url:
        return f"<{url}|{team}>"
    return team


def _format_yesterday_line(e: dict, season_map: dict[str, str] | None = None) -> str:
    name = e.get("player_name", "?")
    summary = e.get("stats_summary", "")
    if not summary or summary in ("No game today", "No games yet — season just opened"):
        return ""
    season_tail = ""
    if season_map:
        season = season_map.get(name)
        if season:
            season_tail = f"  ·  _Season: {season}_"
    return f"• *{name}* ({_team_md(e)}): {summary}{season_tail}"


def _format_today_line(e: dict) -> str:
    name = e.get("player_name", "?")
    status = e.get("game_status", "")
    team = _team_md(e)
    if status == "Scheduled":
        when = e.get("game_time") or "TBD"
        matchup = e.get("game_context") or ""
        return f"• *{name}* ({team}): {matchup} — {when}"
    if status in ("In Progress", "Live"):
        return f"• *{name}* ({team}): :red_circle: live — {e.get('stats_summary','')}"
    if status == "Final":
        return f"• *{name}* ({team}): Final — {e.get('stats_summary','')}"
    return ""


# Summer leagues we pull live game data from automatically. Placements in any
# other league (Northwoods, PGCBL, FCBL, Coastal Plain, Prospect, Cal Ripken,
# etc.) are tracked by hand — no automated stats will ever appear here for them,
# so we shouldn't imply they're "coming soon."
_REACHABLE_LEAGUES = {"Cape Cod", "MLB Draft", "Appalachian", "NECBL"}


def _no_data_active_placements(
    today_entries: list[dict], yest_entries: list[dict]
) -> list[tuple[str, str]]:
    """Active placements (Confirmed / 2nd Half) for whom we have no data
    today or yesterday. Returns (player_name, league) sorted by name so the
    caller can split "league we can't reach" from "just idle today."
    """
    yest_finals = {
        e.get("player_name") for e in yest_entries
        if e.get("game_status") in ("Final", "In Progress")
    }
    out = []
    seen = set()
    for e in today_entries:
        name = e.get("player_name")
        if not name or name in seen:
            continue
        status = (e.get("tags") or {}).get("placement_status", "")
        if status not in ("Confirmed", "2nd Half", ""):
            continue
        # Skip if today has a real game or yesterday had a final.
        if e.get("game_status") in ("Scheduled", "In Progress", "Final", "Live"):
            continue
        if name in yest_finals:
            continue
        seen.add(name)
        league = (e.get("tags") or {}).get("summer_league", "") or ""
        out.append((name, league))
    return sorted(out)


def build_message() -> str:
    yest = _summer_clients(_load(_YESTERDAY_PATH))
    today = _summer_clients(_load(_CURRENT_PATH))
    season_map = _season_lookup()

    # Yesterday: finals only.
    yest_lines = []
    for e in sorted(yest, key=lambda x: x.get("player_name", "")):
        if e.get("game_status") not in ("Final", "In Progress"):
            continue
        line = _format_yesterday_line(e, season_map)
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

    # Client-vs-client matchups: games where 2+ clients are scheduled.
    # Differentiate teammates (same team) from opponents (different teams
    # in the same game) — only opponents are "facing each other"; same-team
    # groupings just get a "same game" note.
    matchup_callouts: list[str] = []
    by_game: dict[str, list[dict]] = {}
    for e in today:
        if e.get("is_yesterday"):
            continue
        if e.get("game_status") != "Scheduled":
            continue
        game = (e.get("game_context") or "").strip()
        if not game or not e.get("player_name"):
            continue
        by_game.setdefault(game, []).append(e)
    for game, entries in sorted(by_game.items()):
        if len(entries) < 2:
            continue
        # Group by team within this game.
        teams: dict[str, list[str]] = {}
        for e in entries:
            team = (e.get("team") or "").split("(")[0].strip()
            teams.setdefault(team, []).append(e.get("player_name", ""))
        when = entries[0].get("game_time") or ""
        when_suffix = f" ({when})" if when else ""
        if len(teams) >= 2:
            # Opponents — the real "client vs client" scenario.
            sides = " vs ".join(
                f"{', '.join(f'*{n}*' for n in sorted(set(names)))} ({team})"
                for team, names in teams.items()
            )
            matchup_callouts.append(f"• :crossed_swords: Client-vs-client: {sides} — {game}{when_suffix}")
        else:
            team, names = next(iter(teams.items()))
            names_md = ", ".join(f"*{n}*" for n in sorted(set(names)))
            matchup_callouts.append(f"• :handshake: {names_md} ({team}) — {game}{when_suffix}")

    if today_lines:
        parts.append(f"\n*Coming up today ({today_date}):*")
        if matchup_callouts:
            parts.append("_Stacked games (multiple clients in one matchup):_")
            parts.extend(matchup_callouts)
            parts.append("")
        parts.extend(today_lines)
    else:
        parts.append(f"\n*Coming up today ({today_date}):*  _no client summer games on the schedule_")

    # Honest split: mid-season, "leagues will open" is stale. Separate the
    # placements in leagues we simply can't pull automatically (tracked by
    # hand — won't ever show here) from reachable-league guys who were just
    # idle in the last day.
    no_data = _no_data_active_placements(today, yest)
    if no_data:
        unreachable = [(n, lg) for n, lg in no_data if lg not in _REACHABLE_LEAGUES]
        idle = [n for n, lg in no_data if lg in _REACHABLE_LEAGUES]
        if unreachable:
            by_lg: dict[str, list[str]] = {}
            for n, lg in unreachable:
                by_lg.setdefault(lg or "league TBD", []).append(n)
            grouped = "; ".join(
                f"{', '.join(sorted(names))} ({lg})"
                for lg, names in sorted(by_lg.items())
            )
            parts.append(
                f"\n_Tracked by hand — we can't pull these leagues automatically, "
                f"so check the league/team site directly: {grouped}._"
            )
        if idle:
            names_md = ", ".join(sorted(idle))
            parts.append(
                f"\n_No game in the last day for {names_md} — just idle; "
                f"they'll reappear the next time they play._"
            )

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
