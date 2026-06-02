"""Summer-ball live pulse entries.

Reads `data/summer_ball_rosters.json` (built by the summer_rosters workflow)
and generates pulse-entry dicts for each matched player who has a summer-ball
game today/yesterday. Output shape matches build_pulse_entry()'s contract so
the main pipeline can append them directly to current_pulse.json.

Today this covers only the MLB Stats API leagues (Cape Cod, Appalachian, MLB
Draft League) — they share one clean endpoint. PrestoSports leagues (NECBL,
etc.) need their own implementation and are tagged "no live data wired yet"
in the meantime so the cards still surface the assignment.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Repo root — this file lives at src/, json files at data/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ROSTER_PATH = _REPO_ROOT / "data" / "summer_ball_rosters.json"

# MLB Stats API → our league short_name. Mirrors src/summer_ball.py classes.
# Maps to the leagueId on MLB Stats API (sportId=22 = College Baseball).
_MLB_LEAGUE_IDS = {"Cape Cod": 565, "Appalachian": 120, "MLB Draft": 5536}
_MLB_LEAGUES = set(_MLB_LEAGUE_IDS.keys())

# Leagues we know about but haven't wired live stats for yet. Cards still get
# generated (with a "No live stats yet" status) so the assignment is visible.
_STUB_LEAGUES = {"NECBL", "Cal Ripken", "PGCBL", "FCBL", "Prospect"}

_STATSAPI = "https://statsapi.mlb.com/api/v1"
_ET = timezone(timedelta(hours=-4))  # EDT for summer; close enough for display

_session = requests.Session()


def _today_et() -> date:
    return datetime.now(_ET).date()


def _yesterday_et() -> date:
    return _today_et() - timedelta(days=1)


def _load_roster() -> Optional[dict]:
    if not _ROSTER_PATH.exists():
        return None
    try:
        with open(_ROSTER_PATH) as f:
            return json.load(f)
    except Exception:
        logger.exception("summer_pulse: failed to read %s", _ROSTER_PATH)
        return None


def _schedule_for_date(sport_id: int, target: date) -> list[dict]:
    """Return all games on `target` for a given MLB sportId.

    sportId=22 covers Cape Cod, Appalachian, MLB Draft League etc.
    """
    try:
        url = (
            f"{_STATSAPI}/schedule"
            f"?sportId={sport_id}&date={target.isoformat()}"
            f"&hydrate=team,linescore"
        )
        resp = _session.get(url, timeout=15).json()
        out = []
        for d in resp.get("dates", []):
            out.extend(d.get("games", []))
        return out
    except Exception:
        logger.exception("summer_pulse: schedule fetch failed for %s", target)
        return []


def _build_team_index(games: list[dict]) -> dict[int, dict]:
    """Map team_id -> game dict for fast lookup."""
    by_team: dict[int, dict] = {}
    for g in games:
        for side in ("home", "away"):
            tid = g.get("teams", {}).get(side, {}).get("team", {}).get("id")
            if tid:
                # Tag which side this team is on so we can format the matchup.
                by_team[tid] = {"game": g, "side": side}
    return by_team


def _player_line(game_pk: int, person_id: int) -> Optional[dict]:
    """Pull a player's line from a finished/live game's boxscore.

    Returns dict like {"summary": "1-3, 2B, RBI, K", "pa": 3, "hr": 0,
    "innings": None} or None if player didn't appear.
    """
    try:
        url = f"{_STATSAPI}/game/{game_pk}/boxscore"
        data = _session.get(url, timeout=15).json()
    except Exception:
        logger.exception("summer_pulse: boxscore fetch failed gamePk=%s", game_pk)
        return None
    for side in ("home", "away"):
        team_block = data.get("teams", {}).get(side, {})
        players = team_block.get("players", {})
        key = f"ID{person_id}"
        if key not in players:
            continue
        p = players[key]
        stats = p.get("stats", {})
        b = stats.get("batting", {}) or {}
        pi = stats.get("pitching", {}) or {}
        # Hitter line
        if b.get("plateAppearances"):
            ab = b.get("atBats", 0)
            h = b.get("hits", 0)
            parts = [f"{h}-{ab}"]
            extras = []
            if b.get("doubles"): extras.append(f"{b['doubles']}×2B")
            if b.get("triples"): extras.append(f"{b['triples']}×3B")
            if b.get("homeRuns"): extras.append(f"{b['homeRuns']}×HR")
            if b.get("rbi"): extras.append(f"{b['rbi']} RBI")
            if b.get("baseOnBalls"): extras.append(f"{b['baseOnBalls']} BB")
            if b.get("strikeOuts"): extras.append(f"{b['strikeOuts']} K")
            if b.get("stolenBases"): extras.append(f"{b['stolenBases']} SB")
            return {
                "summary": ", ".join(parts + extras),
                "kind": "hitter",
                "pa": b.get("plateAppearances", 0),
            }
        # Pitcher line
        if pi.get("inningsPitched"):
            ip = pi.get("inningsPitched", "0.0")
            er = pi.get("earnedRuns", 0)
            h = pi.get("hits", 0)
            bb = pi.get("baseOnBalls", 0)
            k = pi.get("strikeOuts", 0)
            return {
                "summary": f"{ip} IP, {er} ER, {h} H, {bb} BB, {k} K",
                "kind": "pitcher",
                "innings": ip,
            }
    return None


def _format_game_time(iso_str: str) -> Optional[str]:
    """ISO UTC -> '7:00 PM ET' for cards."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(_ET)
        return dt.strftime("%-I:%M %p ET")
    except Exception:
        return None


def _build_entry(
    match: dict, player_info: dict, game_block: Optional[dict],
    when_label: str, is_yesterday: bool,
) -> dict:
    """Assemble a pulse-entry dict from a matched player + game state."""
    player_name = match["player_name"]
    college = match.get("college", "")
    summer_team = match.get("summer_team", "")
    league = match.get("league", "")
    person_id = player_info.get("person_id")

    if not game_block:
        # No game today/yesterday — show the assignment with a holding card.
        return {
            "player_name": player_name,
            "team": f"{summer_team} ({league})",
            "level": "Summer",
            "stats_summary": "No game scheduled",
            "game_context": f"Summer ball — {college}",
            "game_status": "Off Day",
            "game_time": None,
            "game_date": when_label,
            "is_yesterday": is_yesterday,
            "next_game": None,
            "box_score_url": None,
            "player_profile_url": (
                f"https://www.mlb.com/player/{person_id}" if person_id else ""
            ),
            "performance_grade": "— No Data",
            "grade_reason": "",
            "social_search_url": "",
            "data_source": "MLB Stats API",
            "is_client": True,
            "tags": {
                "draft_class": "",
                "position": "",
                "roster_priority": 99,
                "summer_college": college,
                "summer_league": league,
            },
        }

    game = game_block["game"]
    game_pk = game.get("gamePk")
    side = game_block["side"]
    state = game.get("status", {}).get("detailedState", "Scheduled")
    abstract = game.get("status", {}).get("abstractGameState", "Preview")
    home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
    away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
    matchup = f"{away} @ {home}"

    # Status normalization to match the pulse UI's vocabulary.
    if abstract == "Final":
        game_status = "Final"
    elif abstract == "Live":
        game_status = "In Progress"
    elif state == "Postponed":
        game_status = "Postponed"
    elif state == "Cancelled":
        game_status = "Cancelled"
    else:
        game_status = "Scheduled"

    game_time = _format_game_time(game.get("gameDate", ""))

    # Player line — only meaningful when game has actually played.
    summary = ""
    if game_status in ("In Progress", "Final") and person_id:
        line = _player_line(game_pk, person_id)
        if line:
            summary = line["summary"]
        else:
            summary = "Did not appear"
    elif game_status == "Scheduled":
        summary = f"Game at {game_time}" if game_time else "Scheduled"
    elif game_status == "Postponed":
        summary = "Postponed"
    elif game_status == "Cancelled":
        summary = "Cancelled"

    return {
        "player_name": player_name,
        "team": f"{summer_team} ({league})",
        "level": "Summer",
        "stats_summary": summary,
        "game_context": matchup,
        "game_status": game_status,
        "game_time": game_time,
        "game_date": when_label,
        "is_yesterday": is_yesterday,
        "next_game": None,
        "box_score_url": (
            f"https://www.mlb.com/gameday/{game_pk}" if game_pk else None
        ),
        "player_profile_url": (
            f"https://www.mlb.com/player/{person_id}" if person_id else ""
        ),
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "MLB Stats API",
        "is_client": True,
        "tags": {
            "draft_class": "",
            "position": "",
            "roster_priority": 99,
            "summer_college": college,
            "summer_league": league,
        },
    }


def _stub_entry(match: dict) -> dict:
    """Holding card for matched players in leagues we haven't wired stats for."""
    return {
        "player_name": match["player_name"],
        "team": f"{match['summer_team']} ({match['league']})",
        "level": "Summer",
        "stats_summary": "Live stats coming — adapter pending",
        "game_context": f"Summer ball — {match.get('college','')}",
        "game_status": "Roster Confirmed",
        "game_time": None,
        "game_date": None,
        "is_yesterday": False,
        "next_game": None,
        "box_score_url": None,
        "player_profile_url": "",
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "Roster snapshot",
        "is_client": True,
        "tags": {
            "draft_class": "",
            "position": "",
            "roster_priority": 99,
            "summer_college": match.get("college", ""),
            "summer_league": match.get("league", ""),
        },
    }


def _resolve_person_ids(matches: list[dict]) -> dict[str, int]:
    """For MLB-API-league matches, look up each player's MLB person_id by name.

    We stored raw player IDs in the roster file under source_id for MLB-API
    leagues — but the matcher output only carries player_name, not source_id.
    Re-resolve by name against the roster file's full player list (which is
    NOT persisted today — we'd need to extend summer_ball.py). For now,
    fall back to MLB's people search endpoint.
    """
    out: dict[str, int] = {}
    for m in matches:
        name = m.get("player_name", "")
        if not name:
            continue
        try:
            url = f"{_STATSAPI}/people/search?names={name.replace(' ', '+')}&sportIds=22"
            resp = _session.get(url, timeout=10).json()
            people = resp.get("people", []) or []
            if people:
                out[name] = people[0].get("id")
        except Exception:
            continue
    return out


def build_summer_pulse_entries() -> list[dict]:
    """Top-level entry point. Returns pulse entries for all matched summer
    players (today + yesterday for MLB-API leagues; stub cards for others).
    """
    roster = _load_roster()
    if not roster:
        logger.info("summer_pulse: no roster file at %s", _ROSTER_PATH)
        return []

    matches: list[dict] = roster.get("matched", [])
    if not matches:
        return []

    today = _today_et()
    yesterday = _yesterday_et()

    # Pull today + yesterday games once across MLB Stats API leagues (sportId 22).
    today_games = _schedule_for_date(22, today)
    yesterday_games = _schedule_for_date(22, yesterday)
    today_by_team = _build_team_index(today_games)
    yesterday_by_team = _build_team_index(yesterday_games)

    person_ids = _resolve_person_ids(
        [m for m in matches if m.get("league") in _MLB_LEAGUES]
    )

    # Build summer_team_abbr -> team_id per league. Cached so we hit MLB's
    # team list once per league per pulse run.
    team_id_by_abbr_by_league: dict[str, dict[str, int]] = {}
    for league_name in _MLB_LEAGUES:
        team_id_by_abbr_by_league[league_name] = _team_abbr_index(league_name)

    entries: list[dict] = []
    for m in matches:
        league = m.get("league", "")
        player_name = m.get("player_name", "")
        if league in _STUB_LEAGUES:
            entries.append(_stub_entry(m))
            continue
        if league not in _MLB_LEAGUES:
            continue

        person_id = person_ids.get(player_name)
        player_info = {"person_id": person_id}

        # Resolve summer team abbreviation -> team_id using the per-league
        # index built once above.
        summer_team_abbr = m.get("summer_team", "")
        team_id = team_id_by_abbr_by_league.get(league, {}).get(summer_team_abbr)

        if team_id and team_id in today_by_team:
            entries.append(_build_entry(
                m, player_info, today_by_team[team_id],
                when_label=today.isoformat(), is_yesterday=False,
            ))
        elif team_id and team_id in yesterday_by_team:
            entries.append(_build_entry(
                m, player_info, yesterday_by_team[team_id],
                when_label=yesterday.isoformat(), is_yesterday=True,
            ))
        else:
            # No game today or yesterday — emit the off-day card so the player
            # still appears on the Summer tab.
            entries.append(_build_entry(
                m, player_info, None,
                when_label=today.isoformat(), is_yesterday=False,
            ))

    logger.info(
        "summer_pulse: built %d entries (%d MLB-API, %d stub)",
        len(entries),
        sum(1 for e in entries if e.get("data_source") == "MLB Stats API"),
        sum(1 for e in entries if e.get("data_source") == "Roster snapshot"),
    )
    return entries


def _team_abbr_index(league_short_name: str) -> dict[str, int]:
    """Return {team_abbreviation: team_id} for an MLB-API summer league.

    The roster file stores summer_team as the MLB-API team abbreviation
    (e.g. "HAR" for Harwich, "WIL" for Williamsport). We need the team_id
    to look up today's game from the schedule. /people/{id}?hydrate=currentTeam
    returns the pro affiliate (Astros, Rockies, etc.) — NOT the summer team —
    so we go via the league's team list directly.
    """
    league_id = _MLB_LEAGUE_IDS.get(league_short_name)
    if not league_id:
        return {}
    season = _today_et().year
    try:
        url = (
            f"{_STATSAPI}/teams"
            f"?sportIds=22&leagueIds={league_id}&season={season}"
        )
        resp = _session.get(url, timeout=15).json()
    except Exception:
        logger.exception("summer_pulse: team list failed for %s", league_short_name)
        return {}
    out: dict[str, int] = {}
    for t in resp.get("teams", []) or []:
        abbr = t.get("abbreviation", "")
        tid = t.get("id")
        if abbr and tid:
            out[abbr] = tid
    return out
