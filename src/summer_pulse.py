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
# Manual placements from Kent's spreadsheet — the source of truth for who
# is at what summer team. Auto-scraped roster matches augment but never
# override these. See data/summer_ball_placements.json.
_PLACEMENTS_PATH = _REPO_ROOT / "data" / "summer_ball_placements.json"
# Baseball-Reference Register stats cache, refreshed daily by
# scripts/refresh_bbref_stats. Used as next-day fallback for placements
# where our primary live source doesn't surface a current line.
_BBREF_STATS_PATH = _REPO_ROOT / "data" / "bbref_stats.json"

# Stamped on every summer entry so the UI can render "last updated X min ago"
# alongside the data source. Re-resolved at the top of build_summer_pulse_entries.
_NOW_ISO: str = ""

# MLB Stats API → our league short_name. Mirrors src/summer_ball.py classes.
# Maps to the leagueId on MLB Stats API (sportId=22 = College Baseball).
_MLB_LEAGUE_IDS = {"Cape Cod": 565, "Appalachian": 120, "MLB Draft": 5536}
_MLB_LEAGUES = set(_MLB_LEAGUE_IDS.keys())

# PrestoSports leagues — same player-page structure across all of them.
# Each subclass's `host_url` from src.summer_ball.py is the data source.
_PRESTO_HOSTS = {
    "NECBL": "https://necbl.com",
    "Cal Ripken": "https://calripkensrleague.org",
    "PGCBL": "https://pgcbl.com",
    "FCBL": "https://fcbl.prestosports.com",
    "Prospect": "https://prospectleague.com",
}
_STUB_LEAGUES = set(_PRESTO_HOSTS.keys())

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


def _load_placements() -> list[dict]:
    """Kent's manual placement spreadsheet -> list of placement dicts.

    Each entry: player_name, school, summer_team, league, status, draft_class.
    Filters out placeholder rows ("NEED PLACEMENT" etc.) — spreadsheet has
    section dividers / TBD slots we don't want to render as cards.
    """
    if not _PLACEMENTS_PATH.exists():
        logger.info("summer_pulse: no placements file at %s", _PLACEMENTS_PATH)
        return []
    try:
        with open(_PLACEMENTS_PATH) as f:
            data = json.load(f)
        out = []
        for p in data.get("placements", []):
            name = (p.get("player_name") or "").strip()
            # Reject all-caps placeholder rows that don't look like real names.
            if not name or name.isupper() and len(name) > 5:
                continue
            out.append(p)
        return out
    except Exception:
        logger.exception("summer_pulse: failed to read %s", _PLACEMENTS_PATH)
        return []


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


def _presto_entry(match: dict) -> dict:
    """Live card for a PrestoSports-league player via their player page.

    Reads /sports/bsb/{year}/players/{slug} and finds today's/yesterday's
    row in the hitter or pitcher game-log table. Falls back to a "Roster
    Confirmed" holding card if the slug is missing or the page can't be
    parsed.
    """
    league = match.get("league", "")
    host = _PRESTO_HOSTS.get(league)
    slug = match.get("source_id", "")
    if not host or not slug:
        return _holding_entry(match, reason="No player slug captured")

    today = _today_et()
    yesterday = _yesterday_et()
    url = f"{host}/sports/bsb/{today.year}/players/{slug}"
    try:
        resp = _session.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200 or not resp.text:
            return _holding_entry(match, reason=f"player page HTTP {resp.status_code}")
        html = resp.text
    except Exception:
        logger.exception("summer_pulse/presto: fetch failed for %s", url)
        return _holding_entry(match, reason="player page fetch failed")

    line = _parse_presto_player_page(html, today=today, yesterday=yesterday)
    if not line:
        return _holding_entry(match, reason="No game today/yesterday")

    return {
        "player_name": match["player_name"],
        "team": f"{match['summer_team']} ({league})",
        "level": "Summer",
        "stats_summary": line["summary"],
        "game_context": line.get("opponent", ""),
        "game_status": line["status"],
        "game_time": line.get("game_time"),
        "game_date": line.get("date_iso"),
        "is_yesterday": line.get("is_yesterday", False),
        "next_game": None,
        "box_score_url": None,
        "player_profile_url": url,
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "PrestoSports",
        "is_client": True,
        "tags": {
            "draft_class": "",
            "position": "",
            "roster_priority": 99,
            "summer_college": match.get("college", ""),
            "summer_league": league,
        },
    }


def _holding_entry(match: dict, *, reason: str = "") -> dict:
    """Fallback card when live-stats path fails for a known assignment."""
    return {
        "player_name": match["player_name"],
        "team": f"{match['summer_team']} ({match['league']})",
        "level": "Summer",
        "stats_summary": "No game today",
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
        "data_source": f"Roster snapshot ({reason})" if reason else "Roster snapshot",
        "is_client": True,
        "tags": {
            "draft_class": "",
            "position": "",
            "roster_priority": 99,
            "summer_college": match.get("college", ""),
            "summer_league": match.get("league", ""),
        },
    }


def _parse_presto_player_page(html: str, *, today: date, yesterday: date) -> Optional[dict]:
    """Look through hitter + pitcher game-log tables for today's/yesterday's row.

    Presto stores dates like "Jun 4Danbury Westerners" (concatenated when the
    opponent is in the same cell). We match by month-abbreviation + day.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    target_dates = [
        (today, today.strftime("%b %-d"), False),
        (yesterday, yesterday.strftime("%b %-d"), True),
    ]
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_cells = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
        if "date" not in header_cells:
            continue
        is_hitting = "ab" in header_cells and "pa" not in header_cells
        is_pitching = "ip" in header_cells
        if not (is_hitting or is_pitching):
            continue
        date_idx = header_cells.index("date")
        # opponent column
        opp_idx = header_cells.index("opponent") if "opponent" in header_cells else 1
        score_idx = header_cells.index("score") if "score" in header_cells else 2
        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            date_text = cells[date_idx].get_text(" ", strip=True) if len(cells) > date_idx else ""
            opp_text = cells[opp_idx].get_text(" ", strip=True) if len(cells) > opp_idx else ""
            score_text = cells[score_idx].get_text(" ", strip=True) if len(cells) > score_idx else ""
            for game_date, key, is_yest in target_dates:
                if key not in date_text:
                    continue
                # Build summary from the stat cells.
                stat_cells = [c.get_text(strip=True) for c in cells]
                if is_hitting:
                    summary = _format_hitter_log_row(header_cells, stat_cells, score_text)
                else:
                    summary = _format_pitcher_log_row(header_cells, stat_cells, score_text)
                if not summary:
                    continue
                status = "Final" if score_text else "Scheduled"
                return {
                    "summary": summary,
                    "opponent": opp_text,
                    "status": status,
                    "date_iso": game_date.isoformat(),
                    "is_yesterday": is_yest,
                }
    return None


def _format_hitter_log_row(headers: list[str], cells: list[str], score: str) -> Optional[str]:
    def cv(name: str) -> str:
        i = headers.index(name) if name in headers else -1
        return cells[i] if 0 <= i < len(cells) else ""

    ab = cv("ab")
    h = cv("h")
    if not ab or ab == "-":
        return "In lineup" if score else None
    parts = [f"{h}-{ab}"]
    for stat, label in [("2b", "2B"), ("3b", "3B"), ("hr", "HR"),
                         ("rbi", "RBI"), ("bb", "BB"), ("k", "K"), ("sb", "SB")]:
        v = cv(stat)
        if v and v != "-" and v != "0":
            if stat in {"2b", "3b", "hr"} and v.isdigit() and int(v) > 1:
                parts.append(f"{v}×{label}")
            elif stat in {"2b", "3b", "hr"}:
                parts.append(label)
            else:
                parts.append(f"{v} {label}")
    return ", ".join(parts)


def _format_pitcher_log_row(headers: list[str], cells: list[str], score: str) -> Optional[str]:
    def cv(name: str) -> str:
        i = headers.index(name) if name in headers else -1
        return cells[i] if 0 <= i < len(cells) else ""

    ip = cv("ip")
    if not ip or ip == "-":
        return None
    er = cv("er") or "0"
    h = cv("h") or "0"
    bb = cv("bb") or "0"
    k = cv("k") or "0"
    return f"{ip} IP, {er} ER, {h} H, {bb} BB, {k} K"


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
    """Top-level entry point. Returns pulse entries for every player in
    Kent's manual placement spreadsheet (data/summer_ball_placements.json),
    enriched with live stats from MLB API / PrestoSports where available.

    Placements are the source of truth. Auto-scraped matches supply
    source_id / MLB person_id when names match.
    """
    placements = _load_placements()
    if not placements:
        logger.info("summer_pulse: no placements; nothing to render")
        return []

    global _NOW_ISO
    _NOW_ISO = datetime.now(timezone.utc).isoformat()

    # Build name-keyed index of auto-scraped matches for source_id lookup.
    roster = _load_roster() or {}
    matches = roster.get("matched", [])
    auto_by_name: dict[str, dict] = {}
    for m in matches:
        key = (m.get("player_name") or "").strip().lower()
        if key:
            auto_by_name[key] = m

    today = _today_et()
    yesterday = _yesterday_et()

    # Pre-fetch MLB schedule for sportId 22 once per pulse run.
    today_games = _schedule_for_date(22, today)
    yesterday_games = _schedule_for_date(22, yesterday)
    today_by_team = _build_team_index(today_games)
    yesterday_by_team = _build_team_index(yesterday_games)

    # MLB team-abbreviation lookup per league (placement records use team
    # FULL names like "Williamsport Crosscutters"; MLB API gives both
    # name and abbreviation).
    name_to_team_id_by_league: dict[str, dict[str, int]] = {}
    for league_name in _MLB_LEAGUES:
        name_to_team_id_by_league[league_name] = _team_name_to_id(league_name)

    entries: list[dict] = []
    for p in placements:
        name = (p.get("player_name") or "").strip()
        if not name:
            continue
        status = p.get("status", "")
        league = p.get("league", "")
        if status == "Shut Down":
            entries.append(_status_only_card(p, reason="Shut down for season"))
            continue
        if status == "Injured":
            entries.append(_status_only_card(p, reason="Injured"))
            continue
        if not p.get("summer_team") or not league:
            entries.append(_status_only_card(p, reason=status or "No team yet"))
            continue

        auto = auto_by_name.get(name.lower(), {})

        if league in _MLB_LEAGUES:
            # Emit BOTH today's card and yesterday's card when relevant —
            # the Today tab shows today's, Yesterday tab shows yesterday's,
            # 7D rolls them up. Without the yesterday card, Kent's Yesterday
            # tab is empty (reported 2026-06-04).
            today_entry, yest_entry = _mlb_cards_for_placement(
                p, auto,
                today_by_team=today_by_team,
                yesterday_by_team=yesterday_by_team,
                team_name_to_id=name_to_team_id_by_league.get(league, {}),
                today=today, yesterday=yesterday,
            )
            if today_entry:
                entries.append(today_entry)
            if yest_entry:
                entries.append(yest_entry)
        elif league in _STUB_LEAGUES:
            entries.append(_presto_card_from_placement(p, auto))
        else:
            # Northwoods / Coastal Plain / etc. — no scraper, show static card.
            entries.append(_static_placement_card(p))

    # Group cards by league so the UI shows them in a logical order:
    # MLB-API leagues first (most reliable data), then PrestoSports leagues,
    # then BBRef-only / static placements, then status-flagged at the bottom.
    league_order = {
        "MLB Draft": 0, "Cape Cod": 1, "Appalachian": 2,
        "NECBL": 3, "Cal Ripken": 4, "PGCBL": 5, "FCBL": 6, "Prospect": 7,
        "Coastal Plain": 8, "Northwoods": 9,
    }

    def _sort_key(e: dict) -> tuple:
        status = e.get("game_status", "")
        is_inactive = status in (
            "Shut Down", "Injured", "Pending, 1st Half",
            "Pending, 2nd Half", "2nd Half", "Status Update",
        )
        league = (e.get("tags") or {}).get("summer_league", "")
        return (
            1 if is_inactive else 0,
            league_order.get(league, 99),
            league,
            e.get("player_name", ""),
        )

    entries.sort(key=_sort_key)

    # Stamp every summer entry with stats_captured_at so the UI can render
    # "last updated X min ago" next to the data source.
    for e in entries:
        e.setdefault("stats_captured_at", _NOW_ISO)

    # Cross-source agreement check — flag silent errors. Non-fatal.
    try:
        _log_cross_source_disagreements(entries)
    except Exception:
        logger.exception("summer_pulse: cross-source check failed (non-fatal)")

    logger.info(
        "summer_pulse: built %d entries from %d placements",
        len(entries), len(placements),
    )

    try:
        _append_to_game_log(entries)
    except Exception:
        logger.exception("summer_pulse: game-log append failed (non-fatal)")

    # Write Summer entries into window_7d.json + window_season.json so the
    # 7 Days / Season tabs surface summer-ball stats. Non-fatal — if either
    # write fails, today's pulse still ships.
    try:
        _write_summer_window_entries(placements, auto_by_name)
    except Exception:
        logger.exception("summer_pulse: window-window write failed (non-fatal)")

    return entries


def _write_summer_window_entries(placements: list[dict], auto_by_name: dict) -> None:
    """For each MLB-API summer placement, fetch last-7-day + season-to-date
    stats and append a Summer-level entry to window_7d.json + window_season.json.

    Other summer leagues (Presto/Northwoods/CPL etc.) get a roster-only entry
    so Kent sees the placement even before stats accumulate.
    """
    from src.baseball_reference import _STATS_CACHE_PATH as _BBREF_PATH  # noqa
    today = _today_et()
    week_start = today - timedelta(days=7)
    season_start = date(today.year, 1, 1)

    bbref_data: dict = {}
    if _BBREF_PATH.exists():
        try:
            bbref_data = json.loads(_BBREF_PATH.read_text()).get("players", {})
        except Exception:
            bbref_data = {}

    week_entries: list[dict] = []
    season_entries: list[dict] = []
    for p in placements:
        name = (p.get("player_name") or "").strip()
        if not name:
            continue
        status = p.get("status", "")
        if status in ("Shut Down", "Injured"):
            continue
        league = p.get("league", "")
        summer_team = p.get("summer_team", "")
        if not summer_team or not league:
            continue
        auto = auto_by_name.get(name.lower(), {})

        # MLB-API leagues — query byDateRange for real numbers.
        if league in _MLB_LEAGUES:
            person_id = None
            src = auto.get("source_id") if auto else None
            if src and str(src).isdigit():
                person_id = int(src)
            week_line = _summer_window_line(person_id, week_start, today) if person_id else None
            season_line = _summer_window_line(person_id, season_start, today) if person_id else None
            week_entries.append(_summer_window_entry(p, "7d", week_line))
            season_entries.append(_summer_window_entry(p, "season", season_line))
        else:
            # Non-MLB-API league. BBRef cache has season-to-date if we
            # resolved their ID.
            bbref = bbref_data.get(name)
            summary = bbref.get("summary") if bbref and bbref.get("summer_team") else None
            week_entries.append(_summer_window_entry(p, "7d", None))
            season_entries.append(_summer_window_entry(p, "season", summary))

    _merge_summer_into_window(_REPO_ROOT / "data" / "window_7d.json", week_entries)
    _merge_summer_into_window(_REPO_ROOT / "data" / "window_season.json", season_entries)
    logger.info(
        "summer_pulse: window write — %d 7d entries, %d season entries",
        len(week_entries), len(season_entries),
    )


def _summer_window_entry(p: dict, window: str, summary: Optional[str]) -> dict:
    """Build a window entry (matches window_7d.json shape) for one placement."""
    status = p.get("status", "")
    stats_summary = summary or (
        f"{status}" if status and status != "Confirmed"
        else "No games yet — season just opened"
    )
    return {
        "player_name": p["player_name"],
        "team": f"{p['summer_team']} ({p['league']})",
        "level": "Summer",
        "is_client": True,
        "tags": {
            "position": "",
            "draft_class": p.get("draft_class", ""),
            "roster_priority": 99,
            "summer_college": p.get("school", ""),
            "summer_league": p.get("league", ""),
            "placement_status": status,
        },
        "window": window,
        "window_grade": "— No Data",
        "stats": {},
        "stats_summary": stats_summary,
        "games_played": 0,
        "last_updated": _NOW_ISO,
    }


def _merge_summer_into_window(path, summer_entries: list[dict]) -> None:
    """Read an existing window file, strip prior Summer-level entries (so
    re-runs don't duplicate), append fresh Summer entries, write back.
    """
    existing: list[dict] = []
    if path.exists():
        try:
            with open(path) as f:
                existing = json.load(f) or []
        except Exception:
            existing = []
    non_summer = [e for e in existing if e.get("level") != "Summer"]
    merged = non_summer + summer_entries
    with open(path, "w") as f:
        json.dump(merged, f, indent=2)


def _log_cross_source_disagreements(entries: list[dict]) -> None:
    """For each live-sourced Summer entry, compare against BBRef cache and
    log when they disagree on league. Catches stale rosters quietly.
    """
    if _BBREF_STATS_LOADED is None:
        return
    bbref_players = (_BBREF_STATS_LOADED or {}).get("players", {})
    for e in entries:
        if e.get("level") != "Summer":
            continue
        if e.get("data_source") in ("Kent placements", "Baseball-Reference Register"):
            continue
        bbref = bbref_players.get(e.get("player_name", ""))
        if not bbref or not bbref.get("summer_team"):
            continue
        bbref_league = (bbref.get("league") or "").lower()
        live_league = ((e.get("tags") or {}).get("summer_league") or "").lower()
        if bbref_league and live_league and bbref_league != live_league:
            logger.warning(
                "cross-source disagreement for %s: live says %s, BBRef says %s",
                e.get("player_name"), live_league, bbref_league,
            )


def _append_to_game_log(entries: list[dict]) -> None:
    """Append today's Final/In-Progress summer entries to a per-date game log.

    File at data/summer_game_log.json keeps a {date_iso: [entry, ...]} dict.
    Subsequent runs on the same day OVERWRITE that date's entries (avoids
    duplication; the latest pulse always wins). Used for:
    - Historical record (so a player's June stats are recoverable even if
      the upstream source rotates a slug).
    - Cross-source verification (when two sources disagree on the same
      player+date, we can flag for review).
    """
    path = _REPO_ROOT / "data" / "summer_game_log.json"
    log: dict = {}
    if path.exists():
        try:
            with open(path) as f:
                log = json.load(f)
        except Exception:
            log = {}

    today_key = _today_et().isoformat()
    yest_key = _yesterday_et().isoformat()

    bucket_today: list[dict] = []
    bucket_yest: list[dict] = []
    for e in entries:
        if e.get("game_status") not in ("Final", "In Progress"):
            continue
        record = {
            "player_name": e["player_name"],
            "team": e["team"],
            "level": e["level"],
            "game_status": e["game_status"],
            "stats_summary": e["stats_summary"],
            "game_context": e.get("game_context", ""),
            "data_source": e.get("data_source", ""),
            "college": (e.get("tags") or {}).get("summer_college", ""),
            "league": (e.get("tags") or {}).get("summer_league", ""),
        }
        if e.get("is_yesterday"):
            bucket_yest.append(record)
        else:
            bucket_today.append(record)

    if bucket_today:
        log[today_key] = bucket_today
    if bucket_yest:
        # Only fill yesterday's bucket if we don't already have it from a
        # prior run — yesterday's data shouldn't change post-hoc except for
        # source corrections.
        log.setdefault(yest_key, bucket_yest)

    if log:
        with open(path, "w") as f:
            json.dump(log, f, indent=2, sort_keys=True)
        logger.info(
            "summer_pulse: game-log -> %s entries today, %s entries yesterday",
            len(bucket_today), len(bucket_yest),
        )


def _team_name_to_id(league_short_name: str) -> dict[str, int]:
    """Like _team_abbr_index but keyed by full team name (lowercased)."""
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
        tid = t.get("id")
        if not tid:
            continue
        for key in (t.get("name"), t.get("teamName"), t.get("locationName")):
            if key:
                out[key.lower()] = tid
    return out


def _summer_window_line(person_id: int, start: date, end: date) -> Optional[str]:
    """Pull a hitter/pitcher line from MLB Stats API for a date range.
    Used to populate 7D / Season Summer cards."""
    if not person_id:
        return None
    try:
        url = (
            f"{_STATSAPI}/people/{person_id}/stats"
            f"?stats=byDateRange&startDate={start.isoformat()}"
            f"&endDate={end.isoformat()}&group=hitting,pitching"
        )
        resp = _session.get(url, timeout=10).json()
        parts: list[str] = []
        for group in resp.get("stats", []) or []:
            kind = group.get("group", {}).get("displayName", "")
            splits = group.get("splits", []) or []
            if not splits:
                continue
            s = splits[0].get("stat", {})
            if kind == "hitting" and s.get("plateAppearances"):
                parts.append(
                    f"{s.get('hits',0)}-{s.get('atBats',0)}, "
                    f"{s.get('homeRuns',0)} HR, {s.get('rbi',0)} RBI, "
                    f"{s.get('strikeOuts',0)} K · "
                    f"{s.get('avg','-')}/{s.get('obp','-')}/{s.get('slg','-')}"
                )
            elif kind == "pitching" and s.get("inningsPitched"):
                parts.append(
                    f"{s.get('inningsPitched','0.0')} IP, "
                    f"{s.get('earnedRuns',0)} ER, "
                    f"{s.get('strikeOuts',0)} K, "
                    f"{s.get('baseOnBalls',0)} BB · "
                    f"ERA {s.get('era','-')}, WHIP {s.get('whip','-')}"
                )
        return " · ".join(parts) if parts else None
    except Exception:
        return None


def _mlb_cards_for_placement(
    p: dict, auto: dict, *,
    today_by_team: dict, yesterday_by_team: dict,
    team_name_to_id: dict[str, int],
    today: date, yesterday: date,
) -> tuple[Optional[dict], Optional[dict]]:
    """Returns (today_card, yesterday_card) for an MLB-Stats-API placement.

    today_card is always returned (with off-day fallback if no game today).
    yesterday_card is returned only when the team actually played yesterday —
    so the Yesterday tab has real Finals to show.
    """
    summer_team_name = (p.get("summer_team") or "").strip()
    team_id = team_name_to_id.get(summer_team_name.lower())
    person_id = None
    src = auto.get("source_id") if auto else None
    if src and str(src).isdigit():
        person_id = int(src)
    if not person_id:
        try:
            url = f"{_STATSAPI}/people/search?names={p['player_name'].replace(' ', '+')}&sportIds=22"
            resp = _session.get(url, timeout=10).json()
            people = resp.get("people", []) or []
            if people:
                person_id = people[0].get("id")
        except Exception:
            pass

    info = {"person_id": person_id}

    today_card = None
    yest_card = None

    if team_id and team_id in today_by_team:
        today_card = _build_placement_entry(p, info, today_by_team[team_id], today, is_yesterday=False)
    else:
        today_card = _build_placement_entry(p, info, None, today, is_yesterday=False)

    if team_id and team_id in yesterday_by_team:
        yest_card = _build_placement_entry(p, info, yesterday_by_team[team_id], yesterday, is_yesterday=True)

    return today_card, yest_card


def _build_placement_entry(
    p: dict, info: dict, game_block: Optional[dict],
    when: date, is_yesterday: bool,
) -> dict:
    """Same shape as _build_entry but reads team/college/league/status from a
    placement dict instead of an auto-match dict.
    """
    person_id = info.get("person_id")
    status_note = p.get("status", "")
    if not game_block:
        sub = status_note if status_note and status_note != "Confirmed" else "No game today"
        return {
            "player_name": p["player_name"],
            "team": f"{p['summer_team']} ({p['league']})",
            "level": "Summer",
            "stats_summary": sub,
            "game_context": f"Summer ball — {p.get('school','')}",
            "game_status": status_note or "Off Day",
            "game_time": None,
            "game_date": when.isoformat(),
            "is_yesterday": is_yesterday,
            "next_game": None,
            "box_score_url": None,
            "player_profile_url": f"https://www.mlb.com/player/{person_id}" if person_id else "",
            "performance_grade": "— No Data",
            "grade_reason": "",
            "social_search_url": "",
            "data_source": "MLB Stats API (placement)",
            "is_client": True,
            "tags": {
                "draft_class": p.get("draft_class",""),
                "position": "",
                "roster_priority": 99,
                "summer_college": p.get("school",""),
                "summer_league": p.get("league",""),
                "placement_status": status_note,
            },
        }

    game = game_block["game"]
    game_pk = game.get("gamePk")
    state = game.get("status", {}).get("detailedState", "Scheduled")
    abstract = game.get("status", {}).get("abstractGameState", "Preview")
    home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
    away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
    matchup = f"{away} @ {home}"

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

    summary = ""
    if game_status in ("In Progress", "Final") and person_id:
        line = _player_line(game_pk, person_id)
        summary = line["summary"] if line else "Did not appear"
    elif game_status == "Scheduled":
        summary = f"Game at {game_time}" if game_time else "Scheduled"
    elif game_status in ("Postponed", "Cancelled"):
        summary = game_status

    return {
        "player_name": p["player_name"],
        "team": f"{p['summer_team']} ({p['league']})",
        "level": "Summer",
        "stats_summary": summary,
        "game_context": matchup,
        "game_status": game_status,
        "game_time": game_time,
        "game_date": when.isoformat(),
        "is_yesterday": is_yesterday,
        "next_game": None,
        "box_score_url": f"https://www.mlb.com/gameday/{game_pk}" if game_pk else None,
        "player_profile_url": f"https://www.mlb.com/player/{person_id}" if person_id else "",
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "MLB Stats API",
        "is_client": True,
        "tags": {
            "draft_class": p.get("draft_class",""),
            "position": "",
            "roster_priority": 99,
            "summer_college": p.get("school",""),
            "summer_league": p.get("league",""),
            "placement_status": status_note,
        },
    }


def _presto_card_from_placement(p: dict, auto: dict) -> dict:
    """PrestoSports path card built from a placement record.

    Uses the auto-match's source_id (player slug) when available. When not
    (e.g. PGCBL/FCBL adapters returned 0 rosters), tries to find the player
    in the league-wide /players index page — useful once the player has
    appeared in a game (Presto only surfaces them in the directory after
    they have stats).
    """
    host = _PRESTO_HOSTS.get(p.get("league",""))
    if not host:
        return _static_placement_card(p)
    slug = (auto.get("source_id") if auto else "") or _search_presto_slug(host, p["player_name"])
    if not slug:
        return _static_placement_card(p)
    today = _today_et()
    yesterday = _yesterday_et()
    url = f"{host}/sports/bsb/{today.year}/players/{slug}"
    try:
        resp = _session.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200 or not resp.text:
            return _static_placement_card(p)
        html = resp.text
    except Exception:
        return _static_placement_card(p)
    line = _parse_presto_player_page(html, today=today, yesterday=yesterday)
    if not line:
        # No game today/yesterday — keep the placement card.
        return _static_placement_card(p, player_url=url)
    return {
        "player_name": p["player_name"],
        "team": f"{p['summer_team']} ({p['league']})",
        "level": "Summer",
        "stats_summary": line["summary"],
        "game_context": line.get("opponent", ""),
        "game_status": line["status"],
        "game_time": line.get("game_time"),
        "game_date": line.get("date_iso"),
        "is_yesterday": line.get("is_yesterday", False),
        "next_game": None,
        "box_score_url": None,
        "player_profile_url": url,
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "PrestoSports",
        "is_client": True,
        "tags": {
            "draft_class": p.get("draft_class",""),
            "position": "",
            "roster_priority": 99,
            "summer_college": p.get("school",""),
            "summer_league": p.get("league",""),
            "placement_status": p.get("status",""),
        },
    }


# In-process cache for league-wide player-index lookups. Each league's
# /players index is ~5MB; only fetch once per pulse run.
_PRESTO_INDEX_CACHE: dict[str, str] = {}


def _search_presto_slug(host: str, player_name: str) -> Optional[str]:
    """Find a PrestoSports player slug by name in the league-wide players
    leaderboard. Useful when our roster adapter returned 0 (e.g. PGCBL
    Cloudflare-gated or FCBL blocked) but the player has appeared in a
    game and now has a profile in the leaderboard.
    """
    if not host or not player_name:
        return None
    year = _today_et().year
    # PGCBL/Prospect use academic-year format (2025-26); others use calendar.
    candidates = [str(year), f"{year-1}-{str(year)[-2:]}"]
    for ystr in candidates:
        cache_key = f"{host}|{ystr}"
        if cache_key not in _PRESTO_INDEX_CACHE:
            url = f"{host}/sports/bsb/{ystr}/players"
            try:
                resp = _session.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200 and len(resp.text) > 50000:
                    _PRESTO_INDEX_CACHE[cache_key] = resp.text
                    continue
            except Exception:
                pass
            _PRESTO_INDEX_CACHE[cache_key] = ""
    # Search both year-format caches for the player.
    import re
    for ystr in candidates:
        html = _PRESTO_INDEX_CACHE.get(f"{host}|{ystr}", "")
        if not html:
            continue
        # Slug pattern: lowercase firstname + lastname + hash.
        normalized = re.sub(r"[^a-z]", "", player_name.lower())
        # Slugs are alphanumeric; look for any link whose slug starts with the
        # collapsed name or contains the lastname.
        m = re.search(
            rf"/sports/bsb/{re.escape(ystr)}/players/({re.escape(normalized)}[a-z0-9]+)",
            html,
        )
        if m:
            return m.group(1)
    return None


def _static_placement_card(p: dict, *, player_url: str = "") -> dict:
    """Placement-only card when there's no live-stats path (Northwoods etc.)
    or when the live path is wired but no game is happening today/yesterday.

    Fallback order:
    1. Baseball-Reference Register cache (data/bbref_stats.json) — primary
       day-after source. Sanctioned, free, covers all wood-bat leagues.
    2. The Baseball Cube — Cloudflare-blocked through our proxy stack as
       of 2026-06-03, kept for the day BBC stops requiring JS challenge.
    """
    status = p.get("status", "Confirmed")

    if status in ("Confirmed", "2nd Half"):
        # BBRef Register first — clean, sanctioned, doesn't need a proxy.
        bbref = _try_bbref_stats(p)
        if bbref:
            return _placement_card_from_bbref(p, bbref)
        # BBC is currently blocked but kept wired for the day it works.
        bbc_line = _try_baseballcube(p)
        if bbc_line:
            return {
                "player_name": p["player_name"],
                "team": f"{p['summer_team']} ({p['league']})",
                "level": "Summer",
                "stats_summary": bbc_line["summary"],
                "game_context": bbc_line.get("opponent", f"via Baseball Cube ({p.get('school','')})"),
                "game_status": bbc_line.get("status", "Final"),
                "game_time": None,
                "game_date": bbc_line.get("date_iso"),
                "is_yesterday": bbc_line.get("is_yesterday", True),
                "next_game": None,
                "box_score_url": None,
                "player_profile_url": bbc_line.get("profile_url", ""),
                "performance_grade": "— No Data",
                "grade_reason": "",
                "social_search_url": "",
                "data_source": "Baseball Cube (next-day)",
                "is_client": True,
                "tags": {
                    "draft_class": p.get("draft_class",""),
                    "position": "",
                    "roster_priority": 99,
                    "summer_college": p.get("school",""),
                    "summer_league": p.get("league",""),
                    "placement_status": status,
                },
            }

    return {
        "player_name": p["player_name"],
        "team": f"{p['summer_team']} ({p['league']})",
        "level": "Summer",
        "stats_summary": "No game today" if status == "Confirmed" else status,
        "game_context": f"Summer ball — {p.get('school','')}",
        "game_status": status,
        "game_time": None,
        "game_date": None,
        "is_yesterday": False,
        "next_game": None,
        "box_score_url": None,
        "player_profile_url": player_url,
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "Kent placements",
        "is_client": True,
        "tags": {
            "draft_class": p.get("draft_class",""),
            "position": "",
            "roster_priority": 99,
            "summer_college": p.get("school",""),
            "summer_league": p.get("league",""),
            "placement_status": status,
        },
    }


# In-process cache: avoid hitting BBC multiple times for the same player
# within a single pulse run.
_BBC_CACHE: dict[str, Optional[dict]] = {}

# Lazy-loaded once per pulse run.
_BBREF_STATS_LOADED: Optional[dict] = None


def _try_bbref_stats(p: dict) -> Optional[dict]:
    """Return BBRef-cached 2026 summer line for this player, or None.

    The cache file is keyed by player_name (matches placement names
    case-sensitively). Built daily by scripts/refresh_bbref_stats.
    """
    global _BBREF_STATS_LOADED
    if _BBREF_STATS_LOADED is None:
        if not _BBREF_STATS_PATH.exists():
            _BBREF_STATS_LOADED = {"players": {}}
        else:
            try:
                _BBREF_STATS_LOADED = json.loads(_BBREF_STATS_PATH.read_text())
            except Exception:
                _BBREF_STATS_LOADED = {"players": {}}
    record = _BBREF_STATS_LOADED.get("players", {}).get(p["player_name"])
    if not record:
        return None
    # Only return if there's an actual 2026 summer row (not "no row yet").
    if not record.get("summer_team"):
        return None
    return record


def _placement_card_from_bbref(p: dict, bbref: dict) -> dict:
    """Build a placement card seeded with BBRef season-to-date summary."""
    summary = bbref.get("summary") or ""
    bbref_id = bbref.get("bbref_id")
    profile_url = (
        f"https://www.baseball-reference.com/register/player.fcgi?id={bbref_id}"
        if bbref_id else ""
    )
    # Honor placement's league/team labels even if BBRef shows a slightly
    # different team name (Kent's spreadsheet is source of truth for who/where).
    league = p.get("league", "")
    summer_team = p.get("summer_team", "")
    return {
        "player_name": p["player_name"],
        "team": f"{summer_team} ({league})",
        "level": "Summer",
        "stats_summary": summary or "Season totals pending",
        "game_context": f"Summer ball — {p.get('school','')} · season totals",
        "game_status": "Season Stats",
        "game_time": None,
        "game_date": None,
        "is_yesterday": True,
        "next_game": None,
        "box_score_url": None,
        "player_profile_url": profile_url,
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "Baseball-Reference Register",
        "is_client": True,
        "tags": {
            "draft_class": p.get("draft_class",""),
            "position": "",
            "roster_priority": 99,
            "summer_college": p.get("school",""),
            "summer_league": league,
            "placement_status": p.get("status", ""),
        },
    }


def _try_baseballcube(p: dict) -> Optional[dict]:
    """Search The Baseball Cube for a player's most recent summer-ball game.

    Returns {summary, opponent, status, date_iso, profile_url, is_yesterday}
    or None when BBC has no record or the lookup fails. Uses residential
    proxy because BBC is Cloudflare-gated.
    """
    cache_key = f"{p['player_name'].lower()}|{p.get('school','').lower()}"
    if cache_key in _BBC_CACHE:
        return _BBC_CACHE[cache_key]
    try:
        from urllib.parse import quote_plus
        from src.summer_ball import fetch_via_residential_proxy
    except Exception:
        _BBC_CACHE[cache_key] = None
        return None

    name = p["player_name"]
    q = quote_plus(name)
    search_url = f"https://www.thebaseballcube.com/content/search/?search={q}"
    html, diag = fetch_via_residential_proxy(search_url, timeout=25)
    if not html:
        logger.info("bbc[%s]: search blocked (%s)", name, diag.get("error"))
        _BBC_CACHE[cache_key] = None
        return None

    from bs4 import BeautifulSoup
    import re
    soup = BeautifulSoup(html, "html.parser")
    candidate_urls: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/content/player/" in href:
            full = href if href.startswith("http") else f"https://www.thebaseballcube.com{href}"
            candidate_urls.append(full)
        if len(candidate_urls) >= 5:
            break

    # Cloudflare challenge pages on BBC are ~8KB. Real search results are
    # much larger AND have player links. If body is small AND no links =
    # challenge page, not legitimate "no matches" result.
    if not candidate_urls:
        is_challenge = len(html) < 15000
        logger.info(
            "bbc[%s]: %s (got %d bytes, 0 player links)",
            name,
            "Cloudflare-blocked" if is_challenge else "no matches in search",
            len(html),
        )
        _BBC_CACHE[cache_key] = None
        return None

    logger.info("bbc[%s]: %d candidate profiles", name, len(candidate_urls))
    target_college = (p.get("school") or "").lower()
    matched_profile = False
    for url in candidate_urls:
        phtml, _ = fetch_via_residential_proxy(url, timeout=25)
        if not phtml:
            continue
        body = BeautifulSoup(phtml, "html.parser").get_text(" ", strip=True)
        if target_college and target_college not in body.lower():
            continue
        matched_profile = True
        line = _parse_bbc_recent_line(body)
        if line:
            line["profile_url"] = url
            logger.info("bbc[%s]: matched line on %s", name, url)
            _BBC_CACHE[cache_key] = line
            return line
    if matched_profile:
        logger.info("bbc[%s]: matched profile but no parseable recent line", name)
    else:
        logger.info(
            "bbc[%s]: %d profiles checked, none matched college %r",
            name, len(candidate_urls), target_college,
        )
    _BBC_CACHE[cache_key] = None
    return None


def _parse_bbc_recent_line(body: str) -> Optional[dict]:
    """Look for a recent-game stat line in BBC's profile body text.

    BBC formats recent games variously (e.g. "Jun 1 vs OPP: 2-4, HR, 2 RBI").
    Heuristic match — keep light, fail closed.
    """
    import re
    # Date prefix + matchup keyword + numeric line.
    m = re.search(
        r"(\w{3}\s+\d{1,2})\s*(?:vs\.?|@|at)\s+([A-Z][\w .'\-]{2,40})[^\d]*(\d[\d\-, A-Z./]*HR|\d-\d|\d\s+IP)",
        body[:20000],
    )
    if not m:
        return None
    date_str, opp, _ = m.groups()
    # Pull a wider snippet around the match for the full stat line.
    start = m.start()
    snippet = body[start:start + 200]
    return {
        "summary": snippet.split(":")[-1].strip()[:80] if ":" in snippet[:120] else snippet[:80],
        "opponent": opp.strip(),
        "status": "Final",
        "date_iso": None,
        "is_yesterday": True,
    }


def _status_only_card(p: dict, *, reason: str) -> dict:
    """Card for Shut Down / Injured players — show them so they're visible
    but with grade '— No Data' and a clear status."""
    return {
        "player_name": p["player_name"],
        "team": (p.get("summer_team") or p.get("school","") or "—") + (
            f" ({p['league']})" if p.get("league") else ""
        ),
        "level": "Summer",
        "stats_summary": reason,
        "game_context": f"Summer ball — {p.get('school','')}",
        "game_status": p.get("status","") or "Status Update",
        "game_time": None,
        "game_date": None,
        "is_yesterday": False,
        "next_game": None,
        "box_score_url": None,
        "player_profile_url": "",
        "performance_grade": "— No Data",
        "grade_reason": "",
        "social_search_url": "",
        "data_source": "Kent placements",
        "is_client": True,
        "tags": {
            "draft_class": p.get("draft_class",""),
            "position": "",
            "roster_priority": 99,
            "summer_college": p.get("school",""),
            "summer_league": p.get("league",""),
            "placement_status": p.get("status",""),
        },
    }


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
