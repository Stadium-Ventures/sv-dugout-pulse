"""
SV Dugout Pulse — Roster Manager

Fetches the player roster from a Google Sheet published as CSV,
filters to Pro/NCAA levels, and normalizes column names.
"""

import csv
import io
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional

import requests

from .config import (
    COLUMN_MAP,
    EXCLUDED_MLB_IDS,
    INCLUDED_LEVELS,
    RECRUITS_URL,
    ROSTER_CACHE_PATH,
    ROSTER_URL,
)

logger = logging.getLogger(__name__)


def fetch_roster(url: Optional[str] = None) -> list[dict]:
    """
    Download the Google Sheet CSV and return rows as a list of raw dicts
    (keyed by the original Sheet column headers).
    """
    url = (url or ROSTER_URL).strip()
    logger.info("Fetching roster from %s", url)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch roster: %s", exc)
        raise

    reader = csv.DictReader(io.StringIO(resp.text))

    # Validate that expected columns exist
    if reader.fieldnames is None:
        raise ValueError("CSV has no headers")

    missing = [col for col in COLUMN_MAP if col not in reader.fieldnames]
    if missing:
        logger.warning("Missing expected columns in sheet: %s", missing)

    rows = list(reader)
    logger.info("Fetched %d rows from roster", len(rows))
    return rows


def normalize_player(raw: dict) -> dict:
    """
    Map sheet column names to internal keys using COLUMN_MAP.
    Coerce roster_priority to int.
    """
    player = {}
    for sheet_col, internal_key in COLUMN_MAP.items():
        player[internal_key] = raw.get(sheet_col, "").strip()

    # Coerce roster_priority to int (default 99 if missing/invalid)
    try:
        player["roster_priority"] = int(player["roster_priority"])
    except (ValueError, TypeError):
        player["roster_priority"] = 99

    # Coerce mlb_id to int or None (blank for NCAA/HS players)
    raw_id = player.get("mlb_id", "")
    try:
        player["mlb_id"] = int(raw_id) if raw_id else None
    except (ValueError, TypeError):
        player["mlb_id"] = None

    return player


_COACH_TRUTHY = {"yes", "y", "true", "1", "x"}


def _is_coach(raw: dict) -> bool:
    """Coaches share the roster sheet but should never appear in Pulse.

    Two signals: the dedicated "Is Coach" column (Yes/No) or a "Primary Position"
    that contains "coach" (e.g. "Head Coach", "Assistant Coach"). Either is
    sufficient — we don't grade coaches.
    """
    if (raw.get("Is Coach") or "").strip().lower() in _COACH_TRUTHY:
        return True
    if "coach" in (raw.get("Primary Position") or "").strip().lower():
        return True
    return False


def filter_roster(rows: list[dict]) -> list[dict]:
    """
    Keep only players whose Level is in INCLUDED_LEVELS (Pro, NCAA).
    Returns normalized player dicts.
    """
    filtered = []
    excluded_id = 0
    excluded_coach = 0
    for raw in rows:
        level = raw.get("Level", "").strip()
        if level not in INCLUDED_LEVELS:
            continue
        if _is_coach(raw):
            excluded_coach += 1
            continue
        player = normalize_player(raw)
        if player.get("mlb_id") in EXCLUDED_MLB_IDS:
            excluded_id += 1
            continue
        filtered.append(player)

    logger.info(
        "Filtered roster: %d players (kept Pro/NCAA/HS, excluded %d by MLB ID, %d coaches)",
        len(filtered),
        excluded_id,
        excluded_coach,
    )
    return filtered


def get_active_roster(url: Optional[str] = None) -> list[dict]:
    """
    Convenience wrapper: fetch + filter in one call.
    Returns clients (is_client=True).
    """
    raw_rows = fetch_roster(url)
    players = filter_roster(raw_rows)
    for p in players:
        p["is_client"] = True
    return players


def get_recruits(url: Optional[str] = None) -> list[dict]:
    """
    Fetch recruits/following list. Same structure as roster.
    Returns recruits (is_client=False).
    """
    url = url or RECRUITS_URL
    try:
        raw_rows = fetch_roster(url)
        players = filter_roster(raw_rows)
        for p in players:
            p["is_client"] = False
        logger.info("Fetched %d recruits", len(players))
        return players
    except Exception:
        logger.exception("Failed to fetch recruits — continuing without them")
        return []


_ROSTER_CACHE_MAX_AGE_H = 24


def _save_roster_cache(players: list[dict]):
    """Persist roster to disk so we can fall back if Sheets is unreachable."""
    try:
        dir_path = os.path.dirname(ROSTER_CACHE_PATH)
        os.makedirs(dir_path, exist_ok=True)
        payload = {
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "players": players,
        }
        fd, tmp = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            os.replace(tmp, ROSTER_CACHE_PATH)
        except BaseException:
            os.unlink(tmp)
            raise
        logger.debug("Saved roster cache (%d players)", len(players))
    except Exception:
        logger.debug("Failed to save roster cache — non-fatal")


def _load_roster_cache() -> list[dict] | None:
    """Load cached roster if it exists and is < 24 h old."""
    if not os.path.exists(ROSTER_CACHE_PATH):
        return None
    try:
        with open(ROSTER_CACHE_PATH) as f:
            data = json.load(f)
        cached_at = datetime.fromisoformat(data["cached_at"])
        age_h = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
        if age_h > _ROSTER_CACHE_MAX_AGE_H:
            logger.warning("Roster cache is %.1f h old — too stale to use", age_h)
            return None
        players = data.get("players", [])
        players = [p for p in players if p.get("mlb_id") not in EXCLUDED_MLB_IDS]
        logger.info("Loaded roster cache (%d players, %.1f h old)", len(players), age_h)
        return players
    except Exception:
        logger.debug("Failed to load roster cache")
        return None


_MLB_API_BASE = "https://statsapi.mlb.com/api/v1"


def _enrich_pro_team_from_api(players: list[dict]) -> None:
    """Replace sheet team/affiliate with MLB API truth for Pro players.

    Mutates each Pro player dict in place. The Google Sheet's Org/Affiliate
    columns are unreliable (manual data entry, lag on promotions/trades);
    the MLB Stats API's ``currentTeam`` is canonical. Players without an
    ``mlb_id`` keep sheet values — they can't be resolved via API.

    After this runs, every downstream consumer (live fetcher, historical
    aggregator, alerts, dashboard) reads API-correct team data without
    needing its own API lookup.

    Failures (timeout, network blip, MLB API outage) leave the sheet values
    intact rather than blanking them — degraded mode is "show the sheet,"
    not "show nothing." A single shared Session reuses TCP connections so
    the per-player cost is small.
    """
    pro_players = [p for p in players if p.get("level") == "Pro" and p.get("mlb_id")]
    if not pro_players:
        return
    team_cache: dict[int, dict] = {}
    session = requests.Session()
    enriched = 0
    drifted = 0
    for player in pro_players:
        mlb_id = player["mlb_id"]
        try:
            resp = session.get(
                f"{_MLB_API_BASE}/people/{mlb_id}?hydrate=currentTeam",
                timeout=8,
            )
            resp.raise_for_status()
            people = resp.json().get("people", [])
            ct = people[0].get("currentTeam", {}) if people else {}
            team_id = ct.get("id") if isinstance(ct, dict) else None
            if not team_id:
                continue
            if team_id not in team_cache:
                t_resp = session.get(
                    f"{_MLB_API_BASE}/teams/{team_id}", timeout=8,
                )
                t_resp.raise_for_status()
                t_list = t_resp.json().get("teams", [])
                if not t_list:
                    continue
                t = t_list[0]
                team_cache[team_id] = {
                    "name": t.get("name", ""),
                    "sport_id": t.get("sport", {}).get("id", 1),
                    "parent_name": t.get("parentOrgName", ""),
                }
            info = team_cache[team_id]
            api_name = info["name"]
            if not api_name:
                continue
            is_mlb = info["sport_id"] == 1
            api_org = info["parent_name"] if not is_mlb and info["parent_name"] else api_name
            sheet_team = (player.get("team") or "").strip()
            sheet_affiliate = (player.get("affiliate") or "").strip()
            if sheet_team and sheet_team.lower() != api_org.lower():
                logger.warning(
                    "Sheet roster drift for %s (id=%d): sheet org=%r API org=%r — using API",
                    player.get("player_name"), mlb_id, sheet_team, api_org,
                )
                drifted += 1
            elif sheet_affiliate and sheet_affiliate.lower() != api_name.lower():
                logger.info(
                    "Sheet affiliate drift for %s (id=%d): sheet=%r API=%r — using API",
                    player.get("player_name"), mlb_id, sheet_affiliate, api_name,
                )
                drifted += 1
            player["team"] = api_org
            player["affiliate"] = api_name
            player["_api_team_applied"] = True
            enriched += 1
        except Exception as exc:
            logger.debug(
                "MLB API enrichment failed for %s (id=%s): %s — keeping sheet values",
                player.get("player_name"), mlb_id, exc,
            )
    logger.info(
        "MLB API enrichment: %d/%d Pro players resolved, %d sheet drifts logged",
        enriched, len(pro_players), drifted,
    )


def get_all_players() -> list[dict]:
    """
    Fetch both clients and recruits, combined.
    Falls back to cached roster if Google Sheets is unreachable.

    Pro players have their team/affiliate overwritten from the MLB Stats API
    after sheet load — the sheet is the source of identity (name + mlb_id),
    but the API is the source of truth for current org and current affiliate.
    """
    try:
        clients = get_active_roster()
        recruits = get_recruits()
        players = clients + recruits
        _enrich_pro_team_from_api(players)
        _save_roster_cache(players)
        return players
    except Exception:
        logger.warning("Roster fetch failed — trying cached roster")
        cached = _load_roster_cache()
        if cached:
            return cached
        logger.error("No roster available (fetch failed, no usable cache)")
        return []
