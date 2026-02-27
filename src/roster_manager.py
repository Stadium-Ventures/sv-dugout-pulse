"""
SV Dugout Pulse — Roster Manager

Fetches the player roster from a Google Sheet published as CSV,
filters to Pro/NCAA levels, and normalizes column names.
"""

import csv
import io
import logging
from typing import Optional

import requests

from .config import COLUMN_MAP, INCLUDED_LEVELS, RECRUITS_URL, ROSTER_URL

logger = logging.getLogger(__name__)


def fetch_roster(url: Optional[str] = None) -> list[dict]:
    """
    Download the Google Sheet CSV and return rows as a list of raw dicts
    (keyed by the original Sheet column headers).
    """
    url = url or ROSTER_URL
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


def filter_roster(rows: list[dict]) -> list[dict]:
    """
    Keep only players whose Level is in INCLUDED_LEVELS (Pro, NCAA).
    Returns normalized player dicts.
    """
    filtered = []
    for raw in rows:
        level = raw.get("Level", "").strip()
        if level not in INCLUDED_LEVELS:
            continue
        filtered.append(normalize_player(raw))

    logger.info(
        "Filtered roster: %d players (kept Pro/NCAA, dropped HS)", len(filtered)
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


def get_all_players() -> list[dict]:
    """
    Fetch both clients and recruits, combined.
    """
    clients = get_active_roster()
    recruits = get_recruits()
    return clients + recruits
