"""Baseball-Reference Register scraper for collegiate summer-ball stats.

Reads data/bbref_id_cache.json (built by scripts/refresh_player_id_cache.py
from the Chadwick Bureau register) and fetches each player's Register
page from baseball-reference.com. Parses the standard batting + pitching
tables, extracts 2026 summer-league rows, and writes a flat stats cache
that summer_pulse reads as a day-after fallback.

Sanctioned, free, no proxy required. Rate-limited politely (3s between
requests) per Sports Reference's data-use guidance. ~30 fetches max per
refresh; well within respectful-use thresholds.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ID_CACHE_PATH = _REPO_ROOT / "data" / "bbref_id_cache.json"
_STATS_CACHE_PATH = _REPO_ROOT / "data" / "bbref_stats.json"

_BBREF_BASE = "https://www.baseball-reference.com/register/player.fcgi"
_USER_AGENT = "Mozilla/5.0 (compatible; SV-DugoutPulse/1.0)"
_RATE_LIMIT_SEC = 3.0  # Sports Reference guidance: respect free server

# Map BBRef league codes (the "Lg" column on Register pages) -> our
# canonical league short names so summer_pulse can match against
# placement records. Codes verified empirically from sample profiles.
_BBREF_LEAGUE_CODES = {
    "CCBL": "Cape Cod",
    "NWL": "Northwoods",
    "NECL": "NECBL",
    "PGCL": "PGCBL",       # Perfect Game Collegiate
    "FCBL": "FCBL",
    "PROS": "Prospect",
    "CRCL": "Cal Ripken",
    "CPL": "Coastal Plain",
    "APPY": "Appalachian",
    "MDL": "MLB Draft",
    "MLBD": "MLB Draft",
    "GRCL": "Great Lakes",  # Bonus league, not in our placements yet
    "ALSK": "Alaska",
    "VBL": "Valley",
}


def _fetch_player_page(bbref_id: str) -> Optional[str]:
    """Fetch a Register player page. Returns HTML or None."""
    url = f"{_BBREF_BASE}?id={bbref_id}"
    try:
        resp = requests.get(
            url, timeout=20,
            headers={"User-Agent": _USER_AGENT, "Accept": "text/html"},
        )
        if resp.status_code != 200:
            logger.warning("bbref %s: HTTP %d", bbref_id, resp.status_code)
            return None
        return resp.text
    except Exception:
        logger.exception("bbref %s: fetch failed", bbref_id)
        return None


def _parse_summer_rows(html: str, year: int = 2026) -> dict:
    """Pull this player's 2026 summer-league row(s) from their Register page.

    Returns:
      {
        "summer_team": "...",          # Tm column
        "league_code": "CCBL"/"NWL"/...,
        "league": "Cape Cod" / mapped name,
        "games": int, "pa": int, "ab": int, "h": int,
        "doubles": int, "triples": int, "hr": int, "rbi": int,
        "sb": int, "bb": int, "so": int,
        "avg": "...", "obp": "...", "slg": "...",
        "kind": "hitter" | "pitcher",
        "ip": "..." (pitcher only),
        "era": "..." (pitcher only),
        "earned_runs": int (pitcher only),
      }
    Empty dict if no 2026 summer row found yet (season may not have
    started for that player).
    """
    soup = BeautifulSoup(html, "html.parser")
    out: dict = {}

    def _int(s: str) -> int:
        try: return int(s)
        except: return 0

    # Hitter table.
    bat = soup.find("table", {"id": "standard_batting"})
    if bat:
        headers = [c.get_text(strip=True) for c in bat.find_all("tr")[0].find_all(["th", "td"])]
        idx = {h: i for i, h in enumerate(headers)}
        for tr in bat.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if not cells or len(cells) < 5:
                continue
            yr = cells[idx.get("Year", 0)]
            if yr != str(year):
                continue
            lev = cells[idx.get("Lev", 5)] if idx.get("Lev") is not None else ""
            if lev != "Smr":
                continue
            out = {
                "kind": "hitter",
                "summer_team": cells[idx.get("Tm", 3)].replace("\xa0", " "),
                "league_code": cells[idx.get("Lg", 4)],
                "games": _int(cells[idx.get("G", 7)] if idx.get("G") is not None else "0"),
                "pa": _int(cells[idx.get("PA", 8)] if idx.get("PA") is not None else "0"),
                "ab": _int(cells[idx.get("AB", 9)] if idx.get("AB") is not None else "0"),
                "runs": _int(cells[idx.get("R", 10)] if idx.get("R") is not None else "0"),
                "h": _int(cells[idx.get("H", 11)] if idx.get("H") is not None else "0"),
                "doubles": _int(cells[idx.get("2B", 12)] if idx.get("2B") is not None else "0"),
                "triples": _int(cells[idx.get("3B", 13)] if idx.get("3B") is not None else "0"),
                "hr": _int(cells[idx.get("HR", 14)] if idx.get("HR") is not None else "0"),
                "rbi": _int(cells[idx.get("RBI", 15)] if idx.get("RBI") is not None else "0"),
                "sb": _int(cells[idx.get("SB", 16)] if idx.get("SB") is not None else "0"),
                "bb": _int(cells[idx.get("BB", 18)] if idx.get("BB") is not None else "0"),
                "so": _int(cells[idx.get("SO", 19)] if idx.get("SO") is not None else "0"),
                "avg": cells[idx.get("BA")] if idx.get("BA") is not None and idx.get("BA") < len(cells) else "",
                "obp": cells[idx.get("OBP")] if idx.get("OBP") is not None and idx.get("OBP") < len(cells) else "",
                "slg": cells[idx.get("SLG")] if idx.get("SLG") is not None and idx.get("SLG") < len(cells) else "",
            }
            break

    # Pitcher table.
    pit = soup.find("table", {"id": "standard_pitching"})
    if pit:
        headers = [c.get_text(strip=True) for c in pit.find_all("tr")[0].find_all(["th", "td"])]
        idx = {h: i for i, h in enumerate(headers)}
        for tr in pit.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if not cells:
                continue
            yr = cells[idx.get("Year", 0)]
            if yr != str(year):
                continue
            lev = cells[idx.get("Lev", 5)] if idx.get("Lev") is not None else ""
            if lev != "Smr":
                continue
            # Hitter line takes precedence (utility/two-way players).
            if out.get("kind") == "hitter":
                continue
            out = {
                "kind": "pitcher",
                "summer_team": cells[idx.get("Tm", 3)].replace("\xa0", " "),
                "league_code": cells[idx.get("Lg", 4)],
                "games": _int(cells[idx.get("G", 7)] if idx.get("G") is not None else "0"),
                "ip": cells[idx.get("IP")] if idx.get("IP") is not None and idx.get("IP") < len(cells) else "",
                "era": cells[idx.get("ERA")] if idx.get("ERA") is not None and idx.get("ERA") < len(cells) else "",
                "h_allowed": _int(cells[idx.get("H")] if idx.get("H") is not None and idx.get("H") < len(cells) else "0"),
                "earned_runs": _int(cells[idx.get("ER")] if idx.get("ER") is not None and idx.get("ER") < len(cells) else "0"),
                "bb": _int(cells[idx.get("BB")] if idx.get("BB") is not None and idx.get("BB") < len(cells) else "0"),
                "so": _int(cells[idx.get("SO")] if idx.get("SO") is not None and idx.get("SO") < len(cells) else "0"),
                "whip": cells[idx.get("WHIP")] if idx.get("WHIP") is not None and idx.get("WHIP") < len(cells) else "",
            }
            break

    if out:
        out["league"] = _BBREF_LEAGUE_CODES.get(out.get("league_code", ""), out.get("league_code", ""))
    return out


def format_summary(stats: dict) -> str:
    """Turn a parsed stats dict into a one-line summary for cards/email."""
    if not stats:
        return ""
    if stats.get("kind") == "pitcher":
        return (
            f"{stats.get('ip','-')} IP, "
            f"{stats.get('earned_runs',0)} ER, "
            f"{stats.get('so',0)} K, "
            f"{stats.get('bb',0)} BB · "
            f"ERA {stats.get('era','-')}, WHIP {stats.get('whip','-')} "
            f"({stats.get('games',0)} G)"
        )
    return (
        f"{stats.get('h',0)}-for-{stats.get('ab',0)}, "
        f"{stats.get('hr',0)} HR, {stats.get('rbi',0)} RBI · "
        f"{stats.get('avg','-')}/{stats.get('obp','-')}/{stats.get('slg','-')} "
        f"({stats.get('games',0)} G, {stats.get('pa',0)} PA)"
    )


def refresh_all_stats(year: int = 2026) -> dict:
    """For each cached BBRef ID, fetch their Register page and parse the
    {year} summer row. Writes data/bbref_stats.json.

    Last-known-good behavior: if a fetch fails for a player, the previous
    run's stats for that player are preserved (so a transient BBRef
    blip doesn't blank out yesterday's data). Only successful fetches
    overwrite the cache entry.
    """
    if not _ID_CACHE_PATH.exists():
        logger.error("Missing %s — run scripts/refresh_player_id_cache.py first", _ID_CACHE_PATH)
        return {}
    cache = json.loads(_ID_CACHE_PATH.read_text())
    ids = cache.get("ids") or {}
    logger.info("BBRef refresh: %d players queued", len(ids))

    # Load prior cache so we can preserve last-known-good entries.
    prior: dict[str, dict] = {}
    if _STATS_CACHE_PATH.exists():
        try:
            prior = (json.loads(_STATS_CACHE_PATH.read_text()) or {}).get("players", {})
        except Exception:
            prior = {}

    results: dict[str, dict] = {}
    fetch_failures = 0
    for i, (player_name, info) in enumerate(sorted(ids.items())):
        bbref_id = info.get("bbref_minors_id")
        if not bbref_id:
            continue
        if i > 0:
            time.sleep(_RATE_LIMIT_SEC)
        html = _fetch_player_page(bbref_id)
        if not html:
            fetch_failures += 1
            # Preserve prior data if we had it; otherwise mark as failed.
            prior_record = prior.get(player_name)
            if prior_record and prior_record.get("summer_team"):
                results[player_name] = {
                    **prior_record,
                    "is_stale": True,
                    "stale_reason": "BBRef fetch failed; showing last-known-good",
                }
                logger.info("bbref %s: fetch failed, preserved last-known-good", player_name)
            else:
                results[player_name] = {"bbref_id": bbref_id, "error": "fetch failed"}
            continue
        stats = _parse_summer_rows(html, year=year)
        results[player_name] = {
            "bbref_id": bbref_id,
            "year": year,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            **stats,
            "summary": format_summary(stats),
        }
        logger.info("bbref %s: %s", player_name, results[player_name].get("summary") or "no 2026 summer row yet")

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "year": year,
        "fetch_failures": fetch_failures,
        "players": results,
    }
    _STATS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATS_CACHE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))
    logger.info(
        "Wrote %d player stats to %s (%d fetches failed, preserved from prior)",
        len(results), _STATS_CACHE_PATH, fetch_failures,
    )
    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
    refresh_all_stats()
