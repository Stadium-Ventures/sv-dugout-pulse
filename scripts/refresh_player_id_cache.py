"""Resolve placement players to Chadwick Bureau register IDs.

Downloads the Chadwick Bureau register people-*.csv files, looks up each
placement player by (name_last, name_first), and writes a cache of their
`key_bbref_minors` + `key_mlbam` IDs to data/bbref_id_cache.json.

Chadwick repo: https://github.com/chadwickbureau/register
The `key_bbref_minors` ID is what powers Baseball-Reference Register page
URLs (https://www.baseball-reference.com/register/player.fcgi?id={ID}).

Run weekly (or whenever Kent updates the placement spreadsheet). Output
file is small (~30 players × ~200 bytes); fine to commit.
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PLACEMENTS_PATH = _REPO_ROOT / "data" / "summer_ball_placements.json"
_CACHE_PATH = _REPO_ROOT / "data" / "bbref_id_cache.json"
# Manual overrides for players Chadwick hasn't indexed yet. Checked BEFORE
# the Chadwick lookup so manual entries win over auto-resolution.
_OVERRIDES_PATH = _REPO_ROOT / "data" / "bbref_id_overrides.json"

_CHADWICK_BASE = (
    "https://raw.githubusercontent.com/chadwickbureau/register/master/data/"
)
# Chadwick splits the register into people-0.csv .. people-N.csv. The count
# has varied over time (16 mentioned in older docs; 10 as of 2026-06-03).
# Probe lazily and stop when we hit the first 404.
_PEOPLE_MAX_PROBE = 30


def _fetch_people_file(idx: int) -> list[dict]:
    """Download one people-N.csv. Returns [] on 404 (file doesn't exist)."""
    url = f"{_CHADWICK_BASE}people-{idx}.csv"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return list(csv.DictReader(resp.text.splitlines()))


def _build_index() -> dict[tuple[str, str], list[dict]]:
    """Pull all 16 people CSVs in parallel and index by (last, first)
    lowercased. Returns dict keyed by lower(last)|lower(first) -> list of
    matching records (multiple when multiple players share the name).
    """
    index: dict[tuple[str, str], list[dict]] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_people_file, i): i for i in range(_PEOPLE_MAX_PROBE)}
        total = 0
        for fut in as_completed(futures):
            try:
                rows = fut.result()
            except Exception as e:
                logger.warning("people file fetch failed: %s", e)
                continue
            total += len(rows)
            for r in rows:
                # Only keep records that have a key_bbref_minors (the field
                # we actually need). Cuts the in-memory index roughly in half.
                if not r.get("key_bbref_minors"):
                    continue
                key = (
                    (r.get("name_last") or "").strip().lower(),
                    (r.get("name_first") or "").strip().lower(),
                )
                if not key[0]:
                    continue
                index.setdefault(key, []).append(r)
    logger.info("Indexed %d rows with key_bbref_minors (of ~%d total)", sum(len(v) for v in index.values()), total)
    return index


def _normalize_name_for_lookup(name: str) -> tuple[str, str]:
    """Split a player name into (last, first) for index lookup.

    Handles "Cam Flukey" -> ("flukey", "cam"), "Brady St. Pierre" ->
    ("st. pierre", "brady") — we just use the first whitespace token as
    first name and the rest as last name. Chadwick stores names this way.
    """
    name = (name or "").strip()
    if not name:
        return ("", "")
    parts = name.split(None, 1)
    if len(parts) == 1:
        return (parts[0].lower(), "")
    first, last = parts[0], parts[1]
    return (last.lower(), first.lower())


def _pick_best_match(
    candidates: list[dict],
    *,
    placement_school: str = "",
) -> list[dict]:
    """When Chadwick has multiple records for the same name, narrow to the
    most likely 2026-active player. Returns a list (possibly >1 if we still
    can't disambiguate; the consumer can decide).
    """
    if not candidates:
        return []
    if len(candidates) == 1:
        return candidates

    def _played_2026(r: dict) -> bool:
        for k in ("col_played_last", "pro_played_last", "mlb_played_last"):
            v = (r.get(k) or "").strip()
            if v and v >= "2026":
                return True
        return False

    active = [c for c in candidates if _played_2026(c)]
    if not active:
        active = candidates
    # If multiple still, prefer those with a recent college (2024+) since
    # we're tracking NCAA prospects.
    recent_college = [
        c for c in active
        if (c.get("col_played_last") or "") >= "2024"
    ]
    if recent_college:
        return recent_college
    return active


def _load_overrides() -> dict:
    """Manual BBRef ID overrides — for players Chadwick hasn't indexed yet.

    Returns {player_name: {bbref_minors_id, mlbam_id, note}}.
    """
    if not _OVERRIDES_PATH.exists():
        return {}
    try:
        data = json.loads(_OVERRIDES_PATH.read_text())
        return data.get("overrides") or {}
    except Exception:
        logger.warning("Failed to read %s", _OVERRIDES_PATH)
        return {}


def main():
    if not _PLACEMENTS_PATH.exists():
        logger.error("Missing %s", _PLACEMENTS_PATH)
        sys.exit(1)
    placements = json.loads(_PLACEMENTS_PATH.read_text()).get("placements", [])
    placements = [
        p for p in placements
        if p.get("player_name")
        and not (str(p["player_name"]).isupper() and len(p["player_name"]) > 5)
    ]
    overrides = _load_overrides()
    logger.info(
        "Resolving %d placement players against Chadwick register "
        "(%d manual overrides loaded)",
        len(placements), len(overrides),
    )

    index = _build_index()
    cache: dict[str, dict] = {}
    misses: list[str] = []
    multi: list[str] = []
    overridden: list[str] = []

    for p in placements:
        name = p["player_name"]
        # 1) Manual override wins if present.
        if name in overrides:
            ov = overrides[name]
            cache[name] = {
                "bbref_minors_id": ov.get("bbref_minors_id"),
                "mlbam_id": ov.get("mlbam_id"),
                "source": "manual_override",
                "note": ov.get("note", ""),
            }
            overridden.append(name)
            continue
        # 2) Chadwick lookup.
        last, first = _normalize_name_for_lookup(name)
        candidates = index.get((last, first), [])
        narrowed = _pick_best_match(
            candidates, placement_school=p.get("school", ""),
        )
        if not narrowed:
            misses.append(name)
            continue
        chosen = narrowed[0]
        if len(narrowed) > 1:
            multi.append(f"{name} ({len(narrowed)} candidates)")
        cache[name] = {
            "bbref_minors_id": chosen.get("key_bbref_minors"),
            "mlbam_id": chosen.get("key_mlbam") or None,
            "name_given": chosen.get("name_given"),
            "birth_year": chosen.get("birth_year"),
            "col_played_first": chosen.get("col_played_first"),
            "col_played_last": chosen.get("col_played_last"),
            "candidate_count": len(narrowed),
            "source": "chadwick",
        }

    payload = {
        "generated_at_utc": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "placement_count": len(placements),
        "resolved": len(cache),
        "missing": misses,
        "ambiguous": multi,
        "manual_overrides": overridden,
        "ids": cache,
    }
    _CACHE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))
    logger.info(
        "Wrote %d resolved IDs to %s (missing: %d, ambiguous: %d, manual: %d)",
        len(cache), _CACHE_PATH, len(misses), len(multi), len(overridden),
    )
    if misses:
        logger.info("Missing (eligible for manual override): %s", ", ".join(misses[:15]))


if __name__ == "__main__":
    main()
