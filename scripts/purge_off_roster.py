"""One-time (re-runnable) purge of off-roster names from committed data files.

Roster hygiene, 2026-08-19: this repo publishes every data/*.json as a public
GitHub Pages URL, so only names currently on the master roster + recruits
sheets may be stored or surfaced. The pipeline now prunes on write, but the
committed files accumulated names from before those gates existed — this
script cleans them all in one idempotent pass.

Safety: it fetches the LIVE roster sheets (never the stale cache) and aborts
on any fetch failure or an implausibly small roster, so it can never prune
against bad data. Safe to rerun any time:

    python -m scripts.purge_off_roster
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from src.config import HS_NAME_ALIASES  # noqa: E402
from src.roster_manager import fetch_roster, filter_roster  # noqa: E402
from src.config import RECRUITS_URL, ROSTER_URL  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("purge_off_roster")

_DATA = _REPO_ROOT / "data"

# Historical hand-transcription drift in the summer stores — migrate these
# real clients' entries to their roster spellings instead of deleting them.
# (data/summer_ball_placements.json itself was corrected 2026-08-19.)
SUMMER_NAME_FIXES = {
    "Bryson Tweedy": "Brisen Tweedy",
    "Dom Woodward": "Dominic Woodward",
    "Sammy Mitchell": "Sam Mitchell",
}

_MIN_PLAUSIBLE_ROSTER = 20


def _atomic_write(path: Path, data, **kwargs) -> None:
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, **kwargs)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def _load(path: Path):
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def _fetch_live_roster() -> tuple[set[str], set[int]]:
    """Fresh names (lowercased) + MLB IDs from the live roster + recruits sheets.

    Raises on any failure — pruning must never run against a partial roster.
    """
    players = filter_roster(fetch_roster(ROSTER_URL))
    try:
        players += filter_roster(fetch_roster(RECRUITS_URL))
    except Exception:
        # The recruits sheet is small but its names ARE legitimate — a failed
        # fetch would misclassify recruits as off-roster. Hard abort.
        raise SystemExit("Recruits sheet fetch failed — aborting (no pruning done)")
    names = {
        name
        for p in players
        if (name := (p.get("player_name") or "").strip().lower())
    }
    ids = {p["mlb_id"] for p in players if p.get("mlb_id")}
    if len(names) < _MIN_PLAUSIBLE_ROSTER:
        raise SystemExit(
            f"Only {len(names)} roster names fetched — implausible, aborting"
        )
    logger.info("Live roster: %d names, %d MLB IDs", len(names), len(ids))
    return names, ids


def purge_ncaa_game_log(names: set[str]) -> int:
    path = _DATA / "ncaa_game_log.json"
    log = _load(path)
    if log is None:
        return 0
    stale = [k for k in log if k.split("|", 1)[0].strip().lower() not in names]
    for k in stale:
        del log[k]
    if stale:
        _atomic_write(path, log, indent=2, ensure_ascii=False)
        logger.info("ncaa_game_log.json: purged %d key(s): %s", len(stale), sorted(stale))
    return len(stale)


def purge_hs_game_log(names: set[str]) -> tuple[int, int]:
    """Returns (migrated_keys, purged_keys). Aliased keys (old misspellings of
    real clients) are MERGED into the roster-spelling key, not deleted."""
    path = _DATA / "hs_game_log.json"
    log = _load(path)
    if log is None:
        return 0, 0

    migrated = 0
    for old_key, new_key in HS_NAME_ALIASES.items():
        if old_key not in log or old_key == new_key:
            continue
        target = log.setdefault(new_key, [])
        seen = {(e.get("date"), e.get("type")) for e in target}
        for e in log[old_key]:
            if (e.get("date"), e.get("type")) not in seen:
                target.append(e)
                seen.add((e.get("date"), e.get("type")))
        del log[old_key]
        migrated += 1
        logger.info("hs_game_log.json: migrated %r -> %r", old_key, new_key)

    stale = [k for k in log if k.strip().lower() not in names]
    for k in stale:
        del log[k]
    if stale:
        logger.info("hs_game_log.json: purged %d key(s): %s", len(stale), sorted(stale))
    if migrated or stale:
        _atomic_write(path, log, indent=2, ensure_ascii=False)
    return migrated, len(stale)


def purge_team_levels(ids: set[int]) -> int:
    path = _DATA / "_last_team_levels.json"
    state = _load(path)
    if state is None:
        return 0
    stale = [k for k in state if not (k.isdigit() and int(k) in ids)]
    for k in stale:
        logger.info(
            "_last_team_levels.json: purging %s (%s)", k, (state[k] or {}).get("name", "?")
        )
        del state[k]
    if stale:
        _atomic_write(path, state, indent=2, sort_keys=True)
    return len(stale)


def purge_summer_game_log(names: set[str]) -> tuple[int, int]:
    """Returns (renamed_entries, purged_entries)."""
    path = _DATA / "summer_game_log.json"
    log = _load(path)
    if log is None:
        return 0, 0
    renamed = 0
    purged = 0
    for day in list(log):
        kept = []
        for rec in log[day]:
            name = (rec.get("player_name") or "").strip()
            if name in SUMMER_NAME_FIXES:
                rec["player_name"] = SUMMER_NAME_FIXES[name]
                renamed += 1
                name = rec["player_name"]
            if name.lower() in names:
                kept.append(rec)
            else:
                purged += 1
        if kept:
            log[day] = kept
        else:
            del log[day]
    if renamed or purged:
        _atomic_write(path, log, indent=2, sort_keys=True)
        logger.info(
            "summer_game_log.json: renamed %d entries to roster spellings, purged %d",
            renamed, purged,
        )
    return renamed, purged


def purge_health_history(names: set[str]) -> int:
    path = _DATA / "player_health_history.json"
    history = _load(path)
    if history is None:
        return 0
    purged = 0
    for snapshot in history:
        players = snapshot.get("players", [])
        kept = [
            p for p in players
            if (p.get("name") or "").strip().lower() in names
        ]
        purged += len(players) - len(kept)
        snapshot["players"] = kept
    if purged:
        _atomic_write(path, history, indent=2, ensure_ascii=False)
        logger.info("player_health_history.json: purged %d row(s)", purged)
    return purged


def fix_drift_spellings(names: set[str]) -> int:
    """Clean placement-derived and diagnostic files: rename SUMMER_NAME_FIXES
    drift spellings to roster spellings, and drop off-roster names from
    diagnostic name lists. These regenerate on their next scheduled run
    anyway — this just gets the committed (public) copies clean now,
    preserving state (streak counters, IDs) across the rename.
    Returns the number of renames + drops applied."""
    fixed = 0

    def _fix_name(n):
        nonlocal fixed
        if n in SUMMER_NAME_FIXES:
            fixed += 1
            return SUMMER_NAME_FIXES[n]
        return n

    def _clean_name_list(lst):
        """Rename drift spellings, then drop names not on the roster."""
        nonlocal fixed
        out = []
        for n in lst:
            n = _fix_name(n)
            # Diagnostic lists occasionally suffix names ("Foo (2 candidates)").
            base = n.split(" (", 1)[0].strip().lower()
            if base in names:
                out.append(n)
            else:
                fixed += 1
        return out

    # Dict keyed by player name (quiet-streak state).
    path = _DATA / "_last_summer_games.json"
    data = _load(path)
    if data is not None:
        before = fixed
        out = {}
        for k, v in data.items():
            nk = _fix_name(k)
            out.setdefault(nk, v)  # roster-spelling key wins if both exist
        if fixed > before:
            _atomic_write(path, out, indent=2, sort_keys=True)
            logger.info(
                "_last_summer_games.json: renamed %d key(s) to roster spellings",
                fixed - before,
            )

    # BBRef ID cache envelope: names live under ids{} keys and in the
    # missing/ambiguous lists.
    path = _DATA / "bbref_id_cache.json"
    data = _load(path)
    if data is not None:
        before = fixed
        if isinstance(data.get("ids"), dict):
            ids_out = {}
            for k, v in data["ids"].items():
                ids_out.setdefault(_fix_name(k), v)
            data["ids"] = ids_out
        for key in ("missing", "ambiguous"):
            if isinstance(data.get(key), list):
                data[key] = _clean_name_list(data[key])
        if fixed > before:
            _atomic_write(path, data, indent=2, sort_keys=True)
            logger.info("bbref_id_cache.json: cleaned %d name(s)", fixed - before)

    # Lists of entries with a player_name field.
    path = _DATA / "summer_ball_rosters.json"
    data = _load(path)
    if data is not None:
        before = fixed

        def _walk(obj):
            if isinstance(obj, dict):
                if "player_name" in obj:
                    obj["player_name"] = _fix_name(obj["player_name"])
                for v in obj.values():
                    _walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    _walk(v)

        _walk(data)
        if fixed > before:
            _atomic_write(path, data, indent=2, ensure_ascii=False)
            logger.info(
                "summer_ball_rosters.json: renamed %d entrie(s) to roster spellings",
                fixed - before,
            )

    # Run-health diagnostics carry client-name lists — rename drift
    # spellings AND drop removed clients (e.g. a Retired Pro lingering in
    # fallback_clients history).
    def _fix_health_block(h) -> None:
        for key in ("blocked_clients", "carry_forward_clients", "fallback_clients"):
            if isinstance(h.get(key), list):
                h[key] = _clean_name_list(h[key])

    path = _DATA / "fetch_health_history.json"
    history = _load(path)
    if history is not None:
        before = fixed
        for h in history:
            _fix_health_block(h)
        if fixed > before:
            _atomic_write(path, history, indent=2, ensure_ascii=False)
            logger.info("fetch_health_history.json: renamed %d name(s)", fixed - before)

    path = _DATA / "current_pulse.json"
    data = _load(path)
    if isinstance(data, dict) and isinstance(data.get("health"), dict):
        before = fixed
        _fix_health_block(data["health"])
        if fixed > before:
            _atomic_write(path, data, indent=2, ensure_ascii=False)
            logger.info("current_pulse.json health: renamed %d name(s)", fixed - before)

    return fixed


def purge_pulse_file(filename: str, names: set[str]) -> int:
    """Remove off-roster entries from current_pulse / window_* / yesterday_pulse.

    Handles both bare lists and {..., "players": [...]} envelopes, preserving
    the envelope untouched otherwise.
    """
    path = _DATA / filename
    data = _load(path)
    if data is None:
        return 0
    is_envelope = isinstance(data, dict)
    players = data.get("players", []) if is_envelope else data
    kept = [
        p for p in players
        if (p.get("player_name") or "").strip().lower() in names
    ]
    purged = len(players) - len(kept)
    if purged:
        removed = sorted({
            p["player_name"] for p in players
            if (p.get("player_name") or "").strip().lower() not in names
        })
        if is_envelope:
            data["players"] = kept
        else:
            data = kept
        _atomic_write(path, data, indent=2, ensure_ascii=False)
        logger.info("%s: purged %d entrie(s): %s", filename, purged, removed)
    return purged


def main() -> None:
    names, ids = _fetch_live_roster()

    total = 0
    total += purge_ncaa_game_log(names)
    migrated, hs_purged = purge_hs_game_log(names)
    total += hs_purged
    total += purge_team_levels(ids)
    renamed, sg_purged = purge_summer_game_log(names)
    total += sg_purged
    total += purge_health_history(names)
    renamed += fix_drift_spellings(names)
    for fn in (
        "current_pulse.json",
        "yesterday_pulse.json",
        "window_7d.json",
        "window_14d.json",
        "window_30d.json",
        "window_season.json",
    ):
        total += purge_pulse_file(fn, names)

    # Orphaned artifact: zero readers, zero writers, contains a removed client.
    baselines = _DATA / "ncaa_baselines.json"
    if baselines.exists():
        baselines.unlink()
        logger.info("Deleted orphaned ncaa_baselines.json")

    logger.info(
        "Done: %d entries purged, %d HS keys migrated, %d summer entries renamed",
        total, migrated, renamed,
    )


if __name__ == "__main__":
    main()
