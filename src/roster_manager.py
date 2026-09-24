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
    DEFAULT_REGISTRY_URL,
    EXCLUDED_MLB_IDS,
    INCLUDED_LEVELS,
    MLB_CLUB_NAMES,
    PITCHER_POSITION_LABELS,
    RECRUITS_URL,
    REGISTRY_MIN_ROWS,
    REGISTRY_PROJECTION_PATH,
    REGISTRY_TOKEN_ENV,
    ROSTER_CACHE_FIELDS,
    ROSTER_CACHE_PATH,
    ROSTER_URL,
    TWO_WAY_POSITION_LABELS,
)

logger = logging.getLogger(__name__)


def fetch_roster(url: Optional[str] = None) -> list[dict]:
    """
    Download the Google Sheet CSV and return rows as a list of raw dicts
    (keyed by the original Sheet column headers).
    """
    url = (url or ROSTER_URL).strip()
    if not url:
        raise ValueError("Roster CSV URL not configured (ROSTER_URL / RECRUITS_URL secret)")
    # Never log the URL itself — a published-CSV link is a credential-equivalent.
    logger.info("Fetching roster CSV")

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

    # Identity-critical columns missing means this isn't the roster sheet at
    # all (Sheets perm change / error page served with HTTP 200) — hard-fail
    # so the caller falls back to the cache instead of parsing garbage.
    critical = [c for c in ("Player Name", "Level") if c not in reader.fieldnames]
    if critical:
        raise ValueError(f"Roster CSV missing critical columns {critical} — not a roster sheet")

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
    # Registry slug (stable join key). Present on registry-sourced rows and
    # the registry's sheet-shaped CSV; blank on the legacy sheet.
    player["slug"] = str(raw.get("slug") or "").strip()

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

    # Drafted-but-unsigned: the sheet keeps Level=NCAA/HS until the player
    # signs, but Org already names the drafting MLB club. Treat as Pro —
    # the pro fetcher name-searches the MLB API (stats start at debut, empty
    # card until then) and summer-ball tracking drops them automatically.
    if player["level"] in ("NCAA", "HS") and player["team"].lower() in MLB_CLUB_NAMES:
        logger.info(
            "%s: Org is an MLB club (%s) with Level=%s — treating as Pro (drafted)",
            player["player_name"], player["team"], player["level"],
        )
        player["level"] = "Pro"

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
    excluded_retired = 0
    for raw in rows:
        level = raw.get("Level", "").strip()
        if level not in INCLUDED_LEVELS:
            continue
        if _is_coach(raw):
            excluded_coach += 1
            continue
        player = normalize_player(raw)
        # Sheet-side retirement switch: Status=Retired (any level) or
        # Org=Retired suppresses the player without a code change.
        # EXCLUDED_MLB_IDS stays as belt-and-braces for Pro players.
        status = (player.get("status") or "").strip().lower()
        org = (raw.get("Org") or "").strip().lower()
        if status == "retired" or org == "retired":
            excluded_retired += 1
            # A registry-sourced row (it has a slug) may be someone who is no
            # longer a client, and this log is public: count it, never name it.
            if not player.get("slug"):
                logger.info("Excluding %s — marked Retired on the sheet", player.get("player_name"))
            continue
        if player.get("mlb_id") in EXCLUDED_MLB_IDS:
            excluded_id += 1
            continue
        filtered.append(player)

    logger.info(
        "Filtered roster: %d players (kept Pro/NCAA/HS, excluded %d by MLB ID, %d coaches, %d retired)",
        len(filtered),
        excluded_id,
        excluded_coach,
        excluded_retired,
    )
    return filtered


class RegistryRosterError(ValueError):
    """The registry roster read failed or looked implausible — fail closed."""


def roster_source() -> str:
    """Client roster source: "sheet" (default) or "registry" (ROSTER_SOURCE)."""
    src = (os.environ.get("ROSTER_SOURCE") or "sheet").strip().lower()
    if src not in ("sheet", "registry"):
        raise RegistryRosterError(f"ROSTER_SOURCE must be 'sheet' or 'registry', got {src!r}")
    return src


def position_role(label) -> str:
    """Canon position label (RHP, SS, OF, SP, Two-Way, "RHP/OF", …) → this
    app's routing vocabulary: "Pitcher", "Hitter" or "Two-Way". Blank stays
    blank (callers already default a blank position to Hitter)."""
    text = str(label or "").strip()
    if not text:
        return ""
    if text.lower() in TWO_WAY_POSITION_LABELS:
        return "Two-Way"
    parts = [p.strip().lower() for p in text.replace(",", "/").split("/") if p.strip()]
    pitch = [p in PITCHER_POSITION_LABELS for p in parts]
    if all(pitch):
        return "Pitcher"
    if any(pitch):
        return "Two-Way"          # e.g. "RHP/OF"
    return "Hitter"


def fetch_registry_rows(session=None) -> tuple[dict, list[dict]]:
    """GET the sv-registry roster projection → (_meta, rows). Fails closed.

    Raises RegistryRosterError on: no token (no request is made), 401, 403
    (token not scoped read:roster-projection — a re-mint, not a retry), any
    other HTTP error, a body that is not the projection, a body that does not
    assert contains_no_contact_data, a row without slug/name, duplicate slugs,
    one MLB id on two slugs, or fewer than REGISTRY_MIN_ROWS rows.
    """
    token = (os.environ.get(REGISTRY_TOKEN_ENV) or "").strip()
    if not token:
        raise RegistryRosterError(f"{REGISTRY_TOKEN_ENV} is not set — ROSTER_SOURCE=registry needs the registry token")
    base = (os.environ.get("SV_REGISTRY_URL") or DEFAULT_REGISTRY_URL).rstrip("/")
    http = session or requests
    try:
        resp = http.get(base + REGISTRY_PROJECTION_PATH, timeout=30,
                        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    except requests.RequestException as exc:
        raise RegistryRosterError(f"registry roster projection unreachable: {exc}") from exc
    if resp.status_code == 401:
        raise RegistryRosterError(f"registry roster projection returned 401 — {REGISTRY_TOKEN_ENV} is wrong or revoked")
    if resp.status_code == 403:
        raise RegistryRosterError("registry roster projection returned 403 — token not scoped "
                                  "read:roster-projection (re-mint, don't retry)")
    if resp.status_code != 200:
        raise RegistryRosterError(f"registry roster projection returned HTTP {resp.status_code}")
    try:
        body = resp.json()
    except ValueError as exc:
        raise RegistryRosterError("registry roster projection did not return JSON") from exc
    meta = body.get("_meta") if isinstance(body, dict) else None
    rows = body.get("rows") if isinstance(body, dict) else None
    if not isinstance(meta, dict) or not isinstance(rows, list):
        raise RegistryRosterError("registry response is not a roster projection ({_meta, rows} missing)")
    if meta.get("contains_no_contact_data") is not True:
        raise RegistryRosterError("registry projection does not assert contains_no_contact_data — refusing it")
    slugs: set[str] = set()
    id_to_slug: dict[int, str] = {}
    for r in rows:
        slug = str((r or {}).get("slug") or "").strip() if isinstance(r, dict) else ""
        if not slug or not str(r.get("name") or "").strip():
            raise RegistryRosterError("registry projection has a row without slug/name")
        if slug in slugs:
            raise RegistryRosterError(f"registry projection repeats slug {slug!r}")
        slugs.add(slug)
        if r.get("mlb_id") not in (None, ""):
            try:
                mid = int(r["mlb_id"])
            except (TypeError, ValueError):
                continue
            if mid in id_to_slug:
                raise RegistryRosterError(f"registry projection maps MLB id {mid} to two slugs")
            id_to_slug[mid] = slug
    if len(rows) < REGISTRY_MIN_ROWS:
        raise RegistryRosterError(f"registry roster projection returned only {len(rows)} rows "
                                  f"(< {REGISTRY_MIN_ROWS}) — refusing a possibly-truncated roster")
    logger.info("Registry roster projection: %d rows, generated_at=%s", len(rows), meta.get("generated_at"))
    return meta, rows


def registry_row_to_sheet_row(row: dict) -> dict:
    """One projection row → a sheet-shaped raw row (the sheet's own headers),
    so filter_roster / normalize_player / every downstream consumer is
    unchanged. ONLY the fields this app uses are copied: lead agent, Slack
    channel, agency, visits, home state, high school, bats/throws, social
    handles and nicknames never enter a player dict (and so can never reach
    the public cache or dashboard). Peaks are absent from the projection
    (no canon source) and stay blank, which hides their chips/cards."""
    tier = str(row.get("tier") or "").strip().lower()
    if tier == "pro":
        level = "Pro"
    else:
        level = str(row.get("sheet_level") or row.get("level") or "").strip()
    retired = (str(row.get("career_status") or "").strip().lower() == "retired"
               or str(row.get("sheet_status") or "").strip().lower() == "retired")
    status = "Retired" if retired else str(row.get("sheet_status") or "").strip()
    mlb_id = row.get("mlb_id")
    return {
        "slug": str(row.get("slug") or "").strip(),
        "Player Name": str(row.get("name") or "").strip(),
        "MLB_ID": "" if mlb_id in (None, "") else str(mlb_id),
        "Org": str(row.get("org") or "").strip(),
        "Level": level,
        "Position": position_role(row.get("position")),
        "Primary Position": str(row.get("position") or "").strip(),
        "Draft Class": str(row.get("draft_class") or "").strip(),
        "Affiliate": str(row.get("affiliate") or "").strip(),
        "Tier": "" if row.get("priority_tier") in (None, "") else str(row.get("priority_tier")),
        "Status": status,
        "Is Coach": "Yes" if (row.get("is_coach") or tier == "coach") else "",
    }


def get_registry_clients(session=None) -> list[dict]:
    """Clients from the registry projection, filtered exactly like the sheet
    (Level Pro/NCAA/HS, no coaches, no retired, EXCLUDED_MLB_IDS)."""
    _meta, rows = fetch_registry_rows(session)
    players = filter_roster([registry_row_to_sheet_row(r) for r in rows])
    for p in players:
        p["is_client"] = True
    return players


def get_active_roster(url: Optional[str] = None) -> list[dict]:
    """
    Convenience wrapper: fetch + filter in one call.
    Returns clients (is_client=True) from ROSTER_SOURCE (sheet by default).
    """
    if url is None and roster_source() == "registry":
        return get_registry_clients()
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
    if not url:
        # No default any more — without the secret, skip recruits rather than
        # letting fetch_roster("") fall through to the CLIENT roster URL.
        logger.warning("RECRUITS_URL not configured — continuing without recruits")
        return []
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


def cache_safe_player(player: dict) -> dict:
    """The allowlisted subset of a player dict that may be written to the
    committed (public) roster cache — see config.ROSTER_CACHE_FIELDS."""
    return {k: player[k] for k in ROSTER_CACHE_FIELDS if k in player}


def _save_roster_cache(players: list[dict]):
    """Persist roster to disk so we can fall back if Sheets is unreachable.

    Only allowlisted fields are written — the file is public."""
    try:
        dir_path = os.path.dirname(ROSTER_CACHE_PATH)
        os.makedirs(dir_path, exist_ok=True)
        payload = {
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "source": roster_source(),
            "players": [cache_safe_player(p) for p in players],
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


def _cached_roster_count() -> int:
    """Player count in the on-disk cache regardless of age — used only for
    the plausibility comparison, where staleness doesn't matter."""
    try:
        with open(ROSTER_CACHE_PATH) as f:
            return len(json.load(f).get("players", []))
    except Exception:
        return 0


def pro_player_names() -> set[str]:
    """Lowercased names of every player whose master-sheet Level is Pro.

    The summer-ball tracker uses this to drop placements for players who
    signed pro after Kent's placement sheet was uploaded (drafted mid-summer),
    so flipping a player to Pro on the master sheet is the only edit needed.
    Reads the on-disk cache regardless of age — a stale answer beats
    resurrecting a drafted player's summer cards when the sheet fetch flakes.
    """
    try:
        with open(ROSTER_CACHE_PATH) as f:
            players = json.load(f).get("players", [])
    except Exception:
        return set()
    return {
        name
        for p in players
        if p.get("level") == "Pro"
        and (name := (p.get("player_name") or "").strip().lower())
    }


def all_roster_names() -> set[str]:
    """Lowercased names of every player on the roster + recruits sheets.

    Reads the on-disk cache regardless of age — used by non-destructive
    gates (e.g. summer placement filtering). Returns an empty set when no
    cache exists; callers must fail OPEN on an empty set (filter nothing)
    rather than treating "no roster" as "nobody is on the roster".
    """
    try:
        with open(ROSTER_CACHE_PATH) as f:
            players = json.load(f).get("players", [])
    except Exception:
        return set()
    return {
        name
        for p in players
        if (name := (p.get("player_name") or "").strip().lower())
    }


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
        # On the registry, never serve a cache that the SHEET wrote — that
        # would be a silent sheet fallback by another route.
        if roster_source() == "registry" and data.get("source") != "registry":
            logger.error("Roster cache was written from source %r — not usable on ROSTER_SOURCE=registry",
                         data.get("source", "sheet"))
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


# True only when the most recent get_all_players() returned a FRESH,
# plausibility-checked sheet fetch (not the stale-cache fallback). Destructive
# roster-hygiene steps (pruning append-only stores) must check this — pruning
# off a stale cache could delete a just-added player's history.
_roster_fetch_fresh = False


def roster_is_fresh() -> bool:
    """Did the last get_all_players() come from a successful fresh fetch?"""
    return _roster_fetch_fresh


def get_all_players() -> list[dict]:
    """
    Fetch both clients and recruits, combined.
    Falls back to cached roster if Google Sheets is unreachable.

    Pro players have their team/affiliate overwritten from the MLB Stats API
    after sheet load — the sheet is the source of identity (name + mlb_id),
    but the API is the source of truth for current org and current affiliate.
    """
    global _roster_fetch_fresh
    _roster_fetch_fresh = False
    try:
        clients = get_active_roster()
        recruits = get_recruits()
        players = clients + recruits
        # Plausibility guard: a truncated export or a non-roster page served
        # with HTTP 200 can yield an empty or sharply shrunken list. Never
        # let that flow downstream or overwrite the good fallback cache.
        prior = _cached_roster_count()
        if prior >= 20 and len(players) < prior * 0.5:
            raise ValueError(
                f"implausible roster: fetched {len(players)} players vs {prior} cached"
            )
        if roster_source() == "sheet":
            _log_dual_run(clients)
        _enrich_pro_team_from_api(players)
        _save_roster_cache(players)
        _roster_fetch_fresh = True
        if roster_source() == "registry":
            _check_registry_peaks(clients)
        return players
    except Exception as exc:
        if isinstance(exc, RegistryRosterError):
            # Loud in the Actions log. The registry path never falls back to
            # the sheet; the last good REGISTRY cache (< 24 h) keeps the
            # dashboard up with destructive steps disabled (roster_is_fresh
            # stays False), and an empty result aborts the run.
            print(f"::error::Registry roster read failed: {exc}")
        logger.exception("Roster fetch failed or implausible — trying cached roster")
        cached = _load_roster_cache()
        if cached:
            if isinstance(exc, RegistryRosterError) or os.environ.get("ROSTER_SOURCE", "").strip().lower() == "registry":
                _alert_registry_on_saved_list(exc)
            return cached
        logger.error("No roster available (fetch failed, no usable cache)")
        return []


# ---------------------------------------------------------------------------
# Dual run (ROSTER_SOURCE=sheet + token set): compare, never act.
# ---------------------------------------------------------------------------

def dual_run_diff(sheet_clients: list[dict], registry_clients: list[dict]) -> dict:
    """Compare the sheet's clients with the registry's, matched by MLB id
    first, then by exact (case-insensitive) name. Returns lists of names per
    category — for LOCAL review only (scripts/roster_dual_run.py); the CI log
    gets counts only, because this repo's Actions logs are public."""
    reg_by_id = {p["mlb_id"]: p for p in registry_clients if p.get("mlb_id")}
    reg_by_name = {p["player_name"].strip().lower(): p for p in registry_clients}
    matched_reg: set[str] = set()
    out = {k: [] for k in ("sheet_only", "registry_only", "name_differs", "level_differs",
                           "role_differs", "org_differs")}
    for s in sheet_clients:
        r = (reg_by_id.get(s.get("mlb_id")) if s.get("mlb_id") else None) \
            or reg_by_name.get(s["player_name"].strip().lower())
        if r is None:
            out["sheet_only"].append(s["player_name"])
            continue
        matched_reg.add(r.get("slug") or r["player_name"])
        if r["player_name"] != s["player_name"]:
            out["name_differs"].append(f"{s['player_name']} → {r['player_name']}")
        if r.get("level") != s.get("level"):
            out["level_differs"].append(f"{s['player_name']}: {s.get('level')} → {r.get('level')}")
        s_role = s.get("position") or "Hitter"
        r_role = r.get("position") or "Hitter"
        if s_role != r_role:
            out["role_differs"].append(f"{s['player_name']}: {s_role} → {r_role}")
        if (s.get("team") or "").strip().lower() != (r.get("team") or "").strip().lower():
            out["org_differs"].append(f"{s['player_name']}: {s.get('team')!r} → {r.get('team')!r}")
    for r in registry_clients:
        if (r.get("slug") or r["player_name"]) not in matched_reg:
            out["registry_only"].append(r["player_name"])
    return out


def _log_dual_run(sheet_clients: list[dict]) -> None:
    if not (os.environ.get(REGISTRY_TOKEN_ENV) or "").strip():
        return
    try:
        diff = dual_run_diff(sheet_clients, get_registry_clients())
    except Exception as exc:  # observational only — never affects the run
        logger.warning("[dual-run] registry read failed: %s", exc)
        _alert_dual_run_failed(exc)
        return
    counts = {k: len(v) for k, v in diff.items()}
    logger.info("[dual-run] sheet %d vs registry clients — differences (counts only; "
                "details: python -m scripts.roster_dual_run): %s", len(sheet_clients), counts)


# ---------------------------------------------------------------------------
# #sv-automation alerts for the registry path. Before these, a registry read
# failure that fell back to the saved list, a failed dual-run read, and a
# registry roster with no peaks all landed only in the (public) Actions log.
# Each alert posts at most once a day per kind (timestamps in a small state
# file the pulse run commits), goes through scripts/_automation_notify.py,
# and never affects the run. No player names, URLs or tokens in the text.
# ---------------------------------------------------------------------------

ROSTER_ALERT_STATE_PATH = os.path.join(os.path.dirname(ROSTER_CACHE_PATH), "_roster_source_alerts.json")
_ROSTER_ALERT_COOLDOWN_H = 24
_PEAK_KEYS = ("peak_war", "peak_wrc_plus", "peak_era_20tbf")
_VARIABLES_URL = "https://github.com/Stadium-Ventures/sv-dugout-pulse/settings/variables/actions"
_SECRETS_URL = "https://github.com/Stadium-Ventures/sv-dugout-pulse/settings/secrets/actions"


def _send_automation(text: str) -> bool:
    """Seam for tests. The one door to #sv-automation."""
    from scripts._automation_notify import post_automation
    return post_automation(text)


def _post_roster_alert(kind: str, text: str) -> bool:
    """Post once per _ROSTER_ALERT_COOLDOWN_H per kind. Never raises."""
    try:
        now = datetime.now(timezone.utc)
        state: dict = {}
        try:
            with open(ROSTER_ALERT_STATE_PATH) as f:
                state = json.load(f) or {}
        except Exception:
            state = {}
        try:
            last = datetime.fromisoformat(str(state.get(kind, "")))
            if (now - last).total_seconds() < _ROSTER_ALERT_COOLDOWN_H * 3600:
                return False
        except ValueError:
            pass  # never alerted, or unreadable timestamp
        if not _send_automation(text):
            return False
        state[kind] = now.isoformat()
        dir_path = os.path.dirname(ROSTER_ALERT_STATE_PATH)
        os.makedirs(dir_path, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        os.replace(tmp, ROSTER_ALERT_STATE_PATH)
        return True
    except Exception:
        logger.exception("Roster-source alert failed (non-fatal)")
        return False


def _plain_reason(exc: BaseException) -> tuple[str, str]:
    """(what happened, what to do) in plain English for a registry read error."""
    msg = str(exc)
    if "is not set" in msg:
        return ("the SV Registry key is missing from this repo's secrets",
                f"add the SV_REGISTRY_ROSTER_TOKEN secret ({_SECRETS_URL})")
    if "401" in msg:
        return ("SV Registry turned the key down as wrong or revoked",
                f"re-issue the dugout-pulse registry key and update SV_REGISTRY_ROSTER_TOKEN ({_SECRETS_URL})")
    if "403" in msg:
        return ("SV Registry said the key has no roster access",
                "re-issue the dugout-pulse registry key with roster read access")
    if "unreachable" in msg or "HTTP " in msg:
        return ("SV Registry did not answer normally",
                "check that sv-registry.vercel.app is up; it usually clears on its own")
    if "only" in msg or "implausible" in msg:
        return ("SV Registry sent a client list that looked cut short",
                "check the roster projection in SV Registry before the next run")
    return ("SV Registry sent something that did not look like the client list",
            "check the roster projection in SV Registry")


def _alert_registry_on_saved_list(exc: BaseException) -> None:
    what, fix = _plain_reason(exc)
    _post_roster_alert("registry_read_failed", (
        ":warning: *The client list could not be read from SV Registry, so the dashboard is running on the last saved list.*\n"
        f"How we know: this run's roster read failed because {what}. Stats still update, but roster changes "
        "will not show, and the runs start failing once the saved list is a day old.\n"
        f"What to do: 👤 {fix}. To go back to the sheet right away, set ROSTER_SOURCE to sheet ({_VARIABLES_URL})."
    ))


def _alert_dual_run_failed(exc: BaseException) -> None:
    what, fix = _plain_reason(exc)
    _post_roster_alert("dual_run_read_failed", (
        ":warning: *The daily roster check against SV Registry could not run.*\n"
        f"How we know: the stats run tried to read the client list from SV Registry to compare it with the "
        f"sheet, and {what}. The dashboard is fine; it still uses the sheet.\n"
        f"What to do: 👤 {fix}. Days without a clean comparison do not count toward the switch."
    ))


def _check_registry_peaks(clients: list[dict]) -> None:
    """Registry mode: if no Pro client carries any peak, the dashboard's peak
    chips are blank for everyone. Say so instead of shipping it silently."""
    try:
        pros = [p for p in clients if p.get("level") == "Pro"]
        if not pros or any(p.get(k) for p in pros for k in _PEAK_KEYS):
            return
        logger.error("Registry roster carries no peak projections for any of %d Pro clients", len(pros))
        _post_roster_alert("registry_peaks_missing", (
            ":warning: *Peak projection chips are blank for every Pro client on the dashboard.*\n"
            f"How we know: the client list now comes from SV Registry, and it carried no Peak WAR, wRC+ or ERA "
            f"for any of the {len(pros)} Pro players.\n"
            f"What to do: 👤 set ROSTER_SOURCE back to sheet to bring the chips back ({_VARIABLES_URL}). "
            "🛠️ Switch again once SV Registry serves peaks."
        ))
    except Exception:
        logger.exception("Peak check failed (non-fatal)")
