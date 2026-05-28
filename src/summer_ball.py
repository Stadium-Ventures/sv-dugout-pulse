"""SV Dugout Pulse — Summer Ball roster discovery + live stats.

Architecture
------------
Eight (planned) collegiate summer leagues, each with its own roster + stats
source. We model each as a subclass of `SummerLeague` exposing two methods:

  discover_rosters() -> list[PlayerEntry]
      Walk the league's team list and return every player with their college
      affiliation. Used by the daily refresh to build a global name -> team
      map so we can answer "where is Sam Harry playing this summer?"

  fetch_player_stats(entry, start_date, end_date) -> dict | None
      Pull season-window stats for a known summer-team player. Mirrors the
      NCAA spring shape so the email + dashboard render code doesn't have
      to special-case summer.

`SummerBallAggregator` runs all leagues, writes
`data/summer_ball_rosters.json` with a top-level health envelope so the
email + dashboard can show *which* leagues succeeded vs failed each day
(per Tom's "transparency is not an afterthought" ask).

Failure mode for a single league must not block the rest — we catch +
record per league, continue with the others.

Initial implementation status (see registry below):
  - NorthwoodsLeague:  attempted (HTML scrape of public stats pages)
  - CapeCodLeague:     attempted (HTML scrape of capecodbaseball.org)
  - everything else:   stub, returns NotImplemented status

We iterate from real failures rather than try to guess all 8 sites blind.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import os

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMER_ROSTER_PATH = REPO_ROOT / "data" / "summer_ball_rosters.json"


# =============================================================================
# Plain (unproxied) HTTP — for league sites that aren't WAF-gated
# =============================================================================

def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503])
    adapter = HTTPAdapter(pool_connections=8, pool_maxsize=16, max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.4 Safari/605.1.15"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


_http = _make_session()


# =============================================================================
# Residential-proxy fetch — for WAF-gated sources (Pointstreak, Cape Cod, etc.)
#
# Pointstreak is Incapsula-gated; plain `requests` returns 404. Reuse the
# Webshare/IPRoyal/ScraperAPI pool we already use for StatBroadcast.
# =============================================================================

_PROXY_POOL_ENV = ("SB_HTTP_PROXY", "SB_HTTP_PROXY_2", "SB_HTTP_PROXY_3")


def _residential_proxy_pool() -> list[str]:
    return [v for v in (os.environ.get(k, "").strip() for k in _PROXY_POOL_ENV) if v]


def _proxy_label(proxy: str) -> str:
    if not proxy:
        return "direct"
    try:
        from urllib.parse import urlparse
        return urlparse(proxy).hostname or "?"
    except Exception:
        return "?"


def fetch_via_residential_proxy(url: str, timeout: int = 20) -> tuple[Optional[str], dict]:
    """Fetch a URL through residential proxy + Chrome TLS impersonation.

    Rotates through the proxy pool on 403 / 5xx, returns the first successful
    body. Returns (None, diagnostics) if every proxy was rejected.

    ScraperAPI proxies use plain `requests` with verify=False (their own
    proxy mode handles TLS); residential providers use curl_cffi chrome120.
    Diagnostics tells the caller which proxy worked + how many attempts.
    """
    pool = _residential_proxy_pool()
    diagnostics: dict = {"attempts": [], "active": None}
    if not pool:
        diagnostics["error"] = "no_residential_proxies_configured"
        return None, diagnostics

    for proxy in pool:
        label = _proxy_label(proxy)
        try:
            if "scraperapi" in proxy.lower():
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                sess = requests.Session()
                sess.proxies = {"http": proxy, "https": proxy}
                resp = sess.get(url, verify=False, timeout=timeout)
            else:
                from curl_cffi import requests as _cr  # local import
                sess = _cr.Session(impersonate="chrome120",
                                   proxies={"http": proxy, "https": proxy})
                resp = sess.get(url, timeout=timeout)
            status = resp.status_code
            diagnostics["attempts"].append({"proxy": label, "status": status})
            if 200 <= status < 300 and resp.text:
                diagnostics["active"] = label
                return resp.text, diagnostics
        except Exception as e:
            diagnostics["attempts"].append({"proxy": label, "error": str(e)[:120]})
    diagnostics["error"] = "all_proxies_failed"
    return None, diagnostics


def _normalize_name(name: str) -> str:
    """Strip accents/punctuation, collapse whitespace, lowercase.

    Two NCAA roster names and summer-ball roster names rarely match byte-for-byte
    (J.J. Smith vs J. J. Smith vs JJ Smith). Normalizing lets us cross-reference.
    """
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-zA-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _normalize_college(college: str) -> str:
    """Normalize common college-name variants.

    "Sacramento State" vs "Sacramento St." vs "Sac State" all map to the same key.
    """
    s = _normalize_name(college)
    s = s.replace(" university", "").replace(" college", "")
    s = re.sub(r"\bst\b", "state", s)
    return s


# =============================================================================
# Data shape
# =============================================================================

@dataclass
class PlayerEntry:
    name: str
    college: str
    summer_team: str
    league: str            # "Cape Cod" / "Northwoods" / etc.
    source_id: str = ""    # league-internal id used for stats lookup
    profile_url: str = ""  # for transparency / debugging
    raw_name: str = ""     # original, for matching display
    raw_college: str = ""  # original, for matching display

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class LeagueHealth:
    league: str
    status: str  # "ok" | "failed" | "not_implemented"
    player_count: int = 0
    team_count: int = 0
    error: Optional[str] = None
    duration_ms: int = 0
    sample_players: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# =============================================================================
# League base
# =============================================================================

class SummerLeague:
    """Abstract base for a single summer-ball league source."""

    name: str = "Unknown"
    short_name: str = "Unknown"

    def discover_rosters(self) -> list[PlayerEntry]:
        raise NotImplementedError

    def fetch_player_stats(
        self,
        entry: PlayerEntry,
        start_date: date,
        end_date: date,
    ) -> Optional[dict]:
        """Return stats dict in the same shape as NCAA spring (see historical_stats
        ._build_window_entry stats keys) OR None when no game in window.

        Phase 2 — the live-stats half — lands here per-league. For now, the base
        returns None so the pipeline can call this safely without breaking.
        """
        return None


# -----------------------------------------------------------------------------
# Northwoods League
# -----------------------------------------------------------------------------

class NorthwoodsLeague(SummerLeague):
    """Northwoods League stats are iframed from Pointstreak.

    Pointstreak is Incapsula-gated, so the actual fetch goes through the
    residential proxy pool (same one we use for StatBroadcast). The
    Northwoods League's own site is NOT WAF-gated, so we use that to
    discover the current season's leagueid/seasonid before falling back
    to a known default.
    """

    name = "Northwoods League"
    short_name = "Northwoods"

    HOST_URL = "https://northwoodsleague.com/baseball/statistics/?type=batting&sort=AVG"
    # Known fallback IDs from the 2026 season iframe; auto-discovery below
    # overrides these if the iframe URL has changed.
    _DEFAULT_LEAGUE_ID = 120
    _DEFAULT_SEASON_ID = 31974

    def _resolve_ids(self) -> tuple[int, int]:
        try:
            html = _http.get(self.HOST_URL, timeout=15).text
            m = re.search(r"pointstreak\.com[^\"']*leagueid=(\d+)[^\"']*seasonid=(\d+)", html)
            if m:
                return int(m.group(1)), int(m.group(2))
        except Exception:
            logger.exception("Northwoods: leagueid/seasonid discovery failed")
        return self._DEFAULT_LEAGUE_ID, self._DEFAULT_SEASON_ID

    def discover_rosters(self) -> list[PlayerEntry]:
        league_id, season_id = self._resolve_ids()
        entries: dict[str, PlayerEntry] = {}
        for view in ("batting", "pitching"):
            url = (
                f"https://pointstreak.com/stats.html"
                f"?leagueid={league_id}&seasonid={season_id}&view={view}"
            )
            html, diag = fetch_via_residential_proxy(url, timeout=25)
            if not html:
                logger.warning("Northwoods %s: all proxies failed (%s)", view, diag)
                continue
            entries.update(_parse_pointstreak_table(
                html, league=self.short_name, profile_url=url,
            ))
        return list(entries.values())


def _parse_pointstreak_table(
    html: str, *, league: str, profile_url: str
) -> dict[str, PlayerEntry]:
    """Pull (player, team) pairs from a Pointstreak stats page.

    Pointstreak's HTML uses <table> with <tr> rows; player name is usually
    the first text cell, team is one of the later cells. We pick out the
    leftmost cell containing letters + a space as the name, then the next
    cell that looks like a team name.
    """
    out: dict[str, PlayerEntry] = {}
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue
        # First non-blank row with mostly <th> is the header
        headers: list[str] = []
        for row in rows:
            th_cells = row.find_all("th")
            if th_cells:
                headers = [t.get_text(strip=True).lower() for t in th_cells]
                break
        if not headers or "player" not in " ".join(headers):
            continue
        # find column indices
        try:
            name_col = next(i for i, h in enumerate(headers) if "player" in h or h == "name")
        except StopIteration:
            continue
        team_col = next((i for i, h in enumerate(headers) if "team" in h), None)
        college_col = next(
            (i for i, h in enumerate(headers) if "college" in h or h == "school"),
            None,
        )
        for row in rows:
            cells = row.find_all("td")
            if len(cells) <= name_col:
                continue
            name = cells[name_col].get_text(" ", strip=True)
            if not name or any(c.isdigit() for c in name) and len(name) < 4:
                continue
            team = cells[team_col].get_text(" ", strip=True) if team_col is not None and len(cells) > team_col else ""
            college = cells[college_col].get_text(" ", strip=True) if college_col is not None and len(cells) > college_col else ""
            key = _normalize_name(name)
            if not key or key in out:
                continue
            out[key] = PlayerEntry(
                name=key,
                college=_normalize_college(college),
                summer_team=team,
                league=league,
                profile_url=profile_url,
                raw_name=name,
                raw_college=college,
            )
    return out


# -----------------------------------------------------------------------------
# Cape Cod League
# -----------------------------------------------------------------------------

class CapeCodLeague(SummerLeague):
    """Cape Cod League — capecodbaseball.org. Team rosters live at
    /teams/{slug}/roster. Player stats at /players/{slug}.

    URL discovery is brittle and will get iterated as we hit real failures.
    """

    name = "Cape Cod League"
    short_name = "Cape Cod"

    BASE = "https://capecodbaseball.org"
    TEAMS_URL = f"{BASE}/teams/"

    # Known team slugs (manually seeded; expand as needed).
    TEAM_SLUGS = [
        "bourne-braves", "brewster-whitecaps", "chatham-anglers", "cotuit-kettleers",
        "falmouth-commodores", "harwich-mariners", "hyannis-harbor-hawks",
        "orleans-firebirds", "wareham-gatemen", "yarmouth-dennis-red-sox",
    ]

    def discover_rosters(self) -> list[PlayerEntry]:
        entries: list[PlayerEntry] = []
        for slug in self.TEAM_SLUGS:
            url = f"{self.BASE}/teams/{slug}/roster/"
            try:
                resp = _http.get(url, timeout=20)
                if resp.status_code != 200:
                    logger.info("CapeCod: %s -> HTTP %s", slug, resp.status_code)
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                # Look for any roster-table heuristic: a table whose rows
                # contain (name, position, college/year). Iterate broadly.
                for row in soup.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    texts = [c.get_text(" ", strip=True) for c in cells]
                    name_text = texts[0] if texts else ""
                    if not name_text or len(name_text) > 60:
                        continue
                    # Look for a "college" cell — heuristic: contains
                    # "University" / "College" / well-known state name.
                    college = ""
                    for t in texts[1:]:
                        if any(k in t.lower() for k in (
                            "university", "college", "state", "tech", "a&m", "polytech",
                        )):
                            college = t
                            break
                    if not college:
                        continue
                    entries.append(PlayerEntry(
                        name=_normalize_name(name_text),
                        college=_normalize_college(college),
                        summer_team=slug.replace("-", " ").title(),
                        league=self.short_name,
                        profile_url=url,
                        raw_name=name_text,
                        raw_college=college,
                    ))
            except Exception:
                logger.exception("CapeCod: error scraping %s", slug)
        return entries


# -----------------------------------------------------------------------------
# Stubs — these record "not_implemented" status so the transparency page
# shows them; we iterate from there.
# -----------------------------------------------------------------------------

class _StubLeague(SummerLeague):
    """Placeholder — surfaces in the transparency view but does no work yet."""

    def discover_rosters(self) -> list[PlayerEntry]:
        raise NotImplementedError(f"{self.name}: scraper not yet built")


class CoastalPlainLeague(_StubLeague):
    name = "Coastal Plain League"
    short_name = "Coastal Plain"


class NECBL(_StubLeague):
    name = "New England Collegiate Baseball League"
    short_name = "NECBL"


class AppalachianLeague(_StubLeague):
    name = "Appalachian League"
    short_name = "Appalachian"


class AlaskaBaseballLeague(_StubLeague):
    name = "Alaska Baseball League"
    short_name = "Alaska"


class FloridaCollegiateLeague(_StubLeague):
    name = "Florida Collegiate Summer League"
    short_name = "Florida"


class ProspectLeague(_StubLeague):
    name = "Prospect League"
    short_name = "Prospect"


LEAGUES: list[SummerLeague] = [
    NorthwoodsLeague(),
    CapeCodLeague(),
    CoastalPlainLeague(),
    NECBL(),
    AppalachianLeague(),
    AlaskaBaseballLeague(),
    FloridaCollegiateLeague(),
    ProspectLeague(),
]


# =============================================================================
# Aggregator + transparency
# =============================================================================

class SummerBallAggregator:
    """Coordinates per-league discovery + writes the daily roster snapshot."""

    def __init__(self, leagues: Optional[list[SummerLeague]] = None):
        self.leagues = leagues if leagues is not None else LEAGUES

    def discover_all(self) -> tuple[list[PlayerEntry], list[LeagueHealth]]:
        all_players: list[PlayerEntry] = []
        health: list[LeagueHealth] = []
        for league in self.leagues:
            t0 = datetime.now()
            try:
                players = league.discover_rosters()
            except NotImplementedError as e:
                health.append(LeagueHealth(
                    league=league.short_name, status="not_implemented",
                    error=str(e),
                    duration_ms=int((datetime.now() - t0).total_seconds() * 1000),
                ))
                continue
            except Exception as e:
                logger.exception("Discovery failed: %s", league.short_name)
                health.append(LeagueHealth(
                    league=league.short_name, status="failed", error=str(e)[:200],
                    duration_ms=int((datetime.now() - t0).total_seconds() * 1000),
                ))
                continue

            teams = {p.summer_team for p in players}
            samples = [p.raw_name for p in players[:5]]
            health.append(LeagueHealth(
                league=league.short_name, status="ok",
                player_count=len(players), team_count=len(teams),
                sample_players=samples,
                duration_ms=int((datetime.now() - t0).total_seconds() * 1000),
            ))
            all_players.extend(players)
        return all_players, health

    def write_roster_file(self, ncaa_clients: list[dict]) -> dict:
        """Run discovery, match against NCAA client roster, write the snapshot.

        Returns the snapshot dict (also persisted to data/summer_ball_rosters.json).
        """
        players, health = self.discover_all()

        # Build a name -> [PlayerEntry] index, then for each NCAA client try
        # to find a unique match on (normalized_name, normalized_college).
        by_name: dict[str, list[PlayerEntry]] = {}
        for p in players:
            by_name.setdefault(p.name, []).append(p)

        matched: list[dict] = []
        unmatched: list[dict] = []
        ambiguous: list[dict] = []

        for c in ncaa_clients:
            ncaa_name = _normalize_name(c.get("player_name", ""))
            ncaa_college = _normalize_college(c.get("team", ""))
            candidates = by_name.get(ncaa_name, [])
            if not candidates:
                unmatched.append({"player_name": c.get("player_name"), "college": c.get("team")})
                continue
            # Prefer college-match; if multiple, mark ambiguous; if zero, name-only.
            college_match = [p for p in candidates if p.college == ncaa_college]
            if len(college_match) == 1:
                p = college_match[0]
                matched.append({
                    "player_name": c.get("player_name"),
                    "college": c.get("team"),
                    "summer_team": p.summer_team,
                    "league": p.league,
                    "match_strength": "name+college",
                    "profile_url": p.profile_url,
                })
            elif len(college_match) > 1:
                ambiguous.append({
                    "player_name": c.get("player_name"),
                    "college": c.get("team"),
                    "candidates": [{"summer_team": x.summer_team, "league": x.league} for x in college_match],
                })
            elif len(candidates) == 1:
                p = candidates[0]
                matched.append({
                    "player_name": c.get("player_name"),
                    "college": c.get("team"),
                    "summer_team": p.summer_team,
                    "league": p.league,
                    "match_strength": "name-only",
                    "profile_url": p.profile_url,
                })
            else:
                ambiguous.append({
                    "player_name": c.get("player_name"),
                    "college": c.get("team"),
                    "candidates": [{"summer_team": x.summer_team, "league": x.league} for x in candidates],
                })

        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "league_health": [h.to_dict() for h in health],
            "ncaa_clients_total": len(ncaa_clients),
            "ncaa_clients_matched": len(matched),
            "ncaa_clients_unmatched": len(unmatched),
            "ncaa_clients_ambiguous": len(ambiguous),
            "matched": matched,
            "unmatched": unmatched,
            "ambiguous": ambiguous,
            "all_players_count": len(players),
        }

        SUMMER_ROSTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        SUMMER_ROSTER_PATH.write_text(json.dumps(snapshot, indent=2))
        return snapshot


def load_snapshot() -> Optional[dict]:
    if not SUMMER_ROSTER_PATH.exists():
        return None
    return json.loads(SUMMER_ROSTER_PATH.read_text())
