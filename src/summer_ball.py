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

# Cape Cod's cert chain isn't trusted by the Actions runner store; silence
# the urllib3 InsecureRequestWarning for the targeted verify=False call.
try:
    import urllib3 as _urllib3
    _urllib3.disable_warnings(_urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass


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
            body_size = len(resp.text or "")
            diagnostics["attempts"].append(
                {"proxy": label, "status": status, "bytes": body_size}
            )
            # A 200 with <5KB body is almost always a Cloudflare/Incapsula JS
            # challenge page, not real content. Try the next proxy instead of
            # blindly trusting status code.
            if 200 <= status < 300 and body_size >= 5000:
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


_COLLEGE_ALIASES = {
    # Roster sheet uses "SE Louisiana"; summer-ball rosters publish
    # "Southeastern Louisiana". Collapse to one key so name+college matches
    # don't get routed to the "needs review" bucket as false college mismatches.
    "se louisiana": "southeastern louisiana",
    "se louisiana state": "southeastern louisiana",
}


def _normalize_college(college: str) -> str:
    """Normalize common college-name variants.

    "Sacramento State" vs "Sacramento St." vs "Sac State" all map to the same key.
    """
    s = _normalize_name(college)
    s = s.replace(" university", "").replace(" college", "")
    s = re.sub(r"\bst\b", "state", s)
    if s in _COLLEGE_ALIASES:
        s = _COLLEGE_ALIASES[s]
    return s


def _initial_last_key(name: str) -> str:
    """Build "F. Last"-style key from a full or abbreviated name.

    Inputs we handle:
      "Aiden Robbins"   -> "a robbins"
      "Robbins, A"      -> "a robbins"
      "Robbins, Aiden"  -> "a robbins"
      "A. J. Smith"     -> "a smith"   (first initial of first token)
      "JJ Smith"        -> "j smith"
    """
    if not name:
        return ""
    raw = name.strip()
    # "Last, First" form -> swap
    if "," in raw:
        last, first = [p.strip() for p in raw.split(",", 1)]
        if not first or not last:
            return ""
        first_initial = first[0]
    else:
        parts = raw.split()
        if len(parts) < 2:
            return ""
        first_initial = parts[0][0]
        last = parts[-1]
    return _normalize_name(f"{first_initial} {last}")


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

class PointstreakBackedLeague(SummerLeague):
    """Base class for leagues that publish stats via Pointstreak iframe.

    Subclasses set `host_url` (the league's own site, used to auto-discover
    the current leagueid/seasonid from the iframe src) and known fallback
    `_DEFAULT_LEAGUE_ID` / `_DEFAULT_SEASON_ID` for when discovery fails.
    """

    host_url: str = ""              # league site to scrape for iframe src
    _DEFAULT_HOST: str = "bbstats.pointstreak.com"
    _DEFAULT_LEAGUE_ID: int = 0
    _DEFAULT_SEASON_ID: int = 0

    def _resolve_ids(self) -> tuple[str, int, int]:
        if not self.host_url:
            return self._DEFAULT_HOST, self._DEFAULT_LEAGUE_ID, self._DEFAULT_SEASON_ID
        try:
            html = _http.get(self.host_url, timeout=15).text
            m = re.search(
                r"([a-z0-9.-]+\.pointstreak\.com)[^\"']*leagueid=(\d+)[^\"']*seasonid=(\d+)",
                html,
            )
            if m:
                return m.group(1), int(m.group(2)), int(m.group(3))
        except Exception:
            logger.exception("%s: host/leagueid/seasonid discovery failed", self.short_name)
        return self._DEFAULT_HOST, self._DEFAULT_LEAGUE_ID, self._DEFAULT_SEASON_ID

    def discover_rosters(self) -> list[PlayerEntry]:
        host, league_id, season_id = self._resolve_ids()
        if not league_id or not season_id:
            raise RuntimeError(
                f"{self.short_name}: no leagueid/seasonid (autodiscovery from "
                f"{self.host_url or '<none>'} failed and no defaults set)"
            )
        logger.info(
            "%s: using host=%s leagueid=%s seasonid=%s",
            self.short_name, host, league_id, season_id,
        )
        entries: dict[str, PlayerEntry] = {}
        for view in ("batting", "pitching"):
            url = (
                f"https://{host}/stats.html"
                f"?leagueid={league_id}&seasonid={season_id}&view={view}"
            )
            html, diag = fetch_via_residential_proxy(url, timeout=25)
            if not html:
                logger.warning("%s %s: all proxies failed (%s)",
                               self.short_name, view, diag)
                continue
            logger.info(
                "%s %s: fetched %d bytes via %s",
                self.short_name, view, len(html), diag.get("active"),
            )
            found = _parse_pointstreak_table(
                html, league=self.short_name, profile_url=url,
            )
            logger.info("%s %s: parser extracted %d players",
                        self.short_name, view, len(found))
            entries.update(found)
        return list(entries.values())


class NorthwoodsLeague(SummerLeague):
    """Northwoods runs an in-house stats system since 2020. The Pointstreak
    iframe on their site (leagueid=120, seasonid=31974) is a 2019 archive —
    do NOT pull from it. The live data is on northwoodsleague.com itself but
    has no documented JSON API; HTML scraping required.

    Adapter pending — research dated 2026-06-02 confirmed in-house system but
    we haven't reverse-engineered the page structure yet.
    """
    name = "Northwoods League"
    short_name = "Northwoods"

    def discover_rosters(self) -> list[PlayerEntry]:
        raise RuntimeError(
            "Northwoods: in-house stats system at northwoodsleague.com "
            "(post-2020); HTML adapter pending"
        )


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
# Stubs — these record "not_implemented" status so the transparency page
# shows them; we iterate from there.
# -----------------------------------------------------------------------------

class _StubLeague(SummerLeague):
    """Placeholder — surfaces in the transparency view but does no work yet."""

    def discover_rosters(self) -> list[PlayerEntry]:
        raise NotImplementedError(f"{self.name}: scraper not yet built")


# -----------------------------------------------------------------------------
# MLB Stats API leagues (Cape Cod, Appalachian, MLB Draft League)
# -----------------------------------------------------------------------------
#
# After Pointstreak's May 31, 2026 sunset we discovered MLB hosts three of our
# target summer circuits on its official Stats API:
#   - Cape Cod (leagueId 565) — MLB Partner League
#   - Appalachian (leagueId 120) — MLB developmental
#   - MLB Draft League (leagueId 5536) — MLB-owned
#
# The Stats API is the same backbone that powers MLB.com / minor league sites.
# It returns full names (no "Last, F" garbage), exposes college affiliation
# directly on player profiles, and gives live AVG/OBP/SLG/PA — no proxy, no
# WAF, no auth.

class MLBStatsAPILeague(SummerLeague):
    """A summer/wood-bat league hosted on statsapi.mlb.com.

    Subclasses set name, short_name, and `league_id`. Season defaults to the
    current year.
    """

    SPORT_ID: int = 22  # "College Baseball" in MLB's catalog
    BASE: str = "https://statsapi.mlb.com/api/v1"
    league_id: int = 0
    season: int = 0  # 0 = current year

    def _season(self) -> int:
        return self.season or date.today().year

    def discover_rosters(self) -> list[PlayerEntry]:
        if not self.league_id:
            raise RuntimeError(f"{self.short_name}: league_id not set")
        season = self._season()
        teams_url = (
            f"{self.BASE}/teams"
            f"?sportIds={self.SPORT_ID}&leagueIds={self.league_id}&season={season}"
        )
        teams_resp = _http.get(teams_url, timeout=15).json()
        teams = teams_resp.get("teams", []) or []
        logger.info(
            "%s: %d teams for %d season", self.short_name, len(teams), season,
        )
        entries: list[PlayerEntry] = []
        for team in teams:
            team_id = team.get("id")
            team_name = team.get("name", "")
            team_abbr = team.get("abbreviation", "")
            if not team_id:
                continue
            # fullRoster is the broadest — includes pitchers + hitters; some
            # leagues don't populate "active" until games start.
            roster_url = (
                f"{self.BASE}/teams/{team_id}/roster"
                f"?season={season}&rosterType=fullRoster"
            )
            try:
                roster = _http.get(roster_url, timeout=15).json().get("roster", [])
            except Exception:
                logger.exception(
                    "%s: roster fetch failed for team %s (%s)",
                    self.short_name, team_name, team_id,
                )
                continue
            # Hydrate per-player college affiliation via a batch /people call.
            player_ids = [p.get("person", {}).get("id") for p in roster]
            player_ids = [pid for pid in player_ids if pid]
            college_by_id: dict[int, str] = {}
            for chunk_start in range(0, len(player_ids), 40):
                chunk = player_ids[chunk_start:chunk_start + 40]
                ids_csv = ",".join(str(x) for x in chunk)
                try:
                    people = _http.get(
                        f"{self.BASE}/people?personIds={ids_csv}&hydrate=education",
                        timeout=15,
                    ).json().get("people", [])
                except Exception:
                    logger.exception("%s: people fetch failed", self.short_name)
                    continue
                for person in people:
                    colleges = person.get("education", {}).get("colleges", [])
                    if colleges:
                        college_by_id[person["id"]] = colleges[0].get("name", "")
            for p in roster:
                person = p.get("person", {})
                pid = person.get("id")
                full_name = person.get("fullName", "")
                if not full_name or not pid:
                    continue
                college = college_by_id.get(pid, "")
                entries.append(PlayerEntry(
                    name=_normalize_name(full_name),
                    college=_normalize_college(college),
                    summer_team=team_abbr or team_name,
                    league=self.short_name,
                    source_id=str(pid),
                    profile_url=(
                        f"https://www.mlb.com/player/{pid}"
                    ),
                    raw_name=full_name,
                    raw_college=college,
                ))
        logger.info(
            "%s: extracted %d players across %d teams",
            self.short_name, len(entries), len(teams),
        )
        return entries


class CapeCodLeague(MLBStatsAPILeague):
    name = "Cape Cod Baseball League"
    short_name = "Cape Cod"
    league_id = 565


class AppalachianLeague(MLBStatsAPILeague):
    """The Appalachian League converted to wood-bat collegiate summer in 2020.
    Now MLB-hosted on the Stats API (verified 2026-06-02 after Pointstreak
    sunset).
    """
    name = "Appalachian League"
    short_name = "Appalachian"
    league_id = 120


class MLBDraftLeague(MLBStatsAPILeague):
    name = "MLB Draft League"
    short_name = "MLB Draft"
    league_id = 5536


# -----------------------------------------------------------------------------
# Legacy Pointstreak leagues (soft-sunset; held for WCL/CPL/VBL/NYCBL fallback)
# -----------------------------------------------------------------------------

class CoastalPlainLeague(SummerLeague):
    """Coastal Plain League — has its own roster pages on coastalplain.com.

    Each team has a server-rendered roster table at
    /rosters/{team-slug}-roster/ with Name, DOB, Height/Weight, Bats/Throws,
    Year, and School columns. School affiliation makes high-confidence
    name+college matching possible.
    """
    name = "Coastal Plain League"
    short_name = "Coastal Plain"
    host_url = "https://coastalplain.com"

    def discover_rosters(self) -> list[PlayerEntry]:
        index_url = f"{self.host_url}/rosters/"
        try:
            html = _http.get(index_url, timeout=15).text
        except Exception as e:
            raise RuntimeError(f"Coastal Plain: index fetch failed: {e}")
        slugs = sorted(set(re.findall(r"/rosters/([a-z0-9-]+-roster)/?", html)))
        logger.info("Coastal Plain: %d team rosters discovered", len(slugs))
        entries: list[PlayerEntry] = []
        for slug in slugs:
            url = f"{self.host_url}/rosters/{slug}/"
            team_display = slug.replace("-roster", "").replace("-", " ").title()
            try:
                team_html = _http.get(url, timeout=15).text
            except Exception:
                logger.exception("Coastal Plain: %s fetch failed", slug)
                continue
            entries.extend(self._parse_cpl_roster(
                team_html, team_display=team_display, profile_url=url,
            ))
        logger.info("Coastal Plain: extracted %d players total", len(entries))
        return entries

    def _parse_cpl_roster(
        self, html: str, *, team_display: str, profile_url: str,
    ) -> list[PlayerEntry]:
        out: list[PlayerEntry] = []
        soup = BeautifulSoup(html, "html.parser")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 3:
                continue
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
            if "name" not in headers or "school" not in headers:
                continue
            name_idx = headers.index("name")
            school_idx = headers.index("school")
            for row in rows[1:]:
                cells = row.find_all(["th", "td"])
                if len(cells) <= max(name_idx, school_idx):
                    continue
                name = cells[name_idx].get_text(" ", strip=True)
                school = cells[school_idx].get_text(" ", strip=True)
                if not name or len(name) > 60:
                    continue
                out.append(PlayerEntry(
                    name=_normalize_name(name),
                    college=_normalize_college(school),
                    summer_team=team_display,
                    league=self.short_name,
                    profile_url=profile_url,
                    raw_name=name,
                    raw_college=school,
                ))
            break
        return out


# -----------------------------------------------------------------------------
# PrestoSports leagues (NECBL, Cal Ripken, PGCBL, FCBL, Prospect League)
# -----------------------------------------------------------------------------
#
# PrestoSports (Clubessential Holdings) is the de-facto winner among the
# collegiate summer leagues that moved off Pointstreak in 2025-2026. The
# public web pages live under each league's own domain proxied to Presto,
# so we never have to hit the *.prestosports.com subdomain directly (which
# is Cloudflare-gated).
#
# Caveat: rosters do NOT include college affiliation, only hometown. That
# means name+college matching falls through and we rely on either exact
# full-name match or fuzzy initial+last (auto-promoted when single
# candidate). Plan to backfill college via The Baseball Cube cross-ref.

class PrestoSportsLeague(SummerLeague):
    """Base class for leagues hosted on PrestoSports under their own domain.

    Subclasses set `host_url` (e.g. "https://necbl.com") and the year is
    auto-detected from today's date.
    """

    host_url: str = ""
    season_year: int = 0  # 0 = current year
    # Hard-coded team slugs to use when the league's teams index page is
    # JS-rendered (Cal Ripken is the case). Empty = auto-discover from index.
    fallback_team_slugs: list[str] = []
    # Some PrestoSports leagues use academic-year format ("2025-26") rather
    # than calendar year ("2026"). PGCBL + Prospect League use academic;
    # NECBL + Cal Ripken use calendar. Subclasses can override.
    use_academic_year: bool = False

    def _year(self) -> "int | str":
        y = self.season_year or date.today().year
        if self.use_academic_year:
            return f"{y-1}-{str(y)[-2:]}"
        return y

    def _fetch_page(self, url: str) -> str:
        """Direct first, residential proxy fallback for Cloudflare-gated hosts.

        4 of 5 PrestoSports league sites (calripken, fcbl, pgcbl, prospectleague)
        are behind Cloudflare and return 403 or 202-challenge to plain requests.
        Only NECBL's own domain serves directly.
        """
        try:
            resp = _http.get(url, timeout=20)
            if resp.status_code == 200 and len(resp.text) > 5000:
                return resp.text
        except Exception:
            pass
        html, diag = fetch_via_residential_proxy(url, timeout=25)
        if html:
            logger.info("%s: fetched %d bytes via %s",
                        self.short_name, len(html), diag.get("active"))
            return html
        return ""

    def discover_rosters(self) -> list[PlayerEntry]:
        if not self.host_url:
            raise RuntimeError(f"{self.short_name}: host_url not set")
        year = self._year()
        teams_index_url = f"{self.host_url}/sports/bsb/{year}/teams"
        html = self._fetch_page(teams_index_url)
        slugs: set[str] = set()
        if html:
            for m in re.finditer(
                rf"/sports/bsb/{year}/teams/([a-z0-9-]+)/?[?\"' ]", html,
            ):
                slug = m.group(1)
                if "allstars" in slug or "all-stars" in slug:
                    continue
                slugs.add(slug)
        if not slugs:
            if self.fallback_team_slugs:
                slugs = set(self.fallback_team_slugs)
                logger.info("%s: teams index unavailable / JS-rendered; using %d fallback slugs",
                            self.short_name, len(slugs))
            else:
                raise RuntimeError(
                    f"{self.short_name}: teams index returned empty and no "
                    f"fallback_team_slugs configured"
                )
        else:
            logger.info("%s: %d team slugs discovered for %d", self.short_name, len(slugs), year)
        entries: list[PlayerEntry] = []
        for slug in sorted(slugs):
            url = f"{self.host_url}/sports/bsb/{year}/teams/{slug}?view=roster"
            html = self._fetch_page(url)
            if not html:
                logger.info("%s/%s: roster fetch returned empty", self.short_name, slug)
                continue
            count_before = len(entries)
            entries.extend(self._parse_roster_table(html, team_slug=slug, profile_url=url))
            logger.info("%s/%s: extracted %d players",
                        self.short_name, slug, len(entries) - count_before)
        return entries

    @staticmethod
    def _parse_roster_table(
        html: str, *, team_slug: str, profile_url: str,
    ) -> list[PlayerEntry]:
        out: list[PlayerEntry] = []
        soup = BeautifulSoup(html, "html.parser")
        # PrestoSports roster table headers: "# | Name | Position | Year |
        # Status | Height | Weight | Bats | Throws | DOB | Hometown"
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 3:
                continue
            header_cells = rows[0].find_all(["th", "td"])
            headers_lower = [c.get_text(strip=True).lower() for c in header_cells]
            if "name" not in headers_lower or "position" not in headers_lower:
                continue
            name_idx = headers_lower.index("name")
            for row in rows[1:]:
                cells = row.find_all(["th", "td"])
                if len(cells) <= name_idx:
                    continue
                name_cell = cells[name_idx]
                name = name_cell.get_text(" ", strip=True)
                if not name or len(name) > 60:
                    continue
                # Capture Presto's per-player slug (used by summer_pulse.py to
                # fetch game logs). Path looks like /sports/bsb/{year}/players/{slug}.
                player_slug = ""
                a = name_cell.find("a", href=True)
                if a:
                    m = re.search(r"/sports/bsb/\d+/players/([^/?\"' ]+)", a["href"])
                    if m:
                        player_slug = m.group(1)
                summer_team = team_slug.replace("-", " ").title()
                out.append(PlayerEntry(
                    name=_normalize_name(name),
                    college="",  # Presto rosters don't expose college
                    summer_team=summer_team,
                    league="",  # set by subclass via league field on entry below
                    source_id=player_slug,
                    profile_url=profile_url,
                    raw_name=name,
                    raw_college="",
                ))
            # Stop at first roster-shaped table.
            break
        return out

    def discover_rosters_with_league(self) -> list[PlayerEntry]:
        entries = self.discover_rosters()
        for e in entries:
            e.league = self.short_name
        return entries


class NECBL(PrestoSportsLeague):
    name = "New England Collegiate Baseball League"
    short_name = "NECBL"
    host_url = "https://necbl.com"

    def discover_rosters(self) -> list[PlayerEntry]:
        entries = super().discover_rosters()
        for e in entries:
            e.league = self.short_name
        return entries


class CalRipkenLeague(PrestoSportsLeague):
    """Cal Ripken Sr. Collegiate Baseball League — PrestoSports.

    The league's own domain is calripkensrleague.org (with "sr" for "Sr.").
    Teams index is JS-rendered, so we hard-code the 8 slugs (discovered
    once via the marketing /teams/{nick} redirects on the same domain).
    """
    name = "Cal Ripken Sr. Collegiate Baseball League"
    short_name = "Cal Ripken"
    host_url = "https://calripkensrleague.org"
    fallback_team_slugs = [
        "alexandriaaces", "bethesdabigtrain", "metrosouthcountybraves",
        "olneycropdusters", "gaithersburggiants", "dcgrays",
        "southernmarylandsenators", "sstthunderbolts",
    ]

    def discover_rosters(self) -> list[PlayerEntry]:
        entries = super().discover_rosters()
        for e in entries:
            e.league = self.short_name
        return entries


class PGCBL(PrestoSportsLeague):
    """Perfect Game Collegiate Baseball League — PrestoSports since 2025.
    Uses academic-year format (2025-26) for URLs.
    """
    name = "Perfect Game Collegiate Baseball League"
    short_name = "PGCBL"
    host_url = "https://pgcbl.com"
    use_academic_year = True

    def discover_rosters(self) -> list[PlayerEntry]:
        entries = super().discover_rosters()
        for e in entries:
            e.league = self.short_name
        return entries


class FCBL(PrestoSportsLeague):
    """Futures Collegiate Baseball League — PrestoSports.

    Correct league domain is thefuturesleague.com (NOT thefcbl.com or
    fcbl.prestosports.com — both fail). Uses calendar-year URL format.
    """
    name = "Futures Collegiate Baseball League"
    short_name = "FCBL"
    host_url = "https://thefuturesleague.com"

    def discover_rosters(self) -> list[PlayerEntry]:
        entries = super().discover_rosters()
        for e in entries:
            e.league = self.short_name
        return entries


class ProspectLeagueBaseball(PrestoSportsLeague):
    """Prospect League — longstanding PrestoSports customer.
    Uses academic-year format (2025-26).
    """
    name = "Prospect League"
    short_name = "Prospect"
    host_url = "https://prospectleague.com"
    use_academic_year = True

    def discover_rosters(self) -> list[PlayerEntry]:
        entries = super().discover_rosters()
        for e in entries:
            e.league = self.short_name
        return entries


class AlaskaBaseballLeague(_StubLeague):
    name = "Alaska Baseball League"
    short_name = "Alaska"


class FloridaCollegiateLeague(_StubLeague):
    name = "Florida Collegiate Summer League"
    short_name = "Florida"


LEAGUES: list[SummerLeague] = [
    # MLB Stats API (verified live 2026-06-02)
    CapeCodLeague(),
    AppalachianLeague(),
    MLBDraftLeague(),
    # PrestoSports (NECBL + 4 others, verified live 2026-06-02)
    NECBL(),
    CalRipkenLeague(),
    PGCBL(),
    FCBL(),
    ProspectLeagueBaseball(),
    # In-house / pending adapters
    NorthwoodsLeague(),
    # Pointstreak holdouts (soft-sunset; may go dark mid-season)
    CoastalPlainLeague(),
    # Unimplemented (low priority for SV's client base)
    AlaskaBaseballLeague(),
    FloridaCollegiateLeague(),
]


# =============================================================================
# The Baseball Cube — cross-league fallback aggregator
# =============================================================================
#
# Baseball Cube aggregates summer-ball player data across many leagues with
# college affiliation included on the player profile. Used as a fallback
# lookup AFTER per-league discovery — for each NCAA client that didn't
# match in any league's roster, we ask the Cube directly. This unlocks
# leagues whose own sites we haven't implemented (or whose WAF blocks us).
#
# Data is next-day at best — perfect for the Monday recap (which uses the
# 4 AM ET snapshot), not used for live in-game stats.

class BaseballCubeLookup:
    """Search Baseball Cube for an NCAA player and infer their summer team."""

    BASE = "https://www.thebaseballcube.com"
    name = "The Baseball Cube"
    short_name = "BaseballCube"

    def find_player(self, full_name: str, college: str) -> Optional[PlayerEntry]:
        """Return a PlayerEntry if the Cube knows this NCAA player and they
        have an active summer team, else None.

        URL patterns are best-effort — Cube has changed its routing more
        than once. We rely on residential proxy + curl_cffi to clear
        Cloudflare's challenge, then look for summer-league mentions in
        the player profile.
        """
        from urllib.parse import quote_plus
        q = quote_plus(full_name)
        search_url = f"{self.BASE}/content/search/?search={q}"
        html, diag = fetch_via_residential_proxy(search_url, timeout=25)
        if not html:
            logger.info("BaseballCube: search blocked for %s (%s)",
                        full_name, diag.get("error"))
            return None

        soup = BeautifulSoup(html, "html.parser")
        # Find search-result links to player profiles. Cube uses /content/player/
        # for the modern path; older links go to /players/profile.asp.
        candidate_urls: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/content/player/" in href or "profile.asp" in href:
                if href.startswith("/"):
                    href = self.BASE + href
                candidate_urls.append(href)
            if len(candidate_urls) >= 8:  # cap candidates per search
                break

        target_college = _normalize_college(college)
        for url in candidate_urls:
            phtml, _ = fetch_via_residential_proxy(url, timeout=25)
            if not phtml:
                continue
            psoup = BeautifulSoup(phtml, "html.parser")
            body_text = psoup.get_text(" ", strip=True)
            # Confirm we're on the right player by college affiliation.
            if target_college and target_college not in _normalize_college(body_text):
                continue
            # Look for the summer league section. Cube tags it variously —
            # "Summer League", "Cape Cod", "Northwoods" etc. appear as
            # section labels next to a team name.
            summer_team, summer_league = _extract_baseballcube_summer_assignment(body_text)
            if not summer_team:
                continue
            return PlayerEntry(
                name=_normalize_name(full_name),
                college=_normalize_college(college),
                summer_team=summer_team,
                league=summer_league or "Summer (via Baseball Cube)",
                source_id=url,
                profile_url=url,
                raw_name=full_name,
                raw_college=college,
            )
        return None


_SUMMER_LEAGUE_PHRASES = [
    ("Cape Cod Baseball League", "Cape Cod"),
    ("Cape Cod League", "Cape Cod"),
    ("Northwoods League", "Northwoods"),
    ("Coastal Plain League", "Coastal Plain"),
    ("New England Collegiate Baseball League", "NECBL"),
    ("Appalachian League", "Appalachian"),
    ("Alaska Baseball League", "Alaska"),
    ("Florida Collegiate Summer League", "Florida Collegiate"),
    ("Prospect League", "Prospect"),
    ("Valley Baseball League", "Valley"),
    ("California Collegiate League", "California Collegiate"),
    ("Great Lakes Summer Collegiate", "Great Lakes"),
]


def _extract_baseballcube_summer_assignment(body_text: str) -> tuple[str, str]:
    """Look for a known summer-league phrase in the profile body text,
    then grab the team name that appears nearby. Best-effort — Cube's
    layout changes; we tighten this once we see real failures."""
    for phrase, short in _SUMMER_LEAGUE_PHRASES:
        idx = body_text.find(phrase)
        if idx < 0:
            continue
        # Look 200 chars on either side for a team-ish token.
        window = body_text[max(0, idx - 200):idx + 200]
        m = re.search(r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\s+\(.*?\)?\s*" + re.escape(phrase),
                      window)
        if m:
            return m.group(1).strip(), short
        # Fallback: take the token right after the league phrase.
        after = body_text[idx + len(phrase):idx + len(phrase) + 80]
        m2 = re.match(r"\s*[—\-:|]?\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})", after)
        if m2:
            return m2.group(1).strip(), short
    return "", ""


# =============================================================================
# Aggregator + transparency
# =============================================================================

class SummerBallAggregator:
    """Coordinates per-league discovery + writes the daily roster snapshot."""

    def __init__(self, leagues: Optional[list[SummerLeague]] = None,
                 cube: Optional[BaseballCubeLookup] = None):
        self.leagues = leagues if leagues is not None else LEAGUES
        self.cube = cube if cube is not None else BaseballCubeLookup()

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

        # Two indexes for matching:
        #   by_name        — exact (normalized) full-name key
        #   by_initial_last — fuzzy "F. Last" key, catches Pointstreak's
        #                     abbreviated names ("Robbins, A") and most
        #                     informal variants ("JJ Smith" -> "j smith").
        by_name: dict[str, list[PlayerEntry]] = {}
        by_initial_last: dict[str, list[PlayerEntry]] = {}
        for p in players:
            by_name.setdefault(p.name, []).append(p)
            key = _initial_last_key(p.raw_name or p.name)
            if key:
                by_initial_last.setdefault(key, []).append(p)

        matched: list[dict] = []
        possible_matches: list[dict] = []
        unmatched: list[dict] = []
        ambiguous: list[dict] = []

        for c in ncaa_clients:
            ncaa_full_name = c.get("player_name", "")
            ncaa_name = _normalize_name(ncaa_full_name)
            ncaa_college = _normalize_college(c.get("team", ""))

            # 1) Exact full-name match
            exact_candidates = by_name.get(ncaa_name, [])
            if exact_candidates:
                college_match = [p for p in exact_candidates if p.college == ncaa_college]
                if len(college_match) == 1:
                    p = college_match[0]
                    matched.append({
                        "player_name": ncaa_full_name, "college": c.get("team"),
                        "summer_team": p.summer_team, "league": p.league,
                        "summer_name": p.raw_name or p.name,
                        "source_id": p.source_id,
                        "match_strength": "name+college", "profile_url": p.profile_url,
                    })
                    continue
                if len(college_match) > 1:
                    ambiguous.append({
                        "player_name": ncaa_full_name, "college": c.get("team"),
                        "candidates": [
                            {"summer_team": x.summer_team, "league": x.league}
                            for x in college_match
                        ],
                    })
                    continue
                if len(exact_candidates) == 1:
                    p = exact_candidates[0]
                    # If summer roster has a college and it differs from NCAA's,
                    # this is almost certainly a name collision (two different
                    # players with the same name). 2026-06-03 incident: Brooks
                    # Wright at SE Louisiana false-matched to Brooks Wright at
                    # Appalachian State on Boone Bigfoots (CPL). Route to
                    # ambiguous so it appears in the review bucket instead of
                    # silently confirming the wrong player.
                    if p.college and ncaa_college and p.college != ncaa_college:
                        ambiguous.append({
                            "player_name": ncaa_full_name, "college": c.get("team"),
                            "candidates": [{
                                "summer_team": p.summer_team, "league": p.league,
                                "summer_name": p.raw_name or p.name,
                                "summer_college": p.raw_college or p.college,
                            }],
                            "conflict_reason": "college mismatch",
                        })
                        continue
                    matched.append({
                        "player_name": ncaa_full_name, "college": c.get("team"),
                        "summer_team": p.summer_team, "league": p.league,
                        "summer_name": p.raw_name or p.name,
                        "source_id": p.source_id,
                        "match_strength": "name-only", "profile_url": p.profile_url,
                    })
                    continue
                # multiple exact-name candidates, no college info — fall through
                # to ambiguous bucket below.
                ambiguous.append({
                    "player_name": ncaa_full_name, "college": c.get("team"),
                    "candidates": [
                        {"summer_team": x.summer_team, "league": x.league}
                        for x in exact_candidates
                    ],
                })
                continue

            # 2) Fuzzy initial+last match — Pointstreak's abbreviated names.
            # Pointstreak publishes names as "Last, F" (no full first), so a
            # full-name index never hits. When initial+last yields exactly one
            # candidate across all summer leagues, treat it as matched — the
            # 39-client roster vs. 135-Northwoods-player pool makes collisions
            # rare. Multiple candidates still route to possible_matches for
            # manual review.
            fuzzy_candidates = by_initial_last.get(_initial_last_key(ncaa_full_name), [])
            if len(fuzzy_candidates) == 1:
                p = fuzzy_candidates[0]
                matched.append({
                    "player_name": ncaa_full_name, "college": c.get("team"),
                    "summer_team": p.summer_team, "league": p.league,
                    "summer_name": p.raw_name or p.name,
                    "source_id": p.source_id,
                    "match_strength": "initial+last", "profile_url": p.profile_url,
                })
                continue
            if fuzzy_candidates:
                possible_matches.append({
                    "player_name": ncaa_full_name, "college": c.get("team"),
                    "match_strength": "initial+last (manual review)",
                    "candidates": [
                        {
                            "summer_team": x.summer_team, "league": x.league,
                            "summer_name": x.raw_name or x.name,
                            "summer_college": x.raw_college or x.college,
                            "profile_url": x.profile_url,
                        }
                        for x in fuzzy_candidates
                    ],
                })
                continue

            unmatched.append({"player_name": ncaa_full_name, "college": c.get("team")})

        # Second-pass lookup via The Baseball Cube for any still-unmatched
        # clients. Cube includes college affiliation on player profiles, so
        # matches found here are high-confidence (name+college, like our
        # primary path).
        cube_status: dict = {"attempted": 0, "matched": 0, "blocked": 0, "errors": 0}
        if self.cube and unmatched:
            still_unmatched: list[dict] = []
            for u in unmatched:
                cube_status["attempted"] += 1
                try:
                    entry = self.cube.find_player(u["player_name"], u["college"])
                except Exception as e:
                    logger.exception("BaseballCube lookup failed for %s", u["player_name"])
                    cube_status["errors"] += 1
                    still_unmatched.append(u)
                    continue
                if entry is None:
                    still_unmatched.append(u)
                    continue
                cube_status["matched"] += 1
                matched.append({
                    "player_name": u["player_name"],
                    "college": u["college"],
                    "summer_team": entry.summer_team,
                    "league": entry.league,
                    "match_strength": "via Baseball Cube (next-day)",
                    "profile_url": entry.profile_url,
                })
            unmatched = still_unmatched

        # Cross-validate auto-match results against Kent's manual placements
        # spreadsheet. Catches stale-roster issues like Henry Zatkowski (auto-
        # match finds him on Hyannis CCBL from carryover data; placement says
        # Bourne CCBL + Shut Down). The placement file is source of truth;
        # this surfaces disagreements so Kent can either update the sheet or
        # we know the auto-match is stale.
        validation = _validate_against_placements(matched, possible_matches)

        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "league_health": [h.to_dict() for h in health],
            "baseballcube": cube_status,
            "ncaa_clients_total": len(ncaa_clients),
            "ncaa_clients_matched": len(matched),
            "ncaa_clients_possible": len(possible_matches),
            "ncaa_clients_unmatched": len(unmatched),
            "ncaa_clients_ambiguous": len(ambiguous),
            "matched": matched,
            "possible_matches": possible_matches,
            "unmatched": unmatched,
            "ambiguous": ambiguous,
            "placement_validation": validation,
            "all_players_count": len(players),
        }

        SUMMER_ROSTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        SUMMER_ROSTER_PATH.write_text(json.dumps(snapshot, indent=2))
        return snapshot


_PLACEMENTS_PATH = Path(__file__).resolve().parent.parent / "data" / "summer_ball_placements.json"


def _validate_against_placements(matched: list[dict], possible: list[dict]) -> dict:
    """Compare auto-match results to Kent's manual placement spreadsheet.

    Returns:
      {
        "agrees": [{player_name, school, placement_team, auto_team, league}, ...],
        "conflicts": [...same shape but teams differ; "Stale roster" flag],
        "unmatched": [{player_name, school, placement_team, league, status}, ...]  # in sheet, not auto-matched
      }
    """
    result = {"agrees": [], "conflicts": [], "unmatched": []}
    if not _PLACEMENTS_PATH.exists():
        return result
    try:
        placement_data = json.loads(_PLACEMENTS_PATH.read_text())
    except Exception:
        return result
    placements = placement_data.get("placements") or []
    # Filter "NEED PLACEMENT" placeholder rows
    placements = [
        p for p in placements
        if p.get("player_name")
        and not (str(p["player_name"]).isupper() and len(p["player_name"]) > 5)
    ]
    auto_by_name = {(m.get("player_name") or "").lower(): m for m in matched}

    for p in placements:
        name = (p.get("player_name") or "").strip()
        if not name:
            continue
        status = p.get("status", "")
        # Skip status-only entries (Shut Down, Injured); no team to validate.
        if status in ("Shut Down", "Injured") or not p.get("summer_team"):
            continue
        placement_team = p["summer_team"].lower()
        placement_league = (p.get("league") or "").lower()
        auto = auto_by_name.get(name.lower())
        if not auto:
            result["unmatched"].append({
                "player_name": name,
                "school": p.get("school", ""),
                "placement_team": p.get("summer_team"),
                "league": p.get("league"),
                "status": status,
            })
            continue
        auto_team = (auto.get("summer_team") or "").lower()
        auto_league = (auto.get("league") or "").lower()
        # Looser team-name comparison: tokenize both sides and check overlap.
        # Handles "Lexington Blowfish" (sheet) vs "Lexington County Blowfish"
        # (CPL site) — same team, different word ordering. Also covers
        # abbreviation match ("HYA" inside "Hyannis Harbor Hawks") via the
        # substring check.
        def _team_tokens(t: str) -> set:
            # Drop common modifier words that vary between sources.
            stop = {"the", "of", "county", "city", "town", "ny", "ma", "nh"}
            return {w for w in t.replace("-", " ").split() if w and w not in stop}

        p_tokens = _team_tokens(placement_team)
        a_tokens = _team_tokens(auto_team)
        token_overlap = len(p_tokens & a_tokens) >= min(2, min(len(p_tokens), len(a_tokens)))
        if (
            auto_team == placement_team
            or auto_team in placement_team
            or placement_team.replace(" ", "").startswith(auto_team.replace(" ", ""))
            or token_overlap
        ):
            result["agrees"].append({
                "player_name": name,
                "school": p.get("school", ""),
                "league": p.get("league"),
                "team": p.get("summer_team"),
            })
        else:
            result["conflicts"].append({
                "player_name": name,
                "school": p.get("school", ""),
                "placement_team": p.get("summer_team"),
                "placement_league": p.get("league"),
                "auto_team": auto.get("summer_team"),
                "auto_league": auto.get("league"),
                "status": status,
                "reason": "auto-match team differs from spreadsheet — likely stale roster",
            })
    return result


def load_snapshot() -> Optional[dict]:
    if not SUMMER_ROSTER_PATH.exists():
        return None
    return json.loads(SUMMER_ROSTER_PATH.read_text())
