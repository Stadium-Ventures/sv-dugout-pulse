"""PGCBL league-index parse: Presto renders player names inside the anchor
as "A\\r\\n<deep indent>DeCesare" — interior whitespace must be collapsed or
the 60-char name guard drops every player (production returned 0 players
from a 6.9MB page for three weeks). Teams link as `teams?id={teamId}`, not
/teams/{slug}, and coverage requires a union of leaderboard views.

2026-09-24: losing most of those views to a proxy/WAF block (no exception,
just short/empty HTML) silently caps the union at whichever single view got
through — 373 -> 125 players, 125 being exactly one view's own display cap.
Each view's parsed players are now cached so a failed view restores from the
last good fetch instead of just dropping that slice of the league."""
from datetime import datetime, timedelta, timezone

import src.summer_ball as sb
from src.summer_ball import PGCBL, PlayerEntry


def _patch_cache(monkeypatch, tmp_path, initial=None):
    path = tmp_path / "_presto_roster_cache.json"
    if initial is not None:
        import json
        path.write_text(json.dumps(initial))
    monkeypatch.setattr(sb, "PRESTO_CACHE_PATH", path)
    return path


def _page(*players):
    rows = "".join(f"""
<tr>
  <td>1</td>
  <td class="text-nowrap text-left">
<a href="/sports/bsb/2025-26/players/{slug}">\r
                                                                            {initial}\r
                                                                        {last}\r
                                </a>
  </td>
  <td class="text-nowrap text-center"><a href="teams?id={team_id}">{team}</a></td>
  <td>32</td>
</tr>""" for slug, initial, last, team_id, team in players)
    return ("<html><body>" + "x" * 50001
            + "<table><tr><th>Rk</th><th>Name</th><th>Team</th><th>gp</th></tr>"
            + rows + "</table></body></html>")


HITTERS = _page(
    ("anthonydecesared74j", "A", "DeCesare", "96npryi9vztnt1yi", "Amsterdam Mohawks"),
    ("zackjohnsonab12", "Z", "Johnson", "96npryi9vztnt1yi", "Amsterdam Mohawks"),
)
PITCHERS = _page(
    ("dominickzangardi73yy", "D", "Zangardi", "iydzaxcsf4y91vgi", "Niagara Falls Americans"),
    ("anthonydecesared74j", "A", "DeCesare", "96npryi9vztnt1yi", "Amsterdam Mohawks"),  # dupe
)


def test_league_index_unions_views_and_parses_names(monkeypatch, tmp_path):
    _patch_cache(monkeypatch, tmp_path)
    fetched = []

    def fake_fetch(self, url):
        fetched.append(url)
        return HITTERS if "pos=h" in url else PITCHERS

    monkeypatch.setattr(PGCBL, "_fetch_page", fake_fetch)
    entries = PGCBL()._discover_via_league_index()

    assert len(fetched) == len(PGCBL._INDEX_VIEWS)
    # DeCesare deduped across views; three unique players total
    assert len(entries) == 3
    by_slug = {e.source_id: e for e in entries}
    dec = by_slug["anthonydecesared74j"]
    assert dec.raw_name == "A DeCesare"
    assert dec.summer_team == "Amsterdam Mohawks"
    assert dec.league == "PGCBL"
    assert by_slug["dominickzangardi73yy"].summer_team == "Niagara Falls Americans"
    # initial+last fuzzy key must line up with the NCAA-side key
    assert sb._initial_last_key(dec.raw_name) == sb._initial_last_key("Anthony DeCesare")
    assert sb._initial_last_key(by_slug["zackjohnsonab12"].raw_name) == \
        sb._initial_last_key("Zack Johnson")


def test_league_index_tolerates_failed_views(monkeypatch, tmp_path):
    _patch_cache(monkeypatch, tmp_path)

    def fake_fetch(self, url):
        return HITTERS if "pos=h" in url and "r=0" in url else ""

    monkeypatch.setattr(PGCBL, "_fetch_page", fake_fetch)
    entries = PGCBL()._discover_via_league_index()
    assert {e.source_id for e in entries} == {"anthonydecesared74j", "zackjohnsonab12"}


def test_league_index_restores_failed_view_from_cache(monkeypatch, tmp_path):
    """4 of 5 views blocked (empty HTML, no exception) must not silently
    shrink the union — a fresh per-view cache entry fills the gap."""
    cached_at = datetime.now(timezone.utc).isoformat()
    _patch_cache(monkeypatch, tmp_path, initial={
        "PGCBL/index/?sort=ip&view=&pos=p&r=0": {
            "cached_at": cached_at,
            "players": [PlayerEntry(
                name="dominickzangardi73yy", college="",
                summer_team="Niagara Falls Americans", league="PGCBL",
                source_id="dominickzangardi73yy", raw_name="D Zangardi",
            ).to_dict()],
        },
    })

    def fake_fetch(self, url):
        # Only the default view fetch succeeds live; every other view is
        # blocked (empty body, same shape as a WAF/proxy failure).
        return HITTERS if url.endswith("/players") else ""

    monkeypatch.setattr(PGCBL, "_fetch_page", fake_fetch)
    entries = PGCBL()._discover_via_league_index()
    ids = {e.source_id for e in entries}
    assert "anthonydecesared74j" in ids  # live default view
    assert "zackjohnsonab12" in ids      # live default view
    assert "dominickzangardi73yy" in ids  # restored from cache


def test_league_index_stale_cache_not_restored(monkeypatch, tmp_path):
    stale = (datetime.now(timezone.utc)
             - timedelta(days=sb.PRESTO_CACHE_MAX_AGE_DAYS + 1)).isoformat()
    _patch_cache(monkeypatch, tmp_path, initial={
        "PGCBL/index/?sort=ip&view=&pos=p&r=0": {
            "cached_at": stale,
            "players": [PlayerEntry(
                name="dominickzangardi73yy", college="",
                summer_team="Niagara Falls Americans", league="PGCBL",
                source_id="dominickzangardi73yy", raw_name="D Zangardi",
            ).to_dict()],
        },
    })

    def fake_fetch(self, url):
        return HITTERS if url.endswith("/players") else ""

    monkeypatch.setattr(PGCBL, "_fetch_page", fake_fetch)
    entries = PGCBL()._discover_via_league_index()
    ids = {e.source_id for e in entries}
    assert "dominickzangardi73yy" not in ids  # stale cache must drain, not mask


def test_league_index_updates_cache_on_success(monkeypatch, tmp_path):
    path = _patch_cache(monkeypatch, tmp_path)

    def fake_fetch(self, url):
        return HITTERS if "pos=h" in url else PITCHERS

    monkeypatch.setattr(PGCBL, "_fetch_page", fake_fetch)
    PGCBL()._discover_via_league_index()
    import json
    cache = json.loads(path.read_text())
    assert len(cache) == len(PGCBL._INDEX_VIEWS)
    assert all(k.startswith("PGCBL/index/") for k in cache)


def test_slug_veto_on_initial_last_collisions():
    # Real collision from 2026-07-13: 'Lee Ellis' vs loganellisbdyn
    assert sb._slug_contradicts_first_name("loganellisbdyn", "Lee Ellis")
    # True matches and nickname/formal pairs must NOT be vetoed
    assert not sb._slug_contradicts_first_name("evantaylorvx6g", "Evan Taylor")
    assert not sb._slug_contradicts_first_name("zacharyjohnson12ab", "Zack Johnson")
    assert not sb._slug_contradicts_first_name("michaelsmithq1w2", "Mike Smith")
    # Non-name-bearing ids (MLB numeric etc.) carry no evidence
    assert not sb._slug_contradicts_first_name("842079", "Taylor Kirk")
    assert not sb._slug_contradicts_first_name("", "Evan Taylor")
