"""PGCBL league-index parse: Presto renders player names inside the anchor
as "A\\r\\n<deep indent>DeCesare" — interior whitespace must be collapsed or
the 60-char name guard drops every player (production returned 0 players
from a 6.9MB page for three weeks). Teams link as `teams?id={teamId}`, not
/teams/{slug}, and coverage requires a union of leaderboard views."""
import src.summer_ball as sb
from src.summer_ball import PGCBL


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


def test_league_index_unions_views_and_parses_names(monkeypatch):
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


def test_league_index_tolerates_failed_views(monkeypatch):
    def fake_fetch(self, url):
        return HITTERS if "pos=h" in url and "r=0" in url else ""

    monkeypatch.setattr(PGCBL, "_fetch_page", fake_fetch)
    entries = PGCBL()._discover_via_league_index()
    assert {e.source_id for e in entries} == {"anthonydecesared74j", "zackjohnsonab12"}


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
