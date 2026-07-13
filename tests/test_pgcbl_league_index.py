"""PGCBL league-index parse: Presto renders player names inside the anchor
as "A\\r\\n<deep indent>DeCesare" — interior whitespace must be collapsed or
the 60-char name guard drops every player (production returned 0 players
from a 6.9MB page for three weeks)."""
import src.summer_ball as sb
from src.summer_ball import PGCBL

INDEX_HTML = ("<html><body>" + "x" * 50001 + """
<table>
<tr><th>Rk</th><th>Name</th><th>Team</th><th>gp</th></tr>
<tr>
  <td>1</td>
  <td class="text-nowrap text-left">
<a href="/sports/bsb/2025-26/players/anthonydecesared74j">\r
                                                                            A\r
                                                                        DeCesare\r
                                </a>
  </td>
  <td><a href="/sports/bsb/2025-26/teams/amsterdammohawks">Amsterdam Mohawks</a></td>
  <td>32</td>
</tr>
<tr>
  <td>2</td>
  <td class="text-nowrap text-left">
<a href="/sports/bsb/2025-26/players/zackjohnsonab12">\r
                                                                            Z\r
                                                                        Johnson\r
                                </a>
  </td>
  <td><a href="/sports/bsb/2025-26/teams/amsterdammohawks">Amsterdam Mohawks</a></td>
  <td>30</td>
</tr>
</table>
<a href="/sports/bsb/2025-26/players?sort=avg&view=&pos=h&r=0">avg</a>
</body></html>""")


def test_league_index_names_parse_despite_interior_whitespace(monkeypatch):
    monkeypatch.setattr(PGCBL, "_fetch_page", lambda self, url: INDEX_HTML)
    lg = PGCBL()
    entries = lg._discover_via_league_index()
    assert len(entries) == 2
    by_slug = {e.source_id: e for e in entries}
    dec = by_slug["anthonydecesared74j"]
    assert dec.raw_name == "A DeCesare"
    assert dec.summer_team == "Amsterdammohawks"
    assert dec.league == "PGCBL"
    # initial+last fuzzy key must line up with the NCAA-side key
    assert sb._initial_last_key(dec.raw_name) == sb._initial_last_key("Anthony DeCesare")
    assert sb._initial_last_key(by_slug["zackjohnsonab12"].raw_name) == \
        sb._initial_last_key("Zack Johnson")
