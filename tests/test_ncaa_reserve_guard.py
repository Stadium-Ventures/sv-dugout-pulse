"""Regression guard: stats.ncaa.org re-serves the most recent date WITH games
(HTTP 200) when the requested game_date has none. Left unchecked it re-emitted
a season-over player's last final re-dated to the run day every late-night run
(the 06-24 CWS final resurfacing as a run-day "Final" all July).

Two layers under test:
  Layer 1 — NCAAOrgScraper validates the served date and returns no games on a
            mismatch; when the served-date marker is missing it flags the games
            as unverified instead of trusting them.
  Layer 2 — NCAAStatsFetcher._waterfall_fetch corroborates an unverifiable
            NCAA.org Final against ESPN's genuinely date-scoped scoreboard and
            drops it when ESPN lists zero events for that date.
"""
import os
from datetime import date

# stats_engine imports src.config, which reads ROSTER_URL at import time.
# Give it a harmless default so the import never depends on repo secrets.
os.environ.setdefault("ROSTER_URL", "http://example.invalid/roster.csv")

from src import stats_engine  # noqa: E402


# A parseable contest card in stats.ncaa.org's scoreboard shape: two team rows
# sharing the contest id, a logo alt for the name, a score div, a winner class,
# and a box-score link.
def _contest_html(away, away_score, home, home_score, winner="away"):
    return f"""
    <table>
      <tr id="contest_777">
        <td><img class="logo_image" alt="{away}"></td>
        <td><div id="score_1" class="totalcol{' winner_background' if winner == 'away' else ''}">{away_score}</div></td>
      </tr>
      <tr id="contest_777">
        <td><img class="logo_image" alt="{home}"></td>
        <td><div id="score_2" class="totalcol{' winner_background' if winner == 'home' else ''}">{home_score}</div></td>
      </tr>
      <tr><td><a href="/contests/777/box_score">Box Score</a></td></tr>
    </table>
    """


_DATE_INPUT = '<input type="text" id="game_date" class="datepicker" value="{mmddyyyy}">'

# Re-served page: we asked for an empty July date, the site handed back the
# 06/24 CWS final. The final is fully parseable — the point is we must NOT parse
# it, because the served-date marker proves it is not the date we requested.
RESERVED_HTML = (
    _DATE_INPUT.format(mmddyyyy="06/24/2026")
    + _contest_html("Oklahoma", 13, "North Carolina", 2, winner="away")
)

# Matching page: the served date equals the requested date.
MATCHING_HTML = (
    _DATE_INPUT.format(mmddyyyy="04/15/2026")
    + _contest_html("Clemson", 7, "Duke", 3, winner="away")
)

# Same games, but the served-date marker is gone (template change): unverifiable.
NO_MARKER_HTML = _contest_html("Clemson", 7, "Duke", 3, winner="away")


# ----- Layer 1: served-date extraction -----

def test_served_date_parses_every_marker_form():
    sd = stats_engine.NCAAOrgScraper._served_date
    # date-filter input, value after id
    assert sd('<input id="game_date" value="06/24/2026">') == date(2026, 6, 24)
    # date-filter input, value before id
    assert sd('<input value="03/01/2026" class="x" id="game_date">') == date(2026, 3, 1)
    # url-encoded query link
    assert sd('<a href="/contests/livestream_scoreboards?game_date=06%2F24%2F2026&x=1">') == date(2026, 6, 24)
    # plain query link
    assert sd('<a href="/x?game_date=06/24/2026">') == date(2026, 6, 24)


def test_served_date_none_when_marker_missing():
    assert stats_engine.NCAAOrgScraper._served_date("<html>no date anywhere</html>") is None
    assert stats_engine.NCAAOrgScraper._served_date(NO_MARKER_HTML) is None


# ----- Layer 1: _get_scoreboard behavior -----

def test_get_scoreboard_returns_empty_on_reserved_date(monkeypatch):
    """Asked for an empty date, site re-serves another day's final -> no games."""
    monkeypatch.setattr(stats_engine, "_ncaa_org_get", lambda path: RESERVED_HTML)
    scraper = stats_engine.NCAAOrgScraper()
    assert scraper._get_scoreboard(date(2026, 7, 6)) == []


def test_get_scoreboard_parses_matching_date(monkeypatch):
    """Served date equals requested date -> games parse, marked verified."""
    monkeypatch.setattr(stats_engine, "_ncaa_org_get", lambda path: MATCHING_HTML)
    scraper = stats_engine.NCAAOrgScraper()
    games = scraper._get_scoreboard(date(2026, 4, 15))
    assert len(games) == 1
    assert games[0]["state"] == "final"
    assert games[0]["away_name"] == "Clemson"
    assert games[0].get("_served_date_unverified", False) is False


def test_get_scoreboard_flags_unverified_when_marker_missing(monkeypatch):
    """No served-date marker -> games returned but flagged for corroboration."""
    monkeypatch.setattr(stats_engine, "_ncaa_org_get", lambda path: NO_MARKER_HTML)
    scraper = stats_engine.NCAAOrgScraper()
    games = scraper._get_scoreboard(date(2026, 4, 15))
    assert len(games) == 1
    assert games[0]["_served_date_unverified"] is True


# ----- Layer 2: ESPN corroboration -----

def test_espn_date_has_no_events(monkeypatch):
    f = stats_engine.NCAAStatsFetcher()

    monkeypatch.setattr(f._espn, "_get_scoreboard", lambda ds: {"events": []})
    assert f._espn_date_has_no_events("2026-07-06") is True

    monkeypatch.setattr(f._espn, "_get_scoreboard", lambda ds: {"events": [{"id": "1"}]})
    assert f._espn_date_has_no_events("2026-07-06") is False

    def _boom(ds):
        raise RuntimeError("espn unreachable")
    monkeypatch.setattr(f._espn, "_get_scoreboard", _boom)
    # An ESPN error must never assert "no games" — that would drop a real final.
    assert f._espn_date_has_no_events("2026-07-06") is False

    assert f._espn_date_has_no_events(None) is False
    assert f._espn_date_has_no_events("not-a-date") is False


def test_waterfall_drops_unverified_final_when_espn_empty(monkeypatch):
    """The phantom path: unverifiable NCAA.org Final + empty ESPN date -> dropped."""
    f = stats_engine.NCAAStatsFetcher()
    phantom = {
        "game_status": "Final",
        "game_context": "Oklahoma 13, North Carolina 2 | Final",
        "stats_summary": "Did Not Play",
        "game_date": "2026-07-06",
        "_served_date_unverified": True,
    }
    monkeypatch.setattr(f._ncaa_org, "fetch_stats", lambda *a, **k: dict(phantom))
    monkeypatch.setattr(f._espn, "_get_scoreboard", lambda ds: {"events": []})
    f._default_chain = [f._ncaa_org]

    result = f._waterfall_fetch({"player_name": "Lee Sowers", "team": "North Carolina"})
    assert result is None


def test_waterfall_keeps_final_when_espn_has_events(monkeypatch):
    """In season the marker may go missing, but ESPN confirms the game is real:
    the final survives and the internal flag is stripped before emission."""
    f = stats_engine.NCAAStatsFetcher()
    real_final = {
        "game_status": "Final",
        "game_context": "Duke 4, North Carolina 3 | Final",
        "stats_summary": "2-4, 1 HR, 2 RBI",
        "game_date": "2026-04-15",
        "at_bats": 4,
        "hits": 2,
        "_served_date_unverified": True,
    }
    monkeypatch.setattr(f._ncaa_org, "fetch_stats", lambda *a, **k: dict(real_final))
    monkeypatch.setattr(f._espn, "_get_scoreboard", lambda ds: {"events": [{"id": "401"}]})
    f._default_chain = [f._ncaa_org]

    result = f._waterfall_fetch({"player_name": "Some Hitter", "team": "North Carolina"})
    assert result is not None
    assert result["game_status"] == "Final"
    assert result["stats_summary"] == "2-4, 1 HR, 2 RBI"
    assert "_served_date_unverified" not in result
