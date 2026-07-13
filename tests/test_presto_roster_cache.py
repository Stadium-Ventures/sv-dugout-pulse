"""Presto per-team roster cache: a transient proxy failure on one team page
must restore that team from the last good scrape instead of silently
dropping it (Cal Ripken 376→173 on 2026-07-13)."""
from datetime import datetime, timedelta, timezone

import src.summer_ball as sb
from src.summer_ball import CalRipkenLeague, PlayerEntry


ROSTER_HTML = """
<html><body><p>%s</p>
<table>
<tr><th>#</th><th>Name</th><th>Position</th><th>Hometown</th></tr>
<tr><td>1</td><td><a href="/sports/bsb/2026/players/jane-doe">Jane Doe</a></td><td>SS</td><td>Bethesda, MD</td></tr>
<tr><td>2</td><td><a href="/sports/bsb/2026/players/sam-roe">Sam Roe</a></td><td>RHP</td><td>Olney, MD</td></tr>
</table></body></html>
""" % ("x" * 6000)  # padding: _fetch_page treats <5KB bodies as challenge pages


def _league_with_two_teams():
    lg = CalRipkenLeague()
    lg.fallback_team_slugs = ["alphateam", "betateam"]
    return lg


def _patch_cache(monkeypatch, tmp_path, initial=None):
    path = tmp_path / "_presto_roster_cache.json"
    if initial is not None:
        import json
        path.write_text(json.dumps(initial))
    monkeypatch.setattr(sb, "PRESTO_CACHE_PATH", path)
    return path


def test_retry_recovers_transient_failure(monkeypatch, tmp_path):
    _patch_cache(monkeypatch, tmp_path)
    monkeypatch.setattr(sb.time, "sleep", lambda s: None)
    calls = []

    def fake_fetch(self, url):
        calls.append(url)
        if "teams" == url.rsplit("/", 1)[-1].split("?")[0]:
            return ""  # teams index unavailable -> fallback slugs
        # First attempt for alphateam fails, retry succeeds.
        if "alphateam" in url and calls.count(url) == 1:
            return ""
        return ROSTER_HTML

    monkeypatch.setattr(sb.PrestoSportsLeague, "_fetch_page", fake_fetch)
    entries = _league_with_two_teams().discover_rosters()
    teams = {e.summer_team for e in entries}
    assert teams == {"Alphateam", "Betateam"}
    assert len(entries) == 4


def test_persistent_failure_restores_from_cache(monkeypatch, tmp_path):
    cached_at = datetime.now(timezone.utc).isoformat()
    _patch_cache(monkeypatch, tmp_path, initial={
        "Cal Ripken/alphateam": {
            "cached_at": cached_at,
            "players": [PlayerEntry(
                name="jane doe", college="", summer_team="Alphateam",
                league="", raw_name="Jane Doe",
            ).to_dict()],
        },
    })
    monkeypatch.setattr(sb.time, "sleep", lambda s: None)

    def fake_fetch(self, url):
        if "alphateam" in url:
            return ""  # both attempts fail
        if url.rsplit("/", 1)[-1].split("?")[0] == "teams":
            return ""
        return ROSTER_HTML

    monkeypatch.setattr(sb.PrestoSportsLeague, "_fetch_page", fake_fetch)
    entries = _league_with_two_teams().discover_rosters_with_league()
    by_team = {}
    for e in entries:
        by_team.setdefault(e.summer_team, []).append(e)
    assert len(by_team["Alphateam"]) == 1  # restored from cache
    assert len(by_team["Betateam"]) == 2   # fetched live
    # league stamped on restored entries too
    assert all(e.league == "Cal Ripken" for e in entries)


def test_stale_cache_not_restored(monkeypatch, tmp_path):
    stale = (datetime.now(timezone.utc)
             - timedelta(days=sb.PRESTO_CACHE_MAX_AGE_DAYS + 1)).isoformat()
    _patch_cache(monkeypatch, tmp_path, initial={
        "Cal Ripken/alphateam": {
            "cached_at": stale,
            "players": [PlayerEntry(
                name="jane doe", college="", summer_team="Alphateam",
                league="", raw_name="Jane Doe",
            ).to_dict()],
        },
    })
    monkeypatch.setattr(sb.time, "sleep", lambda s: None)

    def fake_fetch(self, url):
        return "" if "alphateam" in url or url.endswith("/teams") else ROSTER_HTML

    monkeypatch.setattr(sb.PrestoSportsLeague, "_fetch_page", fake_fetch)
    entries = _league_with_two_teams().discover_rosters()
    teams = {e.summer_team for e in entries}
    assert "Alphateam" not in teams  # stale cache must drain, not mask


def test_successful_scrape_updates_cache(monkeypatch, tmp_path):
    path = _patch_cache(monkeypatch, tmp_path)
    monkeypatch.setattr(sb.time, "sleep", lambda s: None)

    def fake_fetch(self, url):
        return "" if url.rsplit("/", 1)[-1].split("?")[0] == "teams" else ROSTER_HTML

    monkeypatch.setattr(sb.PrestoSportsLeague, "_fetch_page", fake_fetch)
    _league_with_two_teams().discover_rosters()
    import json
    cache = json.loads(path.read_text())
    assert set(cache) == {"Cal Ripken/alphateam", "Cal Ripken/betateam"}
    assert len(cache["Cal Ripken/alphateam"]["players"]) == 2
