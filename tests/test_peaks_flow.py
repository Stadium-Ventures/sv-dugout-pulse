"""StS peak projections (Peak WAR / wRC+ / ERA) keep flowing on the sheet path.

The dashboard's peak chips (js/app.js peakChipHtml, player.html renderPeak)
read tags.peak_* from current_pulse.json, which main.build_pulse_entry fills
from the roster player dict. On the sheet path those values come from the
roster sheet's Peak columns. The roster cache allowlist (0001) must keep them,
because the cache is the fallback when the sheet fetch fails.

All fixtures are synthetic (made-up names, 9001xx MLB ids).
"""
import json
from datetime import datetime, timezone

import pytest

import main
from src import config, roster_manager as rm

PEAK_KEYS = ("peak_war", "peak_wrc_plus", "peak_era_20tbf")


def sheet_row(name, level="Pro", mlb_id="", position="Hitter", **peaks):
    row = {"Player Name": name, "Level": level, "Org": "Synthetic Club", "MLB_ID": mlb_id,
           "Position": position, "Tier": "2", "DOB": "2001-01-01", "Age": "25",
           "X Handle": "@synthetic", "IG Handle": "@synthetic"}
    row.update(peaks)
    return row


SHEET = [
    sheet_row("Synthetic Peak Bat", mlb_id="900101",
              **{"Peak WAR": "3.1", "Peak wRC+": "112", "Peak ERA (20 TBF)": ""}),
    sheet_row("Synthetic Peak Arm", mlb_id="900102", position="Pitcher",
              **{"Peak WAR": "2.4", "Peak wRC+": "", "Peak ERA (20 TBF)": "3.85"}),
    sheet_row("Synthetic No Peak", mlb_id="900103"),
    sheet_row("Synthetic College Bat", level="NCAA"),
]


@pytest.fixture
def sheet_mode(monkeypatch, tmp_path):
    monkeypatch.delenv("ROSTER_SOURCE", raising=False)
    monkeypatch.delenv("SV_REGISTRY_ROSTER_TOKEN", raising=False)
    path = tmp_path / "roster_cache.json"
    monkeypatch.setattr(rm, "ROSTER_CACHE_PATH", str(path))
    monkeypatch.setattr(rm, "_enrich_pro_team_from_api", lambda players: None)
    monkeypatch.setattr(rm, "get_recruits", lambda url=None: [])
    return path


def entry_for(player):
    return main.build_pulse_entry(player, {"game_status": "N/A"},
                                  {"performance_grade": "", "social_search_url": ""})


def test_peak_columns_are_still_mapped():
    assert config.COLUMN_MAP["Peak WAR"] == "peak_war"
    assert config.COLUMN_MAP["Peak wRC+"] == "peak_wrc_plus"
    assert config.COLUMN_MAP["Peak ERA (20 TBF)"] == "peak_era_20tbf"
    for key in PEAK_KEYS:
        assert key in config.ROSTER_CACHE_FIELDS


def test_sheet_peaks_reach_the_dashboard_tags(monkeypatch, sheet_mode):
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: SHEET)
    players = {p["player_name"]: p for p in rm.get_all_players()}
    assert rm.roster_is_fresh() is True

    bat = entry_for(players["Synthetic Peak Bat"])["tags"]
    assert (bat["peak_war"], bat["peak_wrc_plus"]) == ("3.1", "112")
    assert "peak_era_20tbf" not in bat          # blank stays hidden, as today

    arm = entry_for(players["Synthetic Peak Arm"])["tags"]
    assert (arm["peak_war"], arm["peak_era_20tbf"]) == ("2.4", "3.85")

    none = entry_for(players["Synthetic No Peak"])["tags"]
    assert not any(k in none for k in PEAK_KEYS)


def test_public_cache_keeps_peaks_and_drops_personal_fields(monkeypatch, sheet_mode):
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: SHEET)
    rm.get_all_players()
    cached = {p["player_name"]: p for p in json.loads(sheet_mode.read_text())["players"]}
    assert cached["Synthetic Peak Bat"]["peak_war"] == "3.1"
    assert cached["Synthetic Peak Bat"]["peak_wrc_plus"] == "112"
    assert cached["Synthetic Peak Arm"]["peak_era_20tbf"] == "3.85"
    for p in cached.values():
        assert not {"dob", "age", "x_handle", "ig_handle", "state"} & set(p)


def test_cache_fallback_still_serves_peaks(monkeypatch, sheet_mode):
    sheet_mode.write_text(json.dumps({
        "cached_at": datetime.now(timezone.utc).isoformat(), "source": "sheet",
        "players": [{"player_name": "Synthetic Peak Bat", "level": "Pro", "team": "Synthetic Club",
                     "position": "Hitter", "draft_class": "", "roster_priority": 2,
                     "mlb_id": 900101, "is_client": True,
                     "peak_war": "3.1", "peak_wrc_plus": "112"}]}))

    def boom(url=None):
        raise RuntimeError("sheet unreachable")
    monkeypatch.setattr(rm, "fetch_roster", boom)
    players = rm.get_all_players()
    assert rm.roster_is_fresh() is False
    tags = entry_for(players[0])["tags"]
    assert (tags["peak_war"], tags["peak_wrc_plus"]) == ("3.1", "112")
