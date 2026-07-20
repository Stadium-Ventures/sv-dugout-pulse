"""Drafted/signed players (Level=Pro on the master sheet) must drop out of
summer-ball tracking automatically — flipping the master sheet is the only
edit needed (Kent, 2026-07-20)."""
import json

from src import summer_pulse
from src import roster_manager


def _write_cache(tmp_path, players):
    cache = tmp_path / "roster_cache.json"
    cache.write_text(json.dumps({"cached_at": "2026-07-20T12:00:00+00:00", "players": players}))
    return str(cache)


def test_pro_player_names_reads_level(tmp_path, monkeypatch):
    path = _write_cache(tmp_path, [
        {"player_name": "Signed Guy", "level": "Pro"},
        {"player_name": "College Guy", "level": "NCAA"},
        {"player_name": "", "level": "Pro"},  # blank names never match
    ])
    monkeypatch.setattr(roster_manager, "ROSTER_CACHE_PATH", path)
    assert roster_manager.pro_player_names() == {"signed guy"}


def test_pro_player_names_missing_cache_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(roster_manager, "ROSTER_CACHE_PATH", str(tmp_path / "nope.json"))
    assert roster_manager.pro_player_names() == set()


def test_load_placements_drops_pro_players(tmp_path, monkeypatch):
    placements = tmp_path / "placements.json"
    placements.write_text(json.dumps({"placements": [
        {"player_name": "Signed Guy", "summer_team": "Trenton Thunder",
         "league": "MLB Draft", "status": "Confirmed"},
        {"player_name": "College Guy", "summer_team": "Harwich Mariners",
         "league": "Cape Cod", "status": "Confirmed"},
    ]}))
    monkeypatch.setattr(summer_pulse, "_PLACEMENTS_PATH", placements)
    monkeypatch.setattr(summer_pulse, "pro_player_names", lambda: {"signed guy"})

    names = {p["player_name"] for p in summer_pulse._load_placements()}
    assert names == {"College Guy"}
