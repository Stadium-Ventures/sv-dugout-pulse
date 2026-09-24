"""The committed roster cache is PUBLIC (this repo is public and GitHub Pages
serves data/). It must only ever carry allowlisted, dashboard-level fields —
never DOB, age or any contact/personal field — whatever the source sends.

All fixture values are synthetic.
"""
import json

import pytest

from src import config, roster_manager
from src.roster_manager import cache_safe_player, filter_roster, normalize_player

PERSONAL_KEYS = {"dob", "age", "phone", "email", "address", "father", "mother",
                 "parents", "x_handle", "ig_handle", "state", "home_state", "high_school"}


def _sheet_row(**extra):
    row = {
        "Player Name": "Synthetic Hitter", "MLB_ID": "", "Org": "Synthetic U",
        "Level": "NCAA", "Position": "Hitter", "Tier": "2",
        # Columns the published sheet carries that must never reach the cache.
        "DOB": "1/1/2000", "Age": "20.0", "Phone": "000-000-0000",
        "Email": "synthetic@example.invalid", "X Handle": "@synthetic",
        "IG Handle": "@synthetic", "State (High School)": "ZZ",
    }
    row.update(extra)
    return row


def test_column_map_does_not_read_dob_or_age():
    assert "DOB" not in config.COLUMN_MAP
    assert "Age" not in config.COLUMN_MAP
    p = normalize_player(_sheet_row())
    assert "dob" not in p and "age" not in p


def test_cache_allowlist_has_no_personal_fields():
    assert not PERSONAL_KEYS & set(config.ROSTER_CACHE_FIELDS)


def test_cache_safe_player_strips_everything_not_allowlisted():
    p = normalize_player(_sheet_row())
    p.update({"dob": "x", "age": "x", "email": "x", "lead_agent": "x", "slack_channel_id": "x"})
    safe = cache_safe_player(p)
    assert set(safe) <= set(config.ROSTER_CACHE_FIELDS)
    assert safe["player_name"] == "Synthetic Hitter"


def test_saved_cache_file_contains_only_allowlisted_keys(tmp_path, monkeypatch):
    path = tmp_path / "roster_cache.json"
    monkeypatch.setattr(roster_manager, "ROSTER_CACHE_PATH", str(path))
    players = filter_roster([_sheet_row(), _sheet_row(**{"Player Name": "Synthetic Arm", "Position": "Pitcher"})])
    for p in players:
        p.update({"dob": "x", "age": "x", "is_client": True})
    roster_manager._save_roster_cache(players)
    data = json.loads(path.read_text())
    assert len(data["players"]) == 2
    for p in data["players"]:
        assert set(p) <= set(config.ROSTER_CACHE_FIELDS)
        assert not PERSONAL_KEYS & set(p)
    raw = path.read_text()
    assert "synthetic@example.invalid" not in raw and "1/1/2000" not in raw


def test_no_hard_coded_published_csv_url():
    src = open(config.__file__).read()
    assert "docs.google.com/spreadsheets/d/e/" not in src


def test_fetch_roster_without_url_fails(monkeypatch):
    monkeypatch.setattr(roster_manager, "ROSTER_URL", "")
    with pytest.raises(ValueError):
        roster_manager.fetch_roster()


def test_recruits_without_url_is_empty_not_the_client_sheet(monkeypatch):
    monkeypatch.setattr(roster_manager, "RECRUITS_URL", "")
    called = []
    monkeypatch.setattr(roster_manager, "fetch_roster", lambda url=None: called.append(url) or [])
    assert roster_manager.get_recruits() == []
    assert called == []
