"""Season-over detection shared by the daily recap and quiet-streak alert."""
import json
from datetime import date

from scripts import _summer_season as s


def _write_log(tmp_path, days):
    path = tmp_path / "summer_game_log.json"
    path.write_text(json.dumps(days))
    return path


def test_active_when_real_game_recent(tmp_path):
    log = _write_log(tmp_path, {
        "2026-08-10": [{"player_name": "A", "stats_summary": "2-4, HR"}],
    })
    assert s.season_is_active(today=date(2026, 8, 14), log_path=log)


def test_inactive_once_idle_past_threshold(tmp_path):
    log = _write_log(tmp_path, {
        "2026-08-08": [{"player_name": "A", "stats_summary": "0-3, BB"}],
    })
    # Exactly at the threshold and beyond — season's over.
    assert not s.season_is_active(today=date(2026, 8, 16), log_path=log)
    assert not s.season_is_active(today=date(2026, 8, 20), log_path=log)


def test_inactive_right_up_to_threshold(tmp_path):
    log = _write_log(tmp_path, {
        "2026-08-08": [{"player_name": "A", "stats_summary": "0-3, BB"}],
    })
    # 7 days idle still counts as active — below the 8-day bar.
    assert s.season_is_active(today=date(2026, 8, 15), log_path=log)


def test_dnp_placeholders_dont_count_as_real_games(tmp_path):
    log = _write_log(tmp_path, {
        "2026-08-15": [{"player_name": "A", "stats_summary": "Did not appear"}],
        "2026-08-08": [{"player_name": "A", "stats_summary": "0-3, BB"}],
    })
    assert not s.season_is_active(today=date(2026, 8, 16), log_path=log)


def test_missing_log_is_inactive(tmp_path):
    assert not s.season_is_active(today=date(2026, 8, 16), log_path=tmp_path / "missing.json")


def test_empty_log_is_inactive(tmp_path):
    log = _write_log(tmp_path, {})
    assert not s.season_is_active(today=date(2026, 8, 16), log_path=log)
