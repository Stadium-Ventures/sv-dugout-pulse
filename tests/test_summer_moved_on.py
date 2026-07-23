"""Moved-on filter for the daily summer recap (issue #19): players who are
finished with the team they'd be shown under get dropped, active players and
normal pitcher rest don't."""
import json

from scripts import summer_daily_slack as s


def _log_entry(name, team, summary):
    return {"player_name": name, "team": team, "stats_summary": summary}


def _write_log(tmp_path, monkeypatch, log):
    path = tmp_path / "summer_game_log.json"
    path.write_text(json.dumps(log))
    monkeypatch.setattr(s, "_GAME_LOG_PATH", path)
    return path


def _entry(name, team):
    return {"player_name": name, "level": "Summer", "is_client": True, "team": team}


def test_finished_player_dropped(tmp_path, monkeypatch):
    # Player appeared once long ago; team played 8 more game-days without him.
    team = "Hyannis Harbor Hawks (Cape Cod)"
    log = {"2026-06-26": [_log_entry("Tanner Chun", team, "1-4, K")]}
    for i in range(1, 9):
        log[f"2026-07-{i:02d}"] = [_log_entry("Tanner Chun", team, "Did not appear")]
    _write_log(tmp_path, monkeypatch, log)
    out = s._moved_on_players([_entry("Tanner Chun", team)])
    assert out == {"Tanner Chun": team}


def test_normal_pitcher_rest_kept(tmp_path, monkeypatch):
    # 6 team game-days since last outing — under both bars, stays in.
    team = "Hyannis Harbor Hawks (Cape Cod)"
    log = {"2026-07-14": [_log_entry("Brady St. Pierre", team, "3.0 IP, 0 ER")]}
    for i in range(15, 21):
        log[f"2026-07-{i:02d}"] = [_log_entry("Brady St. Pierre", team, "Did not appear")]
    _write_log(tmp_path, monkeypatch, log)
    assert s._moved_on_players([_entry("Brady St. Pierre", team)]) == {}


def test_newer_team_wins(tmp_path, monkeypatch):
    # Most recent appearance is with a different team — old affiliation drops.
    old = "Cotuit Kettleers (Cape Cod)"
    new = "Bourne Braves (Cape Cod)"
    log = {
        "2026-07-01": [_log_entry("Sam Player", old, "1-3")],
        "2026-07-20": [_log_entry("Sam Player", new, "2-4, HR")],
    }
    _write_log(tmp_path, monkeypatch, log)
    assert s._moved_on_players([_entry("Sam Player", old)]) == {"Sam Player": old}
    assert s._moved_on_players([_entry("Sam Player", new)]) == {}


def test_no_logged_appearance_left_alone(tmp_path, monkeypatch):
    # Only DNP rows — no evidence either way, keep showing him.
    team = "Falmouth Commodores (Cape Cod)"
    log = {
        f"2026-07-{i:02d}": [_log_entry("Dom Woodward", team, "Did not appear")]
        for i in range(1, 15)
    }
    _write_log(tmp_path, monkeypatch, log)
    assert s._moved_on_players([_entry("Dom Woodward", team)]) == {}


def test_missing_log_is_safe(tmp_path, monkeypatch):
    monkeypatch.setattr(s, "_GAME_LOG_PATH", tmp_path / "nope.json")
    assert s._moved_on_players([_entry("Anyone", "Any Team")]) == {}
