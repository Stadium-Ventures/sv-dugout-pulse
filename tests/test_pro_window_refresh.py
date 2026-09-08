"""Once-per-game-day Pro rolling-window refresh from the live path.

Why: the scheduled 6 AM ET historical pass is GitHub-cron best-effort and ran
at 9:37 AM ET on 2026-09-05, so a 7D card viewed at 7:35 AM lacked the prior
night's Final. The first live run after the 4 AM ET day flip now rebuilds the
Pro 7/14/30D entries and stamps a marker so it runs once a day.
"""
import json
import os
from datetime import date

os.environ.setdefault("ROSTER_URL", "http://example.invalid/roster.csv")

import main  # noqa: E402


def test_due_when_no_marker():
    assert main._pro_window_refresh_due(None, date(2026, 9, 5))
    assert main._pro_window_refresh_due({}, date(2026, 9, 5))


def test_not_due_same_game_day():
    assert not main._pro_window_refresh_due({"et_date": "2026-09-05"}, date(2026, 9, 5))


def test_due_on_new_game_day():
    assert main._pro_window_refresh_due({"et_date": "2026-09-04"}, date(2026, 9, 5))


def test_merge_replaces_pro_keeps_everything_else():
    existing = [
        {"player_name": "Old Pro", "level": "Pro", "games_played": 4},
        {"player_name": "College Kid", "level": "NCAA"},
        {"player_name": "Prep Kid", "level": "HS"},
        {"player_name": "Summer Guy", "level": "Summer"},
    ]
    fresh = [{"player_name": "Old Pro", "level": "Pro", "games_played": 5}]
    merged = main._merge_pro_window(existing, fresh)
    by_name = {e["player_name"]: e for e in merged}
    assert by_name["Old Pro"]["games_played"] == 5
    assert set(by_name) == {"Old Pro", "College Kid", "Prep Kid", "Summer Guy"}


def test_once_daily_writes_all_three_windows_and_marker(tmp_path, monkeypatch):
    paths = {w: str(tmp_path / f"window_{w}.json") for w in ("7d", "14d", "30d")}
    for w, p in paths.items():
        json.dump(
            [
                {"player_name": "Carter Johnson", "level": "Pro", "window": w, "games_played": 0},
                {"player_name": "College Kid", "level": "NCAA", "window": w},
            ],
            open(p, "w"),
        )
    marker = str(tmp_path / "pro_window_refresh.json")
    monkeypatch.setattr(main, "WINDOW_7D_PATH", paths["7d"])
    monkeypatch.setattr(main, "WINDOW_14D_PATH", paths["14d"])
    monkeypatch.setattr(main, "WINDOW_30D_PATH", paths["30d"])
    monkeypatch.setattr(main, "PRO_WINDOW_REFRESH_MARKER_PATH", marker)
    monkeypatch.setattr(main, "_today_et", lambda: date(2026, 9, 5))

    calls = []

    def fake_build(self, player, window, start, end):
        calls.append((player["player_name"], window, start, end))
        return {"player_name": player["player_name"], "level": "Pro", "window": window, "games_played": 1}

    monkeypatch.setattr(main.WindowStatsAggregator, "_build_window_entry", fake_build)

    players = [
        {"player_name": "Carter Johnson", "level": "Pro"},
        {"player_name": "College Kid", "level": "NCAA"},
    ]
    assert main._refresh_pro_windows_once_daily(players) is True

    # Only the Pro player was fetched, once per window, ending on the ET game day.
    assert sorted(c[1] for c in calls) == ["14d", "30d", "7d"]
    assert all(c[0] == "Carter Johnson" and c[3] == date(2026, 9, 5) for c in calls)

    for w, p in paths.items():
        data = json.load(open(p))
        entries = data.get("players", data) if isinstance(data, dict) else data
        by_name = {e["player_name"]: e for e in entries}
        assert by_name["Carter Johnson"]["games_played"] == 1, w
        assert "College Kid" in by_name, w

    assert json.load(open(marker))["et_date"] == "2026-09-05"

    # Second run the same game day is a no-op.
    calls.clear()
    assert main._refresh_pro_windows_once_daily(players) is False
    assert calls == []


def test_failed_refresh_leaves_marker_unset(tmp_path, monkeypatch):
    marker = str(tmp_path / "pro_window_refresh.json")
    monkeypatch.setattr(main, "PRO_WINDOW_REFRESH_MARKER_PATH", marker)
    monkeypatch.setattr(main, "_today_et", lambda: date(2026, 9, 5))

    def boom(players, today=None):
        raise RuntimeError("MLB API down")

    monkeypatch.setattr(main, "_refresh_pro_windows", boom)
    assert main._refresh_pro_windows_once_daily([{"player_name": "X", "level": "Pro"}]) is False
    assert not os.path.exists(marker)
