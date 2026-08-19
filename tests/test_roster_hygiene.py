"""Roster hygiene (2026-08-19): only names currently on the master roster +
recruits sheets may be stored or surfaced, and removed names must not
re-accumulate in the append-only stores.

- The HS stats sheet is a STATS source, never a roster source: off-roster
  names are skipped and logged, not synthesized into players.
- Hand-transcribed summer placements gate on roster names so spelling drift
  can't silently publish ("Bryson Tweedy" vs roster "Brisen Tweedy").
- Prune-on-write drops off-roster keys, but only off a FRESH roster fetch.
- Status/Org = "Retired" on the sheet suppresses a player with no code change.
"""
import json
from datetime import date

import main
from src import alerts, hs_stats, roster_manager, summer_pulse
from src.hs_stats import HSGameLog


# ---------------------------------------------------------------------------
# HS sheet gate
# ---------------------------------------------------------------------------

def _parsed_sheet():
    return [{
        "date": date(2026, 4, 1),
        "hitters": [
            {"player": "Roster Kid", "ab": 3, "h": 2, "game_result": "W 5-2"},
            {"player": "Stranger Kid", "ab": 4, "h": 1, "game_result": "W 5-2"},
        ],
        "pitchers": [
            {"player": "Stranger Arm", "ip": 5.0, "h": 3, "er": 1, "bb": 1, "k": 6,
             "game_result": "W 5-2"},
        ],
    }]


def _fresh_log(tmp_path, monkeypatch, initial=None):
    path = tmp_path / "hs_game_log.json"
    if initial is not None:
        path.write_text(json.dumps(initial))
    monkeypatch.setattr(hs_stats, "HS_GAME_LOG_PATH", str(path))
    return HSGameLog(), path


def test_hs_update_skips_off_roster_names(tmp_path, monkeypatch, caplog):
    log, path = _fresh_log(tmp_path, monkeypatch)
    with caplog.at_level("WARNING"):
        log.update_from_sheet(_parsed_sheet(), roster_names={"roster kid"})
    assert set(log._log) == {"Roster Kid"}
    saved = json.loads(path.read_text())
    assert set(saved) == {"Roster Kid"}
    # Skipped names are logged so new misspellings get noticed.
    assert "Stranger Kid" in caplog.text
    assert "Stranger Arm" in caplog.text


def test_hs_update_without_roster_keeps_all():
    # No roster names -> no gate (fail open; e.g. no cache available).
    log = HSGameLog.__new__(HSGameLog)
    log._log = {}
    log.save = lambda: None
    log.update_from_sheet(_parsed_sheet())
    assert set(log._log) == {"Roster Kid", "Stranger Kid", "Stranger Arm"}


def test_hs_update_prunes_only_when_asked(tmp_path, monkeypatch):
    initial = {"Removed Client": [{"date": "2026-03-01", "type": "hitting", "stats": {}}]}
    log, path = _fresh_log(tmp_path, monkeypatch, initial=initial)
    # prune=False (stale-cache path): existing off-roster keys survive.
    log.update_from_sheet(_parsed_sheet(), roster_names={"roster kid"}, prune=False)
    assert "Removed Client" in log._log

    log2, _ = _fresh_log(tmp_path, monkeypatch)
    log2._log = json.loads(path.read_text())
    # prune=True (fresh roster fetch): off-roster keys drop from the file.
    log2.update_from_sheet(_parsed_sheet(), roster_names={"roster kid"}, prune=True)
    saved = json.loads(path.read_text())
    assert set(saved) == {"Roster Kid"}


# ---------------------------------------------------------------------------
# Summer placement gate
# ---------------------------------------------------------------------------

def _write_placements(tmp_path, names):
    p = tmp_path / "placements.json"
    p.write_text(json.dumps({"placements": [
        {"player_name": n, "summer_team": "Anytown Anchors", "league": "NECBL"}
        for n in names
    ]}))
    return p


def test_load_placements_skips_names_not_on_roster(tmp_path, monkeypatch, caplog):
    p = _write_placements(tmp_path, ["Brisen Tweedy", "Bryson Tweedy"])
    monkeypatch.setattr(summer_pulse, "_PLACEMENTS_PATH", p)
    monkeypatch.setattr(summer_pulse, "pro_player_names", lambda: set())
    monkeypatch.setattr(summer_pulse, "all_roster_names", lambda: {"brisen tweedy"})
    with caplog.at_level("WARNING"):
        names = {x["player_name"] for x in summer_pulse._load_placements()}
    assert names == {"Brisen Tweedy"}
    assert "Bryson Tweedy" in caplog.text  # drift is loud, never silent


def test_load_placements_fails_open_without_roster(tmp_path, monkeypatch):
    # Empty roster set = no cache. Filtering everything out would blank the
    # summer dashboard on a cache hiccup — must fail open instead.
    p = _write_placements(tmp_path, ["Anyone AtAll"])
    monkeypatch.setattr(summer_pulse, "_PLACEMENTS_PATH", p)
    monkeypatch.setattr(summer_pulse, "pro_player_names", lambda: set())
    monkeypatch.setattr(summer_pulse, "all_roster_names", lambda: set())
    assert len(summer_pulse._load_placements()) == 1


# ---------------------------------------------------------------------------
# Retired suppression (sheet-side, no code change needed)
# ---------------------------------------------------------------------------

def _row(name, level="NCAA", status="", org="State U"):
    return {"Player Name": name, "Level": level, "Status": status, "Org": org}


def test_filter_roster_excludes_retired_status_and_org():
    rows = [
        _row("Active Guy"),
        _row("Status Retiree", status="Retired"),
        _row("Caps Retiree", status="RETIRED"),
        _row("Org Retiree", level="Pro", org="Retired"),
    ]
    kept = {p["player_name"] for p in roster_manager.filter_roster(rows)}
    assert kept == {"Active Guy"}


def test_filter_roster_other_statuses_kept():
    rows = [_row("Hurt Guy", status="Injured"), _row("FA Guy", status="Free Agent")]
    kept = {p["player_name"] for p in roster_manager.filter_roster(rows)}
    assert kept == {"Hurt Guy", "FA Guy"}


# ---------------------------------------------------------------------------
# NCAA game log prune-on-write
# ---------------------------------------------------------------------------

def test_flush_ncaa_game_log_prunes_off_roster_keys(tmp_path, monkeypatch):
    path = tmp_path / "ncaa_game_log.json"
    path.write_text(json.dumps({
        "Roster Guy|State U": [{"date": "2026-04-01", "opponent": "vs Rival", "stats": {}}],
        "Removed Guy|Old U": [{"date": "2026-04-01", "opponent": "vs Rival", "stats": {}}],
    }))
    monkeypatch.setattr(main, "NCAA_GAME_LOG_PATH", str(path))
    main._ncaa_log_pending.clear()

    # Without roster names (stale-cache path): nothing pruned.
    main._flush_ncaa_game_log()
    assert set(json.loads(path.read_text())) == {"Roster Guy|State U", "Removed Guy|Old U"}

    # With fresh roster names: off-roster key drops even with nothing queued.
    main._flush_ncaa_game_log(roster_names={"roster guy"})
    assert set(json.loads(path.read_text())) == {"Roster Guy|State U"}


# ---------------------------------------------------------------------------
# Team-level (promotion tracking) state prune
# ---------------------------------------------------------------------------

def test_prune_team_state_drops_off_roster_ids(tmp_path, monkeypatch):
    path = tmp_path / "_last_team_levels.json"
    path.write_text(json.dumps({
        "111": {"team_id": 1, "sport_id": 11, "name": "Roster Pro"},
        "222": {"team_id": 2, "sport_id": 12, "name": "Removed Pro"},
    }))
    monkeypatch.setattr(alerts, "_TEAM_STATE_PATH", str(path))
    alerts.prune_team_state({111})
    assert set(json.loads(path.read_text())) == {"111"}


def test_prune_team_state_noops_on_empty_ids(tmp_path, monkeypatch):
    # An empty id set means "no roster" — never interpret as "prune everyone".
    path = tmp_path / "_last_team_levels.json"
    path.write_text(json.dumps({"111": {"name": "Roster Pro"}}))
    monkeypatch.setattr(alerts, "_TEAM_STATE_PATH", str(path))
    alerts.prune_team_state(set())
    assert set(json.loads(path.read_text())) == {"111"}
