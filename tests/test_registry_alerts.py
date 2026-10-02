"""#sv-automation alerts on the registry roster path.

Two things used to land only in the Actions log: a registry read failure
that fell back to the saved list, and a failed dual-run read. A registry
roster with no peaks is expected (peaks come from the SV Scouting Hub) and
stays silent. Offline; synthetic fixtures only.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from src import roster_manager as rm
from test_registry_roster import (  # noqa: F401  (fixtures)
    FILLER, FakeResp, FakeSession, _RequestsShim, _write_cache, body, fake_registry,
    isolated, prow, registry_env,
)


@pytest.fixture
def posts(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(rm, "ROSTER_ALERT_STATE_PATH", str(tmp_path / "_roster_source_alerts.json"))
    monkeypatch.setattr(rm, "_send_automation", lambda text: sent.append(text) or True)
    return sent


def assert_three_beats(text):
    lines = text.split("\n")
    assert len(lines) == 3
    assert lines[1].startswith("How we know:") and lines[2].startswith("What to do:")
    assert "👤" in lines[2]
    for bad in ("—", "–", " -- ", "...", "…", "svt_", "Bearer"):
        assert bad not in text


# ── registry read failure on the saved list ──────────────────────────────

@pytest.mark.parametrize("status,needle", [
    (401, "wrong or revoked"), (403, "no roster access"), (503, "did not answer normally"),
])
def test_registry_failure_on_saved_list_alerts(monkeypatch, registry_env, isolated, posts, status, needle):
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(status, {}))))
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: pytest.fail("sheet fallback"))
    _write_cache(isolated, "registry", [{"player_name": "Registry Era Player", "level": "Pro"}])
    assert [p["player_name"] for p in rm.get_all_players()] == ["Registry Era Player"]
    assert len(posts) == 1
    assert "running on the last saved list" in posts[0] and needle in posts[0]
    assert_three_beats(posts[0])


def test_registry_failure_without_saved_list_leaves_it_to_the_failure_alert(
        monkeypatch, registry_env, isolated, posts):
    # No usable cache: main.py exits 1 and pulse.yml's "Alert on failure" step posts.
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(503, {}))))
    assert rm.get_all_players() == []
    assert posts == []


def test_alert_send_failure_never_breaks_the_run(monkeypatch, registry_env, isolated, tmp_path):
    monkeypatch.setattr(rm, "ROSTER_ALERT_STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setattr(rm, "_send_automation", lambda text: (_ for _ in ()).throw(RuntimeError("slack down")))
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(503, {}))))
    _write_cache(isolated, "registry", [{"player_name": "Registry Era Player", "level": "Pro"}])
    assert len(rm.get_all_players()) == 1
    assert not (tmp_path / "state.json").exists()  # unsent → retried next run


# ── dual run ─────────────────────────────────────────────────────────────

def test_dual_run_read_failure_alerts_once_a_day(monkeypatch, posts):
    monkeypatch.setenv("SV_REGISTRY_ROSTER_TOKEN", "svt_test_not_a_real_token")
    monkeypatch.setattr(rm, "get_registry_clients",
                        lambda: (_ for _ in ()).throw(rm.RegistryRosterError("registry roster projection returned 401")))
    rm._log_dual_run([])
    rm._log_dual_run([])
    assert len(posts) == 1
    assert "roster check against SV Registry could not run" in posts[0]
    assert "still uses the sheet" in posts[0]
    assert_three_beats(posts[0])


def test_dual_run_without_token_is_silent(monkeypatch, posts):
    monkeypatch.delenv("SV_REGISTRY_ROSTER_TOKEN", raising=False)
    rm._log_dual_run([])
    assert posts == []


def test_dual_run_count_line_is_counts_only(monkeypatch, caplog, posts, fake_registry):
    monkeypatch.setenv("ROSTER_SOURCE", "sheet")
    sheet = rm.filter_roster([{"Player Name": "Only On Sheet", "Level": "NCAA", "Org": "U", "Tier": "2"}])
    with caplog.at_level("INFO"):
        rm._log_dual_run(sheet)
    line = [r.getMessage() for r in caplog.records if "[dual-run]" in r.getMessage()][0]
    assert "'sheet_only': 1" in line
    assert "Only On Sheet" not in line and "Synthetic" not in line
    assert posts == []


def test_registry_retired_rows_are_counted_not_named_in_the_public_log(caplog, fake_registry):
    with caplog.at_level("INFO"):
        rm.get_registry_clients()
    assert "Synthetic Retired" not in caplog.text
    assert "Synthetic Sheet Retired" not in caplog.text
    assert "2 retired" in caplog.text


def test_sheet_retired_rows_are_still_named_as_today(caplog):
    with caplog.at_level("INFO"):
        rm.filter_roster([{"Player Name": "Sheet Retired Synthetic", "Level": "Pro", "Org": "X",
                           "Status": "Retired", "Tier": "2"}])
    assert "Excluding Sheet Retired Synthetic" in caplog.text


def test_cooldown_expires_after_a_day(monkeypatch, registry_env, isolated, posts):
    old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    with open(rm.ROSTER_ALERT_STATE_PATH, "w") as f:
        json.dump({"registry_read_failed": old}, f)
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(503, {}))))
    _write_cache(isolated, "registry", [{"player_name": "Registry Era Player", "level": "Pro"}])
    rm.get_all_players()
    assert len(posts) == 1


def test_registry_roster_without_peaks_is_silent(fake_registry, isolated, posts):
    # Peaks come from the SV Scouting Hub in the browser (js/gate.js
    # svPeakValues), not from the roster, so no peaks on the registry rows
    # is expected and must not alert.
    rm.get_all_players()
    assert posts == []
