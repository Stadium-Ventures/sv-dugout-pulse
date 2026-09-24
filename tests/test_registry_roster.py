"""Switchable client-roster loader: ROSTER_SOURCE=sheet|registry.

Offline: the registry is a fake HTTP session. Every fixture row is synthetic
(made-up names and slugs, 9000xx MLB ids). The one real spelling pair used,
"Cam Flukey" / "Cameron Flukey", is the documented cutover rename.
"""
import json
from datetime import datetime, timezone

import pytest

import main
from src import config, roster_manager as rm


# ── fixtures ─────────────────────────────────────────────────────────────

def prow(slug, name, tier="amt", mlb_id=None, level="NCAA", **extra):
    row = {
        "slug": slug, "name": name, "mlb_id": mlb_id, "tier": tier, "level": level,
        "org": "Synthetic U", "team": "", "affiliate": "", "priority_tier": 2,
        "lead_agent": "Synthetic Agent", "slack_channel_id": "C000SYNTH",
        "slack_channel_name": "synthetic", "visits": {"target_2026": 1},
        "is_client": True, "career_status": "active", "is_coach": False,
        "agency": {"status": "x"}, "position": "SS", "draft_class": "2027",
        "home_state": "ZZ", "high_school": "Synthetic HS", "bats": "R", "throws": "R",
        "ig_handle": "@synthetic", "x_handle": "@synthetic", "nicknames": ["Synth"],
        "sheet_status": "", "sheet_level": level if tier == "amt" else "Pro",
    }
    row.update(extra)
    return row


FILLER = [prow(f"synthetic-amateur-{i:02d}", f"Synthetic Amateur {i:02d}") for i in range(45)]
CORE = [
    prow("synthetic-pro-arm", "Synthetic Pro Arm", tier="pro", mlb_id=900001, level="Low-A",
         position="RHP", org="Synthetic Parent Club", affiliate="Synthetic Low-A Club"),
    prow("synthetic-pro-bat", "Synthetic Pro Bat", tier="pro", mlb_id="900002", level="AAA", position="SS"),
    prow("synthetic-two-way", "Synthetic Two Way", tier="pro", mlb_id=900003, level="AA", position="RHP/OF"),
    prow("synthetic-starter", "Synthetic Starter", tier="pro", mlb_id=900004, level="MLB", position="SP"),
    prow("synthetic-retired", "Synthetic Retired", tier="pro", mlb_id=900005, career_status="retired"),
    prow("synthetic-sheet-retired", "Synthetic Sheet Retired", tier="pro", mlb_id=900006, sheet_status="Retired"),
    prow("synthetic-injured", "Synthetic Injured", tier="amt", level="HS", sheet_level="HS", sheet_status="Injured"),
    prow("synthetic-coach", "Synthetic Coach", tier="coach", level="Pro", is_coach=True, position="Head Coach"),
    prow("synthetic-juco", "Synthetic Juco", tier="amt", level="JUCO", sheet_level="JUCO"),
    prow("synthetic-drafted", "Synthetic Drafted", tier="amt", level="NCAA", org="Boston Red Sox"),
    prow("synthetic-no-position", "Synthetic No Position", tier="amt", position=""),
    prow("cameron-flukey-fixture", "Cameron Flukey", tier="pro", mlb_id=900007, level="High-A", position="RHP"),
]
ROWS = CORE + FILLER


def body(rows=None, contact_flag=True):
    rows = ROWS if rows is None else rows
    return {"_meta": {"generated_at": "2026-09-24T00:00:00Z", "rows": len(rows),
                      "contains_no_contact_data": contact_flag}, "rows": rows}


class FakeResp:
    def __init__(self, status=200, payload=None, bad_json=False):
        self.status_code, self._payload, self._bad = status, payload, bad_json

    def json(self):
        if self._bad:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, resp):
        self.resp, self.calls = resp, []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return self.resp


@pytest.fixture
def registry_env(monkeypatch):
    monkeypatch.setenv("ROSTER_SOURCE", "registry")
    monkeypatch.setenv("SV_REGISTRY_ROSTER_TOKEN", "svt_test_not_a_real_token")
    monkeypatch.setenv("SV_REGISTRY_URL", "https://registry.example/")


@pytest.fixture
def fake_registry(monkeypatch, registry_env):
    session = FakeSession(FakeResp(200, body()))
    monkeypatch.setattr(rm, "requests", _RequestsShim(session))
    return session


class _RequestsShim:
    """Replaces the module's `requests` so fetch_registry_rows() hits the fake."""
    RequestException = rm.requests.RequestException

    def __init__(self, session):
        self.session = session

    def get(self, *a, **k):
        return self.session.get(*a, **k)

    def Session(self):  # used by the MLB API enrichment — must never run here
        raise AssertionError("network")


def by_name(players):
    return {p["player_name"]: p for p in players}


# ── source switch ────────────────────────────────────────────────────────

def test_default_source_is_sheet(monkeypatch):
    monkeypatch.delenv("ROSTER_SOURCE", raising=False)
    assert rm.roster_source() == "sheet"


def test_unknown_source_fails_closed(monkeypatch):
    monkeypatch.setenv("ROSTER_SOURCE", "both")
    with pytest.raises(rm.RegistryRosterError):
        rm.roster_source()


# ── fetch / auth / fail closed ───────────────────────────────────────────

def test_missing_token_fails_without_a_request(monkeypatch, registry_env):
    monkeypatch.delenv("SV_REGISTRY_ROSTER_TOKEN")
    s = FakeSession(FakeResp(200, body()))
    with pytest.raises(rm.RegistryRosterError, match="SV_REGISTRY_ROSTER_TOKEN"):
        rm.fetch_registry_rows(s)
    assert s.calls == []


def test_bearer_token_and_url(registry_env):
    s = FakeSession(FakeResp(200, body()))
    meta, rows = rm.fetch_registry_rows(s)
    url, kw = s.calls[0]
    assert url == "https://registry.example/api/roster-projection"
    assert kw["headers"]["Authorization"] == "Bearer svt_test_not_a_real_token"
    assert len(rows) == len(ROWS) and meta["contains_no_contact_data"] is True


@pytest.mark.parametrize("resp,needle", [
    (FakeResp(401, {}), "401"),
    (FakeResp(403, {}), "read:roster-projection"),
    (FakeResp(500, {}), "HTTP 500"),
    (FakeResp(200, bad_json=True), "JSON"),
    (FakeResp(200, {"rows": []}), "not a roster projection"),
    (FakeResp(200, body(contact_flag=False)), "contact"),
    (FakeResp(200, body(FILLER + [{"name": "No Slug"}])), "slug"),
    (FakeResp(200, body(FILLER + [FILLER[0]])), "repeats slug"),
    (FakeResp(200, body(FILLER + [prow("dup-id-a", "Dup A", tier="pro", mlb_id=900099),
                                   prow("dup-id-b", "Dup B", tier="pro", mlb_id=900099)])), "two slugs"),
    (FakeResp(200, body(CORE)), "only"),   # 12 rows < 40 → truncated
])
def test_fail_closed(registry_env, resp, needle):
    with pytest.raises(rm.RegistryRosterError, match=needle):
        rm.fetch_registry_rows(FakeSession(resp))


# ── mapping ──────────────────────────────────────────────────────────────

def test_mapping_and_filtering(fake_registry):
    players = by_name(rm.get_registry_clients())
    # tier pro → Level Pro, whatever the rung
    assert players["Synthetic Pro Arm"]["level"] == "Pro"
    # slug + MLB id keyed
    assert players["Synthetic Pro Arm"]["slug"] == "synthetic-pro-arm"
    assert players["Synthetic Pro Bat"]["mlb_id"] == 900002
    # canon position → Pitcher/Hitter/Two-Way routing
    assert players["Synthetic Pro Arm"]["position"] == "Pitcher"
    assert players["Synthetic Starter"]["position"] == "Pitcher"
    assert players["Synthetic Pro Bat"]["position"] == "Hitter"
    assert players["Synthetic Two Way"]["position"] == "Two-Way"
    assert players["Synthetic No Position"]["position"] == ""
    # other sheet fields
    assert players["Synthetic Pro Bat"]["draft_class"] == "2027"
    assert players["Synthetic Pro Bat"]["roster_priority"] == 2
    assert players["Synthetic Pro Arm"]["team"] == "Synthetic Parent Club"
    assert players["Synthetic Injured"]["status"] == "Injured"
    # drafted-but-unsigned rule still applies (Org = MLB club → Pro)
    assert players["Synthetic Drafted"]["level"] == "Pro"
    # exclusions: retired (either flag), coaches, JUCO (not an included level)
    for gone in ("Synthetic Retired", "Synthetic Sheet Retired", "Synthetic Coach", "Synthetic Juco"):
        assert gone not in players
    assert all(p["is_client"] for p in players.values())


def test_registry_only_fields_never_enter_player_dicts(fake_registry):
    forbidden = {"lead_agent", "slack_channel_id", "slack_channel_name", "visits", "agency",
                 "home_state", "high_school", "bats", "throws", "ig_handle", "x_handle",
                 "nicknames", "dob", "age"}
    for p in rm.get_registry_clients():
        assert not forbidden & {k for k, v in p.items() if v}
        assert set(rm.cache_safe_player(p)) <= set(config.ROSTER_CACHE_FIELDS)


def test_peaks_absent_so_dashboard_chips_stay_hidden(fake_registry):
    p = by_name(rm.get_registry_clients())["Synthetic Pro Bat"]
    entry = main.build_pulse_entry(p, {"game_status": "N/A"},
                                   {"performance_grade": "", "social_search_url": ""})
    for key in ("peak_war", "peak_wrc_plus", "peak_era_20tbf"):
        assert key not in entry["tags"]   # js/app.js peakChipHtml + player.html renderPeak render nothing
    assert entry["tags"]["mlb_id"] == 900002


@pytest.mark.parametrize("label,role", [
    ("RHP", "Pitcher"), ("LHP", "Pitcher"), ("SP", "Pitcher"), ("RP", "Pitcher"), ("P", "Pitcher"),
    ("Pitcher", "Pitcher"), ("SS", "Hitter"), ("OF", "Hitter"), ("C", "Hitter"), ("Hitter", "Hitter"),
    ("Two-Way", "Two-Way"), ("RHP/OF", "Two-Way"), ("", ""), (None, ""),
])
def test_position_role(label, role):
    assert rm.position_role(label) == role


# ── routing: clients from registry, recruits from their own sheet ────────

def test_registry_mode_never_reads_the_client_sheet(monkeypatch, fake_registry):
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: pytest.fail("client sheet read"))
    assert len(rm.get_active_roster()) > 40


def test_recruits_stay_on_their_own_sheet(monkeypatch, fake_registry):
    monkeypatch.setattr(rm, "RECRUITS_URL", "https://recruits.example/csv")
    seen = []
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: seen.append(url) or [
        {"Player Name": "Synthetic Recruit", "Level": "HS", "Org": "Synthetic HS", "Tier": ""}])
    recruits = rm.get_recruits()
    assert seen == ["https://recruits.example/csv"]
    assert [r["player_name"] for r in recruits] == ["Synthetic Recruit"]
    assert recruits[0]["is_client"] is False


# ── get_all_players: no silent sheet fallback, fail closed ───────────────

def _write_cache(path, source, players):
    path.write_text(json.dumps({"cached_at": datetime.now(timezone.utc).isoformat(),
                                **({"source": source} if source else {}), "players": players}))


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    path = tmp_path / "roster_cache.json"
    monkeypatch.setattr(rm, "ROSTER_CACHE_PATH", str(path))
    monkeypatch.setattr(rm, "_enrich_pro_team_from_api", lambda players: None)
    monkeypatch.setattr(rm, "get_recruits", lambda url=None: [])
    return path


def test_registry_success_writes_registry_tagged_allowlisted_cache(fake_registry, isolated):
    players = rm.get_all_players()
    assert rm.roster_is_fresh() is True
    data = json.loads(isolated.read_text())
    assert data["source"] == "registry"
    assert len(data["players"]) == len(players)
    assert all(set(p) <= set(config.ROSTER_CACHE_FIELDS) for p in data["players"])


def test_registry_failure_rejects_a_sheet_written_cache(monkeypatch, registry_env, isolated):
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(503, {}))))
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: pytest.fail("sheet fallback"))
    _write_cache(isolated, None, [{"player_name": "Sheet Era Player", "level": "Pro"}])  # legacy = sheet
    assert rm.get_all_players() == []          # main.py then aborts the run (exit 1)
    assert rm.roster_is_fresh() is False


def test_registry_failure_uses_last_good_registry_cache_without_pruning(monkeypatch, registry_env, isolated):
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(403, {}))))
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: pytest.fail("sheet fallback"))
    _write_cache(isolated, "registry", [{"player_name": "Registry Era Player", "level": "Pro"}])
    players = rm.get_all_players()
    assert [p["player_name"] for p in players] == ["Registry Era Player"]
    assert rm.roster_is_fresh() is False       # destructive prunes stay off


def test_registry_implausible_shrink_fails_closed(monkeypatch, registry_env, isolated):
    monkeypatch.setattr(rm, "requests", _RequestsShim(FakeSession(FakeResp(200, body()))))
    _write_cache(isolated, "registry", [{"player_name": f"Cached {i}", "level": "NCAA"} for i in range(500)])
    players = rm.get_all_players()             # ~50 fresh vs 500 cached → implausible
    assert rm.roster_is_fresh() is False
    assert len(players) == 500                 # served from the registry cache, not accepted as fresh


# ── dual run ─────────────────────────────────────────────────────────────

def test_dual_run_diff_keys_on_mlb_id_then_name(fake_registry):
    registry = rm.get_registry_clients()
    sheet = rm.filter_roster([
        {"Player Name": "Cam Flukey", "MLB_ID": "900007", "Org": "X", "Level": "Pro", "Position": "Pitcher", "Tier": "1"},
        {"Player Name": "Synthetic Pro Bat", "MLB_ID": "900002", "Org": "Synthetic U", "Level": "Pro", "Position": "Pitcher", "Tier": "1"},
        {"Player Name": "Only On Sheet", "MLB_ID": "", "Org": "Synthetic U", "Level": "NCAA", "Position": "Hitter", "Tier": "2"},
    ])
    diff = rm.dual_run_diff(sheet, registry)
    assert "Cam Flukey → Cameron Flukey" in diff["name_differs"]
    assert diff["sheet_only"] == ["Only On Sheet"]
    assert "Synthetic Pro Bat: Pitcher → Hitter" in diff["role_differs"]
    assert "Synthetic Pro Arm" in diff["registry_only"]


def test_dual_run_is_silent_without_token_and_never_raises(monkeypatch, caplog):
    monkeypatch.delenv("SV_REGISTRY_ROSTER_TOKEN", raising=False)
    rm._log_dual_run([])                        # no token → nothing
    monkeypatch.setenv("SV_REGISTRY_ROSTER_TOKEN", "svt_test_not_a_real_token")
    monkeypatch.setattr(rm, "get_registry_clients", lambda: (_ for _ in ()).throw(rm.RegistryRosterError("down")))
    rm._log_dual_run([])                        # registry down → warning only
    assert "dual-run" in caplog.text


def test_sheet_mode_unchanged_and_runs_dual_run(monkeypatch, isolated):
    monkeypatch.delenv("ROSTER_SOURCE", raising=False)
    sheet_rows = [{"Player Name": f"Sheet Player {i:02d}", "Level": "NCAA", "Org": "U", "Tier": "2"} for i in range(30)]
    monkeypatch.setattr(rm, "fetch_roster", lambda url=None: sheet_rows)
    called = []
    monkeypatch.setattr(rm, "_log_dual_run", lambda clients: called.append(len(clients)))
    players = rm.get_all_players()
    assert len(players) == 30 and called == [30]
    assert json.loads(isolated.read_text())["source"] == "sheet"
