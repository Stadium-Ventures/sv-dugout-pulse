"""2026 draft-week behavior: drafted-but-unsigned players (Org = MLB club,
Level still NCAA/HS on the sheet) are treated as Pro, and "is pitching!"
alerts don't fire on games first seen after they went Final."""
from src import alerts
from src.roster_manager import normalize_player


def _sheet_row(name, org, level, **extra):
    row = {"Player Name": name, "Org": org, "Level": level, "Tier": "3"}
    row.update(extra)
    return row


def test_drafted_ncaa_player_becomes_pro():
    p = normalize_player(_sheet_row("Alex Kranzler", "Boston Red Sox", "NCAA"))
    assert p["level"] == "Pro"


def test_drafted_hs_player_becomes_pro():
    p = normalize_player(_sheet_row("Trevor Condon", "St. Louis Cardinals", "HS"))
    assert p["level"] == "Pro"


def test_ncaa_player_with_school_org_stays_ncaa():
    p = normalize_player(_sheet_row("Brooks Wright", "SE Louisiana", "NCAA"))
    assert p["level"] == "NCAA"


def test_hs_committed_to_college_stays_amateur():
    # HS grad committed to a college — Org is a school, not an MLB club.
    p = normalize_player(_sheet_row("Devin Diaz", "University of Miami", "NCAA"))
    assert p["level"] == "NCAA"


def test_existing_pro_player_untouched():
    p = normalize_player(_sheet_row("Kade Brown", "Athletics", "Pro"))
    assert p["level"] == "Pro"


def _run_pitcher_alerts(monkeypatch, game_status, extra_stats=None, pre_sent=None):
    sent = []
    monkeypatch.setattr(alerts, "send_slack_message", lambda *a, **k: sent.append(a[0]) or True)
    monkeypatch.setattr(alerts, "_check_promotion", lambda *a, **k: None)
    monkeypatch.setattr(alerts, "_save_sent_alerts", lambda: None)
    monkeypatch.setattr(alerts, "_sent_alerts", dict(pre_sent or {}))
    monkeypatch.setattr(alerts, "_loaded", True, raising=False)
    player = {"player_name": "Test Arm", "team": "Athletics",
              "roster_priority": 3, "position": "Pitcher"}
    stats = {"game_status": game_status, "game_date": "2026-07-19",
             "is_pitcher_line": True, "ip": 1.0, "stats_summary": "1.0 IP, 0 ER",
             "game_context": "A 1, B 0"}
    stats.update(extra_stats or {})
    alerts.check_and_send_alerts(player, stats)
    return sent


def _pitcher_alert_calls(monkeypatch, game_status):
    sent = _run_pitcher_alerts(monkeypatch, game_status)
    return [m for m in sent if "is pitching" in m]


def test_pitcher_entered_fires_live(monkeypatch):
    assert len(_pitcher_alert_calls(monkeypatch, "Live")) == 1


def test_pitcher_entered_skipped_on_final(monkeypatch):
    assert len(_pitcher_alert_calls(monkeypatch, "Final")) == 0


def test_pitcher_removed_fires_with_line(monkeypatch):
    entered_key = alerts._alert_key("2026-07-19", "Test Arm", "entered")
    sent = _run_pitcher_alerts(monkeypatch, "Live",
                               extra_stats={"pitcher_removed": True},
                               pre_sent={entered_key: True})
    removed = [m for m in sent if "taken out of the game" in m]
    assert len(removed) == 1
    assert "1.0 IP, 0 ER" in removed[0]


def test_pitcher_removed_skipped_without_entered_alert(monkeypatch):
    # Game never observed live before the exit — don't retro-page.
    sent = _run_pitcher_alerts(monkeypatch, "Final",
                               extra_stats={"pitcher_removed": True})
    assert not [m for m in sent if "taken out of the game" in m]
