"""Level-move detection → immediate season rebuild queue (promotions and
demotions shouldn't wait for the 6 AM historical pass to update Season)."""
from src import alerts


def _quiet_slack(monkeypatch, tmp_path):
    monkeypatch.setattr(alerts, "_TEAM_STATE_PATH", str(tmp_path / "levels.json"))
    monkeypatch.setattr(alerts, "send_slack_message", lambda *a, **kw: True)
    monkeypatch.setattr(alerts, "_already_sent", lambda *a, **kw: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda *a, **kw: None)
    alerts.consume_level_moves()  # clear any state from other tests


def test_promotion_queues_season_rebuild(tmp_path, monkeypatch):
    _quiet_slack(monkeypatch, tmp_path)
    player = {"mlb_id": 999}
    # First sighting just records state — no move.
    alerts._check_promotion(player, {"api_sport_id": 12, "api_team_id": 5}, "Test Guy", "Org")
    assert alerts.consume_level_moves() == []
    # AA → AAA promotion queues the rebuild.
    alerts._check_promotion(player, {"api_sport_id": 11, "api_team_id": 6}, "Test Guy", "Org")
    assert alerts.consume_level_moves() == ["Test Guy"]
    # consume clears the queue.
    assert alerts.consume_level_moves() == []


def test_lateral_trade_does_not_queue(tmp_path, monkeypatch):
    _quiet_slack(monkeypatch, tmp_path)
    player = {"mlb_id": 998}
    alerts._check_promotion(player, {"api_sport_id": 12, "api_team_id": 5}, "Trade Guy", "Org")
    # Same level, different affiliate — no season impact.
    alerts._check_promotion(player, {"api_sport_id": 12, "api_team_id": 7}, "Trade Guy", "Org")
    assert alerts.consume_level_moves() == []


def test_demotion_queues_even_if_slack_fails(tmp_path, monkeypatch):
    _quiet_slack(monkeypatch, tmp_path)
    monkeypatch.setattr(alerts, "send_slack_message", lambda *a, **kw: False)
    player = {"mlb_id": 997}
    alerts._check_promotion(player, {"api_sport_id": 1, "api_team_id": 5}, "Optioned Guy", "Org")
    alerts._check_promotion(player, {"api_sport_id": 11, "api_team_id": 6}, "Optioned Guy", "Org")
    assert alerts.consume_level_moves() == ["Optioned Guy"]
