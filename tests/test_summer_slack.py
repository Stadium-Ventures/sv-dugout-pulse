"""Daily summer Slack recap: the reachable-vs-manual league split and the
no-data placement detection."""
from scripts import summer_daily_slack as s


def _entry(name, league, status="Confirmed", game_status="N/A"):
    return {
        "player_name": name,
        "level": "Summer",
        "is_client": True,
        "game_status": game_status,
        "tags": {"placement_status": status, "summer_league": league},
    }


def test_reachable_leagues_constant():
    # The four leagues we pull automatically — keep in sync with the dashboard.
    assert s._REACHABLE_LEAGUES == {"Cape Cod", "MLB Draft", "Appalachian", "NECBL"}


def test_no_data_detects_idle_and_unreachable():
    today = [
        _entry("Idle Cape Guy", "Cape Cod"),                       # no game today
        _entry("Manual Guy", "Northwoods"),                        # unreachable
        _entry("Playing Guy", "Cape Cod", game_status="Scheduled"),  # has a game
        _entry("Hurt Guy", "Cape Cod", status="Injured"),          # excluded status
    ]
    out = s._no_data_active_placements(today, [])
    names = [n for n, _lg in out]
    assert "Idle Cape Guy" in names
    assert "Manual Guy" in names
    assert "Playing Guy" not in names
    assert "Hurt Guy" not in names
    # league comes back so the caller can split reachable vs manual
    assert ("Manual Guy", "Northwoods") in out


def test_yesterday_final_counts_as_data():
    today = [_entry("Played Yesterday", "Cape Cod")]
    yesterday = [{"player_name": "Played Yesterday", "game_status": "Final"}]
    assert s._no_data_active_placements(today, yesterday) == []


def test_shorten_season_hitter_and_pitcher():
    hitter = s._shorten_season("18-59, 0 HR, 14 RBI · .305/.387/.458 (17 G)")
    assert hitter == "17 G, .305/.387/.458, 0 HR, 14 RBI"
    pitcher = s._shorten_season("14.0 IP, 2 ER, 11 K, 3 BB · ERA 1.29 (3 G)")
    assert pitcher.startswith("3 G, 14.0 IP, 1.29 ERA")
