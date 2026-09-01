"""Multi-level season aggregation — the rule under test: counting stats can
combine, rate stats never blend across levels (each level aggregates alone)."""
from src.historical_stats import MLBHistoricalFetcher


def _hitter_game(hits, ab, sport_id, team_name=""):
    f = MLBHistoricalFetcher
    return {
        "stat": {
            "hits": hits, "atBats": ab, "baseOnBalls": 0, "strikeOuts": 0,
            "homeRuns": 0, "rbi": 0, "stolenBases": 0, "doubles": 0,
            "triples": 0, "plateAppearances": ab, "hitByPitch": 0,
            "sacFlies": 0, "runs": 0,
        },
        "_sport_id": sport_id,
        "_level": f._SPORT_LEVEL[sport_id],
        "_team_name": team_name,
        "date": "2026-06-01",
    }


def test_two_levels_split_and_ordered_highest_first():
    f = MLBHistoricalFetcher()
    games = [
        _hitter_game(3, 4, 12),  # AA
        _hitter_game(2, 4, 12),  # AA
        _hitter_game(1, 4, 1),   # MLB
        _hitter_game(0, 3, 1),   # MLB
    ]
    splits = f._aggregate_by_level(games, "Hitter")
    assert [s["level"] for s in splits] == ["MLB", "AA"]
    assert [s["games_played"] for s in splits] == [2, 2]
    # Rates stay per-level: 1-for-7 MLB vs 5-for-8 AA — never one blended AVG.
    assert splits[0]["stats"]["avg"] < 0.2
    assert splits[1]["stats"]["avg"] > 0.6


def test_single_level_returns_no_splits():
    f = MLBHistoricalFetcher()
    games = [_hitter_game(2, 4, 11), _hitter_game(1, 4, 11)]
    assert f._aggregate_by_level(games, "Hitter") == []


def test_three_levels_all_present():
    f = MLBHistoricalFetcher()
    games = [_hitter_game(1, 4, 12), _hitter_game(1, 4, 13), _hitter_game(1, 4, 14)]
    splits = f._aggregate_by_level(games, "Hitter")
    assert [s["level"] for s in splits] == ["AA", "A+", "A"]


def test_complex_league_is_swept_and_ranked_last():
    # Rookie/Complex (sportId 16) must be in the season sweep — a client
    # rehabbing or starting at CPX otherwise vanishes from season totals.
    f = MLBHistoricalFetcher()
    assert 16 in f._SPORT_IDS
    games = [_hitter_game(1, 4, 14), _hitter_game(2, 4, 16)]
    splits = f._aggregate_by_level(games, "Hitter")
    assert [s["level"] for s in splits] == ["A", "CPX"]


def test_pa_includes_sac_bunts():
    # PA comes from the API's plateAppearances (includes sac bunts); the
    # OBP denominator (AB+BB+HBP+SF) does not.
    f = MLBHistoricalFetcher()
    game = _hitter_game(2, 4, 14)
    game["stat"]["plateAppearances"] = 6   # 4 AB + 1 BB + 1 sac bunt
    game["stat"]["baseOnBalls"] = 1
    stats = f._aggregate_batter_stats([game])
    assert stats["pa"] == 6
    assert abs(stats["obp"] - 3 / 5) < 1e-9   # (2 H + 1 BB) / (4 AB + 1 BB)


def test_pa_falls_back_when_api_field_missing():
    f = MLBHistoricalFetcher()
    game = _hitter_game(2, 4, 14)
    del game["stat"]["plateAppearances"]
    stats = f._aggregate_batter_stats([game])
    assert stats["pa"] == 4


def test_current_level_only_reports_verified_lookups():
    # _player_sport falls back to 1 (MLB) when the currentTeam lookup fails;
    # that guess must never surface as a current-level badge.
    f = MLBHistoricalFetcher()
    f._player_sport[123] = 11
    assert f.current_level("Cached Guy", mlb_id=123) is None   # unverified
    f._sport_verified.add(123)
    assert f.current_level("Cached Guy", mlb_id=123) == "AAA"


def test_split_carries_the_affiliate_played_for():
    # The roster only knows the parent org, so the affiliate has to come from
    # the game log or the email can't say where the stats happened.
    f = MLBHistoricalFetcher()
    games = [
        _hitter_game(1, 4, 1, "Colorado Rockies"),
        _hitter_game(2, 4, 11, "Albuquerque Isotopes"),
        _hitter_game(1, 4, 11, "Albuquerque Isotopes"),
    ]
    splits = f._aggregate_by_level(games, "Hitter")
    assert [(s["level"], s["team_name"]) for s in splits] == [
        ("MLB", "Colorado Rockies"), ("AAA", "Albuquerque Isotopes")]


def test_single_level_breakdown_available_on_request():
    # min_levels=1 is what every window uses now: even a one-level month needs
    # to say which affiliate and level it was.
    f = MLBHistoricalFetcher()
    games = [_hitter_game(2, 4, 11, "Iowa Cubs"), _hitter_game(1, 4, 11, "Iowa Cubs")]
    splits = f._aggregate_by_level(games, "Hitter", min_levels=1)
    assert [(s["level"], s["team_name"], s["games_played"]) for s in splits] == [
        ("AAA", "Iowa Cubs", 2)]


def test_mid_level_trade_lists_both_clubs():
    f = MLBHistoricalFetcher()
    games = [
        _hitter_game(1, 4, 12, "Midland RockHounds"),
        _hitter_game(1, 4, 12, "Midland RockHounds"),
        _hitter_game(1, 4, 12, "Somerset Patriots"),
    ]
    splits = f._aggregate_by_level(games, "Hitter", min_levels=1)
    assert splits[0]["team_name"] == "Midland RockHounds / Somerset Patriots"


def test_missing_team_name_is_blank_not_a_crash():
    f = MLBHistoricalFetcher()
    splits = f._aggregate_by_level([_hitter_game(1, 4, 13)], "Hitter", min_levels=1)
    assert splits[0]["team_name"] == ""
