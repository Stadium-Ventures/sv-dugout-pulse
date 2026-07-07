"""Multi-level season aggregation — the rule under test: counting stats can
combine, rate stats never blend across levels (each level aggregates alone)."""
from src.historical_stats import MLBHistoricalFetcher


def _hitter_game(hits, ab, sport_id):
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
