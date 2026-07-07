"""Weekly-email season rows: single-level players get one row; multi-level
players get one row per level (name/grade on the top row only)."""
from scripts import monday_email as m


def _season(stats, **kw):
    base = {"player_name": "Test Guy", "team": "Testers", "games_played": 40,
            "stats": stats}
    base.update(kw)
    return {"week": None, "season": base}


HITTER_STATS = {"pa": 160, "avg": ".300", "obp": ".380", "slg": ".500",
                "ops": ".880", "hr": 8, "rbi": 25, "sb": 3,
                "bb_pct": "9.0%", "k_pct": "18.0%"}


def test_single_level_hitter_one_row():
    html = m._hitter_row(_season(HITTER_STATS), "Pro", "season")
    assert html.count("<tr") == 1
    assert "Test Guy" in html


def test_multi_level_hitter_row_per_level():
    rec = _season({}, level_splits=[
        {"level": "MLB", "games_played": 27, "stats": dict(HITTER_STATS, ops=".690")},
        {"level": "AAA", "games_played": 49, "stats": dict(HITTER_STATS, ops=".910")},
    ])
    html = m._hitter_row(rec, "Pro", "season")
    assert html.count("<tr") == 2
    assert html.count("Test Guy") == 1        # name only on the top-level row
    assert "MLB" in html and "AAA" in html


def test_week_rows_ignore_level_splits():
    # level_splits only applies to the season section, never the week rows.
    rec = {"week": _season(HITTER_STATS)["season"], "season": None}
    rec["week"]["level_splits"] = [{"level": "MLB", "games_played": 1, "stats": HITTER_STATS}]
    html = m._hitter_row(rec, "Pro", "week")
    assert html.count("<tr") == 1


def test_tier_thresholds():
    hot = m._tier_for_record({"stats": {"ops": "1.050"}}, is_pitcher=False)
    cold = m._tier_for_record({"stats": {"ops": ".400"}}, is_pitcher=False)
    assert hot == m.TIER_HOT
    assert cold == m.TIER_COLD
    assert m._tier_for_record({"stats": {"era": "1.20"}}, is_pitcher=True) == m.TIER_ELITE
