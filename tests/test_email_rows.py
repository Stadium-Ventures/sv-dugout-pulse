"""Email rows: one row per level the window covered, each naming the affiliate
the stats were earned at, in BOTH the recent and season blocks."""
from scripts import monday_email as m


def _season(stats, **kw):
    base = {"player_name": "Test Guy", "team": "Testers", "games_played": 40,
            "stats": stats}
    base.update(kw)
    return {"week": None, "season": base}


def _week(stats, **kw):
    base = {"player_name": "Test Guy", "team": "Colorado Rockies",
            "games_played": 11, "stats": stats}
    base.update(kw)
    return {"week": base, "season": None}


HITTER_STATS = {"pa": 160, "avg": ".300", "obp": ".380", "slg": ".500",
                "ops": ".880", "hr": 8, "rbi": 25, "sb": 3,
                "bb_pct": "9.0%", "k_pct": "18.0%"}
PITCHER_STATS = {"ip": 30.0, "era": "3.10", "whip": "1.20", "k": 32, "bb": 9,
                 "k_per_9": "9.6", "bb_per_9": "2.7", "k_pct": "26.0%",
                 "bb_pct": "7.0%"}


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


def test_recent_rows_split_by_level_too():
    # A month can span two levels; the recent block must not blend them into
    # one line that belongs to neither.
    rec = _week({}, level_splits=[
        {"level": "AAA", "team_name": "Las Vegas Aviators", "games_played": 3,
         "stats": dict(HITTER_STATS, pa=11)},
        {"level": "AA", "team_name": "Midland RockHounds", "games_played": 10,
         "stats": dict(HITTER_STATS, pa=45)},
    ])
    html = m._hitter_row(rec, "Pro", "week")
    assert html.count("<tr") == 2
    assert "Las Vegas Aviators" in html and "Midland RockHounds" in html


def test_single_level_row_names_affiliate_and_level():
    rec = _week({}, level_splits=[
        {"level": "AAA", "team_name": "Albuquerque Isotopes", "games_played": 11,
         "stats": HITTER_STATS},
    ])
    html = m._hitter_row(rec, "Pro", "week")
    assert html.count("<tr") == 1
    # Affiliate is the headline, level and parent org sit under it.
    assert "Albuquerque Isotopes" in html
    assert "AAA · Colorado Rockies" in html


def test_pitcher_recent_row_names_affiliate():
    rec = _week({}, level_splits=[
        {"level": "A+", "team_name": "Wilmington Blue Rocks", "games_played": 6,
         "stats": PITCHER_STATS},
    ])
    html = m._pitcher_row(rec, "week")
    assert "Wilmington Blue Rocks" in html and "A+ · Colorado Rockies" in html


def test_mlb_row_shows_level_without_repeating_the_club():
    rec = _week({}, level_splits=[
        {"level": "MLB", "team_name": "Colorado Rockies", "games_played": 11,
         "stats": HITTER_STATS},
    ])
    html = m._hitter_row(rec, "Pro", "week")
    assert html.count("Colorado Rockies") == 1
    assert ">MLB<" in html


def test_row_without_splits_falls_back_to_the_org():
    # Windows written before per-level splits existed must still render.
    html = m._hitter_row(_week(HITTER_STATS), "Pro", "week")
    assert html.count("<tr") == 1
    assert "Colorado Rockies" in html


def test_tier_thresholds():
    hot = m._tier_for_record({"stats": dict(HITTER_STATS, ops="1.050")}, is_pitcher=False)
    cold = m._tier_for_record({"stats": dict(HITTER_STATS, ops=".400")}, is_pitcher=False)
    assert hot == m.TIER_HOT
    assert cold == m.TIER_COLD
    assert m._tier_for_record({"stats": dict(PITCHER_STATS, era="1.20")},
                              is_pitcher=True) == m.TIER_ELITE


def test_sample_floor_means_no_grade():
    # 3 PA / 0.2 IP is a cameo, not a grade: no tier, no circle on the row.
    assert m._tier_for_record({"stats": {"pa": 3, "ops": "1.500"}},
                              is_pitcher=False) == m.TIER_DNP
    assert m._tier_for_record({"stats": {"ip": 0.2, "era": "54.00"}},
                              is_pitcher=True) == m.TIER_DNP
    rec = _week({}, level_splits=[
        {"level": "AA", "team_name": "Fisher Cats", "games_played": 1,
         "stats": dict(HITTER_STATS, pa=3, ops=".667")},
        {"level": "A+", "team_name": "Rome Emperors", "games_played": 13,
         "stats": dict(HITTER_STATS, pa=52, ops=".636")},
    ])
    html = m._hitter_row(rec, "Pro", "week")
    assert html.count("grade-circle") == 1     # only the A+ line is gradeable


def test_headline_split_is_the_biggest_sample():
    w = {"player_name": "Test Guy", "team": "Athletics", "level": "Pro",
         "stats": {}, "level_splits": [
             {"level": "AAA", "team_name": "Las Vegas Aviators", "games_played": 3,
              "stats": dict(HITTER_STATS, pa=11, ops=".855")},
             {"level": "AA", "team_name": "Midland RockHounds", "games_played": 10,
              "stats": dict(HITTER_STATS, pa=45, ops="1.018")},
         ]}
    v = m._view(w, is_pitcher=False)
    assert v["split_level"] == "AA"
    assert v["split_team"] == "Midland RockHounds"
    assert v["stats"]["ops"] == "1.018"
    # ... and that is what the tier and the standout line read from.
    assert m._tier_for_record(v, is_pitcher=False) == m.TIER_HOT
    assert m._where_parts(v, "Pro") == ("AA", "Midland RockHounds, Athletics")


def test_view_falls_back_to_the_entry_without_splits():
    w = {"player_name": "Test Guy", "team": "Athletics", "stats": HITTER_STATS}
    assert m._view(w, is_pitcher=False) is w
    assert m._where_parts(w, "NCAA") == ("NCAA", "Athletics")
