"""MiLB watch: baseline subtraction, lull/surge/idle gating, window choice,
the rolling board, and the locked message format."""
from scripts import milb_watch as m


def _hitter(name="Guy", pa=100, ab=90, h=27, hr=5, bb=10, k=20,
            obp=".370", slg=".480", games=25, level="AA", priority=1):
    return {
        "player_name": name,
        "team": "New York Yankees",
        "level": "Pro",
        "is_client": True,
        "current_level": level,
        "games_played": games,
        "tags": {"position": "Hitter", "roster_priority": priority},
        "stats": {
            "pa": pa, "ab": ab, "h": h, "hr": hr, "bb": bb, "k": k,
            "avg": ".300", "obp": obp, "slg": slg, "ops": ".850",
        },
    }


def _pitcher(name="Arm", ip="60", k=70, bb=20, era="3.00", whip="1.20",
             games=12, level="AA", priority=1):
    return {
        "player_name": name,
        "team": "Boston Red Sox",
        "level": "Pro",
        "is_client": True,
        "current_level": level,
        "games_played": games,
        "tags": {"position": "Pitcher", "roster_priority": priority},
        "stats": {"ip": ip, "k": k, "bb": bb, "era": era, "whip": whip},
    }


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def test_ip_notation_is_thirds_not_decimal():
    # '35.2' is 35 and 2/3 innings — 107 outs — not 35.2 innings.
    assert m._ip_to_outs("35.2") == 107
    assert m._ip_to_outs("45") == 135
    assert m._ip_to_outs("1.1") == 4
    assert m._outs_to_ip_str(107) == "35.2"
    assert m._outs_to_ip_str(135) == "45"
    assert m._ip_to_outs("--") is None


def test_num_handles_window_string_formats():
    assert m._num(".294") == 0.294
    assert m._num("21.1%") == 21.1
    assert m._num("--") is None
    assert m._num(None) is None


# ---------------------------------------------------------------------------
# Baseline subtraction
# ---------------------------------------------------------------------------

def test_hitter_baseline_is_season_minus_recent():
    season = m.hitter_line(_hitter(pa=100, ab=90, h=27, hr=5, bb=10, k=20,
                                   obp=".370", slg=".480")["stats"])
    recent = m.hitter_line(_hitter(pa=30, ab=27, h=6, hr=1, bb=3, k=9,
                                   obp=".300", slg=".333")["stats"])
    baseline = m.subtract_lines(season, recent)
    assert baseline["pa"] == 70
    assert baseline["ab"] == 63
    assert baseline["h"] == 21
    assert baseline["hr"] == 4
    # TB: season .480*90 = 43, recent .333*27 = 9 -> 34 over 63 AB
    assert baseline["tb"] == 34
    rates = m.hitter_rates(baseline)
    assert rates["slg"] == ".540"


def test_subtract_refuses_negative_baseline():
    # A 14-day line holding more events than the season line means the two
    # windows disagree — refuse rather than publish a negative baseline.
    season = m.hitter_line(_hitter(pa=30, ab=27, h=6)["stats"])
    recent = m.hitter_line(_hitter(pa=100, ab=90, h=27)["stats"])
    assert m.subtract_lines(season, recent) is None


def test_pitcher_er_reconstructed_from_era_and_innings():
    # 3.00 ERA over 60 IP = 20 ER.
    line = m.pitcher_line({"ip": "60", "k": 70, "bb": 20, "era": "3.00",
                           "whip": "1.20"})
    assert line["er"] == 20
    assert line["outs"] == 180
    assert m.pitcher_rates(line)["era"] == "3.00"


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

def test_hitter_lull_fires_on_drop_from_own_baseline():
    # Season .850 OPS over 100 PA; last 14 days .500 over 30 PA.
    season = _hitter(pa=100, ab=90, h=27, obp=".370", slg=".480")
    recent = _hitter(pa=30, ab=28, h=5, hr=0, obp=".233", slg=".250")
    v = m.evaluate(season, recent, "14d")
    assert v["status"] == "lull"
    assert v["delta"]["metric"] == "ops"
    assert v["delta"]["value"] < -m.OPS_LULL_DROP
    assert "OPS" in v["reason"] and "14 days" in v["reason"]


def test_elite_hitter_cooling_to_still_good_is_not_a_lull():
    # 1.150 -> .980 is a .170 drop but lands in Hot: not a lull.
    season = _hitter(pa=200, ab=170, h=60, obp=".450", slg=".700")
    recent = _hitter(pa=40, ab=34, h=11, obp=".400", slg=".580")
    v = m.evaluate(season, recent, "14d")
    assert v["status"] != "lull"


def test_hitter_surge_fires_when_recent_lands_solid_or_hot():
    season = _hitter(pa=200, ab=180, h=45, obp=".300", slg=".400")
    recent = _hitter(pa=50, ab=45, h=18, hr=3, obp=".440", slg=".620")
    v = m.evaluate(season, recent, "14d")
    assert v["status"] == "surge"
    assert v["delta"]["value"] > 0


def test_small_recent_sample_is_insufficient_not_a_lull():
    season = _hitter(pa=200, ab=180, h=54, obp=".370", slg=".480")
    recent = _hitter(pa=8, ab=8, h=0, obp=".000", slg=".000")
    v = m.evaluate(season, recent, "14d")
    assert v["status"] == "insufficient"
    assert "Recent sample too small" in v["reason"]


def test_recent_sample_floor_scales_with_window():
    # 30 PA clears the 14-day floor (25) but not the 30-day floor (45).
    season = _hitter(pa=200, ab=180, h=54, obp=".370", slg=".480")
    recent = _hitter(pa=30, ab=28, h=3, obp=".200", slg=".214")
    assert m.evaluate(season, recent, "14d")["status"] == "lull"
    assert m.evaluate(season, recent, "30d")["status"] == "insufficient"


def test_thin_baseline_is_insufficient():
    # A guy who just got promoted has no season baseline to compare against.
    season = _hitter(pa=45, ab=40, h=12, obp=".370", slg=".480")
    recent = _hitter(pa=30, ab=28, h=8, obp=".350", slg=".440")
    v = m.evaluate(season, recent, "14d")
    assert v["status"] == "insufficient"
    assert "Baseline sample too small" in v["reason"]


def test_pitcher_lull_and_surge():
    season = _pitcher(ip="60", k=70, bb=20, era="3.00")
    blown_up = _pitcher(ip="12", k=8, bb=9, era="7.50")
    v = m.evaluate(season, blown_up, "14d")
    assert v["status"] == "lull"
    assert v["delta"]["metric"] == "era"

    dominant = _pitcher(ip="15", k=22, bb=3, era="0.60")
    v = m.evaluate(season, dominant, "14d")
    assert v["status"] == "surge"


def test_idle_fires_only_on_the_freshest_window():
    season = _hitter(pa=300, ab=270, h=81, games=70)
    quiet = _hitter(pa=0, ab=0, h=0, games=0)
    assert m.evaluate(season, quiet, "14d")["status"] == "idle"
    # The 30-day read must not double-report the same absence.
    assert m.evaluate(season, quiet, "30d")["status"] == "insufficient"


def test_idle_needs_a_season_to_be_absent_from():
    season = _hitter(pa=20, ab=18, h=5, games=5)
    quiet = _hitter(pa=0, ab=0, h=0, games=0)
    assert m.evaluate(season, quiet, "14d")["status"] == "insufficient"


# ---------------------------------------------------------------------------
# Scope + window choice
# ---------------------------------------------------------------------------

def test_scope_is_affiliated_milb_clients_only():
    season = [
        _hitter(name="Milb Guy", level="AA"),
        _hitter(name="Big Leaguer", level="MLB"),
        dict(_hitter(name="College Guy"), level="NCAA"),
        dict(_hitter(name="Followed Only"), is_client=False),
    ]
    names = [p["player_name"] for p in m.milb_clients(season)]
    assert names == ["Milb Guy"]


def test_more_actionable_window_wins():
    # Flat over 14 days, clearly slumping over 30 — the 30-day read must win.
    season = _hitter(pa=300, ab=270, h=81, obp=".370", slg=".480")
    flat_14 = _hitter(pa=30, ab=27, h=8, obp=".367", slg=".481")
    slump_30 = _hitter(pa=70, ab=63, h=10, obp=".230", slg=".222")
    v = m.evaluate_windows(season, {"14d": flat_14, "30d": slump_30})
    assert v["status"] == "lull"
    assert v["window"] == "30d"
    # The window that lost is kept for the snapshot, not thrown away.
    assert [a["window"] for a in v["alternates"]] == ["14d"]


def test_evaluate_all_orders_lulls_first():
    season = [
        _hitter(name="Slumping", pa=200, ab=180, h=54, obp=".370", slg=".480"),
        _hitter(name="Fine", pa=200, ab=180, h=54, obp=".370", slg=".480"),
    ]
    recent = [
        _hitter(name="Slumping", pa=40, ab=36, h=4, obp=".200", slg=".194"),
        _hitter(name="Fine", pa=40, ab=36, h=12, obp=".375", slg=".472"),
    ]
    out = m.evaluate_all(season, {"14d": recent})
    assert out[0]["player_name"] == "Slumping"
    assert out[0]["status"] == "lull"


# ---------------------------------------------------------------------------
# Cadence — flag once, then update after 7 days (hitters) / 14 (pitchers)
# ---------------------------------------------------------------------------

def _v(name="Guy", status="lull", kind="hitter"):
    return {"player_name": name, "status": status, "kind": kind}


def test_every_category_can_reach_the_board():
    for status in m.ACTIONABLE_STATUSES:
        assert m.is_actionable(_v(status=status))


def test_steady_insufficient_and_il_never_show():
    for status in ("steady", "insufficient", "il"):
        assert not m.is_actionable(_v(status=status))


def test_a_new_flag_posts_the_day_it_qualifies():
    verdicts = [_v()]
    m.apply_streaks(verdicts, {}, "2026-08-16")
    assert verdicts[0]["new_today"] is True
    assert verdicts[0]["due_today"] is True


def test_a_hitter_stays_quiet_for_a_week_then_updates():
    state = {"Guy": {"status": "lull", "since": "2026-08-10",
                     "last_posted_date": "2026-08-10",
                     "last_posted_status": "lull"}}
    for day, due in (("2026-08-11", False), ("2026-08-13", False),
                     ("2026-08-16", False), ("2026-08-17", True)):
        verdicts = [_v()]
        m.apply_streaks(verdicts, state, day)
        assert verdicts[0]["due_today"] is due, day


def test_a_pitcher_waits_two_weeks():
    state = {"Arm": {"status": "lull", "since": "2026-08-01",
                     "last_posted_date": "2026-08-01",
                     "last_posted_status": "lull"}}
    for day, due in (("2026-08-08", False), ("2026-08-14", False),
                     ("2026-08-15", True)):
        verdicts = [_v(name="Arm", kind="pitcher")]
        m.apply_streaks(verdicts, state, day)
        assert verdicts[0]["due_today"] is due, day


def test_a_status_flip_is_a_new_finding_and_posts_at_once():
    # Slumping last week, trending up today: a different conversation.
    state = {"Guy": {"status": "lull", "since": "2026-08-01",
                     "last_posted_date": "2026-08-15",
                     "last_posted_status": "lull"}}
    verdicts = [_v(status="surge")]
    m.apply_streaks(verdicts, state, "2026-08-16")
    assert verdicts[0]["new_today"] is True
    assert verdicts[0]["due_today"] is True
    assert verdicts[0]["since"] == "2026-08-16"


def test_dropping_off_for_a_day_does_not_reset_the_clock():
    # He posted on the 15th, fell under the bar on the 16th, cleared it again on
    # the 17th. That is not a fresh flag — it's the repetition Kent asked us to
    # stop, so state is kept for everyone, not just the board.
    state = {"Guy": {"status": "lull", "since": "2026-08-10",
                     "last_posted_date": "2026-08-15",
                     "last_posted_status": "lull"}}
    off = [_v(status="steady")]
    m.apply_streaks(off, state, "2026-08-16")
    carried = m.build_state(off, state, set(), "2026-08-16")
    assert carried["Guy"]["last_posted_date"] == "2026-08-15"

    back = [_v()]
    m.apply_streaks(back, carried, "2026-08-17")
    assert back[0]["due_today"] is False


def test_a_long_slump_still_gets_its_weekly_update():
    # Keyed off the last post, not off when the finding started, so a month-long
    # lull doesn't go silent forever.
    state = {"Guy": {"status": "lull", "since": "2026-07-01",
                     "last_posted_date": "2026-08-08",
                     "last_posted_status": "lull"}}
    verdicts = [_v()]
    m.apply_streaks(verdicts, state, "2026-08-16")
    assert verdicts[0]["days_standing"] > 30
    assert verdicts[0]["due_today"] is True


def test_never_posted_is_due_even_without_a_flip():
    # Seen yesterday but suppressed for some other reason: he still owes a post.
    state = {"Guy": {"status": "lull", "since": "2026-08-15",
                     "last_posted_date": None}}
    verdicts = [_v()]
    m.apply_streaks(verdicts, state, "2026-08-16")
    assert verdicts[0]["due_today"] is True


def test_non_actionable_is_never_due():
    verdicts = [_v(status="steady")]
    m.apply_streaks(verdicts, {}, "2026-08-16")
    assert verdicts[0]["due_today"] is False


def test_build_state_stamps_only_players_that_posted():
    verdicts = [_v(name="Posted"), _v(name="Held")]
    m.apply_streaks(verdicts, {}, "2026-08-16")
    state = m.build_state(verdicts, {}, {"Posted"}, "2026-08-16")
    assert state["Posted"]["last_posted_date"] == "2026-08-16"
    assert state["Held"]["last_posted_date"] is None
    # Everyone is carried, board or not.
    assert set(state) == {"Posted", "Held"}


# ---------------------------------------------------------------------------
# Closeouts — "an update 1 week after" fires even if he's back to normal
# ---------------------------------------------------------------------------
#
# BE, 2026-08-18, looking at real players: Kellon Lindsey and Jake Munroe had
# both cleared their bars before their re-report windows closed. `is_due` alone
# gates on `is_actionable`, so a resolved player would never post again —
# exactly the gap these cover.

def test_resolution_owed_once_the_window_elapses():
    prior = {"last_posted_status": "lull", "last_posted_date": "2026-08-10"}
    steady = _v(status="steady")  # hitter: 7-day window
    assert m.resolution_due(steady, prior, "2026-08-16") is False
    assert m.resolution_due(steady, prior, "2026-08-17") is True


def test_resolution_respects_the_pitcher_window():
    prior = {"last_posted_status": "lull", "last_posted_date": "2026-08-01"}
    steady = _v(status="steady", kind="pitcher")
    assert m.resolution_due(steady, prior, "2026-08-14") is False
    assert m.resolution_due(steady, prior, "2026-08-15") is True


def test_resolution_never_owed_if_still_actionable():
    # Still a lull: that's a continuing-concern update via is_due, not a
    # closeout — resolution_due must not also fire for the same player.
    prior = {"last_posted_status": "lull", "last_posted_date": "2026-08-10"}
    assert m.resolution_due(_v(status="lull"), prior, "2026-08-20") is False


def test_resolution_never_owed_for_the_il():
    # IL absences have their own explanation and their own footnote line — not
    # a "back to normal" closeout.
    prior = {"last_posted_status": "idle", "last_posted_date": "2026-08-01"}
    assert m.resolution_due(_v(status="il"), prior, "2026-08-20") is False


def test_resolution_never_owed_without_a_real_prior_flag():
    # Never posted, or the prior post was itself a closeout ("steady") — either
    # way there is nothing to close.
    never_posted = {"last_posted_status": None, "last_posted_date": None}
    assert m.resolution_due(_v(status="steady"), never_posted, "2026-08-20") is False
    already_closed = {"last_posted_status": "steady", "last_posted_date": "2026-08-10"}
    assert m.resolution_due(_v(status="steady"), already_closed, "2026-08-20") is False


def test_apply_resolutions_marks_the_verdict_and_leaves_actionable_ones_alone():
    state = {
        "Resolved": {"last_posted_status": "lull", "last_posted_date": "2026-08-10"},
        "Still Down": {"last_posted_status": "lull", "last_posted_date": "2026-08-10"},
    }
    verdicts = [_v(name="Resolved", status="steady"), _v(name="Still Down", status="lull")]
    m.apply_streaks(verdicts, state, "2026-08-17")
    m.apply_resolutions(verdicts, state, "2026-08-17")
    resolved, still_down = verdicts
    assert resolved["due_today"] is True
    assert resolved["resolution"] == {"from_status": "lull", "from_date": "2026-08-10"}
    # The real grade is left alone — no fifth status invented.
    assert resolved["status"] == "steady"
    # Already due for a normal reason; apply_resolutions must not overwrite that.
    assert still_down["due_today"] is True
    assert "resolution" not in still_down


def test_a_closed_out_player_reads_as_brand_new_if_he_relapses():
    # After the closeout, last_posted_status becomes his real grade ("steady"),
    # which isn't in any ACTIONABLE family — so re-qualifying afterward is a
    # fresh flag, not a continuation, and can't loop back into another closeout.
    closed_out = {"status": "steady", "last_posted_status": "steady",
                 "last_posted_date": "2026-08-17"}
    relapse = _v(status="lull")
    m.apply_streaks([relapse], {"Guy": closed_out}, "2026-08-20")
    assert relapse["new_today"] is True
    assert relapse["due_today"] is True
    assert m.resolution_due(_v(status="steady"), closed_out, "2026-08-31") is False


def test_end_to_end_kellon_lindsey_gets_his_closeout():
    # The real case: flagged lull 08-15, resolved by the time his window (7d,
    # hitter) closes on 08-22.
    state = {"Kellon Lindsey": {"last_posted_status": "lull",
                                "last_posted_date": "2026-08-15"}}
    verdicts = [_v(name="Kellon Lindsey", status="steady")]
    m.apply_streaks(verdicts, state, "2026-08-22")
    m.apply_resolutions(verdicts, state, "2026-08-22")
    assert verdicts[0]["due_today"] is True
    assert verdicts[0]["resolution"]["from_date"] == "2026-08-15"
    # One day earlier, it isn't owed yet.
    early = [_v(name="Kellon Lindsey", status="steady")]
    m.apply_streaks(early, state, "2026-08-21")
    m.apply_resolutions(early, state, "2026-08-21")
    assert early[0]["due_today"] is False


# ---------------------------------------------------------------------------
# Slack copy
# ---------------------------------------------------------------------------

def test_slack_text_groups_by_status_and_names_the_call():
    alerts = [
        {"player_name": "Slumping", "team": "New York Yankees",
         "current_level": "AA", "status": "lull", "reason": "OPS .800 → .560",
         "detail": "3-for-21, 0 HR"},
        {"player_name": "Rising", "team": "Oakland Athletics",
         "current_level": "AA", "status": "surge", "reason": "OPS .764 → .929",
         "detail": "19-for-49, 0 HR"},
    ]
    text = m.build_slack_text(alerts, tracked=33)
    # Up arrow is green (📈), not the red triangle it started as.
    assert "🔻" in text and "📈" in text
    assert "🔺" not in text
    # Team shortens to the nickname, as the social-search URLs do.
    assert "*Slumping*  ·  Yankees  ·  AA" in text
    assert "*Rising*  ·  Athletics  ·  AA" in text
    assert "33 MiLB clients tracked" in text
    assert "*Lull* — form below season baseline" in text
    # Lull section comes before the surge section.
    assert text.index("Slumping") < text.index("Rising")


def test_slack_text_indents_with_blockquotes_not_spaces():
    # Slack strips leading whitespace, so `>` is the only indent that renders.
    alerts = [{"player_name": "Guy", "team": "New York Yankees",
               "current_level": "AA", "status": "lull",
               "reason": "OPS .800 → .560", "detail": "3-for-21, 0 HR"}]
    text = m.build_slack_text(alerts, tracked=33)
    assert "> OPS .800 → .560" in text
    assert "> 3-for-21, 0 HR" in text
    assert "\n    " not in text
    # Blank line between the name line and the section above it.
    assert "\n\n*Guy*" in text


def test_no_advisory_subtext_only_the_logic():
    # Headings name the rule that fired; bodies are numbers. No "worth a call".
    alerts = [
        {"player_name": f"Guy {i}", "team": "New York Yankees",
         "current_level": "AA", "status": "lull", "reason": "OPS .800 → .560"}
        for i in range(4)
    ]
    text = m.build_slack_text(alerts, tracked=33).lower()
    for editorial in ("worth a", "check-in", "make the call", "the tell",
                      "front office looking"):
        assert editorial not in text


def test_suppressed_il_players_get_a_footnote():
    # An empty no-games section otherwise looks like the check didn't run.
    alerts = [{"player_name": "Guy", "team": "New York Yankees",
               "current_level": "AA", "status": "lull", "reason": "OPS .800 → .560"}]
    suppressed = [{"player_name": "Hurt Guy", "team": "Colorado Rockies",
                   "current_level": "AAA", "status": "il",
                   "il": {"description": "Injured 7-Day", "since": "2026-06-22"}}]
    text = m.build_slack_text(alerts, tracked=33, suppressed=suppressed)
    assert "Not shown — on the IL: Hurt Guy (Rockies, AAA since 06/22)." in text


def test_ops_deltas_use_baseball_formatting():
    season = _hitter(pa=200, ab=180, h=54, obp=".370", slg=".480")
    recent = _hitter(pa=40, ab=36, h=4, obp=".200", slg=".194")
    v = m.evaluate(season, recent, "14d")
    # -.376, not -0.376.
    assert "(-." in v["reason"]
    assert "(-0." not in v["reason"]


# ---------------------------------------------------------------------------
# Usage read 1 — role: PA per game played vs his own baseline
# ---------------------------------------------------------------------------

def test_role_read_catches_a_starter_who_stopped_starting():
    # 4.3 PA/G on the season, 1.5 over the window: he's dressing, not starting.
    season = _hitter(pa=400, ab=360, h=108, games=93)
    window = _hitter(pa=9, ab=8, h=2, games=6)
    r = m.role_signal(season, window, "14d", "hitter")
    # Baseline is season minus the window: (400-9) PA over (93-6) G.
    assert r["baseline_pa_per_g"] == 4.49
    assert r["recent_pa_per_g"] == 1.5
    assert r["dropped"] is True
    assert "not starting" in r["summary"]


def test_role_read_is_flat_for_a_man_who_just_had_days_off():
    # Munroe, 2026-08-14: fewer games, but a full starter's complement in each.
    # Days off, not a bench role.
    season = _hitter(pa=401, ab=360, h=108, games=93)
    window = _hitter(pa=13, ab=12, h=4, games=3)
    r = m.role_signal(season, window, "14d", "hitter")
    assert r["dropped"] is False


def test_role_read_needs_a_baseline_long_enough_to_be_a_role():
    season = _hitter(pa=40, ab=36, h=10, games=10)
    window = _hitter(pa=4, ab=4, h=1, games=4)
    assert m.role_signal(season, window, "14d", "hitter") is None


def test_role_read_needs_a_few_games_to_judge():
    season = _hitter(pa=400, ab=360, h=108, games=93)
    window = _hitter(pa=1, ab=1, h=0, games=1)
    assert m.role_signal(season, window, "14d", "hitter") is None


def test_role_read_skips_pitchers():
    season = _pitcher(ip="60", k=70, bb=20, era="3.00", games=12)
    window = _pitcher(ip="3", k=3, bb=1, era="3.00", games=1)
    assert m.role_signal(season, window, "14d", "pitcher") is None


def test_role_drop_promotes_a_steady_verdict():
    season = _hitter(pa=400, ab=360, h=108, obp=".370", slg=".480", games=93)
    recent_14 = _hitter(pa=9, ab=8, h=2, obp=".360", slg=".470", games=6)
    recent_30 = _hitter(pa=30, ab=27, h=8, obp=".365", slg=".475", games=14)
    v = m.evaluate_windows(season, {"14d": recent_14, "30d": recent_30})
    assert v["status"] == "usage_lull"
    assert "per game" in v["reason"]


# ---------------------------------------------------------------------------
# Usage read 2 — share: games played out of his team's games
# ---------------------------------------------------------------------------

def test_share_read_divides_the_schedule_out():
    s = m.share_signal(3, 11, 10, 12)
    assert s["prior_pct"] == 83 and s["recent_pct"] == 27
    assert s["dropped"] is True
    assert "In the lineup for 3 of his team's last 11 games" in s["summary"]


def test_a_shorter_team_week_is_not_a_benching():
    # Team played 12 then 10; he played 10 then 9. Nearly everything, both times.
    assert m.share_signal(9, 10, 10, 12)["dropped"] is False


def test_share_read_needs_enough_team_games():
    assert m.share_signal(1, 4, 5, 6) is None


def test_share_precheck_skips_the_api_when_he_played_throughout():
    # 11 G in 14 days against 12 in the prior 16 — nothing for the schedule to
    # explain, so no lookup.
    recent_14 = _hitter(pa=48, ab=44, h=13, games=11)
    recent_30 = _hitter(pa=100, ab=92, h=27, games=23)
    assert m.share_precheck(recent_14, recent_30) is None


def test_share_precheck_returns_counts_when_games_fell():
    recent_14 = _hitter(pa=13, ab=12, h=4, games=3)
    recent_30 = _hitter(pa=60, ab=55, h=16, games=14)
    assert m.share_precheck(recent_14, recent_30) == (3, 11)


# ---------------------------------------------------------------------------
# Roster context — IL, org changes, and the lineup-share lookup
# ---------------------------------------------------------------------------

def _windows(name, games_14, games_30):
    return {
        "14d": {name: _hitter(name=name, pa=games_14 * 4, ab=games_14 * 4,
                              h=games_14, games=games_14)},
        "30d": {name: _hitter(name=name, pa=games_30 * 4, ab=games_30 * 4,
                              h=games_30, games=games_30)},
    }


def _roster(team_id=1, team_name="Some Club", stint="2026-04-01", unavailable=None):
    return {"team_id": team_id, "team_name": team_name, "stint_start": stint,
            "unavailable": unavailable}


def test_il_players_drop_off_the_no_games_list():
    verdicts = [
        {"player_name": "Hurt Guy", "status": "idle", "kind": "hitter",
         "reason": "No games in the last 14 days (92 G on the season)"},
        {"player_name": "Healthy Guy", "status": "idle", "kind": "hitter",
         "reason": "No games in the last 14 days (40 G on the season)"},
    ]
    il = {"code": "D7", "description": "Injured 7-Day", "since": "2026-06-22"}
    m.apply_roster_context(
        verdicts, {"Hurt Guy": 1, "Healthy Guy": 2}, {}, "2026-08-14",
        lookup=lambda mlb_id: _roster(unavailable=il if mlb_id == 1 else None),
    )
    assert verdicts[0]["status"] == "il"
    assert "Injured 7-Day since 2026-06-22" in verdicts[0]["reason"]
    assert verdicts[1]["status"] == "idle"
    assert not m.is_actionable(verdicts[0])


def test_an_org_change_inside_the_window_voids_the_share_read():
    # Cade Doughty, 2026-08-14: released 08-04, signed with Atlanta 08-10. Rome
    # played 11 games in the window; he was on the club for four of them, so
    # "3 of 11" is the wrong denominator, not a benching (BE flagged it).
    verdicts = [{"player_name": "Doughty", "status": "insufficient",
                 "kind": "hitter", "reason": "Recent sample too small"}]
    m.apply_roster_context(
        verdicts, {"Doughty": 1}, _windows("Doughty", 3, 14), "2026-08-14",
        lookup=lambda _id: _roster(team_name="Rome Emperors", stint="2026-08-10"),
        team_games=lambda *a: 11,
    )
    assert verdicts[0]["status"] == "insufficient"
    assert "usage_share" not in verdicts[0]
    assert "joined Rome Emperors on 2026-08-10" in verdicts[0]["share_check"]


def test_a_settled_player_gets_a_real_share_read():
    verdicts = [{"player_name": "Sat Down", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740 (-.010) in the last 14 days"}]
    m.apply_roster_context(
        verdicts, {"Sat Down": 1}, _windows("Sat Down", 3, 14), "2026-08-14",
        lookup=lambda _id: _roster(stint="2026-04-01"),
        team_games=lambda _tid, start, end: 11 if end == "2026-08-14" else 12,
        il_history=lambda *a: None,
    )
    assert verdicts[0]["status"] == "usage_lull"
    assert verdicts[0]["usage_share"]["dropped"] is True
    assert "In the lineup for" in verdicts[0]["reason"]


def test_no_share_lookup_when_his_games_held_up():
    # Precheck short-circuits before any API call — the lookup would raise.
    def boom(_mlb_id):
        raise AssertionError("should not have been called")

    verdicts = [{"player_name": "Everyday", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(verdicts, {"Everyday": 1},
                           _windows("Everyday", 11, 23), "2026-08-14",
                           lookup=boom)
    assert verdicts[0]["status"] == "steady"


def test_failed_lookup_keeps_the_finding_and_says_so():
    def boom(_mlb_id):
        raise RuntimeError("MLB API 503")

    verdicts = [{"player_name": "Unknown Guy", "status": "idle", "kind": "hitter",
                 "reason": "No games in the last 14 days (40 G on the season)"}]
    m.apply_roster_context(verdicts, {"Unknown Guy": 1}, {}, "2026-08-14",
                           lookup=boom)
    assert verdicts[0]["status"] == "idle"
    assert verdicts[0]["roster_check"] == "lookup failed"


def test_failed_schedule_lookup_leaves_the_verdict_alone():
    def boom(*_args):
        raise RuntimeError("schedule 500")

    verdicts = [{"player_name": "Sat Down", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(verdicts, {"Sat Down": 1},
                           _windows("Sat Down", 3, 14), "2026-08-14",
                           lookup=lambda _id: _roster(), team_games=boom,
                           il_history=lambda *a: None)
    assert verdicts[0]["status"] == "steady"
    assert verdicts[0]["share_check"] == "schedule lookup failed"


def test_missing_mlb_id_is_recorded_not_guessed():
    verdicts = [{"player_name": "No ID", "status": "idle", "kind": "hitter",
                 "reason": "No games"}]
    m.apply_roster_context(verdicts, {}, {}, "2026-08-14",
                           lookup=lambda _id: _roster())
    assert verdicts[0]["status"] == "idle"
    assert verdicts[0]["roster_check"] == "no mlb_id in roster cache"


def test_a_player_just_off_the_il_reads_as_il_not_a_bare_idle():
    # The real case: Sterlin Thompson, 2026-08-19. Placed 07-31, activated
    # 08-18 — his trailing 14-day window is empty because he was hurt for
    # nearly all of it, not because of a benching or a form issue. His CURRENT
    # roster status is "Active" (unavailable=None), so only the transactions
    # history catches this.
    verdicts = [{"player_name": "Sterlin Thompson", "status": "idle",
                 "kind": "hitter", "reason": "No games in the last 14 days (92 G)"}]
    m.apply_roster_context(
        verdicts, {"Sterlin Thompson": 694514}, {}, "2026-08-19",
        lookup=lambda _id: _roster(team_id=342, team_name="Albuquerque Isotopes"),
        il_history=lambda team_id, mlb_id, today: {"placed": "2026-07-31",
                                                    "activated": "2026-08-18"},
    )
    assert verdicts[0]["status"] == "il"
    assert verdicts[0]["il"]["since"] == "2026-08-18"
    assert "Activated from the injured list 2026-08-18" in verdicts[0]["reason"]
    # Never posts, same as any other IL exclusion.
    assert not m.is_actionable(verdicts[0])


def test_an_old_activation_does_not_explain_a_fresh_idle():
    # He came off the IL three weeks ago and STILL hasn't played — that is a
    # real question worth asking, not something the old stint explains away.
    verdicts = [{"player_name": "Guy", "status": "idle", "kind": "hitter",
                 "reason": "No games in the last 14 days (50 G)"}]
    m.apply_roster_context(
        verdicts, {"Guy": 1}, {}, "2026-08-19",
        lookup=lambda _id: _roster(),
        il_history=lambda *a: {"placed": "2026-07-01", "activated": "2026-07-20"},
    )
    assert verdicts[0]["status"] == "idle"


def test_still_on_the_il_is_caught_by_the_ordinary_check_not_this_one():
    # An ONGOING stint is already handled by the current-status branch — the
    # transactions history is only consulted when the current snapshot shows
    # him active. Confirms the two paths don't double up or fight.
    called = []
    verdicts = [{"player_name": "Hurt Guy", "status": "idle", "kind": "hitter",
                 "reason": "No games in the last 14 days (50 G)"}]
    m.apply_roster_context(
        verdicts, {"Hurt Guy": 1}, {}, "2026-08-19",
        lookup=lambda _id: _roster(unavailable={"code": "D7",
                                                "description": "Injured 7-Day",
                                                "since": "2026-08-10"}),
        il_history=lambda *a: called.append(a) or None,
    )
    assert verdicts[0]["status"] == "il"
    assert verdicts[0]["il"]["since"] == "2026-08-10"
    assert called == []  # never even consulted


def test_no_recent_il_stint_leaves_the_idle_finding_alone():
    verdicts = [{"player_name": "Guy", "status": "idle", "kind": "hitter",
                 "reason": "No games in the last 14 days (50 G)"}]
    m.apply_roster_context(
        verdicts, {"Guy": 1}, {}, "2026-08-19",
        lookup=lambda _id: _roster(), il_history=lambda *a: None,
    )
    assert verdicts[0]["status"] == "idle"


def test_a_still_open_stint_from_the_history_lookup_does_not_void_idle():
    # activated=None means the transactions history itself shows no closeout —
    # nothing to explain the absence with, so this must not fire.
    verdicts = [{"player_name": "Guy", "status": "idle", "kind": "hitter",
                 "reason": "No games in the last 14 days (50 G)"}]
    m.apply_roster_context(
        verdicts, {"Guy": 1}, {}, "2026-08-19",
        lookup=lambda _id: _roster(),
        il_history=lambda *a: {"placed": "2026-08-10", "activated": None},
    )
    assert verdicts[0]["status"] == "idle"


def test_il_history_lookup_failure_leaves_the_idle_finding_standing():
    def boom(*_args):
        raise RuntimeError("transactions 500")

    verdicts = [{"player_name": "Guy", "status": "idle", "kind": "hitter",
                 "reason": "No games in the last 14 days (50 G)"}]
    m.apply_roster_context(verdicts, {"Guy": 1}, {}, "2026-08-19",
                           lookup=lambda _id: _roster(), il_history=boom)
    assert verdicts[0]["status"] == "idle"
    assert verdicts[0]["roster_check"] == "IL-history lookup failed"


def test_il_history_only_consulted_for_idle_not_usage_lull():
    # A usage lull already has sample floors guarding it — this check is
    # narrowly scoped to the one finding (idle) that has none.
    called = []
    verdicts = [{"player_name": "Guy", "status": "usage_lull", "kind": "hitter",
                 "reason": "Playing time down", "recent": {"pa": 5},
                 "detail": "some detail"}]
    m.apply_roster_context(
        verdicts, {"Guy": 1}, {}, "2026-08-19",
        lookup=lambda _id: _roster(),
        il_history=lambda *a: called.append(a) or None,
    )
    assert called == []


def test_unavailable_codes_and_keywords_both_recognized():
    assert "D7" in m._UNAVAILABLE_STATUS_CODES
    assert "D60" in m._UNAVAILABLE_STATUS_CODES
    # "Reassigned to Minors" is a roster move, not an absence — must NOT match.
    assert not any(w in "reassigned to minors" for w in m._UNAVAILABLE_KEYWORDS)
    assert "RM" not in m._UNAVAILABLE_STATUS_CODES
    assert any(w in "injured 7-day" for w in m._UNAVAILABLE_KEYWORDS)
    # Development List (Aaron Watson, 2026-08-25): the org parked him on
    # purpose, so a 14-day absence explains itself (Kent, 2026-09-02).
    assert "DEV" in m._UNAVAILABLE_STATUS_CODES
    assert any(w in "development list" for w in m._UNAVAILABLE_KEYWORDS)


def test_development_list_reads_as_unavailable():
    """A `DEV` roster entry resolves to unavailable, like an IL stint."""
    people = {"people": [{"rosterEntries": [
        {"status": {"code": "DEV", "description": "Development List"},
         "team": {"id": 450, "name": "Daytona Tortugas"},
         "startDate": "2026-07-29", "endDate": None, "isActive": True},
    ]}]}
    import types, sys
    fake = types.SimpleNamespace(get=lambda *a, **k: people)
    sys.modules["statsapi"] = fake
    try:
        snap = m.lookup_roster(826209)
    finally:
        del sys.modules["statsapi"]
    assert snap["unavailable"] == {
        "code": "DEV", "description": "Development List",
        "since": "2026-07-29", "team": "Daytona Tortugas",
    }


# ---------------------------------------------------------------------------
# Locked format
# ---------------------------------------------------------------------------

# The approved #dugout-pulse layout, pinned byte-for-byte (BE, 2026-08-14:
# "this exact format needs to be locked"). If you are here because this test
# failed, the message format changed — that is what this test is for. Either
# revert the change, or update this block deliberately and say why in the commit.
# Do not "tidy" the copy to make the test pass.
#
# Deliberate updates:
#   2026-08-14 — footer cadence line, when the 10-day cooldown was removed and
#                every category went to a rolling board (BE).
#
# Deliberate updates:
#   2026-08-14 — footer cadence line, when the cooldown was removed and every
#                category went to a rolling board (BE).
#   2026-08-16 — footer cadence line again, when the rolling board became
#                flag-once-then-update-weekly (Kent: "space out the repetitive
#                player updates"; hitters 7d, pitchers 14d).
#
# Deliberate updates so far:
#   2026-08-14 — footer cadence line, when trending-up stopped being subject to
#                the cooldown and started showing every day (BE).
#   2026-08-18 — added the ✅ Back to normal closeout section. "An update 1
#                week after" wasn't conditional on still qualifying, and the
#                code as it stood silently dropped anyone who resolved before
#                his window closed (BE, real case: Kellon Lindsey). New section
#                only; every other line is byte-identical.
_LOCKED = """*MiLB watch* — recent form vs. season baseline
_33 MiLB clients tracked · 5 findings_

🔻 *Lull* — form below season baseline

*Blake Rambusch*  ·  Mariners  ·  AA
> OPS .723 → .374 (-.349) over 26 PA in the last 14 days
> 3-for-21, 0 HR

*Ryan DeSanto*  ·  Guardians  ·  A
> ERA 3.19 → 5.62 (+2.43) over 8 IP in the last 14 days
> 10 K / 7 BB, 5 ER · BB/9 up 4.5 → 7.9

⏳ *Usage down* — playing time cut 40%+

*Cade Doughty*  ·  Braves  ·  A+
> Appearances down 51% — 22 PA in the prior 16 days (7 G) → 12 PA in the last 14 (3 G)
> Only 12 PA in the last 14 days — too thin for a rate read

📈 *Trending up* — form above season baseline

*Justin Riemer*  ·  Athletics  ·  AA
> OPS .764 → .929 (+.165) over 60 PA in the last 30 days
> 19-for-49, 0 HR

✅ *Back to normal* — re-report window closed out

*Kellon Lindsey*  ·  Dodgers  ·  A
> Flagged a lull 08/15 — now: OPS 1.011 → 1.020 (+.009) in the last 14 days
> 9-for-31, 0 HR

_Not shown — on the IL: Sterlin Thompson (Rockies, AAA since 06/22)._

_Baseline = season to date minus the window being compared._
_14- and 30-day form both checked · flagged once, then updated after 7d for hitters / 14d for pitchers._"""


def test_locked_message_format():
    alerts = [
        {"player_name": "Blake Rambusch", "team": "Seattle Mariners",
         "current_level": "AA", "status": "lull",
         "reason": "OPS .723 → .374 (-.349) over 26 PA in the last 14 days",
         "detail": "3-for-21, 0 HR"},
        {"player_name": "Ryan DeSanto", "team": "Cleveland Guardians",
         "current_level": "A", "status": "lull",
         "reason": "ERA 3.19 → 5.62 (+2.43) over 8 IP in the last 14 days",
         "detail": "10 K / 7 BB, 5 ER · BB/9 up 4.5 → 7.9"},
        {"player_name": "Cade Doughty", "team": "Atlanta Braves",
         "current_level": "A+", "status": "usage_lull",
         "reason": ("Appearances down 51% — 22 PA in the prior 16 days (7 G) → "
                    "12 PA in the last 14 (3 G)"),
         "detail": "Only 12 PA in the last 14 days — too thin for a rate read"},
        {"player_name": "Justin Riemer", "team": "Athletics",
         "current_level": "AA", "status": "surge",
         "reason": "OPS .764 → .929 (+.165) over 60 PA in the last 30 days",
         "detail": "19-for-49, 0 HR"},
        {"player_name": "Kellon Lindsey", "team": "Los Angeles Dodgers",
         "current_level": "A", "status": "steady",
         "reason": "OPS 1.011 → 1.020 (+.009) in the last 14 days",
         "detail": "9-for-31, 0 HR",
         "resolution": {"from_status": "lull", "from_date": "2026-08-15"}},
    ]
    suppressed = [
        {"player_name": "Sterlin Thompson", "team": "Colorado Rockies",
         "current_level": "AAA", "status": "il",
         "il": {"description": "Injured 7-Day", "since": "2026-06-22"}},
    ]
    assert m.build_slack_text(alerts, tracked=33, suppressed=suppressed) == _LOCKED


def test_closeout_section_omitted_when_nobody_resolved():
    alerts = [
        {"player_name": "Blake Rambusch", "team": "Seattle Mariners",
         "current_level": "AA", "status": "lull",
         "reason": "OPS .723 → .374 (-.349) over 26 PA in the last 14 days",
         "detail": "3-for-21, 0 HR"},
    ]
    assert "Back to normal" not in m.build_slack_text(alerts, tracked=33)


def test_closeout_only_message_still_renders():
    # A morning where nobody is a live concern or opportunity, but someone's
    # closeout is due, still produces a real post — not "nothing to send".
    alerts = [
        {"player_name": "Kellon Lindsey", "team": "Los Angeles Dodgers",
         "current_level": "A", "status": "steady",
         "reason": "OPS 1.011 → 1.020 (+.009) in the last 14 days",
         "detail": "9-for-31, 0 HR",
         "resolution": {"from_status": "lull", "from_date": "2026-08-15"}},
    ]
    text = m.build_slack_text(alerts, tracked=33)
    assert "✅ *Back to normal*" in text
    assert "Flagged a lull 08/15 — now:" in text
    assert "🔻" not in text and "📈" not in text


def test_locked_format_survives_an_empty_section():
    # No usage findings today: the section disappears entirely rather than
    # printing an empty heading.
    alerts = [
        {"player_name": "Blake Rambusch", "team": "Seattle Mariners",
         "current_level": "AA", "status": "lull",
         "reason": "OPS .723 → .374 (-.349) over 26 PA in the last 14 days",
         "detail": "3-for-21, 0 HR"},
    ]
    text = m.build_slack_text(alerts, tracked=33)
    assert "Usage down" not in text
    assert "Trending up" not in text
    assert text.endswith(
        "_14- and 30-day form both checked · flagged once, then updated after "
        "7d for hitters / 14d for pitchers._"
    )
    # No stray blank line pile-up where sections were dropped.
    assert "\n\n\n" not in text


def test_share_read_refuses_an_impossible_denominator():
    # A player cannot appear in more games than his club played. When he does,
    # the denominator is wrong — a mid-window level move, a doubleheader counted
    # once, a schedule gap. On 2026-08-17 this shipped "down from 14 of 13
    # (108%)" to #dugout-pulse before the guard existed.
    assert m.share_signal(8, 12, 14, 13) is None
    assert m.share_signal(13, 12, 10, 12) is None
    # The same shapes with a coherent denominator still read fine.
    assert m.share_signal(8, 12, 13, 13)["dropped"] is True


def test_share_spans_match_the_engine_window_starts():
    # src/historical_stats.py builds 14d from today-14 and 30d from today-30, so
    # the prior stretch is today-30..today-15. Drift here compares a player
    # against the wrong slice of his club's schedule.
    assert m._RECENT_DAYS == 14
    assert m._PRIOR_DAYS == 16
    seen = []
    verdicts = [{"player_name": "Guy", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(
        verdicts, {"Guy": 1}, _windows("Guy", 3, 14), "2026-08-17",
        lookup=lambda _id: _roster(stint="2026-04-01"),
        team_games=lambda _tid, start, end: seen.append((start, end)) or 12,
        il_history=lambda *a: None,
    )
    assert seen == [("2026-08-03", "2026-08-17"), ("2026-07-18", "2026-08-02")]


def test_a_move_within_the_concern_family_is_not_a_new_flag():
    # Munroe, 2026-08-17: posted as a usage lull off a broken denominator; once
    # that was fixed he read as a rate lull. Same conversation, so he must not
    # re-post the next morning.
    state = {"Guy": {"status": "usage_lull", "since": "2026-08-17",
                     "last_posted_date": "2026-08-17",
                     "last_posted_status": "usage_lull"}}
    verdicts = [_v(status="lull")]
    m.apply_streaks(verdicts, state, "2026-08-18")
    assert verdicts[0]["due_today"] is False
    assert verdicts[0]["new_today"] is False


def test_concern_to_opportunity_still_posts_at_once():
    state = {"Guy": {"status": "lull", "since": "2026-08-10",
                     "last_posted_date": "2026-08-17",
                     "last_posted_status": "lull"}}
    verdicts = [_v(status="surge")]
    m.apply_streaks(verdicts, state, "2026-08-18")
    assert verdicts[0]["due_today"] is True


def test_status_families():
    assert m.status_family("lull") == m.status_family("usage_lull") == "concern"
    assert m.status_family("idle") == "concern"
    assert m.status_family("surge") == "opportunity"


# ---------------------------------------------------------------------------
# Freshness guard + the stint span the share read needs
# ---------------------------------------------------------------------------

from datetime import datetime, timezone


def _entry(stamp):
    return {"player_name": "Guy", "last_updated": stamp}


def test_fresh_windows_pass():
    now = datetime(2026, 8, 18, 12, 30, tzinfo=timezone.utc)
    stale, age = m.windows_are_stale([_entry("2026-08-18T11:39:13.801124Z")], now)
    assert stale is False
    assert age < 1


def test_yesterdays_windows_are_stale():
    # The historical pass didn't land: posting would restate yesterday's board
    # as if it were today's.
    now = datetime(2026, 8, 18, 12, 30, tzinfo=timezone.utc)
    stale, age = m.windows_are_stale([_entry("2026-08-17T11:39:13.801124Z")], now)
    assert stale is True
    assert 24 < age < 26


def test_undateable_windows_count_as_stale():
    now = datetime(2026, 8, 18, 12, 30, tzinfo=timezone.utc)
    assert m.windows_are_stale([{"player_name": "Guy"}], now) == (True, None)
    assert m.windows_are_stale([], now) == (True, None)


def test_newest_timestamp_wins_across_entries():
    now = datetime(2026, 8, 18, 12, 30, tzinfo=timezone.utc)
    entries = [_entry("2026-08-01T11:00:00Z"), _entry("2026-08-18T11:00:00Z")]
    stale, age = m.windows_are_stale(entries, now)
    assert stale is False


def test_share_read_needs_a_stint_older_than_the_whole_comparison():
    # The prior stretch starts at today-30, so a man promoted three weeks ago
    # would have his old club's games measured against his new club's schedule.
    verdicts = [{"player_name": "Promoted", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(
        verdicts, {"Promoted": 1}, _windows("Promoted", 3, 14), "2026-08-18",
        lookup=lambda _id: _roster(team_name="Somerset Patriots", stint="2026-07-28"),
        team_games=lambda *a: 12,
    )
    assert "usage_share" not in verdicts[0]
    assert "joined Somerset Patriots on 2026-07-28" in verdicts[0]["share_check"]
    assert "30 days" in verdicts[0]["share_check"]


def test_a_settled_stint_still_gets_its_share_read():
    verdicts = [{"player_name": "Settled", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(
        verdicts, {"Settled": 1}, _windows("Settled", 3, 14), "2026-08-18",
        lookup=lambda _id: _roster(stint="2026-04-01"),
        team_games=lambda _tid, start, end: 11 if end == "2026-08-18" else 12,
        il_history=lambda *a: None,
    )
    assert verdicts[0]["usage_share"]["dropped"] is True


def test_a_recent_il_activation_voids_the_share_read_not_just_idle():
    # Sterlin Thompson, 2026-08-19: two games back from a 07-31 to 08-18 IL
    # stint. He's not idle anymore (he's played), but his own recent absence
    # still explains a share crash — "2 of 12 (17%), down from 8 of 14 (57%)"
    # — that has nothing to do with a benching (BE, same root cause as the idle
    # case, a different-shaped symptom).
    verdicts = [{"player_name": "Thompson", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(
        verdicts, {"Thompson": 1}, _windows("Thompson", 2, 9), "2026-08-19",
        lookup=lambda _id: _roster(stint="2026-04-01"),
        team_games=lambda *a: 12,
        il_history=lambda *a: {"placed": "2026-07-31", "activated": "2026-08-18"},
    )
    assert "usage_share" not in verdicts[0]
    assert verdicts[0]["status"] == "steady"
    assert "activated from the injured list 2026-08-18" in verdicts[0]["share_check"]
    assert "not a benching" in verdicts[0]["share_check"]


def test_an_old_activation_does_not_void_a_real_share_read():
    # Activated well outside the 30-day comparison span — nothing left to
    # explain, so a genuine usage drop must still surface.
    verdicts = [{"player_name": "Settled", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(
        verdicts, {"Settled": 1}, _windows("Settled", 3, 14), "2026-08-18",
        lookup=lambda _id: _roster(stint="2026-04-01"),
        team_games=lambda _tid, start, end: 11 if end == "2026-08-18" else 12,
        il_history=lambda *a: {"placed": "2026-05-01", "activated": "2026-05-08"},
    )
    assert verdicts[0]["usage_share"]["dropped"] is True


def test_il_history_lookup_failure_leaves_the_share_finding_alone():
    def boom(*_args):
        raise RuntimeError("transactions 500")

    verdicts = [{"player_name": "Settled", "status": "steady", "kind": "hitter",
                 "reason": "OPS .750 → .740"}]
    m.apply_roster_context(
        verdicts, {"Settled": 1}, _windows("Settled", 3, 14), "2026-08-18",
        lookup=lambda _id: _roster(stint="2026-04-01"),
        team_games=lambda _tid, start, end: 11 if end == "2026-08-18" else 12,
        il_history=boom,
    )
    assert "usage_share" not in verdicts[0]
    assert verdicts[0]["share_check"] == "IL-history lookup failed"


def test_stale_windows_stamp_the_post_and_never_suppress_it():
    # The report goes out every day — non negotiable. Stale data is labelled,
    # not withheld.
    alerts = [{"player_name": "Guy", "team": "New York Yankees",
               "current_level": "AA", "status": "lull",
               "reason": "OPS .800 → .560", "detail": "3-for-21, 0 HR"}]
    text = m.build_slack_text(alerts, tracked=33, stale_as_of="2026-08-17")
    assert "⚠️ _Stats as of 2026-08-17 — this morning's refresh hasn't landed._" in text
    assert "*Guy*" in text
    # The stamp sits under the header, above the first section.
    assert text.index("Stats as of") < text.index("*Guy*")


def test_a_fresh_post_carries_no_stamp():
    alerts = [{"player_name": "Guy", "team": "New York Yankees",
               "current_level": "AA", "status": "lull", "reason": "OPS .800 → .560"}]
    assert "Stats as of" not in m.build_slack_text(alerts, tracked=33)
