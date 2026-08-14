"""MiLB watch: baseline subtraction, lull/surge/idle gating, window choice,
and the alert cooldown."""
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
# Cooldown
# ---------------------------------------------------------------------------

def test_cooldown_suppresses_repeat_alerts_but_not_new_status():
    lull = {"player_name": "Guy", "status": "lull"}
    state = {"Guy": {"last_alert_date": "2026-08-10", "last_alert_status": "lull"}}
    assert not m.due_for_alert(lull, state, "2026-08-14")
    assert m.due_for_alert(lull, state, "2026-08-25")
    # A different status is a different conversation — fires immediately.
    assert m.due_for_alert({"player_name": "Guy", "status": "surge"}, state,
                           "2026-08-14")


def test_steady_and_insufficient_never_alert():
    for status in ("steady", "insufficient"):
        assert not m.due_for_alert({"player_name": "Guy", "status": status}, {},
                                   "2026-08-14")


def test_build_state_stamps_only_players_that_alerted():
    verdicts = [
        {"player_name": "Fired", "status": "lull", "baseline": {"ops": ".800",
                                                                "_ops": 0.8}},
        {"player_name": "Quiet", "status": "steady", "baseline": {"ops": ".750",
                                                                  "_ops": 0.75}},
    ]
    state = m.build_state(verdicts, {}, {"Fired"}, "2026-08-14")
    assert state["Fired"]["last_alert_date"] == "2026-08-14"
    assert state["Fired"]["last_alert_status"] == "lull"
    assert state["Quiet"]["last_alert_date"] is None
    assert state["Quiet"]["baseline_ops"] == ".750"


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
# Usage — a lull can be a drop in playing time, small sample and all
# ---------------------------------------------------------------------------

def test_usage_signal_compares_last_14_days_to_the_16_before_it():
    # 44 PA in the 30-day window, 12 of them in the last 14 days -> 32 PA over
    # the prior 16 days. 0.86/day vs 2.0/day is a 57% cut.
    recent_14 = _hitter(pa=12, ab=11, h=2, games=4)
    recent_30 = _hitter(pa=44, ab=40, h=11, games=15)
    u = m.usage_signal(recent_14, recent_30, "hitter")
    assert u["prior"] == "32 PA"
    assert u["recent"] == "12 PA"
    assert u["prior_games"] == 11 and u["recent_games"] == 4
    assert 55 <= u["drop_pct"] <= 60
    assert u["dropped"] is True


def test_usage_signal_ignores_steady_playing_time():
    recent_14 = _hitter(pa=30, ab=27, h=8, games=11)
    recent_30 = _hitter(pa=64, ab=58, h=17, games=24)
    assert m.usage_signal(recent_14, recent_30, "hitter")["dropped"] is False


def test_usage_signal_needs_a_prior_stretch_worth_comparing():
    # 10 PA over the prior 16 days is under the floor — no read either way.
    recent_14 = _hitter(pa=2, ab=2, h=0, games=1)
    recent_30 = _hitter(pa=12, ab=11, h=3, games=5)
    assert m.usage_signal(recent_14, recent_30, "hitter") is None


def test_small_recent_sample_becomes_a_usage_lull_not_insufficient():
    # The exact case a sample gate would have thrown away: an everyday guy down
    # to 10 PA in two weeks. The rate is unreadable; the usage IS the finding.
    season = _hitter(pa=300, ab=270, h=81, obp=".370", slg=".480", games=70)
    recent_14 = _hitter(pa=10, ab=9, h=2, obp=".300", slg=".333", games=4)
    recent_30 = _hitter(pa=55, ab=50, h=15, obp=".360", slg=".460", games=18)
    v = m.evaluate_windows(season, {"14d": recent_14, "30d": recent_30})
    assert v["status"] == "usage_lull"
    assert "Playing time down" in v["reason"]
    # The rate read it replaced is kept as context, not dropped — here the
    # 30-day line, which is readable and unremarkable. "He's still hitting, he's
    # just not playing" is exactly what the call needs.
    assert v["detail"]
    assert v["usage"]["dropped"] is True
    assert v["alternates"][0]["status"] == "insufficient"


def test_rate_lull_keeps_its_status_and_gains_the_usage_detail():
    season = _hitter(pa=300, ab=270, h=81, obp=".370", slg=".480", games=70)
    recent_14 = _hitter(pa=28, ab=26, h=3, obp=".180", slg=".192", games=9)
    recent_30 = _hitter(pa=90, ab=82, h=20, obp=".300", slg=".350", games=28)
    v = m.evaluate_windows(season, {"14d": recent_14, "30d": recent_30})
    assert v["status"] == "lull"
    assert "playing time down" in v["detail"].lower()


def test_pitcher_usage_measured_in_innings():
    recent_14 = _pitcher(ip="3", k=3, bb=1, era="3.00", games=3)
    recent_30 = _pitcher(ip="18", k=20, bb=5, era="2.50", games=12)
    u = m.usage_signal(recent_14, recent_30, "pitcher")
    assert u["prior"] == "15 IP" and u["recent"] == "3 IP"
    assert u["dropped"] is True


# ---------------------------------------------------------------------------
# IL exclusion
# ---------------------------------------------------------------------------

def test_il_players_drop_off_the_no_games_list():
    verdicts = [
        {"player_name": "Hurt Guy", "status": "idle",
         "reason": "No games in the last 14 days (92 G on the season)"},
        {"player_name": "Healthy Guy", "status": "idle",
         "reason": "No games in the last 14 days (40 G on the season)"},
    ]
    il = {"code": "D7", "description": "Injured 7-Day", "since": "2026-06-22",
          "team": "Albuquerque Isotopes"}
    m.apply_availability(
        verdicts, {"Hurt Guy": 1, "Healthy Guy": 2},
        lookup=lambda mlb_id: il if mlb_id == 1 else None,
    )
    assert verdicts[0]["status"] == "il"
    assert "Injured 7-Day since 2026-06-22" in verdicts[0]["reason"]
    assert verdicts[1]["status"] == "idle"
    # `il` is not an alerting status — it never reaches Slack.
    assert not m.due_for_alert(verdicts[0], {}, "2026-08-14")
    assert m.due_for_alert(verdicts[1], {}, "2026-08-14")


def test_usage_lull_is_also_il_checked():
    verdicts = [{"player_name": "Hurt Guy", "status": "usage_lull",
                 "reason": "Playing time down 70% — 30 PA → 8 PA"}]
    m.apply_availability(verdicts, {"Hurt Guy": 1},
                         lookup=lambda _id: {"code": "D7",
                                             "description": "Injured 7-Day",
                                             "since": "2026-08-01"})
    assert verdicts[0]["status"] == "il"


def test_failed_il_lookup_keeps_the_finding_and_says_so():
    # Dropping a real absence because an API call failed is worse than a noisy
    # line, so the finding stands and the snapshot records that the check didn't
    # run.
    def boom(_mlb_id):
        raise RuntimeError("MLB API 503")

    verdicts = [{"player_name": "Unknown Guy", "status": "idle",
                 "reason": "No games in the last 14 days (40 G on the season)"}]
    m.apply_availability(verdicts, {"Unknown Guy": 1}, lookup=boom)
    assert verdicts[0]["status"] == "idle"
    assert verdicts[0]["il_check"] == "lookup failed"


def test_missing_mlb_id_is_recorded_not_guessed():
    verdicts = [{"player_name": "No ID", "status": "idle", "reason": "No games"}]
    m.apply_availability(verdicts, {}, lookup=lambda _id: None)
    assert verdicts[0]["status"] == "idle"
    assert verdicts[0]["il_check"] == "no mlb_id in roster cache"


def test_unavailable_codes_and_keywords_both_recognized():
    assert "D7" in m._UNAVAILABLE_STATUS_CODES
    assert "D60" in m._UNAVAILABLE_STATUS_CODES
    # "Reassigned to Minors" is a roster move, not an absence — must NOT match.
    assert not any(w in "reassigned to minors" for w in m._UNAVAILABLE_KEYWORDS)
    assert "RM" not in m._UNAVAILABLE_STATUS_CODES
    assert any(w in "injured 7-day" for w in m._UNAVAILABLE_KEYWORDS)
