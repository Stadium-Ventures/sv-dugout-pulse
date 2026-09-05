"""MiLB performance watch — season baseline vs. recent form.

Kent's ask (#justin-riemer, 2026-08-13): *"can we start tracking, and alerting
if lull, proactive front office / farm director calls for our MiLB guys?"* The
prompt was Justin Riemer — hitting .388 with a .920 OPS since Kent's last call
with the Athletics' farm director on July 15, i.e. a big move **against his own
season line** that nobody was watching for.

That's the shape of this watcher. Every other alert in this repo grades a single
game (`src/performance_analyzer.py`) or grades a window against fixed league
thresholds (`src/window_grader.py`). Neither answers "is this guy playing
differently than he has all year?", which is what earns a farm-director call.
So:

  recent    = the trailing 14-day AND 30-day windows, judged separately
  baseline  = season to date MINUS that recent window ("production to date for
              the year", with the recent stretch taken out so a slump can't
              quietly drag down its own comparison)
  verdict   = the same Dugout Pulse quality logic (`src.window_grader`
              thresholds — OPS for hitters, ERA for pitchers) applied to BOTH
              lines, plus the delta between them; the more actionable of the two
              windows wins

Both windows are graded because the two horizons catch different things and the
originating case needed the longer one: Riemer's .920 was "since my last call on
July 15" — about 30 days — and on 14 days he reads +.112, under the bar. 14 days
is the timely read, 30 days is the stable one. Taking the stronger of the two
means a slow four-week slide alerts even when the last two weeks look flat, and
a two-week collapse alerts before the monthly line has caught up.

Four things are worth a call, and all four post:

  🔻 lull       — recent form materially below his own baseline AND landing in
                  Steady/Cold. The alert Kent asked for.
  ⏳ usage lull — he's playing less, read two ways because "fewer plate
                  appearances" mixes up three different causes. ROLE: plate
                  appearances per game PLAYED vs his own season baseline —
                  "when he plays, is he still starting?", the earliest tell,
                  since a man dropped from the lineup still appears for days
                  before his game count moves. SHARE: games played out of his
                  team's games — "is he still in the lineup?", with the schedule
                  divided out so an off-week isn't a benching. Hitters only.
  😶 idle       — played this year, nothing in 14 days, and NOT on the IL.
  📈 surge      — recent form materially above baseline AND landing in
                  Solid/Hot. This is the Riemer case: the call you make to get a
                  guy noticed, not the one you make to defend him.

CADENCE — flag once, then update on a delay. A player posts the day he first
qualifies; after that he waits out his re-report window (7 days for a hitter, 14
for a pitcher) even while he keeps qualifying — and even if what he qualifies for
changes — and if nobody is new and nobody is due the whole post is skipped. The
window is a floor on the PLAYER: once he has been surfaced, nothing about him
goes out again until it elapses (BE, 2026-09-05).

This replaced a rolling board that showed every qualifying player every morning.
That version ran for one day; Kent read it and asked to "space out the
repetitive player updates" (2026-08-16), and BE settled the shape: "a one time
flag for any guy at the time they qualify and otherwise quiet if nobody new
meets a threshold… guys that pop on the report get an update 1 week after",
with Kent adding "hitters 1 week and pitchers 2 weeks". Pitchers wait longer
because their evidence arrives more slowly — seven days of "still struggling"
is often the same two outings restated.

Dropping off the board does NOT reset the clock: state is kept for every tracked
player, so a man who dips under the bar for a day and clears it again tomorrow
is not a fresh flag. A status flip (lull → trending up) IS a fresh flag and
posts the same morning — different conversation, not a repeat.

"An update 1 week after" is not conditional on still qualifying. A flagged
hitter who's back to normal by day 7 still gets that update — a ✅ *Back to
normal* line saying he was flagged, when, and what he reads like now — rather
than silently vanishing. Found as a live gap on 2026-08-18 (BE): Kellon Lindsey
and Jake Munroe had both cleared their bars days before their windows closed,
and the code as it stood would never have surfaced either one again. Fires once
per flag, on the player's own clock (`resolution_due`); a closeout is excluded
from ever re-closing itself, and re-qualifying afterward reads as a brand-new
flag, not a continuation.

Candidates are resolved against their club before anything posts (one
`rosterEntries` call answers both questions). On the IL, rehabbing or otherwise
unavailable → the org already told us why he isn't playing, so it isn't a call:
status `il`, kept in the snapshot with the reason, never posted. Changed orgs
inside the window → the lineup-share denominator is invalid, because he wasn't
on that club for most of its games, so the share read is dropped and says why.
The role read survives an org change untouched — a starter is a starter
anywhere. A failed lookup leaves the finding standing and records that the check
didn't run.

MiLB only: Pro clients whose `current_level` is CPX/A/A+/AA/AAA. MLB guys are
out of scope (different conversation, different people to call).

Windows come from the historical pass (`data/window_season.json`,
`data/window_14d.json`, `data/window_30d.json`), so this only needs to run after
that lands — once a day is plenty. Baselines come from subtracting the recent
window's counting stats off the season line; the window JSON carries no 2B/3B or
ER, so total bases, times-on-base and earned runs are reconstructed from the
rounded SLG/OBP/ERA/WHIP strings and land within a rounding error of the true
count. Immaterial at the .150-OPS / 1.50-ERA bars this fires on, but it's why
`data/milb_watch.json` records the derived counts alongside the rates.

Posts to #dugout-pulse (`SLACK_WEBHOOK_URL`) daily at 8:30 AM ET — feature
output an agent reads on purpose, not an ops finding, per the channel scope rule
in CLAUDE.md. Silent when nothing is actionable.

**The message format is locked** (BE, 2026-08-14). `build_slack_text` is pinned
byte-for-byte by `test_locked_message_format` in `tests/test_milb_watch.py`:
section order, the `>` blockquote body, the `*Name*  ·  Org  ·  Level` line, the
blank lines, the footer wording. Changing any of it fails that test on purpose —
if a change is actually wanted, update the expected block in the same commit and
say why. Do not "tidy" this copy.

State: `data/_milb_watch_state.json` — every tracked player's current status,
when it started (`since`) and when he was last actually posted
(`last_posted_date`). The cadence is keyed off the last post, not off when the
finding began, so a month-long slump still produces its weekly update instead of
going silent forever.
Snapshot: `data/milb_watch.json` — every tracked MiLB client with baseline vs.
recent, whether or not he alerted. That file is the "tracking" half of the ask
and the thing a surface can project later.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from src.window_grader import (
    GRADE_COLD,
    GRADE_HOT,
    GRADE_QUIET,
    GRADE_SOLID,
    grade_hitter_window,
    grade_pitcher_window,
)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SEASON_PATH = _REPO_ROOT / "data" / "window_season.json"
_RECENT_PATHS = {
    "14d": _REPO_ROOT / "data" / "window_14d.json",
    "30d": _REPO_ROOT / "data" / "window_30d.json",
}
# Absence is judged on the freshest window only — "hasn't played in two weeks"
# is the actionable statement, and a 30-day gap trips the same 14-day check.
_IDLE_WINDOW = "14d"
_STATE_PATH = _REPO_ROOT / "data" / "_milb_watch_state.json"
_SNAPSHOT_PATH = _REPO_ROOT / "data" / "milb_watch.json"
# Name -> mlb_id, for the IL lookup. Refreshed by every pulse run.
_ROSTER_CACHE_PATH = _REPO_ROOT / "data" / "roster_cache.json"

_ET = timezone(timedelta(hours=-4))

# ---------------------------------------------------------------------------
# Scope + thresholds
# ---------------------------------------------------------------------------

# Affiliated minor-league levels as the MLB Stats API reports them. "MLB" is
# deliberately absent — a big-leaguer's dip is not a farm-director call.
MILB_LEVELS = {"CPX", "A", "A+", "AA", "AAA"}

# Sample gates. Below these the delta is noise and the call would be
# embarrassing. The recent floors scale with the window — a reliever's 7 IP is a
# real fortnight but a nearly empty month, and 7 IP of bad luck spread over 30
# days was firing a lull on a guy who simply hadn't pitched much.
MIN_BASELINE_PA = 40
MIN_BASELINE_IP = 15.0
MIN_RECENT_PA = {"14d": 25, "30d": 45}
MIN_RECENT_IP = {"14d": 6.0, "30d": 12.0}
_DEFAULT_MIN_RECENT_PA = 25
_DEFAULT_MIN_RECENT_IP = 6.0

# How far form has to move from a guy's own baseline to be worth a call.
# .150 OPS is roughly a full grade tier in window_grader terms.
OPS_LULL_DROP = 0.150
OPS_SURGE_GAIN = 0.150
ERA_LULL_RISE = 1.50
ERA_SURGE_DROP = 1.50

# Idle: has played this year but hasn't appeared in the 14-day window at all.
IDLE_MIN_SEASON_GAMES = 10

# ── Usage: two questions, not one blurred number ──
# A lull is also a drop in usage, and usage moves before the rate does. But
# "fewer plate appearances than before" silently adds up three different causes:
# his team played fewer games, he was in the lineup fewer times, and he batted
# fewer times per game. The first isn't news and the other two mean different
# things, so they're read separately (BE, 2026-08-14).
#
#   ROLE  — plate appearances per game PLAYED vs his own season baseline.
#           "When he plays, is he still starting?" A starter is ~4.3 PA/G, a
#           bench bat ~1.5. This is the earliest tell: a man dropped from the
#           lineup still appears (pinch-hit, late defense) for days before his
#           games-played count moves at all. Immune to schedule and to org
#           changes — a starter is a starter anywhere.
#   SHARE — games he played ÷ his team's games over the same stretch, vs that
#           same share earlier. "Is he still in the lineup?" Dividing by team
#           games is what stops an off-week or a rainout reading as a benching.
#
# Both run on the timespans already loaded: ROLE on the 14- and 30-day windows
# against the season baseline, SHARE on the trailing 14 days vs the 16 before
# them (i.e. the 30-day window minus the 14-day one).
# Span lengths, mirroring src/historical_stats.py: its 14d window starts at
# today-14 and its 30d at today-30, so the prior stretch is the 16 days from
# today-30 to today-15. Any drift between these and the engine's starts makes
# the share read compare a player against the wrong slice of his club's
# schedule.
_RECENT_DAYS = 14
_PRIOR_DAYS = 16

# Hitters only. A pitcher's appearances are his rotation turn, and PA/G is
# meaningless for him.
ROLE_PA_PER_G_RATIO = 0.70      # fires at <= 70% of baseline PA/G (4.3 -> 3.0)
ROLE_MIN_GAMES = 3              # 3 appearances is enough to see he isn't starting
ROLE_MIN_BASELINE_GAMES = 15    # ...against a baseline long enough to be a role

SHARE_DROP_POINTS = 0.25        # 85% -> 60% of team games is a real sit-down
SHARE_MIN_TEAM_GAMES = 8        # a short window can't tell you anything
# Skip the schedule lookup entirely unless his own game count actually fell —
# no point asking the API about a man who played every day.
SHARE_PRECHECK_RATIO = 0.85

# Statuses that can reach the post.
ACTIONABLE_STATUSES = ("lull", "usage_lull", "idle", "surge")

# ── Cadence: flag once, then update on a delay ──
# This ran for one day as a rolling board — everyone who qualified, every
# morning. Kent read it and asked to "space out the repetitive player updates"
# (#justin-riemer, 2026-08-16); BE: "prefer it's a one time flag for any guy at
# the time they qualify and otherwise quiet if nobody new meets a threshold.
# Maybe guys that pop on the report get an update 1 week after". Kent: "Hitters
# 1 week and pitchers 2 weeks".
#
# So a player posts the day he first qualifies, then not again until his
# re-report window is up — and if nobody is new and nobody is due, the whole
# post is skipped. Pitchers wait twice as long because their evidence arrives
# twice as slowly: a starter makes 2-3 appearances a week, so seven days of
# "still struggling" is often the same two outings restated.
#
# A status flip (lull → trending up) is a different conversation, and it is
# WORDED as a fresh flag — but it waits out the window like everything else.
# BE, 2026-09-05: "once a player is surfaced by the report, he does not appear
# again for 1 week as a hitter, 2 weeks as a pitcher for a status check." The
# window is a floor on the player, not on the finding. Letting a flip skip it
# put Dax Kilby in the post on 08-31 (closeout) and again on 09-01 (surge).
REREPORT_DAYS = {"hitter": 7, "pitcher": 14}

# For cadence purposes a finding's FAMILY is what counts, not its exact status.
# All three concerns say the same thing to a reader — "this guy is down" — so
# sliding from a rate lull to a usage lull to an absence is the same
# conversation continuing, not a new flag. Only crossing between concern and
# opportunity is a genuinely new thing to say.
#
# Without this, our own threshold work re-fires players: the 2026-08-17 post
# flagged Jake Munroe as a usage lull off a broken denominator, and once that
# was fixed he read as a rate lull — which would have re-posted him the very
# next morning, one day after Kent saw him, for no reason on the field.
STATUS_FAMILY = {
    "lull": "concern",
    "usage_lull": "concern",
    "idle": "concern",
    "surge": "opportunity",
}


def status_family(status: str) -> str:
    """'concern' / 'opportunity', or the status itself if it isn't a finding."""
    return STATUS_FAMILY.get(status, status)

# Recent form has to LAND badly, not just move. A guy going 1.150 → .980 is
# still one of the best hitters in his league; that is not a lull.
_LULL_LANDING = {GRADE_QUIET, GRADE_COLD}
_SURGE_LANDING = {GRADE_HOT, GRADE_SOLID}

_PITCHER_POSITIONS = {"Pitcher", "RHP", "LHP", "P", "SP", "RP"}

# ---------------------------------------------------------------------------
# Parsing helpers — window JSON stores rates as strings, "--" when no data
# ---------------------------------------------------------------------------

def _num(value) -> float | None:
    """Parse a window stat ('.294', '2.00', '21.1%', 45, '--') to a float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().rstrip("%")
    if not text or text == "--":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _ip_to_outs(value) -> int | None:
    """Convert baseball innings notation to outs. '35.2' is 35 2/3 IP = 107."""
    raw = _num(value)
    if raw is None:
        return None
    whole = int(raw)
    # Round the fractional part rather than truncating: floats coming back from
    # json can land on 35.199999.
    frac = int(round((raw - whole) * 10))
    if frac not in (0, 1, 2):
        # Not real baseball notation (a genuine decimal like 35.5). Treat the
        # decimal as thirds so the arithmetic stays monotonic.
        return int(round(raw * 3))
    return whole * 3 + frac


def _outs_to_ip(outs: int) -> float:
    """Outs to true innings (107 outs -> 35.667), for rate math."""
    return outs / 3.0


def _outs_to_ip_str(outs: int) -> str:
    """Outs back to display notation (107 outs -> '35.2')."""
    whole, frac = divmod(outs, 3)
    return f"{whole}.{frac}" if frac else str(whole)


def _fmt_delta3(value: float) -> str:
    """Signed rate delta, baseball style: -.456 / +.165, no leading zero."""
    sign = "+" if value >= 0 else "-"
    return f"{sign}{_fmt3(abs(value))}"


def _fmt3(value: float) -> str:
    """Format a rate the way the rest of the app does: .294, 1.024."""
    text = f"{value:.3f}"
    return text[1:] if text.startswith("0.") else text


def is_pitcher(entry: dict) -> bool:
    """Pitcher by declared position, or by the stat line actually present."""
    position = ((entry.get("tags") or {}).get("position") or "").strip()
    if position in _PITCHER_POSITIONS:
        return True
    stats = entry.get("stats") or {}
    return "ip" in stats and "pa" not in stats


# ---------------------------------------------------------------------------
# Line extraction + baseline subtraction
# ---------------------------------------------------------------------------

def hitter_line(stats: dict) -> dict | None:
    """Pull a hitter line into counting stats we can add and subtract.

    SLG/OBP are the only route to total bases and times-on-base — the window
    JSON carries no 2B/3B — so TB = slg*ab and TOB = obp*pa, both rounded to
    the nearest whole event.
    """
    pa = _num(stats.get("pa"))
    ab = _num(stats.get("ab"))
    hits = _num(stats.get("h"))
    if pa is None or ab is None or hits is None or pa <= 0:
        return None
    slg = _num(stats.get("slg"))
    obp = _num(stats.get("obp"))
    if slg is None or obp is None:
        return None
    return {
        "pa": int(pa),
        "ab": int(ab),
        "h": int(hits),
        "hr": int(_num(stats.get("hr")) or 0),
        "bb": int(_num(stats.get("bb")) or 0),
        "k": int(_num(stats.get("k")) or 0),
        "tb": int(round(slg * ab)),
        "tob": int(round(obp * pa)),
    }


def hitter_rates(line: dict) -> dict:
    """Rate stats from a counting line, matching the app's display format."""
    pa, ab = line["pa"], line["ab"]
    avg = line["h"] / ab if ab else 0.0
    obp = line["tob"] / pa if pa else 0.0
    slg = line["tb"] / ab if ab else 0.0
    return {
        "pa": pa,
        "ab": ab,
        "h": line["h"],
        "hr": line["hr"],
        "bb": line["bb"],
        "k": line["k"],
        "avg": _fmt3(avg),
        "obp": _fmt3(obp),
        "slg": _fmt3(slg),
        "ops": _fmt3(obp + slg),
        "k_pct": f"{100.0 * line['k'] / pa:.1f}%" if pa else "--",
        "bb_pct": f"{100.0 * line['bb'] / pa:.1f}%" if pa else "--",
        # Numeric copy so callers don't re-parse what we just formatted.
        "_ops": round(obp + slg, 3),
        "_k_pct": round(100.0 * line["k"] / pa, 1) if pa else None,
    }


def pitcher_line(stats: dict) -> dict | None:
    """Pull a pitcher line into subtractable counts.

    ER and hits+walks-allowed are reconstructed from ERA and WHIP against
    innings, since the window JSON carries neither directly.
    """
    outs = _ip_to_outs(stats.get("ip"))
    era = _num(stats.get("era"))
    if outs is None or outs <= 0 or era is None:
        return None
    innings = _outs_to_ip(outs)
    whip = _num(stats.get("whip"))
    return {
        "outs": outs,
        "k": int(_num(stats.get("k")) or 0),
        "bb": int(_num(stats.get("bb")) or 0),
        "er": int(round(era * innings / 9.0)),
        "hbb": int(round(whip * innings)) if whip is not None else None,
    }


def pitcher_rates(line: dict) -> dict:
    """Rate stats from a pitcher counting line."""
    innings = _outs_to_ip(line["outs"])
    era = 9.0 * line["er"] / innings if innings else 0.0
    out = {
        "ip": _outs_to_ip_str(line["outs"]),
        "k": line["k"],
        "bb": line["bb"],
        "er": line["er"],
        "era": f"{era:.2f}",
        "k_per_9": f"{9.0 * line['k'] / innings:.1f}" if innings else "--",
        "bb_per_9": f"{9.0 * line['bb'] / innings:.1f}" if innings else "--",
        "_era": round(era, 2),
        "_bb_per_9": round(9.0 * line["bb"] / innings, 1) if innings else None,
    }
    if line.get("hbb") is not None and innings:
        out["whip"] = f"{line['hbb'] / innings:.2f}"
    return out


def subtract_lines(season: dict, recent: dict) -> dict | None:
    """season − recent, per key. None if the result isn't coherent.

    Windows are generated in the same pass from the same source, but a
    mid-window level move or a source correction can leave the 14-day line
    holding events the season line doesn't. Rather than publish a negative
    baseline, refuse to grade the player this run.
    """
    out: dict = {}
    for key, season_value in season.items():
        recent_value = recent.get(key)
        if season_value is None or recent_value is None:
            out[key] = None
            continue
        diff = season_value - recent_value
        if diff < 0:
            return None
        out[key] = diff
    return out


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------

def evaluate(season_entry: dict, recent_entry: dict | None, window: str = "14d") -> dict:
    """Classify one MiLB client over one recent window. Always returns a verdict.

    status is one of: lull, surge, steady, idle, insufficient.
    """
    name = season_entry.get("player_name") or ""
    tags = season_entry.get("tags") or {}
    verdict: dict = {
        "player_name": name,
        "team": season_entry.get("team") or "",
        "current_level": season_entry.get("current_level") or "",
        "roster_priority": tags.get("roster_priority"),
        "kind": "pitcher" if is_pitcher(season_entry) else "hitter",
        "window": window,
        "season_games": int(_num(season_entry.get("games_played")) or 0),
        "recent_games": int(_num((recent_entry or {}).get("games_played")) or 0),
        "status": "insufficient",
        "reason": "",
        "baseline": None,
        "recent": None,
        "delta": None,
    }
    span = f"the last {window.rstrip('d')} days"

    # ── Idle: on the year but nothing in the recent window ──
    if verdict["recent_games"] == 0:
        if window != _IDLE_WINDOW:
            # Don't double-report the absence from both windows.
            verdict["reason"] = f"No games in {span}"
        elif verdict["season_games"] >= IDLE_MIN_SEASON_GAMES:
            verdict["status"] = "idle"
            verdict["reason"] = (
                f"No games in {span} ({verdict['season_games']} G on the season)"
            )
        else:
            verdict["reason"] = "Not enough games on the season to judge an absence"
        return verdict

    if recent_entry is None:
        verdict["reason"] = f"No {window} window entry"
        return verdict

    season_stats = season_entry.get("stats") or {}
    recent_stats = recent_entry.get("stats") or {}

    if verdict["kind"] == "pitcher":
        season_line = pitcher_line(season_stats)
        recent_line = pitcher_line(recent_stats)
    else:
        season_line = hitter_line(season_stats)
        recent_line = hitter_line(recent_stats)

    if season_line is None or recent_line is None:
        verdict["reason"] = "Incomplete stat line in one of the windows"
        return verdict

    baseline_line = subtract_lines(season_line, recent_line)
    if baseline_line is None:
        verdict["reason"] = (
            "14-day line exceeds the season line — windows disagree, skipping"
        )
        return verdict

    if verdict["kind"] == "pitcher":
        return _evaluate_pitcher(verdict, baseline_line, recent_line)
    return _evaluate_hitter(verdict, baseline_line, recent_line)


def _evaluate_hitter(verdict: dict, baseline_line: dict, recent_line: dict) -> dict:
    baseline = hitter_rates(baseline_line)
    recent = hitter_rates(recent_line)
    verdict["baseline"] = baseline
    verdict["recent"] = recent
    span = _span(verdict)

    if baseline["pa"] < MIN_BASELINE_PA:
        verdict["reason"] = (
            f"Baseline sample too small ({baseline['pa']} PA before "
            f"{span}, need {MIN_BASELINE_PA})"
        )
        return verdict
    min_recent_pa = MIN_RECENT_PA.get(verdict["window"], _DEFAULT_MIN_RECENT_PA)
    if recent["pa"] < min_recent_pa:
        verdict["reason"] = (
            f"Recent sample too small ({recent['pa']} PA in {span}, "
            f"need {min_recent_pa})"
        )
        return verdict

    baseline_grade = grade_hitter_window({"ops": baseline["_ops"]}, "baseline")
    recent_grade = grade_hitter_window({"ops": recent["_ops"]}, verdict["window"])
    delta = round(recent["_ops"] - baseline["_ops"], 3)
    verdict["baseline"]["grade"] = baseline_grade
    verdict["recent"]["grade"] = recent_grade
    verdict["delta"] = {"metric": "ops", "value": delta}

    moved = f"OPS {baseline['ops']} → {recent['ops']} ({_fmt_delta3(delta)})"
    if delta <= -OPS_LULL_DROP and recent_grade in _LULL_LANDING:
        verdict["status"] = "lull"
        verdict["reason"] = f"{moved} over {recent['pa']} PA in {span}"
    elif delta >= OPS_SURGE_GAIN and recent_grade in _SURGE_LANDING:
        verdict["status"] = "surge"
        verdict["reason"] = f"{moved} over {recent['pa']} PA in {span}"
    else:
        verdict["status"] = "steady"
        verdict["reason"] = f"{moved} in {span}"

    verdict["detail"] = _hitter_detail(baseline, recent)
    return verdict


def _evaluate_pitcher(verdict: dict, baseline_line: dict, recent_line: dict) -> dict:
    baseline = pitcher_rates(baseline_line)
    recent = pitcher_rates(recent_line)
    verdict["baseline"] = baseline
    verdict["recent"] = recent
    span = _span(verdict)

    baseline_ip = _outs_to_ip(baseline_line["outs"])
    recent_ip = _outs_to_ip(recent_line["outs"])
    if baseline_ip < MIN_BASELINE_IP:
        verdict["reason"] = (
            f"Baseline sample too small ({baseline['ip']} IP before "
            f"{span}, need {MIN_BASELINE_IP:g})"
        )
        return verdict
    min_recent_ip = MIN_RECENT_IP.get(verdict["window"], _DEFAULT_MIN_RECENT_IP)
    if recent_ip < min_recent_ip:
        verdict["reason"] = (
            f"Recent sample too small ({recent['ip']} IP in {span}, "
            f"need {min_recent_ip:g})"
        )
        return verdict

    baseline_grade = grade_pitcher_window({"era": baseline["_era"]}, "baseline")
    recent_grade = grade_pitcher_window({"era": recent["_era"]}, verdict["window"])
    delta = round(recent["_era"] - baseline["_era"], 2)
    verdict["baseline"]["grade"] = baseline_grade
    verdict["recent"]["grade"] = recent_grade
    verdict["delta"] = {"metric": "era", "value": delta}

    moved = f"ERA {baseline['era']} → {recent['era']} ({delta:+.2f})"
    if delta >= ERA_LULL_RISE and recent_grade in _LULL_LANDING:
        verdict["status"] = "lull"
        verdict["reason"] = f"{moved} over {recent['ip']} IP in {span}"
    elif delta <= -ERA_SURGE_DROP and recent_grade in _SURGE_LANDING:
        verdict["status"] = "surge"
        verdict["reason"] = f"{moved} over {recent['ip']} IP in {span}"
    else:
        verdict["status"] = "steady"
        verdict["reason"] = f"{moved} in {span}"

    verdict["detail"] = _pitcher_detail(baseline, recent)
    return verdict


def _span(verdict: dict) -> str:
    """'the last 14 days' / 'the last 30 days', from the window label."""
    return f"the last {verdict['window'].rstrip('d')} days"


def _hitter_detail(baseline: dict, recent: dict) -> str:
    """The 'what changed' line — what to actually ask the farm director about."""
    parts = [f"{recent['h']}-for-{recent['ab']}, {recent['hr']} HR"]
    b_k, r_k = baseline.get("_k_pct"), recent.get("_k_pct")
    if b_k is not None and r_k is not None and abs(r_k - b_k) >= 5.0:
        direction = "up" if r_k > b_k else "down"
        parts.append(f"K% {direction} {b_k:.1f}% → {r_k:.1f}%")
    return " · ".join(parts)


def _pitcher_detail(baseline: dict, recent: dict) -> str:
    parts = [f"{recent['k']} K / {recent['bb']} BB, {recent['er']} ER"]
    b_bb, r_bb = baseline.get("_bb_per_9"), recent.get("_bb_per_9")
    if b_bb is not None and r_bb is not None and abs(r_bb - b_bb) >= 1.5:
        direction = "up" if r_bb > b_bb else "down"
        parts.append(f"BB/9 {direction} {b_bb:.1f} → {r_bb:.1f}")
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# Usage — role (PA per game) and share (of his team's games)
# ---------------------------------------------------------------------------

def _pa_per_game(entry: dict | None) -> tuple[int, int] | None:
    """(games, PA) for a hitter window entry, or None if unreadable."""
    if not entry:
        return None
    games = int(_num(entry.get("games_played")) or 0)
    pa = int(_num((entry.get("stats") or {}).get("pa")) or 0)
    if games <= 0 or pa <= 0:
        return None
    return games, pa


def role_signal(season_entry: dict, window_entry: dict | None,
                window: str, kind: str) -> dict | None:
    """PA per game played over the window vs his own season baseline.

    Baseline is season-minus-window, the same convention the rate reads use, so
    a long slide can't dilute its own comparison. Returns None when it can't be
    read: a pitcher, a missing window, too few games either side.
    """
    if kind == "pitcher":
        return None
    season = _pa_per_game(season_entry)
    recent = _pa_per_game(window_entry)
    if not season or not recent:
        return None
    baseline_games = season[0] - recent[0]
    baseline_pa = season[1] - recent[1]
    if baseline_games < ROLE_MIN_BASELINE_GAMES or baseline_pa <= 0:
        return None
    if recent[0] < ROLE_MIN_GAMES:
        return None

    baseline_rate = baseline_pa / baseline_games
    recent_rate = recent[1] / recent[0]
    ratio = recent_rate / baseline_rate if baseline_rate else 1.0
    span = f"the last {window.rstrip('d')} days"
    return {
        "read": "role",
        "window": window,
        "baseline_pa_per_g": round(baseline_rate, 2),
        "recent_pa_per_g": round(recent_rate, 2),
        "recent_games": recent[0],
        "drop_pct": round(100.0 * (1 - ratio), 1),
        "dropped": ratio <= ROLE_PA_PER_G_RATIO,
        "summary": (
            f"Batting {recent_rate:.1f} times per game over {span}, down from "
            f"{baseline_rate:.1f} on the season — he's playing but not starting"
        ),
    }


def share_signal(games_recent: int, team_recent: int,
                 games_prior: int, team_prior: int) -> dict | None:
    """Share of his team's games played, last 14 days vs the 16 before.

    Dividing by team games is the whole point: it cancels off-days, rainouts and
    the all-star break, which a raw game count reads as a benching.
    """
    if team_recent < SHARE_MIN_TEAM_GAMES or team_prior < SHARE_MIN_TEAM_GAMES:
        return None
    # A player cannot appear in more games than his club played. If he did, the
    # denominator belongs to the wrong club or the wrong days — a mid-window
    # level move, a doubleheader counted once, a schedule gap. Refuse rather
    # than publish a share over 100%, which is how this surfaced.
    if games_recent > team_recent or games_prior > team_prior:
        return None
    recent = games_recent / team_recent
    prior = games_prior / team_prior
    drop = prior - recent
    return {
        "read": "share",
        "window": "14d",
        "recent": f"{games_recent}/{team_recent}",
        "prior": f"{games_prior}/{team_prior}",
        "recent_pct": round(100.0 * recent),
        "prior_pct": round(100.0 * prior),
        "drop_points": round(100.0 * drop),
        "dropped": drop >= SHARE_DROP_POINTS,
        "summary": (
            f"In the lineup for {games_recent} of his team's last {team_recent} "
            f"games ({recent:.0%}), down from {games_prior} of {team_prior} "
            f"({prior:.0%})"
        ),
    }


def share_precheck(recent_14: dict | None, recent_30: dict | None) -> tuple | None:
    """(games last 14, games prior 16) when a share lookup is worth the API call.

    Returns None when his own game count held up — the schedule can only make a
    steady count look worse, never better, so there is nothing to find.
    """
    if not recent_14 or not recent_30:
        return None
    games_14 = int(_num(recent_14.get("games_played")) or 0)
    games_prior = int(_num(recent_30.get("games_played")) or 0) - games_14
    if games_prior <= 0:
        return None
    # Normalise for the different span lengths before deciding it held up.
    if (games_14 / _RECENT_DAYS) / (games_prior / _PRIOR_DAYS) > SHARE_PRECHECK_RATIO:
        return None
    return games_14, games_prior


# ---------------------------------------------------------------------------
# Injured list — an IL guy isn't a mystery, so he isn't a finding
# ---------------------------------------------------------------------------

# MLB Stats API roster-entry status codes that mean "not available to play".
# An absence with one of these attached explains itself; Kent doesn't need a
# nudge to call about a guy the org already told us is hurt (BE, 2026-08-14).
# DEV = Development List (Aaron Watson, Daytona, 2026-08-25): the org parked
# him off the active roster on purpose, so a 14-day absence is the org's
# decision, not a mystery for Kent to chase (Kent, #dugout-pulse 2026-09-02).
_UNAVAILABLE_STATUS_CODES = {"D7", "D10", "D15", "D60", "DL", "RA", "RM7", "DEV"}
_UNAVAILABLE_KEYWORDS = (
    "injured", "rehab", "suspend", "restricted", "bereavement", "paternity",
    "leave", "development list", "inactive",
)


def _mlb_id_index(roster_cache: dict | None) -> dict:
    """player_name -> mlb_id, from the roster cache the pulse run refreshes."""
    players = (roster_cache or {}).get("players") or []
    return {
        p.get("player_name"): p.get("mlb_id")
        for p in players
        if p.get("player_name") and p.get("mlb_id")
    }


_TEAM_GAMES_CACHE: dict = {}


def team_game_count(team_id, start: str, end: str) -> int:
    """Completed games a club played between two dates, inclusive.

    Cached per (team, span) for the life of the run — clients cluster onto the
    same affiliates, so a 30-odd player board is a handful of real calls. The
    MiLB sport IDs are tried in turn because a club's level isn't known here;
    only one of them returns its schedule.
    """
    key = (team_id, start, end)
    if key in _TEAM_GAMES_CACHE:
        return _TEAM_GAMES_CACHE[key]
    import statsapi  # lazily, as above

    best = 0
    for sport_id in (11, 12, 13, 14, 16):
        try:
            schedule = statsapi.get("schedule", {
                "sportId": sport_id, "teamId": team_id,
                "startDate": start, "endDate": end,
            })
        except Exception:
            continue
        played = sum(
            1
            for day in schedule.get("dates", [])
            for game in day.get("games", [])
            if (game.get("status") or {}).get("abstractGameState") == "Final"
        )
        best = max(best, played)
    _TEAM_GAMES_CACHE[key] = best
    return best


def lookup_roster(mlb_id) -> dict:
    """Where a player stands today: his current club, that stint's start, and
    whether he's unavailable.

    One `rosterEntries` hydration answers both questions this module asks of the
    API, so the IL check and the org-change check cost a single call between
    them. Raises on network/API failure so callers can tell "active" from
    "couldn't check" — silently dropping a real finding because an API call
    failed is the one outcome worse than a noisy line.
    """
    import statsapi  # imported lazily so unit tests don't need the dep

    people = statsapi.get(
        "person", {"personId": mlb_id, "hydrate": "rosterEntries"}
    ).get("people") or []
    snapshot = {"team_id": None, "team_name": None, "stint_start": None,
                "unavailable": None}
    if not people:
        return snapshot
    for entry in people[0].get("rosterEntries") or []:
        if entry.get("endDate") and not entry.get("isActive"):
            continue
        status = entry.get("status") or {}
        code = (status.get("code") or "").upper()
        description = (status.get("description") or "").strip()
        team = entry.get("team") or {}
        unavailable = code in _UNAVAILABLE_STATUS_CODES or any(
            word in description.lower() for word in _UNAVAILABLE_KEYWORDS
        )
        if unavailable and not snapshot["unavailable"]:
            snapshot["unavailable"] = {
                "code": code,
                "description": description or code,
                "since": entry.get("startDate"),
                "team": team.get("name"),
            }
        # The playing stint is the open-ended entry that ISN'T an IL/rehab
        # marker — that's the club whose schedule his lineup share is measured
        # against, and the date he joined it.
        if not unavailable and team.get("id") and not snapshot["team_id"]:
            snapshot["team_id"] = team.get("id")
            snapshot["team_name"] = team.get("name")
            snapshot["stint_start"] = entry.get("startDate")
    return snapshot


def lookup_unavailable(mlb_id) -> dict | None:
    """Back-compat shim: just the IL half of `lookup_roster`."""
    return lookup_roster(mlb_id).get("unavailable")


def _legacy_unavailable(mlb_id) -> dict | None:
    import statsapi

    people = statsapi.get(
        "person", {"personId": mlb_id, "hydrate": "rosterEntries"}
    ).get("people") or []
    if not people:
        return None
    for entry in people[0].get("rosterEntries") or []:
        if entry.get("endDate") and not entry.get("isActive"):
            continue
        status = entry.get("status") or {}
        code = (status.get("code") or "").upper()
        description = (status.get("description") or "").strip()
        if code in _UNAVAILABLE_STATUS_CODES or any(
            word in description.lower() for word in _UNAVAILABLE_KEYWORDS
        ):
            return {
                "code": code,
                "description": description or code,
                "since": entry.get("startDate"),
                "team": (entry.get("team") or {}).get("name"),
            }
    return None


# How far back to check for a closed-out IL stint. Longer than the 30-day
# comparison span plus slack — an idle check only ever needs the trailing 14
# days explained, but a stint can start before the window we're looking at.
_IL_TRANSACTION_LOOKBACK_DAYS = 35


def _mmddyyyy(iso_day: str) -> str:
    """The transactions endpoint wants MM/DD/YYYY — every other endpoint in
    this module wants YYYY-MM-DD. Mixing them up doesn't error, it just
    silently returns zero rows."""
    year, month, day = iso_day.split("-")
    return f"{month}/{day}/{year}"


def lookup_recent_il(team_id, mlb_id: int, today: str,
                     lookback_days: int = _IL_TRANSACTION_LOOKBACK_DAYS) -> dict | None:
    """The player's most recent injured-list stint that has since closed.

    `rosterEntries` (what `lookup_roster` uses) only exposes his CURRENT status.
    Once he's reactivated, the open-ended assignment just flips back to
    "Active" with no trace of the IL dates — fine for "is he on the IL right
    now", blind to "he JUST got off the IL". That gap produced a real false
    idle: Sterlin Thompson was placed on Albuquerque's 7-day IL on 2026-07-31
    and activated 2026-08-18, so his trailing-14-day window was genuinely empty
    for a reason that had nothing to do with a benching or a form issue — the
    current-status check alone had no way to see that (BE, 2026-08-19).

    The transactions endpoint carries the discrete events instead. Returns
    {"placed": date, "activated": date-or-None} for the most recent stint
    touching the lookback window, or None if there isn't one. A stint whose
    placement predates the lookback is invisible here — acceptable, since a
    still-open stint is already caught by `lookup_roster`'s current-status
    check, and real IL stints don't run longer than this lookback.
    """
    import statsapi  # lazily, as elsewhere in this module

    start = _shift(today, -lookback_days)
    transactions = statsapi.get("transactions", {
        "teamId": team_id,
        "startDate": _mmddyyyy(start),
        "endDate": _mmddyyyy(today),
    }).get("transactions") or []

    placed = None
    activated = None
    for txn in sorted(transactions, key=lambda t: t.get("date") or ""):
        if (txn.get("person") or {}).get("id") != mlb_id:
            continue
        description = (txn.get("description") or "").lower()
        if "injured list" not in description:
            continue
        if "activated" in description:
            activated = txn.get("date")
        elif "placed" in description:
            # A fresh placement starts a new stint — any earlier activation
            # belonged to a prior one and no longer applies.
            placed = txn.get("date")
            activated = None
    if placed is None:
        return None
    return {"placed": placed, "activated": activated}


def apply_roster_context(verdicts: list, mlb_ids: dict, windows: dict, today: str,
                         lookup=lookup_roster, team_games=team_game_count,
                         il_history=lookup_recent_il) -> None:
    """Resolve each candidate against his club: IL status and lineup share.

    Two things the stat lines can't tell you, both needing the same roster
    lookup:

    1. **He's on the IL** — or he JUST came off it. Either way the org already
       told us why he isn't playing, so it isn't a call. Becomes status `il`,
       which stays in the snapshot with the reason and never posts. The second
       half needs the transactions history (`lookup_recent_il`), because a
       closed-out stint leaves no trace on his current roster status — Sterlin
       Thompson read as a bare, unexplained "idle" the day he was activated,
       even though his 14-day window was empty because he'd been hurt for
       nearly all of it (BE, 2026-08-19).
    2. **He changed orgs inside the window.** Then "games his team played" is
       the wrong denominator — he wasn't on that team for most of them. Cade
       Doughty was released 2026-08-04 and signed with Atlanta on 08-10; against
       Rome's last 11 games he read as a 27% benching while actually playing
       nearly every day since signing (BE flagged it, 2026-08-14). The share
       read is dropped in that case and says why. The role read (PA per game)
       survives an org change untouched — a starter is a starter anywhere.
    3. **He's recently off the IL, but the share read doesn't know it.** The
       idle exclusion above only covers zero games; a player a few games back
       from a stint still reads as a usage crash, because his own team-games
       share for the stretch he was hurt is genuinely near zero — Thompson two
       games back from his 07-31/08-18 stint read "2 of 12 (17%), down from 8
       of 14 (57%)", his rehab ramp-up mislabeled as a benching (BE,
       2026-08-19). Voided the same way, across the SAME 30-day span (recent 14
       + prior 16) the share read itself compares. The role read is untouched
       here too — it only measures games he actually played, so it has no
       zero-games artifact to distort.

    A lookup that fails leaves whatever the stat lines said and records that the
    check didn't run.
    """
    for verdict in verdicts:
        needs_il = verdict["status"] in ("idle", "usage_lull")
        share_input = share_precheck(
            windows.get("14d", {}).get(verdict["player_name"]),
            windows.get("30d", {}).get(verdict["player_name"]),
        ) if verdict["kind"] == "hitter" else None
        if not needs_il and not share_input:
            continue

        mlb_id = mlb_ids.get(verdict["player_name"])
        if not mlb_id:
            verdict["roster_check"] = "no mlb_id in roster cache"
            continue
        try:
            snapshot = lookup(mlb_id)
        except Exception as exc:
            logger.warning("Roster lookup failed for %s: %s", verdict["player_name"], exc)
            verdict["roster_check"] = "lookup failed"
            continue
        verdict["roster_check"] = "checked"

        unavailable = snapshot.get("unavailable")
        if needs_il and unavailable:
            since = f" since {unavailable['since']}" if unavailable.get("since") else ""
            verdict["il"] = unavailable
            verdict["status"] = "il"
            verdict["reason"] = (
                f"{unavailable['description']}{since} — "
                f"{verdict['reason'][0].lower()}{verdict['reason'][1:]}"
            )
            continue
        if needs_il and verdict["status"] == "idle" and not unavailable:
            team_id = snapshot.get("team_id")
            recent_stint = None
            if team_id:
                try:
                    recent_stint = il_history(team_id, mlb_id, today)
                except Exception as exc:
                    logger.warning(
                        "IL-history lookup failed for %s: %s",
                        verdict["player_name"], exc,
                    )
                    verdict["roster_check"] = "IL-history lookup failed"
            activated = (recent_stint or {}).get("activated")
            # Only voids the idle read when the activation itself falls inside
            # the trailing window being judged — an activation from weeks ago
            # explains nothing about why he still hasn't played since.
            if activated and _days_between(activated, today) <= _RECENT_DAYS:
                verdict["il"] = {
                    "code": "ACT",
                    "description": "Activated from the injured list",
                    "since": activated,
                    "team": snapshot.get("team_name"),
                }
                verdict["status"] = "il"
                verdict["reason"] = f"Activated from the injured list {activated} — no games since"
                continue

        if not share_input or unavailable:
            continue

        # The share read reaches back across BOTH spans — the prior stretch
        # starts at today-30 — so his club must have been his club for all of
        # it. A 14-day guard passed a man promoted three weeks ago and then
        # measured his old club's games against his new club's schedule.
        stint_start = snapshot.get("stint_start")
        comparison_days = _RECENT_DAYS + _PRIOR_DAYS
        if stint_start and _days_between(stint_start, today) < comparison_days:
            verdict["share_check"] = (
                f"joined {snapshot.get('team_name') or 'a new club'} on "
                f"{stint_start} — too new to read a lineup share against "
                f"{comparison_days} days of that club's schedule"
            )
            continue
        team_id = snapshot.get("team_id")
        if not team_id:
            verdict["share_check"] = "no current club on the roster entry"
            continue

        # A recently-closed IL stint breaks the share read the same way the
        # idle read breaks: his own absence, not the club's, explains the low
        # game count. It shows up here as a usage crash rather than a bare
        # zero — Sterlin Thompson two games back from a 07-31 to 08-18 stint
        # read "in the lineup for 2 of 12 (17%), down from 8 of 14 (57%)",
        # which is his rehab ramp-up, not a benching (BE, 2026-08-19, same root
        # cause as the idle case above but a different-shaped symptom). Voided
        # across the SAME span the share read itself compares — recent 14 plus
        # prior 16 — since an activation anywhere in that stretch taints one
        # side of the comparison or the other.
        try:
            recent_stint = il_history(team_id, mlb_id, today)
        except Exception as exc:
            logger.warning(
                "IL-history lookup failed for %s: %s", verdict["player_name"], exc
            )
            verdict["share_check"] = "IL-history lookup failed"
            continue
        activated = (recent_stint or {}).get("activated")
        if activated and _days_between(activated, today) <= comparison_days:
            verdict["share_check"] = (
                f"activated from the injured list {activated} — his usage over "
                f"the last {comparison_days} days is explained by that absence, "
                f"not a benching"
            )
            continue

        games_recent, games_prior = share_input
        try:
            # These spans MUST match src/historical_stats.py's window starts, or
            # the player's game count and the club's cover different days. It
            # builds 14d as today-14..today (15 days inclusive) and 30d as
            # today-30..today, so the prior stretch is today-30..today-15. Using
            # today-13/today-29 here put his 14 games against a 13-game
            # denominator and posted "down from 14 of 13 (108%)" to
            # #dugout-pulse on 2026-08-17.
            team_recent = team_games(team_id, _shift(today, -_RECENT_DAYS), today)
            team_prior = team_games(
                team_id, _shift(today, -(_RECENT_DAYS + _PRIOR_DAYS)),
                _shift(today, -(_RECENT_DAYS + 1)),
            )
        except Exception as exc:
            logger.warning("Schedule lookup failed for %s: %s", verdict["player_name"], exc)
            verdict["share_check"] = "schedule lookup failed"
            continue

        share = share_signal(games_recent, team_recent, games_prior, team_prior)
        if not share:
            verdict["share_check"] = "too few team games to compare"
            continue
        share["team"] = snapshot.get("team_name")
        verdict["usage_share"] = share
        if share["dropped"] and verdict["status"] in ("insufficient", "steady"):
            _promote_to_usage_lull(verdict, share)


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def milb_clients(season: list) -> list:
    """Pro clients currently at an affiliated minor-league level."""
    return [
        p for p in season
        if p.get("level") == "Pro"
        and p.get("is_client") is not False
        and (p.get("current_level") or "") in MILB_LEVELS
    ]


def evaluate_windows(season_entry: dict, recent_windows: dict) -> dict:
    """Grade one client over every recent window; return the winning verdict.

    `recent_windows` maps window label -> that window's entry for this player.
    The winner is the most actionable status, breaking ties on the bigger move,
    with the losing window kept on the verdict as `alternates` so the snapshot
    shows both reads (e.g. "-.456 over 14d, -.180 over 30d").
    """
    verdicts = [
        evaluate(season_entry, entry, window)
        for window, entry in sorted(recent_windows.items())
    ]
    if not verdicts:
        return evaluate(season_entry, None)
    verdicts.sort(key=_window_rank)
    winner = verdicts[0]
    _apply_usage(winner, season_entry, recent_windows)
    winner["alternates"] = [
        {
            "window": v["window"],
            "status": v["status"],
            "reason": v["reason"],
            "delta": v["delta"],
        }
        for v in verdicts[1:]
    ]
    return winner


def _apply_usage(verdict: dict, season_entry: dict, recent_windows: dict) -> None:
    """Attach the role read and let it promote a quiet verdict, in place.

    Role is PA per game played against his own season baseline, computed on both
    the 14- and 30-day windows; the bigger drop leads. It needs no API and no
    team schedule, so it runs for every hitter. The lineup-share read is the
    other half and lands later, in `apply_roster_context`, because it needs to
    know which club he's on.

    A rate lull already carries the call, so a role drop just enriches its
    detail. But a flat or unreadable rate line hiding a man who has stopped
    starting is the finding on its own.
    """
    reads = [
        role_signal(season_entry, entry, window, verdict["kind"])
        for window, entry in sorted(recent_windows.items())
    ]
    reads = [r for r in reads if r]
    if not reads:
        return
    role = max(reads, key=lambda r: r["drop_pct"])
    verdict["usage_role"] = role
    if not role["dropped"]:
        return
    if verdict["status"] in ("insufficient", "steady"):
        _promote_to_usage_lull(verdict, role)
    elif verdict["status"] == "lull":
        detail = verdict.get("detail") or ""
        verdict["detail"] = f"{detail} · {role['summary'].lower()}".strip(" ·")


def _promote_to_usage_lull(verdict: dict, usage: dict) -> None:
    """Turn a quiet verdict into a usage finding, keeping the rate read as context."""
    # "The rate is fine, he's just not playing" is what the call needs. A
    # sample-size message is internal plumbing, so say what it means instead of
    # pasting a threshold into Slack.
    rate_note = verdict.get("detail") or verdict["reason"]
    if "sample too small" in rate_note:
        recent = verdict.get("recent") or {}
        played = f"{recent['ip']} IP" if "ip" in recent else f"{recent.get('pa', 0)} PA"
        rate_note = f"Only {played} in the last 14 days — too thin for a rate read"
    verdict["status"] = "usage_lull"
    verdict["reason"] = usage["summary"]
    verdict["detail"] = rate_note


def _window_rank(verdict: dict) -> tuple:
    """Most actionable status first, then the larger move."""
    delta = verdict.get("delta") or {}
    magnitude = abs(_num(delta.get("value")) or 0.0)
    return (_STATUS_ORDER.get(verdict["status"], 9), -magnitude)


def evaluate_all(season: list, recent_by_window: dict) -> list:
    """One verdict per MiLB client, ordered most-actionable first.

    `recent_by_window` maps window label -> that window's list of entries.
    """
    indexed = {
        window: {p.get("player_name"): p for p in entries if p.get("player_name")}
        for window, entries in recent_by_window.items()
    }
    verdicts = []
    for entry in milb_clients(season):
        name = entry.get("player_name")
        verdicts.append(
            evaluate_windows(
                entry, {window: index.get(name) for window, index in indexed.items()}
            )
        )
    return sorted(verdicts, key=_sort_key)


_STATUS_ORDER = {
    "lull": 0, "usage_lull": 1, "idle": 2, "surge": 3, "steady": 4,
    "il": 5, "insufficient": 6,
}


def _sort_key(verdict: dict) -> tuple:
    """Lulls first, then by roster tier, then by how big the move was."""
    priority = verdict.get("roster_priority")
    try:
        tier = int(priority)
    except (TypeError, ValueError):
        tier = 9
    delta = verdict.get("delta") or {}
    magnitude = abs(_num(delta.get("value")) or 0.0)
    return (_STATUS_ORDER.get(verdict["status"], 9), tier, -magnitude)


# ---------------------------------------------------------------------------
# Standing state — how long each finding has been up, not who's been silenced
# ---------------------------------------------------------------------------

def _today_et_str() -> str:
    return datetime.now(_ET).date().isoformat()


def _shift(iso_day: str, days: int) -> str:
    """ISO date shifted by N days — for schedule spans."""
    return (date.fromisoformat(iso_day) + timedelta(days=days)).isoformat()


def _days_between(iso_a: str, iso_b: str) -> int:
    try:
        return (date.fromisoformat(iso_b) - date.fromisoformat(iso_a)).days
    except Exception:
        return 999


def is_actionable(verdict: dict) -> bool:
    """True when this verdict belongs on today's board.

    That is the whole gate. Nothing is suppressed for having been shown
    yesterday: qualifying today is what puts a player up, and failing to qualify
    is what takes him down.
    """
    return verdict["status"] in ACTIONABLE_STATUSES


def apply_streaks(verdicts: list, state: dict, today: str) -> None:
    """Stamp each verdict with its streak and whether it's due to post, in place.

    `new_today` is a fresh qualification — either he wasn't on the board
    yesterday, or he was on it for a different reason. Those post immediately.
    Everything else waits out its re-report window: 7 days for a hitter, 14 for
    a pitcher. `new_today` drives how a due post is WORDED; it no longer decides
    whether one goes out (see is_due — BE, 2026-09-05).

    The cadence is keyed off `last_posted_date`, not off when the finding
    started, so a man who has been slumping for a month still gets his weekly
    update rather than going silent forever.
    """
    for verdict in verdicts:
        prior = state.get(verdict["player_name"]) or {}
        continuing = (
            status_family(prior.get("status") or "") == status_family(verdict["status"])
            and bool(prior.get("since"))
        )
        since = prior["since"] if continuing else today
        verdict["since"] = since
        verdict["days_standing"] = _days_between(since, today) + 1
        verdict["new_today"] = not continuing
        verdict["last_posted_date"] = prior.get("last_posted_date")
        verdict["last_posted_status"] = prior.get("last_posted_status")
        verdict["rereport_days"] = REREPORT_DAYS.get(verdict["kind"], 7)
        verdict["due_today"] = is_due(verdict, today)


def is_due(verdict: dict, today: str) -> bool:
    """True when this finding belongs in today's post.

    Due when it's actionable AND either he has never been posted, or his
    re-report window has elapsed since the last time he appeared — 7 days for a
    hitter, 14 for a pitcher.

    The window is a FLOOR ON THE PLAYER, not on the finding (BE, 2026-09-05:
    "once a player is surfaced by the report, he does not appear again for 1
    week as a hitter, 2 weeks as a pitcher for a status check"). A change of
    status no longer buys a way past it. It used to: any post whose family
    differed from the last posted one was treated as a brand-new finding and
    went out at once, which put Dax Kilby in #dugout-pulse on 2026-08-31 (his
    ✅ back-to-normal closeout, stamping last_posted_status "steady") and again
    on 2026-09-01, one morning later, when he re-qualified as a surge — the
    exact repetition this cadence exists to stop, and it restarted his clock
    from the 1st on the way through.

    A flip inside the window is not lost, only held: `since` / `new_today` /
    `days_standing` still track it, so when the window elapses he posts as
    whatever he is on that day, described as a fresh qualification.

    Note this keys off what he was last POSTED as, not off what he was
    yesterday. Keying on yesterday meant a man who dipped under the bar for a
    single day read as a fresh flag when he cleared it again the next morning.
    """
    if not is_actionable(verdict):
        return False
    last = verdict.get("last_posted_date")
    if not last:
        return True
    return _days_between(last, today) >= verdict.get("rereport_days", 7)


def resolution_due(verdict: dict, prior: dict, today: str) -> bool:
    """True when a previously-flagged player owes his one-time closeout.

    "Guys that pop on the report get an update 1 week after" (BE, 2026-08-16)
    was never conditional on still qualifying — a hitter who was flagged and
    then quietly returned to normal still gets his update at the 7/14-day mark,
    saying so. Without this, `is_due` alone drops him the moment he's no longer
    actionable and he never gets that update — a silent gap BE caught on
    2026-08-18 looking at Kellon Lindsey and Jake Munroe, both of whom had
    already cleared their bars days before their windows were up.

    Fires once: on the player's OWN re-report clock (from `last_posted_date`),
    not on a fixed calendar date, using the same hitter/pitcher day counts as
    every other update. Excludes IL — that has its own explanation and its own
    line in the message, not a "back to normal" close-out.
    """
    if is_actionable(verdict) or verdict["status"] == "il":
        return False
    last_status = prior.get("last_posted_status")
    # last_status not being a real flag (never posted, or already the target of
    # a prior close-out, which will have overwritten this with a status like
    # "steady" that is not itself actionable) means there is nothing to close.
    if not last_status or last_status not in ACTIONABLE_STATUSES:
        return False
    last_date = prior.get("last_posted_date")
    if not last_date:
        return False
    days = REREPORT_DAYS.get(verdict["kind"], 7)
    return _days_between(last_date, today) >= days


def apply_resolutions(verdicts: list, state: dict, today: str) -> None:
    """Mark the one-time closeout for anyone whose flag window has elapsed, in place.

    Runs after `apply_streaks`, which already decided `due_today` for players
    still qualifying. This only ever turns a False into a True — it never
    touches an already-actionable finding — and it never touches `status`,
    so a closeout renders from the player's real current grade (e.g. "steady")
    rather than inventing a fifth category that the rest of the module has to
    know about.
    """
    for verdict in verdicts:
        if verdict.get("due_today"):
            continue
        prior = state.get(verdict["player_name"]) or {}
        if not resolution_due(verdict, prior, today):
            continue
        verdict["due_today"] = True
        verdict["resolution"] = {
            "from_status": prior.get("last_posted_status"),
            "from_date": prior.get("last_posted_date"),
        }


def build_state(verdicts: list, state: dict, posted_names: set, today: str) -> dict:
    """Carry every tracked player forward, with when he last actually posted.

    Everyone is kept, not just today's board. A man who dips below the bar for a
    day and clears it again tomorrow would otherwise read as a fresh flag and
    re-post, which is exactly the repetition Kent asked us to stop. Holding his
    `last_posted_date` means the cadence survives the gap.
    """
    new_state: dict = {}
    for verdict in verdicts:
        name = verdict["player_name"]
        prior = state.get(name) or {}
        entry = {
            "status": verdict["status"],
            "since": verdict.get("since", today),
            "last_seen_date": today,
            "last_posted_date": (
                today if name in posted_names else prior.get("last_posted_date")
            ),
            "last_posted_status": (
                verdict["status"] if name in posted_names
                else prior.get("last_posted_status")
            ),
        }
        baseline = verdict.get("baseline") or {}
        if "_ops" in baseline:
            entry["baseline_ops"] = baseline["ops"]
        if "_era" in baseline:
            entry["baseline_era"] = baseline["era"]
        new_state[name] = entry
    return new_state


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------

# Section headings state the rule that fired and nothing else. No call advice,
# no "worth a check-in" subtext — the numbers are the message and the reader
# decides what to do with them (BE, 2026-08-14).
#
# 📈 for trending-up: its arrow is green. A red triangle read as bad news on a
# good-news line.
_SECTION = [
    ("lull", "🔻 *Lull* — form below season baseline"),
    ("usage_lull", "⏳ *Usage down* — playing time cut 40%+"),
    ("idle", "😶 *No games in 14 days* — not on the IL"),
    ("surge", "📈 *Trending up* — form above season baseline"),
]

# Wording for a closeout's "flagged as ___" clause. Deliberately not reusing
# the section headings above — those name a rule, this names a past event.
_RESOLUTION_LABELS = {
    "lull": "a lull",
    "usage_lull": "a usage lull",
    "idle": "no games",
    "surge": "trending up",
}


def _short_team(team: str) -> str:
    """'New York Yankees' → 'Yankees'. Matches the social-URL convention."""
    return team.split()[-1] if team else ""


def build_slack_text(alerts: list, tracked: int, suppressed: list | None = None,
                     stale_as_of: str | None = None) -> str:
    """Compose the DM. Assumes alerts is non-empty.

    LOCKED FORMAT — pinned byte-for-byte by `test_locked_message_format`. Do not
    adjust spacing, separators, or wording without updating that test
    deliberately; the layout below is the approved one (BE, 2026-08-14).

    Layout notes, learned by reading a sent one back (2026-08-14):
    - Slack strips leading whitespace, so indentation does nothing. `>` is the
      only way to actually indent a continuation line, so each player's numbers
      go in a blockquote under his name.
    - No call advice anywhere. It repeated on every bullet, and the numbers
      already say what they say (BE, 2026-08-14).
    - A blank line between players is what makes a list of eight scannable.
    - `*bold*` here is Slack mrkdwn (chat.postMessage), NOT standard markdown —
      `**bold**` would render literally.
    """
    lines = [
        "*MiLB watch* — recent form vs. season baseline",
        f"_{tracked} MiLB clients tracked · {len(alerts)} findings_",
    ]
    if stale_as_of:
        # Delivered anyway, but never dressed up as today's numbers.
        lines.append(
            f"⚠️ _Stats as of {stale_as_of} — this morning's refresh hasn't landed._"
        )
    for status, heading in _SECTION:
        group = [a for a in alerts if a["status"] == status]
        if not group:
            continue
        lines += ["", heading]
        for a in group:
            level = a.get("current_level") or "?"
            lines += [
                "",
                f"*{a['player_name']}*  ·  {_short_team(a['team'])}  ·  {level}",
                f"> {a['reason']}",
            ]
            if a.get("detail"):
                lines.append(f"> {a['detail']}")

    # Closeouts: previously flagged, now off the board, and his re-report
    # window is the reason he's here rather than a live concern or opportunity
    # (BE, 2026-08-18 — "since they are hitters and were flagged they need to
    # get their update one week after they were shown", regardless of whether
    # he's still down). `a["status"]` above is his real current grade
    # (steady/insufficient), which is why this is a second pass keyed off
    # `resolution` rather than a fifth entry in _SECTION.
    resolved = [a for a in alerts if a.get("resolution")]
    if resolved:
        lines += ["", "✅ *Back to normal* — re-report window closed out"]
        for a in resolved:
            level = a.get("current_level") or "?"
            res = a["resolution"]
            label = _RESOLUTION_LABELS.get(
                res.get("from_status"), res.get("from_status") or "flagged"
            )
            when = (res.get("from_date") or "")[5:].replace("-", "/")
            lines += [
                "",
                f"*{a['player_name']}*  ·  {_short_team(a['team'])}  ·  {level}",
                f"> Flagged {label} {when} — now: {a['reason']}",
            ]
            if a.get("detail"):
                lines.append(f"> {a['detail']}")

    if suppressed:
        lines += ["", f"_{_suppressed_line(suppressed)}_"]
    lines += [
        "",
        "_Baseline = season to date minus the window being compared._",
        f"_14- and 30-day form both checked · flagged once, then updated after "
        f"{REREPORT_DAYS['hitter']}d for hitters / {REREPORT_DAYS['pitcher']}d "
        f"for pitchers._",
    ]
    return "\n".join(lines)


def _suppressed_line(suppressed: list) -> str:
    """One line naming the IL guys, so an empty no-games section isn't a mystery."""
    parts = []
    for verdict in suppressed:
        il = verdict.get("il") or {}
        since = il.get("since") or ""
        stamp = f" since {since[5:].replace('-', '/')}" if len(since) >= 10 else ""
        parts.append(
            f"{verdict['player_name']} ({_short_team(verdict['team'])}, "
            f"{verdict.get('current_level') or '?'}{stamp})"
        )
    return "Not shown — on the IL: " + ", ".join(parts) + "."


def post_slack(text: str) -> int:
    """Post the findings to #dugout-pulse.

    Went out as a DM for one review cycle while the format was being settled
    (BE, 2026-08-14); now it's channel output like the rest of the product's
    alerts, on the standard `SLACK_WEBHOOK_URL` webhook.
    """
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL not set — would have posted:")
        print(text)
        return 0
    try:
        resp = requests.post(
            webhook,
            json={"text": text},
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.error("Slack send failed: %s %s", resp.status_code, resp.text)
            return 1
        logger.info("Posted to #dugout-pulse")
        return 0
    except Exception:
        logger.exception("Slack send errored")
        return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# The historical pass at 11:00 UTC is what rebuilds the windows. It runs on the
# same shared-runner scheduler as this job and can be just as late, so a run
# scheduled close behind it can read yesterday's files. The report still goes out
# — every day, non-negotiable (BE, 2026-08-18) — but it says which day its
# numbers come from rather than passing them off as this morning's.
MAX_WINDOW_AGE_HOURS = 12


def newest_window_timestamp(entries: list) -> datetime | None:
    """Most recent `last_updated` across a window file's entries."""
    newest = None
    for entry in entries or []:
        raw = entry.get("last_updated")
        if not raw:
            continue
        try:
            stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        if newest is None or stamp > newest:
            newest = stamp
    return newest


def windows_are_stale(entries: list, now: datetime) -> tuple[bool, float | None]:
    """(stale?, age in hours). Unreadable timestamps count as stale."""
    newest = newest_window_timestamp(entries)
    if newest is None:
        return True, None
    age = (now - newest).total_seconds() / 3600.0
    return age > MAX_WINDOW_AGE_HOURS, round(age, 1)


def _report_stale_windows(age_hours: float | None) -> None:
    """Tell #sv-automation the windows didn't refresh, per the message contract.

    The post still goes out either way; this is the note that says why the
    numbers in it are older than they should be.
    """
    try:
        from scripts._automation_notify import post_automation
    except Exception:
        logger.warning("Could not import post_automation for the stale-window notice")
        return
    age = f"{age_hours:.0f} hours old" if age_hours is not None else "undateable"
    post_automation(
        "🛠️ Code change\n"
        "*What broke:* this morning's MiLB watch went out on older stats — the "
        "overnight refresh had not landed, so the post is stamped with the date "
        "the numbers actually come from.\n"
        f"*How we know:* the rolling-window files are {age}.\n"
        "*What to do:* check the 6 AM ET historical pass in the main pulse "
        "workflow; once it lands, re-run MiLB Watch for today's real numbers."
    )


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        logger.exception("Failed to read %s", path)
        return default


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry",
        action="store_true",
        help="Print the post and the snapshot summary; write nothing, send nothing.",
    )
    args = parser.parse_args(argv)

    season = _load_json(_SEASON_PATH, None)
    recent_by_window = {
        window: _load_json(path, None) for window, path in _RECENT_PATHS.items()
    }
    recent_by_window = {
        window: entries
        for window, entries in recent_by_window.items()
        if isinstance(entries, list)
    }
    if not isinstance(season, list) or _IDLE_WINDOW not in recent_by_window:
        # The historical pass hasn't produced windows yet (fresh clone, or a
        # failed run). Not this script's problem to report — the freshness
        # check in cron_health_alert.py owns that.
        logger.info("Window files missing or unreadable — skipping")
        return 0

    # The report goes out every day — that is not negotiable (BE, 2026-08-18).
    # So stale windows never suppress it; they get stamped on the post so nobody
    # mistakes yesterday's numbers for this morning's, and #sv-automation is told
    # the refresh didn't land.
    stale, age_hours = windows_are_stale(
        recent_by_window[_IDLE_WINDOW], datetime.now(timezone.utc)
    )
    stale_as_of = None
    if stale:
        newest = newest_window_timestamp(recent_by_window[_IDLE_WINDOW])
        stale_as_of = newest.date().isoformat() if newest else "an unknown date"
        logger.warning(
            "Windows are stale (%s hours) — posting anyway, stamped as of %s",
            age_hours, stale_as_of,
        )
        _report_stale_windows(age_hours)

    verdicts = evaluate_all(season, recent_by_window)
    if not verdicts:
        logger.info("No MiLB clients in the season window — skipping")
        return 0

    # Resolve candidates against their club: IL stints drop out, lineup share
    # comes in. Only players whose stat lines already look interesting get a
    # lookup, so this is a handful of API calls, not one per client.
    windows_by_name = {
        window: {p.get("player_name"): p for p in entries if p.get("player_name")}
        for window, entries in recent_by_window.items()
    }
    apply_roster_context(
        verdicts,
        _mlb_id_index(_load_json(_ROSTER_CACHE_PATH, {})),
        windows_by_name,
        _today_et_str(),
    )
    verdicts.sort(key=_sort_key)

    state = _load_json(_STATE_PATH, {})
    today = _today_et_str()
    apply_streaks(verdicts, state, today)
    board = [v for v in verdicts if is_actionable(v)]
    # Closeouts for anyone whose flag window elapsed after he'd already gone
    # quiet — a resolved lull is not on the board, but he still owes his update.
    apply_resolutions(verdicts, state, today)
    # New/continuing flags AND resolved closeouts — everyone due for a reason,
    # whether or not he's currently on the board. If that is empty the run
    # stays silent, even when the board itself isn't.
    alerts = [v for v in verdicts if v["due_today"]]
    alerted_names = {a["player_name"] for a in alerts}
    resolved = [v for v in alerts if v.get("resolution")]
    # Named in a footnote: an empty no-games section otherwise looks like the
    # check didn't run.
    suppressed = [v for v in verdicts if v["status"] == "il"]

    counts: dict = {}
    for verdict in verdicts:
        counts[verdict["status"]] = counts.get(verdict["status"], 0) + 1
    logger.info(
        "%d MiLB clients tracked (%s) — %d on the board, %d due to post "
        "(%d closeouts)",
        len(verdicts),
        ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())),
        len(board),
        len(alerts),
        len(resolved),
    )

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_window": "season minus the recent window",
        "recent_windows": sorted(recent_by_window),
        "thresholds": {
            "min_baseline_pa": MIN_BASELINE_PA,
            "min_recent_pa_by_window": MIN_RECENT_PA,
            "min_baseline_ip": MIN_BASELINE_IP,
            "min_recent_ip_by_window": MIN_RECENT_IP,
            "ops_lull_drop": OPS_LULL_DROP,
            "ops_surge_gain": OPS_SURGE_GAIN,
            "era_lull_rise": ERA_LULL_RISE,
            "era_surge_drop": ERA_SURGE_DROP,
            "idle_min_season_games": IDLE_MIN_SEASON_GAMES,
            "role_pa_per_g_ratio": ROLE_PA_PER_G_RATIO,
            "role_min_games": ROLE_MIN_GAMES,
            "share_drop_points": SHARE_DROP_POINTS,
            "share_min_team_games": SHARE_MIN_TEAM_GAMES,
            "cadence": (
                "flag once when he qualifies, then update after "
                f"{REREPORT_DAYS['hitter']}d (hitters) / "
                f"{REREPORT_DAYS['pitcher']}d (pitchers)"
            ),
        },
        "counts": counts,
        "players": [
            dict(v, posted_today=v["player_name"] in alerted_names) for v in verdicts
        ],
    }

    if args.dry:
        print(json.dumps(snapshot["counts"], indent=2))
        print(
            build_slack_text(alerts, len(verdicts), suppressed, stale_as_of)
            if alerts
            else "(nothing actionable — would send nothing)"
        )
        return 0

    _SNAPSHOT_PATH.write_text(json.dumps(snapshot, indent=2))
    _STATE_PATH.write_text(
        json.dumps(
            build_state(verdicts, state, alerted_names, today), indent=2, sort_keys=True
        )
    )

    if not alerts:
        # Silent when healthy.
        return 0
    return post_slack(
        build_slack_text(alerts, len(verdicts), suppressed, stale_as_of)
    )


if __name__ == "__main__":
    sys.exit(main())
