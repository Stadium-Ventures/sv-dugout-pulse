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

Three things are worth a call, and all three post:

  🔻 lull   — recent form materially below his own baseline AND landing in
              Steady/Cold. The alert Kent asked for.
  🔺 surge  — recent form materially above baseline AND landing in Solid/Hot.
              This is the Riemer case: the call you make to get a guy noticed,
              not the one you make to defend him.
  😶 idle   — played this year, no games at all in 14 days. Injury, IL, phantom
              IL, benched, or lost the job — the farm director knows which.

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

Posts to #dugout-pulse (`SLACK_WEBHOOK_URL`) — this is feature output an agent
reads on purpose, not an ops finding, per the channel scope rule in CLAUDE.md.
Silent when nothing is actionable.

State: `data/_milb_watch_state.json` (per-player cooldown, so a two-month slump
doesn't re-post every morning).
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

# One alert per player per this many days, per the summer quiet-streak
# precedent (CLAUDE.md / Tom, 2026-07-28).
REALERT_COOLDOWN_DAYS = 10

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

    moved = f"OPS {baseline['ops']} → {recent['ops']} ({delta:+.3f})"
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


_STATUS_ORDER = {"lull": 0, "idle": 1, "surge": 2, "steady": 3, "insufficient": 4}


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
# Cooldown state
# ---------------------------------------------------------------------------

def _today_et_str() -> str:
    return datetime.now(_ET).date().isoformat()


def _days_between(iso_a: str, iso_b: str) -> int:
    try:
        return (date.fromisoformat(iso_b) - date.fromisoformat(iso_a)).days
    except Exception:
        return 999


def due_for_alert(verdict: dict, state: dict, today: str) -> bool:
    """True when this player/status is actionable and off cooldown.

    Cooldown is per status, so a guy who slumps, gets alerted, then breaks out
    two weeks later still generates the surge call.
    """
    if verdict["status"] not in ("lull", "surge", "idle"):
        return False
    prior = state.get(verdict["player_name"]) or {}
    if prior.get("last_alert_status") != verdict["status"]:
        return True
    last = prior.get("last_alert_date")
    if not last:
        return True
    return _days_between(last, today) >= REALERT_COOLDOWN_DAYS


def build_state(verdicts: list, state: dict, alerted_names: set, today: str) -> dict:
    """Carry forward each player's last-alert stamp, updating those that fired."""
    new_state: dict = {}
    for verdict in verdicts:
        name = verdict["player_name"]
        prior = state.get(name) or {}
        entry = {
            "status": verdict["status"],
            "last_seen_date": today,
            "last_alert_date": prior.get("last_alert_date"),
            "last_alert_status": prior.get("last_alert_status"),
        }
        baseline = verdict.get("baseline") or {}
        if "_ops" in baseline:
            entry["baseline_ops"] = baseline["ops"]
        if "_era" in baseline:
            entry["baseline_era"] = baseline["era"]
        if name in alerted_names:
            entry["last_alert_date"] = today
            entry["last_alert_status"] = verdict["status"]
        new_state[name] = entry
    return new_state


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------

_CALL_ANGLE = {
    "lull": "Worth a farm-director check-in — what are they seeing that the line doesn't show?",
    "surge": "Good week to call — this is the stretch you want the front office looking at.",
    "idle": "Confirm the reason before it becomes a surprise — injury, IL, or role change?",
}

_SECTION = [
    ("lull", "🔻 *Lull — off his own season baseline*"),
    ("idle", "😶 *No games in the last 14 days*"),
    ("surge", "🔺 *Trending up — make the call while it's live*"),
]


def _short_team(team: str) -> str:
    """'New York Yankees' → 'Yankees'. Matches the social-URL convention."""
    return team.split()[-1] if team else ""


def build_slack_text(alerts: list, tracked: int) -> str:
    """Compose the #dugout-pulse post. Assumes alerts is non-empty."""
    lines = ["*MiLB watch — recent form vs. season baseline*"]
    for status, heading in _SECTION:
        group = [a for a in alerts if a["status"] == status]
        if not group:
            continue
        lines.append("")
        lines.append(heading)
        for a in group:
            level = a.get("current_level") or "?"
            lines.append(
                f"• *{a['player_name']}* ({_short_team(a['team'])}, {level}) — "
                f"{a['reason']}"
            )
            detail = a.get("detail")
            if detail:
                lines.append(f"    {detail}")
            lines.append(f"    _{_CALL_ANGLE[status]}_")
    lines.append("")
    lines.append(
        f"_Baseline = season to date with the compared window removed · "
        f"14- and 30-day form both checked · {tracked} MiLB clients tracked · "
        f"one alert per player per {REALERT_COOLDOWN_DAYS} days._"
    )
    return "\n".join(lines)


def post_slack(text: str) -> int:
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL not set — would have posted:")
        print(text)
        return 0
    try:
        resp = requests.post(
            webhook,
            json={"text": text},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.error("Slack send failed: %s %s", resp.status_code, resp.text)
            return 1
        return 0
    except Exception:
        logger.exception("Slack send errored")
        return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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

    verdicts = evaluate_all(season, recent_by_window)
    if not verdicts:
        logger.info("No MiLB clients in the season window — skipping")
        return 0

    state = _load_json(_STATE_PATH, {})
    today = _today_et_str()
    alerts = [v for v in verdicts if due_for_alert(v, state, today)]
    alerted_names = {a["player_name"] for a in alerts}

    counts: dict = {}
    for verdict in verdicts:
        counts[verdict["status"]] = counts.get(verdict["status"], 0) + 1
    logger.info(
        "%d MiLB clients tracked (%s) — %d alerting",
        len(verdicts),
        ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())),
        len(alerts),
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
            "realert_cooldown_days": REALERT_COOLDOWN_DAYS,
        },
        "counts": counts,
        "players": [
            dict(v, alerted=v["player_name"] in alerted_names) for v in verdicts
        ],
    }

    if args.dry:
        print(json.dumps(snapshot["counts"], indent=2))
        print(
            build_slack_text(alerts, len(verdicts))
            if alerts
            else "(nothing actionable — would post nothing)"
        )
        return 0

    _SNAPSHOT_PATH.write_text(json.dumps(snapshot, indent=2))
    _STATE_PATH.write_text(
        json.dumps(build_state(verdicts, state, alerted_names, today), indent=2, sort_keys=True)
    )

    if not alerts:
        # Silent when healthy.
        return 0
    return post_slack(build_slack_text(alerts, len(verdicts)))


if __name__ == "__main__":
    sys.exit(main())
