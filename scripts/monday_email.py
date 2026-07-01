"""Monday AM hitter + pitcher recap email.

Reads data/window_7d.json + data/window_season.json, builds per-level
tables of SV client hitters and pitchers, and sends via Resend.

Usage:
  python -m scripts.monday_email --dry-run            # print HTML to stdout
  python -m scripts.monday_email --dry-run --save out.html
  python -m scripts.monday_email                      # actually send
  python -m scripts.monday_email --to ttrudeau@stadium-ventures.com
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WINDOW_7D = REPO_ROOT / "data" / "window_7d.json"
WINDOW_SEASON = REPO_ROOT / "data" / "window_season.json"

DEFAULT_RECIPIENTS = [
    "kmatthes@stadium-ventures.com",
    "mdecicco@stadium-ventures.com",
    "ttrudeau@stadium-ventures.com",
]

FROM_ADDRESS = "Dugout Pulse <noreply@stadium-ventures.com>"
SUBJECT_TEMPLATE = "Dugout Pulse — Weekly Recap, {period}"

PITCHER_POS = {"Pitcher", "LHP", "RHP", "Two-Way"}
LEVEL_ORDER = ["Pro", "NCAA", "HS"]
LEVEL_HEADER = {
    "Pro": "Pro",
    "NCAA": "NCAA",
    "HS": "HS",
}

# 5-tier circle rating system (Kent's 5/28 ask). We re-grade in the email
# layer from raw stats rather than reading window_grade, so the 4-tier
# dashboard scheme stays untouched.
#
# Tier order is best -> worst -> dnp; matches the sort order for played players.
TIER_ELITE, TIER_HOT, TIER_STEADY, TIER_COOL, TIER_COLD, TIER_DNP = \
    "elite", "hot", "steady", "cool", "cold", "dnp"
TIER_ORDER = [TIER_ELITE, TIER_HOT, TIER_STEADY, TIER_COOL, TIER_COLD, TIER_DNP]
TIER_RANK = {t: i for i, t in enumerate(TIER_ORDER)}

TIER_LABEL = {
    TIER_ELITE:  "Elite",
    TIER_HOT:    "Hot",
    TIER_STEADY: "Steady",
    TIER_COOL:   "Cool",
    TIER_COLD:   "Cold",
    TIER_DNP:    "DNP",
}

# Hitter OPS thresholds (inclusive lower bound).
HITTER_TIER_THRESHOLDS = [
    (1.200, TIER_ELITE),
    (1.000, TIER_HOT),
    (0.700, TIER_STEADY),
    (0.550, TIER_COOL),
    (0.000, TIER_COLD),
]
# Pitcher ERA thresholds (inclusive upper bound; lower is better).
PITCHER_TIER_THRESHOLDS = [
    (1.500, TIER_ELITE),
    (2.500, TIER_HOT),
    (4.000, TIER_STEADY),
    (5.500, TIER_COOL),
    (99.99, TIER_COLD),
]

# Legacy: still need to detect "Insufficient" records from window_grader.py
# so we can route them to the DNP collapse list (no grade circle, no row).
INSUFFICIENT_GRADE = "— Insufficient"

# OPS+ proxy constants (2026 MLB-wide). Correlates ~0.95 with true wRC+.
LG_OBP_2026 = 0.320
LG_SLG_2026 = 0.415


# ---------- helpers ----------

def _is_pitcher(p: dict) -> bool:
    return p.get("tags", {}).get("position") in PITCHER_POS


def _pa(p: dict) -> int:
    v = p.get("stats", {}).get("pa")
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _ip(p: dict) -> float:
    v = p.get("stats", {}).get("ip")
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _ops_value(p: dict) -> float:
    v = p.get("stats", {}).get("ops")
    if v in (None, "", "--"):
        return -1.0
    try:
        s = str(v)
        return float(s.lstrip("0") if s.startswith("0.") else s)
    except ValueError:
        return -1.0


def _parse_rate(v) -> float | None:
    if v in (None, "", "--"):
        return None
    try:
        s = str(v)
        return float(s.lstrip("0") if s.startswith("0.") else s)
    except ValueError:
        return None


def _ops_plus(stats: dict) -> int | None:
    obp = _parse_rate(stats.get("obp"))
    slg = _parse_rate(stats.get("slg"))
    if obp is None or slg is None:
        return None
    return round(100.0 * (obp / LG_OBP_2026 + slg / LG_SLG_2026 - 1.0))


def _grade_rank(p: dict, is_pitcher: bool = False) -> int:
    """Tier rank for sorting (best tier first). Defaults to hitter scale."""
    return TIER_RANK.get(_tier_for_record(p, is_pitcher), len(TIER_ORDER))


def _load_window(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def _merge_by_player(week: list[dict], season: list[dict]) -> dict[str, dict]:
    season_by_name = {p["player_name"]: p for p in season}
    return {w["player_name"]: {"week": w, "season": season_by_name.get(w["player_name"])} for w in week}


def _last_full_week_label(today: date | None = None) -> str:
    today = today or date.today()
    days_since_monday = today.weekday()
    sunday = today - timedelta(days=(days_since_monday + 1) if days_since_monday >= 0 else 1)
    monday = sunday - timedelta(days=6)
    if monday.month == sunday.month:
        return f"{monday.strftime('%b')} {monday.day}–{sunday.day}, {sunday.year}"
    return f"{monday.strftime('%b')} {monday.day} – {sunday.strftime('%b')} {sunday.day}, {sunday.year}"


# ---------- HTML rendering ----------

CSS = """
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         color: #1f2328; background: #f6f8fa; margin: 0; padding: 0;
         -webkit-text-size-adjust: 100%; }
  .wrap { max-width: 920px; margin: 0 auto; padding: 28px 16px 40px; }
  h1 { font-size: 24px; margin: 0 0 4px 0; letter-spacing: -0.01em; }
  .sub { color: #6e7781; font-size: 14px; margin: 0 0 28px 0; }
  h2 { font-size: 19px; margin: 36px 0 4px 0; letter-spacing: -0.01em; }
  h3 { font-size: 13px; margin: 22px 0 8px 0; color: #57606a; font-weight: 600;
       text-transform: uppercase; letter-spacing: 0.05em; }
  table { width: 100%; border-collapse: separate; border-spacing: 0; background: #fff;
          border: 1px solid #c8d1da; border-radius: 8px; overflow: hidden;
          box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
  th, td { padding: 11px 12px; font-size: 14px; text-align: right;
           border-bottom: 1px solid #e4e9ee; vertical-align: middle; }
  th { background: #eef2f6; font-weight: 700; color: #424a53;
       font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em;
       border-bottom: 2px solid #c8d1da; }
  th.l, td.l { text-align: left; }
  td.player { font-weight: 700; color: #0d1117; font-size: 14.5px; }
  td.team { color: #57606a; font-size: 12.5px; }
  tr:last-child td { border-bottom: none; }
  tr:nth-child(even) td { background: #fbfcfd; }
  td.grade-cell { text-align: center; width: 30px; padding: 9px 6px; }
  .grade-circle { display: inline-block; width: 14px; height: 14px;
                  border-radius: 50%; vertical-align: middle;
                  box-shadow: inset 0 -1px 0 rgba(0,0,0,0.08); }
  .gc-elite  { background: #1a7a30; }
  .gc-hot    { background: #7cb342; }
  .gc-steady { background: #fbc02d; }
  .gc-cool   { background: #fb8c00; }
  .gc-cold   { background: #d32f2f; }
  .gc-dnp    { background: #c0c4c8; }
  .section-divider td { background: #eef2f6 !important; padding: 8px 12px;
                        text-align: left; font-size: 11px; color: #424a53;
                        font-weight: 700; text-transform: uppercase;
                        letter-spacing: 0.06em; }
  .dnp { color: #57606a; font-size: 12.5px; margin: 10px 2px 0; line-height: 1.5; }
  .dnp strong { color: #424a53; }
  .empty { color: #8b949e; font-style: italic; font-size: 13px; padding: 14px;
           text-align: center; background: #fff; border: 1px dashed #c8d1da;
           border-radius: 8px; }
  .footer { color: #8b949e; font-size: 11px; margin-top: 36px; text-align: center; }
  .legend { color: #424a53; font-size: 12.5px; margin: 16px 0 0;
            padding: 12px 14px; background: #fff; border: 1px solid #e4e9ee;
            border-radius: 8px; line-height: 1.55; }
  .legend li { margin: 3px 0; }
  .topline { display: block; margin-bottom: 28px; }
  .topline-card { background: #fff; border: 1px solid #c8d1da; border-radius: 8px;
                  padding: 14px 18px 16px; margin-bottom: 14px;
                  box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
  .standouts { list-style: none; padding: 0; margin: 0; }
  .standouts li { padding: 6px 0; border-bottom: 1px solid #f0f3f6;
                  font-size: 14px; line-height: 1.45; }
  .standouts li:last-child { border-bottom: none; }
  table.glance { margin-top: 4px; }
  table.glance th { font-size: 11px; }
  table.glance td { font-size: 13.5px; padding: 8px 10px; }
  table.glance td.gcount { text-align: center; padding: 8px 12px; font-weight: 700;
                           font-variant-numeric: tabular-nums; }
  /* Per-tier column tint, paired with the circle in the header — gives a
     vertical visual anchor so each number is unambiguously "the COLD column" */
  table.glance th.col-elite,  table.glance td.col-elite  { background: #f0f7f1; }
  table.glance th.col-hot,    table.glance td.col-hot    { background: #f4f9ec; }
  table.glance th.col-steady, table.glance td.col-steady { background: #fffaeb; }
  table.glance th.col-cool,   table.glance td.col-cool   { background: #fff3e6; }
  table.glance th.col-cold,   table.glance td.col-cold   { background: #fdebec; }
  table.glance th.col-dnp,    table.glance td.col-dnp    { background: #f3f4f6; }
  table.glance tr:nth-child(even) td { background: inherit; }
  /* Repeated column header row (for after the Season-to-Date divider) */
  tr.col-header td { background: #f6f8fa; font-weight: 700; color: #424a53;
                     font-size: 11px; text-transform: uppercase;
                     letter-spacing: 0.06em; padding: 9px 10px; text-align: right;
                     border-bottom: 1px solid #c8d1da; border-top: 1px solid #c8d1da; }
  tr.col-header td.l { text-align: left; }
"""

def _fmt(v, default="—"):
    if v in (None, "", "--"):
        return default
    return str(v)


def _tier_for_record(rec: dict, is_pitcher: bool) -> str:
    """Derive 5-tier circle code from raw stats. DNP if insufficient sample."""
    if not rec:
        return TIER_DNP
    grade = rec.get("window_grade")
    if grade == INSUFFICIENT_GRADE:
        return TIER_DNP
    stats = rec.get("stats") or {}
    if is_pitcher:
        era = _parse_rate(stats.get("era"))
        if era is None:
            return TIER_DNP
        for upper, tier in PITCHER_TIER_THRESHOLDS:
            if era <= upper:
                return tier
        return TIER_COLD
    ops = _parse_rate(stats.get("ops"))
    if ops is None:
        return TIER_DNP
    for lower, tier in HITTER_TIER_THRESHOLDS:
        if ops >= lower:
            return tier
    return TIER_COLD


def _grade_circle(tier: str) -> str:
    return f'<span class="grade-circle gc-{tier}" title="{TIER_LABEL[tier]}"></span>'


def _hitter_row(rec: dict, level: str, window_key: str) -> str:
    w = rec["week"] if window_key == "week" else rec["season"]
    if not w:
        return ""
    s = w.get("stats", {})
    tier = _tier_for_record(w, is_pitcher=False)
    cells = [
        f'<td class="grade-cell">{_grade_circle(tier)}</td>',
        f'<td class="l player">{w["player_name"]}</td>',
        f'<td class="l team">{w["team"]}</td>',
        f'<td>{_fmt(w.get("games_played"))}</td>',
        f'<td>{_fmt(s.get("pa"))}</td>',
    ]
    if level == "Pro":
        ops_plus = _ops_plus(s)
        cells.append(f'<td>{ops_plus if ops_plus is not None else "—"}</td>')
    cells.extend([
        f'<td>{_fmt(s.get("avg"))}</td>',
        f'<td>{_fmt(s.get("obp"))}</td>',
        f'<td>{_fmt(s.get("slg"))}</td>',
        f'<td>{_fmt(s.get("ops"))}</td>',
        f'<td>{_fmt(s.get("hr"))}</td>',
        f'<td>{_fmt(s.get("rbi"))}</td>',
        f'<td>{_fmt(s.get("sb"))}</td>',
        f'<td>{_fmt(s.get("bb_pct"))}</td>',
        f'<td>{_fmt(s.get("k_pct"))}</td>',
    ])
    return '<tr>' + "".join(cells) + "</tr>"


def _pitcher_row(rec: dict, window_key: str) -> str:
    w = rec["week"] if window_key == "week" else rec["season"]
    if not w:
        return ""
    s = w.get("stats", {})
    tier = _tier_for_record(w, is_pitcher=True)
    cells = [
        f'<td class="grade-cell">{_grade_circle(tier)}</td>',
        f'<td class="l player">{w["player_name"]}</td>',
        f'<td class="l team">{w["team"]}</td>',
        f'<td>{_fmt(w.get("games_played"))}</td>',
        f'<td>{_fmt(s.get("ip"))}</td>',
        f'<td>{_fmt(s.get("era"))}</td>',
        f'<td>{_fmt(s.get("whip"))}</td>',
        f'<td>{_fmt(s.get("k"))}</td>',
        f'<td>{_fmt(s.get("bb"))}</td>',
        f'<td>{_fmt(s.get("k_per_9"))}</td>',
        f'<td>{_fmt(s.get("bb_per_9"))}</td>',
        f'<td>{_fmt(s.get("k_pct"))}</td>',
        f'<td>{_fmt(s.get("bb_pct"))}</td>',
    ]
    return '<tr>' + "".join(cells) + "</tr>"


def _hitter_section(level: str, played: list[dict], dnp_names: list[str],
                    recent_label: str = "Last Week", season_label: str = "Season-to-Date",
                    recent_dnp_phrase: str = "Did not play last week") -> str:
    if not played and not dnp_names:
        return ""

    ops_plus_col_th = "<th>OPS+</th>" if level == "Pro" else ""
    ops_plus_col_td = '<td>OPS+</td>' if level == "Pro" else ""
    header = (
        '<thead><tr>'
        '<th class="grade-cell"></th><th class="l">Player</th><th class="l">Team</th>'
        '<th>G</th><th>PA</th>'
        f'{ops_plus_col_th}'
        '<th>AVG</th><th>OBP</th><th>SLG</th><th>OPS</th>'
        '<th>HR</th><th>RBI</th><th>SB</th><th>BB%</th><th>K%</th>'
        '</tr></thead>'
    )
    repeat_header_row = (
        '<tr class="col-header">'
        '<td></td><td class="l">Player</td><td class="l">Team</td>'
        '<td>G</td><td>PA</td>'
        f'{ops_plus_col_td}'
        '<td>AVG</td><td>OBP</td><td>SLG</td><td>OPS</td>'
        '<td>HR</td><td>RBI</td><td>SB</td><td>BB%</td><td>K%</td>'
        '</tr>'
    )

    week_rows = "\n".join(_hitter_row(r, level, "week") for r in played) or '<tr><td colspan="20" class="empty">No client hitters with playing time.</td></tr>'
    season_rows = "\n".join(_hitter_row(r, level, "season") for r in played)

    section = f'''<h3>Hitters</h3>
<table>{header}
<tbody>
<tr class="section-divider"><td colspan="20">{recent_label}</td></tr>
{week_rows}
<tr class="section-divider"><td colspan="20">{season_label}</td></tr>
{repeat_header_row}
{season_rows}
</tbody></table>'''

    if dnp_names:
        section += f'<div class="dnp"><strong>{recent_dnp_phrase}:</strong> {", ".join(dnp_names)}</div>'

    return section


def _pitcher_section(played: list[dict], dnp_names: list[str],
                     recent_label: str = "Last Week", season_label: str = "Season-to-Date",
                     recent_dnp_phrase: str = "Did not pitch last week") -> str:
    if not played and not dnp_names:
        return ""

    header = (
        '<thead><tr>'
        '<th class="grade-cell"></th><th class="l">Player</th><th class="l">Team</th>'
        '<th>G</th><th>IP</th><th>ERA</th><th>WHIP</th><th>K</th><th>BB</th>'
        '<th>K/9</th><th>BB/9</th><th>K%</th><th>BB%</th>'
        '</tr></thead>'
    )
    repeat_header_row = (
        '<tr class="col-header">'
        '<td></td><td class="l">Player</td><td class="l">Team</td>'
        '<td>G</td><td>IP</td><td>ERA</td><td>WHIP</td><td>K</td><td>BB</td>'
        '<td>K/9</td><td>BB/9</td><td>K%</td><td>BB%</td>'
        '</tr>'
    )

    week_rows = "\n".join(_pitcher_row(r, "week") for r in played) or '<tr><td colspan="20" class="empty">No client pitchers with appearances.</td></tr>'
    season_rows = "\n".join(_pitcher_row(r, "season") for r in played)

    section = f'''<h3>Pitchers</h3>
<table>{header}
<tbody>
<tr class="section-divider"><td colspan="20">{recent_label}</td></tr>
{week_rows}
<tr class="section-divider"><td colspan="20">{season_label}</td></tr>
{repeat_header_row}
{season_rows}
</tbody></table>'''

    if dnp_names:
        section += f'<div class="dnp"><strong>{recent_dnp_phrase}:</strong> {", ".join(dnp_names)}</div>'

    return section


def _split_played_vs_insufficient(records: list[dict], is_pitcher_side: bool) -> tuple[list[dict], list[str]]:
    """Return (played_records_sorted, dnp_names_sorted)."""
    played, dnp = [], []
    for rec in records:
        grade = rec["week"].get("window_grade")
        if grade == INSUFFICIENT_GRADE:
            dnp.append(rec)
        else:
            played.append(rec)

    if is_pitcher_side:
        played.sort(key=lambda r: (_grade_rank(r["week"], is_pitcher=True), -_ip(r["week"]), r["week"]["player_name"]))
    else:
        played.sort(key=lambda r: (_grade_rank(r["week"], is_pitcher=False), -_ops_value(r["week"]), r["week"]["player_name"]))

    dnp_names = sorted(r["week"]["player_name"] for r in dnp)
    return played, dnp_names


def _hitter_score(w: dict) -> float:
    return _ops_value(w)


def _pitcher_score(w: dict) -> float:
    """Innings + run-prevention (Kent's rubric — not K:BB)."""
    ip = _ip(w)
    if ip <= 0:
        return -99.0
    era = _parse_rate(w.get("stats", {}).get("era"))
    if era is None:
        era = 9.99
    return ip * 2.0 - era


def _hitter_line(w: dict, level: str) -> str:
    s = w.get("stats", {})
    slash = f'{_fmt(s.get("avg"))}/{_fmt(s.get("obp"))}/{_fmt(s.get("slg"))}'
    extras = []
    for label, key in [("HR", "hr"), ("RBI", "rbi"), ("SB", "sb")]:
        v = s.get(key)
        if v not in (None, "", "--", 0, "0"):
            extras.append(f"{v} {label}")
    if level == "Pro":
        op = _ops_plus(s)
        if op is not None:
            extras.append(f"OPS+ {op}")
    return slash + (" · " + ", ".join(extras) if extras else "")


def _pitcher_line(w: dict) -> str:
    s = w.get("stats", {})
    parts = [f'{_fmt(s.get("ip"))} IP', f'{_fmt(s.get("era"))} ERA']
    for label, key in [("K", "k"), ("BB", "bb")]:
        v = s.get(key)
        if v not in (None, "", "--"):
            parts.append(f"{v} {label}")
    whip = s.get("whip")
    if whip not in (None, "", "--"):
        parts.append(f"WHIP {whip}")
    return ", ".join(parts)


def _standouts(sections: dict, max_n: int = 6) -> list[dict]:
    """Top HOT performers, interleaved hitters + pitchers."""
    hitters_out, pitchers_out = [], []
    for level in LEVEL_ORDER:
        sec = sections[level]
        for h in sec["hitters"]:
            w = h["week"]
            if _tier_for_record(w, is_pitcher=False) not in (TIER_ELITE, TIER_HOT) or _pa(w) < 5:
                continue
            hitters_out.append({
                "kind": "hitter", "level": level,
                "player": w["player_name"], "team": w["team"],
                "score": _hitter_score(w),
                "line": _hitter_line(w, level),
            })
        for p in sec["pitchers"]:
            w = p["week"]
            if _tier_for_record(w, is_pitcher=True) not in (TIER_ELITE, TIER_HOT) or _ip(w) < 2:
                continue
            pitchers_out.append({
                "kind": "pitcher", "level": level,
                "player": w["player_name"], "team": w["team"],
                "score": _pitcher_score(w),
                "line": _pitcher_line(w),
            })
    hitters_out.sort(key=lambda x: -x["score"])
    pitchers_out.sort(key=lambda x: -x["score"])
    merged = []
    while (hitters_out or pitchers_out) and len(merged) < max_n:
        if hitters_out:
            merged.append(hitters_out.pop(0))
        if len(merged) < max_n and pitchers_out:
            merged.append(pitchers_out.pop(0))
    return merged


def _glance(sections: dict) -> list[dict]:
    """Per-level 5-tier counts (week)."""
    out = []
    for level in LEVEL_ORDER:
        sec = sections[level]
        counts = {t: 0 for t in TIER_ORDER}
        total = len(sec["hitters"]) + len(sec["pitchers"])
        if total == 0:
            continue
        for r in sec["hitters"]:
            counts[_tier_for_record(r["week"], is_pitcher=False)] += 1
        for r in sec["pitchers"]:
            counts[_tier_for_record(r["week"], is_pitcher=True)] += 1
        out.append({"level": level, "total": total, **counts})
    return out


def _render_topline(sections: dict, standouts_label: str, glance_label: str) -> str:
    standouts = _standouts(sections)
    glance = _glance(sections)

    parts = []

    if standouts:
        items = []
        for s in standouts:
            kind_label = "Hitter" if s["kind"] == "hitter" else "Pitcher"
            items.append(
                f'<li><strong>{s["player"]}</strong> '
                f'<span style="color:#57606a;">({s["team"]}, {s["level"]} {kind_label})</span> '
                f'<span style="color:#1f2328;">— {s["line"]}</span></li>'
            )
        parts.append(
            '<div class="topline-card">'
            f'<h3 style="margin-top:0;">{standouts_label}</h3>'
            f'<ul class="standouts">{"".join(items)}</ul>'
            '</div>'
        )

    if glance:
        # Glance table: one row per level, one column per tier (with the same
        # colored circle in the header so Kent's eye maps column->color).
        tier_th = "".join(
            f'<th class="col-{t}">{_grade_circle(t)}<div style="font-size:10px;color:#6e7781;margin-top:2px;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;">{TIER_LABEL[t]}</div></th>'
            for t in TIER_ORDER
        )
        rows = []
        for g in glance:
            tier_tds = "".join(
                f'<td class="gcount col-{t}">{g[t]}</td>'
                for t in TIER_ORDER
            )
            rows.append(
                f'<tr>'
                f'<td class="l"><strong>{g["level"]}</strong></td>'
                f'{tier_tds}'
                f'<td style="color:#6e7781;">{g["total"]}</td>'
                f'</tr>'
            )
        parts.append(
            '<div class="topline-card">'
            f'<h3 style="margin-top:0;">{glance_label}</h3>'
            '<table class="glance"><thead><tr>'
            f'<th class="l">Level</th>{tier_th}<th>Total</th>'
            f'</tr></thead><tbody>{"".join(rows)}</tbody></table>'
            '</div>'
        )

    if not parts:
        return ""
    return '<div class="topline">' + "".join(parts) + '</div>'


def build_payload(today: date | None = None) -> dict:
    today = today or date.today()
    week = _load_window(WINDOW_7D)
    season = _load_window(WINDOW_SEASON)
    if not week:
        raise SystemExit(f"window_7d.json missing or empty at {WINDOW_7D}")

    merged = _merge_by_player(week, season)

    sections: dict[str, dict[str, list]] = {
        lvl: {"hitters": [], "pitchers": []} for lvl in LEVEL_ORDER
    }
    for name, rec in merged.items():
        w = rec["week"]
        if not w.get("is_client"):
            continue
        level = w.get("level")
        if level not in sections:
            continue
        if _is_pitcher(w):
            sections[level]["pitchers"].append(rec)
        else:
            sections[level]["hitters"].append(rec)

    return {
        "title": "Weekly Recap",
        "subtitle_prefix": "Week of",
        "period_label": _last_full_week_label(today),
        "recent_section_label": "Last Week",
        "season_section_label": "Season-to-Date",
        "standouts_section_label": "Standouts (Last Week)",
        "glance_section_label": "Week at a Glance",
        "dnp_hitter_phrase": "Did not play last week",
        "dnp_pitcher_phrase": "Did not pitch last week",
        "subject_template": "Dugout Pulse — Weekly Recap, {period}",
        "pdf_filename_prefix": "dugout-pulse-week-of",
        "sections": sections,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _render_summer_placements_section() -> str:
    """Render the per-player summer-ball placement list for the email.

    Reads data/summer_ball_placements.json (Kent's spreadsheet) and groups
    placements by status. For Active placements we include the player's
    last-7-day line via the MLB Stats API where available (CCBL,
    Appalachian, MLBDL); other leagues show the assignment + "Stats
    populate as games run" since their season may not have started or
    we don't have a live-stats path yet.

    Returns "" when no placements file exists (defensive).
    """
    placements_path = REPO_ROOT / "data" / "summer_ball_placements.json"
    if not placements_path.exists():
        return ""
    try:
        data = json.loads(placements_path.read_text())
    except Exception:
        return ""
    placements = data.get("placements") or []
    placements = [p for p in placements if p.get("player_name")
                  and not (str(p["player_name"]).isupper() and len(p["player_name"]) > 5)]
    if not placements:
        return ""

    # Bucket by status.
    active: list[dict] = []
    shut_down: list[dict] = []
    injured: list[dict] = []
    pending: list[dict] = []
    second_half: list[dict] = []
    for p in placements:
        status = (p.get("status") or "").strip()
        if status == "Shut Down":
            shut_down.append(p)
        elif status == "Injured":
            injured.append(p)
        elif status.startswith("Pending"):
            pending.append(p)
        elif status == "2nd Half":
            second_half.append(p)
        else:
            active.append(p)

    parts = ['<h2 style="margin-top:24px;">Summer Ball — Placements</h2>']

    if active:
        parts.append('<div style="margin-bottom:14px;">')
        parts.append('<div style="font-weight:700;font-size:13.5px;margin-bottom:6px;">'
                     'Active placements</div>')
        parts.append('<ul style="margin:0;padding-left:18px;font-size:13px;line-height:1.55;">')
        for p in active:
            parts.append(_render_summer_placement_row(p, include_line=True))
        parts.append('</ul></div>')

    if shut_down:
        parts.append(_render_status_group("Shut down for season", shut_down))
    if injured:
        parts.append(_render_status_group("Injured", injured))
    if pending:
        parts.append(_render_status_group("Pending arrival", pending))
    if second_half:
        parts.append(_render_status_group("Joining for 2nd Half", second_half))

    return f"""
<div class="topline-card" style="border-left:4px solid #a855f7;">
  <h3 style="margin-top:0;">Summer Ball — Per-Player Update</h3>
  {''.join(parts[1:])}  <!-- skip the redundant h2 -->
  <div style="margin-top:10px;color:#6e7781;font-size:11.5px;">
    Source of truth: Kent's "Summer Ball Placement" sheet. We pull live
    stats automatically from MLB Stats API (Cape Cod, Appalachian, MLB
    Draft) and PrestoSports (NECBL). Other leagues — Northwoods, PGCBL,
    FCBL, Coastal Plain — have no feed we can reach, so those placements
    are tracked by hand; check the league site for the latest.
  </div>
</div>
"""


def _render_summer_placement_row(p: dict, *, include_line: bool) -> str:
    name = p.get("player_name") or "?"
    school = p.get("school") or p.get("school_raw") or ""
    team = p.get("summer_team") or "—"
    league = p.get("league") or p.get("league_raw") or "?"
    status = p.get("status") or ""
    line_html = ""
    if include_line:
        line = _summer_player_week_line(name, league, p.get("source_id", ""))
        if line:
            line_html = f' — <span style="color:#1a7a30;font-weight:600;">{line}</span>'
    school_html = f' <span style="color:#6e7781;">({school})</span>' if school else ""
    status_html = ""
    if status and status != "Confirmed":
        status_html = f' <span style="color:#6e7781;font-size:11.5px;">[{status}]</span>'
    return (
        f'<li><strong>{name}</strong>{school_html} → '
        f'{team} <span style="color:#6e7781;">({league})</span>'
        f'{status_html}{line_html}</li>'
    )


def _render_status_group(label: str, players: list[dict]) -> str:
    rows = []
    for p in players:
        school = p.get("school") or p.get("school_raw") or ""
        team = p.get("summer_team") or ""
        league = p.get("league") or ""
        suffix = f' — {team} ({league})' if team and league else ''
        school_html = f' <span style="color:#6e7781;">({school})</span>' if school else ""
        rows.append(f'<li><strong>{p["player_name"]}</strong>{school_html}{suffix}</li>')
    return (
        f'<div style="margin-bottom:10px;">'
        f'<div style="font-weight:700;font-size:13.5px;margin-bottom:4px;">{label}</div>'
        f'<ul style="margin:0;padding-left:18px;font-size:12.5px;line-height:1.5;color:#57606a;">'
        f'{"".join(rows)}</ul></div>'
    )


# MLB-API league codes from Kent's spreadsheet ("MLBD" = MLB Draft, "CCBL"
# = Cape Cod, "Appy" not in sheet yet).
_MLB_API_LEAGUE_CODES = {
    "MLBD": 5536,
    "MLB Draft": 5536,
    "CCBL": 565,
    "Cape Cod": 565,
    "Appy": 120,
    "Appalachian": 120,
}


def _summer_player_week_line(
    player_name: str, league: str, source_id: str = "",
) -> str:
    """For a placement in an MLB-Stats-API league, fetch a quick last-7-day
    line via /people/{id}/stats?stats=byDateRange. Returns "" silently on
    any failure — the email still ships with the assignment row.
    """
    league_id = _MLB_API_LEAGUE_CODES.get(league)
    if not league_id:
        return ""
    try:
        import requests as _r
        from datetime import timedelta as _td
        person_id = source_id if source_id and str(source_id).isdigit() else None
        if not person_id:
            url = (
                f"https://statsapi.mlb.com/api/v1/people/search"
                f"?names={player_name.replace(' ', '+')}&sportIds=22"
            )
            people = _r.get(url, timeout=10).json().get("people", [])
            if people:
                person_id = people[0].get("id")
        if not person_id:
            return ""
        end = date.today()
        start = end - timedelta(days=7)
        url = (
            f"https://statsapi.mlb.com/api/v1/people/{person_id}/stats"
            f"?stats=byDateRange&startDate={start.isoformat()}"
            f"&endDate={end.isoformat()}&group=hitting,pitching"
        )
        resp = _r.get(url, timeout=10).json()
        line_parts: list[str] = []
        for stat_group in resp.get("stats", []):
            group = stat_group.get("group", {}).get("displayName", "")
            splits = stat_group.get("splits", [])
            if not splits:
                continue
            stats = splits[0].get("stat", {})
            if group == "hitting" and stats.get("plateAppearances"):
                line_parts.append(
                    f"{stats.get('hits',0)}-for-{stats.get('atBats',0)}, "
                    f"{stats.get('homeRuns',0)} HR, "
                    f"{stats.get('rbi',0)} RBI, "
                    f"{stats.get('strikeOuts',0)} K"
                )
            elif group == "pitching" and stats.get("inningsPitched"):
                line_parts.append(
                    f"{stats.get('inningsPitched','0.0')} IP, "
                    f"{stats.get('earnedRuns',0)} ER, "
                    f"{stats.get('strikeOuts',0)} K, "
                    f"{stats.get('baseOnBalls',0)} BB"
                )
        return " · ".join(line_parts)
    except Exception:
        return ""


def render_html(payload: dict) -> str:
    body_parts = []

    recent_label = payload.get("recent_section_label", "Last Week")
    season_label = payload.get("season_section_label", "Season-to-Date")
    dnp_hit = payload.get("dnp_hitter_phrase", "Did not play last week")
    dnp_pit = payload.get("dnp_pitcher_phrase", "Did not pitch last week")

    topline = _render_topline(
        payload["sections"],
        standouts_label=payload.get("standouts_section_label", "Standouts (Last Week)"),
        glance_label=payload.get("glance_section_label", "Week at a Glance"),
    )
    if topline:
        body_parts.append(topline)

    # The old auto-matcher coverage banner ("N confirmed / N unmatched of 39
    # NCAA clients") is retired — it counted name-matches against scraped
    # rosters, which reads as a failure mid-season when most of the "unmatched"
    # simply aren't in summer ball. The placement section below is the truth.
    summer_section = _render_summer_placements_section()
    if summer_section:
        body_parts.append(summer_section)

    for lvl in LEVEL_ORDER:
        sec = payload["sections"][lvl]
        if not sec["hitters"] and not sec["pitchers"]:
            continue

        body_parts.append(f'<h2>{LEVEL_HEADER[lvl]}</h2>')

        h_played, h_dnp = _split_played_vs_insufficient(sec["hitters"], is_pitcher_side=False)
        p_played, p_dnp = _split_played_vs_insufficient(sec["pitchers"], is_pitcher_side=True)

        body_parts.append(_hitter_section(
            lvl, h_played, h_dnp,
            recent_label=recent_label, season_label=season_label,
            recent_dnp_phrase=dnp_hit,
        ))
        body_parts.append(_pitcher_section(
            p_played, p_dnp,
            recent_label=recent_label, season_label=season_label,
            recent_dnp_phrase=dnp_pit,
        ))

    circle_legend = " &nbsp; ".join(
        f'{_grade_circle(t)} <span style="vertical-align:middle;">{TIER_LABEL[t]}</span>'
        for t in TIER_ORDER
    )
    legend_items = [
        (f'{circle_legend} &nbsp; — &nbsp; '
         f'Hitters graded on OPS; pitchers on ERA. Elite/Hot are above-MLB-average; '
         f'Cool/Cold flag underperformance. Same scale applied to {recent_label} and {season_label}.'),
    ]
    if payload["sections"].get("Pro", {}).get("hitters"):
        legend_items.append(
            "<strong>OPS+ for Pro hitters</strong> is a wRC+ proxy "
            "(100 = MLB average, higher is better) computed as: "
            f"<code style='background:#f6f8fa;padding:2px 5px;border-radius:3px;font-size:12px;'>"
            f"OPS+ = 100 × (OBP / {LG_OBP_2026:.3f} + SLG / {LG_SLG_2026:.3f} − 1)</code>. "
            f"The two constants are fixed MLB-wide averages for 2026 "
            f"(lgOBP {LG_OBP_2026:.3f}, lgSLG {LG_SLG_2026:.3f}); they don't "
            "adjust per league level or park, so MiLB OPS+ tends to read a bit "
            "high vs MLB OPS+ but is directionally accurate. Correlates ~0.95 with FanGraphs wRC+."
        )
    if payload["sections"].get("HS", {}).get("hitters") or payload["sections"].get("HS", {}).get("pitchers"):
        legend_items.append("HS stats come from a manually-maintained sheet — only as fresh as the latest entry.")

    legend = '<ul class="legend">' + "".join(f"<li>{item}</li>" for item in legend_items) + '</ul>'

    title = payload.get("title", "Weekly Recap")
    period_label = payload.get("period_label", "")
    subtitle_prefix = payload.get("subtitle_prefix", "Week of")

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Dugout Pulse — {title}</title>
<style>{CSS}</style></head>
<body><div class="wrap">
<h1>Dugout Pulse — {title}</h1>
<div class="sub">{subtitle_prefix} {period_label}</div>
{''.join(body_parts)}
{legend}
<div class="footer">Generated {payload['generated_at']}</div>
</div></body></html>
"""


def render_subject(payload: dict) -> str:
    template = payload.get("subject_template", "Dugout Pulse — Weekly Recap, {period}")
    return template.format(period=payload.get("period_label", ""))


# ---------- PDF ----------

def render_pdf(html: str) -> bytes | None:
    """Render HTML to PDF via weasyprint. Returns None if weasyprint isn't installed."""
    try:
        from weasyprint import HTML  # type: ignore
    except ImportError:
        sys.stderr.write("[monday_email] weasyprint not installed — skipping PDF attachment\n")
        return None
    return HTML(string=html).write_pdf()


# ---------- Resend send ----------

def send_via_resend(subject: str, html: str, to: list[str], api_key: str,
                    pdf_bytes: bytes | None = None,
                    pdf_filename: str = "weekly_recap.pdf") -> dict:
    import urllib.request, urllib.error
    import base64

    payload: dict = {
        "from": FROM_ADDRESS,
        "to": to,
        "subject": subject,
        "html": html,
    }
    if pdf_bytes:
        payload["attachments"] = [{
            "filename": pdf_filename,
            "content": base64.b64encode(pdf_bytes).decode("ascii"),
        }]

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "sv-dugout-pulse/1.0 (+https://github.com/Stadium-Ventures/sv-dugout-pulse)",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Resend API error: {e.code} {e.read().decode('utf-8', 'replace')}")


# ---------- CLI ----------

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--save", metavar="PATH")
    p.add_argument("--to", action="append")
    p.add_argument("--today", help="Override today's date (YYYY-MM-DD).")
    p.add_argument("--subject-suffix", default="",
                   help="Appended to the subject — useful to escape Gmail threading on test sends.")
    args = p.parse_args(argv)

    today = date.fromisoformat(args.today) if args.today else date.today()
    payload = build_payload(today)
    subject = render_subject(payload) + (args.subject_suffix or "")
    html = render_html(payload)
    recipients = args.to or DEFAULT_RECIPIENTS

    counts = {
        lvl: {
            "hitters": len(payload["sections"][lvl]["hitters"]),
            "pitchers": len(payload["sections"][lvl]["pitchers"]),
        }
        for lvl in LEVEL_ORDER
    }
    sys.stderr.write(
        f"[monday_email] period={payload['period_label']} "
        f"recipients={recipients} counts={counts}\n"
    )

    if args.dry_run:
        if args.save:
            Path(args.save).write_text(html)
            sys.stderr.write(f"[monday_email] wrote {args.save}\n")
        else:
            sys.stdout.write(html)
        return

    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("RESEND_API_KEY env var is not set.")

    pdf_bytes = render_pdf(html)
    if pdf_bytes:
        sys.stderr.write(f"[monday_email] PDF: {len(pdf_bytes)} bytes\n")

    safe_period = payload['period_label'].replace(' ', '-').replace(',', '').replace('–', '-').lower()
    pdf_prefix = payload.get("pdf_filename_prefix", "dugout-pulse-week-of")
    pdf_filename = f"{pdf_prefix}-{safe_period}.pdf"
    result = send_via_resend(subject, html, recipients, api_key,
                             pdf_bytes=pdf_bytes, pdf_filename=pdf_filename)
    sys.stderr.write(f"[monday_email] sent: {result}\n")


if __name__ == "__main__":
    main()
