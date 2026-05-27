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
SUBJECT_TEMPLATE = "Dugout Pulse — Weekly Recap, {week}"

PITCHER_POS = {"Pitcher", "LHP", "RHP", "Two-Way"}
LEVEL_ORDER = ["Pro", "NCAA", "HS"]
LEVEL_HEADER = {
    "Pro": "⚾ Pro",
    "NCAA": "🎓 NCAA",
    "HS": "🏫 HS",
}

# Sort order for window_grade values. Grades not in this list fall to the end.
GRADE_RANK = {
    "🔥 Hot": 0,
    "✅ Solid": 1,
    "😐 Steady": 2,
    "🥶 Cold": 3,
}
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


def _grade_rank(p: dict) -> int:
    return GRADE_RANK.get(p.get("window_grade"), 99)


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
  .pill { display: inline-block; padding: 3px 8px; border-radius: 999px;
          font-size: 10.5px; font-weight: 700; letter-spacing: 0.05em;
          text-transform: uppercase; line-height: 1.4; }
  .pill-hot { background: #ffe6cc; color: #b14a00; }
  .pill-solid { background: #d8efd8; color: #1a6b1a; }
  .pill-steady { background: #e1e4e8; color: #4d555c; }
  .pill-cold { background: #d8e7f7; color: #1a4a85; }
  .pill-na { background: #f3f4f6; color: #8b949e; }
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
"""

# Map "🔥 Hot" -> ("HOT", "hot" CSS class). The leading emoji is dropped.
GRADE_PILL = {
    "🔥 Hot": ("HOT", "hot"),
    "✅ Solid": ("SOLID", "solid"),
    "😐 Steady": ("STEADY", "steady"),
    "🥶 Cold": ("COLD", "cold"),
    "— Insufficient": ("—", "na"),
}


def _fmt(v, default="—"):
    if v in (None, "", "--"):
        return default
    return str(v)


def _grade_pill(grade: str | None) -> str:
    if not grade:
        return ""
    label, cls = GRADE_PILL.get(grade, ("—", "na"))
    return f'<span class="pill pill-{cls}">{label}</span>'


def _hitter_row(rec: dict, level: str, window_key: str) -> str:
    w = rec["week"] if window_key == "week" else rec["season"]
    if not w:
        return ""
    s = w.get("stats", {})
    grade = w.get("window_grade") or ""
    cells = [
        f'<td>{_grade_pill(grade)}</td>',
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
    grade = w.get("window_grade") or ""
    cells = [
        f'<td>{_grade_pill(grade)}</td>',
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


def _hitter_section(level: str, played: list[dict], dnp_names: list[str]) -> str:
    if not played and not dnp_names:
        return ""

    ops_plus_col = "<th>OPS+</th>" if level == "Pro" else ""
    header = (
        '<thead><tr>'
        '<th>Grade</th><th class="l">Player</th><th class="l">Team</th>'
        '<th>G</th><th>PA</th>'
        f'{ops_plus_col}'
        '<th>AVG</th><th>OBP</th><th>SLG</th><th>OPS</th>'
        '<th>HR</th><th>RBI</th><th>SB</th><th>BB%</th><th>K%</th>'
        '</tr></thead>'
    )

    week_rows = "\n".join(_hitter_row(r, level, "week") for r in played) or '<tr><td colspan="20" class="empty">No client hitters with playing time.</td></tr>'
    season_rows = "\n".join(_hitter_row(r, level, "season") for r in played)

    section = f'''<h3>Hitters</h3>
<table>{header}
<tbody>
<tr class="section-divider"><td colspan="20">Last Week</td></tr>
{week_rows}
<tr class="section-divider"><td colspan="20">Season-to-Date</td></tr>
{season_rows}
</tbody></table>'''

    if dnp_names:
        section += f'<div class="dnp"><strong>Did not play last week:</strong> {", ".join(dnp_names)}</div>'

    return section


def _pitcher_section(played: list[dict], dnp_names: list[str]) -> str:
    if not played and not dnp_names:
        return ""

    header = (
        '<thead><tr>'
        '<th>Grade</th><th class="l">Player</th><th class="l">Team</th>'
        '<th>G</th><th>IP</th><th>ERA</th><th>WHIP</th><th>K</th><th>BB</th>'
        '<th>K/9</th><th>BB/9</th><th>K%</th><th>BB%</th>'
        '</tr></thead>'
    )

    week_rows = "\n".join(_pitcher_row(r, "week") for r in played) or '<tr><td colspan="20" class="empty">No client pitchers with appearances.</td></tr>'
    season_rows = "\n".join(_pitcher_row(r, "season") for r in played)

    section = f'''<h3>Pitchers</h3>
<table>{header}
<tbody>
<tr class="section-divider"><td colspan="20">Last Week</td></tr>
{week_rows}
<tr class="section-divider"><td colspan="20">Season-to-Date</td></tr>
{season_rows}
</tbody></table>'''

    if dnp_names:
        section += f'<div class="dnp"><strong>Did not pitch last week:</strong> {", ".join(dnp_names)}</div>'

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
        played.sort(key=lambda r: (_grade_rank(r["week"]), -_ip(r["week"]), r["week"]["player_name"]))
    else:
        played.sort(key=lambda r: (_grade_rank(r["week"]), -_ops_value(r["week"]), r["week"]["player_name"]))

    dnp_names = sorted(r["week"]["player_name"] for r in dnp)
    return played, dnp_names


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
        "week_label": _last_full_week_label(today),
        "sections": sections,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def render_html(payload: dict) -> str:
    body_parts = []
    for lvl in LEVEL_ORDER:
        sec = payload["sections"][lvl]
        if not sec["hitters"] and not sec["pitchers"]:
            continue

        body_parts.append(f'<h2>{LEVEL_HEADER[lvl]}</h2>')

        h_played, h_dnp = _split_played_vs_insufficient(sec["hitters"], is_pitcher_side=False)
        p_played, p_dnp = _split_played_vs_insufficient(sec["pitchers"], is_pitcher_side=True)

        body_parts.append(_hitter_section(lvl, h_played, h_dnp))
        body_parts.append(_pitcher_section(p_played, p_dnp))

    legend_items = [
        ('<span class="pill pill-hot">HOT</span> · '
         '<span class="pill pill-solid">SOLID</span> · '
         '<span class="pill pill-steady">STEADY</span> · '
         '<span class="pill pill-cold">COLD</span> — Last Week grade is the last 7 days; '
         'Season-to-Date grade is full-season relative to role baselines.'),
    ]
    if payload["sections"].get("Pro", {}).get("hitters"):
        legend_items.append("OPS+ is a wRC+ proxy (100 = MLB average) using fixed league constants.")
    if payload["sections"].get("HS", {}).get("hitters") or payload["sections"].get("HS", {}).get("pitchers"):
        legend_items.append("HS stats come from a manually-maintained sheet — only as fresh as the latest entry.")

    legend = '<ul class="legend">' + "".join(f"<li>{item}</li>" for item in legend_items) + '</ul>'

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Dugout Pulse — Weekly Recap</title>
<style>{CSS}</style></head>
<body><div class="wrap">
<h1>Dugout Pulse — Weekly Recap</h1>
<div class="sub">Week of {payload['week_label']}</div>
{''.join(body_parts)}
{legend}
<div class="footer">Generated {payload['generated_at']}</div>
</div></body></html>
"""


def render_subject(payload: dict) -> str:
    return SUBJECT_TEMPLATE.format(week=payload["week_label"])


# ---------- Resend send ----------

def send_via_resend(subject: str, html: str, to: list[str], api_key: str) -> dict:
    import urllib.request, urllib.error
    body = json.dumps({
        "from": FROM_ADDRESS,
        "to": to,
        "subject": subject,
        "html": html,
    }).encode("utf-8")
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
        f"[monday_email] week={payload['week_label']} "
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
    result = send_via_resend(subject, html, recipients, api_key)
    sys.stderr.write(f"[monday_email] sent: {result}\n")


if __name__ == "__main__":
    main()
