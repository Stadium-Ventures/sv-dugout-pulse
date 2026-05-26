"""Monday AM hitter recap email.

Reads data/window_7d.json + data/window_season.json, builds a per-level
table of SV client hitters, and sends via Resend.

Usage:
  python -m scripts.monday_email --dry-run            # print HTML to stdout
  python -m scripts.monday_email --dry-run --save out.html
  python -m scripts.monday_email                      # actually send
  python -m scripts.monday_email --to ttrudeau@stadium-ventures.com  # test send to one address
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
SUBJECT_TEMPLATE = "Dugout Pulse — Hitter Recap, week of {week}"

PITCHER_POS = {"Pitcher", "LHP", "RHP", "Two-Way"}
LEVEL_ORDER = ["Pro", "NCAA", "HS"]
LEVEL_LABEL = {"Pro": "Pro", "NCAA": "NCAA", "HS": "HS"}

# OPS+ estimate (wRC+ proxy) league constants. Single set for now —
# correlates ~0.95 with true wRC+ for full-time hitters. Update annually.
# 2026 MLB-wide approximations; MiLB run environments vary by level but
# this gives Kent a directional context score until we wire FanGraphs.
LG_OBP_2026 = 0.320
LG_SLG_2026 = 0.415


def _is_hitter(p: dict) -> bool:
    return p.get("tags", {}).get("position") not in PITCHER_POS


def _pa(p: dict) -> int:
    v = p.get("stats", {}).get("pa")
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _priority(p: dict) -> int:
    v = p.get("tags", {}).get("roster_priority")
    try:
        return int(v)
    except (TypeError, ValueError):
        return 99


def _parse_rate(v) -> float | None:
    """Parse rate-stat strings like '.317' or '0.317' to float."""
    if v in (None, "", "--"):
        return None
    try:
        return float(str(v).lstrip("0") if str(v).startswith("0.") else v)
    except ValueError:
        return None


def _ops_plus(stats: dict) -> int | None:
    """OPS+ estimate using fixed league constants (wRC+ proxy)."""
    obp = _parse_rate(stats.get("obp"))
    slg = _parse_rate(stats.get("slg"))
    if obp is None or slg is None:
        return None
    val = 100.0 * (obp / LG_OBP_2026 + slg / LG_SLG_2026 - 1.0)
    return round(val)


def _load_window(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def _merge_by_player(week: list[dict], season: list[dict]) -> dict[str, dict]:
    """Return {player_name: {'week': record, 'season': record}}."""
    season_by_name = {p["player_name"]: p for p in season}
    out: dict[str, dict] = {}
    for w in week:
        out[w["player_name"]] = {"week": w, "season": season_by_name.get(w["player_name"])}
    return out


def _last_full_week_label(today: date | None = None) -> str:
    """Return e.g. 'May 19-25, 2026' for the Mon-Sun week ending yesterday."""
    today = today or date.today()
    # The Monday email runs Monday AM and covers the prior Mon-Sun.
    days_since_monday = today.weekday()  # Mon=0
    sunday = today - timedelta(days=(days_since_monday + 1) if days_since_monday >= 0 else 1)
    monday = sunday - timedelta(days=6)
    if monday.month == sunday.month:
        return f"{monday.strftime('%b')} {monday.day}-{sunday.day}, {sunday.year}"
    return f"{monday.strftime('%b %-d')}-{sunday.strftime('%b %-d')}, {sunday.year}"


# ---------- HTML rendering ----------

CSS = """
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         color: #222; background: #f7f7f8; margin: 0; padding: 0; }
  .wrap { max-width: 720px; margin: 0 auto; padding: 24px 16px; }
  h1 { font-size: 20px; margin: 0 0 4px 0; }
  .sub { color: #666; font-size: 13px; margin: 0 0 24px 0; }
  h2 { font-size: 16px; margin: 28px 0 8px 0; padding-bottom: 6px;
       border-bottom: 2px solid #1a73e8; }
  table { width: 100%; border-collapse: collapse; background: #fff;
          border-radius: 6px; overflow: hidden; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
  th, td { padding: 8px 10px; font-size: 13px; text-align: right; border-bottom: 1px solid #eee; }
  th { background: #f0f2f5; font-weight: 600; color: #444; text-align: right; }
  th.name, td.name { text-align: left; }
  td.name { font-weight: 600; color: #111; }
  td.team { color: #666; font-size: 12px; text-align: left; }
  th.team { text-align: left; }
  tr:last-child td { border-bottom: none; }
  .empty { color: #888; font-style: italic; font-size: 13px; padding: 12px; }
  .grade { font-size: 12px; }
  .footer { color: #888; font-size: 11px; margin-top: 32px; text-align: center; }
"""


def _fmt(v, default="—"):
    if v in (None, "", "--"):
        return default
    return str(v)


def _render_section_table(level: str, rows: list[dict]) -> str:
    if not rows:
        return f'<h2>{LEVEL_LABEL[level]} Hitters</h2><div class="empty">No client hitters at this level.</div>'

    header_cols = [
        ("name", "Player"), ("team", "Team"),
        ("g", "G"), ("pa", "PA"),
        ("avg", "AVG"), ("obp", "OBP"), ("slg", "SLG"), ("ops", "OPS"),
    ]
    if level == "Pro":
        header_cols.append(("ops_plus", "OPS+"))
    header_cols.extend([
        ("hr", "HR"), ("rbi", "RBI"), ("sb", "SB"),
        ("bb_pct", "BB%"), ("k_pct", "K%"),
    ])

    def th_row(label):
        cells = "".join(
            f'<th class="{c}">{l}</th>' if c in ("name", "team") else f'<th>{l}</th>'
            for c, l in header_cols
        )
        return f"<thead><tr><td colspan='{len(header_cols)}' style='background:#fafafa;padding:6px 10px;text-align:left;font-size:12px;color:#666;font-weight:600;'>{label}</td></tr><tr>{cells}</tr></thead>"

    def body_rows(records, window_key):
        out = []
        for rec in records:
            w = rec["week"] if window_key == "week" else rec["season"]
            if not w:
                continue
            s = w.get("stats", {})
            cells = [
                f'<td class="name">{w["player_name"]}</td>',
                f'<td class="team">{w["team"]}</td>',
                f'<td>{_fmt(w.get("games_played"))}</td>',
                f'<td>{_fmt(s.get("pa"))}</td>',
                f'<td>{_fmt(s.get("avg"))}</td>',
                f'<td>{_fmt(s.get("obp"))}</td>',
                f'<td>{_fmt(s.get("slg"))}</td>',
                f'<td>{_fmt(s.get("ops"))}</td>',
            ]
            if level == "Pro":
                ops_plus = _ops_plus(s)
                cells.append(f'<td>{ops_plus if ops_plus is not None else "—"}</td>')
            cells.extend([
                f'<td>{_fmt(s.get("hr"))}</td>',
                f'<td>{_fmt(s.get("rbi"))}</td>',
                f'<td>{_fmt(s.get("sb"))}</td>',
                f'<td>{_fmt(s.get("bb_pct"))}</td>',
                f'<td>{_fmt(s.get("k_pct"))}</td>',
            ])
            out.append("<tr>" + "".join(cells) + "</tr>")
        return "\n".join(out) or f'<tr><td colspan="{len(header_cols)}" class="empty">No data.</td></tr>'

    return f"""
<h2>{LEVEL_LABEL[level]} Hitters</h2>
<table>
  {th_row("Last week")}
  <tbody>{body_rows(rows, "week")}</tbody>
  {th_row("Season-to-date")}
  <tbody>{body_rows(rows, "season")}</tbody>
</table>
"""


def build_payload(today: date | None = None) -> dict:
    today = today or date.today()
    week = _load_window(WINDOW_7D)
    season = _load_window(WINDOW_SEASON)

    if not week:
        raise SystemExit(f"window_7d.json missing or empty at {WINDOW_7D}")

    merged = _merge_by_player(week, season)

    sections: dict[str, list[dict]] = {lvl: [] for lvl in LEVEL_ORDER}
    for name, rec in merged.items():
        w = rec["week"]
        if not w.get("is_client"):
            continue
        if not _is_hitter(w):
            continue
        level = w.get("level")
        if level not in sections:
            continue
        sections[level].append(rec)

    # Sort each section: priority asc, then weekly PA desc, then name
    for lvl in sections:
        sections[lvl].sort(
            key=lambda r: (_priority(r["week"]), -_pa(r["week"]), r["week"]["player_name"])
        )

    week_label = _last_full_week_label(today)

    return {
        "week_label": week_label,
        "sections": sections,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def render_html(payload: dict) -> str:
    body_parts = [_render_section_table(lvl, payload["sections"][lvl]) for lvl in LEVEL_ORDER]

    notes = []
    if payload["sections"].get("Pro"):
        notes.append("OPS+ is a wRC+ proxy (100 = league average) using fixed MLB league constants.")
    if payload["sections"].get("HS"):
        notes.append("HS stats are sourced from a manually-maintained sheet — only as fresh as the latest entry.")
    note_hs = ""
    if notes:
        items = "".join(f"<li>{n}</li>" for n in notes)
        note_hs = f'<ul style="color:#666;font-size:12px;margin-top:12px;padding-left:18px;">{items}</ul>'

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Dugout Pulse — Weekly Hitter Recap</title>
<style>{CSS}</style></head>
<body><div class="wrap">
<h1>Dugout Pulse — Weekly Hitter Recap</h1>
<div class="sub">Week of {payload['week_label']}</div>
{''.join(body_parts)}
{note_hs}
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
            # Cloudflare in front of api.resend.com 403s urllib's default UA (CF error 1010).
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
    p.add_argument("--dry-run", action="store_true", help="Print HTML to stdout; do not send.")
    p.add_argument("--save", metavar="PATH", help="With --dry-run, write HTML to this file.")
    p.add_argument("--to", action="append", help="Override recipient(s). Repeatable.")
    p.add_argument("--today", help="Override today's date (YYYY-MM-DD) for week-label testing.")
    args = p.parse_args(argv)

    today = date.fromisoformat(args.today) if args.today else date.today()
    payload = build_payload(today)
    subject = render_subject(payload)
    html = render_html(payload)
    recipients = args.to or DEFAULT_RECIPIENTS

    section_counts = {lvl: len(payload["sections"][lvl]) for lvl in LEVEL_ORDER}
    sys.stderr.write(
        f"[monday_email] week={payload['week_label']} "
        f"recipients={recipients} sections={section_counts}\n"
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
