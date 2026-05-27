"""Monthly hitter + pitcher recap email.

Fires on the 1st of each month and covers the prior calendar month
(e.g. June 1 sends "May 2026"). Aggregates per-player stats inline
via the historical_stats aggregator with a custom date range, then
reuses the rendering + PDF + send pipeline from scripts/monday_email.

Usage mirrors monday_email:
  python -m scripts.monthly_email --dry-run --save out.html
  python -m scripts.monthly_email                                  # send
  python -m scripts.monthly_email --to ttrudeau@stadium-ventures.com
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Reuse the rendering + send pipeline from the weekly email module.
from scripts import monday_email as _w

REPO_ROOT = Path(__file__).resolve().parents[1]
WINDOW_SEASON = REPO_ROOT / "data" / "window_season.json"


def _prior_calendar_month(today: date) -> tuple[date, date, str]:
    """Return (first_of_prior, last_of_prior, label) — e.g. (May 1, May 31, "May 2026")."""
    first_of_this = today.replace(day=1)
    last_of_prior = first_of_this - timedelta(days=1)
    first_of_prior = last_of_prior.replace(day=1)
    label = last_of_prior.strftime("%B %Y")
    return first_of_prior, last_of_prior, label


def _aggregate_month(start: date, end: date) -> list[dict]:
    """Build per-player month-window entries via the historical aggregator.

    Slow (5–10 min) — hits MLB StatsAPI per Pro, D1B/game-log per NCAA, HS
    sheet for HS. We only do this once per month so it's fine.
    """
    # Late imports — these pull in scrapers we only need on the monthly cron.
    from main import get_all_players
    from src.historical_stats import WindowStatsAggregator
    from concurrent.futures import ThreadPoolExecutor, as_completed

    players = get_all_players()
    if not players:
        raise SystemExit("No players found — aborting monthly aggregation")

    sys.stderr.write(f"[monthly_email] aggregating {len(players)} players for {start} -> {end}\n")

    agg = WindowStatsAggregator()
    out: list[dict] = []

    def _process(player):
        return agg._build_window_entry(player, "month", start, end)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_process, p): p for p in players}
        for fut in as_completed(futures):
            try:
                entry = fut.result()
                if entry:
                    out.append(entry)
            except Exception:
                p = futures[fut]
                sys.stderr.write(f"[monthly_email] failed: {p.get('player_name','?')}\n")

    sys.stderr.write(f"[monthly_email] aggregated {len(out)} entries\n")
    return out


def build_payload(today: date | None = None) -> dict:
    today = today or date.today()
    start, end, label = _prior_calendar_month(today)

    month_entries = _aggregate_month(start, end)
    season_entries = _w._load_window(WINDOW_SEASON)

    if not season_entries:
        raise SystemExit(f"window_season.json missing or empty at {WINDOW_SEASON}")

    # Reuse the weekly merge logic: it just keys by player_name.
    season_by_name = {p["player_name"]: p for p in season_entries}
    merged = {m["player_name"]: {"week": m, "season": season_by_name.get(m["player_name"])}
              for m in month_entries}

    sections: dict[str, dict[str, list]] = {
        lvl: {"hitters": [], "pitchers": []} for lvl in _w.LEVEL_ORDER
    }
    for name, rec in merged.items():
        m = rec["week"]
        if not m.get("is_client"):
            continue
        level = m.get("level")
        if level not in sections:
            continue
        if _w._is_pitcher(m):
            sections[level]["pitchers"].append(rec)
        else:
            sections[level]["hitters"].append(rec)

    safe_label = label.replace(" ", "-").lower()

    return {
        "title": "Monthly Recap",
        "subtitle_prefix": "",
        "period_label": label,
        "recent_section_label": "Last Month",
        "season_section_label": "Season-to-Date",
        "standouts_section_label": f"Standouts ({label})",
        "glance_section_label": f"{label} at a Glance",
        "dnp_hitter_phrase": "Did not play last month",
        "dnp_pitcher_phrase": "Did not pitch last month",
        "subject_template": "Dugout Pulse — Monthly Recap, {period}",
        "pdf_filename_prefix": f"dugout-pulse-month-{safe_label}",
        "sections": sections,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--save", metavar="PATH")
    p.add_argument("--to", action="append")
    p.add_argument("--today", help="Override today's date (YYYY-MM-DD).")
    p.add_argument("--subject-suffix", default="")
    args = p.parse_args(argv)

    today = date.fromisoformat(args.today) if args.today else date.today()
    payload = build_payload(today)
    subject = _w.render_subject(payload) + (args.subject_suffix or "")
    html = _w.render_html(payload)
    recipients = args.to or _w.DEFAULT_RECIPIENTS

    counts = {
        lvl: {"hitters": len(payload["sections"][lvl]["hitters"]),
              "pitchers": len(payload["sections"][lvl]["pitchers"])}
        for lvl in _w.LEVEL_ORDER
    }
    sys.stderr.write(
        f"[monthly_email] period={payload['period_label']} "
        f"recipients={recipients} counts={counts}\n"
    )

    if args.dry_run:
        if args.save:
            Path(args.save).write_text(html)
            sys.stderr.write(f"[monthly_email] wrote {args.save}\n")
        else:
            sys.stdout.write(html)
        return

    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("RESEND_API_KEY env var is not set.")

    pdf_bytes = _w.render_pdf(html)
    if pdf_bytes:
        sys.stderr.write(f"[monthly_email] PDF: {len(pdf_bytes)} bytes\n")

    # Mirror the weekly script's filename derivation but using the monthly prefix.
    safe_period = payload['period_label'].replace(' ', '-').replace(',', '').lower()
    pdf_filename = f"{payload['pdf_filename_prefix']}.pdf"

    result = _w.send_via_resend(subject, html, recipients, api_key,
                                 pdf_bytes=pdf_bytes, pdf_filename=pdf_filename)
    sys.stderr.write(f"[monthly_email] sent: {result}\n")


if __name__ == "__main__":
    main()
