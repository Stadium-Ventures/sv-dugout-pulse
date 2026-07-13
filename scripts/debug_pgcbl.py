"""One-off diagnostic round 5: per-team players list on the presto subdomain.

Team cells on the leaderboard link to `teams?id={teamId}`. Probe
players?teamId={id}(&view=ext) as a full-roster source for one team.
Temporary — delete along with debug_pgcbl.yml once PGCBL is fixed.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.summer_ball import fetch_via_residential_proxy

BASE = "https://pgcbl.prestosports.com/sports/bsb/2025-26/players"
MOHAWKS = "96npryi9vztnt1yi"


def inspect(label: str, url: str) -> None:
    print(f"\n===== {label}: {url}")
    html, diag = fetch_via_residential_proxy(url, timeout=40)
    if not html:
        print(f"FETCH FAILED: {diag}")
        return
    print(f"bytes: {len(html)}")
    slugs = set(re.findall(r"/sports/bsb/2025-26/players/([a-z0-9-]+)", html))
    print(f"unique player slugs: {len(slugs)}")
    print(f"johnson/taylor slugs: {[s for s in slugs if 'johnson' in s or 'taylor' in s]}")
    teams = set(re.findall(r'teams\?id=([a-z0-9]+)"[^>]*>([^<]{2,40})<', html))
    print(f"team id links: {len(teams)} -> {sorted(t[1] for t in teams)[:20]}")
    # first player row, whitespace-collapsed, log-safe
    m = re.search(r"<tr>(?:(?!</tr>).)*?/players/(?:(?!</tr>).)*?</tr>", html, re.S)
    if m:
        print(f"player row: {re.sub(chr(92)+'s+', ' ', m.group(0))[:400]!r}")


inspect("team players view=ext", f"{BASE}?teamId={MOHAWKS}&view=ext")
inspect("team players plain", f"{BASE}?teamId={MOHAWKS}")
inspect("team players view=lineup", f"{BASE}?teamId={MOHAWKS}&view=lineup")
