"""One-off diagnostic: why does PGCBL parse to 0 players?

Round 3: names are plain text in the leaderboard tables (no profile links).
Print exact name-cell text/markup as repr (single line, log-safe) and probe
the pos/r query params to understand pagination and coverage.
Temporary — delete along with debug_pgcbl.yml once PGCBL is fixed.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs4 import BeautifulSoup

from src.summer_ball import fetch_via_residential_proxy

BASE = "https://pgcbl.prestosports.com/sports/bsb/2025-26/players"


def inspect(label: str, url: str) -> None:
    print(f"\n===== {label}: {url}")
    html, diag = fetch_via_residential_proxy(url, timeout=25)
    if not html:
        print(f"FETCH FAILED: {diag}")
        return
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        if len(rows) > 2:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
            if "name" in headers and "team" in headers:
                table = t
                break
    if table is None:
        print("no Name+Team table found")
        return
    rows = table.find_all("tr")[1:]
    pairs = []
    for r in rows:
        cells = r.find_all(["th", "td"])
        if len(cells) < 3:
            continue
        name = cells[1].get_text(" ", strip=True)
        team = cells[2].get_text(" ", strip=True)
        pairs.append((name, team))
    print(f"data rows: {len(pairs)}, unique: {len(set(pairs))}, teams: {len({t for _, t in pairs})}")
    for name, team in pairs[:5]:
        print(f"  {name!r} | {team!r}")
    print(f"  last: {pairs[-1]!r}")
    # exact markup of one name cell, log-safe
    cells = rows[0].find_all(["th", "td"])
    print(f"  name-cell markup: {str(cells[1])!r}"[:500])


inspect("hitters page 0", f"{BASE}?pos=h&r=0")
inspect("hitters page 1", f"{BASE}?pos=h&r=1")
inspect("pitchers page 0", f"{BASE}?pos=p&r=0")
inspect("no params", BASE)
