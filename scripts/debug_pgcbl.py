"""One-off diagnostic: why does PGCBL parse to 0 players?

Fetches the league-wide players index and one team roster page through the
same proxy path production uses, then prints structural facts (href
patterns, table headers) so we can see what the parser is missing.
Temporary — delete along with debug_pgcbl.yml once PGCBL is fixed.
"""
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs4 import BeautifulSoup

from src.summer_ball import fetch_via_residential_proxy


def inspect(label: str, url: str) -> None:
    print(f"\n===== {label}: {url}")
    html, diag = fetch_via_residential_proxy(url, timeout=25)
    if not html:
        print(f"FETCH FAILED: {diag}")
        return
    print(f"fetched {len(html)} bytes via {diag.get('active')}")
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("title")
    print(f"title: {title.get_text(strip=True) if title else '??'}")

    # What year segments appear in bsb paths?
    years = Counter(re.findall(r"/sports/bsb/([0-9-]+)/", html))
    print(f"bsb year segments: {dict(years.most_common(5))}")

    # Player-ish hrefs
    hrefs = [a["href"] for a in soup.find_all("a", href=True)]
    player_hrefs = [h for h in hrefs if "player" in h.lower()]
    print(f"total hrefs: {len(hrefs)}, containing 'player': {len(player_hrefs)}")
    for h in player_hrefs[:10]:
        print(f"  player href: {h[:120]}")

    # Roster/leaderboard table shapes
    tables = soup.find_all("table")
    print(f"tables: {len(tables)}")
    for i, t in enumerate(tables):
        rows = t.find_all("tr")
        if not rows:
            continue
        headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        print(f"  table[{i}] rows={len(rows)} headers={headers[:12]}")
        if len(rows) > 1:
            first = [c.get_text(strip=True)[:25] for c in rows[1].find_all(["th", "td"])]
            print(f"    row[1]: {first[:12]}")
        # Raw HTML of the second cell in the first two data rows (the Name
        # cell in leaderboard tables) so we can see the actual markup.
        for r in rows[1:3]:
            cells = r.find_all(["th", "td"])
            if len(cells) > 1:
                print(f"    name-cell html: {str(cells[1])[:300]}")

    # JS-rendered hints
    for marker in ("window.__INITIAL_STATE__", "data-reactroot", "id=\"app\"",
                   "ng-app", "presto-widget", "sidearm"):
        if marker in html:
            print(f"  marker present: {marker}")


inspect("team players list (view=ext)",
        "https://pgcbl.com/sports/bsb/2025-26/players?teamId=96npryi9vztnt1yi&view=ext")
inspect("team roster (path form)",
        "https://pgcbl.com/sports/bsb/2025-26/teams/amsterdammohawks/roster")
inspect("league index name cells",
        "https://pgcbl.prestosports.com/sports/bsb/2025-26/players?pos=h&r=0")
