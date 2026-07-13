"""One-off diagnostic round 4: PGCBL coverage + team attribution.

125 players parsed (one alphabetical page) and teams=1 (team cell not an
anchor?). Probe the printer-decorator view for a full unpaginated list and
dump real team-cell markup.
Temporary — delete along with debug_pgcbl.yml once PGCBL is fixed.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs4 import BeautifulSoup

from src.summer_ball import fetch_via_residential_proxy

BASE = "https://pgcbl.prestosports.com/sports/bsb/2025-26/players"


def inspect(label: str, url: str) -> None:
    print(f"\n===== {label}: {url}")
    html, diag = fetch_via_residential_proxy(url, timeout=40)
    if not html:
        print(f"FETCH FAILED: {diag}")
        return
    print(f"bytes: {len(html)}")
    slugs = set(re.findall(r"/sports/bsb/2025-26/players/([a-z0-9-]+)", html))
    print(f"unique player slugs on page: {len(slugs)}")
    print(f"'johnson' in slugs: {[s for s in slugs if 'johnson' in s][:5]}")
    soup = BeautifulSoup(html, "html.parser")
    # first row that contains a player link: dump full row markup, log-safe
    for a in soup.find_all("a", href=True):
        if re.search(r"/sports/bsb/2025-26/players/[a-z0-9-]+", a["href"]):
            row = a.find_parent("tr")
            if row:
                print(f"row markup: {re.sub(r's+', ' ', str(row))!r}"[:700])
            break
    # pagination hints
    pag = {a["href"] for a in soup.find_all("a", href=True)
           if re.search(r"[?&](page|start|offset|begin|r)=", a["href"])}
    print(f"pagination-ish hrefs: {sorted(pag)[:10]}")


inspect("printer-decorator", f"{BASE}?dec=printer-decorator")
inspect("printer-decorator hitters", f"{BASE}?pos=h&dec=printer-decorator")
inspect("default", BASE)
