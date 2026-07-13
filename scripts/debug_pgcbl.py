"""One-off diagnostic round 6: does r= paginate when sort is set?

Sort links enumerate r=0/1/2. Compare slug sets across r values for a
sorted hitters view and a sorted pitchers view.
Temporary — delete along with debug_pgcbl.yml once PGCBL is fixed.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.summer_ball import fetch_via_residential_proxy

BASE = "https://pgcbl.prestosports.com/sports/bsb/2025-26/players"

sets = {}
for label, url in [
    ("h r0", f"{BASE}?sort=gp&view=&pos=h&r=0"),
    ("h r1", f"{BASE}?sort=gp&view=&pos=h&r=1"),
    ("h r2", f"{BASE}?sort=gp&view=&pos=h&r=2"),
    ("p r0", f"{BASE}?sort=ip&view=&pos=p&r=0"),
    ("p r1", f"{BASE}?sort=ip&view=&pos=p&r=1"),
    ("f r0", f"{BASE}?sort=gp&view=&pos=f&r=0"),
    ("f r1", f"{BASE}?sort=gp&view=&pos=f&r=1"),
]:
    html, diag = fetch_via_residential_proxy(url, timeout=40)
    if not html:
        print(f"{label}: FETCH FAILED {diag.get('error')}")
        continue
    slugs = set(re.findall(r"/sports/bsb/2025-26/players/([a-z0-9-]+)", html))
    sets[label] = slugs
    print(f"{label}: {len(html)}b, {len(slugs)} slugs")

if "h r0" in sets and "h r1" in sets:
    print(f"h r0 ∩ r1: {len(sets['h r0'] & sets['h r1'])}, r0 ∪ r1: {len(sets['h r0'] | sets['h r1'])}")
if "h r2" in sets:
    print(f"h r0 ∪ r1 ∪ r2: {len(sets['h r0'] | sets['h r1'] | sets['h r2'])}")
if "p r0" in sets and "p r1" in sets:
    print(f"p r0 ∩ r1: {len(sets['p r0'] & sets['p r1'])}, union: {len(sets['p r0'] | sets['p r1'])}")
if "f r0" in sets and "f r1" in sets:
    print(f"f r0 ∩ r1: {len(sets['f r0'] & sets['f r1'])}, union: {len(sets['f r0'] | sets['f r1'])}")
everything = set().union(*sets.values()) if sets else set()
print(f"union of all views: {len(everything)}")
print(f"zackjohnson-ish: {[s for s in everything if 'johnson' in s]}")
