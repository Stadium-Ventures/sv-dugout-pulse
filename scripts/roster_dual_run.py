"""Dual-run detail report: master sheet vs sv-registry roster projection.

LOCAL USE ONLY. It prints player names, and this repo's Actions logs are
public, so CI logs only the counts (roster_manager._log_dual_run). Run it on
a laptop with the two credentials in the environment:

  ROSTER_URL=... SV_REGISTRY_ROSTER_TOKEN=svt_... python -m scripts.roster_dual_run

Exit code 0 = no differences; 1 = differences to review before the flip.
Expected, explained differences at cutover: name_differs for canon full names
(e.g. "Cam Flukey" → "Cameron Flukey"), registry_only clients the sheet never
got, and org_differs for NCAA short school names.
"""
import sys

from src.roster_manager import dual_run_diff, fetch_roster, filter_roster, get_registry_clients


def main() -> int:
    sheet = filter_roster(fetch_roster())
    registry = get_registry_clients()
    diff = dual_run_diff(sheet, registry)
    print(f"sheet clients: {len(sheet)}   registry clients: {len(registry)}")
    total = 0
    for category, items in diff.items():
        print(f"\n{category}: {len(items)}")
        for item in items:
            print(f"  - {item}")
        total += len(items)
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
