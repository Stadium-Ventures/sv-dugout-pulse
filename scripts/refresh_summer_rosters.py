"""Daily refresh of summer-ball roster discovery.

Runs all configured leagues, writes `data/summer_ball_rosters.json` with
per-league health + NCAA-client match results.

Usage:
  python -m scripts.refresh_summer_rosters         # full run
  python -m scripts.refresh_summer_rosters --dry   # discover but don't write file
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.summer_ball import SummerBallAggregator  # noqa: E402


def _ncaa_clients() -> list[dict]:
    """Return the list of NCAA client players (level=NCAA, is_client=True)."""
    from main import get_all_players
    players = get_all_players()
    return [p for p in players if p.get("level") == "NCAA" and p.get("is_client", True)]


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--dry", action="store_true",
                   help="Run discovery + match but skip writing the roster file.")
    args = p.parse_args(argv)

    clients = _ncaa_clients()
    sys.stderr.write(f"[summer_rosters] discovered {len(clients)} NCAA clients\n")

    agg = SummerBallAggregator()
    if args.dry:
        players, health = agg.discover_all()
        sys.stderr.write(f"[summer_rosters] DRY: {len(players)} total players found\n")
        for h in health:
            sys.stderr.write(f"  {h.league}: {h.status} ({h.player_count} players)\n")
        return

    snapshot = agg.write_roster_file(clients)
    sys.stderr.write(
        f"[summer_rosters] matched={snapshot['ncaa_clients_matched']} "
        f"unmatched={snapshot['ncaa_clients_unmatched']} "
        f"ambiguous={snapshot['ncaa_clients_ambiguous']} "
        f"(of {snapshot['ncaa_clients_total']} clients)\n"
    )
    for h in snapshot["league_health"]:
        sys.stderr.write(
            f"  {h['league']:<14} {h['status']:<16} "
            f"players={h['player_count']:<4} teams={h['team_count']}\n"
        )


if __name__ == "__main__":
    main()
