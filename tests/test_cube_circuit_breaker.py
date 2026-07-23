"""Baseball Cube circuit breaker: a fully blocked Cube (all proxies rejected)
must not burn ~26s per unmatched client — that's what pushed every
2026-07-17..23 roster refresh past its workflow timeout."""
import src.summer_ball as sb
from src.summer_ball import SummerBallAggregator


class _BlockedCube:
    """Simulates Cloudflare rejecting every proxy on every search."""
    last_search_blocked = False

    def __init__(self):
        self.calls = 0

    def find_player(self, full_name, college):
        self.calls += 1
        self.last_search_blocked = True
        return None


class _HealthyMissCube(_BlockedCube):
    """Reachable Cube that just doesn't know these players."""
    def find_player(self, full_name, college):
        self.calls += 1
        self.last_search_blocked = False
        return None


def _clients(n):
    return [
        {"player_name": f"Player {i}", "team": f"College {i}", "level": "NCAA"}
        for i in range(n)
    ]


def _run(agg, monkeypatch, tmp_path, n=10):
    # No leagues → everyone lands in the unmatched bucket for the Cube pass.
    monkeypatch.setattr(sb, "SUMMER_ROSTER_PATH", tmp_path / "rosters.json")
    return agg.write_roster_file(_clients(n))


def test_blocked_cube_trips_breaker(monkeypatch, tmp_path):
    cube = _BlockedCube()
    agg = SummerBallAggregator(leagues=[], cube=cube)
    snapshot = _run(agg, monkeypatch, tmp_path)
    assert cube.calls == sb._CUBE_MAX_CONSECUTIVE_BLOCKS
    assert snapshot["baseballcube"]["blocked"] == sb._CUBE_MAX_CONSECUTIVE_BLOCKS
    assert snapshot["baseballcube"]["skipped"] == 10 - sb._CUBE_MAX_CONSECUTIVE_BLOCKS
    # Skipped clients stay unmatched — nobody silently disappears.
    assert snapshot["ncaa_clients_unmatched"] == 10


def test_healthy_cube_checks_everyone(monkeypatch, tmp_path):
    cube = _HealthyMissCube()
    agg = SummerBallAggregator(leagues=[], cube=cube)
    snapshot = _run(agg, monkeypatch, tmp_path)
    assert cube.calls == 10
    assert snapshot["baseballcube"]["skipped"] == 0
    assert snapshot["baseballcube"]["blocked"] == 0
