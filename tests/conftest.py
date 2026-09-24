"""Test bootstrap: put the repo root on sys.path so `src`, `scripts`, and
`main` import the same way they do in production (repo-root cwd)."""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import pytest


@pytest.fixture(autouse=True)
def _no_roster_source_alerts(monkeypatch, tmp_path):
    """Registry-path alerts (src/roster_manager.py) never reach Slack or the
    real data/ state file from a test run."""
    from src import roster_manager
    monkeypatch.setattr(roster_manager, "ROSTER_ALERT_STATE_PATH",
                        str(tmp_path / "_roster_source_alerts.json"))
    monkeypatch.setattr(roster_manager, "_send_automation", lambda text: False)
