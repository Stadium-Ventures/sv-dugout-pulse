"""Runs the Node unit tests for js/sv-google-auth.js (the Heartbeat sign-in
helper) from pytest, so `python -m pytest tests/` covers them. Skipped when
Node is not installed (GitHub's ubuntu runners ship it)."""
import os
import shutil
import subprocess

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_sv_google_auth_node_suite():
    proc = subprocess.run(
        ["node", "--test", os.path.join(HERE, "js", "sv_google_auth.test.js")],
        capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_page_wires_heartbeat_through_the_auth_helper():
    root = os.path.dirname(HERE)
    app = open(os.path.join(root, "js", "app.js"), encoding="utf-8").read()
    index = open(os.path.join(root, "index.html"), encoding="utf-8").read()
    # Heartbeat is only ever called through authorizedFetch (Bearer ID token).
    assert "authorizedFetch(HEARTBEAT_SUMMARY_URL)" in app
    assert "fetch('https://sv-heartbeat.vercel.app" not in app
    # Hearts hidden when signed out; sign-in prompt present; helper loads first.
    assert "if (!isClient || !heartbeatAvailable) return '';" in app
    assert 'id="hbSignIn"' in index and 'body.hb-signed-out [data-filter="heartbeat"]' in index
    assert index.index("js/sv-google-auth.js") < index.index("js/app.js")
    # Token in memory only.
    helper = open(os.path.join(root, "js", "sv-google-auth.js"), encoding="utf-8").read()
    code = "\n".join(l for l in helper.splitlines() if not l.strip().startswith("//"))
    assert "localStorage" not in code and "sessionStorage" not in code and "document.cookie" not in code
