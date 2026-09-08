#!/usr/bin/env python3
"""Self-test for sv_automation_claim.py. Prints PASS or FAIL(reason). No network.

Exercises claim() against an in-memory fake Ledger instead of the real
Contents API, so it runs in CI with no token and no side effects.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import sv_automation_claim as m  # noqa: E402


class FakeLedger:
    """In-memory stand-in for m.Ledger — same get()/put() shape."""

    def __init__(self, doc=None):
        self.doc = doc
        self.sha = "fake-sha-0" if doc is not None else None
        self.puts = 0

    def get(self):
        return self.sha, self.doc

    def put(self, sha, text, message):
        if sha != self.sha:
            return True  # conflict
        self.puts += 1
        self.doc = json.loads(text)
        self.sha = f"fake-sha-{self.puts}"
        return False


def with_fake_ledger(fake):
    orig = m.Ledger
    m.Ledger = lambda *a, **k: fake  # noqa: E731
    return orig


def restore(orig):
    m.Ledger = orig


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def main():
    failures = []

    def run(name, fn):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            failures.append(f"{name}: {e}")

    import os

    os.environ["GITHUB_TOKEN"] = "fake-token-for-selftest"
    os.environ.pop("SV_AUTOMATION_NO_POST", None)

    def t_first_claim_wins():
        fake = FakeLedger(doc=None)
        orig = with_fake_ledger(fake)
        try:
            r = m.claim("sv-way-config-staleness", ["repo-a: stale"], m.DIGEST_TTL_H, None, None)
        finally:
            restore(orig)
        check(r["post"] is True, f"expected post=True, got {r}")
        check(r["claimed"] is True, f"expected claimed=True, got {r}")
        check(fake.puts == 1, "expected exactly one PUT")

    def t_second_caller_same_day_stays_quiet():
        now = datetime.now(timezone.utc)
        doc = {"_note": "x", "posted": {
            "sv-way-config-staleness:" + m._utc_date(now): {
                "at": now.isoformat().replace("+00:00", "Z"),
                "findings": ["repo-a: stale"],
                "run_url": None,
            }
        }}
        fake = FakeLedger(doc=doc)
        orig = with_fake_ledger(fake)
        try:
            r = m.claim("sv-way-config-staleness", ["repo-b: stale"], m.DIGEST_TTL_H, None, None, now=now)
        finally:
            restore(orig)
        check(r["post"] is False, f"expected post=False (already claimed today), got {r}")
        check("repo-b: stale" in r["group"], "expected repo-b's finding recorded on the row")
        check(fake.puts == 1, "expected exactly one PUT to record the suppressed finding")

    def t_expired_row_reclaims():
        now = datetime.now(timezone.utc)
        stale_at = now - timedelta(hours=m.DIGEST_TTL_H + 1)
        doc = {"_note": "x", "posted": {
            "sv-way-config-staleness:" + m._utc_date(stale_at): {
                "at": stale_at.isoformat().replace("+00:00", "Z"),
                "findings": ["repo-a: stale (yesterday)"],
                "run_url": None,
            }
        }}
        fake = FakeLedger(doc=doc)
        orig = with_fake_ledger(fake)
        try:
            r = m.claim("sv-way-config-staleness", ["repo-a: stale (today)"], m.DIGEST_TTL_H, None, None, now=now)
        finally:
            restore(orig)
        check(r["post"] is True, f"expected post=True for a fresh key/day, got {r}")

    def t_missing_token_fails_closed():
        saved = os.environ.pop("GITHUB_TOKEN", None)
        saved2 = os.environ.pop("SV_REGISTRY_PAT", None)
        try:
            r = m.claim("sv-way-config-staleness", ["x"], m.DIGEST_TTL_H, None, None)
        finally:
            if saved is not None:
                os.environ["GITHUB_TOKEN"] = saved
            if saved2 is not None:
                os.environ["SV_REGISTRY_PAT"] = saved2
        check(r["post"] is True, f"expected fail-closed post=True with no token, got {r}")
        check(r["claimed"] is False, "expected claimed=False when failing closed")

    def t_no_post_seam_never_touches_network():
        os.environ["SV_AUTOMATION_NO_POST"] = "1"
        try:
            r = m.claim("sv-way-config-staleness", ["x"], m.DIGEST_TTL_H, None, None)
        finally:
            del os.environ["SV_AUTOMATION_NO_POST"]
        check(r["post"] is True, f"expected post=True under the no-post seam, got {r}")
        check(r["claimed"] is False, "expected claimed=False under the no-post seam")

    def t_cli_prints_json():
        import subprocess
        env = dict(os.environ)
        env["SV_AUTOMATION_NO_POST"] = "1"
        script = __file__.rsplit("/", 1)[0] + "/sv_automation_claim.py"
        out = subprocess.run(
            [sys.executable, script, "sv-way-config-staleness", "--finding", "x"],
            capture_output=True, text=True, env=env, check=True,
        )
        parsed = json.loads(out.stdout.strip())
        check("post" in parsed and "reason" in parsed, f"expected JSON with post/reason, got {out.stdout!r}")

    run("first_claim_wins", t_first_claim_wins)
    run("second_caller_same_day_stays_quiet", t_second_caller_same_day_stays_quiet)
    run("expired_row_reclaims", t_expired_row_reclaims)
    run("missing_token_fails_closed", t_missing_token_fails_closed)
    run("no_post_seam_never_touches_network", t_no_post_seam_never_touches_network)
    run("cli_prints_json", t_cli_prints_json)

    if failures:
        print("FAIL")
        for f in failures:
            print(f" - {f}")
        sys.exit(1)
    print("PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
