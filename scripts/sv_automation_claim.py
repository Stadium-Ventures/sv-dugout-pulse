#!/usr/bin/env python3
"""Shared #sv-automation claim helper (D130 follow-up, 2026-09-08).

Gates a repo's OWN #sv-automation post (formatted and sent by that repo's own
house notify module — this file sends nothing) against the same durable
"already posted today" ledger scripts/lib/automation-post.cjs (sv-registry)
uses: data/reference/automation-post-ledger.json in Stadium-Ventures/sv-registry,
claimed via a GitHub Contents-API compare-and-swap so it survives a fresh
checkout and is safe across repos claiming the same key concurrently.

Why this exists: D130 found the sv-way-config-staleness detector posting once
PER SIBLING REPO about the same class of finding — six separate #sv-automation
messages for one thing. sv-registry and sv-travel-hub (no notify module of
their own) fixed this by vendoring the full Node poster. THIS repo has its
own house notify module with its own "every post must go through here" rule
(product label, tag, formatting) — vendoring a second poster would violate
that. So this file does ONLY the claim step; the caller still formats and
sends through its own module, and only if this says post=true.

Ledger JSON shape and CAS semantics are a deliberate match to
scripts/lib/automation-post.cjs in sv-registry (SAME repo, SAME file, SAME
key format `detector:YYYY-MM-DD`) — a Node claim and a Python claim for the
same detector on the same day contend for the same row, so grouping still
works even if a Node-based repo and a Python-based repo fire the same
detector on the same day.

FAIL CLOSED: any error establishing the claim (no token, network down, the
API refusing us) prints post=true. A duplicate alert is a nuisance; a missed
one is what the channel exists to prevent.

Usage:
  python3 sv_automation_claim.py <detector> [--finding TEXT]... \
      [--ttl-h N] [--acute-key KEY] [--run-url URL]

Prints one JSON line to stdout: {"post": bool, "reason": str, "key": str}
Always exits 0 (a claim-checker that fails the job it's gating is worse than
the duplicate it's trying to prevent).

Env:
  GITHUB_TOKEN / SV_REGISTRY_PAT   contents:write on the ledger repo (via a
                                    PAT — GITHUB_TOKEN's default repo scope is
                                    this repo, not sv-registry, so a sibling
                                    repo needs SV_REGISTRY_PAT). Absent -> fail
                                    closed (post=true, unclaimed).
  SV_AUTOMATION_LEDGER_REPO        default Stadium-Ventures/sv-registry
  SV_AUTOMATION_LEDGER_BRANCH      default main
  SV_AUTOMATION_NO_POST=1          test seam: never touch the network, print
                                    what the claim WOULD do.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

LEDGER_REPO_PATH = "data/reference/automation-post-ledger.json"
GITHUB_API = "https://api.github.com"
DIGEST_TTL_H = 24
ACUTE_TTL_H = 23
PRUNE_DAYS = 7
MAX_ATTEMPTS = 8
RETRYABLE = {404, 409, 422, 429, 500, 502, 503, 504}

LEDGER_NOTE = (
    'Durable "already posted today" claim store for #sv-automation (D130, 2026-09-08: '
    '"One digest per detector per day, findings grouped"). One row per (detector, UTC date) for grouped '
    'digests, plus per-finding rows for acute findings that must keep paging. Written by '
    'scripts/lib/automation-post.cjs (sv-registry) and sv_automation_claim.py (siblings with their own '
    'house notify module), through a GitHub Contents-API compare-and-swap, so any repo running the same '
    f"detector claims against the same row. Rows older than {PRUNE_DAYS} days are pruned on every write. "
    "Delete a row to force that detector to post again."
)


def _utc_date(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _backoff(attempt: int) -> float:
    return min(4.0, 0.15 * (2 ** (attempt - 1))) + random.uniform(0, 0.12)


class Ledger:
    def __init__(self, token: str, repo: str, branch: str):
        self.token = token
        self.repo = repo
        self.branch = branch
        self.url = f"{GITHUB_API}/repos/{repo}/contents/{urllib.request.quote(LEDGER_REPO_PATH)}"

    def _headers(self, extra=None):
        h = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "sv-automation-claim",
        }
        if extra:
            h.update(extra)
        return h

    def get(self):
        req = urllib.request.Request(f"{self.url}?ref={self.branch}", headers=self._headers())
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                doc = json.loads(resp.read())
                content = base64.b64decode(doc["content"]).decode("utf-8")
                return doc["sha"], json.loads(content)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None, None  # first claim ever
            e.status = e.code
            raise

    def put(self, sha, text: str, message: str):
        body = json.dumps(
            {
                "message": message,
                "branch": self.branch,
                **({"sha": sha} if sha else {}),
                "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, method="PUT",
            headers=self._headers({"Content-Type": "application/json"}),
        )
        try:
            urllib.request.urlopen(req, timeout=15)
            return False  # no conflict
        except urllib.error.HTTPError as e:
            if e.code in (409, 422):
                return True  # conflict — CAS lost, mechanism working
            e.status = e.code
            raise


def _normalize(doc):
    doc = doc if isinstance(doc, dict) else {}
    posted = doc.get("posted")
    return {"_note": LEDGER_NOTE, "posted": dict(posted) if isinstance(posted, dict) else {}}


def _prune(posted: dict, now: datetime) -> dict:
    cutoff = now - timedelta(days=PRUNE_DAYS)
    for k in list(posted.keys()):
        row = posted.get(k) or {}
        at = row.get("at")
        try:
            at_dt = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            del posted[k]
            continue
        if at_dt <= cutoff:
            del posted[k]
    return posted


def claim(detector: str, findings: list[str], ttl_h: float, acute_key: str | None,
          run_url: str | None, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    key = acute_key or f"{detector}:{_utc_date(now)}"
    findings = [f for f in findings if f and str(f).strip()]

    no_post = os.environ.get("SV_AUTOMATION_NO_POST") == "1"
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("SV_REGISTRY_PAT")
    if no_post:
        return {"post": True, "claimed": False, "key": key,
                "reason": "SV_AUTOMATION_NO_POST=1 — not touching the network"}
    if not token:
        return {"post": True, "claimed": False, "key": key,
                "reason": "no claim transport (GITHUB_TOKEN / SV_REGISTRY_PAT absent) — "
                          "failing closed, posting without a claim"}

    ledger = Ledger(
        token,
        os.environ.get("SV_AUTOMATION_LEDGER_REPO", "Stadium-Ventures/sv-registry"),
        os.environ.get("SV_AUTOMATION_LEDGER_BRANCH", "main"),
    )

    last_err = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            sha, raw = ledger.get()
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            last_err = e
            status = getattr(e, "status", None)
            if status not in RETRYABLE:
                break
            time.sleep(_backoff(attempt))
            continue

        doc = _normalize(raw)
        row = doc["posted"].get(key)
        held = False
        if row:
            try:
                row_at = datetime.fromisoformat(str(row.get("at")).replace("Z", "+00:00"))
                held = row_at > now - timedelta(hours=ttl_h)
            except (TypeError, ValueError):
                held = False

        if held:
            known = set(row.get("findings") or [])
            added = [f for f in findings if f not in known]
            if not added:
                return {"post": False, "claimed": True, "key": key,
                        "group": row.get("findings") or [],
                        "reason": "already claimed — identical findings"}
            next_row = dict(row)
            next_row["findings"] = (row.get("findings") or []) + added
            next_row["findings"] = next_row["findings"][:200]
            if run_url:
                sr = (row.get("suppressed_runs") or []) + [run_url]
                next_row["suppressed_runs"] = sr[-20:]
            next_doc = dict(doc)
            next_doc["posted"] = _prune({**doc["posted"], key: next_row}, now)
            try:
                conflict = ledger.put(
                    sha, json.dumps(next_doc, indent=2) + "\n",
                    f"automation-post: record {len(added)} suppressed finding(s) for {key} [skip ci]",
                )
                if conflict:
                    time.sleep(_backoff(attempt))
                    continue
            except (urllib.error.HTTPError, urllib.error.URLError):
                return {"post": False, "claimed": True, "key": key,
                        "group": row.get("findings") or [],
                        "reason": "already claimed (could not record suppressed findings)"}
            return {"post": False, "claimed": True, "key": key,
                    "group": next_row["findings"],
                    "reason": "already claimed today — findings recorded on the row"}

        # Free or expired — claim it.
        next_row = {"at": now.isoformat().replace("+00:00", "Z"), "findings": findings, "run_url": run_url}
        next_doc = dict(doc)
        next_doc["posted"] = _prune({**doc["posted"], key: next_row}, now)
        try:
            conflict = ledger.put(
                sha, json.dumps(next_doc, indent=2) + "\n",
                f"automation-post: claim {key} ({len(findings)} finding(s)) [skip ci]",
            )
            if conflict:
                time.sleep(_backoff(attempt))
                continue
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            last_err = e
            status = getattr(e, "status", None)
            if status not in RETRYABLE:
                break
            time.sleep(_backoff(attempt))
            continue
        return {"post": True, "claimed": True, "key": key, "group": findings, "reason": "claimed"}

    reason = f"claim could not be established after {MAX_ATTEMPTS} attempt(s)"
    if last_err is not None:
        reason += f" ({last_err})"
    reason += " — failing closed, posting without a claim"
    return {"post": True, "claimed": False, "key": key, "group": findings, "reason": reason}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("detector")
    p.add_argument("--finding", action="append", default=[], dest="findings")
    p.add_argument("--ttl-h", type=float, default=DIGEST_TTL_H)
    p.add_argument("--acute-key", default=None)
    p.add_argument("--run-url", default=None)
    args = p.parse_args()

    ttl_h = args.ttl_h if args.ttl_h != DIGEST_TTL_H or not args.acute_key else ACUTE_TTL_H
    try:
        result = claim(args.detector, args.findings, ttl_h, args.acute_key, args.run_url)
    except Exception as e:  # noqa: BLE001 — a claim-checker must never fail the job it gates
        result = {"post": True, "claimed": False, "key": args.detector,
                  "reason": f"unexpected error in claim helper: {e} — failing closed"}
    print(json.dumps(result))
    sys.exit(0)


if __name__ == "__main__":
    main()
