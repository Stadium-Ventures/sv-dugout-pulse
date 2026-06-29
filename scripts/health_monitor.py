"""LLM-in-the-loop health monitor for SV Dugout Pulse.

Runs on a schedule (see .github/workflows/health_monitor.yml). Unlike the
rule-based alerts (BBRef cron check, roster regression, quiet streak), this
*reads across* signals and reasons about whether something is actually wrong:

  1. The run-level health envelope on data/current_pulse.json
  2. The recent capture/severity trend from data/fetch_health_history.json
  3. The last ~24h of #dugout-pulse Slack messages — including non-bot
     (colleague) comments like "this player's stats look wrong"
  4. Recent GitHub Actions run conclusions (failed workflows)
  5. Open GitHub issues already labelled `pulse-health` (so it doesn't
     re-raise the same thing)

Claude then decides what (if anything) is a real, novel, actionable problem,
and the script:
  - opens a GitHub issue (label `pulse-health`) with a diagnosis + a
    ready-to-paste Claude Code fix prompt, for anything needing rework
  - posts a concise plain-English digest to #sv-automation (the muted
    cross-product channel) — silent when nothing is actionable

Env:
  ANTHROPIC_API_KEY          (required) SV Anthropic account
  SLACK_BOT_TOKEN            (optional) xoxb- token with groups:history,
                             invited to #dugout-pulse. Without it, the Slack
                             read is skipped and the monitor runs log-only.
  SV_AUTOMATION_WEBHOOK_URL  (optional) Incoming webhook for #sv-automation
  GITHUB_TOKEN               (required to open issues) provided by Actions
  GITHUB_REPOSITORY          (default Stadium-Ventures/sv-dugout-pulse)
  DUGOUT_PULSE_CHANNEL_ID    (default C0ACLMAHXPF)
  HEALTH_MONITOR_MODEL       (default claude-opus-4-8)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from scripts._automation_notify import post_automation

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PULSE_PATH = _REPO_ROOT / "data" / "current_pulse.json"
_HISTORY_PATH = _REPO_ROOT / "data" / "fetch_health_history.json"

_GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "Stadium-Ventures/sv-dugout-pulse")
_GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
_SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
_CHANNEL_ID = os.environ.get("DUGOUT_PULSE_CHANNEL_ID", "C0ACLMAHXPF")
_MODEL = os.environ.get("HEALTH_MONITOR_MODEL", "claude-opus-4-8")
_ISSUE_LABEL = "pulse-health"


# --------------------------------------------------------------------------- #
# Signal gathering — every fetch degrades gracefully to empty on failure so a
# single upstream outage can't crash the monitor.
# --------------------------------------------------------------------------- #
def _load_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        logger.warning("Could not read %s", path)
        return None


def gather_health() -> dict:
    """Current health envelope + a compact recent trend."""
    pulse = _load_json(_PULSE_PATH) or {}
    current = pulse.get("health") or {}

    trend = []
    history = _load_json(_HISTORY_PATH) or []
    if isinstance(history, list):
        for rec in history[-40:]:
            trend.append({
                "generated_at": rec.get("generated_at"),
                "severity": rec.get("severity"),
                "blocked_clients": len(rec.get("blocked_clients") or []),
                "fallback_clients": len(rec.get("fallback_clients") or []),
                "carry_forward_clients": len(rec.get("carry_forward_clients") or []),
                "total_clients": rec.get("total_clients"),
                "blocked_sources": rec.get("blocked_sources") or [],
            })
    return {
        "generated_at": pulse.get("generated_at"),
        "current": current,
        "recent_runs_trend": trend,
    }


def gather_slack(hours: int = 24) -> dict:
    """Recent #dugout-pulse messages, flagging non-bot (colleague) comments."""
    if not _SLACK_BOT_TOKEN:
        return {"available": False, "reason": "SLACK_BOT_TOKEN not set", "messages": []}

    oldest = (datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp()
    try:
        resp = requests.get(
            "https://slack.com/api/conversations.history",
            headers={"Authorization": f"Bearer {_SLACK_BOT_TOKEN}"},
            params={"channel": _CHANNEL_ID, "oldest": f"{oldest:.6f}", "limit": 200},
            timeout=20,
        )
        data = resp.json()
        if not data.get("ok"):
            return {"available": False, "reason": data.get("error", "unknown"),
                    "messages": []}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "messages": []}

    messages = []
    for m in data.get("messages", []):
        # A human comment has a real user and is not a bot/app/webhook post.
        is_human = (
            "user" in m
            and "bot_id" not in m
            and m.get("subtype") not in ("bot_message",)
            and not m.get("app_id")
        )
        messages.append({
            "from": "colleague" if is_human else "bot",
            "user": m.get("user", ""),
            "text": (m.get("text") or "")[:500],
            "reply_count": m.get("reply_count", 0),
        })
    messages.reverse()  # chronological
    return {"available": True, "messages": messages}


def _gh(method: str, path: str, **kw):
    if not _GITHUB_TOKEN:
        return None
    url = f"https://api.github.com/repos/{_GITHUB_REPO}{path}"
    try:
        resp = requests.request(
            method, url,
            headers={
                "Authorization": f"Bearer {_GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=20, **kw,
        )
        return resp
    except Exception:
        logger.exception("GitHub API call failed: %s %s", method, path)
        return None


def gather_ci_runs(limit: int = 25) -> list:
    resp = _gh("GET", f"/actions/runs?per_page={limit}")
    if not resp or resp.status_code != 200:
        return []
    out = []
    for r in resp.json().get("workflow_runs", []):
        # Skip the high-frequency pulse data-commit runs unless they failed.
        if r.get("name") == "Update pulse data" and r.get("conclusion") == "success":
            continue
        out.append({
            "name": r.get("name"),
            "conclusion": r.get("conclusion"),
            "status": r.get("status"),
            "created_at": r.get("created_at"),
            "url": r.get("html_url"),
        })
    return out[:limit]


def gather_open_issues() -> list:
    resp = _gh("GET", f"/issues?labels={_ISSUE_LABEL}&state=open&per_page=50")
    if not resp or resp.status_code != 200:
        return []
    return [{"number": i["number"], "title": i["title"]}
            for i in resp.json() if "pull_request" not in i]


# --------------------------------------------------------------------------- #
# Reasoning
# --------------------------------------------------------------------------- #
_SYSTEM = """You are the automated health monitor for SV Dugout Pulse, a live \
college/pro/HS baseball stats pipeline that scrapes box scores and posts \
milestone alerts to Slack.

You are given: the current run-level health envelope, a trend of recent runs, \
the last 24h of the #dugout-pulse Slack channel (including any non-bot \
colleague comments), recent GitHub Actions run results, and the titles of \
GitHub issues already open for known problems.

Your job: decide what — if anything — is a REAL, NOVEL, ACTIONABLE problem a \
human should rework, versus normal operation. Be conservative. Do NOT raise:
- expected mid-week low game volume (Tue/Wed/Mon are light pre-postseason)
- a single transient GitHub runner failure ("not acquired by Runner") that a \
later run recovered from
- a small number of blocked/fallback clients on a heavy slate (the proxy pool \
and carry-forward are working as designed)
- anything already covered by an open issue (mark matches_open_issue=true)

DO surface:
- a sustained capture-rate collapse or a source blocked across many runs
- a colleague in #dugout-pulse saying something looks wrong/broken/stale
- a workflow that has failed repeatedly (not a one-off)
- a roster/parser regression that hasn't already been alerted

Writing rules (these go to humans):
- Plain English. Three beats: what's wrong, how we know (cite the evidence), \
what to do. No jargon, no thresholds, no raw field names in the summary.
- suggested_fix_prompt is a self-contained instruction a human can paste \
straight into Claude Code in this repo to start the fix — name the likely \
files/area and the symptom.
- digest_summary is ONE short paragraph for a muted Slack channel; write it \
only when something is actionable, else leave it empty."""

_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "overall_status": {"type": "string", "enum": ["healthy", "degraded", "critical"]},
        "digest_summary": {"type": "string"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "severity": {"type": "string", "enum": ["info", "warn", "critical"]},
                    "category": {"type": "string", "enum": [
                        "data_pipeline", "slack_colleague", "infra_ci", "scoring", "other"]},
                    "summary": {"type": "string"},
                    "suggested_fix_prompt": {"type": "string"},
                    "raise_issue": {"type": "boolean"},
                    "matches_open_issue": {"type": "boolean"},
                    "colleague_quote": {"type": "string"},
                },
                "required": ["title", "severity", "category", "summary",
                             "suggested_fix_prompt", "raise_issue",
                             "matches_open_issue", "colleague_quote"],
            },
        },
    },
    "required": ["overall_status", "digest_summary", "findings"],
}


def reason(context: dict) -> dict:
    import anthropic

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
    user = (
        "Here is the current state of SV Dugout Pulse. Assess it and return "
        "your findings.\n\n```json\n"
        + json.dumps(context, indent=2, default=str)[:120000]
        + "\n```"
    )
    resp = client.messages.create(
        model=_MODEL,
        max_tokens=16000,
        thinking={"type": "adaptive"},
        system=_SYSTEM,
        output_config={"format": {"type": "json_schema", "schema": _SCHEMA}},
        messages=[{"role": "user", "content": user}],
    )
    text = next((b.text for b in resp.content if b.type == "text"), "{}")
    return json.loads(text)


# --------------------------------------------------------------------------- #
# Actions
# --------------------------------------------------------------------------- #
def ensure_label():
    _gh("POST", "/labels", json={
        "name": _ISSUE_LABEL, "color": "d73a4a",
        "description": "Raised by the automated health monitor; needs a human/Claude Code fix.",
    })  # 422 if it already exists — harmless.


def create_issue(finding: dict) -> str:
    sev = finding["severity"].upper()
    body = (
        f"_Raised automatically by the Dugout Pulse health monitor "
        f"({datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC})._\n\n"
        f"**Severity:** {sev} · **Area:** {finding['category']}\n\n"
        f"## What's wrong\n{finding['summary']}\n\n"
    )
    if finding.get("colleague_quote"):
        body += f"## From #dugout-pulse\n> {finding['colleague_quote']}\n\n"
    body += (
        "## Suggested fix prompt (paste into Claude Code)\n```\n"
        f"{finding['suggested_fix_prompt']}\n```\n"
    )
    resp = _gh("POST", "/issues", json={
        "title": f"[pulse-health] {finding['title']}",
        "body": body,
        "labels": [_ISSUE_LABEL],
    })
    if resp and resp.status_code == 201:
        return resp.json().get("html_url", "")
    logger.error("Issue creation failed: %s", resp.text if resp else "no response")
    return ""


def main() -> int:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY not set — cannot run health monitor")
        return 1

    open_issues = gather_open_issues()
    context = {
        "now_utc": datetime.now(timezone.utc).isoformat(),
        "health": gather_health(),
        "slack_dugout_pulse_24h": gather_slack(),
        "recent_ci_runs": gather_ci_runs(),
        "open_health_issues": open_issues,
    }

    try:
        verdict = reason(context)
    except Exception:
        logger.exception("Claude reasoning failed")
        return 1

    findings = verdict.get("findings", [])
    status = verdict.get("overall_status", "healthy")
    logger.info("Overall status: %s | %d finding(s)", status, len(findings))

    # Raise issues for novel, actionable findings.
    new_issues = []
    if any(f.get("raise_issue") and not f.get("matches_open_issue") for f in findings):
        ensure_label()
    for f in findings:
        if f.get("raise_issue") and not f.get("matches_open_issue"):
            url = create_issue(f)
            if url:
                new_issues.append((f, url))
                logger.info("Opened issue: %s", url)

    # Post a digest only when something is actionable (no debug noise).
    digest = (verdict.get("digest_summary") or "").strip()
    if findings and digest:
        icon = {"critical": ":rotating_light:", "degraded": ":warning:"}.get(
            status, ":mag:")
        lines = [f"{icon} *Dugout Pulse health monitor* — {status}", "", digest]
        if new_issues:
            lines.append("")
            lines.append("*Filed for follow-up:*")
            for f, url in new_issues:
                lines.append(f"• <{url}|{f['title']}>")
        post_automation("\n".join(lines))
    else:
        logger.info("Nothing actionable — staying silent.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
