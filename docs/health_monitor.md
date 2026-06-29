# Automated Health Monitoring

Two layers watch Dugout Pulse. Both post to **`#sv-automation`** — the muted,
cross-product channel — never to `#dugout-pulse` (Kent's request, 2026-06-29).
Live milestone alerts and performance recaps stay on `#dugout-pulse`.

## Layer 1 — rule-based alerts (already existed; now routed to #sv-automation)

| Script | Fires when | Workflow |
|---|---|---|
| `scripts/cron_health_alert.py` | BBRef refresh hasn't run in 24h+ | `cron_health_alert.yml` (6 PM ET) |
| `scripts/summer_roster_regression_alert.py` | A summer league's roster count drops >50% | `summer_rosters.yml` (4×/day) |
| `scripts/summer_quiet_streak_alert.py` | A client hasn't appeared in a game in 5+ days | `summer_rosters.yml` (4×/day) |

All three post through `scripts/_automation_notify.py` → `SV_AUTOMATION_WEBHOOK_URL`.

## Layer 2 — LLM health monitor (`scripts/health_monitor.py`)

Runs twice a day (`health_monitor.yml`, 9 AM + 7 PM ET). Reasons *across* signals
rather than firing on a single rule:

1. The health envelope on `data/current_pulse.json`
2. The recent capture/severity trend from `data/fetch_health_history.json`
3. The last 24h of `#dugout-pulse` — **including non-bot colleague comments**
   ("this player's stats look wrong")
4. Recent GitHub Actions run conclusions
5. Open `pulse-health` GitHub issues (so it doesn't re-raise known problems)

Claude (`claude-opus-4-8`) decides what's a real, novel, actionable problem, then:

- **Opens a GitHub issue** labelled `pulse-health` with a diagnosis + a
  ready-to-paste Claude Code fix prompt — the handoff for human/Claude rework.
- **Posts a plain-English digest** to `#sv-automation` only when something is
  actionable (silent otherwise — no debug noise).

It is deliberately conservative: it ignores expected mid-week low volume,
one-off runner failures, and normal proxy fallback.

## Setup (GitHub → repo Settings → Secrets and variables → Actions)

| Secret | What | Used by |
|---|---|---|
| `SV_AUTOMATION_WEBHOOK_URL` | Incoming Webhook URL for `#sv-automation` | both layers (posting) |
| `ANTHROPIC_API_KEY` | SV Anthropic account key | Layer 2 |
| `SLACK_BOT_TOKEN` | `xoxb-…` bot token w/ `groups:history`, invited to `#dugout-pulse` | Layer 2 (reading the channel) |

Without `SLACK_BOT_TOKEN`, Layer 2 still runs on logs/CI only and skips the
colleague-comment read. Without `SV_AUTOMATION_WEBHOOK_URL`, posts are logged
but not sent (never falls back to `#dugout-pulse`).

The Slack bot is intended to be a **shared SV automation bot** — one app/token
invited into each product channel as other products add readers, not one bot
per product.

## Reusing the notifier in other SV products

Copy `scripts/_automation_notify.py`, set `SV_AUTOMATION_WEBHOOK_URL` to the
same `#sv-automation` webhook, and call `post_automation(text)`.
