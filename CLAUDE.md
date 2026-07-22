# SV Dugout Pulse

GitHub Actions player-monitoring dashboard: Python scrapers rebuild a static
GitHub Pages site every 15 min during game hours and alert on notable
performances. Orchestrator: `main.py`; scraper waterfall: `src/stats_engine.py`;
docs: `README.md`, `docs/health_monitor.md`, `docs/SECRETS.md`.

- Bot commits (`Update pulse data [bot]`) land every 15 minutes — always
  `git pull` before pushing; never force-push main.
- `data/**` is machine-written (except `data/summer_ball_placements.json`,
  which is hand-transcribed from Kent's sheet).

## Slack channels — scope rule (keep every new alert compliant)

- **#dugout-pulse** (`SLACK_WEBHOOK_URL`) — feature output humans read on
  purpose: milestone alerts, daily summer recap, placement-conflict pings.
- **#sv-automation** (`SV_AUTOMATION_WEBHOOK_URL`, channel ID `C0BE0ELP92Q`) —
  bugs, failures, and health findings ONLY. It's muted; a post there means
  "act on this." Never move feature output here, never leave ops noise on
  #dugout-pulse (Kent's rule, 2026-06-29).
- **Every** #sv-automation post goes through `scripts/_automation_notify.py`
  (`post_automation`) so it carries the "Dugout Pulse" product label, and must
  follow the message contract:
  1. Lead with the product label (the helper does this).
  2. Tag each finding `🛠️ Code change` vs `👤 Manual`.
  3. Three plain-English beats: **what broke / how we know / what to do**.
     No internal thresholds, jargon, or dev-only diagnostics.
  4. **Silent when healthy** — no "all good" posts, ever.

## Health checks (where they live)

- `scripts/cron_health_alert.py` + `.github/workflows/cron_health_alert.yml`
  ("Daily Health Check", 22:00 UTC) — daily self-health-check: freshness of
  `current_pulse.json`, `bbref_stats.json`, `summer_ball_rosters.json`.
  Catches silent cron skips and timeout-CANCELLED runs (those never trigger
  `if: failure()` alerts). Test the wiring:
  `gh workflow run cron_health_alert.yml -f test=true`.
- `scripts/health_monitor.py` + `health_monitor.yml` (13:00 + 23:00 UTC) —
  LLM monitor; opens `pulse-health` GitHub issues, digest only when actionable.
- `pulse.yml` "Alert on failure" step — per-run failure alert.
- `scripts/summer_roster_regression_alert.py`, `summer_quiet_streak_alert.py`
  — rule-based summer alerts (run inside `summer_rosters.yml`).

Secrets (values + provenance): `docs/SECRETS.md`. Never commit a webhook URL
or secret value.

## SV Internal Hub registry

This app is registered at https://sv-internal-hub.vercel.app/apps/sv-dugout-pulse.
Whenever a change in this session adds, removes, or alters any of the following,
update `sv-app.json` at the repo root **in the same session** — don't leave it
for later (the hub reads it hourly and merges it over the registry):
- scheduled jobs / crons
- data sources in or destinations out (Slack channels, sheets, DBs, emails)
- hosting, deployment, or access/auth
- monitoring or known issues
- ownership or who uses it

Also update the `runbook` steps in `sv-app.json` if the local-dev or deploy
process changed.
