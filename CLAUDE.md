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
  purpose: milestone alerts, daily summer recap, placement-conflict pings, the
  daily MiLB watch (8:30 AM ET).
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

## Alerts that compare a player to himself

`scripts/milb_watch.py` + `milb_watch.yml` ("MiLB Watch", 12:30 UTC) is the one
alert here that grades **relative to a player's own season line**, not against a
game or a fixed league threshold: baseline = season to date minus the compared
window, recent = trailing 14d and 30d (the more actionable read wins), verdict =
`src/window_grader.py` thresholds applied to both. Built for Kent's 2026-08-13
ask in #justin-riemer — MiLB clients whose form has moved enough to justify a
front-office or farm-director call. It reads four things: a rate lull, a **usage
lull**, an absence, and a surge. A lull is also a drop in usage — so a thin
sample is a signal, not a gate — and usage is read on two horizons: sustained
(14 days vs the 16 before) and week over week (last 7 vs the 7 before, hitters
only, which is what catches an everyday player dropping to a bench role before
two weeks of it accumulate). Absences are IL-checked against the MLB Stats API
and dropped when the org already explained them. It is a **rolling board, not an alert stream** —
every category shows every morning it qualifies and drops off the morning it
doesn't; there is no cooldown and nothing is suppressed for having been posted
before (BE, 2026-08-14). Preview any change with
`python -m scripts.milb_watch --dry`; unit tests in `tests/test_milb_watch.py`.
It must run **after** the 11:00 UTC historical pass, which is what rebuilds the
`window_*.json` files it reads.

**Its Slack format is locked** (BE, 2026-08-14) and pinned byte-for-byte by
`test_locked_message_format`. That test failing means the message layout
changed — revert, or update the expected block deliberately and say why. Don't
reformat that copy in passing.

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
