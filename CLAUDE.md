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

## Summer alerts go quiet with the season

`scripts/_summer_season.py`'s `season_is_active()` gates both the daily recap
(`summer_daily_slack.py`) and the quiet-streak alert (`summer_quiet_streak_alert.py`):
off once no tracked placement has logged a real (played) game in 8+ days,
back on the first time one does. Kent, 2026-08-17: the daily recap kept
posting "no client summer activity" for a week-plus after every reachable
league had actually finished. This replaces a manual off-switch — no action
needed at next year's season open or close, it follows `data/summer_game_log.json`.

## Alerts that compare a player to himself

`scripts/milb_watch.py` + `milb_watch.yml` ("MiLB Watch", 11:50 UTC) is the one
alert here that grades **relative to a player's own season line**, not against a
game or a fixed league threshold: baseline = season to date minus the compared
window, recent = trailing 14d and 30d (the more actionable read wins), verdict =
`src/window_grader.py` thresholds applied to both. Built for Kent's 2026-08-13
ask in #justin-riemer — MiLB clients whose form has moved enough to justify a
front-office or farm-director call. It reads four things: a rate lull, a **usage
lull**, an absence, and a surge. A lull is also a drop in usage — so a thin
sample is a signal, not a gate — and usage is read two ways, hitters only, on
the same 14/30 spans: **role** (PA per game PLAYED vs his season baseline —
"when he plays, is he still starting?", the earliest tell) and **share** (games
played out of his team's games — "is he still in the lineup?", which divides the
schedule out so an off-week isn't a benching). Candidates are resolved against
the MLB Stats API roster before posting: IL stints drop out, and an **org change
inside the window voids the share read** — a man signed four days ago hasn't
missed his new club's earlier games (Doughty, 2026-08-14). Role survives an org
change; share doesn't. A player just ACTIVATED off the IL gets the same
exclusion even though his current roster status now reads "Active" — the
transactions endpoint (`lookup_recent_il`), not `rosterEntries`, is what still
knows he was out, and without it an idle read for "no games in 14 days" has no
way to tell a real absence from a stint that just closed (Sterlin Thompson,
placed 07-31, activated 08-18 — his whole window was the injury, BE,
2026-08-19). The same closed-out stint voids the SHARE half of usage too, not
just idle — a few games back from injury reads as a usage crash otherwise,
since his own team-games share for the hurt stretch is genuinely near zero,
which is his rehab ramp-up, not a benching. Preview any change with
`python -m scripts.milb_watch --dry`; unit tests in `tests/test_milb_watch.py`.
It must run **after** the 11:00 UTC historical pass, which is what rebuilds the
`window_*.json` files it reads — 11:50 UTC is chosen so GitHub's habitual ~40
minute cron lag lands the post near 8:30 AM ET. Because that pass can be late
too, and because the report goes out **every day, non-negotiable** (BE,
2026-08-18), a stale refresh never suppresses the post — the script stamps it
with the date its numbers actually come from and tells #sv-automation, rather
than grading yesterday's numbers as today's or going dark.

**Cadence: flag once, then update on a delay.** A player posts the day he first
qualifies, then waits out his re-report window — **7 days hitters, 14 pitchers**
— even while he keeps qualifying, and the whole post is skipped when nobody is
new and nobody is due. Kent asked for this after reading the first edition,
which showed every qualifying player every morning: "space out the repetitive
player updates" (2026-08-16). Dropping off the board does NOT reset the clock
(state is kept for every tracked player, not just today's board). A status flip
(lull → trending up) is a new finding and is WORDED as one, but it waits out
the window like everything else: the window is a floor on the PLAYER (BE,
2026-09-05: "once a player is surfaced by the report, he does not appear again
for 1 week as a hitter, 2 weeks as a pitcher for a status check"). It used to
post at once, which put Dax Kilby in the channel on 08-31 for his closeout and
again on 09-01 as a surge — a closeout always changes the status family, so
every closeout armed a same-week re-post. A flip inside the window is held,
not lost. That "update 1
week after" is owed even if he's resolved by then — a hitter flagged and back to
normal a week later still gets a ✅ *Back to normal* line rather than silently
vanishing (BE, 2026-08-18, catching the gap live on Kellon Lindsey and Jake
Munroe). Fires once, on the player's own clock; re-qualifying afterward reads as
a brand-new flag — and still waits out the window from that closeout.

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
