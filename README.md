# SV Dugout Pulse

Real-time baseball player tracker for Stadium Ventures. Monitors MLB, MiLB, and NCAA players, displays stats on a live dashboard, and sends Slack alerts for notable performances.

## Live Dashboard

**URL:** https://stadium-ventures.github.io/sv-dugout-pulse

## Quick Reference

| Item | Value |
|------|-------|
| **Dashboard URL** | https://stadium-ventures.github.io/sv-dugout-pulse |
| **GitHub Repo** | https://github.com/Stadium-Ventures/sv-dugout-pulse |
| **Roster Source** | Google Sheet (published as CSV) |
| **Slack Channel** | #dugout-pulse |
| **Update Frequency** | Every 15 minutes during game hours |
| **Game Hours Coverage** | 11am ET - 2:30am ET (covers early NCAA games through late West Coast games) |

## Key Files

| File | Purpose |
|------|---------|
| `index.html` | The dashboard UI |
| `data/current_pulse.json` | Player stats data (auto-updated by cron) |
| `src/roster_manager.py` | Fetches roster from Google Sheet |
| `src/stats_engine.py` | Fetches stats from MLB API + NCAA scrapers |
| `src/performance_analyzer.py` | Grades performances (Milestone/Standout/Good/Routine/Soft Flag) |
| `src/alerts.py` | Slack alert logic |
| `src/config.py` | Settings, thresholds, column mappings |
| `main.py` | Main script that orchestrates everything |
| `generate_test_data.py` | Creates fake data for UI testing |
| `.github/workflows/pulse.yml` | Automated cron schedule |

## Slack Alerts

Alerts are sent to #dugout-pulse when:

| Trigger | Who | Example |
|---------|-----|---------|
| Home run | Any player, any tier | "Dax Kilby (T1) just hit a HR!" |
| Pitcher enters game | Any pitcher, any tier | "Garrett Whitlock (T1) is pitching!" |
| 5+ strikeouts | Any pitcher, any tier | "Garrett Whitlock (T1) has 6 K's!" |
| 3+ times on base | T1/T2 hitters only | "Kellon Lindsey (T1) has reached base 3+ times!" |

### Adjusting Alert Rules

Edit `src/alerts.py` to change thresholds or add new triggers. The `check_and_send_alerts()` function contains all the logic.

## Data Sources

| Level | Source | Reliability |
|-------|--------|-------------|
| MLB/MiLB | Official MLB Stats API (free, no key required) | High |
| NCAA | Sidearm Sports → StatBroadcast → D1Baseball → stats.ncaa.org | Varies by school |

### NCAA Notes

NCAA data is fragmented. The system tries multiple scrapers in order:
1. **Sidearm Sports** - Many Power 5 schools
2. **StatBroadcast** - Live stats platform
3. **D1Baseball.com** - Covers all D1
4. **stats.ncaa.org** - Fallback

If a school's data isn't populating, you may need to add their specific feed URL to `src/stats_engine.py`.

## How to Edit Code

### Option 1: Edit directly on GitHub (simple changes)
1. Go to https://github.com/Stadium-Ventures/sv-dugout-pulse
2. Navigate to the file you want to edit
3. Click the pencil icon (Edit)
4. Make changes
5. Click "Commit changes"
6. Dashboard updates automatically within minutes

### Option 2: Use Claude Code (complex changes)
1. Open Terminal
2. Run: `cd ~/sv-dugout-pulse && claude`
3. Describe what you want to change
4. Claude will edit the code
5. Push changes: `git add -A && git commit -m "Your message" && git push`

## How to Resume This Project in Claude Code

```bash
cd ~/sv-dugout-pulse
claude
```

Then tell Claude what you want to do. It will have access to all the code.

## Secrets (stored in GitHub)

These are configured in GitHub → Settings → Secrets and variables → Actions:

| Secret | Purpose |
|--------|---------|
| `ROSTER_URL` | Google Sheet CSV URL |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook |

**Do not share these publicly.** If compromised, regenerate them.

## Manual Actions

### Run the pulse manually (without waiting for cron)
1. Go to GitHub repo → Actions tab
2. Click "SV Dugout Pulse" in the left sidebar
3. Click "Run workflow" → "Run workflow"

### Regenerate test data
```bash
cd ~/sv-dugout-pulse
python3 generate_test_data.py
```

### Test locally
```bash
cd ~/sv-dugout-pulse
python3 -m http.server 8888
# Then open http://localhost:8888
```

## Schedule Details

The cron runs every 15 minutes during these windows (UTC):
- 3:00 PM - 11:59 PM UTC
- 12:00 AM - 6:45 AM UTC

**In Eastern Time:** 11:00 AM - 2:45 AM (next day)

This covers:
- Early afternoon NCAA games
- Evening MLB/MiLB games
- Late West Coast games

To change the schedule, edit `.github/workflows/pulse.yml`.

## Troubleshooting

### Dashboard shows old data
- Check the "Last updated" timestamp on the dashboard
- Go to Actions tab and verify the workflow ran successfully
- Manually trigger a run if needed

### Slack alerts not working
- Verify `SLACK_WEBHOOK_URL` secret is set correctly
- Test the webhook: `curl -X POST -H 'Content-type: application/json' --data '{"text":"Test"}' YOUR_WEBHOOK_URL`

### NCAA player shows "No game data"
- The school's feed may not be configured
- Check `src/stats_engine.py` for the school's scraper settings
- May need to add the school's specific feed URL

### Pro player shows "No game data"
- Player may not have played that day
- Player name in roster may not match MLB database exactly
- Check MLB Stats API can find them: player lookup uses exact name matching

## Roster Management

The roster is managed in Google Sheets, not in this code. To add/remove players:
1. Edit the Google Sheet
2. Changes are picked up automatically on the next cron run

**Important columns:**
- `Level` must be "Pro" or "NCAA" (High School players are filtered out)
- `Tier` (1-4) determines roster priority and some alert rules
- `Org` is the team name (must match what the stats APIs expect)

## Performance Grades

| Grade | Emoji | Hitter Criteria | Pitcher Criteria |
|-------|-------|-----------------|------------------|
| Milestone | 💎 | Debut, 1st HR, cycle | Debut, no-hitter, 1st win/save |
| Standout | 🔥 | HR, 3+ hits, 3+ RBI | QS, 5+ Ks, save |
| Good | ✅ | 2+ hits, RBI, run | 3+ clean IP |
| Routine | 😐 | 1 hit, standard game | Average outing |
| Soft Flag | 🚩 | 0-for-4+ | 3+ ER in short outing |

## Future Improvements

- [ ] Add more NCAA school-specific scrapers as we discover feed URLs
- [ ] Tune alert thresholds based on real-world noise levels
- [ ] Consider custom domain (e.g., pulse.stadiumventures.com)
- [ ] Add historical game log view per player

---

*Built with Claude Code, February 2026*
