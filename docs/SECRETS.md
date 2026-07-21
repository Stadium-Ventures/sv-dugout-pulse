# Secrets — where they live and how to get them

This file documents every secret this repo uses: what it's for and where the
canonical value lives. **Never put an actual secret value in this file, in
code, or in any commit.**

All CI secrets are GitHub Actions secrets on this repo
(https://github.com/Stadium-Ventures/sv-dugout-pulse/settings/secrets/actions).
Values can't be read back out of GitHub — get them from the sources below and
set with `gh secret set NAME -R Stadium-Ventures/sv-dugout-pulse`.

| Name | What it's for | Where the value comes from |
|---|---|---|
| `ANTHROPIC_API_KEY` | LLM health monitor | Anthropic Console → API Keys (https://console.anthropic.com/settings/keys) |
| `SLACK_WEBHOOK_URL` | Posts to the Dugout Pulse channel | Slack app → Incoming Webhooks (https://api.slack.com/apps) |
| `SV_AUTOMATION_WEBHOOK_URL` | Failure alerts → #sv-automation | Same Slack app page. Reference copy: Vercel sv-heartbeat env (https://vercel.com/stadium-ventures/sv-heartbeat/settings/environment-variables). |
| `SLACK_BOT_TOKEN` | Slack Web API access | Slack app → OAuth & Permissions |
| `RESEND_API_KEY` | Email sends | Resend dashboard → API Keys (https://resend.com/api-keys) |
| `ROSTER_URL`, `RECRUITS_URL` | Published-CSV URLs of source Google Sheets (config) | Google Sheets → File → Share → Publish to web → CSV |
| `HS_STATS_URL` | HS stats source sheet — **referenced in a workflow but NOT currently set on the repo** | Google Sheets published-CSV URL; set it if/when the HS workflow needs it |
| `SB_PROXY_URL`, `SB_HTTP_PROXY`, `SB_HTTP_PROXY_2`, `SB_HTTP_PROXY_3` | Proxy endpoints for the StatBroadcast bypass | Provisioned by Tom at the proxy provider; ask Tom |
| `GITHUB_TOKEN` | Built-in Actions token | Provided automatically by GitHub — nothing to set |

## Conventions

- To hand a secret to a teammate, set it where they need it (`gh secret set`)
  rather than pasting the value in Slack.
