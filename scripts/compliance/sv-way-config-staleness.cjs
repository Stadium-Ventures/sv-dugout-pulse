#!/usr/bin/env node
// scripts/compliance/sv-way-config-staleness.cjs
//
// sv-way.config.json is a hand-authored judgment call (scripts/compliance/
// sv-way-check.cjs only READS it — nothing generates it, and nothing safely
// could: writing "role"/"roles_why"/"open_questions" correctly requires
// actually understanding the repo, and a script that guessed would violate
// this org's own "don't invent facts, leave a field out rather than guess"
// rule for exactly this kind of doc.
//
// What CAN be done for free, with no LLM/API cost: detect that the config is
// PROBABLY stale, so a human or a Claude session knows to redo the reading
// pass, instead of it silently drifting for weeks (which is how the
// 2026-07-29 batch got caught 5-6 weeks behind on 2026-09-03).
//
// Method: pull the latest date out of the config's own "_note" field (every
// copy in this org stamps "Authored <date>" and/or "Corrected <date>" there),
// then check whether any commit since that date touched a path this config's
// own content depends on. The watch-path list is derived from the config
// itself (its `roles` keys, `canon_trees`, `repo`) plus a fixed generic list
// (CLAUDE.md, package.json, sv-app.json, supabase/, api/) — so a config that
// declares more roles automatically gets watched more closely, with zero
// per-repo hand-tuning.
//
// REPO-AGNOSTIC ON PURPOSE, same as sv-way-check.cjs — drops into any
// Stadium Ventures repo unchanged. Exit 0 + silent when nothing looks stale;
// exit 1 + a plain-English summary on stdout when it does, for the calling
// workflow to post through that repo's #sv-automation contract.
//
//   node scripts/compliance/sv-way-config-staleness.cjs
//   node scripts/compliance/sv-way-config-staleness.cjs --json

'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const ROOT = path.resolve(__dirname, '..', '..');
const JSON_OUT = process.argv.includes('--json');
const CONFIG_PATH = path.join(ROOT, 'sv-way.config.json');

function git(args) {
  try {
    return execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' }).trim();
  } catch {
    return '';
  }
}

function latestNoteDate(note) {
  const dates = String(note || '').match(/\d{4}-\d{2}-\d{2}/g) || [];
  if (!dates.length) return null;
  return dates.sort().at(-1); // ISO strings sort chronologically
}

// Pull path-shaped tokens out of a `roles` key like
// "apps/staff/ (staff console)" or "src/ + public/ + docs/ (athlete PWA)".
function extractPathTokens(str) {
  return String(str || '')
    .split(/[+,]| \(/)
    .map((s) => s.trim())
    .filter((s) => /^[\w.-][\w./-]*\/?$/.test(s)); // looks like a relative path, not prose
}

function watchPaths(cfg) {
  // Cheap, low-churn, always-meaningful-when-touched metadata files.
  const set = new Set(['CLAUDE.md', 'sv-app.json', 'package.json']);
  for (const t of Array.isArray(cfg.canon_trees) ? cfg.canon_trees : []) set.add(t);
  // Only watch roles NOT declared "surface" — a surface is presentation code
  // this config already expects to churn constantly (UI polish, content
  // tweaks) without invalidating anything it claims. write-home /
  // operational-store / read-duty-consumer / any other declared role is a
  // structural fact the config depends on, so those trees ARE watched.
  for (const [k, v] of Object.entries(cfg.roles || {})) {
    if (String(v).trim().toLowerCase() === 'surface') continue;
    for (const t of extractPathTokens(k)) set.add(t);
  }
  return [...set].filter((p) => fs.existsSync(path.join(ROOT, p)));
}

function commitsTouchingSince(p, sinceDate) {
  const out = git(['log', `--since=${sinceDate}T00:00:00`, '--format=%H|%ad|%s', '--date=short', '--', p]);
  if (!out) return [];
  return out.split('\n').map((line) => {
    const [sha, date, ...rest] = line.split('|');
    return { sha: sha.slice(0, 7), date, subject: rest.join('|') };
  });
}

function main() {
  if (!fs.existsSync(CONFIG_PATH)) {
    return { stale: false, skipped: 'no sv-way.config.json in this repo' };
  }
  const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  // Prefer an explicit `_last_reviewed` stamp; fall back to the latest date
  // mentioned in `_note` prose (every WP1-authored config carries one: "...
  // Authored 2026-07-29 ... Corrected 2026-09-03 ..."). A config with
  // neither — e.g. one that is itself living/continuously edited rather than
  // a point-in-time snapshot — has no freshness signal to check against, so
  // this reports SKIP, not stale. A false "stale" alarm every run is exactly
  // the noise this org's own automation doctrine warns against (see the
  // mirror-parity-drift lesson in sv-registry's own sv-way.config.json:
  // recurring false positives "train readers to scroll past real drift").
  const noteDate = cfg._last_reviewed || latestNoteDate(cfg._note);
  if (!noteDate) {
    return { stale: false, skipped: 'no _last_reviewed field and no dated _note to check freshness against' };
  }
  if (!git(['rev-parse', '--is-inside-work-tree'])) {
    return { stale: false, skipped: 'not a git checkout (or full history unavailable) — cannot diff since note date' };
  }

  const signals = [];
  for (const p of watchPaths(cfg)) {
    const commits = commitsTouchingSince(p, noteDate);
    if (commits.length) signals.push({ path: p, mostRecent: commits[0], count: commits.length });
  }

  return { stale: signals.length > 0, noteDate, signals };
}

const result = main();

if (JSON_OUT) {
  console.log(JSON.stringify(result, null, 2));
  process.exit(result.stale ? 1 : 0);
}

if (result.skipped) {
  console.log(`sv-way.config.json staleness check: skipped (${result.skipped})`);
  process.exit(0);
}
if (!result.stale) {
  console.log(`sv-way.config.json staleness check: clean (no signal-path commits since ${result.noteDate})`);
  process.exit(0);
}

console.log('sv-way.config.json is likely stale.');
if (result.reason) {
  console.log(`Reason: ${result.reason}`);
} else {
  console.log(`Its _note says it was last authored/corrected ${result.noteDate}, but these paths it depends on changed since:`);
  for (const s of result.signals) {
    console.log(`  - ${s.path} (${s.count} commit${s.count === 1 ? '' : 's'}, most recent ${s.mostRecent.date} ${s.mostRecent.sha}: ${s.mostRecent.subject})`);
  }
}
console.log('\nThis check only detects drift — it does not rewrite the file. Have a Claude Code session (or the repo owner) re-read the repo and refresh sv-way.config.json, same method as the original authoring pass.');
process.exit(1);
