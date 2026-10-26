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
// Method: the baseline is the most recent commit that changed
// sv-way.config.json itself, made by a person rather than a bot. That commit
// is the last time someone read the repo and wrote the config down, so the
// check flags any later commit (<baseline>..HEAD) that touched a path this
// config's own content depends on. Work done earlier the same day, including
// the very commits a refresh covered, sits before the baseline and no longer
// counts. If the config has no usable history (a shallow clone, or a config
// never committed), the check falls back to the date in `_last_reviewed`, or
// the latest date in `_note` ("Authored <date>" / "Corrected <date>"), and
// counts every commit since that date. A config with no date at all is still
// skipped (see main()).
//
// Bot commits never count, as the baseline or as drift: an author name
// ending in "[bot]" (sv-way-sync[bot], github-actions[bot]), the GitHub
// Actions bot email, or a subject starting with "[bot]" (the org's marker
// for a scheduled job that commits under a person's name). Without this the
// doctrine-block sync in this same workflow, and any daily data refresh,
// would raise the alarm on their own every week.
//
// The watch-path list is derived from the config itself (its non-surface
// `roles` keys and `canon_trees`) and NOTHING else (d-2026-10-20-258 (1)):
// drift is a change to a path the config makes a role claim about. Until
// 2026-10-20 a fixed generic list (CLAUDE.md, package.json, sv-app.json) was
// watched too, so an app-card edit or a doctrine-block sync posted "may be
// stale" to #sv-automation across five repos on 10-19. Changes to those
// non-role paths are now QUIET: listed under `quiet` in --json and in the
// log, never a stale verdict, never posted.
//
// REPO-AGNOSTIC ON PURPOSE, same as sv-way-check.cjs — drops into any
// Stadium Ventures repo unchanged. Exit 0 + silent when nothing looks stale;
// exit 1 + a plain-English summary on stdout when it does, for the calling
// workflow to post through that repo's #sv-automation contract.
//
// STRUCTURAL CHANGE ONLY (BE 2026-10-26, "fix sv automation durably"): a role
// claim is about which trees exist and what they hold, so only a commit that
// ADDS, DELETES or RENAMES a file under a watched path counts as drift. Edits to
// existing files (new content rows, a tweak to a writer) never invalidate a role
// and used to post every week: performance-portal's content CSV, a script edit in
// a write-home. A new migration, a new writer file or a removed tree still flags.
//
// NOT A #sv-automation POST (same ruling): a stale config is housekeeping, not a
// break. With --write the check records its verdict in sv-way.staleness.json at
// the repo root (rewritten only when the verdict or its evidence changes, so a
// clean repo never churns), and the SV Internal Hub's Monday hygiene digest reads
// that file from every repo and lists a stale config beside the other registry
// gaps. Canon lives HERE in sv-registry; every other repo pulls it fresh from
// https://sv-internal-hub.vercel.app/tools/sv-way-config-staleness.cjs on each
// run of its sv-way-sync workflow, so the copies can no longer drift apart.
//
//   node scripts/compliance/sv-way-config-staleness.cjs
//   node scripts/compliance/sv-way-config-staleness.cjs --json
//   node scripts/compliance/sv-way-config-staleness.cjs --write   (records sv-way.staleness.json, always exits 0)

'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const ROOT = path.resolve(__dirname, '..', '..');
const JSON_OUT = process.argv.includes('--json');
const WRITE = process.argv.includes('--write');
const STATE_PATH = path.join(ROOT, 'sv-way.staleness.json');
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

// Metadata a config does not make role claims about: watched for the log only (QUIET).
const QUIET_PATHS = ['CLAUDE.md', 'sv-app.json', 'package.json'];

function watchPaths(cfg) {
  const set = new Set();
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

// One commit per line: sha, author date, author name, author email, subject,
// separated by the ASCII unit separator so a "|" in a subject is harmless.
const LOG_FORMAT = '--format=%H%x1f%ad%x1f%an%x1f%ae%x1f%s';

function gitLog(args) {
  const out = git(['log', LOG_FORMAT, '--date=short', ...args]);
  if (!out) return [];
  return out.split('\n').map((line) => {
    const [sha, date, name, email, subject] = line.split('\x1f');
    return { sha, date, name: name || '', email: email || '', subject: subject || '' };
  });
}

const BOT_EMAIL = /^(?:actions?@github\.com|(?:\d+\+)?github-actions\[bot\]@users\.noreply\.github\.com)$/i;

// The doctrine-block sync commits under whatever identity the sibling repo's
// workflow uses, so its own subject marks it as automated (d-258 (1)).
const DOCTRINE_SYNC = /^Sync SV Way doctrine block\b/i;

function isBot(c) {
  return /\[bot\]$/i.test(c.name.trim()) || BOT_EMAIL.test(c.email.trim()) || /^\[bot\]/i.test(c.subject.trim())
    || DOCTRINE_SYNC.test(c.subject.trim());
}

// The last human commit that changed the config, or null when there is no
// usable history to anchor on (a shallow clone's oldest commit would look
// like it "added" every file, so a shallow checkout always falls back).
function configBaseline() {
  if (git(['rev-parse', '--is-shallow-repository']) !== 'false') return null;
  const human = gitLog(['--', 'sv-way.config.json']).find((c) => !isBot(c));
  return human ? human.sha : null;
}

function commitsTouchingSince(p, baseline, sinceDate, structuralOnly = true) {
  const range = baseline ? [`${baseline}..HEAD`] : [`--since=${sinceDate}T00:00:00`];
  return gitLog([...range, ...(structuralOnly ? ['--diff-filter=ADR'] : []), '--', p])
    .filter((c) => !isBot(c))
    .map((c) => ({ sha: c.sha.slice(0, 7), date: c.date, subject: c.subject }));
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
    return { stale: false, skipped: 'not a git checkout (or full history unavailable), so there is nothing to diff since the note date' };
  }

  const baseline = configBaseline();
  const signals = [];
  const watched = watchPaths(cfg);
  for (const p of watched) {
    const commits = commitsTouchingSince(p, baseline, noteDate);
    if (commits.length) signals.push({ path: p, mostRecent: commits[0], count: commits.length });
  }
  const quiet = [];
  for (const p of QUIET_PATHS.filter((q) => !watched.includes(q) && fs.existsSync(path.join(ROOT, q)))) {
    const commits = commitsTouchingSince(p, baseline, noteDate, false); // listed for the log only, so every human commit
    if (commits.length) quiet.push({ path: p, mostRecent: commits[0], count: commits.length });
  }

  return { stale: signals.length > 0, noteDate, signals, quiet };
}

const result = main();

if (WRITE) {
  // The verdict, minus anything that changes on every run, so a clean repo never gets a churn commit.
  const state = {
    _what: 'Written by scripts/compliance/sv-way-config-staleness.cjs --write (sv-way-sync workflow). Read by the SV Internal Hub Monday hygiene digest. Do not hand-edit.',
    stale: !!result.stale,
    skipped: result.skipped || null,
    config_reviewed: result.noteDate || null,
    drift: (result.signals || []).map((s) => ({ path: s.path, commits: s.count, latest: { sha: s.mostRecent.sha, date: s.mostRecent.date, subject: s.mostRecent.subject } })),
  };
  const next = JSON.stringify(state, null, 2) + '\n';
  let prev = null;
  try { prev = fs.readFileSync(STATE_PATH, 'utf8'); } catch { prev = null; }
  if (prev !== next) { fs.writeFileSync(STATE_PATH, next); console.log(`sv-way.staleness.json: ${state.stale ? 'stale' : 'clean'} (updated)`); }
  else console.log(`sv-way.staleness.json: ${state.stale ? 'stale' : 'clean'} (unchanged)`);
  process.exit(0);
}

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
  for (const q of result.quiet || []) console.log(`  quiet: ${q.path} changed (${q.count} commit${q.count === 1 ? '' : 's'}); not a role path, so not drift`);
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
console.log('\nThis check only detects drift; it does not rewrite the file. Have a Claude Code session (or the repo owner) re-read the repo and refresh sv-way.config.json, same method as the original authoring pass.');
process.exit(1);
