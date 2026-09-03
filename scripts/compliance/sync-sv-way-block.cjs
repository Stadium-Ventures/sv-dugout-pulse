#!/usr/bin/env node
// scripts/compliance/sync-sv-way-block.cjs
//
// Closes the loop the staleness watchdog (sv-way-config-staleness.cjs)
// deliberately leaves open. That script can only DETECT drift in
// sv-way.config.json because fixing it requires judgment — writing an
// accurate `role`/`roles_why` needs someone to actually read the repo.
//
// The "🧭 The SV Way — North Star doctrine" block every CLAUDE.md carries is
// a different kind of thing entirely. THE-SV-WAY.md itself says so, in its
// own words (the line right above the fenced block this script syncs):
//
//   "Every surface that references this doctrine ... carries the block below
//   verbatim, byte-identical, and nothing more. The substance lives here so
//   it can't drift; the pointer is deliberately tiny and stable."
//
// A verbatim-copy requirement has zero judgment in it — there's nothing to
// understand, only a string to match. That makes it safe to fully automate,
// unlike sv-way.config.json's content. This script:
//
//   1. Fetches the canonical block from the public, unauthenticated doctrine
//      mirror (https://sv-internal-hub.vercel.app/sv-way.md) — no token, no
//      cross-repo secret, works the same from any repo's own Actions run.
//   2. Finds that same block in this repo's own CLAUDE.md.
//   3. If the repo doesn't carry the block at all, does nothing — whether a
//      given repo should carry the pointer is a content decision, not
//      something this script gets to make.
//   4. If the repo carries it and it differs even by one byte, rewrites
//      exactly that block in place and leaves everything else in the file
//      untouched.
//
// Exit 0 + silent if nothing changed or nothing to do. Exit 0 + prints what
// changed if it rewrote CLAUDE.md (the caller workflow commits that with the
// repo's own built-in GITHUB_TOKEN — no new secret, no cross-repo PAT).
// Exit 1 only if the local file's block boundaries can't be safely
// identified (never guesses at where to splice).
//
//   node scripts/compliance/sync-sv-way-block.cjs [--check]   # --check: exit 1 if would change, don't write

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');

const ROOT = path.resolve(__dirname, '..', '..');
const CLAUDE_MD = path.join(ROOT, 'CLAUDE.md');
const SOURCE_URL = 'https://sv-internal-hub.vercel.app/sv-way.md';
const HEADER_LINE = '## 🧭 The SV Way — North Star doctrine (read this first, every session)';
const CHECK_ONLY = process.argv.includes('--check');

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { 'User-Agent': 'sv-way-block-sync' } }, (res) => {
        if (res.statusCode !== 200) return reject(new Error(`GET ${url} -> ${res.statusCode}`));
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => resolve(body));
      })
      .on('error', reject);
  });
}

// Extract the fenced ```...``` block whose first line is HEADER_LINE.
function extractFencedBlock(md) {
  const lines = md.split('\n');
  const headerIdx = lines.findIndex((l) => l.trim() === HEADER_LINE);
  if (headerIdx === -1) return null;
  // Walk backward to the opening fence, forward to the closing fence.
  let openIdx = headerIdx;
  while (openIdx >= 0 && lines[openIdx].trim() !== '```') openIdx--;
  if (lines[openIdx]?.trim() !== '```') return null;
  let closeIdx = headerIdx;
  while (closeIdx < lines.length && lines[closeIdx].trim() !== '```') closeIdx++;
  if (lines[closeIdx]?.trim() !== '```') return null;
  return lines.slice(openIdx, closeIdx + 1).join('\n');
}

// Find the same block as it appears bare (no fence) inside a CLAUDE.md: from
// the header line up to (not including) the next top-level `## ` heading, or
// EOF. This is the shape every repo's CLAUDE.md actually carries it in.
function findLocalBlock(md) {
  const lines = md.split('\n');
  const startIdx = lines.findIndex((l) => l.trim() === HEADER_LINE);
  if (startIdx === -1) return null;
  let endIdx = lines.length;
  for (let i = startIdx + 1; i < lines.length; i++) {
    if (/^## /.test(lines[i])) {
      endIdx = i;
      break;
    }
  }
  // Trim trailing blank lines from the captured range so the diff is exact
  // text, not whitespace noise.
  while (endIdx > startIdx + 1 && lines[endIdx - 1].trim() === '') endIdx--;
  return { startIdx, endIdx, text: lines.slice(startIdx, endIdx).join('\n'), lines };
}

async function main() {
  if (!fs.existsSync(CLAUDE_MD)) {
    console.log('No CLAUDE.md in this repo — nothing to sync.');
    return 0;
  }
  const localMd = fs.readFileSync(CLAUDE_MD, 'utf8');
  const local = findLocalBlock(localMd);
  if (!local) {
    console.log('This repo\'s CLAUDE.md does not carry the SV Way block — nothing to sync (not this script\'s call to add one).');
    return 0;
  }

  const remoteMd = await fetchText(SOURCE_URL);
  const fenced = extractFencedBlock(remoteMd);
  if (!fenced) {
    console.error('Could not find the canonical fenced block at the doctrine source — refusing to guess. No change made.');
    return 1;
  }
  // Strip the ``` fence lines to get the bare block, matching local's shape.
  const canonical = fenced.split('\n').slice(1, -1).join('\n').trim();

  if (local.text.trim() === canonical) {
    console.log('SV Way block is already in sync.');
    return 0;
  }

  console.log('SV Way block differs from canon:');
  console.log(`  local:  ${local.text.split('\n').length} lines`);
  console.log(`  canon:  ${canonical.split('\n').length} lines`);

  if (CHECK_ONLY) {
    console.log('\n--check: not writing. Run without --check to sync.');
    return 1;
  }

  // local.endIdx already points AT the first preserved blank line before the
  // next heading (the trim loop above walked back past it without removing
  // it from `lines`) — so the slice from endIdx carries the right spacing on
  // its own; adding another blank here would double it up.
  const newLines = [...local.lines.slice(0, local.startIdx), ...canonical.split('\n'), ...local.lines.slice(local.endIdx)];
  fs.writeFileSync(CLAUDE_MD, newLines.join('\n'));
  console.log('\nCLAUDE.md updated — SV Way block now matches canon byte-for-byte.');
  return 0;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error('sync-sv-way-block failed:', err.message);
    process.exit(1);
  });
