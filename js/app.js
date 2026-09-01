// SV Dugout Pulse — dashboard logic.
// Extracted from index.html (2026-07-07) so app code and markup can evolve
// separately and functions are testable. Loaded at end of <body>, so the DOM
// is ready when this runs — do not move the tag into <head>.

const GRADE_ORDER = ['Milestone','Standout','Good','Routine','Off Day','DNP','Scheduled','No Data'];
const GRADE_CLASS = {
  'Milestone':'grade-milestone','Standout':'grade-standout','Good':'grade-good',
  'Routine':'grade-routine','Off Day':'grade-flag','DNP':'grade-nodata','Scheduled':'grade-scheduled','No Data':'grade-nodata'
};

// Window grade classes
const WINDOW_GRADE_CLASS = {
  'Hot':'grade-hot','Solid':'grade-solid','Steady':'grade-quiet',
  'Cold':'grade-cold','Insufficient':'grade-insufficient'
};

let allPlayers = [];
let dataGeneratedAt = null;
let runHealth = null;
let yesterdaySourceDate = null;
let autoRefreshTimer = null;
// Distinguishes "data still loading" from "data loaded, zero filter matches".
// Prevents the false "No players match" empty state during the initial fetch.
let dataLoaded = false;
let filters = { roster:'client', level:'all', position:'all', status:'all', grade:'all', heartbeat:'all' };
// Sticky filters — restore the user's last picks (saved on every manual filter
// click, never on tab-driven auto-defaults). Button states sync after DOM init.
const _FILTERS_LS_KEY = 'dp_filters_v1';
try {
  const saved = JSON.parse(localStorage.getItem(_FILTERS_LS_KEY) || 'null');
  if (saved && typeof saved === 'object') {
    for (const k of Object.keys(filters)) {
      if (typeof saved[k] === 'string') filters[k] = saved[k];
    }
  }
} catch (e) { /* corrupt storage — keep defaults */ }
function _persistFilters() {
  try { localStorage.setItem(_FILTERS_LS_KEY, JSON.stringify(filters)); } catch (e) {}
}
let searchQuery = '';
let heartbeatData = new Map();

// Time window state
let currentWindow = 'today';
const windowData = { today: null, yesterday: null, '7d': null, '14d': null, '30d': null, season: null };
const _windowFetchFailed = { today: false, yesterday: false, '7d': false, season: false };
const _scrollPositions = { today: 0, yesterday: 0, '7d': 0, season: 0 };
const WINDOW_PATHS = {
  today: 'data/current_pulse.json',
  yesterday: 'data/yesterday_pulse.json',
  '7d': 'data/window_7d.json',
  '14d': 'data/window_14d.json',
  '30d': 'data/window_30d.json',
  season: 'data/window_season.json'
};

// ---- Recent-form momentum (Pro: last 7D vs last 30D) --------------------
// Built from window_7d.json + window_30d.json on load, keyed by player, so the
// chip is available on any tab. Only "hot"/"cold" are stored — steady = no chip.
let momentumByPlayer = {};
const _PITCHER_POS = ['Pitcher','LHP','RHP','LHR','RHR','SP','RP','CL'];
function _numOr(v) {
  if (v == null || v === '--' || v === '') return null;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}
function _computeMomentum(s7, s30, isPitcher) {
  if (isPitcher) {
    const e7 = _numOr(s7.era), e30 = _numOr(s30.era), ip7 = _numOr(s7.ip), ip30 = _numOr(s30.ip);
    if (e7 == null || e30 == null || ip30 == null || ip30 < 8 || ip7 == null || ip7 < 2) return null;
    const better = e30 - e7;                 // ERA trending down = improving
    if (better >= 1.0) return { trend: 'hot', label: '🔥 heating up' };
    if (better <= -1.0) return { trend: 'cold', label: '🧊 cooling' };
    return null;
  }
  const o7 = _numOr(s7.ops), o30 = _numOr(s30.ops), pa7 = _numOr(s7.pa), pa30 = _numOr(s30.pa);
  if (o7 == null || o30 == null || pa30 == null || pa30 < 15 || pa7 == null || pa7 < 5) return null;
  const d = o7 - o30;
  if (d >= 0.150) return { trend: 'hot', label: '🔥 heating up' };
  if (d <= -0.150) return { trend: 'cold', label: '🧊 cooling' };
  return null;
}
async function loadMomentum() {
  try {
    const [r7, r30] = await Promise.all([
      fetch('data/window_7d.json?t=' + Date.now()),
      fetch('data/window_30d.json?t=' + Date.now()),
    ]);
    if (!r7.ok || !r30.ok) return;
    const j7 = await r7.json(), j30 = await r30.json();
    const arr7 = j7.players || j7, arr30 = j30.players || j30;
    const by7 = {};
    for (const e of arr7) by7[e.player_name] = e;
    const next = {};
    for (const e of arr30) {
      if (e.level !== 'Pro') continue;
      const s30 = e.stats || {}, s7 = (by7[e.player_name] || {}).stats || {};
      const pos = (e.tags && e.tags.position) || '';
      const isP = _PITCHER_POS.includes(pos) || ('ip' in s30);
      const m = _computeMomentum(s7, s30, isP);
      if (m) next[e.player_name] = m;
    }
    momentumByPlayer = next;
    if (typeof render === 'function' && dataLoaded) render();
  } catch (e) { /* non-fatal — chip just won't show */ }
}
function momentumChipHtml(p) {
  if (!p || p.level !== 'Pro') return '';
  const m = momentumByPlayer[p.player_name];
  return m ? `<span class="momentum-chip momentum-${m.trend}" title="Recent form: last 7 days vs last 30 days">${m.label}</span>` : '';
}

// ---- "Since yesterday" change strip --------------------------------------
// Surfaces the things worth noticing first: Pro clients whose team changed
// (call-up / send-down / trade) and yesterday's standout games. Dismissible
// per day (sessionStorage). Today tab only; hidden when there's nothing new.
function _changeStripDismissKey() {
  const et = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  return 'dp_changestrip_' + et.toISOString().slice(0, 10);
}
async function loadChangeStrip() {
  const el = document.getElementById('changeStrip');
  if (!el) return;
  try { if (sessionStorage.getItem(_changeStripDismissKey())) return; } catch (e) {}
  let yest;
  try {
    const r = await fetch('data/yesterday_pulse.json?t=' + Date.now());
    if (!r.ok) return;
    const raw = await r.json();
    yest = raw.players || raw;
  } catch (e) { return; }
  const firstByName = arr => {
    const m = {};
    for (const e of arr) if (e.player_name && !(e.player_name in m)) m[e.player_name] = e;
    return m;
  };
  const yMap = firstByName(yest.filter(p => p.level === 'Pro'));
  const tMap = firstByName((allPlayers || []).filter(p => p.level === 'Pro' && p.is_client !== false));
  const rows = [];
  // Team moves (call-up / send-down / trade) — team string changed overnight.
  for (const [name, t] of Object.entries(tMap)) {
    const y = yMap[name];
    if (y && y.team && t.team && y.team !== t.team) {
      rows.push(`<div class="cs-row">📈 <strong>${esc(name)}</strong> moved: ${esc(y.team)} → ${esc(t.team)}</div>`);
    }
  }
  // Yesterday's standout games for clients (dedup by name — doubleheader and
  // merge artifacts can produce repeated entries).
  const seenStandouts = new Set();
  for (const e of yest) {
    if (e.is_client === false || seenStandouts.has(e.player_name)) continue;
    const g = e.performance_grade || '';
    if ((g.includes('Milestone') || g.includes('Standout')) && rows.length < 6) {
      seenStandouts.add(e.player_name);
      rows.push(`<div class="cs-row">🔥 <strong>${esc(e.player_name)}</strong> yesterday: ${esc(e.stats_summary || '')}</div>`);
    }
  }
  if (!rows.length) return;
  el.innerHTML = `
    <div class="cs-head">Since yesterday
      <span class="cs-dismiss" title="Dismiss for today" onclick="try{sessionStorage.setItem(_changeStripDismissKey(),'1')}catch(e){};this.closest('.change-strip').hidden=true;">✕</span>
    </div>
    ${rows.slice(0, 6).join('')}`;
  el.hidden = currentWindow !== 'today';
}

// ---- Data-confidence dot ------------------------------------------------
// Shown ONLY when there's a caveat (no dot = clean & fresh). Yellow = carried
// forward from an earlier capture; red = a source blocked us / box never posted.
function confidenceDotHtml(p) {
  if (!p || p.level === 'Summer') return '';                       // summer has its own states
  if (p.game_status === 'N/A' || p.game_status === 'Scheduled') return '';  // nothing fetched yet
  const diag = Array.isArray(p.fetch_diagnostic) ? p.fetch_diagnostic : [];
  const blocked = diag.some(d => /block|403|waf/i.test(d.outcome || ''));
  if (blocked || p.stats_unavailable) {
    return '<span class="conf-dot conf-red" title="A source blocked our request — this line may be stale."></span>';
  }
  if (p.stats_captured_at && dataGeneratedAt) {
    const lagMin = (new Date(dataGeneratedAt) - new Date(p.stats_captured_at)) / 60000;
    if (lagMin >= 2) return '<span class="conf-dot conf-yellow" title="Carried forward from an earlier capture — not refreshed this run."></span>';
  }
  return '';
}

function gradeKey(g) {
  for (const k of GRADE_ORDER) { if (g.includes(k)) return k; }
  return 'No Data';
}

// Deduplicate players by name+team+game_number (git merge can introduce dupes)
function _dedupPlayers(players) {
  const seen = new Set();
  return players.filter(p => {
    const k = `${p.player_name}|${p.team}|${p.game_number || 0}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

const STATUS_PRIORITY = { 'Live': 0, 'Final': 1, 'Scheduled': 2, 'Cancelled': 3, 'N/A': 4 };
function statusTier(s) { return STATUS_PRIORITY[s] ?? 4; }

// Sub-sort within Live: 0=has stats, 1=in game no stats yet, 2=not confirmed in game
function liveSubTier(p) {
  const s = p.stats_summary || '';
  if (s === 'Game in progress — not in lineup' || s === 'Game in progress' || s === "Game in progress — hasn't pitched") return 2;
  if (s === 'In lineup' || s === 'In starting lineup' || s === 'Game in progress — not yet pitching') return 1;
  return 0;
}

function hasRealStats(p) {
  const s = p.stats_summary || '';
  return p.game_status === 'Final' && !s.startsWith('Did Not Play') && s !== 'No game scheduled' && s !== '';
}

function sortPlayers(players) {
  return players.slice().sort((a, b) => {
    // Status tier: Live > Final > Scheduled > Cancelled > N/A
    const aSt = statusTier(a.game_status);
    const bSt = statusTier(b.game_status);
    if (aSt !== bSt) return aSt - bSt;
    // Within Live: stats > in lineup/not yet pitching > not in game, then tier
    if (a.game_status === 'Live') {
      const aLive = liveSubTier(a);
      const bLive = liveSubTier(b);
      if (aLive !== bLive) return aLive - bLive;
    }
    // Within Final: players with real stats before DNP
    const aHas = hasRealStats(a) ? 0 : 1;
    const bHas = hasRealStats(b) ? 0 : 1;
    if (aHas !== bHas) return aHas - bHas;
    // Grade order (performance first)
    const aG = GRADE_ORDER.indexOf(gradeKey(a.performance_grade));
    const bG = GRADE_ORDER.indexOf(gradeKey(b.performance_grade));
    if (aG !== bG) return aG - bG;
    // Roster priority (tier)
    return (a.tags.roster_priority || 99) - (b.tags.roster_priority || 99);
  });
}

function matchesFilters(p) {
  if (searchQuery && !p.player_name.toLowerCase().includes(searchQuery)) return false;
  if (filters.roster === 'client' && p.is_client === false) return false;
  if (filters.roster === 'following' && p.is_client !== false) return false;
  if (filters.level !== 'all' && p.level !== filters.level) return false;
  if (filters.position !== 'all') {
    const pitcherPositions = ['Pitcher','LHP','RHP','LHR','RHR','SP','RP','CL'];
    const pos = p.tags?.position || '';
    if (filters.position === 'Pitcher' ? !pitcherPositions.includes(pos) : pitcherPositions.includes(pos)) return false;
  }
  if (filters.status !== 'all' && p.game_status !== filters.status) return false;
  if (filters.grade !== 'all' && !p.performance_grade.includes(filters.grade)) return false;
  if (filters.heartbeat !== 'all') {
    if (p.is_client === false) return false;
    const hb = heartbeatData.get(p.player_name.toLowerCase());
    if ((hb ? hb.status : 'gray') !== filters.heartbeat) return false;
  }
  return true;
}

function heartbeatHtml(playerName, isClient) {
  if (!isClient) return '';
  const key = playerName.toLowerCase();
  const hb = heartbeatData.get(key);
  const status = hb ? hb.status : 'gray';
  const colorClass = 'heartbeat-' + status;
  let title;
  if (hb) {
    title = 'Love Score: ' + hb.loveScore;
    if (hb.daysSinceContact != null) title += ' \u2014 Last contact: ' + hb.daysSinceContact + ' day' + (hb.daysSinceContact !== 1 ? 's' : '') + ' ago';
  } else {
    title = 'No Heartbeat data';
  }
  return `<a class="heartbeat-link" href="https://sv-heartbeat.vercel.app/" target="_blank" rel="noopener" title="${esc(title, true)}"><span class="heartbeat-icon ${colorClass}">\u2665</span></a>`;
}

function renderCard(p) {
  const gk = gradeKey(p.performance_grade);
  // "Stats unavailable" cards (we never got a box score) get an amber pill so
  // they're visually distinct from real DNPs (which are gray, deliberate).
  const gc = p.stats_unavailable ? 'grade-unavailable' : (GRADE_CLASS[gk] || 'grade-nodata');
  const isLive = p.game_status === 'Live';
  const isScheduled = p.game_status === 'Scheduled';
  const isClient = p.is_client !== false;
  const pri = p.tags.roster_priority || 4;
  const isYesterday = p.is_yesterday === true;
  // Manual-league summer placement with no game to show — reframe from a
  // broken-looking "— No Data" into an explicit "tracked by hand" state.
  const manualCard = isManualSummerCard(p);

  // Grade-tinted background class
  const TINT_MAP = { 'Milestone':'grade-tint-milestone', 'Standout':'grade-tint-standout', 'Good':'grade-tint-good', 'Off Day':'grade-tint-flag' };
  const tintClass = TINT_MAP[gk] || '';

  // Elevated stats for top performances
  const isElevated = gk === 'Milestone' || gk === 'Standout';

  // Game date display for yesterday entries
  let gameDateHtml = '';
  if (isYesterday && p.game_date) {
    const d = new Date(p.game_date + 'T12:00:00');
    const label = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    gameDateHtml = `<div class="game-date-label">${label}</div>`;
  }

  // Game time display — omitted for scheduled games since stats_summary
  // and game_context already show the time.
  const gameTimeHtml = '';

  // "As of HH:MM" badge — shown when stats were carried forward from an
  // earlier capture (run-to-run cache OR game log) and the lag is more
  // than 2 minutes. Honest signal to the user that this isn't fresh.
  let statsAsofHtml = '';
  if (p.stats_captured_at && dataGeneratedAt) {
    const captured = new Date(p.stats_captured_at);
    const generated = new Date(dataGeneratedAt);
    const lagMin = Math.floor((generated - captured) / 60000);
    if (lagMin >= 2) {
      const captTime = captured.toLocaleTimeString('en-US', {
        hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York'
      });
      const lagLabel = lagMin < 60 ? `${lagMin}m ago` : `${Math.floor(lagMin / 60)}h ago`;
      statsAsofHtml = `<span class="stats-asof" title="Stats captured at ${esc(captTime)} ET — current run could not refresh">as of ${esc(captTime)} · ${esc(lagLabel)}</span>`;
    }
  }

  // Plain-English status line — surfaced when the card looks empty so a
  // non-technical user knows whether it's our problem (blocked) or just no
  // box-score entry (player not in lineup). Hover for raw per-source detail.
  const summaryLc = (p.stats_summary || '').toLowerCase();
  const isFallbackState =
    summaryLc.includes('not in lineup') ||
    summaryLc.includes("hasn't pitched") ||
    summaryLc === 'game in progress' ||
    summaryLc.startsWith('did not play') ||
    summaryLc.startsWith('stats unavailable');
  let fetchDiagHtml = '';
  if (isFallbackState && Array.isArray(p.fetch_diagnostic) && p.fetch_diagnostic.length) {
    const blockedRe = /block|403|waf/i;
    const blockedSources = [...new Set(
      p.fetch_diagnostic.filter(d => blockedRe.test(d.outcome || '')).map(d => d.source || 'unknown')
    )];
    const triedSources = [...new Set(p.fetch_diagnostic.map(d => d.source || 'unknown'))];
    // Dedupe outcomes per source: if a source appears twice (e.g. StatBroadcast
    // "Couldn't reach site" on first try then "No game listed today" on retry),
    // collapse to the most informative outcome. Priority: any blocked outcome
    // wins (it's the actionable one), then "Game found, not in box score"
    // (high-signal), else the last outcome we saw.
    const outcomeRank = (o) => {
      const s = (o || '').toLowerCase();
      if (blockedRe.test(s)) return 3;
      if (s.includes('not in box')) return 2;
      if (s.includes("couldn't reach")) return 1;
      return 0;
    };
    const bySource = {};
    p.fetch_diagnostic.forEach(d => {
      const src = d.source || '?';
      if (!bySource[src] || outcomeRank(d.outcome) >= outcomeRank(bySource[src].outcome)) {
        bySource[src] = d;
      }
    });
    const techDetail = Object.values(bySource)
      .map(d => `${d.source || '?'}: ${d.outcome || '?'}`).join(' · ');
    // Did at least one source confirm the game exists and check the box score?
    // That's the "he just hasn't played yet" signal — high confidence the
    // system is working, the player just isn't in the lineup/box yet.
    const sawBoxScore = p.fetch_diagnostic.some(d =>
      (d.outcome || '').toLowerCase().includes('not in box'));
    const isPitcher = /pitcher|hp\b|two-way/i.test((p.tags && p.tags.position) || '');
    // Recovery-plan tail tells Kent what we're doing about it. The Yesterday
    // view points at the overnight backfill (3 AM + 5 AM ET, when StatBroadcast
    // is least loaded); live cards retry every 15 min until games end and
    // then fall through to the overnight backfill if still blocked.
    const recoveryTail = p.stats_unavailable
      ? ' Auto-retry overnight at ~3 AM and ~5 AM ET when StatBroadcast load drops.'
      : ' Auto-retry every 15 min; if still blocked at game end, overnight backfill picks it up at ~3 AM ET.';
    if (blockedSources.length) {
      // Both residential proxies failed for this event (the "blocked (Cloudflare 403)"
      // outcome only fires after the backup proxy also 403s and the event is poisoned).
      fetchDiagHtml = `<div class="card-status card-status-blocked" title="${esc(techDetail)}">
        <span class="card-status-icon">🚫</span>
        <span>${esc(blockedSources.join(', '))} blocked our request (both residential proxies hit Cloudflare WAF).${recoveryTail} Hover for detail.</span>
      </div>`;
    } else if (p.stats_unavailable) {
      fetchDiagHtml = `<div class="card-status card-status-blocked" title="${esc(techDetail)}">
        <span class="card-status-icon">⚠️</span>
        <span>Box score never posted to ${esc(triedSources.join(', '))}.${recoveryTail} Hover for detail.</span>
      </div>`;
    } else {
      // Position-aware "system is working, he just hasn't played yet" copy.
      // Pitchers don't appear in the box until they enter the game; hitters
      // either haven't batted yet or were left out of the starting lineup.
      const summary = sawBoxScore
        ? (isPitcher
            ? "Game is live — he hasn't entered yet. Pitchers only show in the box once they take the mound."
            : "Game is live — he isn't in the box yet (still waiting for a plate appearance or not in the lineup).")
        : "No game listed yet on any of our sources.";
      fetchDiagHtml = `<div class="card-status card-status-info" title="${esc(techDetail)}">
        <span class="card-status-icon">ℹ️</span>
        <span>${esc(summary)} Hover for detail.</span>
      </div>`;
    }
  }

  // Next game display when no game today
  const nextGameHtml = p.next_game && !isLive && !isScheduled && p.game_status !== 'Final'
    ? `<div class="next-game"><span class="next-label">Next:</span> <span class="next-value">${esc(p.next_game.display)}</span></div>`
    : '';

  // Summer cards in a status-only state (Shut Down, Injured, Pending, etc.)
  // get a muted treatment so they're visually distinct from live-game cards.
  const summerStatus = (p.tags && p.tags.placement_status) || '';
  const isInactiveSummer = p.level === 'Summer' && (
    ['Shut Down','Injured','Pending, 1st Half','Pending, 2nd Half','2nd Half','Status Update']
      .indexOf(p.game_status) !== -1
  );
  // Pending placement (rostered, not appearing yet) — muted + explained below.
  const pendingArrival = summerPlacementPending(p);
  const inactiveCls = (isInactiveSummer || manualCard || pendingArrival) ? ' summer-inactive' : '';

  return `
    <div class="card${isLive ? ' live' : ''}${inactiveCls} level-${p.level === 'Pro' ? 'pro' : p.level === 'Summer' ? 'summer' : 'ncaa'}${tintClass ? ' ' + tintClass : ''}">
      <div class="card-top">
        ${isLive ? '<div class="live-dot"></div>' : ''}
        ${confidenceDotHtml(p)}
        ${p.level === 'Summer'
          ? `<a class="player-name" href="summer_player.html?name=${encodeURIComponent(p.player_name)}">${esc(p.player_name)}</a>`
          : `<a class="player-name" href="player.html?name=${encodeURIComponent(p.player_name)}">${esc(p.player_name)}</a>`}
        ${heartbeatHtml(p.player_name, isClient)}
        ${isYesterday ? '<span class="badge badge-yesterday">Yesterday</span>' : ''}
        ${p.split_squad ? '<span class="badge badge-gm">SS</span>' : p.game_number ? `<span class="badge badge-gm">Gm ${p.game_number}</span>` : ''}
        <span class="badge ${isClient ? 'badge-client' : 'badge-following'}">${isClient ? 'Client' : 'Recruit'}</span>
        <span class="badge ${levelBadgeClass(p.level)}">${levelBadgeLabel(p.level)}</span>
        ${p.current_level ? `<span class="badge" style="background:rgba(34,197,94,0.15);color:#4ade80" title="Current level">${esc(p.current_level)}</span>` : ''}
        ${p.level === 'Summer' ? '' : `<span class="badge badge-tier-dim">T${pri}</span>`}
      </div>
      <div class="team-name">${esc(p.team)}</div>
      <div class="grade-bar ${gc}">${esc(p.stats_unavailable ? '⚠️ Stats Unavailable' : manualCard ? 'Tracked manually' : pendingArrival ? 'Pending' : p.performance_grade)}${momentumChipHtml(p)}</div>
      ${p.grade_reason && !manualCard && !pendingArrival ? `<div class="grade-reason">${esc(p.grade_reason)}</div>` : ''}
      <div class="stats-line${isElevated ? ' stats-elevated' : ''}">${(manualCard || (pendingArrival && !isScheduled)) ? '' : highlightStats(p.stats_summary) + statsAsofHtml}</div>
      ${p.game_context ? (p.box_score_url
        ? `<a class="game-context" href="${esc(p.box_score_url, true)}" target="_blank" rel="noopener">${esc(p.game_context)}</a>`
        : `<div class="game-context">${esc(p.game_context)}</div>`) : ''}
      ${fetchDiagHtml}
      ${gameDateHtml}
      ${gameTimeHtml}
      ${nextGameHtml}
      <div class="card-tags">
        <span class="tag">${esc(p.tags.position)}</span>
        ${p.tags.draft_class && p.tags.draft_class !== 'N/A' ? `<span class="tag">${esc(p.tags.draft_class)}</span>` : ''}
        ${peakChipHtml(p)}
      </div>
      ${summerSparklineHtml(p)}
      ${summerNoDataHint(p)}
      ${summerPendingHint(p)}
      <div class="card-actions">
        <a class="btn" href="${esc(p.social_search_url)}" target="_blank" rel="noopener">X Search</a>
        ${p.box_score_url ? `<a class="btn" href="${esc(p.box_score_url, true)}" target="_blank" rel="noopener">${p.game_status === 'Scheduled' ? 'Preview' : 'Box Score'}</a>` : ''}
        ${(p.tags && p.tags.league_site_url) ? `<a class="btn" href="${esc(p.tags.league_site_url, true)}" target="_blank" rel="noopener" title="Open the league's official team page in a new tab">League Site</a>` : ''}
      </div>
    </div>`;
}

// Summer leagues we pull live game data from (MLB Stats API / PrestoSports).
// Placements in any OTHER league are tracked by hand — no automated stats will
// ever appear, so those cards get a "tracked manually" treatment instead of a
// generic "No Data" that reads as broken. Keep in sync with loadSummerBanner.
const _SUMMER_REACHABLE_LEAGUES = new Set(['Cape Cod', 'MLB Draft', 'Appalachian', 'NECBL']);

function summerLeagueOf(p) {
  return (p && p.tags && p.tags.summer_league) || '';
}

// True for a Summer placement in a league we can't auto-pull, with no live/
// scheduled game to show. Expected-empty (tracked by hand), NOT a failure.
// Injured / shut-down placements are excluded — they get the inactive treatment.
function isManualSummerCard(p) {
  if (!p || p.level !== 'Summer') return false;
  const lg = summerLeagueOf(p);
  if (!lg || _SUMMER_REACHABLE_LEAGUES.has(lg)) return false;
  if (['Live', 'In Progress', 'Final', 'Scheduled'].indexOf(p.game_status) !== -1) return false;
  const st = (p.tags && p.tags.placement_status) || '';
  return st !== 'Shut Down' && st !== 'Injured';
}

const _summerHintBox = msg =>
  `<div style="margin-top:8px;padding:6px 9px;background:rgba(125,133,144,0.08);border-left:2px solid #7d8590;border-radius:4px;font-size:11px;color:#7d8590;line-height:1.4;">${esc(msg)}</div>`;

function summerNoDataHint(p) {
  if (p.level !== 'Summer') return '';
  const league = summerLeagueOf(p);
  const hasSiteLink = !!(p.tags && p.tags.league_site_url);
  // Tracked-by-hand leagues: always explain, whatever the game_status.
  if (isManualSummerCard(p)) {
    return _summerHintBox(hasSiteLink
      ? `We can't auto-pull ${league} — this placement is tracked by hand. Use the League Site button below for the latest.`
      : `We can't auto-pull ${league} — this placement is tracked by hand (no public stats feed).`);
  }
  // Reachable leagues sitting in an off-day / pre-game state — soft hedge.
  const isOffDay = ['Off Day', 'Roster Confirmed', 'Status Update'].indexOf(p.game_status) !== -1;
  if (!isOffDay) return '';
  return _summerHintBox('No stats here yet — likely an off day or pre-game. Will populate once today’s game starts.');
}

// Summaries that mean "no production" (rostered/scheduled but nothing recorded).
const _SUMMER_EMPTY_SUMMARIES = new Set([
  '', 'did not appear', 'no game today', 'no game scheduled',
  'no games yet — season just opened',
]);

// A placement marked Pending (or 2nd-Half, before that half opens) that isn't
// producing yet. They show as cards because their team has games, but the box
// scores come back empty/DNP — so surface the pending status instead of a bare
// "Did not appear" that reads like a bad game. Once real stats flow, this goes
// false and they render like any other player.
function summerPlacementPending(p) {
  if (!p || p.level !== 'Summer') return false;
  if (isManualSummerCard(p)) return false;  // manual-league treatment covers it
  const st = (p.tags && p.tags.placement_status) || '';
  if (!(st.indexOf('Pending') === 0 || st === '2nd Half')) return false;
  const s = (p.stats_summary || '').trim().toLowerCase();
  const scheduled = p.game_status === 'Scheduled' || s.startsWith('game at');
  const hasProduction = !scheduled && !_SUMMER_EMPTY_SUMMARIES.has(s);
  return !hasProduction;
}

function summerPendingHint(p) {
  if (!summerPlacementPending(p)) return '';
  const st = (p.tags && p.tags.placement_status) || 'Pending';
  const half = st.includes('2nd') ? ' (second half)' : st.includes('1st') ? ' (first half)' : '';
  return _summerHintBox(
    `Pending roster spot${half} — rostered but not appearing in games yet. `
    + `Stats fill in automatically once they get on the field.`);
}

// Peak projection chip (Scout the Statline, via the roster sheet). Pro
// players with pro stats only — everyone else renders nothing.
function peakChipHtml(p) {
  const t = p.tags || {};
  const parts = [];
  if (t.peak_war) parts.push(`${esc(t.peak_war)} WAR`);
  if (t.peak_wrc_plus) parts.push(`${esc(t.peak_wrc_plus)} wRC+`);
  if (t.peak_era_20tbf) parts.push(`${esc(t.peak_era_20tbf)} ERA/20`);
  if (!parts.length) return '';
  return `<span class="tag" title="Peak projection — Scout the Statline">Peak: ${parts.join(' · ')}</span>`;
}

function levelBadgeClass(level) {
  if (level === 'Pro') return 'badge-pro';
  if (level === 'NCAA') return 'badge-ncaa';
  if (level === 'HS') return 'badge-hs';
  if (level === 'Summer') return 'badge-summer';
  return 'badge-pro';
}

function levelBadgeLabel(level) {
  if (level === 'Pro') return '⚾ Pro';
  if (level === 'NCAA') return '🎓 NCAA';
  if (level === 'HS') return '🏫 HS';
  if (level === 'Summer') return '☀️ Summer';
  return level;
}

function esc(s, forAttr) {
  if (!s) return '';
  s = s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  if (forAttr) s = s.replace(/'/g,"\\'").replace(/"/g,'&quot;');
  return s;
}

function highlightStats(summary) {
  if (!summary) return '';
  let s = esc(summary);
  s = s.replace(/\b(\d+\s)?(HR)\b/g, '<span class="stat-highlight-hr">$&</span>');
  s = s.replace(/\b(\d+\s)?(3B)\b/g, '<span class="stat-highlight-xbh">$&</span>');
  s = s.replace(/\b(\d+\s)?(2B)\b/g, '<span class="stat-highlight-xbh">$&</span>');
  s = s.replace(/\b(\d+\s)?(RBI)\b/g, '<span class="stat-highlight-rbi">$&</span>');
  s = s.replace(/\b(\d+\s)?(SB)\b/g, '<span class="stat-highlight-sb">$&</span>');
  s = s.replace(/\b(\d+\s)?(BB)\b/g, '<span class="stat-highlight-bb">$&</span>');
  s = s.replace(/\b(\d+\s)?(K)\b/g, '<span class="stat-highlight-k">$&</span>');
  return s;
}

function windowGradeKey(g) {
  if (!g) return 'Insufficient';
  for (const k of ['Hot','Solid','Steady','Cold','Insufficient']) {
    if (g.includes(k)) return k;
  }
  return 'Insufficient';
}

function rateColor(val, thresholds) {
  // thresholds = [hot, warm, cold] e.g. [.900, .750, .550] for OPS
  if (val === '--' || val == null) return '';
  const n = parseFloat(val);
  if (isNaN(n)) return '';
  if (n >= thresholds[0]) return 'rate-hot';
  if (n >= thresholds[1]) return 'rate-warm';
  if (n < thresholds[2]) return 'rate-ice';
  return '';
}

function eraColor(val) {
  if (val === '--' || val == null) return '';
  const n = parseFloat(val);
  if (isNaN(n)) return '';
  if (n <= 2.00) return 'rate-hot';
  if (n <= 3.50) return 'rate-warm';
  if (n >= 5.00) return 'rate-ice';
  return '';
}

// Compact one-line stat summary for a per-level breakdown row.
function _levelLine(s, isPitcher) {
  if (isPitcher) {
    return `${s.era ?? '--'} ERA, ${s.whip ?? '--'} WHIP · ${s.ip ?? '--'} IP, ${s.k ?? '--'} K`;
  }
  return `${s.avg ?? '--'}/${s.obp ?? '--'}/${s.slg ?? '--'} · ${s.hr ?? '--'} HR, ${s.rbi ?? '--'} RBI`;
}
function levelBreakdownHtml(splits, isPitcher, currentLevel) {
  if (!splits) return '';
  const rows = splits.map(sp =>
    `<div class="lvl-row"><span class="lvl-tag">${_summerEscape(sp.level)}</span>`
    + `<span class="lvl-line">${_summerEscape(_levelLine(sp.stats || {}, isPitcher))}</span>`
    + (sp.level === currentLevel ? '<span class="lvl-now" title="Current level">now</span>' : '')
    + `<span class="lvl-g">${sp.games_played} G</span></div>`
  ).join('');
  return `<div class="level-breakdown"><div class="level-breakdown-head">By level</div>${rows}</div>`;
}

function renderWindowCard(p) {
  const position = p.tags?.position || 'Hitter';
  // Multi-level season: rates don't blend across levels, so the headline grid
  // shows ONE level's line and we list every level in a breakdown below.
  // Headline = the player's CURRENT level when we know it ("how's he doing
  // where he is now"), falling back to the top level played.
  const splits = (p.level_splits && p.level_splits.length > 1) ? p.level_splits : null;
  const headline = splits ? (splits.find(s => s.level === p.current_level) || splits[0]) : null;
  const stats = headline ? (headline.stats || {}) : (p.stats || {});
  const isPitcher = ['Pitcher','LHP','RHP','LHR','RHR','SP','RP','CL'].includes(position) || 'ip' in stats;
  // Headline grade tracks the headline level too, so it can't disagree
  // with the line shown (falls back to the combined grade until data refreshes).
  const gradeStr = headline ? (headline.window_grade || p.window_grade) : p.window_grade;
  const gk = windowGradeKey(gradeStr);
  const gc = WINDOW_GRADE_CLASS[gk] || 'grade-insufficient';
  const isClient = p.is_client !== false;
  // Games badge tracks the headline level, not the season total — the
  // headline shows that level's line as-is; the breakdown below has the rest.
  const gp = headline ? (headline.games_played ?? 0) : (p.games_played ?? 0);
  const gpLabel = gp === 1 ? '1 G' : `${gp} G`;

  let statsHtml;
  if (isPitcher) {
    const isSparse = stats.ip === '--';
    const sp = isSparse ? 'sparse' : '';
    statsHtml = `
      <div class="stat-row pitcher-row">
        <div class="stat-cell">
          <span class="stat-label">IP</span>
          <span class="stat-value ${sp}">${stats.ip ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">K</span>
          <span class="stat-value ${sp}">${stats.k ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">BB</span>
          <span class="stat-value ${sp}">${stats.bb ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">ERA</span>
          <span class="stat-value ${sp} ${isSparse ? '' : eraColor(stats.era)}">${stats.era ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">WHIP</span>
          <span class="stat-value ${sp}">${stats.whip ?? '--'}</span>
        </div>
      </div>
      <div class="stat-row" style="grid-template-columns: repeat(2, 1fr); max-width: 40%;">
        <div class="stat-cell">
          <span class="stat-label">K%</span>
          <span class="stat-value ${sp}">${stats.k_pct ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">BB%</span>
          <span class="stat-value ${sp}">${stats.bb_pct ?? '--'}</span>
        </div>
      </div>`;
  } else {
    const isSparse = stats.pa === '--';
    const sp = isSparse ? 'sparse' : '';
    statsHtml = `
      <div class="stat-row hitter-row">
        <div class="stat-cell">
          <span class="stat-label">PA</span>
          <span class="stat-value ${sp}">${stats.pa ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">H</span>
          <span class="stat-value ${sp}">${stats.h ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">HR</span>
          <span class="stat-value ${sp}">${stats.hr ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">BB</span>
          <span class="stat-value ${sp}">${stats.bb ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">K</span>
          <span class="stat-value ${sp}">${stats.k ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">SB</span>
          <span class="stat-value ${sp}">${stats.sb ?? '--'}</span>
        </div>
      </div>
      <div class="stat-row">
        <div class="stat-cell">
          <span class="stat-label">AVG</span>
          <span class="stat-value ${sp} ${isSparse ? '' : rateColor(stats.avg, [.350, .275, .200])}">${stats.avg ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">OBP</span>
          <span class="stat-value ${sp} ${isSparse ? '' : rateColor(stats.obp, [.400, .330, .280])}">${stats.obp ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">SLG</span>
          <span class="stat-value ${sp} ${isSparse ? '' : rateColor(stats.slg, [.550, .420, .320])}">${stats.slg ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">OPS</span>
          <span class="stat-value ${sp} ${isSparse ? '' : rateColor(stats.ops, [.900, .750, .550])}">${stats.ops ?? '--'}</span>
        </div>
      </div>
      <div class="stat-row" style="grid-template-columns: repeat(2, 1fr); max-width: 40%;">
        <div class="stat-cell">
          <span class="stat-label">K%</span>
          <span class="stat-value ${sp}">${stats.k_pct ?? '--'}</span>
        </div>
        <div class="stat-cell">
          <span class="stat-label">BB%</span>
          <span class="stat-value ${sp}">${stats.bb_pct ?? '--'}</span>
        </div>
      </div>`;
  }

  // Game log drill-down (7D only)
  let gameLogHtml = '';
  if (p.game_log && p.game_log.length > 0) {
    const logId = 'gl-' + p.player_name.replace(/\s+/g, '-').replace(/[^a-zA-Z0-9-]/g, '') + (p.game_number ? `-gm${p.game_number}` : '');
    const rows = p.game_log.map(g => {
      const d = new Date(g.date + 'T12:00:00');
      const dateStr = `${d.getMonth()+1}/${d.getDate()}`;
      const s = g.stats || {};
      let statLine;
      if ('ip' in s && !('ab' in s)) {
        // Pitcher line (handle '--' sentinel for sparse/missing data)
        const gIp = (s.ip == null || s.ip === '--') ? '0' : s.ip;
        const gEr = (s.er == null || s.er === '--') ? 0 : s.er;
        const gK  = (s.k  == null || s.k  === '--') ? 0 : s.k;
        const gBb = (s.bb == null || s.bb === '--') ? 0 : s.bb;
        statLine = `${gIp} IP, ${gEr} ER, ${gK} K, ${gBb} BB`;
      } else {
        // Hitter line
        const parts = [`${s.h ?? 0}-${s.ab ?? 0}`];
        if ((s.hr ?? 0) > 0) parts.push(`${s.hr} HR`);
        if ((s.rbi ?? 0) > 0) parts.push(`${s.rbi} RBI`);
        if ((s.r ?? 0) > 0) parts.push(`${s.r} R`);
        if ((s.bb ?? 0) > 0) parts.push(`${s.bb} BB`);
        if ((s.k ?? 0) > 0) parts.push(`${s.k} K`);
        if ((s.sb ?? 0) > 0) parts.push(`${s.sb} SB`);
        statLine = parts.join(', ');
      }
      const oppStr = g.opponent ? `<span class="game-log-opp">${esc(g.opponent)}</span> ` : '';
      const boxUrl = g.box_score_url || '';
      const rowInner = `<span class="game-log-date">${dateStr}</span><span class="game-log-stats">${oppStr}${esc(statLine)}</span>`;
      if (boxUrl) {
        return `<a class="game-log-row game-log-link" href="${esc(boxUrl, true)}" target="_blank" rel="noopener">${rowInner}</a>`;
      }
      return `<div class="game-log-row">${rowInner}</div>`;
    }).join('');
    gameLogHtml = `
      <div class="game-log-toggle" id="glt-${logId}" onclick="toggleGameLog('${logId}')" role="button" aria-expanded="false">
        <span class="game-log-chevron" aria-hidden="true">&#x25B6;</span>
        Game log (${p.game_log.length})
      </div>
      <div class="game-log-entries" id="gle-${logId}">
        ${rows}
      </div>`;
  }

  // Window-card names link to the in-app player page (Summer has its own).
  const detailUrl = (p.level === 'Summer' ? 'summer_player.html' : 'player.html')
    + '?name=' + encodeURIComponent(p.player_name);

  return `
    <div class="window-card level-${p.level === 'Pro' ? 'pro' : p.level === 'Summer' ? 'summer' : 'ncaa'}">
      <div class="card-top">
        <a class="player-name" href="${detailUrl}">${esc(p.player_name)}</a>
        ${heartbeatHtml(p.player_name, isClient)}
        <span class="badge ${isClient ? 'badge-client' : 'badge-following'}">${isClient ? 'Client' : 'Recruit'}</span>
        <span class="badge ${levelBadgeClass(p.level)}">${levelBadgeLabel(p.level)}</span>
        ${p.current_level ? `<span class="badge" style="background:rgba(34,197,94,0.15);color:#4ade80" title="Current level (where he is right now)">${_summerEscape(p.current_level)}</span>` : ''}
        ${splits ? `<span class="badge" style="background:rgba(59,130,246,0.15);color:#5b9bf3" title="Played ${splits.length} levels this season — stats above are the ${_summerEscape(headline.level)} line">${_summerEscape(headline.level)} +${splits.length - 1}</span>` : ''}
        <span class="badge" style="background:rgba(107,114,128,0.15);color:#9ca3af">${gpLabel}</span>
      </div>
      <div class="team-name">${esc(p.team)}</div>
      <div class="window-grade ${gc}">${esc(gradeStr || '— No Data')}${splits ? ` · ${_summerEscape(headline.level)}` : ''}${momentumChipHtml(p)}</div>
      ${statsHtml}
      ${levelBreakdownHtml(splits, isPitcher, p.current_level)}
      ${gameLogHtml}
    </div>`;
}

function matchesWindowFilters(p) {
  if (searchQuery && !p.player_name.toLowerCase().includes(searchQuery)) return false;
  if (filters.roster === 'client' && p.is_client === false) return false;
  if (filters.roster === 'following' && p.is_client !== false) return false;
  if (filters.level !== 'all' && p.level !== filters.level) return false;
  if (filters.position !== 'all') {
    const pitcherPositions = ['Pitcher','LHP','RHP','LHR','RHR','SP','RP','CL'];
    const pos = p.tags?.position || '';
    if (filters.position === 'Pitcher' ? !pitcherPositions.includes(pos) : pitcherPositions.includes(pos)) return false;
  }
  if (filters.heartbeat !== 'all') {
    if (p.is_client === false) return false;
    const hb = heartbeatData.get(p.player_name.toLowerCase());
    if ((hb ? hb.status : 'gray') !== filters.heartbeat) return false;
  }
  return true;
}

function sortWindowPlayers(players) {
  return players.slice().sort((a, b) => {
    // Clients before following
    const aClient = a.is_client !== false ? 0 : 1;
    const bClient = b.is_client !== false ? 0 : 1;
    if (aClient !== bClient) return aClient - bClient;
    // Grade order (Hot > Solid > Quiet > Cold > Insufficient)
    const gradeOrder = ['Hot','Solid','Steady','Cold','Insufficient'];
    const aG = gradeOrder.indexOf(windowGradeKey(a.window_grade));
    const bG = gradeOrder.indexOf(windowGradeKey(b.window_grade));
    if (aG !== bG) return aG - bG;
    // Within same grade: sort by OPS (hitters) or ERA (pitchers) desc/asc
    const aOps = parseFloat(a.stats?.ops) || 0;
    const bOps = parseFloat(b.stats?.ops) || 0;
    const aEra = parseFloat(a.stats?.era);
    const bEra = parseFloat(b.stats?.era);
    // If both have ERA (pitchers), lower ERA first
    if (!isNaN(aEra) && !isNaN(bEra) && aEra !== bEra) return aEra - bEra;
    // Otherwise sort by OPS descending
    if (aOps !== bOps) return bOps - aOps;
    // Roster priority
    return (a.tags?.roster_priority || 99) - (b.tags?.roster_priority || 99);
  });
}

function _runFreshness() {
  // Returns { minutes, label, stale } based on dataGeneratedAt vs now.
  if (!dataGeneratedAt) return { minutes: null, label: '', stale: false };
  const minutes = Math.floor((Date.now() - new Date(dataGeneratedAt)) / 60000);
  const label = minutes < 1 ? 'just now'
    : minutes < 60 ? `${minutes}m ago`
    : minutes < 1440 ? `${Math.floor(minutes / 60)}h ago`
    : 'a long time ago';
  // Cron fires every 15 min during game hours; >25 min means at least one missed run.
  const stale = minutes !== null && minutes >= 25;
  return { minutes, label, stale };
}

function _renderTodayBanner(el) {
  const fresh = _runFreshness();
  const sev = runHealth && runHealth.severity;
  const blockedSrcs = (runHealth && runHealth.blocked_sources) || [];
  const blockedClients = (runHealth && runHealth.blocked_clients) || [];
  const carriedClients = (runHealth && runHealth.carry_forward_clients) || [];
  const fallbackClients = (runHealth && runHealth.fallback_clients) || [];

  // Stale run detected — overrides upstream severity since data isn't being refreshed.
  if (fresh.stale) {
    const mins = fresh.minutes;
    const cls = mins >= 60 ? 'critical' : 'warning';
    const icon = mins >= 60 ? '⛔' : '⏱️';
    const title = mins >= 60
      ? `No update in ${fresh.label} — pulse appears to be down`
      : `Last update was ${fresh.label} — expected one every 15 min`;
    el.style.display = 'block';
    el.className = `health-banner ${cls}`;
    el.innerHTML = `
      <div class="health-banner-icon">${icon}</div>
      <div class="health-banner-body">
        <div class="health-banner-title">${esc(title)}</div>
        <div class="health-banner-detail">Stats on the page may be outdated. <a href="diagnostics.html">Check source health →</a></div>
      </div>`;
    return;
  }

  // Active issue surfaced by main.py's run-health calculation.
  if (sev === 'warning' || sev === 'critical') {
    let title = '';
    let detailParts = [];
    if (blockedSrcs.length) {
      title = `${blockedSrcs.join(', ')} ${blockedSrcs.length === 1 ? 'is' : 'are'} blocked — live data may be incomplete`;
    } else if (sev === 'warning') {
      title = 'Several live games could not be located in any source';
    } else {
      title = 'Multiple sources failing — see status line on affected cards';
    }
    if (blockedClients.length) {
      detailParts.push(`${blockedClients.length} client${blockedClients.length === 1 ? '' : 's'} affected (${blockedClients.slice(0, 3).join(', ')}${blockedClients.length > 3 ? `, +${blockedClients.length - 3} more` : ''})`);
    }
    if (carriedClients.length) {
      detailParts.push(`${carriedClients.length} carried forward from earlier capture`);
    }
    if (fallbackClients.length && !blockedClients.length) {
      detailParts.push(`${fallbackClients.length} stuck on "not in lineup" / DNP`);
    }
    // Tell Kent what happens next, not just what's broken.
    if (blockedSrcs.length || blockedClients.length) {
      detailParts.push(`Auto-retry every 15 min during games; overnight backfill at ~3 AM ET catches anything still blocked`);
    }
    detailParts.push(`<a href="diagnostics.html">View source health history →</a>`);
    el.style.display = 'block';
    el.className = `health-banner ${sev}`;
    el.innerHTML = `
      <div class="health-banner-icon">${sev === 'critical' ? '⛔' : '⚠️'}</div>
      <div class="health-banner-body">
        <div class="health-banner-title">${esc(title)}</div>
        <div class="health-banner-detail">${detailParts.join(' · ')}</div>
      </div>`;
    return;
  }

  // Pre-game state — Today tab loaded but nobody has played yet.  Look at
  // the rendered list to find the earliest scheduled game so we can tell
  // Kent when the night actually starts.
  const todays = allPlayers.filter(p =>
    p.game_status && p.game_status !== 'N/A' && !p.is_yesterday
  );
  const live = todays.filter(p => p.game_status === 'Live');
  const final = todays.filter(p => p.game_status === 'Final');
  const scheduled = todays.filter(p => p.game_status === 'Scheduled');

  if (todays.length > 0 && live.length === 0 && final.length === 0 && scheduled.length > 0) {
    // Earliest scheduled game_time across clients — group by team so we
    // count distinct games rather than players.
    const games = new Map();
    for (const p of scheduled) {
      const key = p.team || p.player_name;
      if (!games.has(key)) games.set(key, p);
    }
    const earliest = scheduled
      .map(p => p.game_time || '')
      .filter(Boolean)
      .sort()[0] || '';
    const gameCount = games.size;
    el.style.display = 'block';
    el.className = 'health-banner info';
    el.innerHTML = `
      <div class="health-banner-icon">🌙</div>
      <div class="health-banner-body">
        <div class="health-banner-title">${gameCount} game${gameCount === 1 ? '' : 's'} on tonight${earliest ? ` — first pitch ${esc(earliest)}` : ''}</div>
        <div class="health-banner-detail">Stats will populate as games go live. Last refresh: ${esc(fresh.label)}.</div>
      </div>`;
    return;
  }

  // Healthy — compact green pill with absolute + relative timestamp.
  if (dataGeneratedAt) {
    const gen = new Date(dataGeneratedAt);
    const timeStr = gen.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit',
      timeZone: 'America/New_York', timeZoneName: 'short',
    });
    el.style.display = 'block';
    el.className = 'health-banner ok';
    el.innerHTML = `
      <div class="health-banner-icon">🟢</div>
      <div class="health-banner-body">
        <div class="health-banner-title">Pulse is healthy — last update ${esc(timeStr)} (${esc(fresh.label)})</div>
      </div>`;
    return;
  }

  el.style.display = 'none';
  el.innerHTML = '';
}

function _renderYesterdayBanner(el) {
  // Yesterday tab: surface stats_unavailable players that the overnight
  // repair pass couldn't recover.  Reassures Kent we're still trying.
  const data = (windowData && windowData.yesterday) || [];
  if (!Array.isArray(data) || !data.length) {
    el.style.display = 'none';
    el.innerHTML = '';
    return;
  }
  const stuck = data.filter(p => p.stats_unavailable);
  if (!stuck.length) {
    el.style.display = 'none';
    el.innerHTML = '';
    return;
  }
  const names = stuck.slice(0, 3).map(p => p.player_name).join(', ');
  const more = stuck.length > 3 ? `, +${stuck.length - 3} more` : '';
  el.style.display = 'block';
  el.className = 'health-banner recovery';
  el.innerHTML = `
    <div class="health-banner-icon">🔄</div>
    <div class="health-banner-body">
      <div class="health-banner-title">${stuck.length} player${stuck.length === 1 ? '' : 's'} couldn't capture last night — still retrying</div>
      <div class="health-banner-detail">${esc(names)}${esc(more)} · We re-check every 15 min until the box score lands.</div>
    </div>`;
}

function renderHealthBanner() {
  const el = document.getElementById('healthBanner');
  if (!el) return;
  if (currentWindow === 'today') {
    _renderTodayBanner(el);
  } else if (currentWindow === 'yesterday') {
    _renderYesterdayBanner(el);
  } else {
    el.style.display = 'none';
    el.innerHTML = '';
  }
}

// "Live now" header pill — count of players currently in a live game (Today
// view only). Click filters the grid to Live.
function updateLivePill() {
  const el = document.getElementById('livePill');
  if (!el) return;
  const n = currentWindow === 'today'
    ? (allPlayers || []).filter(p => p.game_status === 'Live').length
    : 0;
  if (!n) { el.style.display = 'none'; return; }
  el.textContent = `🔴 ${n} playing now`;
  el.style.display = 'inline-flex';
}

function render() {
  const grid = document.getElementById('grid');
  const empty = document.getElementById('empty');
  const noGameSection = document.getElementById('noGameSection');
  renderHealthBanner();
  updateLivePill();
  // Change strip only lives on the Today view.
  const _cs = document.getElementById('changeStrip');
  if (_cs) _cs.hidden = currentWindow !== 'today' || !_cs.innerHTML;

  if (currentWindow === 'today') {
    const allFiltered = allPlayers.filter(matchesFilters);

    // Split into active (have a game) and no-game players
    const hasStatusFilter = filters.status !== 'all';
    const activePlayers = allFiltered.filter(p => p.game_status !== 'N/A');
    const noGamePlayers = hasStatusFilter ? [] : allFiltered.filter(p => p.game_status === 'N/A');

    const sorted = sortPlayers(activePlayers);

    if (sorted.length === 0 && noGamePlayers.length === 0) {
      grid.innerHTML = '';
      empty.style.display = 'block';
      // Before the first successful fetch, allPlayers is [] and filters
      // would naturally produce zero matches — that's loading state, not an
      // empty-result state. Keep the splash language so it's clear DP is
      // working, not broken.
      empty.textContent = dataLoaded
        ? 'No players match the current filters.'
        : 'Loading player data…';
      noGameSection.style.display = 'none';
    } else {
      empty.style.display = 'none';
      // Group cards by game_status. MLB API and our scrapers emit values
      // beyond the obvious four (Live/Final/Scheduled/Cancelled) — Delayed,
      // Suspended, Warmup, Forfeit, etc. Match each known status into the
      // best bucket, then dump anything unrecognized into "Other" so we
      // never silently drop a player. (Past bug: Carter Johnson hidden on
      // 2026-05-19 because his game went "Delayed" mid-game and the old
      // strict equality match had no bucket for it.)
      const groups = [
        { label: 'In Progress', match: s => s === 'Live' || s === 'Delayed' || s === 'Suspended' || s === 'Warmup' || s === 'Pre-Game' || s === 'Manager Challenge' },
        { label: 'Final', match: s => s === 'Final' || s === 'Forfeit' || s === 'Completed Early' },
        { label: 'Scheduled', match: s => s === 'Scheduled' },
        { label: 'Postponed', match: s => s === 'Cancelled' || s === 'Postponed' },
        // Summer-ball-specific placement states (Pending/Injured/Shut Down
        // etc.). These aren't game-day statuses — they're roster flags from
        // Kent's placement spreadsheet. Group them under a friendly label.
        { label: 'Awaiting / Inactive', match: s => [
            'Confirmed', '2nd Half', 'Pending, 1st Half', 'Pending, 2nd Half',
            'Injured', 'Shut Down', 'Off Day', 'Roster Confirmed', 'Status Update',
            'Season Stats',
          ].indexOf(s) !== -1 },
      ];
      const placed = new Set();
      let html = '';
      for (const g of groups) {
        const section = sorted.filter(p => g.match(p.game_status || ''));
        if (section.length === 0) continue;
        section.forEach(p => placed.add(p));
        html += `<div class="status-section-header">${g.label}</div>`;
        html += section.map(renderCard).join('');
      }
      const leftover = sorted.filter(p => !placed.has(p));
      if (leftover.length > 0) {
        const unknownStatuses = [...new Set(leftover.map(p => p.game_status || '(blank)'))].join(', ');
        console.warn('Today render: catch-all bucket caught statuses:', unknownStatuses);
        html += `<div class="status-section-header">Other</div>`;
        html += leftover.map(renderCard).join('');
      }
      grid.innerHTML = html;
    }

    // Render collapsed no-game section
    if (noGamePlayers.length > 0) {
      noGameSection.style.display = 'block';
      document.getElementById('noGameLabel').textContent = `No game today (${noGamePlayers.length})`;
      const sortedNoGame = noGamePlayers.slice().sort((a, b) => {
        const ac = a.is_client !== false ? 0 : 1;
        const bc = b.is_client !== false ? 0 : 1;
        if (ac !== bc) return ac - bc;
        return (a.tags.roster_priority || 99) - (b.tags.roster_priority || 99);
      });
      document.getElementById('noGameList').innerHTML = sortedNoGame.map(p => {
        const nextText = p.next_game ? p.next_game.display : 'No game scheduled';
        const isClient = p.is_client !== false;
        return `<div class="no-game-row">
          <span class="no-game-name">${esc(p.player_name)}</span>
          ${heartbeatHtml(p.player_name, isClient)}
          <span class="no-game-team">${esc(p.team)}</span>
          <span class="no-game-next">${esc(nextText)}</span>
        </div>`;
      }).join('');
    } else {
      noGameSection.style.display = 'none';
    }

    manageAutoRefresh();
  } else if (currentWindow === 'yesterday') {
    noGameSection.style.display = 'none';
    const data = windowData.yesterday || [];
    // Only show players whose team actually played — exclude any stale or wrong-date entries
    const filtered = sortPlayers(data.filter(p => p.game_status === 'Final' && matchesFilters(p)));

    if (windowData.yesterday === null) {
      empty.style.display = 'none';
      grid.innerHTML = Array(6).fill(
        `<div class="skeleton-card">
          <div class="skeleton-bar skeleton-name"></div>
          <div class="skeleton-bar skeleton-team"></div>
          <div class="skeleton-bar skeleton-stat"></div>
          <div class="skeleton-bar skeleton-stat-short"></div>
          <div class="skeleton-bar skeleton-stat-wide"></div>
        </div>`
      ).join('');
    } else if (filtered.length === 0) {
      grid.innerHTML = '';
      empty.style.display = 'block';
      if (_windowFetchFailed.yesterday) {
        empty.textContent = 'Couldn\u2019t load yesterday data \u2014 check your connection and pull to refresh.';
      } else if (data.length > 0) {
        empty.textContent = 'No matches \u2014 try clearing your filters.';
      } else {
        empty.textContent = 'No games yesterday.';
      }
    } else {
      empty.style.display = 'none';
      grid.innerHTML = filtered.map(renderCard).join('');
    }
  } else {
    noGameSection.style.display = 'none';
    // Window view - use window card renderer
    if (windowData[currentWindow] === null) {
      // Show skeleton loading cards
      empty.style.display = 'none';
      grid.innerHTML = Array(6).fill(
        `<div class="skeleton-card">
          <div class="skeleton-bar skeleton-name"></div>
          <div class="skeleton-bar skeleton-team"></div>
          <div class="skeleton-bar skeleton-stat"></div>
          <div class="skeleton-bar skeleton-stat-short"></div>
          <div class="skeleton-bar skeleton-stat-wide"></div>
        </div>`
      ).join('');
    } else {
      const data = windowData[currentWindow] || [];
      const filtered = sortWindowPlayers(data.filter(matchesWindowFilters));
      if (filtered.length === 0) {
        grid.innerHTML = '';
        empty.style.display = 'block';
        if (_windowFetchFailed[currentWindow]) {
          empty.textContent = 'Couldn\u2019t load data \u2014 check your connection and pull to refresh.';
        } else if (data.length > 0) {
          empty.textContent = 'No matches \u2014 try clearing your filters.';
        } else {
          empty.textContent = 'No data available for this window.';
        }
      } else {
        empty.style.display = 'none';
        grid.innerHTML = filtered.map(renderWindowCard).join('');
      }
    }
  }
  updateGradeFilterCounts();
}

function updateGradeFilterCounts() {
  // Only show counts for today/yesterday views
  if (currentWindow !== 'today' && currentWindow !== 'yesterday') return;
  const players = currentWindow === 'today' ? allPlayers : (windowData.yesterday || []);
  // Count grades across all players (ignoring other filters for distribution view)
  const counts = {};
  const gradeValues = ['Milestone', 'Standout', 'Good', 'Routine', 'Off Day', 'Scheduled'];
  gradeValues.forEach(g => counts[g] = 0);
  for (const p of players) {
    const gk = gradeKey(p.performance_grade || '');
    if (counts[gk] !== undefined) counts[gk]++;
  }
  const gradeGroup = document.querySelector('[data-filter="grade"]');
  if (!gradeGroup) return;
  const gradeEmojis = { 'Milestone': '\u{1f48e}', 'Standout': '\u{1f525}', 'Good': '\u2705', 'Routine': '\u{1f610}', 'Off Day': '\u{1f6a9}', 'Scheduled': '\u{1f552}' };
  gradeGroup.querySelectorAll('.filter-btn').forEach(btn => {
    const val = btn.dataset.value;
    if (val === 'all') return;
    const count = counts[val] || 0;
    const emoji = gradeEmojis[val] || '';
    btn.textContent = count > 0 ? `${emoji} ${count}` : emoji;
  });
}

function toggleFiltersForWindow(window) {
  // Show status/grade filters for today and yesterday views
  const statusFilter = document.querySelector('[data-filter="status"]');
  const gradeFilter = document.querySelector('[data-filter="grade"]');

  if (window === 'today' || window === 'yesterday') {
    statusFilter.style.display = 'flex';
    gradeFilter.style.display = 'flex';
  } else {
    statusFilter.style.display = 'none';
    gradeFilter.style.display = 'none';
    // Reset these filters when hidden
    filters.status = 'all';
    filters.grade = 'all';
  }
}

function _setFilterButton(key, value) {
  const grp = document.querySelector(`.filter-group[data-filter="${key}"]`);
  if (grp) grp.querySelectorAll('.filter-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.value === value));
}

// Rolling windows (7/14/30D) are only meaningful mid-season for Pro (NCAA/HS are
// out of season → empty cells). Auto-focus Pro on those tabs, and cleanly revert
// to All elsewhere — but never override a level the user picked themselves.
let _autoLevelPro = false;
function applyLevelDefaultForWindow(window) {
  const rolling = window === '7d' || window === '14d' || window === '30d';
  if (rolling && filters.level === 'all') {
    filters.level = 'Pro';
    _autoLevelPro = true;
    _setFilterButton('level', 'Pro');
  } else if (!rolling && _autoLevelPro && filters.level === 'Pro') {
    filters.level = 'all';
    _autoLevelPro = false;
    _setFilterButton('level', 'all');
  }
}

async function switchWindow(window) {
  if (window === currentWindow) return;

  // Save scroll position for current tab
  _scrollPositions[currentWindow] = globalThis.scrollY || 0;

  // Update active tab
  document.querySelectorAll('.time-tab').forEach(t => {
    t.classList.remove('active');
    t.setAttribute('aria-selected', 'false');
  });
  const activeTab = document.querySelector(`[data-window="${window}"]`);
  activeTab.classList.add('active');
  activeTab.setAttribute('aria-selected', 'true');
  currentWindow = window;

  // Toggle filter visibility
  toggleFiltersForWindow(window);
  applyLevelDefaultForWindow(window);
  updateTimestamp();

  // Load data if not cached
  if (window !== 'today' && windowData[window] === null) {
    document.getElementById('grid').innerHTML =
      '<div class="loading-splash"><div class="loading-spinner"></div><div class="loading-text">Loading...</div></div>';
    _windowFetchFailed[window] = false;
    try {
      const resp = await fetch(WINDOW_PATHS[window] + '?t=' + Date.now(), { cache: 'no-store' });
      if (resp.ok) {
        const raw = await resp.json();
        // yesterday_pulse.json uses the same envelope as current_pulse
        if (window === 'yesterday') {
          windowData[window] = _dedupPlayers(raw.players || []);
          yesterdaySourceDate = raw.source_date || null;
        } else {
          windowData[window] = Array.isArray(raw) ? _dedupPlayers(raw) : raw;
        }
      } else {
        windowData[window] = [];
        _windowFetchFailed[window] = true;
      }
    } catch (e) {
      console.error('Failed to load window data:', e);
      windowData[window] = [];
      _windowFetchFailed[window] = true;
    }
  }

  updateTimestamp();
  render();

  // Restore scroll position for the new tab
  requestAnimationFrame(() => globalThis.scrollTo(0, _scrollPositions[window] || 0));
}

// Pulse ingests data every 15 minutes around the clock — the GitHub Actions
// cron covers 10 AM-2:45 AM ET, and an external scheduler fills the morning
// gap (2:45-10 AM ET) with 15-min workflow_dispatch runs. So the next update
// is always the next :00, :15, :30, or :45 on the wall clock.
function getNextUpdateTime() {
  const now = new Date();
  const h = now.getUTCHours(), m = now.getUTCMinutes();
  const nextM = Math.ceil((m + 1) / 15) * 15;
  const next = new Date(now);
  if (nextM >= 60) {
    next.setUTCHours(h + 1, 0, 0, 0);
  } else {
    next.setUTCMinutes(nextM, 0, 0);
  }
  return next;
}

function updateNextUpdate() {
  const el = document.getElementById('nextUpdate');
  if (!el) return;
  const next = getNextUpdateTime();
  const timeStr = next.toLocaleTimeString('en-US', {
    hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York', timeZoneName: 'short'
  });
  el.textContent = 'Next update: ' + timeStr;
}

function _getExpectedYesterday() {
  // Mirror the backend 4 AM ET day-flip: before 4 AM ET, "today" is still the prior calendar day
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  if (et.getHours() < 4) et.setDate(et.getDate() - 1);
  const yesterday = new Date(et);
  yesterday.setDate(yesterday.getDate() - 1);
  return yesterday.toISOString().slice(0, 10);
}

function updateStaleBanner() {
  const banner = document.getElementById('staleBanner');
  if (!banner) return;
  if (currentWindow !== 'yesterday' || !yesterdaySourceDate) {
    banner.style.display = 'none';
    return;
  }
  const expected = _getExpectedYesterday();
  if (yesterdaySourceDate !== expected) {
    banner.textContent = 'Yesterday data may be outdated — results are from ' +
      new Date(yesterdaySourceDate + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }) +
      ', not the most recent game day.';
    banner.style.display = 'block';
  } else {
    banner.style.display = 'none';
  }
}

function updateTimestamp() {
  const el = document.getElementById('updated');
  updateStaleBanner();
  // Yesterday tab: show source date instead of stale relative time
  if (currentWindow === 'yesterday') {
    if (yesterdaySourceDate) {
      const d = new Date(yesterdaySourceDate + 'T12:00:00');
      el.textContent = 'Results from ' + d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
    } else {
      el.textContent = '';
    }
    return;
  }
  // Window tabs (7d/season): show last_updated from window data
  if (currentWindow !== 'today') {
    const data = windowData[currentWindow];
    if (Array.isArray(data) && data.length > 0 && data[0].last_updated) {
      const d = new Date(data[0].last_updated);
      const timeStr = d.toLocaleDateString('en-US', {
        month: 'short', day: 'numeric', timeZone: 'America/New_York'
      }) + ', ' + d.toLocaleTimeString('en-US', {
        hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York', timeZoneName: 'short'
      });
      el.textContent = 'Updated ' + timeStr;
    } else {
      el.textContent = '';
    }
    return;
  }
  // Today tab: show absolute last update time
  if (!dataGeneratedAt) {
    el.textContent = '';
    return;
  }
  const gen = new Date(dataGeneratedAt);
  const timeStr = gen.toLocaleTimeString('en-US', {
    hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York', timeZoneName: 'short'
  });
  const now = new Date();
  const diffMin = Math.floor((now - gen) / 60000);
  const ago = diffMin < 1 ? 'just now'
    : diffMin < 60 ? `${diffMin}m ago`
    : diffMin < 1440 ? `${Math.floor(diffMin / 60)}h ago`
    : '';
  el.textContent = `Last update: ${timeStr}${ago ? ` (${ago})` : ''}`;
}

function _isGameHours() {
  // We used to gate auto-refresh to the cron's "live" window (10 AM-2:45 AM ET)
  // and stop polling during the 2:45-10:00 AM gap. But fresh data does arrive
  // during the gap (early-morning college games, overnight backfill, manual
  // dispatches), and we burned a user when stats landed at 9:17 AM but the
  // dashboard was still showing the 9:02 AM snapshot because auto-refresh was
  // sleeping. Always poll on the Today tab — cost is one HTTP GET every 5
  // min, negligible.
  return true;
}

function manageAutoRefresh() {
  const indicator = document.getElementById('autoRefreshIndicator');
  const shouldRefresh = currentWindow === 'today' && _isGameHours();
  if (shouldRefresh) {
    if (!autoRefreshTimer) {
      autoRefreshTimer = setInterval(async () => {
        const [raw] = await Promise.all([
          _fetchWithRetry('data/current_pulse.json?t=' + Date.now()),
          fetchHeartbeat()
        ]);
        if (raw) {
          if (Array.isArray(raw)) {
            allPlayers = _dedupPlayers(raw);
      dataLoaded = true;
            dataGeneratedAt = null;
            runHealth = null;
          } else {
            allPlayers = _dedupPlayers(raw.players || []);
      dataLoaded = true;
            dataGeneratedAt = raw.generated_at || null;
            runHealth = raw.health || null;
          }
          updateTimestamp();
          render();
          indicator.querySelector('span').textContent = 'Auto-refreshing';
        } else {
          indicator.querySelector('span').textContent = 'Update failed — retrying';
          // Retry sooner on failure instead of waiting full 5 min
          clearInterval(autoRefreshTimer);
          autoRefreshTimer = null;
          setTimeout(() => setupAutoRefresh(), 15000);
        }
      }, 300000);
    }
    indicator.classList.add('active');
  } else {
    if (autoRefreshTimer) {
      clearInterval(autoRefreshTimer);
      autoRefreshTimer = null;
    }
    indicator.classList.remove('active');
  }
}

function toggleNoGame() {
  const header = document.getElementById('noGameHeader');
  const list = document.getElementById('noGameList');
  header.classList.toggle('open');
  list.classList.toggle('open');
  header.setAttribute('aria-expanded', header.classList.contains('open'));
}

function toggleGameLog(id) {
  const toggle = document.getElementById('glt-' + id);
  const entries = document.getElementById('gle-' + id);
  if (toggle) {
    toggle.classList.toggle('open');
    toggle.setAttribute('aria-expanded', toggle.classList.contains('open'));
  }
  if (entries) entries.classList.toggle('open');
}

// Filter buttons
document.querySelectorAll('.filter-group').forEach(group => {
  const key = group.dataset.filter;
  group.querySelectorAll('.filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      group.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      filters[key] = btn.dataset.value;
      // User picked a level explicitly — stop auto-managing it across tabs.
      if (key === 'level') _autoLevelPro = false;
      _persistFilters();
      updateClearFiltersBtn();
      render();
    });
  });
});

// Sync button active states with any filters restored from localStorage.
// Every group syncs (including back to "all") so buttons never lie about state.
for (const [k, v] of Object.entries(filters)) _setFilterButton(k, v);
updateClearFiltersBtn();

// Clicking the live pill jumps to the Live status filter and scrolls up.
document.getElementById('livePill').addEventListener('click', () => {
  filters.status = 'Live';
  const grp = document.querySelector('.filter-group[data-filter="status"]');
  if (grp) grp.querySelectorAll('.filter-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.value === 'Live'));
  updateClearFiltersBtn();
  render();
  window.scrollTo({ top: 0, behavior: 'smooth' });
});

function updateClearFiltersBtn() {
  const btn = document.getElementById('clearFiltersBtn');
  const hasActive = Object.values(filters).some(v => v !== 'all') || searchQuery;
  btn.style.display = hasActive ? 'inline-block' : 'none';
}

document.getElementById('clearFiltersBtn').addEventListener('click', () => {
  for (const key of Object.keys(filters)) filters[key] = 'all';
  _autoLevelPro = false;
  document.querySelectorAll('.filter-group').forEach(group => {
    group.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    const allBtn = group.querySelector('[data-value="all"]');
    if (allBtn) allBtn.classList.add('active');
  });
  document.getElementById('searchInput').value = '';
  searchQuery = '';
  _persistFilters();
  updateClearFiltersBtn();
  render();
});

// Time window tabs
document.querySelectorAll('.time-tab').forEach(tab => {
  tab.addEventListener('click', () => switchWindow(tab.dataset.window));
});

// Search input (debounced to avoid re-rendering on every keystroke)
let _searchTimer = null;
document.getElementById('searchInput').addEventListener('input', e => {
  clearTimeout(_searchTimer);
  _searchTimer = setTimeout(() => {
    searchQuery = e.target.value.toLowerCase().trim();
    updateClearFiltersBtn();
    render();
  }, 350);
});

// Relative timestamp refresh
setInterval(updateTimestamp, 10000);

// Swipe gestures + pull-to-refresh
const TAB_ORDER = ['today', 'yesterday', '7d', 'season'];
let touchStartX = 0, touchStartY = 0, touchTracking = false;

document.addEventListener('touchstart', e => {
  if (e.target.closest('input, button, a, select')) { touchTracking = false; return; }
  touchTracking = true;
  touchStartX = e.touches[0].clientX;
  touchStartY = e.touches[0].clientY;
}, { passive: true });

document.addEventListener('touchend', e => {
  if (!touchTracking) return;
  touchTracking = false;
  const dx = e.changedTouches[0].clientX - touchStartX;
  const dy = e.changedTouches[0].clientY - touchStartY;
  const absDx = Math.abs(dx);
  const absDy = Math.abs(dy);
  if (absDx < 30 && absDy < 30) return;
  // Horizontal swipe → switch tabs
  if (absDx > 50 && absDx > absDy * 1.5) {
    const idx = TAB_ORDER.indexOf(currentWindow);
    if (dx < 0 && idx < TAB_ORDER.length - 1) switchWindow(TAB_ORDER[idx + 1]);
    else if (dx > 0 && idx > 0) switchWindow(TAB_ORDER[idx - 1]);
    return;
  }
  // Pull down at top → refresh
  if (dy > 80 && absDy > absDx * 2 && window.scrollY <= 0) {
    refreshData();
  }
}, { passive: true });

async function _fetchWithRetry(url, retries = 1, delayMs = 3000) {
  for (let i = 0; i <= retries; i++) {
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (resp.ok) return await resp.json();
    } catch (e) { /* retry */ }
    if (i < retries) await new Promise(r => setTimeout(r, delayMs));
  }
  return null;
}

async function refreshData() {
  const indicator = document.getElementById('pullRefresh');
  indicator.classList.add('active');
  indicator.textContent = 'Refreshing...';
  indicator.style.color = '';
  const path = WINDOW_PATHS[currentWindow];
  const [raw] = await Promise.all([
    _fetchWithRetry(path + '?t=' + Date.now()),
    fetchHeartbeat()
  ]);
  if (raw) {
    if (currentWindow === 'today') {
      allPlayers = _dedupPlayers(Array.isArray(raw) ? raw : (raw.players || []));
      dataLoaded = true;
      dataGeneratedAt = Array.isArray(raw) ? null : (raw.generated_at || null);
      runHealth = Array.isArray(raw) ? null : (raw.health || null);
      updateTimestamp();
    } else if (currentWindow === 'yesterday') {
      windowData.yesterday = _dedupPlayers(raw.players || []);
      yesterdaySourceDate = raw.source_date || null;
      updateTimestamp();
    } else {
      windowData[currentWindow] = Array.isArray(raw) ? _dedupPlayers(raw) : raw;
    }
    render();
    setTimeout(() => indicator.classList.remove('active'), 500);
  } else {
    indicator.textContent = 'Refresh failed — check connection';
    indicator.style.color = '#ef4444';
    setTimeout(() => { indicator.classList.remove('active'); indicator.textContent = 'Refreshing...'; indicator.style.color = ''; }, 3000);
  }
}

// Fetch Heartbeat summary (fire-and-forget, graceful degradation)
async function fetchHeartbeat() {
  try {
    const resp = await fetch('https://sv-heartbeat.vercel.app/api/heartbeat/summary');
    if (!resp.ok) return;
    const data = await resp.json();
    const entries = Array.isArray(data) ? data : (data.players || data.summary || []);
    for (const p of entries) {
      const name = (p.name || p.player_name || '').toLowerCase();
      if (name) heartbeatData.set(name, {
        status: (p.status || 'gray').toLowerCase(),
        loveScore: p.loveScore ?? p.love_score ?? null,
        daysSinceContact: p.daysSinceLeadContact ?? p.daysSinceContact ?? p.days_since_contact ?? null
      });
    }
  } catch (e) {
    // Heartbeat unavailable — hearts won't render, that's fine
  }
}

// Summer ball coverage banner — placement-driven (see loadSummerBanner).

function _summerEscape(s) {
  return String(s ?? '').replace(/[&<>"]/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'
  })[c]);
}

// Globals populated by loadSummerBanner — used by renderCard to surface
// per-card "Awaiting BBRef" / "cache stale" hints on Summer placements.
let bbrefStats = null;
let bbrefCacheHours = null;

// Per-player last-5 game classifications, populated by loadSummerGameLog.
// Map: player_name -> ['good','ok','bad','dnp','good']  (oldest → newest)
let summerSparklines = {};

function _classifyGame(summary) {
  // Returns 'good' | 'ok' | 'bad' | 'dnp' based on a one-line stat string.
  if (!summary) return 'dnp';
  const s = summary.toLowerCase();
  if (s.includes('did not appear') || s.includes('no game')) return 'dnp';
  // Pitcher line, e.g. "3.0 IP, 0 ER, 4 K"
  const ipMatch = s.match(/([\d.]+)\s*ip/);
  if (ipMatch) {
    const ip = parseFloat(ipMatch[1]);
    const er = (s.match(/(\d+)\s*er/) || [])[1];
    const k = (s.match(/(\d+)\s*k\b/) || [])[1];
    const erN = er === undefined ? null : parseInt(er, 10);
    const kN = k === undefined ? 0 : parseInt(k, 10);
    if (erN === 0 && ip >= 2) return 'good';
    if (erN !== null && erN >= 3) return 'bad';
    if (kN >= 4) return 'good';
    return 'ok';
  }
  // Hitter line, e.g. "2-4, HR, 2 RBI" or "0-3, K"
  const ab = s.match(/(\d+)-(\d+)/);
  if (ab) {
    const h = parseInt(ab[1], 10);
    const at = parseInt(ab[2], 10);
    const hr = (s.match(/(\d+)?\s*hr\b/) || [])[1];
    if (h >= 2 || hr) return 'good';
    if (at >= 3 && h === 0) return 'bad';
    if (h >= 1) return 'ok';
    return 'bad';
  }
  return 'dnp';
}

async function loadSummerGameLog() {
  try {
    const resp = await fetch('data/summer_game_log.json?t=' + Date.now());
    if (!resp.ok) return;
    const log = await resp.json();
    const dates = Object.keys(log).sort();  // oldest → newest
    // Walk newest → oldest building per-player up to 5 most recent entries.
    const out = {};
    for (let i = dates.length - 1; i >= 0; i--) {
      const entries = log[dates[i]] || [];
      for (const e of entries) {
        const name = e.player_name;
        if (!name) continue;
        if (!out[name]) out[name] = [];
        if (out[name].length >= 5) continue;
        out[name].push(_classifyGame(e.stats_summary || ''));
      }
    }
    // Reverse each player's array so oldest is first (left → right).
    for (const k of Object.keys(out)) out[k] = out[k].reverse();
    summerSparklines = out;
  } catch (e) { /* non-fatal */ }
}

function summerSparklineHtml(p) {
  if (p.level !== 'Summer') return '';
  const seq = summerSparklines[p.player_name];
  if (!seq || !seq.length) return '';
  const dotColor = c => c === 'good' ? '#1a7a30'
                     : c === 'ok'   ? '#b08900'
                     : c === 'bad'  ? '#cf222e'
                     :                '#7d8590';
  const dots = seq.map(c =>
    `<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${dotColor(c)};margin-right:3px;"></span>`
  ).join('');
  return `<div style="margin-top:6px;font-size:10px;color:#7d8590;display:flex;align-items:center;gap:6px;" title="Last ${seq.length} games (oldest → newest): green = quality outing, yellow = OK, red = poor, grey = DNP">
    <span>L${seq.length}</span><span>${dots}</span>
  </div>`;
}

async function loadSummerBanner() {
  const el = document.getElementById('summerBanner');
  if (!el) return;
  // Placement-driven view. Source of truth = Kent's spreadsheet, imported to
  // summer_ball_placements.json. Live stats only come from the four leagues on
  // MLB Stats API / PrestoSports; everything else is tracked by hand. (The old
  // auto-matcher snapshot in summer_ball_rosters.json is now a QA-only feed used
  // by the Monday email + regression alert, not this banner.)
  let placements, pulse;
  try {
    const [pRes, cRes] = await Promise.all([
      fetch('data/summer_ball_placements.json?t=' + Date.now()),
      fetch('data/current_pulse.json?t=' + Date.now())
    ]);
    pulse = cRes.ok ? await cRes.json() : {};
    // Summer ball ends in early August. Once the pipeline reports the season
    // over there are no summer cards left in the pulse, so the banner and the
    // level filter would both point at nothing. Both return on their own when
    // next summer's games start. Absent flag = older pulse, so keep showing.
    if (pulse && pulse.summer_season_active === false) {
      const summerFilterBtn = document.querySelector('.filter-btn-summer');
      if (summerFilterBtn) summerFilterBtn.hidden = true;
      el.hidden = true;
      return;
    }
    if (!pRes.ok) return;
    placements = await pRes.json();
  } catch (e) { return; }
  // Pull BBRef cache freshness in parallel — used for per-card hints +
  // the top-of-banner refresh pill.
  try {
    const bb = await fetch('data/bbref_stats.json?t=' + Date.now());
    if (bb.ok) {
      bbrefStats = await bb.json();
      const ts = bbrefStats && bbrefStats.generated_at_utc;
      if (ts) {
        bbrefCacheHours = (Date.now() - new Date(ts).getTime()) / 3600000;
        const freshEl = document.getElementById('bbrefFreshness');
        if (freshEl) {
          const hrs = bbrefCacheHours;
          const ago = hrs < 1 ? `${Math.round(hrs * 60)}m` : hrs < 24 ? `${Math.round(hrs)}h` : `${Math.round(hrs/24)}d`;
          // Drop the explicit BBRef pill — research confirmed BBRef is a
          // post-season archive, not a mid-season fallback. Leaving the
          // element empty rather than misleading anyone.
          freshEl.innerHTML = '';
        }
      }
    }
  } catch (e) { /* non-fatal */ }
  // Leagues we pull live game data from automatically. Everything else is
  // tracked by hand — no automated stats will ever appear for those placements.
  const REACHABLE = new Set(['Cape Cod', 'MLB Draft', 'Appalachian', 'NECBL']);

  const rows = (Array.isArray(placements) ? placements : (placements.placements || []))
    .filter(r => r && r.player_name && r.player_name !== 'NEED PLACEMENT');
  if (!rows.length) { el.hidden = true; return; }

  // Index today's summer pulse entries by player so we can tell who has a game.
  const pulsePlayers = (pulse && pulse.players) || (Array.isArray(pulse) ? pulse : []);
  const byName = {};
  for (const e of pulsePlayers) {
    if (e.level !== 'Summer' || !e.player_name) continue;
    (byName[e.player_name] = byName[e.player_name] || []).push(e);
  }
  const hasGame = n => (byName[n] || []).some(e =>
    ['Scheduled', 'Live', 'In Progress', 'Final'].includes(e.game_status));
  const isLiveNow = n => (byName[n] || []).some(e =>
    ['Live', 'In Progress'].includes(e.game_status));

  // Bucket every real placement.
  const live = [], idle = [], manual = [], out = [];
  const leagueCounts = {};   // league -> { count, reachable } — active only
  for (const r of rows) {
    const st = r.status || '';
    const reachable = REACHABLE.has(r.league);
    if (st === 'Shut Down' || st === 'Injured') { out.push(r); continue; }
    // Chips reflect active placements by league (blank leagues get no chip).
    if (r.league) {
      (leagueCounts[r.league] = leagueCounts[r.league] || { count: 0, reachable }).count++;
    }
    if (!reachable) { manual.push(r); continue; }
    if (hasGame(r.player_name)) live.push(r); else idle.push(r);
  }
  const playingNow = rows.filter(r => isLiveNow(r.player_name)).length;
  const trackedLive = live.length + idle.length;
  const total = rows.length;

  // Chips: one per league with an SV placement. Reachable = green; manual
  // (no automated feed) = muted with a "manual" tag. No zero-placement noise.
  const chips = Object.entries(leagueCounts)
    .sort((a, b) => b[1].count - a[1].count)
    .map(([lg, info]) => info.reachable
      ? `<span class="summer-chip summer-chip-ok">${_summerEscape(lg)} · ${info.count}</span>`
      : `<span class="summer-chip summer-chip-stub" title="No automated stats feed — tracked by hand">${_summerEscape(lg)} · ${info.count} · manual</span>`
    ).join('');

  const bySort = (a, b) => a.player_name.localeCompare(b.player_name);
  const _li = (r, note) => {
    const sch = r.school ? ` <span class="college">(${_summerEscape(r.school)})</span>` : '';
    const team = r.summer_team
      ? ` <span class="meta">(${_summerEscape(r.summer_team)}${r.league ? ', ' + _summerEscape(r.league) : ''})</span>`
      : '';
    const n = note ? ` <span class="meta">— ${_summerEscape(note)}</span>` : '';
    return `<li><strong>${_summerEscape(r.player_name)}</strong>${sch}${team}${n}</li>`;
  };

  const detailParts = [];
  if (manual.length) {
    detailParts.push(
      `<div class="summer-section-title">Tracked by hand — no automated feed</div>`
      + `<div class="meta" style="margin:4px 0;">These leagues have no public stats feed we can pull automatically. Check the team/league site directly.</div>`
      + `<ul class="summer-list">${manual.slice().sort(bySort).map(r => _li(r)).join('')}</ul>`
    );
  }
  if (idle.length) {
    detailParts.push(
      `<div class="summer-section-title">No game in the last day</div>`
      + `<ul class="summer-list">${idle.slice().sort(bySort).map(r => _li(r)).join('')}</ul>`
    );
  }
  if (out.length) {
    detailParts.push(
      `<div class="summer-section-title">Out — injured / shut down</div>`
      + `<ul class="summer-list">${out.slice().sort(bySort).map(r => _li(r, r.status)).join('')}</ul>`
    );
  }

  const nowBit = playingNow ? ` · <strong style="color:#1a7a30;">${playingNow}</strong> playing now` : '';
  const idleBit = idle.length ? ` · <strong>${idle.length}</strong> idle today` : '';

  el.innerHTML = `
    <div class="summer-head" onclick="document.getElementById('summerBanner').classList.toggle('expanded')">
      <div>
        <div class="summer-title">Summer Ball · ${total} placements</div>
        <div class="summer-stat">
          <strong>${trackedLive}</strong> tracked live${nowBit}${idleBit}
          · <strong>${manual.length}</strong> tracked by hand${out.length ? ` · <strong>${out.length}</strong> out` : ''}
        </div>
      </div>
      <span class="summer-toggle">details</span>
    </div>
    <div class="summer-chips">${chips}</div>
    <div class="summer-detail">${detailParts.join('')}</div>
  `;
  el.hidden = false;
}
loadSummerBanner();
// Load summer game log + re-render once dots are ready (fire-and-forget,
// non-blocking — cards render without sparkline first, then refresh).
loadSummerGameLog().then(() => {
  if (typeof render === 'function' && dataLoaded) render();
});
// Recent-form momentum (Pro 7D-vs-30D) — fire-and-forget; re-renders when ready.
loadMomentum();

// Load data — supports both envelope {generated_at, players} and legacy array
(async () => {
  // Fetch pulse data and heartbeat in parallel
  const [raw] = await Promise.all([
    _fetchWithRetry('data/current_pulse.json?t=' + Date.now(), 2, 3000),
    fetchHeartbeat()
  ]);
  if (raw) {
    if (Array.isArray(raw)) {
      allPlayers = _dedupPlayers(raw);
      dataLoaded = true;
      dataGeneratedAt = null;
      runHealth = null;
    } else {
      allPlayers = _dedupPlayers(raw.players || []);
      dataLoaded = true;
      dataGeneratedAt = raw.generated_at || null;
      runHealth = raw.health || null;
    }
    updateTimestamp();
    render();
    loadChangeStrip();  // needs allPlayers — fire-and-forget after first render
  } else {
    document.getElementById('grid').innerHTML = '';
    document.getElementById('empty').textContent = 'Failed to load pulse data — check your connection and refresh.';
    document.getElementById('empty').style.display = 'block';
  }
})();

// Show next update time and keep it current
updateNextUpdate();
setInterval(updateNextUpdate, 60000);
