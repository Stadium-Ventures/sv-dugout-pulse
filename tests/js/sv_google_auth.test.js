// node --test tests/js/sv_google_auth.test.js — unit tests for js/sv-google-auth.js (no network,
// no browser). Tokens are synthetic unsigned JWTs; only claims matter here —
// Heartbeat verifies signatures server-side.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const { createAuth, CLIENT_ID, ALLOWED_DOMAIN } = require(path.join(__dirname, '..', '..', 'js', 'sv-google-auth.js'));

const NOW = 1790000000;
const b64u = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
const jwt = (claims) => `${b64u({ alg: 'RS256', kid: 'test' })}.${b64u(claims)}.sig`;
const good = (extra = {}) => jwt({
  iss: 'https://accounts.google.com', aud: CLIENT_ID, hd: ALLOWED_DOMAIN,
  email: 'someone@' + ALLOWED_DOMAIN, email_verified: true, exp: NOW + 3600, iat: NOW, ...extra,
});
const mk = () => createAuth({ now: () => NOW });

function fakeFetch(status) {
  const calls = [];
  const f = async (url, init) => { calls.push({ url, init }); return { status, ok: status >= 200 && status < 300, json: async () => [] }; };
  f.calls = calls;
  return f;
}

test('uses sv-registry OAuth client id and SV domain', () => {
  assert.strictEqual(CLIENT_ID, '970904391216-emekreu9hdntj6k76qhkr0fjvrvd9fcm.apps.googleusercontent.com');
  assert.strictEqual(ALLOWED_DOMAIN, 'stadium-ventures.com');
});

test('keeps a valid SV credential in memory and never touches web storage', () => {
  const touched = [];
  const trap = new Proxy({}, { get: (_, k) => { touched.push(k); return () => null; } });
  globalThis.localStorage = trap; globalThis.sessionStorage = trap;
  try {
    const a = mk();
    assert.strictEqual(a.setCredential(good()), true);
    assert.strictEqual(a.getToken(), good());
    assert.deepStrictEqual(touched, []);
  } finally { delete globalThis.localStorage; delete globalThis.sessionStorage; }
});

test('rejects wrong audience, wrong domain, unverified email, expired or malformed', () => {
  for (const bad of [good({ aud: 'other-client' }), good({ hd: 'gmail.com', email: 'x@gmail.com' }),
                     good({ email_verified: false }), good({ exp: NOW - 1 }), good({ exp: NOW + 30 }),
                     'not-a-jwt', '', null]) {
    const a = mk();
    assert.strictEqual(a.setCredential(bad), false);
    assert.strictEqual(a.getToken(), '');
  }
});

test('domain can come from the email when hd is absent', () => {
  const a = mk();
  assert.strictEqual(a.setCredential(good({ hd: undefined })), true);
});

test('token expires in place', () => {
  let t = NOW;
  const a = createAuth({ now: () => t });
  a.setCredential(good({ exp: NOW + 600 }));
  assert.ok(a.getToken());
  t = NOW + 600;                      // inside the 60s margin
  assert.strictEqual(a.getToken(), '');
});

test('signed out: no request is made at all (never credential-less)', async () => {
  const f = fakeFetch(200);
  const r = await mk().authorizedFetch('https://hb.example/api/heartbeat/summary', f);
  assert.deepStrictEqual(r, { state: 'signed-out' });
  assert.strictEqual(f.calls.length, 0);
});

test('signed in: sends the ID token as Bearer', async () => {
  const a = mk(); a.setCredential(good());
  const f = fakeFetch(200);
  const r = await a.authorizedFetch('https://hb.example/api/heartbeat/summary', f);
  assert.strictEqual(r.state, 'ok');
  assert.strictEqual(f.calls[0].init.headers.Authorization, 'Bearer ' + good());
});

for (const status of [401, 403]) {
  test(`${status} drops the token and reports denied`, async () => {
    const a = mk(); a.setCredential(good());
    const changes = []; a.onChange((s) => changes.push(s));
    const r = await a.authorizedFetch('u', fakeFetch(status));
    assert.deepStrictEqual(r, { state: 'denied', status });
    assert.strictEqual(a.getToken(), '');
    assert.deepStrictEqual(changes, [false]);
  });
}

test('5xx and network errors are transient, token kept', async () => {
  const a = mk(); a.setCredential(good());
  assert.deepStrictEqual(await a.authorizedFetch('u', fakeFetch(503)), { state: 'error', status: 503 });
  assert.deepStrictEqual(await a.authorizedFetch('u', async () => { throw new Error('offline'); }), { state: 'error' });
  assert.ok(a.getToken());
});

test('onChange fires on sign-in and a throwing listener does not break it', () => {
  const a = mk(); const seen = [];
  a.onChange(() => { throw new Error('boom'); });
  a.onChange((s) => seen.push(s));
  a.setCredential(good());
  assert.deepStrictEqual(seen, [true]);
});
