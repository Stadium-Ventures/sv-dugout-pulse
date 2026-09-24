// Google sign-in for calls that need an SV identity (Heartbeat, 2026-09-24).
//
// Heartbeat (sv-heartbeat.vercel.app) is closing its /api routes to anonymous
// callers. This page is a static site with no server, so it cannot hold a
// secret; instead the person viewing it signs in with their
// @stadium-ventures.com Google account (Google Identity Services) and the page
// sends that Google ID token as `Authorization: Bearer` on Heartbeat calls.
//
// - The client id is sv-registry's OAuth web client (api/_lib/auth.js
//   CLIENT_ID). It is a public identifier, not a secret.
// - The ID token lives in memory only — never localStorage, sessionStorage or a
//   cookie. A reload asks Google again (silently for a returning user).
// - The checks here (audience, domain, expiry) only decide whether to bother
//   sending the token; Heartbeat verifies the signature and every claim.
// - Not signed in, or Heartbeat says 401/403 → the caller hides the Heartbeat
//   UI and shows a sign-in prompt. Nothing else on the page depends on this.
(function (root) {
  'use strict';

  var CLIENT_ID = '970904391216-emekreu9hdntj6k76qhkr0fjvrvd9fcm.apps.googleusercontent.com';
  var ALLOWED_DOMAIN = 'stadium-ventures.com';
  var EXPIRY_MARGIN_S = 60;

  function decodeJwtPayload(jwt) {
    try {
      var part = String(jwt || '').split('.')[1];
      if (!part) return null;
      var b64 = part.replace(/-/g, '+').replace(/_/g, '/');
      while (b64.length % 4) b64 += '=';
      var json = typeof atob === 'function'
        ? decodeURIComponent(Array.prototype.map.call(atob(b64), function (c) {
            return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
          }).join(''))
        : Buffer.from(b64, 'base64').toString('utf8');
      return JSON.parse(json);
    } catch (e) {
      return null;
    }
  }

  // Core, with no DOM or network of its own — unit-tested in tests/js/.
  function createAuth(opts) {
    opts = opts || {};
    var clientId = opts.clientId || CLIENT_ID;
    var domain = opts.domain || ALLOWED_DOMAIN;
    var now = opts.now || function () { return Math.floor(Date.now() / 1000); };
    var token = '';          // in memory only
    var claims = null;
    var listeners = [];

    function notify() {
      for (var i = 0; i < listeners.length; i++) {
        try { listeners[i](!!token); } catch (e) { /* a listener must not break sign-in */ }
      }
    }

    function acceptable(c) {
      if (!c || c.aud !== clientId) return false;
      var d = c.hd || String(c.email || '').split('@')[1] || '';
      if (d !== domain) return false;
      if (c.email_verified === false) return false;
      return typeof c.exp === 'number' && c.exp - EXPIRY_MARGIN_S > now();
    }

    var api = {
      // Called with the GIS credential (a Google ID token). Returns true if kept.
      setCredential: function (jwt) {
        var c = decodeJwtPayload(jwt);
        if (!acceptable(c)) { api.clear(); return false; }
        token = String(jwt);
        claims = c;
        notify();
        return true;
      },
      // The current ID token, or '' when signed out or about to expire.
      getToken: function () {
        if (token && !acceptable(claims)) { token = ''; claims = null; }
        return token;
      },
      clear: function () {
        var had = !!token;
        token = '';
        claims = null;
        if (had) notify();
      },
      onChange: function (fn) { listeners.push(fn); },
      // GET with the ID token as Bearer. Never sends a credential-less request.
      //   {state: 'ok', response}      2xx
      //   {state: 'signed-out'}        no usable token — no request made
      //   {state: 'denied', status}    401/403 — token dropped, sign in again
      //   {state: 'error', status?}    anything else (network, 5xx)
      authorizedFetch: function (url, fetchImpl) {
        var f = fetchImpl || (typeof fetch === 'function' ? fetch : null);
        var t = api.getToken();
        if (!t) return Promise.resolve({ state: 'signed-out' });
        if (!f) return Promise.resolve({ state: 'error' });
        return Promise.resolve()
          .then(function () { return f(url, { headers: { Authorization: 'Bearer ' + t } }); })
          .then(function (resp) {
            if (resp.status === 401 || resp.status === 403) {
              api.clear();
              return { state: 'denied', status: resp.status };
            }
            if (!resp.ok) return { state: 'error', status: resp.status };
            return { state: 'ok', response: resp };
          }, function () { return { state: 'error' }; });
      }
    };
    return api;
  }

  // ── Browser wiring (skipped under Node tests) ────────────────────────────
  function startBrowser(auth) {
    var gisLoaded = false;
    function withGis(cb) {
      if (root.google && root.google.accounts && root.google.accounts.id) return cb(root.google.accounts.id);
      if (gisLoaded) return;
      gisLoaded = true;
      var s = document.createElement('script');
      s.src = 'https://accounts.google.com/gsi/client';
      s.async = true;
      s.defer = true;
      s.onload = function () { if (root.google && root.google.accounts) cb(root.google.accounts.id); };
      document.head.appendChild(s);
    }

    var initialized = false;
    function init(id) {
      if (initialized) return id;
      initialized = true;
      id.initialize({
        client_id: CLIENT_ID,
        hd: ALLOWED_DOMAIN,
        auto_select: true,          // returning users sign in silently
        itp_support: true,
        use_fedcm_for_prompt: true,
        callback: function (r) { auth.setCredential(r && r.credential); }
      });
      return id;
    }

    // Render the Google button into `el` and try a silent sign-in.
    auth.requestSignIn = function (el) {
      withGis(function (id) {
        init(id);
        if (el && !el.hasChildNodes()) {
          id.renderButton(el, { type: 'standard', size: 'small', text: 'signin_with', theme: 'outline' });
        }
        id.prompt();
      });
    };
    // Silent attempt on load; the visible button is rendered by the page.
    withGis(function (id) { init(id).prompt(); });
  }

  var auth = createAuth();
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { createAuth: createAuth, decodeJwtPayload: decodeJwtPayload,
                       CLIENT_ID: CLIENT_ID, ALLOWED_DOMAIN: ALLOWED_DOMAIN };
  } else {
    root.svGoogleAuth = auth;
    if (typeof document !== 'undefined') startBrowser(auth);
  }
})(typeof window !== 'undefined' ? window : globalThis);
