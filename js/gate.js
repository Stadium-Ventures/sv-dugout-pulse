// Shared-password gate for Dugout Pulse (Tom, 2026-09-23).
//
// The password is NOT in this file. It is checked by the SV Scouting Hub
// (/api/scouting-ref?ping=1), which holds it server-side, and remembered in
// this browser afterwards. The same stored password then unlocks the scouting
// reference panels (BA reports, prospect ranks, TruMedia) that the hub serves
// on request — none of that data is committed to this repo.
(function () {
  var KEY = 'dp_hub_pw';
  window.SV_HUB = 'https://sv-scouting-hub.vercel.app';
  window.svHubPassword = function () { try { return localStorage.getItem(KEY) || ''; } catch (e) { return ''; } };
  window.svHubFetch = function (query) {
    var pw = window.svHubPassword();
    if (!pw) return Promise.resolve(null);
    return fetch(window.SV_HUB + '/api/scouting-ref?' + query, { headers: { Authorization: 'Bearer ' + pw } })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
  };

  function verify(pw) {
    return fetch(window.SV_HUB + '/api/scouting-ref?ping=1', { headers: { Authorization: 'Bearer ' + pw } })
      .then(function (r) { return r.status === 204; })
      .catch(function () { return false; });
  }

  var css = document.createElement('style');
  css.textContent =
    '#dpGate{position:fixed;inset:0;z-index:9999;background:#0f1117;display:flex;align-items:center;justify-content:center;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;color:#e4e4e7}' +
    '#dpGate form{background:#1a1d27;border:1px solid #2a2e3a;border-radius:14px;padding:28px 26px;width:min(92vw,340px);display:flex;flex-direction:column;gap:12px}' +
    '#dpGate h1{font-size:18px;margin:0}#dpGate p{font-size:13px;color:#71717a;margin:0}' +
    '#dpGate input{font-size:16px;padding:10px 12px;border-radius:8px;border:1px solid #2a2e3a;background:#0f1117;color:#e4e4e7;outline:none}' +
    '#dpGate input:focus{border-color:#3b82f6}' +
    '#dpGate button{font-size:15px;font-weight:600;padding:10px;border:0;border-radius:999px;background:#3b82f6;color:#fff;cursor:pointer}' +
    '#dpGate button[disabled]{opacity:.6}' +
    '#dpGate .err{color:#ef4444;font-size:13px;min-height:16px}' +
    'body.dp-locked > :not(#dpGate){filter:blur(6px);pointer-events:none;user-select:none}';
  document.head.appendChild(css);

  function mount() {
    document.body.classList.add('dp-locked');
    var wrap = document.createElement('div');
    wrap.id = 'dpGate';
    wrap.innerHTML =
      '<form autocomplete="off"><h1>SV Dugout Pulse</h1><p>Internal. Enter the team password.</p>' +
      '<input type="password" name="pw" placeholder="Password" autofocus>' +
      '<div class="err"></div><button type="submit">Enter</button></form>';
    document.body.appendChild(wrap);
    var form = wrap.querySelector('form');
    var input = wrap.querySelector('input');
    var err = wrap.querySelector('.err');
    var btn = wrap.querySelector('button');
    form.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var pw = input.value.trim();
      if (!pw) return;
      btn.disabled = true; err.textContent = '';
      verify(pw).then(function (ok) {
        btn.disabled = false;
        if (ok) {
          try { localStorage.setItem(KEY, pw); } catch (e) {}
          wrap.remove();
          document.body.classList.remove('dp-locked');
          document.dispatchEvent(new Event('sv-hub-unlocked'));
        } else {
          err.textContent = 'That is not it, or the hub is unreachable.';
          input.select();
        }
      });
    });
    setTimeout(function () { input.focus(); }, 50);
  }

  var stored = window.svHubPassword();
  if (stored) {
    // Re-check quietly; a changed hub password re-prompts instead of failing silently.
    verify(stored).then(function (ok) { if (!ok) { try { localStorage.removeItem(KEY); } catch (e) {} mount(); } });
    return;
  }
  if (document.body) mount(); else document.addEventListener('DOMContentLoaded', mount);
})();
