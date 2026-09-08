/* =============================================================================
 * Temporal on Aerospike — demo control plane
 *
 * Vanilla ES module. No build step, no dependencies. Served by the Go control
 * service from a go:embed FS at "/".
 *
 * The page has one idea to teach: Temporal's ordered task queues are emulated
 * on Aerospike by bucketing. Everything under "Ordered task queues" exists to
 * make that mechanism visible — the record key, the bucket boundaries, and the
 * server-side ordering inside each bucket.
 *
 * Ordering note: the API returns shards, categories, buckets and entries
 * already in order. Nothing here re-sorts, and nothing iterates an object's
 * keys where an array was given — the ordering IS the lesson.
 *
 * Offline preview:  index.html?mock=1   (see ./mock/mock.js)
 * ========================================================================== */

const QS = new URLSearchParams(location.search);
export const MOCK = QS.get('mock') === '1';

if (MOCK) {
  // Installs fake fetch + EventSource. Imported only in mock mode, so the real
  // deployment never parses it.
  const m = await import('./mock/mock.js');
  m.install();
}

/* ── config ────────────────────────────────────────────────────────────── */

const POLL_MS          = 2000;   // state + aerospike refresh cadence
const REQ_TIMEOUT_MS   = 8000;   // normal request budget
const RUN_TIMEOUT_MS   = 120000; // a workflow run may legitimately take a while
// Three orders of magnitude. 1 is the single-run path; 10 and 100 go through
// the batch endpoint, which caps concurrency server-side.
const BATCH_SIZES      = [10, 100];
// Each workflow now sleeps 10s on a durable timer, so 100 runs at 50 concurrent
// is two rounds — a little over 20s. The stall that can affect a single run
// applies here too, so keep generous headroom.
const BATCH_TIMEOUT_MS = 300000;
const SSE_RETRY_MS     = 2000;   // manual reconnect delay once EventSource gives up
const MAX_RUNS         = 12;
const MAX_LOG          = 300;

/* ── dom ───────────────────────────────────────────────────────────────── */

const $ = (id) => document.getElementById(id);

const el = {
  hero:          $('hero'),
  tileSqlite:    $('tile-sqlite'),
  tileAerospike: $('tile-aerospike'),
  trackArrow:    $('track-arrow'),
  pillReady:     $('pill-ready'),
  pillServer:    $('pill-server'),
  pillNs:        $('pill-ns'),
  pillStream:    $('pill-stream'),
  stepper:       $('stepper'),

  btnRun:        $('btn-run'),
  btnRun10:      $('btn-run-10'),
  btnRun100:     $('btn-run-100'),
  hintRun:       $('hint-run'),
  runsBody:      $('runs-body'),
  runsEmpty:     $('runs-empty'),

  btnSwitch:     $('btn-switch'),
  hintSwitch:    $('hint-switch'),
  log:           $('log'),
  btnClearLog:   $('btn-clear-log'),

  btnReset:      $('btn-reset'),
  hintReset:     $('hint-reset'),
  resetConfirm:  $('reset-confirm'),
  btnResetGo:    $('btn-reset-go'),
  btnResetCancel:$('btn-reset-cancel'),

  statReach:     $('stat-reach'),
  statSc:        $('stat-sc'),
  statDead:      $('stat-dead'),
  statUnavail:   $('stat-unavail'),
  statObjects:   $('stat-objects'),
  asNotice:      $('as-notice'),

  rules:         $('rules'),
  htask:         $('htask'),
  htaskEmpty:    $('htask-empty'),

  footMode:      $('foot-mode'),
};

/* ── state ─────────────────────────────────────────────────────────────── */

const state = {
  // from /api/state
  backend:       null,      // "sqlite" | "aerospike" | null (unknown)
  temporalReady: false,
  switching:     false,     // server-reported
  namespace:     null,
  stateError:    null,

  // local
  serverUp:      null,      // null = never talked to it yet
  localSwitch:   false,     // we asked for a switch; server may not report it yet
  localReset:    false,     // we asked for a reset; same
  expectDown:    false,     // downtime is expected -> not an error
  running:       false,     // a workflow run is in flight
  switchPending: false,     // the POST /api/switch itself is in flight
  batchRunning:  0,         // size of the bulk run in flight, 0 when idle
  resetPending:  false,     // the POST /api/reset itself is in flight
  confirmReset:  false,     // the destructive-action confirmation is showing

  // aerospike
  health:        null,
  healthErr:     null,
  htask:         null,      // the /api/aerospike/history-tasks payload
  htaskErr:      null,

  runs:          [],
  streamStatus:  'connecting',
};

const isSwitching = () => state.switching || state.localSwitch || state.localReset;
const onAerospike = () => state.backend === 'aerospike';
const busyTransition = () =>
  isSwitching() || state.switchPending || state.resetPending;

/* ── tiny helpers ──────────────────────────────────────────────────────── */

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

const nfmt = (n) =>
  (typeof n === 'number' && isFinite(n) ? n.toLocaleString('en-US') : '—');

function fmtBytes(n) {
  if (typeof n !== 'number' || !isFinite(n) || n < 0) return '';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1048576).toFixed(1)} MiB`;
}

function fmtDuration(ms) {
  if (typeof ms !== 'number' || !isFinite(ms)) return '—';
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`;
}

const shortId = (s, n = 22) =>
  !s ? '—' : (s.length > n ? `${s.slice(0, n - 3)}…` : s);

const backendLabel = (b) =>
  b === 'aerospike' ? 'Aerospike' : b === 'sqlite' ? 'SQLite' : 'unknown';

/* ── time / key encoding helpers ───────────────────────────────────────── */

/* RFC3339 -> nanoseconds since epoch, as BigInt. Date.parse only carries
   milliseconds, so any digits past the third are picked out of the string. */
function rfc3339Nanos(s) {
  const ms = Date.parse(s);
  if (!isFinite(ms)) return null;
  const frac = /\.(\d+)/.exec(String(s));
  const digits = frac ? frac[1].padEnd(9, '0').slice(0, 9) : '000000000';
  const secs = Math.floor(ms / 1000);
  return BigInt(secs) * 1000000000n + BigInt(digits);
}

/* Wall-clock part of a timestamp. Dates are almost never useful here — every
   timer in the demo fires within minutes — so the time is what gets shown. */
function fmtClock(s, { ms = true } = {}) {
  const d = new Date(s);
  if (isNaN(d)) return String(s ?? '—');
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return ms
    ? `${hh}:${mm}:${ss}.${String(d.getMilliseconds()).padStart(3, '0')}`
    : `${hh}:${mm}:${ss}`;
}

const hex64 = (v) => v.toString(16).padStart(16, '0');

/* The exact bytes the store writes as the map key for a scheduled task:
   bigendian(fireTimeNanos) || bigendian(taskID). Reproduced here so the
   16-byte blob is a thing you can look at, not a thing you are told about. */
function scheduledKeyHex(fireTime, taskId) {
  const nanos = rfc3339Nanos(fireTime);
  if (nanos === null || typeof taskId !== 'number') return null;
  return { fire: hex64(nanos), id: hex64(BigInt(Math.trunc(taskId))) };
}

/* ── api ───────────────────────────────────────────────────────────────── */

async function api(path, { method = 'GET', body, timeout = REQ_TIMEOUT_MS } = {}) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeout);
  try {
    const res = await fetch(path, {
      method,
      body,
      cache: 'no-store',
      signal: ctl.signal,
      headers: body ? { 'content-type': 'application/json' } : undefined,
    });
    if (!res.ok) {
      // Only surface a body we can show in one line — an error page is noise.
      const ct = res.headers.get('content-type') || '';
      let detail = '';
      if (!/html/i.test(ct)) {
        detail = (await res.text().catch(() => '')).replace(/\s+/g, ' ').trim().slice(0, 140);
      }
      throw new Error(`${res.status} ${res.statusText || 'error'}${detail ? ` — ${detail}` : ''}`);
    }
    const text = await res.text();
    return text ? JSON.parse(text) : null;
  } finally {
    clearTimeout(timer);
  }
}

/* ── log ───────────────────────────────────────────────────────────────── */

function log(type, message, ts) {
  if (!message) return;
  const li = document.createElement('li');
  li.dataset.type = type || 'progress';

  const when = ts ? new Date(ts) : new Date();
  const time = isNaN(when) ? '' : when.toLocaleTimeString(undefined, { hour12: false });

  const t = document.createElement('span');
  t.className = 'ts';
  t.textContent = time;
  const m = document.createElement('span');
  m.className = 'msg';
  m.textContent = message;

  li.append(t, m);

  // Only auto-scroll if the viewer has not scrolled up to read something.
  const pinned = el.log.scrollTop + el.log.clientHeight >= el.log.scrollHeight - 24;
  el.log.appendChild(li);
  while (el.log.childElementCount > MAX_LOG) el.log.removeChild(el.log.firstElementChild);
  if (pinned) el.log.scrollTop = el.log.scrollHeight;
}

/* ── SSE ───────────────────────────────────────────────────────────────── */

let es = null;
let esRetry = null;

function setStream(status) {
  if (state.streamStatus === status) return;
  state.streamStatus = status;
  renderStreamPill();
}

function connectEvents() {
  clearTimeout(esRetry);
  if (es) { try { es.close(); } catch { /* already gone */ } }

  setStream(state.streamStatus === 'live' ? 'reconnecting' : 'connecting');
  es = new EventSource('/api/events');

  es.onopen = () => setStream('live');

  es.onmessage = (ev) => {
    setStream('live');
    let data = null;
    try { data = JSON.parse(ev.data); } catch { /* not JSON */ }
    if (!data) { log('progress', ev.data); return; }
    log(data.type, data.message, data.ts);
    // A state event means something changed server-side; refresh immediately.
    if (data.type === 'state') scheduleTick(0);
  };

  es.onerror = () => {
    // EventSource retries by itself while CONNECTING. Only step in once it
    // has actually given up. This is the normal path during a server restart.
    setStream('reconnecting');
    if (!es || es.readyState === EventSource.CLOSED) {
      try { es && es.close(); } catch { /* ignore */ }
      es = null;
      clearTimeout(esRetry);
      esRetry = setTimeout(connectEvents, SSE_RETRY_MS);
    }
  };
}

/* ── polling ───────────────────────────────────────────────────────────── */

let pollTimer = null;
let ticking = false;

function scheduleTick(delay = POLL_MS) {
  clearTimeout(pollTimer);
  pollTimer = setTimeout(tick, delay);
}

async function tick() {
  clearTimeout(pollTimer);
  if (document.hidden || ticking) { scheduleTick(); return; }
  ticking = true;
  try {
    await refreshState();
    if (state.serverUp) await refreshAerospike();
    render();
  } finally {
    ticking = false;
    scheduleTick();
  }
}

async function refreshState() {
  const prevBackend = state.backend;
  const prevUp = state.serverUp;
  try {
    const s = await api('/api/state', { timeout: 4000 });
    state.serverUp      = true;
    state.backend       = s?.backend ?? null;
    state.temporalReady = !!s?.temporalReady;
    state.switching     = !!s?.switching;
    state.namespace     = s?.namespace ?? state.namespace;
    // The service reports `error` when it cannot read the Deployment (bad RBAC,
    // no cluster). `backend` then falls back to a default rather than the truth,
    // so this must be surfaced -- otherwise the demo confidently shows the wrong
    // store.
    const prevStateErr  = state.stateError;
    state.stateError    = s?.error ?? null;
    if (state.stateError && state.stateError !== prevStateErr) {
      log('error', `Cannot read the live backend: ${state.stateError}`);
    }

    if (prevUp === false) log('local', 'Control plane is back.');
    if (prevBackend && prevBackend !== state.backend) {
      log('state', `Persistence store is now ${backendLabel(state.backend)}.`);
    }
    // The switch has landed: drop our optimistic flag.
    if (state.localSwitch && onAerospike() && !state.switching) {
      state.localSwitch = false;
      state.expectDown  = false;
      log('state', 'Switch complete — Temporal is serving from Aerospike.');
    }
    // Same for the reset, which lands the other way round.
    if (state.localReset && state.backend === 'sqlite' && !state.switching) {
      state.localReset = false;
      state.expectDown = false;
      state.runs = [];
      log('state', 'Reset complete — Aerospike is empty and Temporal is back on SQLite.');
    }
    if (!isSwitching()) state.expectDown = false;
  } catch (err) {
    // During a switch the server restarts, so refused connections are expected.
    if (state.serverUp !== false) {
      log(state.expectDown ? 'local' : 'error',
          state.expectDown
            ? 'Control plane restarting — waiting for it to come back…'
            : `Cannot reach the control plane: ${err.message}`);
    }
    state.serverUp = false;
  }
}

async function refreshAerospike() {
  const [h, t] = await Promise.allSettled([
    api('/api/aerospike/health',        { timeout: 4000 }),
    api('/api/aerospike/history-tasks', { timeout: 6000 }),
  ]);

  if (h.status === 'fulfilled') { state.health = h.value; state.healthErr = null; }
  else { state.health = null; state.healthErr = h.reason?.message || String(h.reason); }

  if (t.status === 'fulfilled') { state.htask = t.value || null; state.htaskErr = null; }
  else { state.htask = null; state.htaskErr = t.reason?.message || String(t.reason); }
}

/* ── actions ───────────────────────────────────────────────────────────── */

async function runWorkflow() {
  if (state.running || isSwitching() || !state.serverUp) return;
  state.running = true;
  render();
  log('local', 'Starting workflow…');

  const t0 = performance.now();
  try {
    const r = await api('/api/workflow/run', { method: 'POST', timeout: RUN_TIMEOUT_MS });
    // The server tells us which store the run actually executed against. Trust
    // that over our polled view of the backend — it is authoritative for the run.
    const store = r?.persistenceStore || state.backend || 'unknown';
    pushRun({
      ok:         true,
      store,
      workflowId: r?.workflowId ?? '(no id)',
      runId:      r?.runId ?? '',
      result:     r?.result ?? '(no result)',
      durationMs: typeof r?.durationMs === 'number' ? r.durationMs : Math.round(performance.now() - t0),
    });
    log('progress', `Workflow completed on ${backendLabel(store)}: ${r?.result ?? ''}`);
  } catch (err) {
    pushRun({
      ok:         false,
      store:      state.backend || 'unknown',
      workflowId: '—',
      runId:      '',
      result:     err.message || String(err),
      durationMs: Math.round(performance.now() - t0),
    });
    log('error', `Workflow failed: ${err.message || err}`);
  } finally {
    state.running = false;
    render();
    scheduleTick(150); // let the bucket view move straight away
  }
}

// A batch adds ONE summary row, not a hundred. The run table exists to make the
// store-per-run contrast readable; a hundred identical greetings would bury it.
async function runBatch(size) {
  if (state.running || state.batchRunning || isSwitching() || !state.serverUp) return;
  state.batchRunning = size;
  render();
  log('local', `Starting ${size} workflows…`);

  const t0 = performance.now();
  try {
    const r = await api('/api/workflow/run-batch', {
      method: 'POST',
      body: JSON.stringify({ count: size }),
      timeout: BATCH_TIMEOUT_MS,
    });
    const store = r?.persistenceStore || state.backend || 'unknown';
    const failed = r?.failed ?? 0;
    pushRun({
      ok:         failed === 0,
      store,
      batch:      true,
      workflowId: `${r?.completed ?? 0}/${r?.requested ?? size} workflows`,
      runId:      '',
      result:     failed === 0
        ? `all completed · fastest ${r?.fastestMs ?? '?'}ms, slowest ${r?.slowestMs ?? '?'}ms`
        : `${failed} failed · ${r?.firstError ?? ''}`,
      durationMs: typeof r?.durationMs === 'number' ? r.durationMs : Math.round(performance.now() - t0),
    });
    log(failed === 0 ? 'progress' : 'error',
        `${r?.completed ?? 0}/${r?.requested ?? size} workflows completed on ${backendLabel(store)} in ${r?.durationMs ?? '?'}ms`);
  } catch (err) {
    log('error', `Batch failed: ${err.message || err}`);
  } finally {
    state.batchRunning = 0;
    render();
    scheduleTick(150);
  }
}

function pushRun(run) {
  state.runs.unshift({ ...run, at: new Date(), fresh: true });
  state.runs = state.runs.slice(0, MAX_RUNS);
}

async function startSwitch() {
  if (busyTransition() || onAerospike()) return;

  state.switchPending = true;
  state.localSwitch   = true;
  state.expectDown    = true;
  state.confirmReset  = false;
  render();
  log('local', 'Requested switch to Aerospike. The stack will reconfigure and Temporal will restart.');

  try {
    await api('/api/switch', {
      method: 'POST',
      body: JSON.stringify({ backend: 'aerospike' }),
      timeout: 15000,
    });
  } catch (err) {
    state.localSwitch = false;
    state.expectDown  = false;
    log('error', `Switch request failed: ${err.message || err}`);
  } finally {
    state.switchPending = false;
    render();
    scheduleTick(300);
  }
}

function askReset() {
  if (busyTransition() || state.serverUp === false) return;
  state.confirmReset = true;
  render();
  el.btnResetCancel.focus();
}

function cancelReset() {
  state.confirmReset = false;
  render();
}

async function startReset() {
  if (busyTransition()) return;

  state.confirmReset  = false;
  state.resetPending  = true;
  state.localReset    = true;
  state.expectDown    = true;
  render();
  log('local', 'Requested reset. Aerospike will be wiped and Temporal moved back to SQLite.');

  try {
    await api('/api/reset', { method: 'POST', timeout: 15000 });
  } catch (err) {
    state.localReset = false;
    state.expectDown = false;
    log('error', `Reset request failed: ${err.message || err}`);
  } finally {
    state.resetPending = false;
    render();
    scheduleTick(300);
  }
}

/* ── render ────────────────────────────────────────────────────────────── */

function render() {
  renderHero();
  renderStepper();
  renderActions();
  renderRuns();
  renderHealth();
  renderRules();
  renderHtask();
}

function pill(node, tone, text) {
  node.dataset.tone = tone;
  node.querySelector('.pill-text').textContent = text;
}

function renderStreamPill() {
  const s = state.streamStatus;
  pill(el.pillStream,
    s === 'live' ? 'ok' : s === 'reconnecting' ? 'warn' : 'idle',
    s === 'live' ? 'events: live'
      : s === 'reconnecting' ? 'events: reconnecting' : 'events: connecting');
}

function renderHero() {
  // An unreadable backend is shown as unknown, not as its fallback value.
  const b = state.stateError ? 'unknown' : (state.backend || 'unknown');
  el.hero.dataset.backend  = b;
  el.hero.dataset.switching = String(isSwitching());

  el.tileSqlite.dataset.active    = String(b === 'sqlite');
  el.tileAerospike.dataset.active = String(b === 'aerospike');
  el.trackArrow.dataset.moving    = String(isSwitching());
  el.trackArrow.dataset.reverse   = String(state.localReset);

  if (isSwitching()) {
    pill(el.pillReady, 'warn', 'Temporal: restarting');
  } else if (state.serverUp === false) {
    pill(el.pillReady, 'err', 'Temporal: unknown');
  } else if (state.temporalReady) {
    pill(el.pillReady, 'ok', 'Temporal: ready');
  } else if (state.serverUp === null) {
    pill(el.pillReady, 'idle', 'Temporal: checking…');
  } else {
    pill(el.pillReady, 'warn', 'Temporal: not ready');
  }

  if (state.stateError) {
    pill(el.pillServer, 'err', 'control plane: cannot read backend');
  } else if (state.serverUp === true) {
    pill(el.pillServer, 'ok', 'control plane: connected');
  } else if (state.serverUp === false) {
    pill(el.pillServer, isSwitching() ? 'warn' : 'err',
      isSwitching() ? 'control plane: restarting' : 'control plane: unreachable');
  } else {
    pill(el.pillServer, 'idle', 'control plane: connecting');
  }

  el.pillNs.innerHTML = `namespace <code>${esc(state.namespace || '—')}</code>`;
}

function renderStepper() {
  const runsOnAerospike = state.runs.filter((r) => r.store === 'aerospike').length;
  let now;
  if (state.localReset)                     now = 1;
  else if (isSwitching())                   now = 3;
  else if (!onAerospike())                  now = state.runs.length === 0 ? 2 : 3;
  else if (runsOnAerospike === 0)           now = 4;
  else                                      now = 5;

  for (const li of el.stepper.children) {
    const n = Number(li.dataset.step);
    li.dataset.state = n < now ? 'done' : n === now ? 'now' : 'todo';
  }
}

function renderActions() {
  // Run workflow
  const runBlocked = isSwitching() || state.serverUp === false;
  const anyRunning = state.running || state.batchRunning > 0;
  el.btnRun.disabled = runBlocked || anyRunning;
  el.btnRun.dataset.busy = String(state.running);

  for (const [size, btn] of [[BATCH_SIZES[0], el.btnRun10], [BATCH_SIZES[1], el.btnRun100]]) {
    btn.disabled = runBlocked || anyRunning;
    btn.dataset.busy = String(state.batchRunning === size);
    btn.querySelector('.btn-label').textContent =
      state.batchRunning === size ? `Running ${size}…` : `Run ${size}`;
  }
  el.btnRun.querySelector('.btn-label').textContent =
    state.running ? 'Running…' : 'Run workflow';

  if (isSwitching()) {
    hint(el.hintRun, 'warn', 'Disabled while the stack reconfigures — Temporal is restarting.');
  } else if (state.serverUp === false) {
    hint(el.hintRun, 'err', 'Control plane unreachable.');
  } else if (!state.temporalReady && state.serverUp) {
    hint(el.hintRun, 'warn', 'Temporal is not reporting ready yet.');
  } else {
    hint(el.hintRun, '', `Executes one workflow with one activity against ${backendLabel(state.backend)}.`);
  }

  // Switch
  const switchBusy = isSwitching() || state.switchPending;
  el.btnSwitch.disabled = switchBusy || onAerospike() || state.serverUp === false;
  el.btnSwitch.dataset.busy = String(switchBusy && !state.localReset);
  el.btnSwitch.querySelector('.btn-label').textContent =
    onAerospike() ? 'Running on Aerospike' : switchBusy ? 'Switching…' : 'Switch to Aerospike';

  if (onAerospike() && !switchBusy) {
    hint(el.hintSwitch, 'ok', 'Done. Temporal is persisting to Aerospike — run the workflow again.');
  } else if (state.localReset) {
    hint(el.hintSwitch, 'warn', 'Resetting back to SQLite.');
  } else if (switchBusy) {
    hint(el.hintSwitch, 'warn', 'Reconfiguring. This takes tens of seconds and the server restarts.');
  } else if (state.serverUp === false) {
    hint(el.hintSwitch, 'err', 'Control plane unreachable.');
  } else {
    hint(el.hintSwitch, '', 'Rewrites the persistence config, restarts Temporal, and reconnects.');
  }

  // Reset — recovery only, never part of the happy path.
  const resetBusy = state.localReset || state.resetPending;
  el.btnReset.disabled = busyTransition() || state.serverUp === false;
  el.btnReset.dataset.busy = String(resetBusy);
  el.btnReset.querySelector('.btn-label').textContent =
    resetBusy ? 'Resetting…' : 'Reset demo';

  el.resetConfirm.hidden = !state.confirmReset;
  el.btnResetGo.disabled = busyTransition();

  if (resetBusy) {
    hint(el.hintReset, 'warn', 'Wiping Aerospike and restarting Temporal on SQLite.');
  } else if (state.serverUp === false) {
    hint(el.hintReset, 'err', 'Control plane unreachable.');
  } else {
    hint(el.hintReset, '', 'Destructive. Deletes every Aerospike record and puts the demo back at step 1.');
  }
}

function hint(node, tone, text) {
  if (tone) node.dataset.tone = tone; else delete node.dataset.tone;
  node.textContent = text;
}

let runsSig = '';
function renderRuns() {
  const sig = state.runs.map((r) => `${r.at.getTime()}:${r.store}:${r.ok}`).join('|');
  el.runsEmpty.hidden = state.runs.length > 0;
  if (sig === runsSig) return;
  runsSig = sig;

  el.runsBody.textContent = '';
  for (const r of state.runs) {
    const tr = document.createElement('tr');
    if (r.fresh) { tr.className = 'run-row-fresh'; r.fresh = false; }
    tr.innerHTML = `
      <td class="c-store"><span class="store-tag" data-b="${esc(r.store)}">${esc(r.store)}</span></td>
      <td class="c-id"><span class="run-id" title="${esc(r.workflowId)}${r.runId ? ` · run ${esc(r.runId)}` : ''}">${esc(shortId(r.workflowId))}</span></td>
      <td class="c-dur">${esc(fmtDuration(r.durationMs))}</td>
      <td class="c-res"><span class="run-result" data-ok="${r.ok}">${esc(r.result)}</span></td>`;
    el.runsBody.appendChild(tr);
  }
}

function setStat(node, tone, value) {
  node.dataset.tone = tone;
  node.querySelector('.stat-value').textContent = value;
}

function renderHealth() {
  const h = state.health;
  const reachable = !!h?.reachable;

  if (!h) {
    setStat(el.statReach,   state.healthErr ? 'warn' : 'idle', state.healthErr ? 'no data' : '—');
    setStat(el.statSc,      'idle', '—');
    setStat(el.statDead,    'idle', '—');
    setStat(el.statUnavail, 'idle', '—');
    setStat(el.statObjects, 'idle', '—');
  } else {
    setStat(el.statReach, reachable ? 'ok' : 'err', reachable ? 'up' : 'down');
    setStat(el.statSc, h.strongConsistency ? 'ok' : 'warn', h.strongConsistency ? 'on' : 'off');
    setStat(el.statDead,    h.deadPartitions        ? 'err' : 'ok', nfmt(h.deadPartitions));
    setStat(el.statUnavail, h.unavailablePartitions ? 'err' : 'ok', nfmt(h.unavailablePartitions));
    setStat(el.statObjects, h.objects ? 'ok' : 'idle', nfmt(h.objects));
  }

  let notice = '', tone = '';
  if (isSwitching()) {
    notice = 'Reconfiguring. Buckets appear as Temporal writes its first history tasks.';
    tone = 'warn';
  } else if (state.healthErr && state.serverUp) {
    notice = `Namespace health unavailable: ${state.healthErr}`;
    tone = 'warn';
  } else if (h && !reachable) {
    notice = 'Aerospike is not reachable from the control plane.';
    tone = 'err';
  } else if (reachable && !onAerospike()) {
    notice = 'Aerospike is up but Temporal is not using it yet — switch the store to populate it.';
    tone = '';
  }
  el.asNotice.hidden = !notice;
  el.asNotice.textContent = notice;
  if (tone) el.asNotice.dataset.tone = tone; else delete el.asNotice.dataset.tone;
}

/* ── bucketing rules ───────────────────────────────────────────────────── */

let rulesSig = '';
function renderRules() {
  const b = state.htask?.bucketing;
  const shift = typeof b?.immediateShift === 'number' ? b.immediateShift : null;
  const secs  = typeof b?.scheduledBucketSeconds === 'number' ? b.scheduledBucketSeconds : null;

  const sig = `${shift}|${secs}`;
  if (sig === rulesSig) return;
  rulesSig = sig;

  const perBucket = shift === null ? null : 2 ** shift;

  el.rules.innerHTML = `
    <div class="rule" data-kind="immediate">
      <span class="rule-kind">Immediate</span>
      <span class="rule-cats">transfer · visibility · outbound</span>
      <div class="rule-body">
        <p class="rule-line"><span class="rule-tag">bucket</span>
          <code>taskID &gt;&gt; ${shift === null ? '?' : esc(shift)}</code>
          <span class="rule-why">${perBucket === null ? '' : `${esc(nfmt(perBucket))} task ids per record`}</span></p>
        <p class="rule-line"><span class="rule-tag">map key</span>
          <code>int64 taskID</code>
          <span class="rule-why">ordered by task id alone</span></p>
      </div>
    </div>
    <div class="rule" data-kind="scheduled">
      <span class="rule-kind">Scheduled</span>
      <span class="rule-cats">timer</span>
      <div class="rule-body">
        <p class="rule-line"><span class="rule-tag">bucket</span>
          <code>fireTime ÷ ${secs === null ? '?' : esc(secs)}s</code>
          <span class="rule-why">${secs === null ? '' : `one record per ${esc(secs)} seconds of fire time`}</span></p>
        <p class="rule-line"><span class="rule-tag">map key</span>
          <code>16-byte blob: BE(fireTime) ‖ BE(taskID)</code>
          <span class="rule-why">bytewise order = <code>(fireTime, taskID)</code></span></p>
      </div>
    </div>`;
}

/* ── the bucket view ───────────────────────────────────────────────────── */

// Shards the viewer has expanded. Survives the subtree rebuild that happens
// whenever the history-task payload changes.
const openShards = new Set();

let htaskSig = '';
function renderHtask() {
  const data = state.htask;
  const shards = Array.isArray(data?.shards) ? data.shards : [];

  // Empty / error states first — they replace the view entirely.
  let emptyMsg = '';
  if (state.htaskErr && state.serverUp) {
    emptyMsg = `Could not read history tasks: ${state.htaskErr}`;
  } else if (shards.length === 0) {
    emptyMsg = isSwitching()
      ? 'Reconfiguring — buckets appear once Temporal writes its first history task.'
      : onAerospike()
        ? 'No history tasks in Aerospike right now. Run a workflow: transfer and timer tasks are written, drained by the history service, and show up here — including the buckets they leave behind.'
        : 'Temporal is on SQLite, so nothing is stored here yet. Switch the store, run the workflow, and its task queues appear as bucketed Aerospike records.';
  }

  el.htaskEmpty.hidden = !emptyMsg;
  el.htaskEmpty.textContent = emptyMsg;

  const sig = JSON.stringify({ d: data, e: state.htaskErr, s: !!emptyMsg });
  if (sig === htaskSig) return;
  htaskSig = sig;

  el.htask.textContent = '';
  if (emptyMsg) return;

  const bucketing = data?.bucketing || {};
  for (const shard of shards) el.htask.appendChild(shardBlock(shard, bucketing));
}

// Shards are collapsed by default.
//
// A busy store has four shards of several categories of many buckets each, and
// all of it expanded is a wall. Native <details> rather than a JS toggle: it is
// keyboard accessible and needs no state of its own — which matters because
// this subtree is rebuilt whenever the payload changes, and any open/closed
// state kept in JS would be lost on every poll. Open shards are remembered in
// `openShards` and reapplied, so a shard you expanded stays expanded while the
// counts underneath it keep moving.
function shardBlock(shard, bucketing) {
  const sec = document.createElement('details');
  sec.className = 'shard';
  sec.open = openShards.has(String(shard.shardId));
  sec.addEventListener('toggle', () => {
    if (sec.open) openShards.add(String(shard.shardId));
    else openShards.delete(String(shard.shardId));
  });

  const cats = Array.isArray(shard.categories) ? shard.categories : [];
  const buckets = cats.reduce((n, c) => n + (c.buckets?.length || 0), 0);
  const entries = cats.reduce(
    (n, c) => n + (c.buckets || []).reduce((m, b) => m + (b.count || 0), 0), 0);

  const head = document.createElement('summary');
  head.className = 'shard-head';
  head.innerHTML = `
    <span class="shard-badge">shard <b>${esc(shard.shardId)}</b></span>
    <span class="shard-counts">${esc(buckets)} bucket${buckets === 1 ? '' : 's'} ·
      ${esc(entries)} task${entries === 1 ? '' : 's'}</span>
    <span class="shard-note">Cassandra would keep all of this in one partition keyed
      <code>shard_id = ${esc(shard.shardId)}</code>.</span>`;
  sec.appendChild(head);

  for (const cat of cats) sec.appendChild(categoryBlock(shard, cat, bucketing));
  if (cats.length === 0) sec.appendChild(emptyLine('No task categories in this shard.'));
  return sec;
}

function categoryBlock(shard, cat, bucketing) {
  const scheduled = !!cat.scheduled;
  const wrap = document.createElement('div');
  wrap.className = 'cat';
  wrap.dataset.scheduled = String(scheduled);

  const head = document.createElement('div');
  head.className = 'cat-head';
  head.innerHTML = `
    <span class="cat-name">${esc(cat.name ?? 'category')}</span>
    <span class="cat-id">category&nbsp;${esc(cat.id)}</span>
    <span class="kind" data-scheduled="${scheduled}">${scheduled ? 'scheduled' : 'immediate'}</span>
    <span class="cat-key">map key ${scheduled
      ? '<code>BE(fireTime) ‖ BE(taskID)</code>'
      : '<code>int64 taskID</code>'}</span>`;
  wrap.appendChild(head);

  const rail = document.createElement('div');
  rail.className = 'rail';

  const axis = document.createElement('div');
  axis.className = 'rail-axis';
  axis.innerHTML = `<span class="axis-text">range read walks buckets in ascending order</span><span class="axis-arrow" aria-hidden="true"></span>`;
  wrap.appendChild(axis);

  const buckets = Array.isArray(cat.buckets) ? cat.buckets : [];
  buckets.forEach((b, i) => {
    if (i > 0) {
      const chev = document.createElement('span');
      chev.className = 'rail-chev';
      chev.setAttribute('aria-hidden', 'true');
      chev.textContent = '›';
      rail.appendChild(chev);
    }
    rail.appendChild(bucketCard(shard, cat, b, bucketing));
  });
  if (buckets.length === 0) rail.appendChild(emptyLine('No buckets — this queue has never been written.'));

  wrap.appendChild(rail);
  return wrap;
}

/* The visible bucket boundary. This is the bit that turns a list of tasks into
   an explanation: the boundary is computed from the bucketing function the API
   reports, not from the entries that happen to be inside. */
function boundaryText(cat, b, bucketing) {
  const n = Number(b.bucket);
  if (!isFinite(n)) return '';

  if (cat.scheduled) {
    const secs = Number(bucketing.scheduledBucketSeconds);
    if (!isFinite(secs) || secs <= 0) return '';
    const from = new Date(n * secs * 1000);
    const to   = new Date((n + 1) * secs * 1000);
    return `fires ${fmtClock(from, { ms: false })} → ${fmtClock(to, { ms: false })}`;
  }

  const shift = Number(bucketing.immediateShift);
  if (!isFinite(shift) || shift < 0) return '';
  const lo = n * 2 ** shift;
  const hi = lo + 2 ** shift - 1;
  return `task ids ${nfmt(lo)} → ${nfmt(hi)}`;
}

function bucketCard(shard, cat, b, bucketing) {
  const count   = Number(b.count) || 0;
  const entries = Array.isArray(b.entries) ? b.entries : [];
  const empty   = count === 0;

  const art = document.createElement('article');
  art.className = 'bucket';
  art.dataset.empty = String(empty);

  const head = document.createElement('header');
  head.className = 'bk-head';
  head.innerHTML = `
    <span class="bk-n">bucket <b>${esc(b.bucket)}</b></span>
    <span class="bk-count">${esc(nfmt(count))}<span class="unit">${count === 1 ? 'entry' : 'entries'}</span></span>`;
  art.appendChild(head);

  const key = document.createElement('code');
  key.className = 'bk-key';
  key.textContent = b.recordKey ?? `${shard.shardId}:${cat.id}:${b.bucket}`;
  art.appendChild(key);

  const bound = boundaryText(cat, b, bucketing);
  if (bound) {
    const bd = document.createElement('div');
    bd.className = 'bk-bound';
    bd.textContent = bound;
    art.appendChild(bd);
  }

  if (empty) {
    const p = document.createElement('p');
    p.className = 'bk-drained';
    p.textContent = 'drained — every task acked and removed, but the record itself has not been retired yet';
    art.appendChild(p);
  } else {
    const ol = document.createElement('ol');
    ol.className = 'bk-entries';
    // Server order, verbatim. Do not sort.
    for (const e of entries) ol.appendChild(entryRow(cat, e));
    art.appendChild(ol);

    if (entries.length < count) {
      const more = document.createElement('p');
      more.className = 'bk-more';
      more.textContent = `+ ${nfmt(count - entries.length)} more in this record — the API returns the first ${nfmt(entries.length)}`;
      art.appendChild(more);
    }
  }

  const foot = document.createElement('footer');
  foot.className = 'bk-foot';
  foot.innerHTML = `K-ordered map <code>t</code>${empty ? '' : ' — returned in key order by the server'}`;
  art.appendChild(foot);

  return art;
}

function valueTag(e) {
  const val = document.createElement('span');
  val.className = 'ent-val';
  val.title = 'serialized proto — held as bytes, never decoded by the store';
  val.textContent = `blob ${fmtBytes(e.bytes) || '—'}`;
  return val;
}

function entryRow(cat, e) {
  const li = document.createElement('li');
  const hexes = cat.scheduled ? scheduledKeyHex(e.fireTime, e.taskId) : null;

  // Scheduled: the map key is 16 bytes, so it gets a line of its own and the
  // human reading of it sits underneath. Immediate: the map key IS the task id,
  // so one line is the whole story.
  if (hexes) {
    li.className = 'ent ent-sched';
    const kb = document.createElement('span');
    kb.className = 'kb';
    kb.innerHTML =
      `<span class="kb-fire" title="big-endian fireTime nanos">${esc(hexes.fire)}</span>` +
      `<span class="kb-join">‖</span>` +
      `<span class="kb-id" title="big-endian task id">${esc(hexes.id)}</span>`;

    const foot = document.createElement('span');
    foot.className = 'ent-foot';
    const dec = document.createElement('span');
    dec.className = 'ent-decoded';
    dec.textContent = `${fmtClock(e.fireTime)} · task ${nfmt(e.taskId)}`;
    foot.append(dec, valueTag(e));

    li.append(kb, foot);
    return li;
  }

  li.className = 'ent';
  const keyBox = document.createElement('span');
  keyBox.className = 'ent-key';

  if (typeof e.taskId === 'number') {
    keyBox.innerHTML = `<span class="kb"><span class="kb-id">${esc(nfmt(e.taskId))}</span></span>`;
    // The label is only worth a second line when it says something the key does
    // not — "task 1048587" under 1,048,587 is noise.
    if (e.label && e.label !== `task ${e.taskId}`) {
      const d = document.createElement('span');
      d.className = 'ent-decoded';
      d.textContent = e.label;
      keyBox.appendChild(d);
    }
  } else {
    keyBox.innerHTML = `<span class="kb">${esc(e.label ?? '(no key)')}</span>`;
  }

  li.append(keyBox, valueTag(e));
  return li;
}

function emptyLine(text) {
  const p = document.createElement('p');
  p.className = 'empty';
  p.textContent = text;
  return p;
}

/* ── wiring ────────────────────────────────────────────────────────────── */

el.btnRun.addEventListener('click', runWorkflow);
el.btnRun10.addEventListener('click', () => runBatch(BATCH_SIZES[0]));
el.btnRun100.addEventListener('click', () => runBatch(BATCH_SIZES[1]));
el.btnSwitch.addEventListener('click', startSwitch);
el.btnReset.addEventListener('click', askReset);
el.btnResetCancel.addEventListener('click', cancelReset);
el.btnResetGo.addEventListener('click', startReset);
el.btnClearLog.addEventListener('click', () => { el.log.textContent = ''; });

document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape' && state.confirmReset) cancelReset();
});

document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    clearTimeout(pollTimer);           // stop polling while the tab is hidden
  } else {
    scheduleTick(0);                   // and catch up the moment it is shown
  }
});

window.addEventListener('beforeunload', () => { try { es && es.close(); } catch { /* ignore */ } });

el.footMode.innerHTML = MOCK
  ? '<span class="mock-flag">mock mode — no backend, canned data</span>'
  : 'live';

render();
renderStreamPill();
connectEvents();
tick();
