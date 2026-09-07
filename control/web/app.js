/* =============================================================================
 * Temporal on Aerospike — demo control plane
 *
 * Vanilla ES module. No build step, no dependencies. Served by the Go control
 * service from a go:embed FS at "/".
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
const SSE_RETRY_MS     = 2000;   // manual reconnect delay once EventSource gives up
const MAX_RUNS         = 12;
const MAX_LOG          = 300;
const RECORD_LIMIT     = 25;

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
  hintRun:       $('hint-run'),
  runsBody:      $('runs-body'),
  runsEmpty:     $('runs-empty'),

  btnSwitch:     $('btn-switch'),
  hintSwitch:    $('hint-switch'),
  log:           $('log'),
  btnClearLog:   $('btn-clear-log'),

  statReach:     $('stat-reach'),
  statSc:        $('stat-sc'),
  statDead:      $('stat-dead'),
  statUnavail:   $('stat-unavail'),
  statObjects:   $('stat-objects'),
  asNotice:      $('as-notice'),

  sets:          $('sets'),
  setsEmpty:     $('sets-empty'),

  records:       $('records'),
  recordsSet:    $('records-set'),
  recordsDesc:   $('records-desc'),
  recordsCount:  $('records-count'),
  recordsList:   $('records-list'),
  btnCloseRecs:  $('btn-close-records'),

  footMode:      $('foot-mode'),
};

/* ── state ─────────────────────────────────────────────────────────────── */

const state = {
  // from /api/state
  backend:       null,      // "sqlite" | "aerospike" | null (unknown)
  temporalReady: false,
  switching:     false,     // server-reported
  namespace:     null,

  // local
  serverUp:      null,      // null = never talked to it yet
  localSwitch:   false,     // we asked for a switch; server may not report it yet
  expectDown:    false,     // downtime is expected -> not an error
  running:       false,     // a workflow run is in flight
  switchPending: false,     // the POST /api/switch itself is in flight

  // aerospike
  health:        null,
  healthErr:     null,
  sets:          [],
  setsErr:       null,

  selectedSet:   null,
  records:       null,      // null = not loaded yet, [] = genuinely empty
  recordsErr:    null,
  recordsBusy:   false,

  runs:          [],
  streamStatus:  'connecting',
};

const isSwitching = () => state.switching || state.localSwitch;
const onAerospike = () => state.backend === 'aerospike';

/* ── tiny helpers ──────────────────────────────────────────────────────── */

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

const nfmt = (n) => (typeof n === 'number' && isFinite(n) ? n.toLocaleString() : '—');

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

/* Bins holding serialized protos are opaque — never pretend they are text. */
const OPAQUE_RE = /blob|byte|proto|binary/i;
const isOpaque = (type) => OPAQUE_RE.test(String(type ?? ''));

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
  const [h, s] = await Promise.allSettled([
    api('/api/aerospike/health', { timeout: 4000 }),
    api('/api/aerospike/sets',   { timeout: 4000 }),
  ]);

  if (h.status === 'fulfilled') { state.health = h.value; state.healthErr = null; }
  else { state.health = null; state.healthErr = h.reason?.message || String(h.reason); }

  if (s.status === 'fulfilled') {
    state.sets = Array.isArray(s.value) ? s.value : [];
    state.setsErr = null;
  } else {
    state.sets = [];
    state.setsErr = s.reason?.message || String(s.reason);
  }

  if (state.selectedSet) await refreshRecords(state.selectedSet, { quiet: true });
}

async function refreshRecords(name, { quiet = false } = {}) {
  if (!quiet) { state.recordsBusy = true; state.recordsErr = null; render(); }
  try {
    const r = await api(
      `/api/aerospike/records?set=${encodeURIComponent(name)}&limit=${RECORD_LIMIT}`,
      { timeout: 6000 },
    );
    if (state.selectedSet !== name) return; // selection moved on while we waited
    state.records    = Array.isArray(r) ? r : [];
    state.recordsErr = null;
  } catch (err) {
    if (state.selectedSet !== name) return;
    if (!quiet) { state.records = null; state.recordsErr = err.message || String(err); }
  } finally {
    state.recordsBusy = false;
    if (!quiet) render();
  }
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
    pushRun({
      ok:         true,
      workflowId: r?.workflowId ?? '(no id)',
      runId:      r?.runId ?? '',
      result:     r?.result ?? '(no result)',
      durationMs: typeof r?.durationMs === 'number' ? r.durationMs : Math.round(performance.now() - t0),
    });
    log('progress', `Workflow completed on ${backendLabel(state.backend)}: ${r?.result ?? ''}`);
  } catch (err) {
    pushRun({
      ok:         false,
      workflowId: '—',
      runId:      '',
      result:     err.message || String(err),
      durationMs: Math.round(performance.now() - t0),
    });
    log('error', `Workflow failed: ${err.message || err}`);
  } finally {
    state.running = false;
    render();
    scheduleTick(150); // let the object counts move straight away
  }
}

function pushRun(run) {
  state.runs.unshift({ ...run, backend: state.backend || 'unknown', at: new Date(), fresh: true });
  state.runs = state.runs.slice(0, MAX_RUNS);
}

async function startSwitch() {
  if (isSwitching() || onAerospike() || state.switchPending) return;

  state.switchPending = true;
  state.localSwitch   = true;
  state.expectDown    = true;
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

function selectSet(name) {
  if (state.selectedSet === name) return closeRecords();
  state.selectedSet = name;
  state.records     = null;
  state.recordsErr  = null;
  refreshRecords(name);
  render();
  el.records.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeRecords() {
  state.selectedSet = null;
  state.records     = null;
  state.recordsErr  = null;
  render();
}

/* ── render ────────────────────────────────────────────────────────────── */

function render() {
  renderHero();
  renderStepper();
  renderActions();
  renderRuns();
  renderHealth();
  renderSets();
  renderRecords();
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
  const runsOnAerospike = state.runs.filter((r) => r.backend === 'aerospike').length;
  let now;
  if (isSwitching())                        now = 3;
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
  el.btnRun.disabled = runBlocked || state.running;
  el.btnRun.dataset.busy = String(state.running);
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
  el.btnSwitch.dataset.busy = String(switchBusy);
  el.btnSwitch.querySelector('.btn-label').textContent =
    onAerospike() ? 'Running on Aerospike' : switchBusy ? 'Switching…' : 'Switch to Aerospike';

  if (onAerospike() && !switchBusy) {
    hint(el.hintSwitch, 'ok', 'Done. Temporal is persisting to Aerospike — run the workflow again.');
  } else if (switchBusy) {
    hint(el.hintSwitch, 'warn', 'Reconfiguring. This takes tens of seconds and the server restarts.');
  } else if (state.serverUp === false) {
    hint(el.hintSwitch, 'err', 'Control plane unreachable.');
  } else {
    hint(el.hintSwitch, '', 'Rewrites the persistence config, restarts Temporal, and reconnects.');
  }
}

function hint(node, tone, text) {
  if (tone) node.dataset.tone = tone; else delete node.dataset.tone;
  node.textContent = text;
}

let runsSig = '';
function renderRuns() {
  const sig = state.runs.map((r) => `${r.at.getTime()}:${r.backend}:${r.ok}`).join('|');
  el.runsEmpty.hidden = state.runs.length > 0;
  if (sig === runsSig) return;
  runsSig = sig;

  el.runsBody.textContent = '';
  for (const r of state.runs) {
    const tr = document.createElement('tr');
    if (r.fresh) { tr.className = 'run-row-fresh'; r.fresh = false; }
    tr.innerHTML = `
      <td class="c-store"><span class="store-tag" data-b="${esc(r.backend)}">${esc(backendLabel(r.backend))}</span></td>
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
    notice = 'Reconfiguring. Sets appear as Temporal writes its first records.';
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

let setsSig = '';
function renderSets() {
  const sets = state.sets;
  const sig = JSON.stringify(sets) + '|' + state.selectedSet;
  el.setsEmpty.hidden = sets.length > 0;
  if (sets.length === 0) {
    el.setsEmpty.textContent = state.setsErr
      ? `Could not list sets: ${state.setsErr}`
      : onAerospike()
        ? 'No sets yet. Aerospike creates them implicitly on first write.'
        : 'No sets yet — Temporal has not written to Aerospike.';
  }
  if (sig === setsSig) return;
  setsSig = sig;

  el.sets.textContent = '';
  for (const s of sets) {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'set-card';
    b.dataset.selected = String(state.selectedSet === s.name);
    b.dataset.set = s.name;
    b.innerHTML = `
      <span class="set-top">
        <span class="set-name">${esc(s.name)}</span>
        <span class="set-count">${esc(nfmt(s.objects))}<span class="unit">obj</span></span>
      </span>
      <span class="set-desc">${esc(s.description || 'No description supplied by the API.')}</span>`;
    b.addEventListener('click', () => selectSet(s.name));
    el.sets.appendChild(b);
  }
}

let recordsSig = '';
function renderRecords() {
  if (!state.selectedSet) {
    el.records.hidden = true;
    recordsSig = '';
    return;
  }
  el.records.hidden = false;
  el.recordsSet.textContent = state.selectedSet;

  const meta = state.sets.find((s) => s.name === state.selectedSet);
  el.recordsDesc.textContent = meta?.description || '';

  const n = state.records?.length ?? 0;
  el.recordsCount.textContent = state.recordsBusy
    ? 'loading…'
    : state.records === null ? ''
    : `showing ${n}${n >= RECORD_LIMIT ? ` of ${nfmt(meta?.objects)}` : ''}`;

  const sig = JSON.stringify({ s: state.selectedSet, r: state.records, e: state.recordsErr, b: state.recordsBusy });
  if (sig === recordsSig) return;
  recordsSig = sig;

  el.recordsList.textContent = '';

  if (state.recordsErr) {
    el.recordsList.appendChild(emptyLine(`Could not read records: ${state.recordsErr}`));
    return;
  }
  if (state.records === null) {
    el.recordsList.appendChild(emptyLine('Loading records…'));
    return;
  }
  if (state.records.length === 0) {
    el.recordsList.appendChild(emptyLine(
      `No records in ${state.selectedSet} yet — the set exists but holds nothing right now.`));
    return;
  }

  for (const rec of state.records) el.recordsList.appendChild(recordCard(rec));
}

function emptyLine(text) {
  const p = document.createElement('p');
  p.className = 'empty';
  p.textContent = text;
  return p;
}

function recordCard(rec) {
  const art = document.createElement('article');
  art.className = 'rec';

  const head = document.createElement('header');
  head.className = 'rec-head';
  head.innerHTML = `
    <code class="rec-key">${esc(rec.key ?? '(no key)')}</code>
    <span class="rec-digest">digest ${esc(rec.digest ?? '—')}</span>`;
  art.appendChild(head);

  const bins = document.createElement('div');
  bins.className = 'bins';
  for (const bin of rec.bins ?? []) {
    const opaque = isOpaque(bin.type);
    const row = document.createElement('div');
    row.className = 'bin';
    row.innerHTML = `
      <span class="bin-name">${esc(bin.name)}</span>
      <span class="type-tag" data-opaque="${opaque}">${esc(bin.type ?? '?')}</span>
      <span class="bin-size">${esc(fmtBytes(bin.size))}</span>
      <span class="bin-preview" data-opaque="${opaque}"></span>`;

    // Previews are server-supplied strings — set as text, never as markup.
    const prev = row.querySelector('.bin-preview');
    prev.textContent = bin.preview ?? '';
    if (opaque) {
      const note = document.createElement('span');
      note.className = 'opaque-note';
      note.textContent = 'serialized proto — held as bytes, not decoded';
      prev.appendChild(note);
    }
    bins.appendChild(row);
  }
  if (!(rec.bins ?? []).length) bins.appendChild(emptyLine('No bins returned for this record.'));
  art.appendChild(bins);
  return art;
}

/* ── wiring ────────────────────────────────────────────────────────────── */

el.btnRun.addEventListener('click', runWorkflow);
el.btnSwitch.addEventListener('click', startSwitch);
el.btnCloseRecs.addEventListener('click', closeRecords);
el.btnClearLog.addEventListener('click', () => { el.log.textContent = ''; });

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
