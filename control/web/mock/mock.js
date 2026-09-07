/* =============================================================================
 * Offline mock for the control plane UI.
 *
 * Loaded only when the page is opened with ?mock=1 — the real deployment never
 * parses this file. It replaces window.fetch and window.EventSource with a small
 * in-memory simulation of the API contract, including:
 *
 *   - a scripted "switch to Aerospike" that takes ~16s and drops the server
 *     mid-way, so the reconnect / expected-downtime paths get exercised
 *   - object counts that climb as workflows run
 *   - one set ("nexus") that stays empty, to exercise the zero-record view
 *
 * Preview:
 *     cd control/web && python3 -m http.server 8000
 *     open http://localhost:8000/?mock=1          # full-speed switch (~16s)
 *     open http://localhost:8000/?mock=1&fast=1   # compressed switch (~4s)
 * ========================================================================== */

const FAST = new URLSearchParams(location.search).get('fast') === '1';
const T = (ms) => Math.round(ms * (FAST ? 0.25 : 1));

/* ── simulated world ───────────────────────────────────────────────────── */

const world = {
  backend: 'sqlite',
  temporalReady: true,
  switching: false,
  namespace: 'demo',
  serverDown: false,   // control plane refusing connections (server restarting)
  runs: 0,             // workflows run since moving to aerospike
  runSeq: 0,
};

const SETS = [
  { name: 'shard',       base: 4, per: 0, description: 'Shard ownership leases. One record per history shard; range_id is the fencing token every conditional write is checked against.' },
  { name: 'exec',        base: 0, per: 1, description: 'Mutable workflow state — one record per run. Pending activities, timers, child workflows and signals live in ordered maps inside that single record.' },
  { name: 'curr',        base: 0, per: 1, description: 'Current-execution pointer. Maps a workflow ID to the run ID that is currently active, so a second start can be rejected.' },
  { name: 'htask',       base: 0, per: 6, description: 'History task queues — transfer, timer and visibility tasks, bucketed into K-ordered maps so a range scan becomes a walk over buckets.' },
  { name: 'hnode',       base: 0, per: 4, description: 'History events. Each record holds one batch of workflow history events as a serialized proto blob; this is the immutable log.' },
  { name: 'hbranch',     base: 0, per: 1, description: 'History branch index. A K-ordered map of node ID to transaction ID — the read path scans this first, then batch-gets only the page it needs.' },
  { name: 'htree',       base: 0, per: 1, description: 'History trees. Tracks the branches created by workflow resets and retries.' },
  { name: 'tq',          base: 2, per: 0, description: 'Task queue metadata: the range_id lease held by matching, plus worker user data and its version.' },
  { name: 'task',        base: 0, per: 2, description: 'Dispatchable tasks waiting for a worker, in bucketed K-ordered maps keyed by task ID.' },
  { name: 'ns',          base: 3, per: 0, description: 'Namespace registry, plus the name-to-ID pointer records that make lookup by name possible.' },
  { name: 'nsmeta',      base: 1, per: 0, description: 'Namespace metadata — a single record holding the global notification version.' },
  { name: 'clustermeta', base: 2, per: 0, description: 'Cluster metadata and membership records for this Temporal cluster.' },
  { name: 'nexus',       base: 0, per: 0, description: 'Nexus endpoint registry. Empty until a Nexus endpoint is created — a good example of a set that exists in the model but holds nothing.' },
];

const setCount = (s) => s.base + s.per * world.runs;
const totalObjects = () => SETS.reduce((n, s) => n + setCount(s), 0);

/* ── deterministic fake payloads ───────────────────────────────────────── */

function hexPreview(seed, bytes = 12) {
  let x = seed * 2654435761 % 4294967296;
  const out = [];
  for (let i = 0; i < bytes; i++) {
    x = (x * 1103515245 + 12345) % 4294967296;
    out.push(((x >>> 16) & 0xff).toString(16).padStart(2, '0'));
  }
  return `${out.join(' ')} …`;
}

const digest = (seed) => hexPreview(seed + 7777, 10).replace(/ |…/g, '').slice(0, 20);

const NS_ID  = 'a4f1c2d0-9e33-4b6a-8f21-6d0e5b7c1a99';
const TREE   = '3f7b1e02-55c4-4a7d-9d1b-2c8e40aa77b1';
const BRANCH = '9c0a6d18-1b42-4f83-bb27-70e5c3d92f04';

function recordsFor(set) {
  const n = setCount(set);
  if (n === 0) return [];
  const out = [];
  const cap = Math.min(n, 25);
  for (let i = 0; i < cap; i++) out.push(makeRecord(set.name, i));
  return out;
}

function makeRecord(name, i) {
  const s = i + 1;
  const wf  = `hello-workflow-${String((i % Math.max(world.runs, 1)) + 1).padStart(3, '0')}`;
  const run = `${digest(i * 3 + 1).slice(0, 8)}-4b2f-4c11-9a7e-${digest(i * 5 + 2).slice(0, 12)}`;
  const B = (nm, size, seed) => ({ name: nm, type: 'blob', preview: hexPreview(seed, 12), size });
  const I = (nm, v) => ({ name: nm, type: 'int', preview: String(v), size: 8 });
  const S = (nm, v) => ({ name: nm, type: 'str', preview: v, size: v.length });
  const M = (nm, entries, size) => ({ name: nm, type: 'map(k-ordered)', preview: entries, size });
  const L = (nm, v, size) => ({ name: nm, type: 'list', preview: v, size });

  switch (name) {
    case 'shard':
      return { key: String(i + 1), digest: digest(s), bins: [
        I('range_id', 12 + i), B('info', 486 + i * 13, s * 11), S('enc', 'Proto3') ] };

    case 'exec':
      return { key: `${(i % 4) + 1}:${NS_ID}:${wf}:${run}`, digest: digest(s * 2), bins: [
        B('info',  1204 + i * 37, s * 17),
        B('state', 3312 + i * 91, s * 23),
        I('next_id', 11 + i), I('ver', 3 + (i % 4)), I('csum', 1849302 + i),
        M('act',  '{ 5 → blob(214 B) }', 214),
        M('tmr',  '{ }', 0),
        M('chld', '{ }', 0),
        L('buf',  '[ ]', 0) ] };

    case 'curr':
      return { key: `${(i % 4) + 1}:${NS_ID}:${wf}`, digest: digest(s * 3), bins: [
        S('run_id', run), I('state', 2), I('status', 2), I('lwv', 7 + i),
        I('start_time', 1757251200000 + i * 1000), L('req_ids', '[ "req-1" ]', 24) ] };

    case 'htask':
      return { key: `${(i % 4) + 1}:${(i % 3) + 1}:${Math.floor(i / 3)}`, digest: digest(s * 5), bins: [
        M('t', `{ ${1048576 + i * 7} → blob(168 B), ${1048577 + i * 7} → blob(171 B) }`, 339) ] };

    case 'hnode':
      return { key: `${TREE}:${BRANCH}:${i + 1}:${1000 + i}`, digest: digest(s * 7), bins: [
        B('events', 742 + i * 211, s * 29), S('enc', 'Proto3') ] };

    case 'hbranch':
      return { key: `${TREE}:${BRANCH}`, digest: digest(s * 11), bins: [
        B('info', 312, s * 31),
        M('idx', '{ 0x0000000000000001…7ffffffffffffc17 → 1000, … }', 96) ] };

    case 'htree':
      return { key: TREE, digest: digest(s * 13), bins: [
        M('br', `{ ${BRANCH} → blob(288 B) }`, 288) ] };

    case 'tq':
      return { key: `${NS_ID}:hello-task-queue:${i + 1}`, digest: digest(s * 17), bins: [
        I('range_id', 3 + i), B('info', 214, s * 37), B('user_data', 96, s * 41), I('ud_version', 1) ] };

    case 'task':
      return { key: `${NS_ID}:hello-task-queue:1:${i}`, digest: digest(s * 19), bins: [
        M('t', `{ ${2097152 + i * 3} → blob(142 B) }`, 142) ] };

    case 'ns':
      return i === 0
        ? { key: NS_ID, digest: digest(s * 23), bins: [
            B('detail', 918, s * 43), I('notification_version', 4), S('name', 'demo') ] }
        : { key: `name:${i === 1 ? 'demo' : 'temporal-system'}`, digest: digest(s * 29), bins: [
            S('id', NS_ID) ] };

    case 'nsmeta':
      return { key: 'metadata', digest: digest(s * 31), bins: [ I('notification_version', 4) ] };

    case 'clustermeta':
      return { key: i === 0 ? 'active' : 'membership:history:1', digest: digest(s * 37), bins: [
        B('data', 462 + i * 20, s * 47), I('version', 2), S('enc', 'Proto3') ] };

    default:
      return { key: `${name}-${i}`, digest: digest(s * 41), bins: [ B('data', 128, s) ] };
  }
}

/* ── event bus (fake SSE) ──────────────────────────────────────────────── */

const listeners = new Set();

function emit(type, message) {
  const payload = JSON.stringify({ type, message, ts: new Date().toISOString() });
  for (const l of listeners) l(payload);
}

class MockEventSource {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;

  constructor(url) {
    this.url = url;
    this.readyState = MockEventSource.CONNECTING;
    this.onopen = this.onmessage = this.onerror = null;
    this._deliver = (data) => this.onmessage && this.onmessage({ data });

    setTimeout(() => {
      if (this.readyState === MockEventSource.CLOSED) return;
      if (world.serverDown) {
        // Server refusing connections: fail hard so the app schedules a retry.
        this.readyState = MockEventSource.CLOSED;
        this.onerror && this.onerror(new Event('error'));
        return;
      }
      this.readyState = MockEventSource.OPEN;
      listeners.add(this._deliver);
      this.onopen && this.onopen(new Event('open'));
    }, 120);
  }

  addEventListener(type, fn) { if (type === 'message') this.onmessage = fn; }

  close() {
    this.readyState = MockEventSource.CLOSED;
    listeners.delete(this._deliver);
  }

  _drop() {
    listeners.delete(this._deliver);
    this.readyState = MockEventSource.CLOSED;
    this.onerror && this.onerror(new Event('error'));
  }
}

const openStreams = new Set();

class TrackedEventSource extends MockEventSource {
  constructor(url) { super(url); openStreams.add(this); }
  close() { super.close(); openStreams.delete(this); }
}

/* Server went away: every open stream fails, exactly as it would on restart. */
function dropAllStreams() {
  for (const es of [...openStreams]) {
    openStreams.delete(es);
    if (es.readyState !== MockEventSource.CLOSED) es._drop();
  }
}

/* ── the scripted switch ───────────────────────────────────────────────── */

const STEPS = [
  [   0, 'progress', 'Draining in-flight workflow tasks…'],
  [1200, 'progress', 'Stopping temporal-server (sqlite)'],
  [2600, 'progress', 'Starting Aerospike EE 8.x — namespace "temporal", strong consistency'],
  [4200, 'progress', 'Staging roster: node BB9020011AC4202, partitions 4096'],
  [5400, 'progress', 'recluster — namespace serving'],
  [6400, 'progress', 'Visibility store untouched — only the operational store moves'],
  [7200, 'state',    'Rewriting persistence config: default store -> aerospike'],
  [7600, 'progress', 'Restarting temporal-server with the out-of-tree data store factory…'],
  // server goes away here
  [13500, 'progress', 'temporal-server up — frontend, history, matching, worker'],
  [14600, 'progress', 'Namespace "demo" registered'],
  [15400, 'state',    'Persistence store is aerospike. Ready.'],
];

const DOWN_FROM = 7900;
const DOWN_TO   = 13200;

function runSwitch() {
  world.switching = true;
  world.temporalReady = false;

  for (const [at, type, msg] of STEPS) setTimeout(() => emit(type, msg), T(at));

  setTimeout(() => { world.serverDown = true; dropAllStreams(); }, T(DOWN_FROM));
  setTimeout(() => { world.serverDown = false; }, T(DOWN_TO));

  setTimeout(() => {
    world.backend = 'aerospike';
    world.switching = false;
    world.temporalReady = true;
  }, T(15400));
}

/* ── fake fetch ────────────────────────────────────────────────────────── */

const json = (body, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

async function mockFetch(input, init = {}) {
  const url = new URL(typeof input === 'string' ? input : input.url, location.href);
  const path = url.pathname.replace(/\/+$/, '') || '/';
  const method = (init.method || 'GET').toUpperCase();

  // Anything the mock does not own falls through to the real network (the page
  // itself, style.css, …).
  if (!path.startsWith('/api/')) return realFetch(input, init);

  if (world.serverDown) {
    await sleep(60);
    throw new TypeError('Failed to fetch'); // exactly what a refused connection looks like
  }

  await sleep(40 + Math.random() * 60);

  if (path === '/api/state') {
    return json({
      backend: world.backend,
      temporalReady: world.temporalReady,
      switching: world.switching,
      namespace: world.namespace,
    });
  }

  if (path === '/api/switch' && method === 'POST') {
    if (world.switching || world.backend === 'aerospike') return json({ error: 'already switching' }, 409);
    runSwitch();
    return new Response('', { status: 202 });
  }

  if (path === '/api/workflow/run' && method === 'POST') {
    if (world.switching) return json({ error: 'switch in progress' }, 503);
    await sleep(500 + Math.random() * 700);
    world.runSeq++;
    if (world.backend === 'aerospike') world.runs++;
    return json({
      workflowId: `hello-workflow-${String(world.runSeq).padStart(3, '0')}`,
      runId: `${digest(world.runSeq).slice(0, 8)}-4b2f-4c11-9a7e-${digest(world.runSeq * 3).slice(0, 12)}`,
      result: `Hello world, from ${world.backend === 'aerospike' ? 'Aerospike' : 'SQLite'}`,
      durationMs: 420 + Math.floor(Math.random() * 380),
    });
  }

  if (path === '/api/aerospike/health') {
    const live = world.backend === 'aerospike' || world.switching;
    return json({
      reachable: true,
      strongConsistency: true,
      deadPartitions: 0,
      unavailablePartitions: 0,
      objects: live ? totalObjects() : 0,
    });
  }

  if (path === '/api/aerospike/sets') {
    if (world.backend !== 'aerospike') return json([]);
    return json(SETS.map((s) => ({ name: s.name, objects: setCount(s), description: s.description })));
  }

  if (path === '/api/aerospike/records') {
    const name = url.searchParams.get('set');
    const limit = Number(url.searchParams.get('limit')) || 25;
    const set = SETS.find((s) => s.name === name);
    if (!set || world.backend !== 'aerospike') return json([]);
    return json(recordsFor(set).slice(0, limit));
  }

  return json({ error: 'not found' }, 404);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let realFetch = null;

export function install() {
  realFetch = window.fetch.bind(window);
  window.fetch = mockFetch;
  window.EventSource = TrackedEventSource;
  console.info(`[mock] control-plane mock installed${FAST ? ' (fast switch)' : ''}`);
}
