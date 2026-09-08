/* =============================================================================
 * Offline mock for the control plane UI.
 *
 * Loaded only when the page is opened with ?mock=1 — the real deployment never
 * parses this file. It replaces window.fetch and window.EventSource with a small
 * in-memory simulation of the API contract, including:
 *
 *   - a scripted "switch to Aerospike" that takes ~16s and drops the server
 *     mid-way, so the reconnect / expected-downtime paths get exercised
 *   - a scripted "reset" that wipes the world and goes back to SQLite, dropping
 *     the server the same way
 *   - a bucket layout for /api/aerospike/history-tasks with more than one shard,
 *     immediate and scheduled categories, several buckets each, a drained
 *     (count 0) bucket, and a bucket whose entry list is capped
 *
 * Everything the endpoint returns is already in order — shards, categories,
 * buckets and entries — exactly as the real API promises.
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

/* ── bucketing, mirrored from store/aerospike/tasks.go ─────────────────── */

const IMMEDIATE_SHIFT         = 12;          // 4096 task ids per bucket
const SCHEDULED_BUCKET_SECONDS = 60;         // one bucket per minute
const ENTRY_CAP                = 6;          // what the API returns per bucket

/* Fixed at load so repeated polls return stable timestamps rather than a
   clock that visibly crawls under the viewer. */
const MINUTE0 = Math.floor(Date.now() / 60000) - 2;

/* ── the bucket layout ─────────────────────────────────────────────────── */

/* Entries in an immediate bucket: map key is the int64 task id itself. */
function immediateEntries(bucket, offsets) {
  const base = bucket * 2 ** IMMEDIATE_SHIFT;
  return offsets.map((off, i) => {
    const taskId = base + off;
    return { label: `task ${taskId}`, taskId, fireTime: null, bytes: 132 + ((i * 17) % 61) };
  });
}

/* Entries in a scheduled bucket: map key is BE(fireTime)||BE(taskID), so the
   pair is what determines order. Seconds stay inside the bucket's minute. */
function scheduledEntries(bucket, specs) {
  const baseMs = bucket * SCHEDULED_BUCKET_SECONDS * 1000;
  return specs.map(([sec, taskId], i) => {
    const fire = new Date(baseMs + sec * 1000);
    return {
      label: `${fire.toISOString()} · task ${taskId}`,
      taskId,
      fireTime: fire.toISOString(),
      bytes: 148 + ((i * 23) % 47),
    };
  });
}

function bucketOf(shardId, catId, bucket, count, entries) {
  return {
    bucket,
    recordKey: `${shardId}:${catId}:${bucket}`,
    count,
    entries: entries.slice(0, ENTRY_CAP),
  };
}

/* A layout rich enough to be worth looking at:
 *
 *   shard 1 · transfer   three buckets, the middle one drained
 *   shard 1 · timer      two minutes of timers, second one drained
 *   shard 3 · transfer   a hot bucket well past the entry cap, then a fresh one
 *   shard 3 · timer      one minute of timers
 *   shard 3 · visibility a single small bucket
 *
 * Counts grow as workflows are run, so the view moves during the demo.
 */
function historyTasks() {
  const n = world.runs;

  const shard1Transfer = (() => {
    const b0 = immediateEntries(256, [11, 12, 13, 27]);
    const b2 = immediateEntries(258, [4, 5, 6, 9, 14, 15, 21, 40]);
    return {
      id: 1, name: 'transfer', scheduled: false,
      buckets: [
        bucketOf(1, 1, 256, b0.length, b0),
        bucketOf(1, 1, 257, 0, []),                       // drained, not retired
        bucketOf(1, 1, 258, Math.min(3 + n, b2.length), b2.slice(0, Math.min(3 + n, b2.length))),
      ],
    };
  })();

  const shard1Timer = {
    id: 2, name: 'timer', scheduled: true,
    buckets: [
      (() => {
        const e = scheduledEntries(MINUTE0, [[3, 1048591], [17, 1048604]]);
        return bucketOf(1, 2, MINUTE0, e.length, e);
      })(),
      bucketOf(1, 2, MINUTE0 + 1, 0, []),                 // drained, not retired
      (() => {
        const e = scheduledEntries(MINUTE0 + 2, [
          [0, 1056771], [6, 1056772], [6, 1056918], [31, 1057004], [58, 1057130],
        ]);
        return bucketOf(1, 2, MINUTE0 + 2, e.length, e);
      })(),
    ],
  };

  const shard3Transfer = (() => {
    const hot   = immediateEntries(1024, [0, 1, 2, 3, 4, 5, 6, 7]);
    const fresh = immediateEntries(1025, [2, 3, 8, 19]);
    return {
      id: 1, name: 'transfer', scheduled: false,
      buckets: [
        bucketOf(3, 1, 1024, 1042 + n * 3, hot),          // far past the entry cap
        bucketOf(3, 1, 1025, fresh.length, fresh),
      ],
    };
  })();

  const shard3Timer = {
    id: 2, name: 'timer', scheduled: true,
    buckets: [
      (() => {
        const e = scheduledEntries(MINUTE0 + 1, [[12, 4194329], [12, 4194330], [44, 4194411]]);
        return bucketOf(3, 2, MINUTE0 + 1, e.length, e);
      })(),
    ],
  };

  const shard3Visibility = (() => {
    const e = immediateEntries(1024, [9, 22]);
    return {
      id: 4, name: 'visibility', scheduled: false,
      buckets: [bucketOf(3, 4, 1024, e.length, e)],
    };
  })();

  return {
    bucketing: {
      immediateShift: IMMEDIATE_SHIFT,
      scheduledBucketSeconds: SCHEDULED_BUCKET_SECONDS,
    },
    shards: [
      { shardId: 1, categories: [shard1Transfer, shard1Timer] },
      { shardId: 3, categories: [shard3Transfer, shard3Timer, shard3Visibility] },
    ],
  };
}

/* Objects in the namespace: a plausible number that climbs with the demo. */
const totalObjects = () => 26 + world.runs * 14;

/* ── deterministic fake ids ────────────────────────────────────────────── */

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

/* ── the scripted reset ────────────────────────────────────────────────── */

const RESET_STEPS = [
  [   0, 'progress', 'Stopping the demo worker'],
  [ 900, 'progress', 'Truncating Aerospike sets — htask, htaskidx, exec, curr, hnode, hbranch, htree, task, tq, ns'],
  [2600, 'progress', 'Durable deletes flushed — namespace "temporal" is empty'],
  [3200, 'state',    'Rewriting persistence config: default store -> sqlite'],
  [3600, 'progress', 'Restarting temporal-server on the built-in SQLite store…'],
  // server goes away here
  [8200, 'progress', 'temporal-server up — frontend, history, matching, worker'],
  [8900, 'progress', 'Namespace "demo" registered'],
  [9400, 'state',    'Persistence store is sqlite. Demo is back at step 1.'],
];

const RESET_DOWN_FROM = 3900;
const RESET_DOWN_TO   = 7900;

function runReset() {
  world.switching = true;
  world.temporalReady = false;

  for (const [at, type, msg] of RESET_STEPS) setTimeout(() => emit(type, msg), T(at));

  setTimeout(() => { world.serverDown = true; dropAllStreams(); }, T(RESET_DOWN_FROM));
  setTimeout(() => { world.serverDown = false; }, T(RESET_DOWN_TO));

  setTimeout(() => {
    world.backend = 'sqlite';
    world.switching = false;
    world.temporalReady = true;
    world.runs = 0;
  }, T(9400));
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

  if (path === '/api/reset' && method === 'POST') {
    if (world.switching) return json({ error: 'switch in progress' }, 409);
    runReset();
    return new Response('', { status: 202 });
  }

  if (path === '/api/workflow/run' && method === 'POST') {
    if (world.switching) return json({ error: 'switch in progress' }, 503);
    await sleep(500 + Math.random() * 700);
    world.runSeq++;
    const store = world.backend;
    if (store === 'aerospike') world.runs++;
    return json({
      workflowId: `hello-workflow-${String(world.runSeq).padStart(3, '0')}`,
      runId: `${digest(world.runSeq).slice(0, 8)}-4b2f-4c11-9a7e-${digest(world.runSeq * 3).slice(0, 12)}`,
      // The activity itself names the store it ran on.
      result: `Hello world, from ${store === 'aerospike' ? 'Aerospike' : 'SQLite'}`,
      durationMs: 420 + Math.floor(Math.random() * 380),
      persistenceStore: store,
    });
  }

  if (path === '/api/workflow/run-batch' && method === 'POST') {
    if (world.switching) return json({ error: 'switch in progress' }, 503);
    let count = 100;
    try { count = JSON.parse(init.body || '{}').count || 100; } catch { /* default */ }

    // 10 at a time on the server, so roughly count/10 rounds. Compressed here
    // so the harness stays usable.
    await sleep(1200 + Math.random() * 600);
    world.runSeq += count;
    const store = world.backend;
    if (store === 'aerospike') world.runs += count;
    return json({
      requested: count,
      completed: count,
      failed: 0,
      durationMs: 1800 + Math.floor(Math.random() * 900),
      fastestMs: 38 + Math.floor(Math.random() * 20),
      slowestMs: 380 + Math.floor(Math.random() * 200),
      persistenceStore: store,
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

  if (path === '/api/aerospike/history-tasks') {
    if (world.backend !== 'aerospike') {
      return json({
        bucketing: {
          immediateShift: IMMEDIATE_SHIFT,
          scheduledBucketSeconds: SCHEDULED_BUCKET_SECONDS,
        },
        shards: [],
      });
    }
    return json(historyTasks());
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
