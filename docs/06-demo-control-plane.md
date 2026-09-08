# The demo control plane

A hosted, clickable demonstration of the thing this project set out to prove: that Temporal's
operational persistence can be moved onto Aerospike, and that a workflow does not care which store
is underneath it.

The narrative it exists to support, in order:

1. Temporal is running on **SQLite** — its own built-in dev store.
2. Run a workflow with an activity. It completes.
3. Switch the persistence backend to **Aerospike**.
4. Run the same workflow again. It completes identically.
5. Browse the data now sitting in Aerospike and see Temporal's model in it.

## Shape

```mermaid
flowchart TB
    user["browser"] --> ing["Traefik ingress"]
    ing -->|"/"| cp["control-plane<br/><i>Go service, embedded UI</i>"]
    ing -->|"/temporal"| tui["temporal-ui"]

    cp -->|"1 · PATCH TEMPORAL_ENVIRONMENT"| k8s[("k8s API<br/><i>ServiceAccount:<br/>get/patch one Deployment</i>")]
    k8s -->|"rolling restart"| tmp["temporal<br/><i>one image, two configs</i>"]
    cp -->|"2 · ensure namespace<br/>3 · host worker, run workflow"| tmp
    cp -->|"4 · scan sets, read records"| aero[("Aerospike<br/>SC namespace")]

    tmp -.->|"env=sqlite"| lite[("SQLite<br/>emptyDir")]
    tmp -.->|"env=aerospike"| aero
```

## Why the switch is cheap

Two facts, both verified rather than assumed, collapse what looked like the hard part:

**One binary already serves both stores.** `cmd/temporal-aerospike-server` links the SQLite plugin
*and* registers the Aerospike factory, and Temporal's `DataStoreFactoryProvider` dispatches on the
**shape of the config** — `SQL != nil` versus `CustomDataStoreConfig != nil` — not on a build flag.

**The selection is an environment variable.** A single ConfigMap holds `sqlite.yaml` and
`aerospike.yaml`; the container picks one with `TEMPORAL_ENVIRONMENT`, which the binary already
reads. So switching is a patch of one env var on one Deployment, and Kubernetes performs the
rolling restart itself. No ConfigMap surgery, no orchestrated stop/start, no second image.

**SQLite needs no infrastructure at all.** It self-provisions its schema when the connect attribute
`setup` is `"true"`, so the baseline store is a file on an `emptyDir` — no container, no init job.

## The switch sequence

Naive versions of this are broken in a way that only shows up after the first switch:

```mermaid
sequenceDiagram
    participant UI as browser
    participant CP as control-plane
    participant K8 as k8s API
    participant T as temporal

    UI->>CP: POST /api/switch {aerospike}
    CP->>CP: stop the worker
    CP->>K8: patch TEMPORAL_ENVIRONMENT
    K8->>T: rolling restart
    CP->>K8: wait for rollout + pod ready
    Note over CP,T: The new store is EMPTY --<br/>the demo namespace does not exist
    CP->>T: register namespace
    CP->>CP: restart the worker
    CP-->>UI: progress at every step (SSE)
```

Skip the namespace registration and "Run workflow" fails immediately after every switch, with an
error that points nowhere useful.

## The first run after a switch is slow, so the switch absorbs it

Measured on the deployed box: the first workflow after a switch took **43 s**, against ~50 ms
steady state. It does not happen every time, which makes it worse — the demo's whole claim is that
the workflow behaves identically on either store, and an unexplained 40-second hang after clicking
**Run workflow** reads as broken rather than as a cold start.

The cause is a task queue that matching has just had to create while a poller was already
long-polling it. Rather than leave it to chance, `runSwitch` executes one throwaway workflow before
reporting the switch complete.

The trade-off is explicit: **the switch goes from ~15 s to ~69 s**, and the presenter's next click
is ~45 ms. The same wall-clock time is spent either way; this spends it against a live progress log,
where waiting is expected, instead of behind a button that is supposed to feel instant.

If a fast switch matters more than a fast first click, drop the warm-up block in
`control/server.go` — it is deliberately self-contained, and a warm-up failure never fails the
switch.

## Deliberate scope

- **No data migration.** Switching stores means the previous store's workflows are simply not there.
  That is the honest behaviour, and saying so is part of the demo.
- **No Elasticsearch.** Visibility runs on SQLite in both modes, one file per backend. This isolates
  the variable being demonstrated — only the operational store changes — and takes the box from
  ~6 GB to ~2.5 GB. `deploy/docker-compose.yml` still shows the Elasticsearch pairing for the
  production-shaped setup.
- **`sendKey` is on.** The store normally never stores the user key (see R9 in
  [04-open-questions.md](04-open-questions.md)), so a record browser could only show digests. The
  demo enables it so records read as `3:<namespace-id>:<workflow-id>:<run-id>`. It remains off by
  default, and no store code reads the key back.

## Components

| Piece | Lives in | Does |
|---|---|---|
| Control plane | `cmd/control-plane`, `control/` | Orchestration, Temporal worker, Aerospike browser, HTTP + SSE |
| Web UI | `control/web/` | Plain HTML/JS/CSS, no build step, embedded via `go:embed` |
| Manifests | `deploy/k8s/` | k3s: Aerospike StatefulSet, Temporal, UI, control plane, RBAC, ingress |
| Install guide | `deploy/k8s/README.md` | Fresh box to running demo |

The control plane's Kubernetes permissions are deliberately minimal: `get`/`patch` on one
Deployment and `get`/`list` on Pods, in one namespace. Choosing k3s over Docker Compose was
primarily about this — the Compose equivalent would have meant mounting the Docker socket into an
internet-facing web app, which is root on the host.

## Running it

Locally, against the Compose stack:

```sh
docker compose -f deploy/docker-compose.yml up -d
temporal operator namespace create --namespace demo --retention 24h

TEMPORAL_ADDRESS=127.0.0.1:7233 AEROSPIKE_HOST=127.0.0.1:3000 \
AEROSPIKE_NAMESPACE=temporal PORT=8090 go run ./cmd/control-plane
```

Everything works except the switch, which needs Kubernetes. The UI also runs standalone against
canned data with `?mock=1` — see `control/web/mock/`.

On a box: [deploy/k8s/README.md](../deploy/k8s/README.md), which documents the Civo deployment
including k3s install, native image builds, both credentials, and TLS via cert-manager.

Deployed at `https://temporal-meets-aerospike.example.com` behind basic auth, with a Let's
Encrypt certificate and an HTTP-to-HTTPS redirect ahead of the auth middleware, so credentials are
never requested over plaintext.
