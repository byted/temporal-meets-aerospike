# Open questions

Each entry: what is unknown, why it matters, and the experiment that settles it. Resolutions are
appended with a date, not overwritten.

---

## Q1 — Per-task TTL has no direct equivalent

**Status:** open · raised 2026-09-07

Cassandra sets a **per-task** TTL (`USING TTL ?`) on matching tasks. Aerospike TTL is
**per-record**, and our tasks are map entries inside a bucket record, so entries cannot expire
individually.

**Plan:** store the expiry timestamp in the task value and rely on `CompleteTasksLessThan` plus
range delete for reclamation, with a whole-bucket TTL as a backstop. Note the namespace runs
`nsup-period 0` (NSUP off), so a positive record TTL would be rejected — enabling bucket TTL means
enabling NSUP, which interacts with durable deletes and transactions.

**Experiment:** run `TaskQueueTaskSuite` and check whether any assertion depends on entry-level
expiry rather than explicit completion.

---

## Q2 — Bucket sizing is a guess

**Status:** open · raised 2026-09-07

`taskID >> 12` (4096 entries) and 1-minute timer buckets were chosen by reasoning, not measurement.
Too large → multi-MiB records rewritten on every enqueue, and `KEY_BUSY` under concurrency. Too
small → a range read fans out across many records.

**Experiment:** Iteration 2 load run. Record p50/p99 record size per `htask` bucket, the
`err_rw_pending_limit` counter, and the bucket count touched per `GetHistoryTasks`. Tune in
Iteration 3.

---

## Q3 — Mutable state record size vs. the 8 MiB ceiling

**Status:** open · raised 2026-09-07

One `exec` record holds execution info, state, six collection bins and buffered events. The hard
ceiling is 8 MiB, and **every update rewrites the whole record**. Temporal's own
`TransactionSizeLimit` config is the natural cap, but the interaction is unverified.

**Experiment:** instrument record size in `ExecutionMutableStateSuite`; assert against a threshold
well below 8 MiB. Confirm what Temporal does when `TransactionSizeLimitError` is returned.

---

## Q4 — MRT cost is undocumented

**Status:** open · raised 2026-09-07

No official figure exists. Mechanically it is ≥3 extra round trips (monitor-record write, commit
verify batch, roll-forward). If the overhead is large, Iteration 3 should collapse the calls that
touch a single record onto plain `Operate`, which needs no transaction.

**Experiment:** micro-benchmark `Operate` vs. a 3-record MRT against the local node before the
Iteration 2 load run, so the load numbers can be interpreted.

---

## Q5 — Reading back the conflicting record after an aborted MRT

**Status:** open · raised 2026-09-07

`CurrentWorkflowConditionFailedError` carries seven fields describing the *conflicting* record.
Cassandra gets them free from the LWT's returned `previous` map. We must re-read after the abort,
which opens an ABA window: the record may have changed again between abort and read-back.

**Options:** (a) re-read and accept the race, since Temporal retries the whole operation anyway;
(b) read the `curr` record *before* opening the transaction and carry the snapshot forward.

**Experiment:** `ExecutionMutableStateSuite` has explicit current-workflow-conflict cases. Start
with (a); if assertions on the returned fields are flaky, switch to (b).

---

## Resolved

### R9 — Aerospike does not store the user key by default

*Resolved 2026-09-07, Phase 1.*

`Key.Value()` returns nil on a record read back from a scan unless the write set `sendKey`. The
first `DeleteNamespaceByName` implementation recovered a namespace id that way and silently got an
empty string, orphaning the id record -- caught by `TestDeleteNamespace`, which then saw
`Unavailable` instead of `NamespaceNotFound`.

Rule for the rest of the store: **never recover identity from a record's key.** Carry any field
you need to read back in a bin. Enabling `sendKey` would also work but costs storage on every
record for the benefit of a handful of administrative paths.

### R8 — Value conditions need filter expressions, not generation CAS

*Resolved 2026-09-07, Phase 1.* See [03-data-model.md](03-data-model.md#conditional-writes-filter-expressions-not-generation-cas).

Generation CAS would produce spurious `ShardOwnershipLostError` because generation moves on every
write, while Temporal re-calls `UpdateShard` with an unchanged `rangeID`.
`WritePolicy.FilterExpression` + `FILTERED_OUT` expresses the actual condition.

### R7 — Cluster membership does not need Aerospike TTLs

*Resolved 2026-09-07, Phase 1.*

Membership records are the only part of the operational store with an expiry. Since the namespace
runs `nsup-period 0`, a positive record TTL would be rejected outright -- and enabling NSUP would
drag in interactions with durable deletes and transactions that the docs advise against.

Instead each record stores its own expiry timestamp, reads filter on it, and
`PruneClusterMembership` deletes what has lapsed. Temporal calls prune on a timer, and its own
suite (`TestClusterMembershipUpsertExpiresCorrectly`) drives prune explicitly and allows five
seconds -- so nothing depends on server-side expiry. All seven membership tests pass.

This is the same shape as the answer to [Q1](#q1--per-task-ttl-has-no-direct-equivalent), which
increases confidence in that plan.

### R1 — Can the store live outside the Temporal tree? — **Yes**

*Resolved 2026-09-07, Phase 0.*

Verified by compiling, against `go.temporal.io/server v1.31.2`, an external type that satisfies
`persistenceclient.AbstractDataStoreFactory` and is accepted by
`temporal.WithCustomDataStoreFactory()`. No fork is needed for the store itself. Temporal's
functional suite (`tests/`) remains un-importable and is the one thing that will need a patched
checkout, in Iteration 2.

### R2 — Can Aerospike reproduce `ORDER BY (visibility_ts, task_id)`? — **Yes**

*Resolved 2026-09-07, research.*

Secondary indexes cannot: they are explicitly unordered. K-ordered map CDTs can, and
[CDT ordering](https://aerospike.com/docs/server/guide/data-types/cdt-ordering) documents that
`BYTES` keys compare bytewise. A 16-byte big-endian `fireTime ‖ taskID` blob key therefore sorts
exactly as Cassandra's compound clustering key does.

### R6 — Does `max-record-size 8M` parse at namespace scope? — **Yes**

*Resolved 2026-09-07, Phase 0.*

Aerospike 8.1.2.4 starts cleanly with it in the `namespace` stanza. A config error would have been
fatal and immediate.

### R5 — Architecture: three layers, keep them straight

*Resolved 2026-09-07, Phase 0.*

There are three separate architecture decisions in this project and they are easy to conflate:

| Layer | What runs there | Architecture |
|---|---|---|
| Host tools | `go test ./conformance`, `go vet`, the capability tests | `darwin/arm64` — native, follows the Mac |
| Containers | Aerospike, Elasticsearch, the UI, init containers | `linux/<arch>`, chosen by Docker |
| Our server binary | built *inside* a container by `deploy/Dockerfile` | inherits that container's arch |

On Apple Silicon, Docker Desktop's Linux VM is itself **arm64**; amd64 images run translated
inside it. So a native arm64 container is the default — *unless* `DOCKER_DEFAULT_PLATFORM` is set
in the shell, which overrides the host default for every container with no warning. That was the
case here, and it is a reasonable thing to have set globally for other projects.

Measured cost of getting it wrong: `asinfo -v build` took **6.0s** emulated vs **0.33s** native.

Mitigated by `platform: ${TMA_PLATFORM:-}` on **every** service in
`deploy/docker-compose.yml` plus `deploy/.env.example`. Applied uniformly on purpose — a stack
with an arm64 database and an emulated amd64 server works fine and quietly produces nonsense
numbers. All six images publish both architectures, so either choice is valid as long as it is
consistent.

Harmless in Iteration 1, **fatal to Iteration 2** — any throughput number measured under
emulation is meaningless. See [05](05-iteration-2-3-sketch.md).

### R4 — Do MRT, generation CAS and CDT ordering behave as documented? — **Yes, all verified**

*Resolved 2026-09-07, Phase 0.*

`store/aerospike/capability_test.go` asserts each of these against a live node, so a wrong edition,
server version, or namespace mode fails loudly instead of surfacing as a subtle bug later:

| Assertion | Result |
|---|---|
| Namespace reports `strong-consistency=true`, `unavailable_partitions=0` | pass |
| MRT across 3 records commits atomically | pass |
| MRT abort leaves **no** record behind | pass |
| `EXPECT_GEN_EQUAL` rejects a stale-generation write with `GENERATION_ERROR` | pass |
| K-ordered int map range is begin-inclusive, end-exclusive, ascending | pass |
| **16-byte big-endian `fireTime‖taskID` BLOB keys sort as `(fireTime, taskID)`** | pass |

The last row is the one the timer-task model stands on, and it is now measured rather than
inferred from documentation.

### R3 — Is single-node strong consistency with RF1 allowed? — **Yes**

*Resolved 2026-09-07, research.*

[Docs](https://aerospike.com/docs/database/manage/namespace/consistency) state SC requires cluster
size ≥ replication factor and explicitly instruct setting `replication-factor 1` for a single-node
deployment. The roster must still be staged and the cluster reclustered before the namespace
serves anything — handled by the `roster-init` container.
