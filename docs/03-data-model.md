# Data model: Cassandra's shape mapped onto Aerospike's

This is the living document. When the model changes during implementation, it changes here first.

## The problem in one picture

Cassandra gives Temporal three things Aerospike does not have:

| Cassandra | Aerospike equivalent | Consequence |
|---|---|---|
| Partition key we choose (`shard_id`) | none — partition = 12 bits of the key digest | cannot co-locate a shard's records |
| Clustering key with server-side ordering | none across records | no ordered range scan |
| Single-partition conditional (LWT) batch | MRT — but across *any* records | actually **more** general |

So the mapping is: **the clustering key becomes a map key inside a record, and the partition
becomes a bucketed set of records.**

```mermaid
flowchart LR
    subgraph C["Cassandra — one partition, clustered"]
        direction TB
        cp["partition key<br/><b>shard_id = 42</b>"]
        c1["task_id 1000"]
        c2["task_id 1001"]
        c3["task_id 5000"]
        cp --- c1 --- c2 --- c3
    end

    subgraph A["Aerospike — bucketed records, ordered inside"]
        direction TB
        b0["record htask/<b>42:2:0</b><br/>K-ordered map 't'<br/>{ 1000 → blob, 1001 → blob }"]
        b1["record htask/<b>42:2:1</b><br/>K-ordered map 't'<br/>{ 5000 → blob }"]
        b0 --- b1
    end

    C ==>|"bucket = taskID >> 12<br/>map key = taskID"| A
```

Bucket index is monotonic in the key, and the server orders within a bucket, so reading buckets in
order yields a globally ordered stream. That is the whole trick.

## Sets

Namespace `temporal`. Aerospike creates sets implicitly on first write, so there is no DDL.

| Set | Primary key | Bins |
|---|---|---|
| `shard` | `<shardID>` | `range_id` int, `info` blob, `enc` |
| `exec` | `<shardID>:<nsID>:<wfID>:<runID>` | `info`, `state`, `next_id`, `ver`, `csum`; K-ordered maps `act` `tmr` `chld` `cncl` `sig` `chasm`; lists `sigreq` `buf` |
| `curr` | `<shardID>:<nsID>:<wfID>` | `run_id`, `state`, `status`, `lwv`, `start_time`, `req_ids` |
| `htask` | `<shardID>:<categoryID>:<bucket>` | K-ordered map `t` |
| `hnode` | `<treeID>:<branchID>:<nodeID>:<txnID>` | `events` blob, `enc` |
| `hbranch` | `<treeID>:<branchID>` | `info` blob + K-ordered map `idx`: `nodeID → txnID` |
| `htree` | `<treeID>` | K-ordered map `branchID → branch info` |
| `tq` | `<nsID>:<name>:<type>` | `range_id`, `info`, `user_data`, `ud_version` |
| `task` | `<nsID>:<name>:<type>:<bucket>` | K-ordered map `t`: `taskID → task blob` |
| `ns` | `<nsID>` and `name:<name>` | namespace record and name→id pointer |
| `nsmeta` | `metadata` | `notification_version` |
| `clustermeta`, `membership`, `queue`, `queuev2`, `nexus`, `schema` | — | direct translations |

## Map key encoding

Two orderings exist in Temporal's task model, and both must be exact.

```mermaid
flowchart TB
    subgraph IM["Immediate categories — transfer, visibility, outbound"]
        i1["Cassandra: ORDER BY task_id"]
        i2["Aerospike map key: <b>int64 taskID</b>"]
        i1 --> i2
    end
    subgraph SC["Scheduled categories — timers"]
        s1["Cassandra: ORDER BY (visibility_ts, task_id)"]
        s2["Aerospike map key:<br/><b>16-byte BLOB</b><br/>bigendian(fireTimeNanos) ‖ bigendian(taskID)"]
        s3["BYTES keys compare bytewise,<br/>and big-endian encoding of a<br/>non-negative int64 is order-preserving"]
        s1 --> s2 --> s3
    end
```

## Bucketing

Bucketing does two jobs at once, and both are load-bearing:

1. **Size.** Every Aerospike update rewrites the whole record. An unbucketed shard queue would
   become a multi-megabyte record rewritten on every enqueue.
2. **Contention.** `transaction-pending-limit` (default 20) means a single "shard 42 task queue"
   record would be the hottest key in the system and would start returning `KEY_BUSY`.

| Category kind | Bucket function |
|---|---|
| Immediate | `bucket = taskID >> 12` (4096 tasks per bucket) |
| Scheduled | `bucket = fireTime` truncated to 1 minute |

Both are first guesses, to be tuned against measurement in Iteration 3 — see
[04-open-questions.md](04-open-questions.md).

**Range read** `[min, max)` → compute the covering bucket span, then per bucket issue
`MapGetByKeyRangeOp`, in bucket order, stopping once the page is full.
**Range delete** → `MapRemoveByKeyRangeOp` per bucket, then durable-delete emptied buckets.
**Pagination token** → our own encoding of `(bucket, lastKey)`. Temporal treats the token as
opaque, so we define the format.

## Atomicity: `UpdateWorkflowExecution`

The call that decides whether the approach works. In Cassandra it is one Paxos batch carrying five
conditions. Here it is one MRT whose commit-time read verification supplies the same fencing.

```mermaid
sequenceDiagram
    autonumber
    participant H as history service
    participant S as aerospike ExecutionStore
    participant DB as Aerospike (SC namespace)

    H->>S: UpdateWorkflowExecution(rangeID, mutation, newTasks)
    S->>DB: Txn begin
    S->>DB: Get shard/<shardID>
    Note over S: range_id mismatch → ShardOwnershipLostError
    S->>DB: Get curr/<shard:ns:wf>
    Note over S: run_id mismatch → CurrentWorkflowConditionFailedError
    S->>DB: Operate exec/... — ver CAS + sparse MapPut / MapRemove
    S->>DB: Operate htask/... — MapPutItems per bucket
    S->>DB: Commit
    DB-->>S: verify recorded read versions

    alt verification passes
        DB-->>S: committed
        S-->>H: ok
    else concurrent writer moved the lease or the record
        DB-->>S: aborted
        S->>DB: re-read to classify the conflict
        S-->>H: ShardOwnershipLost / WorkflowConditionFailed
    end
```

Two rules fall out of this:

- **Reads must be point or batch reads.** Queries and scans silently ignore `policy.Txn`, so any
  read participating in the transaction has to address known keys. This is another reason the
  bucketed-CDT model wins over a secondary index: bucket keys are computable, index results are not.
- **Sparse deltas, never read-modify-write.** `UpsertActivityInfos` / `DeleteActivityInfos` arrive
  as sparse maps; the store never sees the full collection. They apply as `MapPutItemsOp` /
  `MapRemoveByKeyOp` within a single `Operate`.

## Error mapping

| Aerospike condition | Temporal error |
|---|---|
| `CREATE_ONLY` write on existing shard record | `ShardAlreadyExistError` |
| `range_id` mismatch, or MRT abort on the shard record | `ShardOwnershipLostError` |
| `ver` (db record version) mismatch | `WorkflowConditionFailedError{NextEventID, DBRecordVersion}` |
| `curr` run-id / state mismatch | `CurrentWorkflowConditionFailedError` (7 fields) |
| task-queue `range_id` mismatch | `ConditionFailedError` |
| `KEY_BUSY` (14), `MRT_BLOCKED` (120) | retryable — surface as `Unavailable` |
| `MRT_EXPIRED` (122) | `TimeoutError` |

Aerospike does not hand back the conflicting record the way a Cassandra LWT does, so each failure
path performs an explicit read-back to populate these. `CurrentWorkflowConditionFailedError` needs
seven fields, so that read-back is not optional.

## Client policy defaults

| Policy | Value | Why |
|---|---|---|
| `CommitLevel` | `COMMIT_ALL` | mandatory in an SC namespace |
| `ReadModeSC` | `SESSION`, `LINEARIZE` for shard-ownership reads | task-queue reads must not lose tasks |
| `MaxRetries` | `0` on conditional writes | a blind retry of a non-idempotent write is a correctness bug |
| `DurableDelete` | `true` for every delete inside an MRT | required by the transaction contract |
| `RecordExistsAction` | matched per call site | `CREATE_ONLY` for insert-if-absent, `REPLACE` for full overwrite |
