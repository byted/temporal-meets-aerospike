package aerospike

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// History tasks are the one place Aerospike's model diverges sharply from
// Cassandra's, so the reasoning lives here.
//
// Cassandra stores every task for a shard in the shard's partition, ordered by
// a clustering key, and answers "tasks in [min, max) ascending" with a single
// range scan. Aerospike has no clustering key and its secondary indexes are
// explicitly unordered, so there is no equivalent scan.
//
// What Aerospike does guarantee is ordering *inside* a K-ordered map. So the
// clustering key becomes a map key and the partition becomes a set of bucketed
// records: htask/<shard>:<category>:<bucket>, each holding a K-ordered map of
// taskKey -> task blob.
//
// Bucketing does two jobs. It caps record size -- every Aerospike update
// rewrites the whole record, so an unbucketed shard queue would rewrite
// megabytes per enqueue -- and it spreads writes so one shard's queue is not a
// single hot key hitting transaction-pending-limit.
const (
	setHistoryTask    = "htask"
	setHistoryTaskIdx = "htaskidx"
	binTaskMap        = "t"
	binBucketSet      = "b"

	// immediateBucketShift buckets immediate categories at 4096 task ids each.
	immediateBucketShift = 12

	// scheduledBucketNanos buckets scheduled categories by fire time, one
	// bucket per minute.
	scheduledBucketNanos = int64(60 * 1e9)
)

// taskKeyCodec encodes a tasks.Key into an Aerospike map key that sorts the way
// the category requires.
//
// Immediate categories order by task id alone, so an int64 key suffices.
//
// Scheduled categories order by (fire time, task id). Aerospike has no compound
// key, but CDT ordering compares BYTES keys bytewise, and big-endian encoding of
// a non-negative int64 is order-preserving -- so a 16-byte fireTime||taskID blob
// reproduces Cassandra's (visibility_ts, task_id) clustering order exactly.
// This is verified empirically in capability_test.go.
type taskKeyCodec struct {
	scheduled bool
}

func codecFor(category tasks.Category) taskKeyCodec {
	return taskKeyCodec{scheduled: category.Type() == tasks.CategoryTypeScheduled}
}

func (c taskKeyCodec) encode(key tasks.Key) any {
	if !c.scheduled {
		return key.TaskID
	}
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(key.FireTime.UnixNano()))
	binary.BigEndian.PutUint64(b[8:16], uint64(key.TaskID))
	return b
}

// bucket returns the record a key belongs to. Bucket indices are monotonic in
// the sort key, so reading buckets in order yields a globally ordered stream.
func (c taskKeyCodec) bucket(key tasks.Key) int64 {
	if !c.scheduled {
		return key.TaskID >> immediateBucketShift
	}
	return key.FireTime.UnixNano() / scheduledBucketNanos
}

func (k *keyBuilder) historyTaskKey(shardID int32, categoryID int, bucket int64) (*as.Key, error) {
	return k.newKey(setHistoryTask, fmt.Sprintf("%d:%d:%d", shardID, categoryID, bucket))
}

// historyTaskIndexKey names the record listing which buckets actually hold
// tasks for a (shard, category).
//
// This index is not an optimisation, it is a requirement. A caller may ask for
// the whole key space -- Temporal does exactly that when draining a queue --
// and the numeric bucket span of [0, MaxInt64) is 2^51 buckets, which no loop
// can walk. The set of *populated* buckets is tiny by comparison, so range
// operations consult this first and touch only those.
func (k *keyBuilder) historyTaskIndexKey(shardID int32, categoryID int) (*as.Key, error) {
	return k.newKey(setHistoryTaskIdx, fmt.Sprintf("%d:%d", shardID, categoryID))
}

// populatedBuckets returns the buckets in [first, last] that hold tasks,
// in ascending order.
func (s *executionStore) populatedBuckets(
	ctx context.Context, shardID int32, categoryID int, first, last int64,
) ([]int64, error) {
	if last < first {
		return nil, nil
	}
	key, err := s.client.keys.historyTaskIndexKey(shardID, categoryID)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
		// End-exclusive, so last+1 includes the final bucket.
		as.MapGetByKeyRangeOp(binBucketSet, first, last+1, as.MapReturnType.KEY))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, convertError("populatedBuckets", err)
	}

	raw, _ := rec.Bins[binBucketSet].([]any)
	out := make([]int64, 0, len(raw))
	for _, v := range raw {
		if b, ok := asInt64(v); ok {
			out = append(out, b)
		}
	}
	return out, nil
}

// writeTasks stores tasks as part of the caller's transaction. Grouping by
// bucket keeps this to one Operate per touched record rather than one per task,
// which matters against the 4096-write cap on a transaction.
func (s *executionStore) writeTasks(
	ctx context.Context,
	t *txnScope,
	shardID int32,
	byCategory map[tasks.Category][]p.InternalHistoryTask,
) error {
	for category, taskList := range byCategory {
		if len(taskList) == 0 {
			continue
		}
		codec := codecFor(category)

		// Grouped as operations rather than a map: a scheduled task's encoded
		// key is a []byte, which Go cannot use as a map key at all.
		grouped := make(map[int64][]*as.Operation)
		for _, task := range taskList {
			bucket := codec.bucket(task.Key)
			grouped[bucket] = append(grouped[bucket],
				as.MapPutOp(kOrderedMap, binTaskMap, codec.encode(task.Key), blobValue(task.Blob)))
		}

		indexKey, err := s.client.keys.historyTaskIndexKey(shardID, category.ID())
		if err != nil {
			return err
		}

		for bucket, ops := range grouped {
			key, err := s.client.keys.historyTaskKey(shardID, category.ID(), bucket)
			if err != nil {
				return err
			}
			// One Operate per bucket, carrying every task destined for it --
			// the point of bucketing is round trips per bucket, not per task.
			if _, err := s.client.as.Operate(withCtxW(ctx, t.write), key, ops...); err != nil {
				return convertError("writeTasks", err)
			}
			// Register the bucket so range reads can find it without walking
			// the numeric span.
			if _, err := s.client.as.Operate(withCtxW(ctx, t.write), indexKey,
				as.MapPutOp(kOrderedMap, binBucketSet, bucket, 1)); err != nil {
				return convertError("writeTasks", err)
			}
		}
	}
	return nil
}

// AddHistoryTasks writes tasks outside a mutable-state change, fenced on the
// shard lease exactly as the execution paths are.
func (s *executionStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	t := s.begin()
	defer t.finish()

	if err := s.assertShardOwnership(ctx, t, request.ShardID, request.RangeID); err != nil {
		return err
	}
	if err := s.writeTasks(ctx, t, request.ShardID, request.Tasks); err != nil {
		return err
	}
	return t.commit("AddHistoryTasks")
}

// bucketRange returns the inclusive span of buckets covering [min, max).
//
// Bucket indices are monotonic in the sort key, so reading buckets in ascending
// order and concatenating their in-bucket ranges yields one globally ordered
// stream -- which is what replaces Cassandra's single clustered range scan.
func (c taskKeyCodec) bucketRange(minKey, maxKey tasks.Key) (int64, int64) {
	first := c.bucket(minKey)
	// The max key is exclusive, so a max landing exactly on a bucket boundary
	// contributes nothing and the previous bucket is the last one to read.
	last := c.bucket(maxKey)
	if c.boundaryIsExclusiveStart(maxKey) {
		last--
	}
	if last < first {
		last = first - 1 // empty range
	}
	return first, last
}

// boundaryIsExclusiveStart reports whether maxKey sits exactly at the start of
// its bucket, in which case that bucket contributes no entries.
func (c taskKeyCodec) boundaryIsExclusiveStart(maxKey tasks.Key) bool {
	if !c.scheduled {
		return maxKey.TaskID&((1<<immediateBucketShift)-1) == 0
	}
	return maxKey.FireTime.UnixNano()%scheduledBucketNanos == 0
}

// taskPageToken records where a page stopped: the bucket, and the last key read
// within it. Temporal treats the token as opaque, so the format is ours.
type taskPageToken struct {
	Bucket  int64  `json:"b"`
	LastKey []byte `json:"k"`
}

func encodeTaskPageToken(bucket int64, lastKey []byte) ([]byte, error) {
	return json.Marshal(taskPageToken{Bucket: bucket, LastKey: lastKey})
}

func decodeTaskPageToken(raw []byte) (*taskPageToken, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var tok taskPageToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, serviceerror.NewInternalf("invalid history task page token: %v", err)
	}
	return &tok, nil
}

func (s *executionStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	codec := codecFor(request.TaskCategory)

	token, err := decodeTaskPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	firstBucket, lastBucket := codec.bucketRange(request.InclusiveMinTaskKey, request.ExclusiveMaxTaskKey)
	if token != nil && token.Bucket > firstBucket {
		firstBucket = token.Bucket
	}

	buckets, err := s.populatedBuckets(ctx, request.ShardID, request.TaskCategory.ID(), firstBucket, lastBucket)
	if err != nil {
		return nil, err
	}

	batchSize := request.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	minKey := codec.encodeValue(request.InclusiveMinTaskKey)
	maxKey := codec.encodeValue(request.ExclusiveMaxTaskKey)

	resp := &p.InternalGetHistoryTasksResponse{}

	// The page token records the last key *emitted*, not the next one pending.
	// Recording the pending key instead silently drops it on resume, because
	// the resume filter skips everything at or before the token.
	var lastEmittedBucket int64
	var lastEmittedKey []byte

	for _, bucket := range buckets {
		key, err := s.client.keys.historyTaskKey(request.ShardID, request.TaskCategory.ID(), bucket)
		if err != nil {
			return nil, err
		}

		// Server-side ordered range read: begin-inclusive, end-exclusive,
		// exactly Temporal's [InclusiveMinTaskKey, ExclusiveMaxTaskKey).
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapGetByKeyRangeOp(binTaskMap, minKey, maxKey, as.MapReturnType.KEY_VALUE))
		if err != nil {
			if isNotFound(err) {
				continue // no tasks ever written to this bucket
			}
			return nil, convertError("GetHistoryTasks", err)
		}

		entries := recordMapPairs(rec, binTaskMap)
		for _, entry := range entries {
			// Resume within a partially-consumed bucket.
			if token != nil && bucket == token.Bucket &&
				codec.compareEncoded(entry.Key, token.LastKey) <= 0 {
				continue
			}

			if len(resp.Tasks) == batchSize {
				resp.NextPageToken, err = encodeTaskPageToken(lastEmittedBucket, lastEmittedKey)
				if err != nil {
					return nil, err
				}
				return resp, nil
			}

			taskKey, ok := codec.decode(entry.Key)
			if !ok {
				continue
			}
			blob := blobFromValue(entry.Value)
			if blob == nil {
				continue
			}
			resp.Tasks = append(resp.Tasks, p.InternalHistoryTask{Key: taskKey, Blob: blob})
			lastEmittedBucket = bucket
			lastEmittedKey = codec.encodedBytes(entry.Key)
		}
	}

	return resp, nil
}

func (s *executionStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	codec := codecFor(request.TaskCategory)
	key, err := s.client.keys.historyTaskKey(
		request.ShardID, request.TaskCategory.ID(), codec.bucket(request.TaskKey))
	if err != nil {
		return err
	}

	_, err = s.client.as.Operate(withCtxW(ctx, s.client.write), key,
		as.MapRemoveByKeyOp(binTaskMap, codec.encode(request.TaskKey), as.MapReturnType.NONE))
	if err != nil && !isNotFound(err) {
		return convertError("CompleteHistoryTask", err)
	}
	return nil
}

// RangeCompleteHistoryTasks deletes every task in [min, max).
//
// Cassandra does this with a single range tombstone. Aerospike has no range
// delete across records, so this issues one server-side MapRemoveByKeyRange per
// covered bucket -- still one round trip per bucket rather than per task, which
// is the point of bucketing.
func (s *executionStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	codec := codecFor(request.TaskCategory)
	firstBucket, lastBucket := codec.bucketRange(request.InclusiveMinTaskKey, request.ExclusiveMaxTaskKey)

	buckets, err := s.populatedBuckets(ctx, request.ShardID, request.TaskCategory.ID(), firstBucket, lastBucket)
	if err != nil {
		return err
	}

	minKey := codec.encodeValue(request.InclusiveMinTaskKey)
	maxKey := codec.encodeValue(request.ExclusiveMaxTaskKey)

	indexKey, err := s.client.keys.historyTaskIndexKey(request.ShardID, request.TaskCategory.ID())
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		key, err := s.client.keys.historyTaskKey(request.ShardID, request.TaskCategory.ID(), bucket)
		if err != nil {
			return err
		}
		// Delete and report what is left, so an emptied bucket can be retired
		// from the index rather than being re-read forever.
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapRemoveByKeyRangeOp(binTaskMap, minKey, maxKey, as.MapReturnType.NONE),
			as.MapSizeOp(binTaskMap))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return convertError("RangeCompleteHistoryTasks", err)
		}
		// Two operations targeted the same bin, so the client returns their
		// results as a list; the size is the last element.
		if remaining, ok := lastOpResultInt(rec, binTaskMap); ok && remaining == 0 {
			if err := s.retireBucket(ctx, key, indexKey, bucket); err != nil {
				return err
			}
		}
	}
	return nil
}

// retireBucket drops an emptied bucket record and its index entry.
//
// Best-effort: a bucket left behind costs one wasted read on the next range
// scan, which is not worth failing an otherwise successful deletion over.
func (s *executionStore) retireBucket(ctx context.Context, bucketKey, indexKey *as.Key, bucket int64) error {
	if _, err := s.client.as.Delete(withCtxW(ctx, s.client.delete), bucketKey); err != nil && !isNotFound(err) {
		return convertError("RangeCompleteHistoryTasks", err)
	}
	if _, err := s.client.as.Operate(withCtxW(ctx, s.client.write), indexKey,
		as.MapRemoveByKeyOp(binBucketSet, bucket, as.MapReturnType.NONE)); err != nil && !isNotFound(err) {
		return convertError("RangeCompleteHistoryTasks", err)
	}
	return nil
}

// encodeValue is encode with the exact type the range operations expect.
func (c taskKeyCodec) encodeValue(key tasks.Key) any { return c.encode(key) }

// decode reverses encode.
func (c taskKeyCodec) decode(raw any) (tasks.Key, bool) {
	if !c.scheduled {
		id, ok := asInt64(raw)
		if !ok {
			return tasks.Key{}, false
		}
		return tasks.NewImmediateKey(id), true
	}
	b := asBytes(raw)
	if len(b) != 16 {
		return tasks.Key{}, false
	}
	fireTime := int64(binary.BigEndian.Uint64(b[0:8]))
	taskID := int64(binary.BigEndian.Uint64(b[8:16]))
	return tasks.NewKey(time.Unix(0, fireTime).UTC(), taskID), true
}

// encodedBytes renders a map key as bytes for the page token, whichever form
// the client handed back.
func (c taskKeyCodec) encodedBytes(raw any) []byte {
	if !c.scheduled {
		id, _ := asInt64(raw)
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(id))
		return b
	}
	return asBytes(raw)
}

// compareEncoded orders two encoded keys, for resuming inside a bucket.
func (c taskKeyCodec) compareEncoded(raw any, tokenKey []byte) int {
	return bytes.Compare(c.encodedBytes(raw), tokenKey)
}

// lastOpResultInt reads the final result for a bin when an Operate carried
// several operations against it. The Aerospike client collapses a single
// operation to a scalar but returns a list when there is more than one, which
// is easy to trip over.
func lastOpResultInt(rec *as.Record, bin string) (int64, bool) {
	switch v := rec.Bins[bin].(type) {
	case []any:
		if len(v) == 0 {
			return 0, false
		}
		return asInt64(v[len(v)-1])
	default:
		return asInt64(v)
	}
}

// --- Replication DLQ ---
//
// The DLQ is a per-(shard, source cluster) queue of replication tasks that
// could not be applied. Cassandra stores it in the executions table under a
// reserved row type, using the source cluster name where a workflow id would
// go. Here it reuses the same bucketed-map machinery as history tasks, in its
// own set -- the access pattern is identical (ordered range read, range delete
// by task id), so there is no reason to model it differently.
//
// Single-cluster deployments never reach these paths, but Temporal's task suite
// exercises them.

const setReplicationDLQ = "dlq"

func (k *keyBuilder) dlqKey(shardID int32, sourceCluster string, bucket int64) (*as.Key, error) {
	return k.newKey(setReplicationDLQ, fmt.Sprintf("%d:%s:%d", shardID, sourceCluster, bucket))
}

func (k *keyBuilder) dlqIndexKey(shardID int32, sourceCluster string) (*as.Key, error) {
	return k.newKey(setReplicationDLQ, fmt.Sprintf("%d:%s:idx", shardID, sourceCluster))
}

// dlqCodec: replication tasks are ordered by task id alone, like any immediate
// category.
var dlqCodec = taskKeyCodec{scheduled: false}

func (s *executionStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) error {
	blob, err := s.serializer.ReplicationTaskInfoToBlob(request.TaskInfo)
	if err != nil {
		return convertError("PutReplicationTaskToDLQ", err)
	}

	taskID := request.TaskInfo.GetTaskId()
	bucket := dlqCodec.bucket(tasks.NewImmediateKey(taskID))

	key, err := s.client.keys.dlqKey(request.ShardID, request.SourceClusterName, bucket)
	if err != nil {
		return err
	}
	indexKey, err := s.client.keys.dlqIndexKey(request.ShardID, request.SourceClusterName)
	if err != nil {
		return err
	}

	if _, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
		as.MapPutOp(kOrderedMap, binTaskMap, taskID, blobValue(blob))); err != nil {
		return convertError("PutReplicationTaskToDLQ", err)
	}
	if _, err := s.client.as.Operate(withCtxW(ctx, s.client.write), indexKey,
		as.MapPutOp(kOrderedMap, binBucketSet, bucket, 1)); err != nil {
		return convertError("PutReplicationTaskToDLQ", err)
	}
	return nil
}

func (s *executionStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetReplicationTasksFromDLQResponse, error) {
	token, err := decodeTaskPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	firstBucket, lastBucket := dlqCodec.bucketRange(
		request.InclusiveMinTaskKey, request.ExclusiveMaxTaskKey)
	if token != nil && token.Bucket > firstBucket {
		firstBucket = token.Bucket
	}

	buckets, err := s.dlqPopulatedBuckets(ctx, request.ShardID, request.SourceClusterName, firstBucket, lastBucket)
	if err != nil {
		return nil, err
	}

	batchSize := request.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	minKey := dlqCodec.encodeValue(request.InclusiveMinTaskKey)
	maxKey := dlqCodec.encodeValue(request.ExclusiveMaxTaskKey)

	resp := &p.InternalGetReplicationTasksFromDLQResponse{}
	var lastEmittedBucket int64
	var lastEmittedKey []byte

	for _, bucket := range buckets {
		key, err := s.client.keys.dlqKey(request.ShardID, request.SourceClusterName, bucket)
		if err != nil {
			return nil, err
		}
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapGetByKeyRangeOp(binTaskMap, minKey, maxKey, as.MapReturnType.KEY_VALUE))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, convertError("GetReplicationTasksFromDLQ", err)
		}

		for _, entry := range recordMapPairs(rec, binTaskMap) {
			if token != nil && bucket == token.Bucket &&
				dlqCodec.compareEncoded(entry.Key, token.LastKey) <= 0 {
				continue
			}
			if len(resp.Tasks) == batchSize {
				resp.NextPageToken, err = encodeTaskPageToken(lastEmittedBucket, lastEmittedKey)
				if err != nil {
					return nil, err
				}
				return resp, nil
			}
			taskKey, ok := dlqCodec.decode(entry.Key)
			if !ok {
				continue
			}
			blob := blobFromValue(entry.Value)
			if blob == nil {
				continue
			}
			resp.Tasks = append(resp.Tasks, p.InternalHistoryTask{Key: taskKey, Blob: blob})
			lastEmittedBucket = bucket
			lastEmittedKey = dlqCodec.encodedBytes(entry.Key)
		}
	}
	return resp, nil
}

func (s *executionStore) dlqPopulatedBuckets(
	ctx context.Context, shardID int32, sourceCluster string, first, last int64,
) ([]int64, error) {
	if last < first {
		return nil, nil
	}
	key, err := s.client.keys.dlqIndexKey(shardID, sourceCluster)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
		as.MapGetByKeyRangeOp(binBucketSet, first, last+1, as.MapReturnType.KEY))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, convertError("dlqPopulatedBuckets", err)
	}
	raw, _ := rec.Bins[binBucketSet].([]any)
	out := make([]int64, 0, len(raw))
	for _, v := range raw {
		if b, ok := asInt64(v); ok {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *executionStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {
	bucket := dlqCodec.bucket(request.TaskKey)
	key, err := s.client.keys.dlqKey(request.ShardID, request.SourceClusterName, bucket)
	if err != nil {
		return err
	}
	_, err = s.client.as.Operate(withCtxW(ctx, s.client.write), key,
		as.MapRemoveByKeyOp(binTaskMap, dlqCodec.encode(request.TaskKey), as.MapReturnType.NONE))
	if err != nil && !isNotFound(err) {
		return convertError("DeleteReplicationTaskFromDLQ", err)
	}
	return nil
}

func (s *executionStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	firstBucket, lastBucket := dlqCodec.bucketRange(
		request.InclusiveMinTaskKey, request.ExclusiveMaxTaskKey)

	buckets, err := s.dlqPopulatedBuckets(ctx, request.ShardID, request.SourceClusterName, firstBucket, lastBucket)
	if err != nil {
		return err
	}

	minKey := dlqCodec.encodeValue(request.InclusiveMinTaskKey)
	maxKey := dlqCodec.encodeValue(request.ExclusiveMaxTaskKey)

	for _, bucket := range buckets {
		key, err := s.client.keys.dlqKey(request.ShardID, request.SourceClusterName, bucket)
		if err != nil {
			return err
		}
		_, err = s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapRemoveByKeyRangeOp(binTaskMap, minKey, maxKey, as.MapReturnType.NONE))
		if err != nil && !isNotFound(err) {
			return convertError("RangeDeleteReplicationTaskFromDLQ", err)
		}
	}
	return nil
}

// IsReplicationDLQEmpty asks whether *any* task exists at or after the given
// key. Note it deliberately ignores ExclusiveMaxTaskKey -- callers leave it
// zero-valued, and honouring it would make the range [0, 0) and the answer
// always "empty". Cassandra's implementation ignores it too.
func (s *executionStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	indexKey, err := s.client.keys.dlqIndexKey(request.ShardID, request.SourceClusterName)
	if err != nil {
		return false, err
	}

	firstBucket := dlqCodec.bucket(request.InclusiveMinTaskKey)

	// A nil range end means "to the end of the map" -- unbounded above.
	rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), indexKey,
		as.MapGetByKeyRangeOp(binBucketSet, firstBucket, nil, as.MapReturnType.KEY))
	if err != nil {
		if isNotFound(err) {
			return true, nil
		}
		return false, convertError("IsReplicationDLQEmpty", err)
	}

	raw, _ := rec.Bins[binBucketSet].([]any)
	minKey := dlqCodec.encodeValue(request.InclusiveMinTaskKey)

	for _, v := range raw {
		bucket, ok := asInt64(v)
		if !ok {
			continue
		}
		key, err := s.client.keys.dlqKey(request.ShardID, request.SourceClusterName, bucket)
		if err != nil {
			return false, err
		}
		bucketRec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapGetByKeyRangeOp(binTaskMap, minKey, nil, as.MapReturnType.KEY))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return false, convertError("IsReplicationDLQEmpty", err)
		}
		if entries, _ := bucketRec.Bins[binTaskMap].([]any); len(entries) > 0 {
			return false, nil
		}
	}
	return true, nil
}
