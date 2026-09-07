package aerospike

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

// The matching task store: task queue metadata, the tasks themselves, and the
// per-queue user data that carries build-id versioning.
//
// Tasks reuse the bucketed K-ordered map + bucket index pattern that history
// tasks use, for the same reason: it is the only construct where Aerospike
// guarantees ordering, and the numeric key span is far larger than the
// populated set.
//
// Two flavours share the code. The classic store orders by task id alone; the
// fair store orders by (pass, task id), which becomes a 16-byte big-endian blob
// key exactly as scheduled history tasks do.
const (
	setTaskQueue     = "tq"
	setTask          = "task"
	setTaskIdx       = "taskidx"
	setTaskQueueData = "tqdata"
	setBuildIDIndex  = "buildidx"

	binExpiry     = "expiry"
	binUserData   = "udata"
	binUDVersion  = "ud_version"
	binQueueNames = "queues"

	// taskBucketShift buckets tasks at 4096 ids each, matching history tasks.
	taskBucketShift = 12
)

func (k *keyBuilder) taskQueueKey(namespaceID, name string, taskType enumspb.TaskQueueType) (*as.Key, error) {
	return k.newKey(setTaskQueue, fmt.Sprintf("%s:%s:%d", namespaceID, name, taskType))
}

func (k *keyBuilder) taskKey(namespaceID, name string, taskType enumspb.TaskQueueType, subqueue int, bucket int64) (*as.Key, error) {
	return k.newKey(setTask, fmt.Sprintf("%s:%s:%d:%d:%d", namespaceID, name, taskType, subqueue, bucket))
}

func (k *keyBuilder) taskIndexKey(namespaceID, name string, taskType enumspb.TaskQueueType, subqueue int) (*as.Key, error) {
	return k.newKey(setTaskIdx, fmt.Sprintf("%s:%s:%d:%d", namespaceID, name, taskType, subqueue))
}

func (k *keyBuilder) taskQueueUserDataKey(namespaceID, name string) (*as.Key, error) {
	return k.newKey(setTaskQueueData, fmt.Sprintf("%s:%s", namespaceID, name))
}

func (k *keyBuilder) buildIDIndexKey(namespaceID, buildID string) (*as.Key, error) {
	return k.newKey(setBuildIDIndex, fmt.Sprintf("%s:%s", namespaceID, buildID))
}

// --- task key encoding ---

// matchingTaskCodec mirrors taskKeyCodec but over (pass, taskID) rather than
// (fireTime, taskID). Pass is only used by the fair store.
type matchingTaskCodec struct{ fair bool }

func (c matchingTaskCodec) encode(pass, taskID int64) any {
	if !c.fair {
		return taskID
	}
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(pass))
	binary.BigEndian.PutUint64(b[8:16], uint64(taskID))
	return b
}

func (c matchingTaskCodec) bucket(taskID int64) int64 { return taskID >> taskBucketShift }

// --- task queue metadata ---

func (s *taskStore) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	key, err := s.client.keys.taskQueueKey(request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		return err
	}

	bins := append(
		blobBins(binData, binEncoding, request.TaskQueueInfo),
		as.NewBin(binRangeID, request.RangeID),
		as.NewBin(binExpiry, expiryNanos(request.ExpiryTime)),
	)

	// Cassandra expresses this as INSERT ... IF NOT EXISTS.
	if err := s.client.as.PutBins(withCtxW(ctx, s.client.create), key, bins...); err != nil {
		if isKeyExists(err) {
			return &p.ConditionFailedError{
				Msg: fmt.Sprintf("task queue %q already exists", request.TaskQueue),
			}
		}
		return convertError("CreateTaskQueue", err)
	}
	return nil
}

func (s *taskStore) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (*p.InternalGetTaskQueueResponse, error) {
	key, err := s.client.keys.taskQueueKey(request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key)
	if err != nil {
		if isNotFound(err) {
			return nil, serviceerror.NewNotFoundf("task queue %q not found", request.TaskQueue)
		}
		return nil, convertError("GetTaskQueue", err)
	}
	return &p.InternalGetTaskQueueResponse{
		RangeID:       binInt64(rec, binRangeID),
		TaskQueueInfo: readBlob(rec, binData, binEncoding),
	}, nil
}

func (s *taskStore) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	key, err := s.client.keys.taskQueueKey(request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		return nil, err
	}

	bins := append(
		blobBins(binData, binEncoding, request.TaskQueueInfo),
		as.NewBin(binRangeID, request.RangeID),
		as.NewBin(binExpiry, expiryNanos(request.ExpiryTime)),
	)

	policy := s.client.condWrite(rangeIDEquals(request.PrevRangeID))
	if err := s.client.as.PutBins(withCtxW(ctx, policy), key, bins...); err != nil {
		if isFilteredOut(err) || isNotFound(err) {
			return nil, &p.ConditionFailedError{
				Msg: fmt.Sprintf("task queue %q: expected range_id=%d",
					request.TaskQueue, request.PrevRangeID),
			}
		}
		return nil, convertError("UpdateTaskQueue", err)
	}
	return &p.UpdateTaskQueueResponse{}, nil
}

func (s *taskStore) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	key, err := s.client.keys.taskQueueKey(
		request.TaskQueue.NamespaceID, request.TaskQueue.TaskQueueName, request.TaskQueue.TaskQueueType)
	if err != nil {
		return err
	}

	policy := *s.client.delete
	policy.FilterExpression = rangeIDEquals(request.RangeID)

	if _, err := s.client.as.Delete(withCtxW(ctx, &policy), key); err != nil {
		if isFilteredOut(err) {
			return &p.ConditionFailedError{
				Msg: fmt.Sprintf("task queue %q: expected range_id=%d",
					request.TaskQueue.TaskQueueName, request.RangeID),
			}
		}
		if isNotFound(err) {
			return nil
		}
		return convertError("DeleteTaskQueue", err)
	}
	return nil
}

// --- tasks ---

func (s *taskStore) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	tqKey, err := s.client.keys.taskQueueKey(request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		return nil, err
	}

	codec := matchingTaskCodec{fair: s.fair}

	t := s.begin()
	defer t.finish()

	// Fence on the task queue's range id, the same optimistic lock the shard
	// lease uses. Reading it inside the transaction means a concurrent owner
	// change aborts the whole write.
	rec, err := s.client.as.Get(withCtx(ctx, t.read), tqKey, binRangeID)
	if err != nil {
		if isNotFound(err) {
			return nil, &p.ConditionFailedError{
				Msg: fmt.Sprintf("task queue %q not found", request.TaskQueue),
			}
		}
		return nil, convertError("CreateTasks", err)
	}
	if actual := binInt64(rec, binRangeID); actual != request.RangeID {
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("task queue %q: expected range_id=%d, found %d",
				request.TaskQueue, request.RangeID, actual),
		}
	}

	// Group by (subqueue, bucket) so each record takes one Operate.
	type bucketRef struct {
		subqueue int
		bucket   int64
	}
	grouped := make(map[bucketRef][]*as.Operation)
	for _, task := range request.Tasks {
		ref := bucketRef{subqueue: task.Subqueue, bucket: codec.bucket(task.TaskId)}
		grouped[ref] = append(grouped[ref],
			as.MapPutOp(kOrderedMap, binTaskMap,
				codec.encode(task.TaskPass, task.TaskId),
				taskValue(task)))
	}

	for ref, ops := range grouped {
		key, err := s.client.keys.taskKey(
			request.NamespaceID, request.TaskQueue, request.TaskType, ref.subqueue, ref.bucket)
		if err != nil {
			return nil, err
		}
		if _, err := s.client.as.Operate(withCtxW(ctx, t.write), key, ops...); err != nil {
			return nil, convertError("CreateTasks", err)
		}

		indexKey, err := s.client.keys.taskIndexKey(
			request.NamespaceID, request.TaskQueue, request.TaskType, ref.subqueue)
		if err != nil {
			return nil, err
		}
		if _, err := s.client.as.Operate(withCtxW(ctx, t.write), indexKey,
			as.MapPutOp(kOrderedMap, binBucketSet, ref.bucket, 1)); err != nil {
			return nil, convertError("CreateTasks", err)
		}
	}

	if request.TaskQueueInfo != nil {
		bins := append(
			blobBins(binData, binEncoding, request.TaskQueueInfo),
			as.NewBin(binRangeID, request.RangeID),
		)
		if err := s.client.as.PutBins(withCtxW(ctx, t.write), tqKey, bins...); err != nil {
			return nil, convertError("CreateTasks", err)
		}
	}

	if err := t.commit("CreateTasks"); err != nil {
		return nil, err
	}
	return &p.CreateTasksResponse{UpdatedMetadata: request.TaskQueueInfo != nil}, nil
}

// taskValue stores the payload alongside its expiry.
//
// Cassandra sets a per-task TTL. Aerospike TTL is per *record* and our tasks are
// map entries, so an entry cannot expire on its own -- and the namespace runs
// with NSUP off anyway. The expiry travels with the value and reads filter on
// it; reclamation is CompleteTasksLessThan's job, which Temporal calls
// regularly. See docs/04-open-questions.md Q1.
func taskValue(task *p.InternalCreateTask) []any {
	return []any{task.Task.Data, task.Task.EncodingType.String(), expiryNanos(task.ExpiryTime)}
}

func taskFromValue(v any) (*commonpb.DataBlob, int64, bool) {
	parts, ok := v.([]any)
	if !ok || len(parts) < 3 {
		return nil, 0, false
	}
	data := asBytes(parts[0])
	enc, _ := parts[1].(string)
	expiry, _ := asInt64(parts[2])
	if data == nil {
		return nil, 0, false
	}
	return p.NewDataBlob(data, enc), expiry, true
}

func (s *taskStore) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	codec := matchingTaskCodec{fair: s.fair}

	token, err := decodeTaskPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	firstBucket := codec.bucket(request.InclusiveMinTaskID)
	if token != nil && token.Bucket > firstBucket {
		firstBucket = token.Bucket
	}
	// The fair store has no upper bound on task id; the classic one does.
	lastBucket := int64(-1)
	unbounded := s.fair || request.ExclusiveMaxTaskID <= 0
	if !unbounded {
		lastBucket = codec.bucket(request.ExclusiveMaxTaskID)
		if request.ExclusiveMaxTaskID&((1<<taskBucketShift)-1) == 0 {
			lastBucket--
		}
	}

	buckets, err := s.populatedTaskBuckets(ctx, request, firstBucket, lastBucket, unbounded)
	if err != nil {
		return nil, err
	}

	pageSize := request.PageSize
	if pageSize <= 0 {
		pageSize = 1000
	}

	minKey := codec.encode(request.InclusiveMinPass, request.InclusiveMinTaskID)
	var maxKey any
	if !unbounded {
		maxKey = codec.encode(0, request.ExclusiveMaxTaskID)
	}

	now := time.Now().UTC().UnixNano()
	resp := &p.InternalGetTasksResponse{}
	var lastEmittedBucket int64
	var lastEmittedKey []byte

	for _, bucket := range buckets {
		key, err := s.client.keys.taskKey(
			request.NamespaceID, request.TaskQueue, request.TaskType, request.Subqueue, bucket)
		if err != nil {
			return nil, err
		}
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapGetByKeyRangeOp(binTaskMap, minKey, maxKey, as.MapReturnType.KEY_VALUE))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, convertError("GetTasks", err)
		}

		for _, entry := range recordMapPairs(rec, binTaskMap) {
			encoded := matchingEncodedBytes(codec, entry.Key)
			if token != nil && bucket == token.Bucket && bytesCompare(encoded, token.LastKey) <= 0 {
				continue
			}
			if len(resp.Tasks) == pageSize {
				resp.NextPageToken, err = encodeTaskPageToken(lastEmittedBucket, lastEmittedKey)
				if err != nil {
					return nil, err
				}
				return resp, nil
			}
			blob, expiry, ok := taskFromValue(entry.Value)
			if !ok {
				continue
			}
			// Expired tasks are invisible even before completion removes them.
			if expiry > 0 && expiry <= now {
				continue
			}
			resp.Tasks = append(resp.Tasks, blob)
			lastEmittedBucket = bucket
			lastEmittedKey = encoded
		}
	}
	return resp, nil
}

func (s *taskStore) populatedTaskBuckets(
	ctx context.Context, request *p.GetTasksRequest, first, last int64, unbounded bool,
) ([]int64, error) {
	if !unbounded && last < first {
		return nil, nil
	}
	indexKey, err := s.client.keys.taskIndexKey(
		request.NamespaceID, request.TaskQueue, request.TaskType, request.Subqueue)
	if err != nil {
		return nil, err
	}
	var end any
	if !unbounded {
		end = last + 1
	}
	rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), indexKey,
		as.MapGetByKeyRangeOp(binBucketSet, first, end, as.MapReturnType.KEY))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, convertError("GetTasks", err)
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

func (s *taskStore) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	codec := matchingTaskCodec{fair: s.fair}

	lastBucket := codec.bucket(request.ExclusiveMaxTaskID)
	getReq := &p.GetTasksRequest{
		NamespaceID: request.NamespaceID,
		TaskQueue:   request.TaskQueueName,
		TaskType:    request.TaskType,
		Subqueue:    request.Subqueue,
	}
	buckets, err := s.populatedTaskBuckets(ctx, getReq, 0, lastBucket, false)
	if err != nil {
		return 0, err
	}

	maxKey := codec.encode(request.ExclusiveMaxPass, request.ExclusiveMaxTaskID)
	minKey := codec.encode(0, 0)

	deleted := 0
	for _, bucket := range buckets {
		if request.Limit > 0 && deleted >= request.Limit {
			break
		}
		key, err := s.client.keys.taskKey(
			request.NamespaceID, request.TaskQueueName, request.TaskType, request.Subqueue, bucket)
		if err != nil {
			return deleted, err
		}
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapRemoveByKeyRangeOp(binTaskMap, minKey, maxKey, as.MapReturnType.COUNT))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return deleted, convertError("CompleteTasksLessThan", err)
		}
		if n, ok := lastOpResultInt(rec, binTaskMap); ok {
			deleted += int(n)
		}
	}
	return deleted, nil
}

// --- task queue user data ---

func (s *taskStore) GetTaskQueueUserData(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (*p.InternalGetTaskQueueUserDataResponse, error) {
	key, err := s.client.keys.taskQueueUserDataKey(request.NamespaceID, request.TaskQueue)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key)
	if err != nil {
		if isNotFound(err) {
			return nil, serviceerror.NewNotFoundf(
				"task queue user data for %q not found", request.TaskQueue)
		}
		return nil, convertError("GetTaskQueueUserData", err)
	}
	return &p.InternalGetTaskQueueUserDataResponse{
		Version:  binInt64(rec, binUDVersion),
		UserData: readBlob(rec, binUserData, binEncoding),
	}, nil
}

// UpdateTaskQueueUserData applies a versioned update per task queue and keeps
// the build-id -> task-queue index in step. All of it in one transaction, which
// Cassandra does with a conditional batch.
func (s *taskStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) error {
	// Callers reuse these flags across calls, so start from a clean slate:
	// a batch that fails must not leave a queue looking applied from an
	// earlier, successful batch.
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = false
		}
	}

	t := s.begin()
	defer t.finish()

	for taskQueue, update := range request.Updates {
		key, err := s.client.keys.taskQueueUserDataKey(request.NamespaceID, taskQueue)
		if err != nil {
			return err
		}

		if update.Version == 0 {
			// First write for this queue.
			policy := *t.write
			policy.RecordExistsAction = as.CREATE_ONLY
			err = s.client.as.PutBins(withCtxW(ctx, &policy), key,
				as.NewBin(binUserData, update.UserData.Data),
				as.NewBin(binEncoding, update.UserData.EncodingType.String()),
				as.NewBin(binUDVersion, int64(1)),
			)
			if err != nil {
				if isKeyExists(err) {
					return markConflict(update)
				}
				return convertError("UpdateTaskQueueUserData", err)
			}
		} else {
			policy := s.client.condWriteTxn(
				as.ExpEq(as.ExpIntBin(binUDVersion), as.ExpIntVal(update.Version)), t.txn)
			err = s.client.as.PutBins(withCtxW(ctx, policy), key,
				as.NewBin(binUserData, update.UserData.Data),
				as.NewBin(binEncoding, update.UserData.EncodingType.String()),
				as.NewBin(binUDVersion, update.Version+1),
			)
			if err != nil {
				if isFilteredOut(err) || isNotFound(err) {
					return markConflict(update)
				}
				return convertError("UpdateTaskQueueUserData", err)
			}
		}

		for _, buildID := range update.BuildIdsAdded {
			idxKey, err := s.client.keys.buildIDIndexKey(request.NamespaceID, buildID)
			if err != nil {
				return err
			}
			if _, err := s.client.as.Operate(withCtxW(ctx, t.write), idxKey,
				as.MapPutOp(kOrderedMap, binQueueNames, taskQueue, 1)); err != nil {
				return convertError("UpdateTaskQueueUserData", err)
			}
		}
		for _, buildID := range update.BuildIdsRemoved {
			idxKey, err := s.client.keys.buildIDIndexKey(request.NamespaceID, buildID)
			if err != nil {
				return err
			}
			if _, err := s.client.as.Operate(withCtxW(ctx, t.write), idxKey,
				as.MapRemoveByKeyOp(binQueueNames, taskQueue, as.MapReturnType.NONE)); err != nil && !isNotFound(err) {
				return convertError("UpdateTaskQueueUserData", err)
			}
		}

	}

	// Applied is only set after the transaction commits. The updates are
	// all-or-nothing, so marking a queue applied while a later one in the same
	// batch may still conflict would be a lie -- and the suite checks exactly
	// that.
	if err := t.commit("UpdateTaskQueueUserData"); err != nil {
		return err
	}
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = true
		}
	}
	return nil
}

// markConflict flags the one update that lost its version check. Every other
// update in the batch stays unapplied and unconflicted, because the transaction
// rolls back as a whole.
func markConflict(update *p.InternalSingleTaskQueueUserDataUpdate) error {
	if update.Conflicting != nil {
		*update.Conflicting = true
	}
	if update.Applied != nil {
		*update.Applied = false
	}
	return &p.ConditionFailedError{
		Msg: fmt.Sprintf("task queue user data version mismatch: expected %d", update.Version),
	}
}

func (s *taskStore) GetTaskQueuesByBuildId(
	ctx context.Context,
	request *p.GetTaskQueuesByBuildIdRequest,
) ([]string, error) {
	key, err := s.client.keys.buildIDIndexKey(request.NamespaceID, request.BuildID)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key, binQueueNames)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, convertError("GetTaskQueuesByBuildId", err)
	}
	var out []string
	for _, pair := range recordMapPairs(rec, binQueueNames) {
		if name, ok := pair.Key.(string); ok {
			out = append(out, name)
		}
	}
	return out, nil
}

func (s *taskStore) CountTaskQueuesByBuildId(
	ctx context.Context,
	request *p.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	queues, err := s.GetTaskQueuesByBuildId(ctx, &p.GetTaskQueuesByBuildIdRequest{
		NamespaceID: request.NamespaceID,
		BuildID:     request.BuildID,
	})
	if err != nil {
		return 0, err
	}
	return len(queues), nil
}

// --- shared helpers ---

// begin mirrors executionStore.begin; task queue writes need the same
// transaction discipline.
func (s *taskStore) begin() *txnScope {
	txn := as.NewTxnWithCapacity(16, 16)
	read, write, del := s.client.txnPolicies(txn)
	return &txnScope{client: s.client, txn: txn, read: read, write: write, del: del}
}

func expiryNanos(ts interface{ AsTime() time.Time }) int64 {
	if ts == nil {
		return 0
	}
	t := ts.AsTime()
	if t.IsZero() || t.Unix() <= 0 {
		return 0
	}
	return t.UnixNano()
}

func matchingEncodedBytes(codec matchingTaskCodec, raw any) []byte {
	if !codec.fair {
		id, _ := asInt64(raw)
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(id))
		return b
	}
	return asBytes(raw)
}

func bytesCompare(a, b []byte) int {
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
