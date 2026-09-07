package aerospike

import (
	"context"
	"encoding/binary"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
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
	setHistoryTask = "htask"
	binTaskMap     = "t"

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

		// bucket -> map of encoded key -> blob value
		grouped := make(map[int64]map[any]any)
		for _, task := range taskList {
			bucket := codec.bucket(task.Key)
			if grouped[bucket] == nil {
				grouped[bucket] = make(map[any]any)
			}
			grouped[bucket][codec.encode(task.Key)] = blobValue(task.Blob)
		}

		for bucket, entries := range grouped {
			key, err := s.client.keys.historyTaskKey(shardID, category.ID(), bucket)
			if err != nil {
				return err
			}
			_, err = s.client.as.Operate(withCtxW(ctx, t.write), key,
				as.MapPutItemsOp(kOrderedMap, binTaskMap, entries))
			if err != nil {
				return convertError("writeTasks", err)
			}
		}
	}
	return nil
}
