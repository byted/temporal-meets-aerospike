package aerospike

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

// Stores that are declared but not yet implemented. The factory must be able to
// construct all nine stores from the very first phase, because several are
// built eagerly at boot regardless of whether they are ever called -- the
// frontend long-polls ListNexusEndpoints, and history depends on QueueV2.
//
// Each stub returns Unimplemented rather than panicking, so that starting a
// server against a partial store produces a legible error naming the missing
// method instead of a crash.
//
// Phase 2 replaces executionStore, Phase 5 the rest.

func unimplemented(method string) error {
	return serviceerror.NewUnimplementedf("aerospike: %s is not implemented yet", method)
}

// Remaining ExecutionStore methods. Phase 3 replaces the task methods, Phase 4
// the history-event ones.

func (s *executionStore) ListConcreteExecutions(context.Context, *p.ListConcreteExecutionsRequest) (*p.InternalListConcreteExecutionsResponse, error) {
	// Scavenger-only, and needs a scan with no key scope -- the one access
	// pattern Aerospike has no good answer for. Out of scope for the PoC.
	return nil, unimplemented("ListConcreteExecutions")
}

// --- TaskStore: type and the one method that stays unsupported ---

type taskStore struct {
	client *client
	fair   bool
}

var _ p.TaskStore = (*taskStore)(nil)

func newTaskStore(c *client, fair bool) *taskStore { return &taskStore{client: c, fair: fair} }

func (s *taskStore) GetName() string { return StoreName }
func (s *taskStore) Close()          {}

// ListTaskQueue is unsupported, matching Cassandra, which returns the same.
func (s *taskStore) ListTaskQueue(context.Context, *p.ListTaskQueueRequest) (*p.InternalListTaskQueueResponse, error) {
	return nil, serviceerror.NewUnavailable("ListTaskQueue is not supported")
}

// ListTaskQueueUserDataEntries needs an unscoped scan and is only used by
// administrative tooling.
func (s *taskStore) ListTaskQueueUserDataEntries(context.Context, *p.ListTaskQueueUserDataEntriesRequest) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	return nil, unimplemented("ListTaskQueueUserDataEntries")
}

// --- Queue v1, the namespace replication queue (Phase 5) ---

type queueStore struct {
	client    *client
	queueType p.QueueType
}

var _ p.Queue = (*queueStore)(nil)

func newQueueStore(c *client, queueType p.QueueType) *queueStore {
	return &queueStore{client: c, queueType: queueType}
}

func (s *queueStore) Close() {}

// Init must succeed even before the queue is implemented: it runs at boot for
// every service that depends on the namespace replication queue.
func (s *queueStore) Init(context.Context, *commonpb.DataBlob) error { return nil }

func (s *queueStore) EnqueueMessage(context.Context, *commonpb.DataBlob) error {
	return unimplemented("Queue.EnqueueMessage")
}
func (s *queueStore) ReadMessages(context.Context, int64, int) ([]*p.QueueMessage, error) {
	return nil, nil
}
func (s *queueStore) DeleteMessagesBefore(context.Context, int64) error { return nil }
func (s *queueStore) UpdateAckLevel(context.Context, *p.InternalQueueMetadata) error {
	return unimplemented("Queue.UpdateAckLevel")
}
func (s *queueStore) GetAckLevels(context.Context) (*p.InternalQueueMetadata, error) {
	return &p.InternalQueueMetadata{}, nil
}
func (s *queueStore) EnqueueMessageToDLQ(context.Context, *commonpb.DataBlob) (int64, error) {
	return 0, unimplemented("Queue.EnqueueMessageToDLQ")
}
func (s *queueStore) ReadMessagesFromDLQ(context.Context, int64, int64, int, []byte) ([]*p.QueueMessage, []byte, error) {
	return nil, nil, nil
}
func (s *queueStore) DeleteMessageFromDLQ(context.Context, int64) error              { return nil }
func (s *queueStore) RangeDeleteMessagesFromDLQ(context.Context, int64, int64) error { return nil }
func (s *queueStore) UpdateDLQAckLevel(context.Context, *p.InternalQueueMetadata) error {
	return unimplemented("Queue.UpdateDLQAckLevel")
}
func (s *queueStore) GetDLQAckLevels(context.Context) (*p.InternalQueueMetadata, error) {
	return &p.InternalQueueMetadata{}, nil
}
