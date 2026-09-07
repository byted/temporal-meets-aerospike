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
func (s *executionStore) AddHistoryTasks(context.Context, *p.InternalAddHistoryTasksRequest) error {
	return unimplemented("AddHistoryTasks")
}
func (s *executionStore) GetHistoryTasks(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
	return nil, unimplemented("GetHistoryTasks")
}
func (s *executionStore) CompleteHistoryTask(context.Context, *p.CompleteHistoryTaskRequest) error {
	return unimplemented("CompleteHistoryTask")
}
func (s *executionStore) RangeCompleteHistoryTasks(context.Context, *p.RangeCompleteHistoryTasksRequest) error {
	return unimplemented("RangeCompleteHistoryTasks")
}

// Replication DLQ: single-cluster deployments never reach these.
func (s *executionStore) PutReplicationTaskToDLQ(context.Context, *p.PutReplicationTaskToDLQRequest) error {
	return unimplemented("PutReplicationTaskToDLQ")
}
func (s *executionStore) GetReplicationTasksFromDLQ(context.Context, *p.GetReplicationTasksFromDLQRequest) (*p.InternalGetReplicationTasksFromDLQResponse, error) {
	return nil, unimplemented("GetReplicationTasksFromDLQ")
}
func (s *executionStore) DeleteReplicationTaskFromDLQ(context.Context, *p.DeleteReplicationTaskFromDLQRequest) error {
	return unimplemented("DeleteReplicationTaskFromDLQ")
}
func (s *executionStore) RangeDeleteReplicationTaskFromDLQ(context.Context, *p.RangeDeleteReplicationTaskFromDLQRequest) error {
	return unimplemented("RangeDeleteReplicationTaskFromDLQ")
}
func (s *executionStore) IsReplicationDLQEmpty(context.Context, *p.GetReplicationTasksFromDLQRequest) (bool, error) {
	return true, nil
}

// --- TaskStore (Phase 5) ---

type taskStore struct {
	client *client
	fair   bool
}

var _ p.TaskStore = (*taskStore)(nil)

func newTaskStore(c *client, fair bool) *taskStore { return &taskStore{client: c, fair: fair} }

func (s *taskStore) GetName() string { return StoreName }
func (s *taskStore) Close()          {}

func (s *taskStore) CreateTaskQueue(context.Context, *p.InternalCreateTaskQueueRequest) error {
	return unimplemented("CreateTaskQueue")
}
func (s *taskStore) GetTaskQueue(context.Context, *p.InternalGetTaskQueueRequest) (*p.InternalGetTaskQueueResponse, error) {
	return nil, unimplemented("GetTaskQueue")
}
func (s *taskStore) UpdateTaskQueue(context.Context, *p.InternalUpdateTaskQueueRequest) (*p.UpdateTaskQueueResponse, error) {
	return nil, unimplemented("UpdateTaskQueue")
}
func (s *taskStore) ListTaskQueue(context.Context, *p.ListTaskQueueRequest) (*p.InternalListTaskQueueResponse, error) {
	// Cassandra returns Unavailable("unsupported operation") here too.
	return nil, serviceerror.NewUnavailable("ListTaskQueue is not supported")
}
func (s *taskStore) DeleteTaskQueue(context.Context, *p.DeleteTaskQueueRequest) error {
	return unimplemented("DeleteTaskQueue")
}
func (s *taskStore) CreateTasks(context.Context, *p.InternalCreateTasksRequest) (*p.CreateTasksResponse, error) {
	return nil, unimplemented("CreateTasks")
}
func (s *taskStore) GetTasks(context.Context, *p.GetTasksRequest) (*p.InternalGetTasksResponse, error) {
	return nil, unimplemented("GetTasks")
}
func (s *taskStore) CompleteTasksLessThan(context.Context, *p.CompleteTasksLessThanRequest) (int, error) {
	return 0, unimplemented("CompleteTasksLessThan")
}
func (s *taskStore) GetTaskQueueUserData(context.Context, *p.GetTaskQueueUserDataRequest) (*p.InternalGetTaskQueueUserDataResponse, error) {
	return nil, unimplemented("GetTaskQueueUserData")
}
func (s *taskStore) UpdateTaskQueueUserData(context.Context, *p.InternalUpdateTaskQueueUserDataRequest) error {
	return unimplemented("UpdateTaskQueueUserData")
}
func (s *taskStore) ListTaskQueueUserDataEntries(context.Context, *p.ListTaskQueueUserDataEntriesRequest) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	return nil, unimplemented("ListTaskQueueUserDataEntries")
}
func (s *taskStore) GetTaskQueuesByBuildId(context.Context, *p.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	return nil, unimplemented("GetTaskQueuesByBuildId")
}
func (s *taskStore) CountTaskQueuesByBuildId(context.Context, *p.CountTaskQueuesByBuildIdRequest) (int, error) {
	return 0, unimplemented("CountTaskQueuesByBuildId")
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

// --- QueueV2, the DLQ (Phase 5) ---

type queueV2Store struct{ client *client }

var _ p.QueueV2 = (*queueV2Store)(nil)

func newQueueV2Store(c *client) *queueV2Store { return &queueV2Store{client: c} }

func (s *queueV2Store) EnqueueMessage(context.Context, *p.InternalEnqueueMessageRequest) (*p.InternalEnqueueMessageResponse, error) {
	return nil, unimplemented("QueueV2.EnqueueMessage")
}
func (s *queueV2Store) ReadMessages(context.Context, *p.InternalReadMessagesRequest) (*p.InternalReadMessagesResponse, error) {
	return nil, unimplemented("QueueV2.ReadMessages")
}
func (s *queueV2Store) CreateQueue(context.Context, *p.InternalCreateQueueRequest) (*p.InternalCreateQueueResponse, error) {
	return nil, unimplemented("QueueV2.CreateQueue")
}
func (s *queueV2Store) RangeDeleteMessages(context.Context, *p.InternalRangeDeleteMessagesRequest) (*p.InternalRangeDeleteMessagesResponse, error) {
	return nil, unimplemented("QueueV2.RangeDeleteMessages")
}
func (s *queueV2Store) ListQueues(context.Context, *p.InternalListQueuesRequest) (*p.InternalListQueuesResponse, error) {
	return nil, unimplemented("QueueV2.ListQueues")
}

// --- NexusEndpointStore (Phase 5) ---

type nexusEndpointStore struct{ client *client }

var _ p.NexusEndpointStore = (*nexusEndpointStore)(nil)

func newNexusEndpointStore(c *client) *nexusEndpointStore {
	return &nexusEndpointStore{client: c}
}

func (s *nexusEndpointStore) GetName() string { return StoreName }
func (s *nexusEndpointStore) Close()          {}

func (s *nexusEndpointStore) CreateOrUpdateNexusEndpoint(context.Context, *p.InternalCreateOrUpdateNexusEndpointRequest) error {
	return unimplemented("CreateOrUpdateNexusEndpoint")
}
func (s *nexusEndpointStore) DeleteNexusEndpoint(context.Context, *p.DeleteNexusEndpointRequest) error {
	return unimplemented("DeleteNexusEndpoint")
}
func (s *nexusEndpointStore) GetNexusEndpoint(context.Context, *p.GetNexusEndpointRequest) (*p.InternalNexusEndpoint, error) {
	return nil, unimplemented("GetNexusEndpoint")
}

// ListNexusEndpoints must return an empty list rather than an error: the
// frontend's endpoint registry long-polls it at startup and treats a failure as
// a fatal condition.
func (s *nexusEndpointStore) ListNexusEndpoints(context.Context, *p.ListNexusEndpointsRequest) (*p.InternalListNexusEndpointsResponse, error) {
	return &p.InternalListNexusEndpointsResponse{TableVersion: 0}, nil
}
