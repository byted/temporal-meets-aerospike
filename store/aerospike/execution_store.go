package aerospike

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"go.temporal.io/api/serviceerror"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

// permanentRunID is the discriminator Cassandra uses for a workflow's
// current-execution row. Kept identical so the two stores describe the same
// logical model, even though nothing shares data between them.
const permanentRunID = "30000000-0000-f000-f000-000000000001"

type executionStore struct {
	client     *client
	serializer serialization.Serializer
}

var _ p.ExecutionStore = (*executionStore)(nil)

func newExecutionStore(c *client, serializer serialization.Serializer) *executionStore {
	return &executionStore{client: c, serializer: serializer}
}

func (s *executionStore) GetName() string { return StoreName }
func (s *executionStore) Close()          {}

// GetHistoryBranchUtil must return a *constructed* util. The zero value has a
// nil serializer and panics inside ParseHistoryBranchInfo on the first create.
func (s *executionStore) GetHistoryBranchUtil() p.HistoryBranchUtil {
	return p.NewHistoryBranchUtil(s.serializer)
}

// currentRecordDiscriminator mirrors Cassandra's getCurrentRecordRunID: all
// workflows in a (shard, namespace, workflowID) share one current-execution
// record; other CHASM archetypes get their own.
func currentRecordDiscriminator(archetypeID chasm.ArchetypeID) string {
	if archetypeID == chasm.UnspecifiedArchetypeID || archetypeID == chasm.WorkflowArchetypeID {
		return permanentRunID
	}
	return fmt.Sprintf("archetype-%d", archetypeID)
}

// txnScope bundles a transaction with the policies bound to it, and makes the
// abort-unless-committed discipline hard to get wrong.
type txnScope struct {
	client    *client
	txn       *as.Txn
	read      *as.BasePolicy
	write     *as.WritePolicy
	del       *as.WritePolicy
	committed bool
}

func (s *executionStore) begin() *txnScope {
	txn := as.NewTxnWithCapacity(16, 16)
	read, write, del := s.client.txnPolicies(txn)
	return &txnScope{client: s.client, txn: txn, read: read, write: write, del: del}
}

// finish aborts unless the caller committed. Always defer it.
func (t *txnScope) finish() {
	if t.committed {
		return
	}
	_, _ = t.client.as.Abort(t.txn)
}

func (t *txnScope) commit(operation string) error {
	if _, err := t.client.as.Commit(t.txn); err != nil {
		return classifyCommitError(operation, err)
	}
	t.committed = true
	return nil
}

// classifyCommitError turns a failed commit into Temporal's vocabulary. A
// transaction whose commit-time verification failed means a concurrent writer
// touched our read or write set between our checks and the commit; Temporal
// retries a ConditionFailedError, which is the honest description.
func classifyCommitError(operation string, err error) error {
	if isTxnConflict(err) || isFilteredOut(err) {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("%s: concurrent modification detected (%v)", operation, err),
		}
	}
	return convertError(operation, err)
}

// assertShardOwnership reads the shard lease *inside* the transaction and
// compares range_id.
//
// Reading it inside the transaction is what makes this a fence rather than a
// check: the read joins the transaction's read set, so if another host takes
// the lease before we commit, the commit-time verification fails and the whole
// unit of work is rolled back. That is exactly what Cassandra's
// "IF range_id = ?" clause buys inside its single-partition batch.
func (s *executionStore) assertShardOwnership(
	ctx context.Context, t *txnScope, shardID int32, rangeID int64,
) error {
	key, err := s.client.keys.shardKey(shardID)
	if err != nil {
		return err
	}
	rec, err := s.client.as.Get(withCtx(ctx, t.read), key, binRangeID)
	if err != nil {
		if isNotFound(err) {
			return &p.ShardOwnershipLostError{
				ShardID: shardID,
				Msg:     fmt.Sprintf("shard %d has no lease record", shardID),
			}
		}
		return convertError("assertShardOwnership", err)
	}
	if actual := binInt64(rec, binRangeID); actual != rangeID {
		return &p.ShardOwnershipLostError{
			ShardID: shardID,
			Msg: fmt.Sprintf("shard %d: expected range_id=%d, found %d",
				shardID, rangeID, actual),
		}
	}
	return nil
}

// currentExecution is the decoded current-run pointer.
type currentExecution struct {
	exists           bool
	runID            string
	stateBlob        *as.Record
	lastWriteVersion int64
	state            *persistencespb.WorkflowExecutionState
}

func (s *executionStore) readCurrent(
	ctx context.Context, t *txnScope, key *as.Key,
) (*currentExecution, error) {
	rec, err := s.client.as.Get(withCtx(ctx, t.read), key)
	if err != nil {
		if isNotFound(err) {
			return &currentExecution{}, nil
		}
		return nil, convertError("readCurrentExecution", err)
	}

	cur := &currentExecution{
		exists:           true,
		runID:            binString(rec, binCurrentRunID),
		stateBlob:        rec,
		lastWriteVersion: binInt64(rec, binLastWriteVer),
	}
	// Decoding failures are tolerated: the state is only needed to populate a
	// diagnostic error, and an undecodable blob must not mask the conflict.
	if blob := readBlob(rec, binExecState, binExecStateEc); blob != nil {
		if state, err := serialization.DefaultDecoder.WorkflowExecutionStateFromBlob(blob); err == nil {
			cur.state = state
		}
	}
	return cur, nil
}

// currentConflict builds the error Temporal expects when the current-execution
// pointer is not where the caller believed.
//
// Aerospike does not hand back the conflicting record on a rejected write the
// way a Cassandra LWT does. Rather than write-then-read-back, this store reads
// the record inside the transaction *before* writing, so the seven fields below
// come from a snapshot that is part of the transaction's read set -- no
// second round trip, and no ABA window.
func currentConflict(requestRunID string, cur *currentExecution) error {
	state := cur.state
	if state == nil {
		state = &persistencespb.WorkflowExecutionState{}
	}
	return &p.CurrentWorkflowConditionFailedError{
		Msg: fmt.Sprintf("encountered current workflow error, request run ID: %v, actual run ID: %v",
			requestRunID, cur.runID),
		RequestIDs:       state.RequestIds,
		RunID:            state.RunId,
		State:            state.State,
		Status:           state.Status,
		LastWriteVersion: cur.lastWriteVersion,
		StartTime:        timestamp.TimeValuePtr(state.StartTime),
	}
}

// assertExecutionCondition enforces the mutable-state optimistic lock.
// Cassandra CASes on db_record_version when it is set, and falls back to
// next_event_id for records written before that field existed.
func (s *executionStore) assertExecutionCondition(
	ctx context.Context, t *txnScope, key *as.Key,
	dbRecordVersion int64, condition int64, runID string,
) error {
	rec, err := s.client.as.Get(withCtx(ctx, t.read), key, binDBVersion, binNextEventID)
	if err != nil {
		if isNotFound(err) {
			// A *missing* record is not a version conflict. Cassandra's LWT
			// cannot match a row that is not there, and its error extraction
			// falls through to the generic ConditionFailedError; callers
			// distinguish the two, so this must not be upgraded to
			// WorkflowConditionFailedError.
			return &p.ConditionFailedError{
				Msg: fmt.Sprintf("workflow execution %s not found", runID),
			}
		}
		return convertError("assertExecutionCondition", err)
	}

	actualDBVersion := binInt64(rec, binDBVersion)
	actualNextEventID := binInt64(rec, binNextEventID)

	if dbRecordVersion == 0 {
		if actualNextEventID != condition {
			return &p.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("workflow execution %s: expected next_event_id=%d, found %d",
					runID, condition, actualNextEventID),
				NextEventID:     actualNextEventID,
				DBRecordVersion: actualDBVersion,
			}
		}
		return nil
	}

	if actualDBVersion != dbRecordVersion-1 {
		return &p.WorkflowConditionFailedError{
			Msg: fmt.Sprintf("workflow execution %s: expected db_record_version=%d, found %d",
				runID, dbRecordVersion-1, actualDBVersion),
			NextEventID:     actualNextEventID,
			DBRecordVersion: actualDBVersion,
		}
	}
	return nil
}

// currentBins renders the current-execution pointer.
func currentBins(runID string, stateBlob *as_DataBlob, lastWriteVersion int64, state int32) []*as.Bin {
	return []*as.Bin{
		as.NewBin(binCurrentRunID, runID),
		as.NewBin(binExecState, stateBlob.Data),
		as.NewBin(binExecStateEc, stateBlob.Encoding),
		as.NewBin(binLastWriteVer, lastWriteVersion),
		as.NewBin(binWorkflowStat, int(state)),
	}
}

// as_DataBlob is a tiny local shape so currentBins does not depend on the
// caller having a *commonpb.DataBlob versus its parts.
type as_DataBlob struct {
	Data     []byte
	Encoding string
}

func (s *executionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	newWorkflow := request.NewWorkflowSnapshot
	shardID := request.ShardID

	if err := p.ValidateCreateWorkflowStateStatus(
		newWorkflow.ExecutionState.State, newWorkflow.ExecutionState.Status); err != nil {
		return nil, err
	}

	execKey, err := s.client.keys.executionKey(
		shardID, newWorkflow.NamespaceID, newWorkflow.WorkflowID, newWorkflow.RunID)
	if err != nil {
		return nil, err
	}
	currentKey, err := s.client.keys.currentExecutionKey(
		shardID, newWorkflow.NamespaceID, newWorkflow.WorkflowID,
		currentRecordDiscriminator(request.ArchetypeID))
	if err != nil {
		return nil, err
	}

	t := s.begin()
	defer t.finish()

	if err := s.assertShardOwnership(ctx, t, shardID, request.RangeID); err != nil {
		return nil, err
	}

	stateBlob := &as_DataBlob{
		Data:     newWorkflow.ExecutionStateBlob.Data,
		Encoding: newWorkflow.ExecutionStateBlob.EncodingType.String(),
	}

	switch request.Mode {
	case p.CreateWorkflowModeBypassCurrent:
		// The workflow is a zombie; the current pointer is left alone.

	case p.CreateWorkflowModeUpdateCurrent:
		cur, err := s.readCurrent(ctx, t, currentKey)
		if err != nil {
			return nil, err
		}
		// Cassandra conditions on all three of run id, last write version and
		// state==COMPLETED, so that a still-running workflow cannot be
		// displaced.
		if !cur.exists ||
			cur.runID != request.PreviousRunID ||
			cur.lastWriteVersion != request.PreviousLastWriteVersion ||
			(cur.state != nil && cur.state.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED) {
			return nil, currentConflict(request.PreviousRunID, cur)
		}
		if err := s.client.as.PutBins(withCtxW(ctx, t.write), currentKey,
			currentBins(newWorkflow.RunID, stateBlob,
				newWorkflow.LastWriteVersion, int32(newWorkflow.ExecutionState.State))...); err != nil {
			return nil, convertError("CreateWorkflowExecution", err)
		}

	case p.CreateWorkflowModeBrandNew:
		cur, err := s.readCurrent(ctx, t, currentKey)
		if err != nil {
			return nil, err
		}
		if cur.exists {
			// Cassandra expresses this as INSERT ... IF NOT EXISTS.
			return nil, currentConflict("", cur)
		}
		if err := s.client.as.PutBins(withCtxW(ctx, t.write), currentKey,
			currentBins(newWorkflow.RunID, stateBlob,
				newWorkflow.LastWriteVersion, int32(newWorkflow.ExecutionState.State))...); err != nil {
			return nil, convertError("CreateWorkflowExecution", err)
		}

	default:
		return nil, serviceerror.NewInternalf("CreateWorkflowExecution: unknown mode: %v", request.Mode)
	}

	// The execution record must not already exist.
	exists, err := s.recordExists(ctx, t, execKey)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, &p.WorkflowConditionFailedError{
			Msg: fmt.Sprintf("workflow execution already exists: run %s", newWorkflow.RunID),
		}
	}

	if err := s.writeSnapshot(ctx, t, execKey, &newWorkflow); err != nil {
		return nil, err
	}
	if err := s.writeTasks(ctx, t, shardID, newWorkflow.Tasks); err != nil {
		return nil, err
	}
	if err := s.appendEvents(ctx, t, request.NewWorkflowNewEvents); err != nil {
		return nil, err
	}

	if err := t.commit("CreateWorkflowExecution"); err != nil {
		return nil, err
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (s *executionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot
	shardID := request.ShardID

	if err := p.ValidateUpdateWorkflowStateStatus(
		updateWorkflow.ExecutionState.State, updateWorkflow.ExecutionState.Status); err != nil {
		return err
	}

	execKey, err := s.client.keys.executionKey(
		shardID, updateWorkflow.NamespaceID, updateWorkflow.WorkflowID, updateWorkflow.RunID)
	if err != nil {
		return err
	}
	currentKey, err := s.client.keys.currentExecutionKey(
		shardID, updateWorkflow.NamespaceID, updateWorkflow.WorkflowID,
		currentRecordDiscriminator(request.ArchetypeID))
	if err != nil {
		return err
	}

	t := s.begin()
	defer t.finish()

	if err := s.assertShardOwnership(ctx, t, shardID, request.RangeID); err != nil {
		return err
	}

	switch request.Mode {
	case p.UpdateWorkflowModeIgnoreCurrent:
		// noop

	case p.UpdateWorkflowModeBypassCurrent:
		if err := s.assertNotCurrent(ctx, t, currentKey, updateWorkflow.RunID, &updateWorkflow); err != nil {
			return err
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		cur, err := s.readCurrent(ctx, t, currentKey)
		if err != nil {
			return err
		}
		// Cassandra conditions the current-pointer update on current_run_id
		// still being the run we are updating.
		if !cur.exists || cur.runID != updateWorkflow.RunID {
			return currentConflict(updateWorkflow.RunID, cur)
		}

		if newWorkflow != nil {
			if updateWorkflow.NamespaceID != newWorkflow.NamespaceID {
				return serviceerror.NewInternal(
					"UpdateWorkflowExecution: cannot continue as new to another namespace")
			}
			// Continue-as-new: the pointer moves to the new run.
			blob := &as_DataBlob{
				Data:     newWorkflow.ExecutionStateBlob.Data,
				Encoding: newWorkflow.ExecutionStateBlob.EncodingType.String(),
			}
			if err := s.client.as.PutBins(withCtxW(ctx, t.write), currentKey,
				currentBins(newWorkflow.RunID, blob, newWorkflow.LastWriteVersion,
					int32(newWorkflow.ExecutionState.State))...); err != nil {
				return convertError("UpdateWorkflowExecution", err)
			}
		} else {
			blob := &as_DataBlob{
				Data:     updateWorkflow.ExecutionStateBlob.Data,
				Encoding: updateWorkflow.ExecutionStateBlob.EncodingType.String(),
			}
			if err := s.client.as.PutBins(withCtxW(ctx, t.write), currentKey,
				currentBins(updateWorkflow.RunID, blob, updateWorkflow.LastWriteVersion,
					int32(updateWorkflow.ExecutionState.State))...); err != nil {
				return convertError("UpdateWorkflowExecution", err)
			}
		}

	default:
		return serviceerror.NewInternalf("UpdateWorkflowExecution: unknown mode: %v", request.Mode)
	}

	if err := s.assertExecutionCondition(ctx, t, execKey,
		updateWorkflow.DBRecordVersion, updateWorkflow.Condition, updateWorkflow.RunID); err != nil {
		return err
	}

	if err := s.applyMutation(ctx, t, execKey, &updateWorkflow); err != nil {
		return err
	}
	if err := s.writeTasks(ctx, t, shardID, updateWorkflow.Tasks); err != nil {
		return err
	}

	if newWorkflow != nil {
		newKey, err := s.client.keys.executionKey(
			shardID, newWorkflow.NamespaceID, newWorkflow.WorkflowID, newWorkflow.RunID)
		if err != nil {
			return err
		}
		exists, err := s.recordExists(ctx, t, newKey)
		if err != nil {
			return err
		}
		if exists {
			return &p.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("new workflow execution already exists: run %s", newWorkflow.RunID),
			}
		}
		if err := s.writeSnapshot(ctx, t, newKey, newWorkflow); err != nil {
			return err
		}
		if err := s.writeTasks(ctx, t, shardID, newWorkflow.Tasks); err != nil {
			return err
		}
	}

	if err := s.appendEvents(ctx, t, request.UpdateWorkflowNewEvents); err != nil {
		return err
	}
	if err := s.appendEvents(ctx, t, request.NewWorkflowNewEvents); err != nil {
		return err
	}

	return t.commit("UpdateWorkflowExecution")
}

func (s *executionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	shardID := request.ShardID
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot
	currentWorkflow := request.CurrentWorkflowMutation

	currentKey, err := s.client.keys.currentExecutionKey(
		shardID, resetWorkflow.NamespaceID, resetWorkflow.WorkflowID,
		currentRecordDiscriminator(request.ArchetypeID))
	if err != nil {
		return err
	}

	t := s.begin()
	defer t.finish()

	if err := s.assertShardOwnership(ctx, t, shardID, request.RangeID); err != nil {
		return err
	}

	switch request.Mode {
	case p.ConflictResolveWorkflowModeBypassCurrent:
		// Bypassing the current record is only legitimate for a run that is
		// not the current one -- otherwise the pointer would be left naming a
		// workflow whose state we just rewrote.
		if err := s.assertNotCurrent(ctx, t, currentKey,
			resetWorkflow.ExecutionState.RunId, currentWorkflow); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		// Whichever workflow ends up current, the pointer must currently name
		// the run we were told to expect.
		var expectedRunID string
		var pointTo *p.InternalWorkflowSnapshot
		switch {
		case newWorkflow != nil:
			pointTo = newWorkflow
		default:
			pointTo = &resetWorkflow
		}
		if currentWorkflow != nil {
			expectedRunID = currentWorkflow.RunID
		} else {
			expectedRunID = resetWorkflow.RunID
		}

		cur, err := s.readCurrent(ctx, t, currentKey)
		if err != nil {
			return err
		}
		if !cur.exists || cur.runID != expectedRunID {
			return currentConflict(expectedRunID, cur)
		}

		blob := &as_DataBlob{
			Data:     pointTo.ExecutionStateBlob.Data,
			Encoding: pointTo.ExecutionStateBlob.EncodingType.String(),
		}
		if err := s.client.as.PutBins(withCtxW(ctx, t.write), currentKey,
			currentBins(pointTo.RunID, blob, pointTo.LastWriteVersion,
				int32(pointTo.ExecutionState.State))...); err != nil {
			return convertError("ConflictResolveWorkflowExecution", err)
		}

	default:
		return serviceerror.NewInternalf(
			"ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode)
	}

	// Reset the target workflow. A snapshot replaces the record wholesale.
	resetKey, err := s.client.keys.executionKey(
		shardID, resetWorkflow.NamespaceID, resetWorkflow.WorkflowID, resetWorkflow.RunID)
	if err != nil {
		return err
	}
	if err := s.assertExecutionCondition(ctx, t, resetKey,
		resetWorkflow.DBRecordVersion, resetWorkflow.Condition, resetWorkflow.RunID); err != nil {
		return err
	}
	if err := s.writeSnapshot(ctx, t, resetKey, &resetWorkflow); err != nil {
		return err
	}
	if err := s.writeTasks(ctx, t, shardID, resetWorkflow.Tasks); err != nil {
		return err
	}

	if currentWorkflow != nil {
		curKey, err := s.client.keys.executionKey(
			shardID, currentWorkflow.NamespaceID, currentWorkflow.WorkflowID, currentWorkflow.RunID)
		if err != nil {
			return err
		}
		if err := s.assertExecutionCondition(ctx, t, curKey,
			currentWorkflow.DBRecordVersion, currentWorkflow.Condition, currentWorkflow.RunID); err != nil {
			return err
		}
		if err := s.applyMutation(ctx, t, curKey, currentWorkflow); err != nil {
			return err
		}
		if err := s.writeTasks(ctx, t, shardID, currentWorkflow.Tasks); err != nil {
			return err
		}
	}

	if newWorkflow != nil {
		newKey, err := s.client.keys.executionKey(
			shardID, newWorkflow.NamespaceID, newWorkflow.WorkflowID, newWorkflow.RunID)
		if err != nil {
			return err
		}
		exists, err := s.recordExists(ctx, t, newKey)
		if err != nil {
			return err
		}
		if exists {
			return &p.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("new workflow execution already exists: run %s", newWorkflow.RunID),
			}
		}
		if err := s.writeSnapshot(ctx, t, newKey, newWorkflow); err != nil {
			return err
		}
		if err := s.writeTasks(ctx, t, shardID, newWorkflow.Tasks); err != nil {
			return err
		}
	}

	if err := s.appendEvents(ctx, t, request.ResetWorkflowEventsNewEvents); err != nil {
		return err
	}
	if err := s.appendEvents(ctx, t, request.NewWorkflowEventsNewEvents); err != nil {
		return err
	}
	if err := s.appendEvents(ctx, t, request.CurrentWorkflowEventsNewEvents); err != nil {
		return err
	}

	return t.commit("ConflictResolveWorkflowExecution")
}

func (s *executionStore) SetWorkflowExecution(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
) error {
	snapshot := request.SetWorkflowSnapshot
	shardID := request.ShardID

	execKey, err := s.client.keys.executionKey(
		shardID, snapshot.NamespaceID, snapshot.WorkflowID, snapshot.RunID)
	if err != nil {
		return err
	}

	t := s.begin()
	defer t.finish()

	if err := s.assertShardOwnership(ctx, t, shardID, request.RangeID); err != nil {
		return err
	}
	if err := s.assertExecutionCondition(ctx, t, execKey,
		snapshot.DBRecordVersion, snapshot.Condition, snapshot.RunID); err != nil {
		return err
	}
	if err := s.writeSnapshot(ctx, t, execKey, &snapshot); err != nil {
		return err
	}
	if err := s.writeTasks(ctx, t, shardID, snapshot.Tasks); err != nil {
		return err
	}

	return t.commit("SetWorkflowExecution")
}

func (s *executionStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	key, err := s.client.keys.executionKey(
		request.ShardID, request.NamespaceID, request.WorkflowID, request.RunID)
	if err != nil {
		return nil, err
	}

	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key)
	if err != nil {
		if isNotFound(err) {
			return nil, serviceerror.NewNotFoundf(
				"workflow execution not found: namespace %s, workflow %s, run %s",
				request.NamespaceID, request.WorkflowID, request.RunID)
		}
		return nil, convertError("GetWorkflowExecution", err)
	}

	state := mutableStateFromRecord(rec)
	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: state.DBRecordVersion,
	}, nil
}

func (s *executionStore) GetCurrentExecution(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, error) {
	key, err := s.client.keys.currentExecutionKey(
		request.ShardID, request.NamespaceID, request.WorkflowID,
		currentRecordDiscriminator(request.ArchetypeID))
	if err != nil {
		return nil, err
	}

	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key)
	if err != nil {
		if isNotFound(err) {
			return nil, serviceerror.NewNotFoundf(
				"current workflow execution not found: namespace %s, workflow %s",
				request.NamespaceID, request.WorkflowID)
		}
		return nil, convertError("GetCurrentExecution", err)
	}

	blob := readBlob(rec, binExecState, binExecStateEc)
	if blob == nil {
		return nil, serviceerror.NewUnavailable(
			"current execution record has no execution state")
	}
	state, err := serialization.DefaultDecoder.WorkflowExecutionStateFromBlob(blob)
	if err != nil {
		return nil, convertError("GetCurrentExecution", err)
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID:          binString(rec, binCurrentRunID),
		ExecutionState: state,
	}, nil
}

func (s *executionStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	key, err := s.client.keys.executionKey(
		request.ShardID, request.NamespaceID, request.WorkflowID, request.RunID)
	if err != nil {
		return err
	}
	if _, err := s.client.as.Delete(withCtxW(ctx, s.client.delete), key); err != nil {
		return convertError("DeleteWorkflowExecution", err)
	}
	return nil
}

// DeleteCurrentWorkflowExecution removes the pointer only if it still names the
// given run, so a concurrent continue-as-new is not clobbered.
func (s *executionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	key, err := s.client.keys.currentExecutionKey(
		request.ShardID, request.NamespaceID, request.WorkflowID,
		currentRecordDiscriminator(request.ArchetypeID))
	if err != nil {
		return err
	}

	policy := *s.client.delete
	policy.FilterExpression = as.ExpEq(
		as.ExpStringBin(binCurrentRunID), as.ExpStringVal(request.RunID))

	if _, err := s.client.as.Delete(withCtxW(ctx, &policy), key); err != nil {
		if isFilteredOut(err) || isNotFound(err) {
			// Not ours to delete, or already gone. Both are success.
			return nil
		}
		return convertError("DeleteCurrentWorkflowExecution", err)
	}
	return nil
}

// assertNotCurrent fails if the given run *is* the current execution. Used by
// the bypass-current modes, where the caller has asserted the run is a zombie.
//
// A missing current record is fine: there is nothing to contradict.
func (s *executionStore) assertNotCurrent(
	ctx context.Context, t *txnScope, currentKey *as.Key, runID string,
	currentWorkflow *p.InternalWorkflowMutation,
) error {
	cur, err := s.readCurrent(ctx, t, currentKey)
	if err != nil {
		return err
	}
	if !cur.exists || cur.runID != runID {
		return nil
	}

	var startTime *time.Time
	if currentWorkflow != nil && currentWorkflow.ExecutionState != nil {
		startTime = timestamp.TimeValuePtr(currentWorkflow.ExecutionState.StartTime)
	}
	// Deliberately sparse: Cassandra reports this case with zero-valued fields
	// because the assertion is about identity, not about the record's contents.
	return &p.CurrentWorkflowConditionFailedError{
		Msg: fmt.Sprintf(
			"assertion on current record failed. Current run ID is not expected: %v", cur.runID),
		StartTime: startTime,
	}
}

// --- helpers ---

func (s *executionStore) recordExists(ctx context.Context, t *txnScope, key *as.Key) (bool, error) {
	exists, err := s.client.as.Exists(withCtx(ctx, t.read), key)
	if err != nil {
		return false, convertError("recordExists", err)
	}
	return exists, nil
}

// writeSnapshot makes the record describe exactly the supplied state.
//
// Deliberately NOT RecordExistsAction=REPLACE: that is incompatible with the
// CDT operations the collections need (the server returns PARAMETER_ERROR).
// snapshotOps clears each collection bin first instead -- see its comment.
func (s *executionStore) writeSnapshot(
	ctx context.Context, t *txnScope, key *as.Key, snapshot *p.InternalWorkflowSnapshot,
) error {
	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), key, snapshotOps(snapshot)...); err != nil {
		return convertError("writeSnapshot", err)
	}
	return nil
}

// appendEvents writes the history nodes that accompany a mutable-state change,
// inside the same transaction.
func (s *executionStore) appendEvents(
	ctx context.Context, t *txnScope, requests []*p.InternalAppendHistoryNodesRequest,
) error {
	for _, req := range requests {
		if err := s.appendHistoryNodesIn(ctx, t, req); err != nil {
			return err
		}
	}
	return nil
}

// applyMutation applies sparse deltas in a single Operate, so the server
// mutates the collections in place under one record lock.
func (s *executionStore) applyMutation(
	ctx context.Context, t *txnScope, key *as.Key, mutation *p.InternalWorkflowMutation,
) error {
	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), key, mutationOps(mutation)...); err != nil {
		return convertError("applyMutation", err)
	}
	return nil
}
