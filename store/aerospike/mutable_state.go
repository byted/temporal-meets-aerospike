package aerospike

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
)

// Mutable state lives in one Aerospike record. Cassandra spreads it across one
// row plus six collection columns in the same partition; here the collections
// become K-ordered map bins on a single record, which is the only way to keep
// them co-located (a record is Aerospike's unit of placement).
//
// The critical property to preserve: an update supplies *sparse* deltas
// (UpsertActivityInfos / DeleteActivityInfos and friends). The store never sees
// the full collection, so it must never read-modify-write one. Every delta is
// applied as a server-side map operation inside a single Operate call.
const (
	binExecInfo    = "info"
	binExecInfoEnc = "info_enc"
	binExecState   = "state"
	binExecStateEc = "state_enc"
	binNextEventID = "next_id"
	binDBVersion   = "ver"
	binChecksum    = "csum"
	binChecksumEnc = "csum_enc"

	// Collection bins, each a K-ordered map.
	binActivities = "act"
	binTimers     = "tmr"
	binChildren   = "chld"
	binCancels    = "cncl"
	binSignals    = "sig"
	binChasm      = "chasm"
	binSignalReqs = "sigreq"

	// Buffered events are an ordered list, appended to and cleared wholesale.
	binBuffered = "buf"

	// Current-execution record bins.
	binCurrentRunID = "run_id"
	binLastWriteVer = "lwv"
	binWorkflowStat = "wf_state"
)

// kOrderedMap is the policy for every collection bin. Key ordering is not
// needed for correctness on these -- they are looked up by exact key -- but an
// ordered map is O(log N) where an unordered one is O(N), and the ordering is
// what makes whole-map reads deterministic.
var kOrderedMap = as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)

// blobValue encodes a DataBlob as a map value. Two elements rather than a
// nested map: smaller on the wire and the shape is fixed.
func blobValue(b *commonpb.DataBlob) []any {
	if b == nil {
		return nil
	}
	return []any{b.Data, b.EncodingType.String()}
}

func blobFromValue(v any) *commonpb.DataBlob {
	parts, ok := v.([]any)
	if !ok || len(parts) < 2 {
		return nil
	}
	data := asBytes(parts[0])
	enc, _ := parts[1].(string)
	if data == nil {
		return nil
	}
	return p.NewDataBlob(data, enc)
}

// chasmValue flattens a CHASM node's two blobs into one map value.
func chasmValue(n p.InternalChasmNode) []any {
	var metaData, dataData []byte
	var metaEnc, dataEnc string
	if n.Metadata != nil {
		metaData, metaEnc = n.Metadata.Data, n.Metadata.EncodingType.String()
	}
	if n.Data != nil {
		dataData, dataEnc = n.Data.Data, n.Data.EncodingType.String()
	}
	return []any{metaData, metaEnc, dataData, dataEnc}
}

func chasmFromValue(v any) p.InternalChasmNode {
	parts, ok := v.([]any)
	if !ok || len(parts) < 4 {
		return p.InternalChasmNode{}
	}
	metaData := asBytes(parts[0])
	metaEnc, _ := parts[1].(string)
	dataData := asBytes(parts[2])
	dataEnc, _ := parts[3].(string)

	node := p.InternalChasmNode{}
	if metaData != nil {
		node.Metadata = p.NewDataBlob(metaData, metaEnc)
	}
	if dataData != nil {
		node.Data = p.NewDataBlob(dataData, dataEnc)
	}
	return node
}

// snapshotOps renders a full snapshot: every collection is cleared and then
// repopulated, so the record ends up describing exactly the supplied state.
// Used on create, reset and set -- anywhere the caller supplies complete state
// rather than a delta.
//
// Note this cannot use RecordExistsAction=REPLACE, which would be the obvious
// way to express "make the record exactly this". REPLACE means "drop all bins
// and write these", which contradicts a CDT map operation's read-modify
// semantics: the server rejects the combination with PARAMETER_ERROR. Clearing
// each collection bin explicitly, under normal UPDATE semantics, is the
// equivalent that actually works.
func snapshotOps(s *p.InternalWorkflowSnapshot) []*as.Operation {
	ops := []*as.Operation{}
	// Clear first. Ops within one Operate are applied in order against the
	// server's in-memory copy, so the repopulation below sees an empty map.
	for _, bin := range collectionBins {
		ops = append(ops, as.PutOp(as.NewBin(bin, nil)))
	}
	// A snapshot has no buffered events by construction: creating or resetting
	// a workflow discards them.
	ops = append(ops, as.PutOp(as.NewBin(binBuffered, nil)))

	ops = append(ops,
		as.PutOp(as.NewBin(binExecInfo, s.ExecutionInfoBlob.Data)),
		as.PutOp(as.NewBin(binExecInfoEnc, s.ExecutionInfoBlob.EncodingType.String())),
		as.PutOp(as.NewBin(binExecState, s.ExecutionStateBlob.Data)),
		as.PutOp(as.NewBin(binExecStateEc, s.ExecutionStateBlob.EncodingType.String())),
		as.PutOp(as.NewBin(binNextEventID, s.NextEventID)),
		as.PutOp(as.NewBin(binDBVersion, s.DBRecordVersion)),
	)
	ops = append(ops, checksumOps(s.Checksum)...)

	ops = appendWholeMap(ops, binActivities, int64BlobMap(s.ActivityInfos))
	ops = appendWholeMap(ops, binTimers, stringBlobMap(s.TimerInfos))
	ops = appendWholeMap(ops, binChildren, int64BlobMap(s.ChildExecutionInfos))
	ops = appendWholeMap(ops, binCancels, int64BlobMap(s.RequestCancelInfos))
	ops = appendWholeMap(ops, binSignals, int64BlobMap(s.SignalInfos))
	ops = appendWholeMap(ops, binChasm, chasmMap(s.ChasmNodes))
	ops = appendWholeMap(ops, binSignalReqs, signalRequestedMap(s.SignalRequestedIDs))

	return ops
}

// mutationOps renders a mutation as *sparse* operations. This is the path that
// must never read-modify-write.
func mutationOps(m *p.InternalWorkflowMutation) []*as.Operation {
	ops := []*as.Operation{
		as.PutOp(as.NewBin(binExecInfo, m.ExecutionInfoBlob.Data)),
		as.PutOp(as.NewBin(binExecInfoEnc, m.ExecutionInfoBlob.EncodingType.String())),
		as.PutOp(as.NewBin(binExecState, m.ExecutionStateBlob.Data)),
		as.PutOp(as.NewBin(binExecStateEc, m.ExecutionStateBlob.EncodingType.String())),
		as.PutOp(as.NewBin(binNextEventID, m.NextEventID)),
		as.PutOp(as.NewBin(binDBVersion, m.DBRecordVersion)),
	}
	ops = append(ops, checksumOps(m.Checksum)...)

	ops = append(ops, upsertInt64Blobs(binActivities, m.UpsertActivityInfos)...)
	ops = append(ops, deleteInt64Keys(binActivities, m.DeleteActivityInfos)...)

	ops = append(ops, upsertStringBlobs(binTimers, m.UpsertTimerInfos)...)
	ops = append(ops, deleteStringKeys(binTimers, m.DeleteTimerInfos)...)

	ops = append(ops, upsertInt64Blobs(binChildren, m.UpsertChildExecutionInfos)...)
	ops = append(ops, deleteInt64Keys(binChildren, m.DeleteChildExecutionInfos)...)

	ops = append(ops, upsertInt64Blobs(binCancels, m.UpsertRequestCancelInfos)...)
	ops = append(ops, deleteInt64Keys(binCancels, m.DeleteRequestCancelInfos)...)

	ops = append(ops, upsertInt64Blobs(binSignals, m.UpsertSignalInfos)...)
	ops = append(ops, deleteInt64Keys(binSignals, m.DeleteSignalInfos)...)

	for k, node := range m.UpsertChasmNodes {
		ops = append(ops, as.MapPutOp(kOrderedMap, binChasm, k, chasmValue(node)))
	}
	ops = append(ops, deleteStringKeys(binChasm, m.DeleteChasmNodes)...)

	for id := range m.UpsertSignalRequestedIDs {
		ops = append(ops, as.MapPutOp(kOrderedMap, binSignalReqs, id, 1))
	}
	ops = append(ops, deleteStringKeys(binSignalReqs, m.DeleteSignalRequestedIDs)...)

	// Buffered events: clear wins over append, matching Cassandra, where the
	// DELETE is applied before the append within the same batch.
	if m.ClearBufferedEvents {
		// Writing a nil bin deletes it, which is how a bin is cleared in
		// Aerospike. An empty list would be rejected as a parameter error.
		ops = append(ops, as.PutOp(as.NewBin(binBuffered, nil)))
	}
	if m.NewBufferedEvents != nil {
		ops = append(ops, as.ListAppendOp(binBuffered, blobValue(m.NewBufferedEvents)))
	}

	return ops
}

func checksumOps(checksum *commonpb.DataBlob) []*as.Operation {
	if checksum == nil {
		return []*as.Operation{
			as.PutOp(as.NewBin(binChecksum, nil)),
			as.PutOp(as.NewBin(binChecksumEnc, nil)),
		}
	}
	return []*as.Operation{
		as.PutOp(as.NewBin(binChecksum, checksum.Data)),
		as.PutOp(as.NewBin(binChecksumEnc, checksum.EncodingType.String())),
	}
}

// collectionBins are the map-valued bins on an execution record. Listed once so
// a snapshot can clear them all without the list drifting out of sync.
var collectionBins = []string{
	binActivities, binTimers, binChildren, binCancels,
	binSignals, binChasm, binSignalReqs,
}

// appendWholeMap writes a complete collection, or nothing when it is empty.
// The bin was already cleared by snapshotOps, so an empty collection is
// correctly represented by its absence. Writing an empty map instead is not an
// option -- the server rejects it with PARAMETER_ERROR.
func appendWholeMap(ops []*as.Operation, bin string, m map[any]any) []*as.Operation {
	if len(m) == 0 {
		return ops
	}
	return append(ops, as.MapPutItemsOp(kOrderedMap, bin, m))
}

func upsertInt64Blobs(bin string, blobs map[int64]*commonpb.DataBlob) []*as.Operation {
	ops := make([]*as.Operation, 0, len(blobs))
	for k, b := range blobs {
		ops = append(ops, as.MapPutOp(kOrderedMap, bin, k, blobValue(b)))
	}
	return ops
}

func upsertStringBlobs(bin string, blobs map[string]*commonpb.DataBlob) []*as.Operation {
	ops := make([]*as.Operation, 0, len(blobs))
	for k, b := range blobs {
		ops = append(ops, as.MapPutOp(kOrderedMap, bin, k, blobValue(b)))
	}
	return ops
}

func deleteInt64Keys(bin string, keys map[int64]struct{}) []*as.Operation {
	ops := make([]*as.Operation, 0, len(keys))
	for k := range keys {
		ops = append(ops, as.MapRemoveByKeyOp(bin, k, as.MapReturnType.NONE))
	}
	return ops
}

func deleteStringKeys(bin string, keys map[string]struct{}) []*as.Operation {
	ops := make([]*as.Operation, 0, len(keys))
	for k := range keys {
		ops = append(ops, as.MapRemoveByKeyOp(bin, k, as.MapReturnType.NONE))
	}
	return ops
}

func int64BlobMap(in map[int64]*commonpb.DataBlob) map[any]any {
	out := make(map[any]any, len(in))
	for k, v := range in {
		out[k] = blobValue(v)
	}
	return out
}

func stringBlobMap(in map[string]*commonpb.DataBlob) map[any]any {
	out := make(map[any]any, len(in))
	for k, v := range in {
		out[k] = blobValue(v)
	}
	return out
}

func chasmMap(in map[string]p.InternalChasmNode) map[any]any {
	out := make(map[any]any, len(in))
	for k, v := range in {
		out[k] = chasmValue(v)
	}
	return out
}

func signalRequestedMap(in map[string]struct{}) map[any]any {
	out := make(map[any]any, len(in))
	for k := range in {
		out[k] = 1
	}
	return out
}

// mutableStateFromRecord reverses the encoding above.
func mutableStateFromRecord(rec *as.Record) *p.InternalWorkflowMutableState {
	state := &p.InternalWorkflowMutableState{
		ExecutionInfo:   readBlob(rec, binExecInfo, binExecInfoEnc),
		ExecutionState:  readBlob(rec, binExecState, binExecStateEc),
		NextEventID:     binInt64(rec, binNextEventID),
		DBRecordVersion: binInt64(rec, binDBVersion),
		Checksum:        readBlob(rec, binChecksum, binChecksumEnc),

		ActivityInfos:       readInt64BlobMap(rec, binActivities),
		TimerInfos:          readStringBlobMap(rec, binTimers),
		ChildExecutionInfos: readInt64BlobMap(rec, binChildren),
		RequestCancelInfos:  readInt64BlobMap(rec, binCancels),
		SignalInfos:         readInt64BlobMap(rec, binSignals),
		ChasmNodes:          readChasmMap(rec, binChasm),
		SignalRequestedIDs:  readSignalRequested(rec, binSignalReqs),
		BufferedEvents:      readBufferedEvents(rec, binBuffered),
	}
	return state
}

// recordMapPairs normalises the two shapes an Aerospike map bin can take on
// read. This is easy to get wrong: a K-ordered map deserializes to
// []as.MapPair, preserving order, while an unordered one deserializes to
// map[any]any. Asserting only the latter silently yields an empty collection
// rather than an error.
func recordMapPairs(rec *as.Record, bin string) []as.MapPair {
	switch m := rec.Bins[bin].(type) {
	case []as.MapPair:
		return m
	case map[any]any:
		out := make([]as.MapPair, 0, len(m))
		for k, v := range m {
			out = append(out, as.MapPair{Key: k, Value: v})
		}
		return out
	default:
		return nil
	}
}

func readInt64BlobMap(rec *as.Record, bin string) map[int64]*commonpb.DataBlob {
	raw := recordMapPairs(rec, bin)
	if len(raw) == 0 {
		return nil
	}
	out := make(map[int64]*commonpb.DataBlob, len(raw))
	for _, pair := range raw {
		key, ok := asInt64(pair.Key)
		if !ok {
			continue
		}
		if blob := blobFromValue(pair.Value); blob != nil {
			out[key] = blob
		}
	}
	return out
}

func readStringBlobMap(rec *as.Record, bin string) map[string]*commonpb.DataBlob {
	raw := recordMapPairs(rec, bin)
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]*commonpb.DataBlob, len(raw))
	for _, pair := range raw {
		key, ok := pair.Key.(string)
		if !ok {
			continue
		}
		if blob := blobFromValue(pair.Value); blob != nil {
			out[key] = blob
		}
	}
	return out
}

func readChasmMap(rec *as.Record, bin string) map[string]p.InternalChasmNode {
	raw := recordMapPairs(rec, bin)
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]p.InternalChasmNode, len(raw))
	for _, pair := range raw {
		key, ok := pair.Key.(string)
		if !ok {
			continue
		}
		out[key] = chasmFromValue(pair.Value)
	}
	return out
}

func readSignalRequested(rec *as.Record, bin string) []string {
	raw := recordMapPairs(rec, bin)
	if len(raw) == 0 {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, pair := range raw {
		if key, ok := pair.Key.(string); ok {
			out = append(out, key)
		}
	}
	return out
}

func readBufferedEvents(rec *as.Record, bin string) []*commonpb.DataBlob {
	raw, _ := rec.Bins[bin].([]any)
	if len(raw) == 0 {
		return nil
	}
	out := make([]*commonpb.DataBlob, 0, len(raw))
	for _, v := range raw {
		if blob := blobFromValue(v); blob != nil {
			out = append(out, blob)
		}
	}
	return out
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	default:
		return 0, false
	}
}
