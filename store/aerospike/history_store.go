package aerospike

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
)

// History events use an index record plus one record per node.
//
// Cassandra's history_node table is clustered (node_id ASC, txn_id DESC) inside
// a tree partition, which answers "nodes in [min, max) in order, paginated"
// directly. Aerospike has no clustering key, and the obvious substitute --
// packing a whole branch into one bucketed CDT map -- is unsafe here in a way it
// is not for tasks: a history node carries a batch of workflow events and can be
// megabytes, while a task is a few hundred bytes. Bucketing those would risk the
// 8 MiB record ceiling, and every append would rewrite the whole bucket.
//
// So the ordering and the payload are separated:
//
//	hbranch/<tree>:<branch>   K-ordered map: sortKey -> prevTxnID   (the index)
//	hnode/<tree>:<branch>:<node>:<txn>   the event blob             (the payload)
//
// A read range-scans the index -- ordered and paginated by the server -- then
// batch-gets only the payloads for the page. Metadata-only reads never touch the
// payload records at all.
const (
	setHistoryTree   = "htree"
	setHistoryBranch = "hbranch"
	setHistoryNode   = "hnode"

	binBranchIndex = "idx"     // K-ordered map: sortKey -> prevTxnID
	binTreeBranch  = "br"      // K-ordered map: branchID -> serialized TreeInfo
	binTreeID      = "tree_id" // the tree's own id; the user key is not stored
	binPrevTxnID   = "prev_txn"
	binNodeID      = "node_id"
	binTxnID       = "txn_id"
)

// historyNodeSortKey encodes (nodeID ASC, txnID DESC) as 16 bytes.
//
// Aerospike compares BYTES map keys bytewise, and big-endian encoding of a
// non-negative int64 is order-preserving -- so the first eight bytes sort by
// node id ascending. Descending txn order is obtained by storing the
// complement: Cassandra clusters txn_id DESC because "for the same eventID, the
// node with the larger TransactionID always wins", and the reader relies on
// seeing that one first.
func historyNodeSortKey(nodeID, txnID int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(nodeID))
	binary.BigEndian.PutUint64(b[8:16], uint64(math.MaxInt64-txnID))
	return b
}

func decodeHistoryNodeSortKey(b []byte) (nodeID, txnID int64, ok bool) {
	if len(b) != 16 {
		return 0, 0, false
	}
	nodeID = int64(binary.BigEndian.Uint64(b[0:8]))
	txnID = math.MaxInt64 - int64(binary.BigEndian.Uint64(b[8:16]))
	return nodeID, txnID, true
}

func (k *keyBuilder) historyTreeKey(treeID string) (*as.Key, error) {
	return k.newKey(setHistoryTree, treeID)
}

func (k *keyBuilder) historyBranchKey(treeID, branchID string) (*as.Key, error) {
	return k.newKey(setHistoryBranch, fmt.Sprintf("%s:%s", treeID, branchID))
}

func (k *keyBuilder) historyNodeKey(treeID, branchID string, nodeID, txnID int64) (*as.Key, error) {
	return k.newKey(setHistoryNode, fmt.Sprintf("%s:%s:%d:%d", treeID, branchID, nodeID, txnID))
}

func (s *executionStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	t := s.begin()
	defer t.finish()

	if err := s.appendHistoryNodesIn(ctx, t, request); err != nil {
		return err
	}
	return t.commit("AppendHistoryNodes")
}

// appendHistoryNodesIn writes one history node inside a caller's transaction.
//
// Cassandra appends history nodes *before* the mutable-state batch, as separate
// unconditional writes, so a failed batch leaves orphaned nodes behind for the
// scavenger. Folding them into the same transaction removes that failure mode
// entirely -- events and mutable state now commit together or not at all.
func (s *executionStore) appendHistoryNodesIn(
	ctx context.Context,
	t *txnScope,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	branch := request.BranchInfo
	node := request.Node

	nodeKey, err := s.client.keys.historyNodeKey(branch.TreeId, branch.BranchId, node.NodeID, node.TransactionID)
	if err != nil {
		return err
	}
	indexKey, err := s.client.keys.historyBranchKey(branch.TreeId, branch.BranchId)
	if err != nil {
		return err
	}

	// Index entry and payload must land together: an index entry with no
	// payload is a phantom node, and a payload with no index entry is invisible
	// and leaks. A new branch additionally registers itself on the tree record.
	nodeBins := []*as.Bin{
		as.NewBin(binNodeID, node.NodeID),
		as.NewBin(binTxnID, node.TransactionID),
		as.NewBin(binPrevTxnID, node.PrevTransactionID),
		as.NewBin(binData, node.Events.Data),
		as.NewBin(binEncoding, node.Events.EncodingType.String()),
	}
	if err := s.client.as.PutBins(withCtxW(ctx, t.write), nodeKey, nodeBins...); err != nil {
		return convertError("AppendHistoryNodes", err)
	}

	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), indexKey,
		as.MapPutOp(kOrderedMap, binBranchIndex,
			historyNodeSortKey(node.NodeID, node.TransactionID), node.PrevTransactionID),
	); err != nil {
		return convertError("AppendHistoryNodes", err)
	}

	if request.IsNewBranch {
		treeKey, err := s.client.keys.historyTreeKey(branch.TreeId)
		if err != nil {
			return err
		}
		if _, err := s.client.as.Operate(withCtxW(ctx, t.write), treeKey,
			as.PutOp(as.NewBin(binTreeID, branch.TreeId)),
			as.MapPutOp(kOrderedMap, binTreeBranch, branch.BranchId, blobValue(request.TreeInfo)),
		); err != nil {
			return convertError("AppendHistoryNodes", err)
		}
	}

	return nil
}

func (s *executionStore) ReadHistoryBranch(
	ctx context.Context,
	request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {
	branch, err := s.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	indexKey, err := s.client.keys.historyBranchKey(branch.TreeId, request.BranchID)
	if err != nil {
		return nil, err
	}

	// Server-ordered range read. MapGetByKeyRange is begin-inclusive and
	// end-exclusive, which is exactly [MinNodeID, MaxNodeID).
	begin := historyNodeSortKey(request.MinNodeID, math.MaxInt64)
	end := historyNodeSortKey(request.MaxNodeID, math.MaxInt64)

	rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), indexKey,
		as.MapGetByKeyRangeOp(binBranchIndex, begin, end, as.MapReturnType.KEY_VALUE))
	if err != nil {
		if isNotFound(err) {
			return &p.InternalReadHistoryBranchResponse{}, nil
		}
		return nil, convertError("ReadHistoryBranch", err)
	}

	entries := recordMapPairs(rec, binBranchIndex)
	if len(entries) == 0 {
		return &p.InternalReadHistoryBranchResponse{}, nil
	}

	// The server returns ascending order; a reverse read wants the tail first.
	if request.ReverseOrder {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}

	// Our own pagination token: the sort key of the last entry returned. The
	// token is opaque to Temporal, so its format is ours to choose.
	start := 0
	if len(request.NextPageToken) > 0 {
		for i, e := range entries {
			key := asBytes(e.Key)
			if compareSortKeys(key, request.NextPageToken, request.ReverseOrder) > 0 {
				start = i
				break
			}
			start = i + 1
		}
	}
	entries = entries[start:]

	pageSize := request.PageSize
	var nextToken []byte
	if pageSize > 0 && len(entries) > pageSize {
		entries = entries[:pageSize]
		nextToken = asBytes(entries[len(entries)-1].Key)
	}

	nodes := make([]p.InternalHistoryNode, 0, len(entries))
	type pending struct {
		index int
		key   *as.Key
	}
	var toFetch []pending

	for _, e := range entries {
		raw := asBytes(e.Key)
		nodeID, txnID, ok := decodeHistoryNodeSortKey(raw)
		if !ok {
			continue
		}
		prevTxnID, _ := asInt64(e.Value)

		nodes = append(nodes, p.InternalHistoryNode{
			NodeID:            nodeID,
			TransactionID:     txnID,
			PrevTransactionID: prevTxnID,
		})

		if !request.MetadataOnly {
			key, err := s.client.keys.historyNodeKey(branch.TreeId, request.BranchID, nodeID, txnID)
			if err != nil {
				return nil, err
			}
			toFetch = append(toFetch, pending{index: len(nodes) - 1, key: key})
		}
	}

	// Metadata-only reads stop here, having touched exactly one record.
	if len(toFetch) > 0 {
		keys := make([]*as.Key, len(toFetch))
		for i, pf := range toFetch {
			keys[i] = pf.key
		}
		records, err := s.client.as.BatchGet(withCtxB(ctx, s.client.batch), keys, binData, binEncoding)
		if err != nil {
			return nil, convertError("ReadHistoryBranch", err)
		}
		for i, r := range records {
			if r == nil {
				// Index entry with no payload. Should be impossible given
				// AppendHistoryNodes writes both in one transaction; skip
				// rather than fabricate an empty event batch.
				continue
			}
			nodes[toFetch[i].index].Events = readBlob(r, binData, binEncoding)
		}
	}

	return &p.InternalReadHistoryBranchResponse{
		Nodes:         nodes,
		NextPageToken: nextToken,
	}, nil
}

func compareSortKeys(a, b []byte, reverse bool) int {
	cmp := 0
	switch {
	case len(a) != len(b):
		if len(a) < len(b) {
			cmp = -1
		} else {
			cmp = 1
		}
	default:
		for i := range a {
			if a[i] != b[i] {
				if a[i] < b[i] {
					cmp = -1
				} else {
					cmp = 1
				}
				break
			}
		}
	}
	if reverse {
		return -cmp
	}
	return cmp
}

// ForkHistoryBranch registers the new branch on the tree. The forked nodes are
// not copied -- a branch shares its ancestors' nodes, which is why
// DeleteHistoryBranch takes explicit ranges.
func (s *executionStore) ForkHistoryBranch(
	ctx context.Context,
	request *p.InternalForkHistoryBranchRequest,
) error {
	treeKey, err := s.client.keys.historyTreeKey(request.ForkBranchInfo.TreeId)
	if err != nil {
		return err
	}
	if _, err := s.client.as.Operate(withCtxW(ctx, s.client.write), treeKey,
		as.PutOp(as.NewBin(binTreeID, request.ForkBranchInfo.TreeId)),
		as.MapPutOp(kOrderedMap, binTreeBranch, request.NewBranchID, blobValue(request.TreeInfo)),
	); err != nil {
		return convertError("ForkHistoryBranch", err)
	}
	return nil
}

func (s *executionStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	branch := request.BranchInfo

	treeKey, err := s.client.keys.historyTreeKey(branch.TreeId)
	if err != nil {
		return err
	}

	t := s.begin()
	defer t.finish()

	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), treeKey,
		as.MapRemoveByKeyOp(binTreeBranch, branch.BranchId, as.MapReturnType.NONE),
	); err != nil && !isNotFound(err) {
		return convertError("DeleteHistoryBranch", err)
	}

	for _, br := range request.BranchRanges {
		if err := s.deleteBranchNodesFrom(ctx, t, branch.TreeId, br.BranchId, br.BeginNodeId); err != nil {
			return err
		}
	}

	return t.commit("DeleteHistoryBranch")
}

// deleteBranchNodesFrom removes every node at or after beginNodeID on a branch,
// index entries and payloads together.
//
// Cassandra expresses this as a single range tombstone. Aerospike has no
// equivalent, so the index is range-read to enumerate what exists and each
// payload is deleted explicitly -- the cost of not having range deletes.
func (s *executionStore) deleteBranchNodesFrom(
	ctx context.Context, t *txnScope, treeID, branchID string, beginNodeID int64,
) error {
	indexKey, err := s.client.keys.historyBranchKey(treeID, branchID)
	if err != nil {
		return err
	}

	begin := historyNodeSortKey(beginNodeID, math.MaxInt64)

	rec, err := s.client.as.Operate(withCtxW(ctx, t.write), indexKey,
		as.MapGetByKeyRangeOp(binBranchIndex, begin, nil, as.MapReturnType.KEY))
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return convertError("deleteBranchNodes", err)
	}

	for _, raw := range mapKeyList(rec, binBranchIndex) {
		nodeID, txnID, ok := decodeHistoryNodeSortKey(raw)
		if !ok {
			continue
		}
		nodeKey, err := s.client.keys.historyNodeKey(treeID, branchID, nodeID, txnID)
		if err != nil {
			return err
		}
		if _, err := s.client.as.Delete(withCtxW(ctx, t.del), nodeKey); err != nil && !isNotFound(err) {
			return convertError("deleteBranchNodes", err)
		}
	}

	// Nil as the range end means "to the end of the map".
	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), indexKey,
		as.MapRemoveByKeyRangeOp(binBranchIndex, begin, nil, as.MapReturnType.NONE),
	); err != nil && !isNotFound(err) {
		return convertError("deleteBranchNodes", err)
	}
	return nil
}

func (s *executionStore) DeleteHistoryNodes(
	ctx context.Context,
	request *p.InternalDeleteHistoryNodesRequest,
) error {
	branch := request.BranchInfo

	nodeKey, err := s.client.keys.historyNodeKey(
		branch.TreeId, branch.BranchId, request.NodeID, request.TransactionID)
	if err != nil {
		return err
	}
	indexKey, err := s.client.keys.historyBranchKey(branch.TreeId, branch.BranchId)
	if err != nil {
		return err
	}

	t := s.begin()
	defer t.finish()

	if _, err := s.client.as.Delete(withCtxW(ctx, t.del), nodeKey); err != nil && !isNotFound(err) {
		return convertError("DeleteHistoryNodes", err)
	}
	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), indexKey,
		as.MapRemoveByKeyOp(binBranchIndex,
			historyNodeSortKey(request.NodeID, request.TransactionID), as.MapReturnType.NONE),
	); err != nil && !isNotFound(err) {
		return convertError("DeleteHistoryNodes", err)
	}

	return t.commit("DeleteHistoryNodes")
}

func (s *executionStore) GetHistoryTreeContainingBranch(
	ctx context.Context,
	request *p.InternalGetHistoryTreeContainingBranchRequest,
) (*p.InternalGetHistoryTreeContainingBranchResponse, error) {
	branch, err := s.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	treeKey, err := s.client.keys.historyTreeKey(branch.TreeId)
	if err != nil {
		return nil, err
	}

	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), treeKey, binTreeBranch)
	if err != nil {
		if isNotFound(err) {
			return &p.InternalGetHistoryTreeContainingBranchResponse{}, nil
		}
		return nil, convertError("GetHistoryTreeContainingBranch", err)
	}

	var infos []*commonpb.DataBlob
	for _, pair := range recordMapPairs(rec, binTreeBranch) {
		if blob := blobFromValue(pair.Value); blob != nil {
			infos = append(infos, blob)
		}
	}
	return &p.InternalGetHistoryTreeContainingBranchResponse{TreeInfos: infos}, nil
}

// mapKeyList reads the []byte keys returned by a MapReturnType.KEY operation.
func mapKeyList(rec *as.Record, bin string) [][]byte {
	raw, ok := rec.Bins[bin].([]any)
	if !ok {
		return nil
	}
	out := make([][]byte, 0, len(raw))
	for _, v := range raw {
		if b := asBytes(v); b != nil {
			out = append(out, b)
		}
	}
	return out
}

// GetAllHistoryTreeBranches enumerates every branch of every tree.
//
// This is the one access pattern Aerospike genuinely cannot serve well: there
// is no key scope to narrow it, so it is a full set scan. It exists because the
// history scavenger and Temporal's own legacy test suite need it, not because
// it belongs on any hot path -- nothing in the workflow execution path calls it.
//
// A production deployment with many trees would want this replaced by an
// external index or a maintenance job driven by PartitionFilter, so the scan
// can be parallelised and resumed. See docs/04-open-questions.md.
func (s *executionStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {
	sp := as.NewScanPolicy()
	sp.TotalTimeout = s.client.cfg.TotalTimeout
	sp.SocketTimeout = s.client.cfg.SocketTimeout

	recordset, err := s.client.as.ScanAll(sp, s.client.keys.namespace, s.client.keys.set(setHistoryTree))
	if err != nil {
		return nil, convertError("GetAllHistoryTreeBranches", err)
	}
	defer func() { _ = recordset.Close() }()

	var all []p.InternalHistoryBranchDetail
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, convertError("GetAllHistoryTreeBranches", res.Err)
		}
		treeID := binString(res.Record, binTreeID)
		for _, pair := range recordMapPairs(res.Record, binTreeBranch) {
			branchID, ok := pair.Key.(string)
			if !ok {
				continue
			}
			blob := blobFromValue(pair.Value)
			if blob == nil {
				continue
			}
			all = append(all, p.InternalHistoryBranchDetail{
				TreeID:   treeID,
				BranchID: branchID,
				Encoding: blob.EncodingType.String(),
				Data:     blob.Data,
			})
		}
	}

	// Deterministic order so the cursor below is stable across calls.
	sortByDigest(all, func(b p.InternalHistoryBranchDetail) string {
		return b.TreeID + ":" + b.BranchID
	})

	start := pageStart(len(all), request.NextPageToken, func(i int) string {
		return all[i].TreeID + ":" + all[i].BranchID
	})
	end := pageEnd(start, request.PageSize, len(all))

	resp := &p.InternalGetAllHistoryTreeBranchesResponse{Branches: all[start:end]}
	if end < len(all) {
		last := all[end-1]
		resp.NextPageToken = []byte(last.TreeID + ":" + last.BranchID)
	}
	return resp, nil
}
