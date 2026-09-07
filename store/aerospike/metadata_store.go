package aerospike

import (
	"context"

	as "github.com/aerospike/aerospike-client-go/v8"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

// metadataStore implements persistence.MetadataStore.
//
// The model mirrors Cassandra's two tables plus a counter:
//
//	ns     name -> {record, notification_version, is_global}
//	nsid   id   -> name
//	nsmeta "metadata" -> notification_version
//
// Cassandra cannot write the first two atomically, so it inserts into nsid,
// then conditionally into ns, and on failure runs a best-effort compensating
// delete -- leaving an orphan behind if that delete also fails, which its own
// comment acknowledges may need a background cleanup job.
//
// Aerospike's multi-record transactions remove that whole class of problem:
// all three records move together or not at all.
type metadataStore struct {
	client *client
}

var _ p.MetadataStore = (*metadataStore)(nil)

func newMetadataStore(c *client) *metadataStore { return &metadataStore{client: c} }

func (m *metadataStore) GetName() string { return StoreName }
func (m *metadataStore) Close()          {}

func (m *metadataStore) CreateNamespace(
	ctx context.Context,
	request *p.InternalCreateNamespaceRequest,
) (*p.CreateNamespaceResponse, error) {
	nameKey, err := m.client.keys.namespaceKey(request.Name)
	if err != nil {
		return nil, err
	}
	idKey, err := m.client.keys.namespaceByIDKey(request.ID)
	if err != nil {
		return nil, err
	}
	metaKey, err := m.client.keys.namespaceMetadataKey()
	if err != nil {
		return nil, err
	}

	metadata, err := m.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}

	txn := as.NewTxnWithCapacity(16, 16)
	_, txnWrite, _ := m.client.txnPolicies(txn)

	abort := func() {
		if _, aerr := m.client.as.Abort(txn); aerr != nil {
			// Nothing useful to do; the transaction expires on its own.
			_ = aerr
		}
	}

	// Insert-if-absent on both the name and the id record. Either collision
	// means the namespace already exists.
	createPolicy := *txnWrite
	createPolicy.RecordExistsAction = as.CREATE_ONLY

	nameBins := append(
		blobBins(binData, binEncoding, request.Namespace),
		as.NewBin(binNotifVersion, metadata.NotificationVersion),
		as.NewBin(binIsGlobal, request.IsGlobal),
		as.NewBin(binNamespaceID, request.ID),
	)
	if err := m.client.as.PutBins(withCtxW(ctx, &createPolicy), nameKey, nameBins...); err != nil {
		abort()
		if isKeyExists(err) {
			return nil, serviceerror.NewNamespaceAlreadyExistsf(
				"namespace already exists. Name: %q", request.Name)
		}
		return nil, convertError("CreateNamespace", err)
	}

	if err := m.client.as.PutBins(withCtxW(ctx, &createPolicy), idKey,
		as.NewBin(binNamespaceName, request.Name)); err != nil {
		abort()
		if isKeyExists(err) {
			return nil, serviceerror.NewNamespaceAlreadyExistsf(
				"namespace already exists. NamespaceId: %v", request.ID)
		}
		return nil, convertError("CreateNamespace", err)
	}

	if err := m.bumpNotificationVersion(ctx, metaKey, metadata.NotificationVersion, txn); err != nil {
		abort()
		return nil, err
	}

	if _, err := m.client.as.Commit(txn); err != nil {
		return nil, m.conflictOrError("CreateNamespace", err)
	}

	return &p.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *metadataStore) GetNamespace(
	ctx context.Context,
	request *p.GetNamespaceRequest,
) (*p.InternalGetNamespaceResponse, error) {
	switch {
	case request.ID != "" && request.Name != "":
		return nil, serviceerror.NewInvalidArgument(
			"GetNamespace operation failed. Both ID and Name specified in request.")
	case request.ID == "" && request.Name == "":
		return nil, serviceerror.NewInvalidArgument(
			"GetNamespace operation failed. Both ID and Name are empty.")
	}

	name := request.Name
	if request.ID != "" {
		idKey, err := m.client.keys.namespaceByIDKey(request.ID)
		if err != nil {
			return nil, err
		}
		rec, err := m.client.as.Get(withCtx(ctx, m.client.read), idKey, binNamespaceName)
		if err != nil {
			if isNotFound(err) {
				return nil, serviceerror.NewNamespaceNotFound(request.ID)
			}
			return nil, convertError("GetNamespace", err)
		}
		name = binString(rec, binNamespaceName)
	}

	nameKey, err := m.client.keys.namespaceKey(name)
	if err != nil {
		return nil, err
	}
	rec, err := m.client.as.Get(withCtx(ctx, m.client.read), nameKey)
	if err != nil {
		if isNotFound(err) {
			if request.ID != "" {
				// The id pointer resolved but the record is missing. Under a
				// transaction that should be impossible, so treat it as
				// transient rather than reporting a definitive "not found".
				return nil, serviceerror.NewUnavailable("namespace info temporarily unavailable")
			}
			return nil, serviceerror.NewNamespaceNotFound(name)
		}
		return nil, convertError("GetNamespace", err)
	}

	return namespaceFromRecord(rec), nil
}

func (m *metadataStore) UpdateNamespace(
	ctx context.Context,
	request *p.InternalUpdateNamespaceRequest,
) error {
	nameKey, err := m.client.keys.namespaceKey(request.Name)
	if err != nil {
		return err
	}
	metaKey, err := m.client.keys.namespaceMetadataKey()
	if err != nil {
		return err
	}

	txn := as.NewTxnWithCapacity(16, 16)
	_, txnWrite, _ := m.client.txnPolicies(txn)

	bins := append(
		blobBins(binData, binEncoding, request.Namespace),
		as.NewBin(binNotifVersion, request.NotificationVersion),
		as.NewBin(binIsGlobal, request.IsGlobal),
		as.NewBin(binNamespaceID, request.Id),
	)

	if err := m.client.as.PutBins(withCtxW(ctx, txnWrite), nameKey, bins...); err != nil {
		_, _ = m.client.as.Abort(txn)
		return convertError("UpdateNamespace", err)
	}

	if err := m.bumpNotificationVersion(ctx, metaKey, request.NotificationVersion, txn); err != nil {
		_, _ = m.client.as.Abort(txn)
		return err
	}

	if _, err := m.client.as.Commit(txn); err != nil {
		return m.conflictOrError("UpdateNamespace", err)
	}
	return nil
}

// RenameNamespace moves the name-keyed record and repoints the id record.
// Cassandra warns this can leave the database inconsistent and must be retried
// until it succeeds; here the whole rename is one transaction.
func (m *metadataStore) RenameNamespace(
	ctx context.Context,
	request *p.InternalRenameNamespaceRequest,
) error {
	oldKey, err := m.client.keys.namespaceKey(request.PreviousName)
	if err != nil {
		return err
	}
	newKey, err := m.client.keys.namespaceKey(request.Name)
	if err != nil {
		return err
	}
	idKey, err := m.client.keys.namespaceByIDKey(request.Id)
	if err != nil {
		return err
	}
	metaKey, err := m.client.keys.namespaceMetadataKey()
	if err != nil {
		return err
	}

	txn := as.NewTxnWithCapacity(16, 16)
	_, txnWrite, txnDelete := m.client.txnPolicies(txn)
	fail := func(op string, err error) error {
		_, _ = m.client.as.Abort(txn)
		return convertError(op, err)
	}

	bins := append(
		blobBins(binData, binEncoding, request.Namespace),
		as.NewBin(binNotifVersion, request.NotificationVersion),
		as.NewBin(binIsGlobal, request.IsGlobal),
		as.NewBin(binNamespaceID, request.Id),
	)

	// The destination name must be free. Cassandra gets this from IF NOT
	// EXISTS; CREATE_ONLY is the same condition, and it also rejects renaming
	// a namespace to the name it already has -- which the suite asserts.
	createNew := *txnWrite
	createNew.RecordExistsAction = as.CREATE_ONLY

	if err := m.client.as.PutBins(withCtxW(ctx, &createNew), newKey, bins...); err != nil {
		_, _ = m.client.as.Abort(txn)
		if isKeyExists(err) {
			return serviceerror.NewUnavailablef(
				"RenameNamespace failed: namespace %q already exists", request.Name)
		}
		return convertError("RenameNamespace", err)
	}
	if _, err := m.client.as.Delete(withCtxW(ctx, txnDelete), oldKey); err != nil {
		return fail("RenameNamespace", err)
	}
	if err := m.client.as.PutBins(withCtxW(ctx, txnWrite), idKey,
		as.NewBin(binNamespaceName, request.Name)); err != nil {
		return fail("RenameNamespace", err)
	}
	if err := m.bumpNotificationVersion(ctx, metaKey, request.NotificationVersion, txn); err != nil {
		_, _ = m.client.as.Abort(txn)
		return err
	}

	if _, err := m.client.as.Commit(txn); err != nil {
		return m.conflictOrError("RenameNamespace", err)
	}
	return nil
}

func (m *metadataStore) DeleteNamespace(
	ctx context.Context,
	request *p.DeleteNamespaceRequest,
) error {
	idKey, err := m.client.keys.namespaceByIDKey(request.ID)
	if err != nil {
		return err
	}
	rec, err := m.client.as.Get(withCtx(ctx, m.client.read), idKey, binNamespaceName)
	if err != nil {
		if isNotFound(err) {
			return nil // already gone
		}
		return convertError("DeleteNamespace", err)
	}
	return m.deleteNamespace(ctx, binString(rec, binNamespaceName), request.ID)
}

func (m *metadataStore) DeleteNamespaceByName(
	ctx context.Context,
	request *p.DeleteNamespaceByNameRequest,
) error {
	nameKey, err := m.client.keys.namespaceKey(request.Name)
	if err != nil {
		return err
	}
	rec, err := m.client.as.Get(withCtx(ctx, m.client.read), nameKey, binNamespaceID)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return convertError("DeleteNamespaceByName", err)
	}

	// The id is carried on the record rather than decoded from the opaque
	// namespace detail. Note that Aerospike does not store the user key with a
	// record unless sendKey is set, so recovering the id from the key of the
	// nsid record is not an option.
	return m.deleteNamespace(ctx, request.Name, binString(rec, binNamespaceID))
}

func (m *metadataStore) deleteNamespace(ctx context.Context, name, id string) error {
	nameKey, err := m.client.keys.namespaceKey(name)
	if err != nil {
		return err
	}
	metaKey, err := m.client.keys.namespaceMetadataKey()
	if err != nil {
		return err
	}

	metadata, err := m.GetMetadata(ctx)
	if err != nil {
		return err
	}

	txn := as.NewTxnWithCapacity(16, 16)
	_, _, txnDelete := m.client.txnPolicies(txn)

	if _, err := m.client.as.Delete(withCtxW(ctx, txnDelete), nameKey); err != nil {
		_, _ = m.client.as.Abort(txn)
		return convertError("DeleteNamespace", err)
	}
	if id != "" {
		idKey, err := m.client.keys.namespaceByIDKey(id)
		if err != nil {
			_, _ = m.client.as.Abort(txn)
			return err
		}
		if _, err := m.client.as.Delete(withCtxW(ctx, txnDelete), idKey); err != nil {
			_, _ = m.client.as.Abort(txn)
			return convertError("DeleteNamespace", err)
		}
	}
	if err := m.bumpNotificationVersion(ctx, metaKey, metadata.NotificationVersion, txn); err != nil {
		_, _ = m.client.as.Abort(txn)
		return err
	}

	if _, err := m.client.as.Commit(txn); err != nil {
		return m.conflictOrError("DeleteNamespace", err)
	}
	return nil
}

func (m *metadataStore) ListNamespaces(
	ctx context.Context,
	request *p.InternalListNamespacesRequest,
) (*p.InternalListNamespacesResponse, error) {
	// Namespaces are few and this is an administrative path, so a set scan is
	// acceptable here in a way it would not be on a workflow hot path.
	//
	// Aerospike scans return records in digest order, which is stable for a
	// fixed record set. We page on that order using the digest as the cursor.
	sp := as.NewScanPolicy()
	sp.TotalTimeout = m.client.cfg.TotalTimeout
	sp.SocketTimeout = m.client.cfg.SocketTimeout

	recordset, err := m.client.as.ScanAll(sp, m.client.keys.namespace, m.client.keys.set(setNamespace))
	if err != nil {
		return nil, convertError("ListNamespaces", err)
	}
	defer func() { _ = recordset.Close() }()

	type entry struct {
		digest string
		ns     *p.InternalGetNamespaceResponse
	}
	var all []entry
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, convertError("ListNamespaces", res.Err)
		}
		all = append(all, entry{
			digest: string(res.Record.Key.Digest()),
			ns:     namespaceFromRecord(res.Record),
		})
	}

	// Sort by digest so paging is deterministic across calls.
	sortByDigest(all, func(e entry) string { return e.digest })

	start := 0
	if len(request.NextPageToken) > 0 {
		token := string(request.NextPageToken)
		for i, e := range all {
			if e.digest > token {
				start = i
				break
			}
			start = i + 1
		}
	}

	pageSize := request.PageSize
	if pageSize <= 0 {
		pageSize = len(all)
	}
	end := start + pageSize
	if end > len(all) {
		end = len(all)
	}

	resp := &p.InternalListNamespacesResponse{}
	for _, e := range all[start:end] {
		resp.Namespaces = append(resp.Namespaces, e.ns)
	}
	if end < len(all) {
		resp.NextPageToken = []byte(all[end-1].digest)
	}
	return resp, nil
}

func (m *metadataStore) GetMetadata(ctx context.Context) (*p.GetMetadataResponse, error) {
	key, err := m.client.keys.namespaceMetadataKey()
	if err != nil {
		return nil, err
	}
	rec, err := m.client.as.Get(withCtx(ctx, m.client.read), key, binNotifVersion)
	if err != nil {
		if isNotFound(err) {
			// Expected on a fresh cluster, before any namespace exists.
			return &p.GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, convertError("GetMetadata", err)
	}
	return &p.GetMetadataResponse{NotificationVersion: binInt64(rec, binNotifVersion)}, nil
}

// bumpNotificationVersion advances the counter, conditional on it still being
// where the caller thought. This is the serialization point for all namespace
// mutations -- it is what makes concurrent creates conflict rather than
// interleave.
//
// Version 0 means the record does not exist yet, so the first bump is an
// insert; later bumps are conditional updates.
func (m *metadataStore) bumpNotificationVersion(
	ctx context.Context,
	key *as.Key,
	expected int64,
	txn *as.Txn,
) error {
	if expected == 0 {
		policy := *m.client.write
		policy.Txn = txn
		policy.RecordExistsAction = as.CREATE_ONLY
		err := m.client.as.PutBins(withCtxW(ctx, &policy), key, as.NewBin(binNotifVersion, int64(1)))
		if err == nil {
			return nil
		}
		if isKeyExists(err) {
			return serviceerror.NewUnavailable(
				"namespace metadata changed concurrently; retry")
		}
		return convertError("bumpNotificationVersion", err)
	}

	policy := m.client.condWriteTxn(
		as.ExpEq(as.ExpIntBin(binNotifVersion), as.ExpIntVal(expected)), txn)
	err := m.client.as.PutBins(withCtxW(ctx, policy), key,
		as.NewBin(binNotifVersion, expected+1))
	if err == nil {
		return nil
	}
	if isFilteredOut(err) {
		return serviceerror.NewUnavailable(
			"namespace metadata changed concurrently; retry")
	}
	return convertError("bumpNotificationVersion", err)
}

// conflictOrError turns a failed commit into the right Temporal error. A
// transaction that lost its commit-time verification is a conditional failure,
// which Temporal retries.
func (m *metadataStore) conflictOrError(operation string, err error) error {
	if isTxnConflict(err) || isFilteredOut(err) {
		return serviceerror.NewUnavailablef(
			"%s failed because of conditional failure", operation)
	}
	return convertError(operation, err)
}

func namespaceFromRecord(rec *as.Record) *p.InternalGetNamespaceResponse {
	return &p.InternalGetNamespaceResponse{
		Namespace:           readBlob(rec, binData, binEncoding),
		IsGlobal:            binBool(rec, binIsGlobal),
		NotificationVersion: binInt64(rec, binNotifVersion),
	}
}

// sortByDigest is a tiny insertion sort; namespace counts are small enough
// that pulling in a generic sort with a closure is not worth the indirection.
func sortByDigest[T any](xs []T, keyOf func(T) string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && keyOf(xs[j]) < keyOf(xs[j-1]); j-- {
			xs[j], xs[j-1] = xs[j-1], xs[j]
		}
	}
}
