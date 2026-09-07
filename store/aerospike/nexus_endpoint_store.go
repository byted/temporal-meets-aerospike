package aerospike

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

// Nexus endpoints are a small, globally-versioned table: every mutation bumps a
// single table version, and readers use it to detect staleness. Cassandra keeps
// one row per endpoint plus a partition-status row holding that version; the
// shape here is the same, with the version on its own record.
const (
	setNexusEndpoint = "nexus"
	setNexusVersion  = "nexusver"

	binTableVersion = "tbl_version"
	binEndpointID   = "ep_id"
)

func (k *keyBuilder) nexusEndpointKey(id string) (*as.Key, error) {
	return k.newKey(setNexusEndpoint, id)
}

func (k *keyBuilder) nexusTableVersionKey() (*as.Key, error) {
	return k.newKey(setNexusVersion, "table")
}

type nexusEndpointStore struct{ client *client }

var _ p.NexusEndpointStore = (*nexusEndpointStore)(nil)

func newNexusEndpointStore(c *client) *nexusEndpointStore {
	return &nexusEndpointStore{client: c}
}

func (s *nexusEndpointStore) GetName() string { return StoreName }
func (s *nexusEndpointStore) Close()          {}

func (s *nexusEndpointStore) begin() *txnScope {
	txn := as.NewTxnWithCapacity(16, 16)
	read, write, del := s.client.txnPolicies(txn)
	return &txnScope{client: s.client, txn: txn, read: read, write: write, del: del}
}

func (s *nexusEndpointStore) tableVersion(ctx context.Context, pol *as.BasePolicy) (int64, error) {
	key, err := s.client.keys.nexusTableVersionKey()
	if err != nil {
		return 0, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, pol), key, binTableVersion)
	if err != nil {
		if isNotFound(err) {
			return 0, nil // no endpoint has ever been written
		}
		return 0, convertError("nexusTableVersion", err)
	}
	return binInt64(rec, binTableVersion), nil
}

func (s *nexusEndpointStore) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *p.InternalCreateOrUpdateNexusEndpointRequest,
) error {
	endpointKey, err := s.client.keys.nexusEndpointKey(request.Endpoint.ID)
	if err != nil {
		return err
	}
	versionKey, err := s.client.keys.nexusTableVersionKey()
	if err != nil {
		return err
	}

	// The endpoint row and the table version move together: a reader that saw
	// the new version must be able to see the endpoint that caused it.
	t := s.begin()
	defer t.finish()

	current, err := s.tableVersion(ctx, t.read)
	if err != nil {
		return err
	}
	if current != request.LastKnownTableVersion {
		return fmt.Errorf("%w: table version is %d, caller had %d",
			p.ErrNexusTableVersionConflict, current, request.LastKnownTableVersion)
	}

	// Endpoint version 0 means "must not already exist".
	existing, err := s.client.as.Get(withCtx(ctx, t.read), endpointKey, binVersion)
	switch {
	case err != nil && !isNotFound(err):
		return convertError("CreateOrUpdateNexusEndpoint", err)
	case err == nil && request.Endpoint.Version == 0:
		return p.ErrNexusEndpointVersionConflict
	case err == nil && binInt64(existing, binVersion) != request.Endpoint.Version:
		return p.ErrNexusEndpointVersionConflict
	case err != nil && request.Endpoint.Version != 0:
		return p.ErrNexusEndpointVersionConflict
	}

	bins := append(
		blobBins(binData, binEncoding, request.Endpoint.Data),
		as.NewBin(binVersion, request.Endpoint.Version+1),
		as.NewBin(binEndpointID, request.Endpoint.ID),
	)
	if err := s.client.as.PutBins(withCtxW(ctx, t.write), endpointKey, bins...); err != nil {
		return convertError("CreateOrUpdateNexusEndpoint", err)
	}
	if err := s.client.as.PutBins(withCtxW(ctx, t.write), versionKey,
		as.NewBin(binTableVersion, current+1)); err != nil {
		return convertError("CreateOrUpdateNexusEndpoint", err)
	}

	return t.commit("CreateOrUpdateNexusEndpoint")
}

func (s *nexusEndpointStore) DeleteNexusEndpoint(
	ctx context.Context,
	request *p.DeleteNexusEndpointRequest,
) error {
	endpointKey, err := s.client.keys.nexusEndpointKey(request.ID)
	if err != nil {
		return err
	}
	versionKey, err := s.client.keys.nexusTableVersionKey()
	if err != nil {
		return err
	}

	t := s.begin()
	defer t.finish()

	current, err := s.tableVersion(ctx, t.read)
	if err != nil {
		return err
	}
	if current != request.LastKnownTableVersion {
		return fmt.Errorf("%w: table version is %d, caller had %d",
			p.ErrNexusTableVersionConflict, current, request.LastKnownTableVersion)
	}

	if _, err := s.client.as.Get(withCtx(ctx, t.read), endpointKey, binVersion); err != nil {
		if isNotFound(err) {
			// Wording matters: the suite matches on "nexus endpoint not found".
			return serviceerror.NewNotFoundf("nexus endpoint not found, id: %v", request.ID)
		}
		return convertError("DeleteNexusEndpoint", err)
	}

	if _, err := s.client.as.Delete(withCtxW(ctx, t.del), endpointKey); err != nil {
		return convertError("DeleteNexusEndpoint", err)
	}
	if err := s.client.as.PutBins(withCtxW(ctx, t.write), versionKey,
		as.NewBin(binTableVersion, current+1)); err != nil {
		return convertError("DeleteNexusEndpoint", err)
	}

	return t.commit("DeleteNexusEndpoint")
}

func (s *nexusEndpointStore) GetNexusEndpoint(
	ctx context.Context,
	request *p.GetNexusEndpointRequest,
) (*p.InternalNexusEndpoint, error) {
	key, err := s.client.keys.nexusEndpointKey(request.ID)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key)
	if err != nil {
		if isNotFound(err) {
			return nil, serviceerror.NewNotFoundf("nexus endpoint not found, id: %v", request.ID)
		}
		return nil, convertError("GetNexusEndpoint", err)
	}
	return &p.InternalNexusEndpoint{
		ID:      request.ID,
		Version: binInt64(rec, binVersion),
		Data:    readBlob(rec, binData, binEncoding),
	}, nil
}

// ListNexusEndpoints must never fail on an empty table: the frontend's endpoint
// registry long-polls it at startup and treats an error as fatal.
func (s *nexusEndpointStore) ListNexusEndpoints(
	ctx context.Context,
	request *p.ListNexusEndpointsRequest,
) (*p.InternalListNexusEndpointsResponse, error) {
	current, err := s.tableVersion(ctx, s.client.read)
	if err != nil {
		return nil, err
	}
	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != current {
		return nil, fmt.Errorf("%w: table version is %d, caller had %d",
			p.ErrNexusTableVersionConflict, current, request.LastKnownTableVersion)
	}

	sp := as.NewScanPolicy()
	sp.TotalTimeout = s.client.cfg.TotalTimeout

	recordset, err := s.client.as.ScanAll(sp, s.client.keys.namespace, s.client.keys.set(setNexusEndpoint))
	if err != nil {
		return nil, convertError("ListNexusEndpoints", err)
	}
	defer func() { _ = recordset.Close() }()

	var all []p.InternalNexusEndpoint
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, convertError("ListNexusEndpoints", res.Err)
		}
		all = append(all, p.InternalNexusEndpoint{
			ID:      binString(res.Record, binEndpointID),
			Version: binInt64(res.Record, binVersion),
			Data:    readBlob(res.Record, binData, binEncoding),
		})
	}
	sortByDigest(all, func(e p.InternalNexusEndpoint) string { return e.ID })

	start := pageStart(len(all), request.NextPageToken, func(i int) string { return all[i].ID })
	end := pageEnd(start, request.PageSize, len(all))

	resp := &p.InternalListNexusEndpointsResponse{
		TableVersion: current,
		Endpoints:    all[start:end],
	}
	if end < len(all) {
		resp.NextPageToken = []byte(all[end-1].ID)
	}
	return resp, nil
}
