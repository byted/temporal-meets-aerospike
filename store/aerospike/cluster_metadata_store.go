package aerospike

import (
	"context"
	"net"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	p "go.temporal.io/server/common/persistence"
)

// clusterMetadataStore implements persistence.ClusterMetadataStore.
//
// Two unrelated concerns share this interface:
//
//   - Cluster metadata: one record per cluster, guarded by an optimistic-lock
//     version. Read at boot before any service starts.
//   - Cluster membership: one record per host, refreshed by heartbeat and
//     reaped by an explicit prune.
//
// Membership deliberately does not use Aerospike TTLs. The namespace runs with
// nsup-period 0 (no expiration reaper), because expiration interacts badly with
// durable deletes and transactions -- and a positive TTL would be rejected
// outright. Instead each record carries its own expiry timestamp, reads filter
// on it, and PruneClusterMembership deletes what has lapsed. Temporal calls
// prune on a timer and its own test suite drives prune explicitly, so nothing
// depends on server-side expiry.
type clusterMetadataStore struct {
	client *client
}

var _ p.ClusterMetadataStore = (*clusterMetadataStore)(nil)

func newClusterMetadataStore(c *client) *clusterMetadataStore {
	return &clusterMetadataStore{client: c}
}

func (s *clusterMetadataStore) GetName() string { return StoreName }
func (s *clusterMetadataStore) Close()          {}

func (s *clusterMetadataStore) GetClusterMetadata(
	ctx context.Context,
	request *p.InternalGetClusterMetadataRequest,
) (*p.InternalGetClusterMetadataResponse, error) {
	key, err := s.client.keys.clusterMetadataKey(request.ClusterName)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), key)
	if err != nil {
		return nil, convertError("GetClusterMetadata", err)
	}
	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: readBlob(rec, binData, binEncoding),
		Version:         binInt64(rec, binVersion),
	}, nil
}

// SaveClusterMetadata returns whether the write applied. Version 0 means
// "create if absent"; anything else is a compare-and-set on the stored version.
func (s *clusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *p.InternalSaveClusterMetadataRequest,
) (bool, error) {
	key, err := s.client.keys.clusterMetadataKey(request.ClusterName)
	if err != nil {
		return false, err
	}

	bins := append(
		blobBins(binData, binEncoding, request.ClusterMetadata),
		as.NewBin(binVersion, request.Version+1),
	)

	if request.Version == 0 {
		err = s.client.as.PutBins(withCtxW(ctx, s.client.create), key, bins...)
		if err == nil {
			return true, nil
		}
		if isKeyExists(err) {
			return false, nil
		}
		return false, convertError("SaveClusterMetadata", err)
	}

	policy := s.client.condWrite(
		as.ExpEq(as.ExpIntBin(binVersion), as.ExpIntVal(request.Version)))
	err = s.client.as.PutBins(withCtxW(ctx, policy), key, bins...)
	if err == nil {
		return true, nil
	}
	if isFilteredOut(err) || isNotFound(err) {
		return false, nil
	}
	return false, convertError("SaveClusterMetadata", err)
}

func (s *clusterMetadataStore) DeleteClusterMetadata(
	ctx context.Context,
	request *p.InternalDeleteClusterMetadataRequest,
) error {
	key, err := s.client.keys.clusterMetadataKey(request.ClusterName)
	if err != nil {
		return err
	}
	if _, err := s.client.as.Delete(withCtxW(ctx, s.client.delete), key); err != nil {
		return convertError("DeleteClusterMetadata", err)
	}
	return nil
}

func (s *clusterMetadataStore) ListClusterMetadata(
	ctx context.Context,
	request *p.InternalListClusterMetadataRequest,
) (*p.InternalListClusterMetadataResponse, error) {
	records, err := s.scanSet(ctx, setClusterMeta, "ListClusterMetadata")
	if err != nil {
		return nil, err
	}

	type entry struct {
		digest string
		md     *p.InternalGetClusterMetadataResponse
	}
	all := make([]entry, 0, len(records))
	for _, rec := range records {
		all = append(all, entry{
			digest: string(rec.Key.Digest()),
			md: &p.InternalGetClusterMetadataResponse{
				ClusterMetadata: readBlob(rec, binData, binEncoding),
				Version:         binInt64(rec, binVersion),
			},
		})
	}
	sortByDigest(all, func(e entry) string { return e.digest })

	start := pageStart(len(all), request.NextPageToken, func(i int) string { return all[i].digest })
	end := pageEnd(start, request.PageSize, len(all))

	resp := &p.InternalListClusterMetadataResponse{}
	for _, e := range all[start:end] {
		resp.ClusterMetadata = append(resp.ClusterMetadata, e.md)
	}
	if end < len(all) {
		resp.NextPageToken = []byte(all[end-1].digest)
	}
	return resp, nil
}

func (s *clusterMetadataStore) UpsertClusterMembership(
	ctx context.Context,
	request *p.UpsertClusterMembershipRequest,
) error {
	key, err := s.client.keys.clusterMemberKey(request.HostID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	// Expiry is stored rather than applied as a record TTL; see the type
	// comment for why.
	expiry := now.Add(request.RecordExpiry)

	bins := []*as.Bin{
		as.NewBin(binHostID, request.HostID),
		as.NewBin(binRole, int(request.Role)),
		as.NewBin(binRPCAddress, request.RPCAddress.String()),
		as.NewBin(binRPCPort, int(request.RPCPort)),
		as.NewBin(binSessionStart, request.SessionStart.UTC().UnixNano()),
		as.NewBin(binLastHeartbeat, now.UnixNano()),
		as.NewBin(binVersion, expiry.UnixNano()),
	}

	// A heartbeat replaces the whole record rather than merging, so a member
	// that changes role or address cannot leave a stale bin behind.
	if err := s.client.as.PutBins(withCtxW(ctx, s.client.replace), key, bins...); err != nil {
		return convertError("UpsertClusterMembership", err)
	}
	return nil
}

func (s *clusterMetadataStore) GetClusterMembers(
	ctx context.Context,
	request *p.GetClusterMembersRequest,
) (*p.GetClusterMembersResponse, error) {
	records, err := s.scanSet(ctx, setClusterMember, "GetClusterMembers")
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	type entry struct {
		digest string
		member *p.ClusterMember
	}
	var all []entry

	for _, rec := range records {
		m := memberFromRecord(rec)

		// Lapsed records are invisible even before prune removes them.
		if !m.RecordExpiry.After(now) {
			continue
		}
		if request.LastHeartbeatWithin > 0 &&
			m.LastHeartbeat.Before(now.Add(-request.LastHeartbeatWithin)) {
			continue
		}
		if len(request.HostIDEquals) > 0 && !bytesEqual(m.HostID, request.HostIDEquals) {
			continue
		}
		if request.RPCAddressEquals != nil && !m.RPCAddress.Equal(request.RPCAddressEquals) {
			continue
		}
		if request.RoleEquals != p.All && m.Role != request.RoleEquals {
			continue
		}
		if !request.SessionStartedAfter.IsZero() && !m.SessionStart.After(request.SessionStartedAfter) {
			continue
		}

		all = append(all, entry{digest: string(rec.Key.Digest()), member: m})
	}
	sortByDigest(all, func(e entry) string { return e.digest })

	start := pageStart(len(all), request.NextPageToken, func(i int) string { return all[i].digest })
	end := pageEnd(start, request.PageSize, len(all))

	resp := &p.GetClusterMembersResponse{}
	for _, e := range all[start:end] {
		resp.ActiveMembers = append(resp.ActiveMembers, e.member)
	}
	if end < len(all) {
		resp.NextPageToken = []byte(all[end-1].digest)
	}
	return resp, nil
}

func (s *clusterMetadataStore) PruneClusterMembership(
	ctx context.Context,
	request *p.PruneClusterMembershipRequest,
) error {
	records, err := s.scanSet(ctx, setClusterMember, "PruneClusterMembership")
	if err != nil {
		return err
	}

	now := time.Now().UTC().UnixNano()
	pruned := 0
	for _, rec := range records {
		if request.MaxRecordsPruned > 0 && pruned >= request.MaxRecordsPruned {
			break
		}
		if binInt64(rec, binVersion) > now {
			continue // still live
		}
		if _, err := s.client.as.Delete(withCtxW(ctx, s.client.delete), rec.Key); err != nil {
			if isNotFound(err) {
				continue // another pruner got there first
			}
			return convertError("PruneClusterMembership", err)
		}
		pruned++
	}
	return nil
}

// scanSet reads a whole set. Only used on administrative paths -- cluster
// metadata and membership are both tiny and read rarely. No workflow hot path
// scans anything.
func (s *clusterMetadataStore) scanSet(ctx context.Context, logicalSet, operation string) ([]*as.Record, error) {
	sp := as.NewScanPolicy()
	sp.TotalTimeout = s.client.cfg.TotalTimeout
	sp.SocketTimeout = s.client.cfg.SocketTimeout

	recordset, err := s.client.as.ScanAll(sp, s.client.keys.namespace, s.client.keys.set(logicalSet))
	if err != nil {
		return nil, convertError(operation, err)
	}
	defer func() { _ = recordset.Close() }()

	var out []*as.Record
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, convertError(operation, res.Err)
		}
		out = append(out, res.Record)
	}
	return out, nil
}

func memberFromRecord(rec *as.Record) *p.ClusterMember {
	return &p.ClusterMember{
		Role:          p.ServiceType(binInt64(rec, binRole)),
		HostID:        binBytes(rec, binHostID),
		RPCAddress:    net.ParseIP(binString(rec, binRPCAddress)),
		RPCPort:       uint16(binInt64(rec, binRPCPort)),
		SessionStart:  time.Unix(0, binInt64(rec, binSessionStart)).UTC(),
		LastHeartbeat: time.Unix(0, binInt64(rec, binLastHeartbeat)).UTC(),
		RecordExpiry:  time.Unix(0, binInt64(rec, binVersion)).UTC(),
	}
}

// pageStart resolves an opaque digest cursor to an index. Tokens are ours to
// define -- Temporal treats them as bytes.
func pageStart(n int, token []byte, digestAt func(int) string) int {
	if len(token) == 0 {
		return 0
	}
	cursor := string(token)
	for i := 0; i < n; i++ {
		if digestAt(i) > cursor {
			return i
		}
	}
	return n
}

func pageEnd(start, pageSize, n int) int {
	if pageSize <= 0 {
		return n
	}
	end := start + pageSize
	if end > n {
		return n
	}
	return end
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
