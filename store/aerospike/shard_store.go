package aerospike

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	p "go.temporal.io/server/common/persistence"
)

// shardStore implements persistence.ShardStore.
//
// A shard record is the lease for one history shard: a monotonically
// increasing range_id plus the serialized ShardInfo. Every mutating operation
// elsewhere in the store fences against it, so this is the smallest and most
// contended record in the system.
type shardStore struct {
	client      *client
	clusterName string
}

var _ p.ShardStore = (*shardStore)(nil)

func newShardStore(c *client, clusterName string) *shardStore {
	return &shardStore{client: c, clusterName: clusterName}
}

func (s *shardStore) GetName() string        { return StoreName }
func (s *shardStore) GetClusterName() string { return s.clusterName }
func (s *shardStore) Close()                 {} // the factory owns the client

func (s *shardStore) GetOrCreateShard(
	ctx context.Context,
	request *p.InternalGetOrCreateShardRequest,
) (*p.InternalGetOrCreateShardResponse, error) {
	key, err := s.client.keys.shardKey(request.ShardID)
	if err != nil {
		return nil, err
	}

	// Ownership reads are the one place session consistency is not enough: we
	// must not hand back a stale view of who holds the lease.
	rec, err := s.client.as.Get(withCtx(ctx, s.client.readLinearize), key, binData, binEncoding)
	switch {
	case err == nil:
		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: readBlob(rec, binData, binEncoding),
		}, nil
	case !isNotFound(err):
		return nil, convertError("GetOrCreateShard", err)
	case request.CreateShardInfo == nil:
		// Caller only wanted to read, and there is nothing there.
		return nil, convertError("GetOrCreateShard", err)
	}

	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, err
	}

	bins := append(
		blobBins(binData, binEncoding, shardInfo),
		as.NewBin(binRangeID, rangeID),
	)

	err = s.client.as.PutBins(withCtxW(ctx, s.client.create), key, bins...)
	if err != nil {
		if isKeyExists(err) {
			// Someone created it between our read and our write. Re-read, and
			// clear the constructor so this cannot loop.
			request.CreateShardInfo = nil
			return s.GetOrCreateShard(ctx, request)
		}
		return nil, convertError("GetOrCreateShard", err)
	}

	return &p.InternalGetOrCreateShardResponse{ShardInfo: shardInfo}, nil
}

func (s *shardStore) UpdateShard(
	ctx context.Context,
	request *p.InternalUpdateShardRequest,
) error {
	key, err := s.client.keys.shardKey(request.ShardID)
	if err != nil {
		return err
	}

	bins := append(
		blobBins(binData, binEncoding, request.ShardInfo),
		as.NewBin(binRangeID, request.RangeID),
	)
	if request.Owner != "" {
		bins = append(bins, as.NewBin(binOwner, request.Owner))
	}

	// The value condition Temporal's protocol actually specifies:
	// "UPDATE ... IF range_id = <previous>".
	policy := s.client.condWrite(rangeIDEquals(request.PreviousRangeID))

	err = s.client.as.PutBins(withCtxW(ctx, policy), key, bins...)
	if err == nil {
		return nil
	}

	if isFilteredOut(err) || isNotFound(err) {
		return s.shardOwnershipLost(ctx, key, request)
	}
	return convertError("UpdateShard", err)
}

// AssertShardOwnership is a no-op, matching the Cassandra store. Ownership is
// asserted by the range_id condition carried on every mutating write, so a
// separate probe would add a round trip without adding a guarantee.
func (s *shardStore) AssertShardOwnership(
	ctx context.Context,
	request *p.AssertShardOwnershipRequest,
) error {
	return nil
}

// shardOwnershipLost builds the error Temporal expects when the lease has
// moved. Aerospike, unlike a Cassandra LWT, does not return the conflicting
// record alongside the rejection, so we read it back to say what the range_id
// actually was. A best-effort read: the message is diagnostic, and failing to
// produce it must not mask the real error.
func (s *shardStore) shardOwnershipLost(
	ctx context.Context,
	key *as.Key,
	request *p.InternalUpdateShardRequest,
) error {
	actual := "unknown"
	if rec, err := s.client.as.Get(withCtx(ctx, s.client.readLinearize), key, binRangeID, binOwner); err == nil && rec != nil {
		actual = fmt.Sprintf("range_id=%d owner=%q", binInt64(rec, binRangeID), binString(rec, binOwner))
	} else if err != nil && isNotFound(err) {
		actual = "record missing"
	}

	return &p.ShardOwnershipLostError{
		ShardID: request.ShardID,
		Msg: fmt.Sprintf(
			"failed to update shard: expected range_id=%d, found %s",
			request.PreviousRangeID, actual,
		),
	}
}

// rangeIDEquals is the shard-lease fence, shared by every store that mutates
// shard-scoped data.
func rangeIDEquals(rangeID int64) *as.Expression {
	return as.ExpEq(as.ExpIntBin(binRangeID), as.ExpIntVal(rangeID))
}
