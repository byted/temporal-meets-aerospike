package aerospike

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
)

// client wraps the Aerospike client with the policies this store always wants.
//
// One client per process is the documented rule -- it is thread-safe and owns
// the connection pools and cluster state. Creating one per request is a known
// cause of port exhaustion. The DataStoreFactory owns exactly one and closes
// it on shutdown.
type client struct {
	as   *as.Client
	cfg  *Config
	keys *keyBuilder

	// Policies are built once and reused. Allocating a policy per call is an
	// documented anti-pattern on hot paths.
	read   *as.BasePolicy
	// readLinearize is for shard-ownership reads, where session consistency is
	// not enough: we must not act on a stale view of who owns the lease.
	readLinearize *as.BasePolicy
	write  *as.WritePolicy
	create *as.WritePolicy // CREATE_ONLY: insert-if-absent
	replace *as.WritePolicy // REPLACE: full overwrite, drops absent bins
	delete *as.WritePolicy
	batch  *as.BatchPolicy
	info   *as.InfoPolicy
}

func newClient(cfg *Config) (*client, error) {
	hosts, err := parseHosts(cfg.Hosts)
	if err != nil {
		return nil, err
	}

	cp := as.NewClientPolicy()
	cp.Timeout = cfg.ConnectTimeout
	cp.MinConnectionsPerNode = cfg.MinConnsPerNode
	cp.ConnectionQueueSize = cfg.MaxConnsPerNode
	cp.UseServicesAlternate = cfg.UseServicesAlternate
	if cfg.User != "" {
		cp.User = cfg.User
		cp.Password = cfg.Password
	}

	asClient, err := as.NewClientWithPolicyAndHost(cp, hosts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to Aerospike at %v: %w", cfg.Hosts, err)
	}

	c := &client{as: asClient, cfg: cfg, keys: newKeyBuilder(cfg)}
	c.buildPolicies()

	// Set the client-level defaults too, so that any call passing nil still
	// gets our timeouts rather than the library's.
	asClient.DefaultPolicy = c.read
	asClient.DefaultWritePolicy = c.write
	asClient.DefaultBatchPolicy = c.batch
	asClient.DefaultInfoPolicy = c.info

	return c, nil
}

func (c *client) buildPolicies() {
	base := func() *as.BasePolicy {
		bp := as.NewPolicy()
		bp.SocketTimeout = c.cfg.SocketTimeout
		bp.TotalTimeout = c.cfg.TotalTimeout
		// Session consistency is the SC default: monotonic reads and
		// read-your-writes, which is what most of the store needs.
		bp.ReadModeSC = as.ReadModeSCSession
		return bp
	}

	c.read = base()

	c.readLinearize = base()
	c.readLinearize.ReadModeSC = as.ReadModeSCLinearize

	newWrite := func() *as.WritePolicy {
		wp := as.NewWritePolicy(0, 0)
		wp.SocketTimeout = c.cfg.SocketTimeout
		wp.TotalTimeout = c.cfg.TotalTimeout
		// COMMIT_ALL is mandatory in a strong-consistency namespace;
		// COMMIT_MASTER is rejected outright.
		wp.CommitLevel = as.COMMIT_ALL
		// Never blind-retry a write. Every write in this store is either
		// conditional or part of a transaction, and replaying one is a
		// correctness bug rather than a latency win.
		wp.MaxRetries = 0
		return wp
	}

	c.write = newWrite()

	c.create = newWrite()
	c.create.RecordExistsAction = as.CREATE_ONLY

	c.replace = newWrite()
	c.replace.RecordExistsAction = as.REPLACE

	c.delete = newWrite()
	// Durable deletes are required inside a transaction, and are what stop a
	// deleted record reappearing after a cold restart. Enterprise-only.
	c.delete.DurableDelete = true

	c.batch = as.NewBatchPolicy()
	c.batch.SocketTimeout = c.cfg.SocketTimeout
	c.batch.TotalTimeout = c.cfg.TotalTimeout
	c.batch.ReadModeSC = as.ReadModeSCSession

	c.info = as.NewInfoPolicy()
	c.info.Timeout = c.cfg.SocketTimeout
}

func (c *client) Close() {
	if c.as != nil {
		c.as.Close()
	}
}

// casWrite returns a write policy that fails unless the record's generation
// still matches gen. This is how every optimistic lock in the store is
// expressed -- Temporal's rangeID and version fields are CAS tokens.
func (c *client) casWrite(gen uint32) *as.WritePolicy {
	wp := *c.write
	wp.GenerationPolicy = as.EXPECT_GEN_EQUAL
	wp.Generation = gen
	wp.RecordExistsAction = as.UPDATE
	return &wp
}

// condWrite returns a write policy whose write only applies if expr evaluates
// true on the server. This is the store's workhorse conditional: it is a
// *value* condition, which is what Temporal's protocol actually specifies
// ("IF range_id = ?", "IF db_record_version = ?", "IF current_run_id = ?").
//
// Generation CAS would be the wrong tool here. Generation increments on every
// write, so a caller holding a rangeID it believes current would still be
// rejected after an unrelated update to the same record -- and Temporal does
// call UpdateShard repeatedly with an unchanged rangeID. Value conditions do
// not have that false-positive.
//
// A rejected write surfaces as FILTERED_OUT; see isFilteredOut.
func (c *client) condWrite(expr *as.Expression) *as.WritePolicy {
	wp := *c.write
	wp.FilterExpression = expr
	// The record must already exist for a condition on its bins to be
	// meaningful; without this, a filter on a missing record would create it.
	wp.RecordExistsAction = as.UPDATE_ONLY
	return &wp
}

// condWriteTxn is condWrite bound to a transaction.
func (c *client) condWriteTxn(expr *as.Expression, txn *as.Txn) *as.WritePolicy {
	wp := c.condWrite(expr)
	wp.Txn = txn
	return wp
}

// txnPolicies returns read/write policies bound to a transaction. Every
// command that should participate must carry it; note that scans and queries
// silently ignore the field, so all reads inside a transaction must address
// known keys.
func (c *client) txnPolicies(txn *as.Txn) (*as.BasePolicy, *as.WritePolicy, *as.WritePolicy) {
	rp := *c.read
	rp.Txn = txn

	wp := *c.write
	wp.Txn = txn

	dp := *c.delete
	dp.Txn = txn

	return &rp, &wp, &dp
}

func parseHosts(hosts []string) ([]*as.Host, error) {
	out := make([]*as.Host, 0, len(hosts))
	for _, h := range hosts {
		host, portStr, err := net.SplitHostPort(strings.TrimSpace(h))
		if err != nil {
			// Bare hostname: fall back to the standard client port.
			out = append(out, as.NewHost(strings.TrimSpace(h), 3000))
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port in host %q: %w", h, err)
		}
		out = append(out, as.NewHost(host, port))
	}
	return out, nil
}

// blobBins renders a DataBlob as the (data, encoding) bin pair used throughout
// the store. Everything below the DataStoreFactory seam is opaque bytes.
func blobBins(dataBin, encBin string, blob *commonpb.DataBlob) []*as.Bin {
	if blob == nil {
		return []*as.Bin{as.NewBin(dataBin, nil), as.NewBin(encBin, nil)}
	}
	return []*as.Bin{
		as.NewBin(dataBin, blob.Data),
		as.NewBin(encBin, blob.EncodingType.String()),
	}
}

// readBlob reconstructs a DataBlob from a record's bins.
func readBlob(rec *as.Record, dataBin, encBin string) *commonpb.DataBlob {
	if rec == nil {
		return nil
	}
	data, _ := rec.Bins[dataBin].([]byte)
	enc, _ := rec.Bins[encBin].(string)
	if data == nil {
		return nil
	}
	return p.NewDataBlob(data, enc)
}

// Typed bin readers. Aerospike returns integers as int regardless of the
// width written, so these normalise at the boundary rather than at every use.
func binInt64(rec *as.Record, name string) int64 {
	switch v := rec.Bins[name].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func binString(rec *as.Record, name string) string {
	s, _ := rec.Bins[name].(string)
	return s
}

func binBool(rec *as.Record, name string) bool {
	switch v := rec.Bins[name].(type) {
	case bool:
		return v
	case int:
		return v != 0
	default:
		return false
	}
}

func binBytes(rec *as.Record, name string) []byte {
	b, _ := rec.Bins[name].([]byte)
	return b
}
