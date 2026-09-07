package aerospike

import (
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Aerospike has no DDL: sets are created implicitly on first write. These are
// the logical tables of the store. Set names are limited to 63 bytes, so the
// names are short and the optional test prefix is kept short too.
const (
	setShard           = "shard"    // shard lease + serialized ShardInfo
	setNamespace       = "ns"       // namespace record, keyed by namespace name
	setNamespaceByID   = "nsid"     // namespace id -> name pointer
	setNamespaceMeta   = "nsmeta"   // the single notification_version record
	setClusterMeta     = "cmeta"    // per-cluster metadata + version
	setClusterMember   = "cmember"  // cluster membership, expires via TTL
	setSchema          = "schema"   // our schema version record
)

// allSets is used by the test harness to truncate between runs. Keep it in
// sync as stores are added.
var allSets = []string{
	setShard,
	setNamespace,
	setNamespaceByID,
	setNamespaceMeta,
	setClusterMeta,
	setClusterMember,
	setSchema,
}

// keyBuilder constructs Aerospike keys within one namespace, applying the
// configured set prefix.
type keyBuilder struct {
	namespace string
	prefix    string
}

func newKeyBuilder(cfg *Config) *keyBuilder {
	return &keyBuilder{namespace: cfg.Namespace, prefix: cfg.SetPrefix}
}

// set returns the physical set name for a logical set.
func (k *keyBuilder) set(logical string) string {
	return k.prefix + logical
}

// sets returns every physical set name, for truncation.
func (k *keyBuilder) sets() []string {
	out := make([]string, 0, len(allSets))
	for _, s := range allSets {
		out = append(out, k.set(s))
	}
	return out
}

func (k *keyBuilder) newKey(logicalSet string, userKey any) (*as.Key, error) {
	key, err := as.NewKey(k.namespace, k.set(logicalSet), userKey)
	if err != nil {
		return nil, fmt.Errorf("building %s key %v: %w", logicalSet, userKey, err)
	}
	return key, nil
}

// shardKey identifies one history shard's lease record.
func (k *keyBuilder) shardKey(shardID int32) (*as.Key, error) {
	return k.newKey(setShard, int(shardID))
}

// namespaceKey is keyed by namespace *name*, mirroring Cassandra's
// namespaces_by_name table -- the row that carries the record and its
// notification version.
func (k *keyBuilder) namespaceKey(name string) (*as.Key, error) {
	return k.newKey(setNamespace, name)
}

// namespaceByIDKey is the id -> name pointer, mirroring namespaces_by_id.
func (k *keyBuilder) namespaceByIDKey(id string) (*as.Key, error) {
	return k.newKey(setNamespaceByID, id)
}

// namespaceMetadataKey is the single record holding notification_version.
// Every namespace mutation bumps it under a conditional check.
func (k *keyBuilder) namespaceMetadataKey() (*as.Key, error) {
	return k.newKey(setNamespaceMeta, "metadata")
}

func (k *keyBuilder) clusterMetadataKey(clusterName string) (*as.Key, error) {
	return k.newKey(setClusterMeta, clusterName)
}

// clusterMemberKey is keyed by the host UUID. These records carry a TTL and
// are the one place in the store where expiry is load-bearing.
func (k *keyBuilder) clusterMemberKey(hostID []byte) (*as.Key, error) {
	return k.newKey(setClusterMember, hostID)
}

func (k *keyBuilder) schemaVersionKey() (*as.Key, error) {
	return k.newKey(setSchema, "version")
}

// Bin names. Aerospike caps these at 15 bytes, so they are abbreviated; the
// mapping is documented here rather than guessed at each call site.
const (
	binRangeID       = "range_id"  // shard lease fencing token
	binData          = "data"      // serialized proto payload
	binEncoding      = "enc"       // DataBlob encoding type
	binOwner         = "owner"     // shard owner host identity
	binNamespaceName = "ns_name"   // namespace name (on the id -> name pointer)
	binNamespaceID   = "ns_id"     // namespace id (on the name-keyed record)
	binNotifVersion  = "notif_ver" // namespace notification version
	binIsGlobal      = "is_global"
	binVersion       = "version"   // cluster metadata optimistic-lock version
	binRole          = "role"
	binRPCAddress    = "rpc_addr"
	binRPCPort       = "rpc_port"
	binSessionStart  = "sess_start"
	binLastHeartbeat = "last_hb"
	binHostID        = "host_id"
	binSchemaVersion = "schema_ver"
	binMinCompatible = "min_compat"
)
