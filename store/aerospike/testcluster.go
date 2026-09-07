package aerospike

import (
	"fmt"
	"os"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// TestCluster implements persistencetests.PersistenceTestCluster so Temporal's
// own conformance suites can run against this store.
//
// Isolation works differently from the Cassandra harness. Cassandra creates a
// randomly-named keyspace per run and drops it. Aerospike namespaces are
// declared in the *server* config and cannot be created at runtime, and set
// names persist until the server restarts -- so minting a fresh set prefix per
// run would leak set metadata indefinitely and eventually hit the per-namespace
// set limit.
//
// Instead the prefix is fixed and setup truncates. That is deterministic and
// leak-free, at the cost of requiring that only one test binary run against a
// given namespace at a time. Go runs test functions within a package serially
// unless they call t.Parallel, so this holds for the conformance suites as
// written.
type TestCluster struct {
	cfg    *Config
	logger log.Logger
	client *client
}

// TestSetPrefix keeps conformance data separate from anything real that might
// share the namespace.
const TestSetPrefix = "test_"

// NewTestCluster builds a harness pointed at a local Aerospike node. Honours
// AEROSPIKE_HOST / AEROSPIKE_PORT / AEROSPIKE_NAMESPACE so CI can retarget it.
func NewTestCluster(logger log.Logger) *TestCluster {
	host := envOr("AEROSPIKE_HOST", "127.0.0.1")
	port := envOr("AEROSPIKE_PORT", "3000")
	namespace := envOr("AEROSPIKE_NAMESPACE", "temporal")

	return &TestCluster{
		cfg: &Config{
			Hosts:     []string{fmt.Sprintf("%s:%s", host, port)},
			Namespace: namespace,
			SetPrefix: TestSetPrefix,
			// Tests run on the host while the node runs in a container and
			// advertises its container IP. deploy/aerospike.conf sets
			// alternate-access-address for exactly this.
			UseServicesAlternate: true,
			MaxConnsPerNode:      32,
			ConnectTimeout:       10 * time.Second,
			SocketTimeout:        10 * time.Second,
			TotalTimeout:         15 * time.Second,
			TxnTimeout:           30 * time.Second,
		},
		logger: logger,
	}
}

// Config returns the persistence configuration Temporal's TestBase feeds into
// DataStoreFactoryProvider. The customDatastore block is what routes it here.
func (t *TestCluster) Config() config.Persistence {
	return config.Persistence{
		DefaultStore:    "aerospike-test",
		NumHistoryShards: 4,
		DataStores: map[string]config.DataStore{
			"aerospike-test": {
				CustomDataStoreConfig: &config.CustomDatastoreConfig{
					Name: StoreName,
					Options: map[string]any{
						"hosts":                t.cfg.Hosts,
						"namespace":            t.cfg.Namespace,
						"setPrefix":            t.cfg.SetPrefix,
						"useServicesAlternate": t.cfg.UseServicesAlternate,
					},
				},
			},
		},
	}
}

func (t *TestCluster) SetupTestDatabase() {
	c, err := newClient(t.cfg)
	if err != nil {
		panic(fmt.Sprintf("aerospike test cluster: %v -- is the node running? "+
			"docker compose -f deploy/docker-compose.yml up -d aerospike roster-init", err))
	}
	t.client = c
	t.truncateAll()
}

func (t *TestCluster) TearDownTestDatabase() {
	if t.client == nil {
		return
	}
	t.truncateAll()
	t.client.Close()
	t.client = nil
}

// truncateAll empties every set this store uses. Truncate is a server-side
// operation, so this stays fast regardless of how much a suite wrote.
func (t *TestCluster) truncateAll() {
	for _, set := range t.client.keys.sets() {
		err := t.client.as.Truncate(nil, t.cfg.Namespace, set, nil)
		if err != nil && !isNotFound(err) {
			// A set that was never written to does not exist, which is not a
			// failure. Anything else is worth surfacing but not fatal --
			// setup truncates again on the next run.
			t.logger.Warn("truncating set failed",
				tag.NewStringTag("set", set), tag.Error(err))
		}
	}
	// Truncation is asynchronous with respect to in-flight reads; give the
	// server a moment so a suite does not observe records it just cleared.
	time.Sleep(100 * time.Millisecond)
}

// AbstractFactoryForTest returns the factory to install on TestBase so that
// DataStoreFactoryProvider routes to this store.
func (t *TestCluster) AbstractFactoryForTest() *AbstractFactory { return NewAbstractFactory() }

// NewTestFactory builds a DataStoreFactory directly, for the suites in
// common/persistence/tests that take raw stores rather than a TestBase.
func NewTestFactory(logger log.Logger) (*Factory, func(), error) {
	cluster := NewTestCluster(logger)
	cluster.SetupTestDatabase()

	factory, err := NewFactoryForTest(cluster.cfg, "aerospike-test-cluster", logger)
	if err != nil {
		cluster.TearDownTestDatabase()
		return nil, nil, err
	}

	return factory, func() {
		factory.Close()
		cluster.TearDownTestDatabase()
	}, nil
}

// Ping reports whether an Aerospike node is reachable, so tests can skip with a
// useful message instead of failing opaquely.
func Ping() error {
	cluster := NewTestCluster(log.NewNoopLogger())
	c, err := newClient(cluster.cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	if !c.as.IsConnected() {
		return fmt.Errorf("aerospike client is not connected")
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
