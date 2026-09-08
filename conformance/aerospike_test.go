// Package conformance runs Temporal's own persistence test suites against the
// Aerospike store.
//
// This is the project's primary feedback loop. The suites are Temporal's, not
// ours: passing them is the definition of a correct store, and they encode
// semantics that no amount of reading the interface would reveal.
//
// They are importable because common/persistence/tests is a normal package and
// persistencetests.NewTestBaseForCluster is exported -- so no fork of Temporal
// is needed. Temporal's *functional* suite (tests/) is package-private and is
// deferred to Iteration 2.
//
// Requires a running node:
//
//	docker compose -f deploy/docker-compose.yml up -d aerospike roster-init
package conformance

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/tests"

	"github.com/byted/temporal-meets-aerospike/store/aerospike"
)

// requireAerospike skips rather than fails when no node is reachable, so that
// `go test ./...` on a machine without the compose stack up is not a wall of
// red. A reachable-but-broken node still fails loudly.
// NOTE on teardown: these tests use t.Cleanup rather than defer. Some suites
// (QueueV2) run parallel subtests, which execute *after* the enclosing test
// function returns -- a deferred teardown closes the Aerospike client out from
// under them, and the failure surfaces as "Partition map empty", which looks
// like a cluster problem rather than a test-lifecycle one.
func requireAerospike(t *testing.T) {
	t.Helper()
	if err := aerospike.Ping(); err != nil {
		t.Skipf("no Aerospike node reachable (%v)\n"+
			"start one with: docker compose -f deploy/docker-compose.yml up -d aerospike roster-init", err)
	}
}

func testLogger() log.Logger { return log.NewTestLogger() }

// newTestBase wires Temporal's legacy TestBase to this store. The
// AbstractDataStoreFactory field is the hook that makes
// DataStoreFactoryProvider route to us.
func newTestBase(t *testing.T) *persistencetests.TestBase {
	t.Helper()
	logger := testLogger()
	cluster := aerospike.NewTestCluster(logger)

	base := persistencetests.NewTestBaseForCluster(cluster, logger)
	base.AbstractDataStoreFactory = aerospike.NewAbstractFactory()
	return base
}

// --- Suites over raw stores (common/persistence/tests) ---

func TestAerospikeShardStoreSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("creating shard store: %v", err)
	}

	suite.Run(t, tests.NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testLogger(),
	))
}

func TestAerospikeExecutionMutableStateStoreSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("creating shard store: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("creating execution store: %v", err)
	}

	suite.Run(t, tests.NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testLogger(),
	))
}

func TestAerospikeExecutionMutableStateTaskStoreSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("creating shard store: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("creating execution store: %v", err)
	}

	suite.Run(t, tests.NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testLogger(),
	))
}

func TestAerospikeHistoryStoreSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("creating execution store: %v", err)
	}

	suite.Run(t, tests.NewHistoryEventsSuite(t, executionStore, testLogger()))
}

func TestAerospikeTaskQueueSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	taskStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("creating task store: %v", err)
	}
	suite.Run(t, tests.NewTaskQueueSuite(t, taskStore, testLogger()))
}

func TestAerospikeTaskQueueTaskSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	taskStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("creating task store: %v", err)
	}
	suite.Run(t, tests.NewTaskQueueTaskSuite(t, taskStore, testLogger()))
}

func TestAerospikeTaskQueueFairTaskSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	fairStore, err := factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("creating fair task store: %v", err)
	}
	suite.Run(t, tests.NewTaskQueueFairTaskSuite(t, fairStore, testLogger()))
}

func TestAerospikeTaskQueueUserDataSuite(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	taskStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("creating task store: %v", err)
	}
	suite.Run(t, tests.NewTaskQueueUserDataSuite(t, taskStore, testLogger()))
}

func TestAerospikeQueueV2(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	queue, err := factory.NewQueueV2()
	if err != nil {
		t.Fatalf("creating queue: %v", err)
	}
	tests.RunQueueV2TestSuite(t, queue)
}

func TestAerospikeNexusEndpoints(t *testing.T) {
	requireAerospike(t)

	factory, tearDown, err := aerospike.NewTestFactory(testLogger())
	if err != nil {
		t.Fatalf("creating Aerospike factory: %v", err)
	}
	t.Cleanup(tearDown)

	store, err := factory.NewNexusEndpointStore()
	if err != nil {
		t.Fatalf("creating nexus endpoint store: %v", err)
	}
	tests.RunNexusEndpointTestSuite(t, store, new(atomic.Int64))
}

// --- Suites over a TestBase (common/persistence/persistence-tests) ---

func TestAerospikeMetadataPersistenceV2(t *testing.T) {
	requireAerospike(t)

	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = newTestBase(t)
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestAerospikeHistoryV2Persistence(t *testing.T) {
	requireAerospike(t)

	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = newTestBase(t)
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestAerospikeClusterMetadataPersistence(t *testing.T) {
	requireAerospike(t)

	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = newTestBase(t)
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}
