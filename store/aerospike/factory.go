package aerospike

import (
	"context"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

// AbstractFactory is the entry point Temporal calls to construct this store.
// Register it with:
//
//	temporal.NewServer(temporal.WithCustomDataStoreFactory(aerospike.NewAbstractFactory()))
//
// and select it in YAML with a `customDatastore` block. Temporal dispatches on
// the presence of that key rather than its name, so only one custom datastore
// factory can exist per process.
type AbstractFactory struct{}

var _ persistenceclient.AbstractDataStoreFactory = (*AbstractFactory)(nil)

func NewAbstractFactory() *AbstractFactory { return &AbstractFactory{} }

func (f *AbstractFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
	serializer serialization.Serializer,
) p.DataStoreFactory {
	// This signature cannot report an error, and Temporal calls it during fx
	// graph construction. A misconfiguration here is not recoverable and must
	// not surface later as a confusing nil dereference, so fail loudly.
	factory, err := NewFactory(cfg, clusterName, logger)
	if err != nil {
		logger.Fatal("unable to initialize the Aerospike persistence store", tag.Error(err))
	}
	return factory
}

// Factory implements persistence.DataStoreFactory.
//
// It owns the single Aerospike client for the process. Note that Temporal
// constructs a factory more than once during startup -- ApplyClusterMetadataConfigProvider
// builds a throwaway one to read cluster metadata before any service starts --
// so construction must be repeatable and Close must be safe to call on each.
type Factory struct {
	client      *client
	clusterName string
	logger      log.Logger
}

var _ p.DataStoreFactory = (*Factory)(nil)

func NewFactory(cfg config.CustomDatastoreConfig, clusterName string, logger log.Logger) (*Factory, error) {
	parsed, err := NewConfig(cfg)
	if err != nil {
		return nil, err
	}

	c, err := newClient(parsed)
	if err != nil {
		return nil, err
	}

	// Bounded independently of any caller's deadline: this runs at boot.
	ctx, cancel := context.WithTimeout(context.Background(), parsed.ConnectTimeout)
	defer cancel()

	if err := checkSchemaVersion(ctx, c); err != nil {
		c.Close()
		return nil, err
	}

	logger.Info("aerospike persistence store initialized",
		tag.NewStringTag("namespace", parsed.Namespace),
		tag.NewStringTag("hosts", joinHosts(parsed.Hosts)),
		tag.NewStringTag("schema-version", Version),
	)

	return &Factory{client: c, clusterName: clusterName, logger: logger}, nil
}

// NewFactoryForTest builds a factory directly from a parsed config, skipping
// the YAML round trip. Used by the conformance harness.
func NewFactoryForTest(cfg *Config, clusterName string, logger log.Logger) (*Factory, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := checkSchemaVersion(ctx, c); err != nil {
		c.Close()
		return nil, err
	}
	return &Factory{client: c, clusterName: clusterName, logger: logger}, nil
}

func (f *Factory) Close() {
	if f.client != nil {
		f.client.Close()
		f.client = nil
	}
}

func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return newShardStore(f.client, f.clusterName), nil
}

func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return newMetadataStore(f.client), nil
}

func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return newClusterMetadataStore(f.client), nil
}

func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	return newExecutionStore(f.client), nil
}

func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return newTaskStore(f.client, false), nil
}

func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	return newTaskStore(f.client, true), nil
}

func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return newQueueStore(f.client, queueType), nil
}

func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	return newQueueV2Store(f.client), nil
}

func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return newNexusEndpointStore(f.client), nil
}

func joinHosts(hosts []string) string {
	out := ""
	for i, h := range hosts {
		if i > 0 {
			out += ","
		}
		out += h
	}
	return out
}
