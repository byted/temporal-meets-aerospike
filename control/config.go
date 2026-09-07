// Package control implements the demo control plane: a small HTTP service that
// runs Temporal's persistence backend switch live, in front of an audience.
//
// The demo is a loop. Run a workflow on SQLite. Flip the backend to Aerospike.
// Run the same workflow again and watch it behave identically. Then browse the
// records Aerospike is now holding. Everything here exists to make that loop
// one click per step and to narrate what is happening while it happens.
//
// The four moving parts are deliberately kept apart:
//
//	backend.go   the Kubernetes side -- read, patch and watch the Deployment
//	workflow.go  the Temporal side -- namespace, worker, workflow execution
//	browser.go   the Aerospike side -- read-only introspection
//	server.go    the HTTP side -- API, SSE, static assets, and the sequencing
package control

import (
	"net"
	"os"
	"strings"
	"time"
)

// Config is the control plane's entire configuration surface. Every field has
// a default that works in the k3s deployment, so an unconfigured pod comes up
// working rather than failing on a missing variable mid-demo.
type Config struct {
	// Port is the HTTP listen port.
	Port string

	// TemporalAddress is the frontend gRPC address.
	TemporalAddress string
	// DemoNamespace is the Temporal namespace the demo workflow runs in. It is
	// re-registered after every backend switch -- see Server.runSwitch.
	DemoNamespace string

	// AerospikeHost is a "host:port" seed for the record browser.
	AerospikeHost string
	// AerospikeNamespace is the Aerospike namespace holding Temporal's data.
	AerospikeNamespace string
	// AerospikeSetPrefix mirrors the store's setPrefix option. Empty in the
	// demo deployment; only the conformance harness sets one.
	AerospikeSetPrefix string
	// UseServicesAlternate makes the client use each node's
	// alternate-access-address. See useServicesAlternateDefault for why this
	// is inferred rather than simply defaulted.
	UseServicesAlternate bool

	// KubeNamespace and TemporalDeployment identify the Deployment whose
	// TEMPORAL_ENVIRONMENT variable selects the persistence backend.
	KubeNamespace      string
	TemporalDeployment string

	// BasicAuthUser and BasicAuthPassword gate every route. Auth is skipped
	// entirely when either is unset, which is what makes local development
	// bearable; the k3s manifests set both.
	BasicAuthUser     string
	BasicAuthPassword string

	// RolloutTimeout bounds the wait for the Deployment to come back after a
	// patch. Generous: a cold Temporal pod on a laptop k3s can take a while.
	RolloutTimeout time.Duration
	// WorkflowTimeout bounds one demo workflow execution.
	WorkflowTimeout time.Duration
}

// ConfigFromEnv reads the configuration, applying defaults for everything the
// environment does not set.
func ConfigFromEnv() Config {
	cfg := Config{
		Port:               envOr("PORT", "8080"),
		TemporalAddress:    envOr("TEMPORAL_ADDRESS", "temporal:7233"),
		DemoNamespace:      envOr("DEMO_NAMESPACE", "demo"),
		AerospikeHost:      envOr("AEROSPIKE_HOST", "aerospike:3000"),
		AerospikeNamespace: envOr("AEROSPIKE_NAMESPACE", "temporal"),
		AerospikeSetPrefix: os.Getenv("AEROSPIKE_SET_PREFIX"),
		KubeNamespace:      envOr("K8S_NAMESPACE", inClusterNamespace()),
		TemporalDeployment: envOr("TEMPORAL_DEPLOYMENT_NAME", "temporal"),
		BasicAuthUser:      os.Getenv("BASIC_AUTH_USER"),
		BasicAuthPassword:  os.Getenv("BASIC_AUTH_PASSWORD"),
		RolloutTimeout:     5 * time.Minute,
		WorkflowTimeout:    60 * time.Second,
	}
	cfg.UseServicesAlternate = useServicesAlternateDefault(cfg.AerospikeHost)
	if v := os.Getenv("AEROSPIKE_USE_SERVICES_ALTERNATE"); v != "" {
		cfg.UseServicesAlternate = v == "true" || v == "1"
	}
	return cfg
}

// useServicesAlternateDefault infers the setting from the seed address rather
// than picking a side, because both answers are wrong half the time.
//
// An Aerospike node advertises the address it is configured with, not the one
// you reached it on. deploy/aerospike.conf sets alternate-access-address to
// 127.0.0.1 so a host-side client can reach a containerised node; an in-cluster
// client must *not* use that, or it will try to connect to itself.
//
// Reaching the node over loopback is exactly the case the alternate address
// exists for, so that is the signal. AEROSPIKE_USE_SERVICES_ALTERNATE overrides
// it for anything this heuristic does not cover.
func useServicesAlternateDefault(hostPort string) bool {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// inClusterNamespace reads the namespace the pod is running in. The projected
// service-account volume is the only place this is available; outside a cluster
// it does not exist and "default" is as good a guess as any.
func inClusterNamespace() string {
	const path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	b, err := os.ReadFile(path)
	if err != nil {
		return "default"
	}
	if ns := strings.TrimSpace(string(b)); ns != "" {
		return ns
	}
	return "default"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
