package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// The stack this talks to:
//
//	docker compose -f deploy/docker-compose.yml up -d
//	temporal operator namespace create --namespace demo --retention 24h
//
// Skips rather than fails when no server is reachable, so `go test ./...` on a
// machine without the stack up is not a wall of red.
const (
	defaultAddress   = "127.0.0.1:7233"
	defaultNamespace = "demo"
)

func dial(t *testing.T) sdkclient.Client {
	t.Helper()

	address := os.Getenv("TEMPORAL_ADDRESS")
	if address == "" {
		address = defaultAddress
	}
	namespace := os.Getenv("TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = defaultNamespace
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := sdkclient.DialContext(ctx, sdkclient.Options{
		HostPort:  address,
		Namespace: namespace,
	})
	if err != nil {
		t.Skipf("no Temporal server at %s (%v)\n"+
			"start one with: docker compose -f deploy/docker-compose.yml up -d", address, err)
	}

	// Dial is lazy about the namespace; check it exists before running.
	if _, err := c.WorkflowService().DescribeNamespace(ctx,
		describeNamespaceRequest(namespace)); err != nil {
		c.Close()
		var notFound *serviceerror.NamespaceNotFound
		if ok := asNamespaceNotFound(err, &notFound); ok {
			t.Skipf("namespace %q does not exist; create it with:\n"+
				"  temporal operator namespace create --namespace %s --retention 24h",
				namespace, namespace)
		}
		t.Skipf("cannot reach Temporal at %s: %v", address, err)
	}

	t.Cleanup(c.Close)
	return c
}

// TestWorkflowWithActivity is the headline check for the whole project: a
// workflow that calls an activity, run against Aerospike, start to finish.
func TestWorkflowWithActivity(t *testing.T) {
	c := dial(t)

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(Greet)
	w.RegisterActivity(PickGreeting)

	if err := w.Start(); err != nil {
		t.Fatalf("starting worker: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: TaskQueue,
	}, Greet, "world")
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	t.Logf("workflow started: id=%s run=%s", run.GetID(), run.GetRunID())

	var greeting string
	if err := run.Get(ctx, &greeting); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}

	const want = "Hello world, from Aerospike"
	if greeting != want {
		t.Fatalf("got %q, want %q", greeting, want)
	}
	t.Logf("workflow completed: %s", greeting)

	// Describe it back: this reads mutable state and the current-execution
	// pointer through the store, which a completed run alone does not prove.
	desc, err := c.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	if err != nil {
		t.Fatalf("describing workflow: %v", err)
	}
	if status := desc.WorkflowExecutionInfo.Status.String(); status != "Completed" {
		t.Fatalf("workflow status is %s, want Completed", status)
	}
}
