package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/stefanselent/temporal-meets-aerospike/e2e"
)

// demoRetention is the namespace retention period. Short: the demo namespace
// is recreated on every backend switch and nothing in it outlives the talk.
const demoRetention = 24 * time.Hour

// namespaceVisibleTimeout bounds the wait for a freshly registered namespace to
// become usable. See EnsureNamespace for why a register is not enough.
const namespaceVisibleTimeout = 30 * time.Second

// WorkflowRunner owns the demo's Temporal client and worker.
//
// The worker is not a fixture, it is a resource with a lifetime tied to the
// backend. Switching persistence stores replaces every Temporal pod behind the
// Service and empties the store, so the worker must come down before the switch
// and be rebuilt afterwards -- an SDK worker cannot be restarted after Stop,
// and a client whose connections point at pods that no longer exist is worse
// than no client at all.
type WorkflowRunner struct {
	address   string
	namespace string
	timeout   time.Duration
	logger    *slog.Logger

	// mu guards the client/worker pair as a unit. Every public method takes it,
	// so a switch tearing the worker down cannot interleave with a workflow run.
	mu     sync.Mutex
	client sdkclient.Client
	worker worker.Worker
}

// ErrWorkerNotRunning distinguishes "the demo is not ready yet" from "the
// workflow failed". They arrive at the same handler and mean opposite things to
// an operator standing in front of an audience, so the HTTP layer maps this one
// to 503 and everything else to 500.
var ErrWorkerNotRunning = errors.New("demo worker is not running; the backend switch may still be in progress")

// RunResult is what a demo workflow execution produced.
type RunResult struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
	Result     string `json:"result"`
	DurationMs int64  `json:"durationMs"`
}

func NewWorkflowRunner(cfg Config, logger *slog.Logger) *WorkflowRunner {
	return &WorkflowRunner{
		address:   cfg.TemporalAddress,
		namespace: cfg.DemoNamespace,
		timeout:   cfg.WorkflowTimeout,
		logger:    logger,
	}
}

// EnsureNamespace registers the demo namespace, tolerating the case where it
// already exists.
//
// This is the step that a naive switch leaves out and that breaks the demo. The
// backend switch points Temporal at a different, *empty* store; namespaces live
// in that store, so the namespace that existed a moment ago is simply not there
// any more. Without this, every "run workflow" click after a switch fails with
// NamespaceNotFound.
func (r *WorkflowRunner) EnsureNamespace(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, err := r.connectLocked(ctx)
	if err != nil {
		return err
	}

	_, err = c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        r.namespace,
		Description:                      "temporal-meets-aerospike demo",
		WorkflowExecutionRetentionPeriod: durationpb.New(demoRetention),
	})
	var alreadyExists *serviceerror.NamespaceAlreadyExists
	switch {
	case err == nil, errors.As(err, &alreadyExists):
	default:
		return fmt.Errorf("registering namespace %q: %w", r.namespace, err)
	}

	// Registration writes to persistence; it does not make the namespace
	// usable. The frontend serves namespaces from a registry that refreshes on
	// an interval, and it caches negative lookups too, so a start immediately
	// after a register can still see NamespaceNotFound. Poll until it resolves.
	return r.waitForNamespace(ctx, c)
}

func (r *WorkflowRunner) waitForNamespace(ctx context.Context, c sdkclient.Client) error {
	ctx, cancel := context.WithTimeout(ctx, namespaceVisibleTimeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		_, lastErr = c.WorkflowService().DescribeNamespace(ctx,
			&workflowservice.DescribeNamespaceRequest{Namespace: r.namespace})
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("namespace %q did not become visible within %s: %w",
				r.namespace, namespaceVisibleTimeout, lastErr)
		case <-ticker.C:
		}
	}
}

// StartWorker builds a fresh worker and starts polling. Calling it while a
// worker is already running is a no-op, so the HTTP layer does not have to
// track whether it has been started.
func (r *WorkflowRunner) StartWorker(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.worker != nil {
		return nil
	}

	c, err := r.connectLocked(ctx)
	if err != nil {
		return err
	}

	// The workflow and activity are the same ones the e2e test runs. Importing
	// them rather than copying them is what makes the demo evidence: the
	// audience is watching the code that the test suite already proved works.
	w := worker.New(c, e2e.TaskQueue, worker.Options{})
	w.RegisterWorkflow(e2e.Greet)
	w.RegisterActivity(e2e.PickGreeting)

	if err := w.Start(); err != nil {
		return fmt.Errorf("starting demo worker on task queue %q: %w", e2e.TaskQueue, err)
	}
	r.worker = w
	r.logger.Info("demo worker started", "taskQueue", e2e.TaskQueue, "namespace", r.namespace)
	return nil
}

// StopWorker stops the worker and drops the client.
//
// Both, not just the worker. The client is dropped because the Temporal pods it
// holds connections to are about to be replaced; reconnecting to a Service
// whose endpoints have all changed is something gRPC will eventually do on its
// own, and "eventually" is not a property to demo in front of an audience.
func (r *WorkflowRunner) StopWorker() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetLocked()
}

func (r *WorkflowRunner) resetLocked() {
	if r.worker != nil {
		r.worker.Stop()
		r.worker = nil
	}
	if r.client != nil {
		r.client.Close()
		r.client = nil
	}
}

// Run executes one demo workflow and waits for its result.
func (r *WorkflowRunner) Run(ctx context.Context) (*RunResult, error) {
	c, err := r.clientForRun(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	opts := sdkclient.StartWorkflowOptions{
		ID:                       "demo-" + uuid.NewString(),
		TaskQueue:                e2e.TaskQueue,
		WorkflowExecutionTimeout: r.timeout,
	}

	started := time.Now()
	run, err := c.ExecuteWorkflow(ctx, opts, e2e.Greet, "world")
	if err != nil {
		return nil, fmt.Errorf("starting demo workflow: %w", err)
	}

	var result string
	if err := run.Get(ctx, &result); err != nil {
		return nil, fmt.Errorf("demo workflow %s did not complete: %w", run.GetID(), err)
	}

	return &RunResult{
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
		Result:     result,
		DurationMs: time.Since(started).Milliseconds(),
	}, nil
}

// clientForRun checks the worker is up and hands back the client, holding the
// lock only long enough to do so.
//
// The lock is deliberately not held across the execution. A demo workflow takes
// seconds, and /api/state polls Ready throughout -- holding the mutex for the
// duration would freeze the UI for exactly as long as the interesting thing is
// happening. sdkclient.Client is safe for concurrent use, so the only race left
// is a switch closing the client mid-run, which surfaces as a failed workflow
// rather than corruption. The HTTP layer already refuses to run one during a
// switch.
func (r *WorkflowRunner) clientForRun(ctx context.Context) (sdkclient.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.worker == nil {
		return nil, ErrWorkerNotRunning
	}
	return r.connectLocked(ctx)
}

// Ready reports whether the frontend answers and the demo namespace exists --
// the two things that have to be true for the run button to work.
func (r *WorkflowRunner) Ready(ctx context.Context) bool {
	r.mu.Lock()
	c, err := r.connectLocked(ctx)
	r.mu.Unlock()
	if err != nil {
		return false
	}
	_, err = c.WorkflowService().DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: r.namespace})
	return err == nil
}

// Close releases everything. Safe to call more than once.
func (r *WorkflowRunner) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetLocked()
}

// connectLocked returns the cached client, dialling one if needed. Caller holds
// r.mu.
//
// A failed dial is not cached: during a rollout the frontend is unreachable for
// a few seconds, and the next call should simply try again.
func (r *WorkflowRunner) connectLocked(ctx context.Context) (sdkclient.Client, error) {
	if r.client != nil {
		return r.client, nil
	}

	c, err := sdkclient.DialContext(ctx, sdkclient.Options{
		HostPort:  r.address,
		Namespace: r.namespace,
		Logger:    newSDKLogger(r.logger),
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to Temporal at %s: %w", r.address, err)
	}
	r.client = c
	return c, nil
}

// sdkLogger adapts slog to the SDK's logger interface, so worker and workflow
// logs land in the same stream as everything else this process emits.
type sdkLogger struct{ log *slog.Logger }

func newSDKLogger(l *slog.Logger) *sdkLogger {
	return &sdkLogger{log: l.With("source", "temporal-sdk")}
}

func (l *sdkLogger) Debug(msg string, kv ...any) { l.log.Debug(msg, kv...) }
func (l *sdkLogger) Info(msg string, kv ...any)  { l.log.Info(msg, kv...) }
func (l *sdkLogger) Warn(msg string, kv ...any)  { l.log.Warn(msg, kv...) }
func (l *sdkLogger) Error(msg string, kv ...any) { l.log.Error(msg, kv...) }
