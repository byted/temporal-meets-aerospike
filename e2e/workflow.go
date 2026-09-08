// Package e2e runs a real workflow, with a real activity, against a Temporal
// server backed by Aerospike.
//
// The conformance suites prove each store behaves correctly in isolation. This
// proves the thing that actually matters: that a workflow runs end to end when
// every one of them is wired together behind a live server.
package e2e

import (
	"context"
	"fmt"
	"time"

	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is shared by the worker and the starter.
const TaskQueue = "aerospike-e2e"

// GreetResult is what one run produced: the greeting, and the persistence store
// the server was actually running on while it produced it.
//
// The store is carried out of the run rather than looked up alongside it
// because it is evidence *about that execution*. The demo's whole claim is that
// the same workflow runs on either store, and a claim like that is worth
// nothing if the store name is asserted by anything other than the run itself.
type GreetResult struct {
	Greeting string `json:"greeting"`
	// PersistenceStore is the name the server reports for its operational
	// store, verbatim: "aerospike" or "sqlite" on this deployment.
	PersistenceStore string `json:"persistenceStore"`
}

// Greet runs one activity and returns its result. Deliberately minimal: the
// point is to exercise the persistence path, not the SDK.
//
// Even this much writes to every store: a shard lease, mutable state, a current
// execution pointer, history events, transfer tasks for the workflow and
// activity dispatch, timer tasks for the workflow-task and activity timeouts,
// and matching task queues.
// DemoSleep is how long the workflow sleeps before doing its work.
//
// It exists for the demo rather than for the workflow. A durable timer is the
// one thing that writes to a *scheduled* task category, and scheduled tasks are
// where the interesting half of the data model lives: they key on a 16-byte
// big-endian fireTime||taskID blob, bucketed one minute at a time, so bytewise
// order equals (fireTime, taskID). Without a sleep the store only ever holds
// immediate tasks and the bucket view has nothing to show for half of what
// docs/03-data-model.md describes.
//
// Ten seconds also spreads a batch across more than one 60-second bucket when
// the runs are staggered, which is the point of having buckets at all.
const DemoSleep = 10 * time.Second

func Greet(ctx workflow.Context, name string) (GreetResult, error) {
	// A durable timer, not a Go sleep: this is workflow code, so the wait has
	// to survive a worker restart and be replayable. It becomes a timer task in
	// the persistence store, which is the whole reason it is here.
	if err := workflow.Sleep(ctx, DemoSleep); err != nil {
		return GreetResult{}, err
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	var result GreetResult
	if err := workflow.ExecuteActivity(ctx, PickGreeting, name).Get(ctx, &result); err != nil {
		return GreetResult{}, err
	}
	return result, nil
}

// PickGreeting is the activity Greet calls. It asks the server which store it
// is running on and names it in the greeting.
//
// The lookup happens here, on every execution, rather than once when the worker
// is built, because the store changes underneath a long-lived worker. Yes, the
// control plane restarts the worker on every switch -- but that is a property
// of one caller, not of this code, and "correct as long as nobody forgets to
// restart the worker" is exactly the kind of cached answer that goes stale
// silently and produces a demo that confidently prints the wrong store. Asking
// per execution cannot be stale: the answer comes from the same server that
// just persisted this workflow's history, and it costs one cheap RPC on a path
// that already does several.
//
// It also belongs in an activity rather than in Greet: it queries the outside
// world, and workflow code must stay deterministic and replayable.
func PickGreeting(ctx context.Context, name string) (GreetResult, error) {
	activity.GetLogger(ctx).Info("picking a greeting", "name", name)

	store, err := persistenceStore(ctx)
	if err != nil {
		return GreetResult{}, err
	}

	return GreetResult{
		Greeting:         fmt.Sprintf("Hello %s, from %s", name, storeDisplayName(store)),
		PersistenceStore: store,
	}, nil
}

// persistenceStore asks the server which operational store it is configured
// against. This is the same field `temporal operator cluster describe` prints
// as PersistenceStore, and the server sources it from the execution manager it
// is actually using -- so it reports the live configuration, not a guess.
//
// An error is returned rather than a fallback name. A greeting that says
// "Aerospike" because the lookup failed is precisely the bug this exists to
// fix; the activity's retry policy gets a second chance instead.
func persistenceStore(ctx context.Context) (string, error) {
	info, err := activity.GetClient(ctx).WorkflowService().GetClusterInfo(ctx,
		&workflowservice.GetClusterInfoRequest{})
	if err != nil {
		return "", fmt.Errorf("asking the server which persistence store it uses: %w", err)
	}
	store := info.GetPersistenceStore()
	if store == "" {
		return "", fmt.Errorf("the server reported no persistence store")
	}
	return store, nil
}

// storeDisplayName renders a store name the way the demo says it out loud.
// Anything unrecognised is passed through verbatim: reporting a name nobody
// expected is useful, inventing one is not.
func storeDisplayName(store string) string {
	switch store {
	case "sqlite":
		return "SQLite"
	case "aerospike":
		return "Aerospike"
	default:
		return store
	}
}
