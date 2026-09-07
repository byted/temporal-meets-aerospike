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

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is shared by the worker and the starter.
const TaskQueue = "aerospike-e2e"

// Greet runs one activity and returns its result. Deliberately minimal: the
// point is to exercise the persistence path, not the SDK.
//
// Even this much writes to every store: a shard lease, mutable state, a current
// execution pointer, history events, transfer tasks for the workflow and
// activity dispatch, timer tasks for the workflow-task and activity timeouts,
// and matching task queues.
func Greet(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	var greeting string
	if err := workflow.ExecuteActivity(ctx, PickGreeting, name).Get(ctx, &greeting); err != nil {
		return "", err
	}
	return greeting, nil
}

// PickGreeting is the activity Greet calls.
func PickGreeting(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("picking a greeting", "name", name)
	return fmt.Sprintf("Hello %s, from Aerospike", name), nil
}
