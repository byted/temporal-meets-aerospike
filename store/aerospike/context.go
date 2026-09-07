package aerospike

import (
	"context"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// The Aerospike Go client has no context.Context in its command API at all --
// deadlines are expressed only through policy timeouts. These helpers translate
// a caller's context deadline into TotalTimeout so Temporal's per-call
// deadlines are still honoured.
//
// Known limitation: a context cancelled *during* a command cannot interrupt it,
// because the client has nothing to observe. The command runs to its timeout.
// Cancellation before the call is detected and returns immediately.
//
// The shared policies stay immutable; each helper copies before mutating.

// minTimeout guards against handing the client a zero or negative timeout,
// which it would interpret as "no timeout" -- the opposite of what an expired
// deadline means.
const minTimeout = time.Millisecond

func deadlineTimeout(ctx context.Context, fallback time.Duration) (time.Duration, error) {
	if ctx == nil {
		return fallback, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return fallback, nil
	}
	remaining := time.Until(deadline)
	if remaining < minTimeout {
		return 0, context.DeadlineExceeded
	}
	return remaining, nil
}

func withCtx(ctx context.Context, base *as.BasePolicy) *as.BasePolicy {
	timeout, err := deadlineTimeout(ctx, base.TotalTimeout)
	if err != nil {
		// Let the command run and fail fast rather than swallowing the error
		// here; callers convert it. A tiny timeout produces a prompt failure.
		timeout = minTimeout
	}
	pol := *base
	pol.TotalTimeout = timeout
	return &pol
}

func withCtxW(ctx context.Context, base *as.WritePolicy) *as.WritePolicy {
	timeout, err := deadlineTimeout(ctx, base.TotalTimeout)
	if err != nil {
		timeout = minTimeout
	}
	pol := *base
	pol.TotalTimeout = timeout
	return &pol
}

func withCtxB(ctx context.Context, base *as.BatchPolicy) *as.BatchPolicy {
	timeout, err := deadlineTimeout(ctx, base.TotalTimeout)
	if err != nil {
		timeout = minTimeout
	}
	pol := *base
	pol.TotalTimeout = timeout
	return &pol
}
