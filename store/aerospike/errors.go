package aerospike

import (
	"errors"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"go.temporal.io/api/serviceerror"
)

// convertError maps an Aerospike client error onto the error vocabulary
// Temporal's persistence layer expects. Getting this wrong is not cosmetic:
// the history service branches on these types to decide whether to retry, to
// reload a shard, or to fail a workflow task.
//
// Anything transient becomes Unavailable, which Temporal retries. Anything we
// do not recognise becomes Unavailable too rather than Internal -- an unknown
// storage error is far more likely to be a blip than a bug in the caller.
func convertError(operation string, err error) error {
	if err == nil {
		return nil
	}

	var asErr as.Error
	if !errors.As(err, &asErr) {
		return serviceerror.NewUnavailablef("%s failed: %v", operation, err)
	}

	switch {
	case asErr.Matches(types.KEY_NOT_FOUND_ERROR):
		return serviceerror.NewNotFoundf("%s: record not found", operation)

	case asErr.Matches(types.TIMEOUT, types.MRT_EXPIRED):
		// MRT_EXPIRED means the transaction outlived mrt-duration. Both are
		// timeouts from Temporal's point of view.
		return &persistenceTimeout{operation: operation, err: err}

	case asErr.Matches(
		types.KEY_BUSY,    // too many concurrent commands on one key
		types.MRT_BLOCKED, // another transaction holds the record
		types.MRT_ALREADY_LOCKED,
		types.DEVICE_OVERLOAD,
		types.SERVER_NOT_AVAILABLE,
	):
		return serviceerror.NewUnavailablef("%s: %v (retryable)", operation, err)

	case asErr.Matches(types.MRT_TOO_MANY_WRITES):
		// The 4096-write cap. Temporal has its own notion of an oversized
		// transaction and knows how to shrink the unit of work.
		return &transactionTooLarge{operation: operation, err: err}

	default:
		return serviceerror.NewUnavailablef("%s failed: %v", operation, err)
	}
}

// isNotFound reports whether an Aerospike error is a plain missing record,
// which several call sites treat as a normal outcome rather than a failure.
func isNotFound(err error) bool {
	var asErr as.Error
	return errors.As(err, &asErr) && asErr.Matches(types.KEY_NOT_FOUND_ERROR)
}

// isGenerationMismatch reports whether a conditional write lost its CAS. This
// is how every optimistic-lock check in the store detects a conflict.
func isGenerationMismatch(err error) bool {
	var asErr as.Error
	return errors.As(err, &asErr) && asErr.Matches(types.GENERATION_ERROR)
}

// isFilteredOut reports whether a conditional write was rejected by its
// server-side filter expression. This is our analogue of a Cassandra LWT
// returning applied=false: the record exists but did not match the condition.
func isFilteredOut(err error) bool {
	var asErr as.Error
	return errors.As(err, &asErr) && asErr.Matches(types.FILTERED_OUT)
}

// isKeyExists reports whether a CREATE_ONLY write found the record present.
func isKeyExists(err error) bool {
	var asErr as.Error
	return errors.As(err, &asErr) && asErr.Matches(types.KEY_EXISTS_ERROR)
}

// isTxnConflict reports whether a transaction failed because another writer
// touched its read or write set. Distinct from a generation mismatch: the
// commit-time verification failed rather than a single conditional write.
func isTxnConflict(err error) bool {
	var asErr as.Error
	if !errors.As(err, &asErr) {
		return false
	}
	return asErr.Matches(
		types.MRT_VERSION_MISMATCH,
		types.MRT_BLOCKED,
		types.MRT_ALREADY_LOCKED,
		types.TXN_FAILED,
	)
}

type persistenceTimeout struct {
	operation string
	err       error
}

func (e *persistenceTimeout) Error() string {
	return fmt.Sprintf("%s timed out: %v", e.operation, e.err)
}
func (e *persistenceTimeout) Unwrap() error { return e.err }

type transactionTooLarge struct {
	operation string
	err       error
}

func (e *transactionTooLarge) Error() string {
	return fmt.Sprintf("%s exceeded the Aerospike transaction write limit: %v", e.operation, e.err)
}
func (e *transactionTooLarge) Unwrap() error { return e.err }
