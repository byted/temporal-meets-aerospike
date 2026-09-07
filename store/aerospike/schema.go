package aerospike

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/blang/semver/v4"
)

// Version is this store's schema version.
//
// Aerospike has no DDL -- namespaces are declared in the server config and sets
// appear on first write -- so there is nothing to "apply". What still matters is
// catching a binary pointed at data written by an incompatible version, which
// is what this record is for.
//
// Temporal's own startup check (temporal/fx.go verifyPersistenceCompatibleVersion)
// hardcodes Cassandra and SQL and has no hook for a custom datastore, so a
// custom store gets no version validation for free. We do it here instead, at
// factory construction, which is the earliest point we control.
const (
	Version              = "1.0"
	MinCompatibleVersion = "1.0"
)

// checkSchemaVersion reads the stored version and compares it to this build's.
// On an empty namespace it writes the version record and returns.
//
// Tolerates a stored version *newer* than ours in the same way Temporal's own
// checker does, so that a rollback does not wedge the cluster -- provided the
// stored min-compatible version still admits us.
func checkSchemaVersion(ctx context.Context, c *client) error {
	key, err := c.keys.schemaVersionKey()
	if err != nil {
		return err
	}

	rec, err := c.as.Get(withCtx(ctx, c.read), key, binSchemaVersion, binMinCompatible)
	if err != nil {
		if !isNotFound(err) {
			return convertError("checkSchemaVersion", err)
		}
		// Fresh namespace: stamp it. CREATE_ONLY so that two services racing
		// at boot cannot clobber each other; losing the race is fine, we then
		// simply validate what the winner wrote.
		err = c.as.PutBins(withCtxW(ctx, c.create), key,
			as.NewBin(binSchemaVersion, Version),
			as.NewBin(binMinCompatible, MinCompatibleVersion),
		)
		if err == nil {
			return nil
		}
		if !isKeyExists(err) {
			return convertError("checkSchemaVersion", err)
		}
		if rec, err = c.as.Get(withCtx(ctx, c.read), key, binSchemaVersion, binMinCompatible); err != nil {
			return convertError("checkSchemaVersion", err)
		}
	}

	stored := binString(rec, binSchemaVersion)
	storedMinCompatible := binString(rec, binMinCompatible)
	if stored == "" {
		return fmt.Errorf("aerospike schema version record exists but is empty; namespace may be corrupt")
	}

	storedVer, err := semver.ParseTolerant(stored)
	if err != nil {
		return fmt.Errorf("parsing stored schema version %q: %w", stored, err)
	}
	ourVer, err := semver.ParseTolerant(Version)
	if err != nil {
		return fmt.Errorf("parsing build schema version %q: %w", Version, err)
	}

	if storedVer.LT(ourVer) {
		return fmt.Errorf(
			"aerospike schema version %s is older than this build requires (%s)",
			stored, Version)
	}

	if storedMinCompatible != "" {
		minVer, err := semver.ParseTolerant(storedMinCompatible)
		if err != nil {
			return fmt.Errorf("parsing stored min-compatible version %q: %w", storedMinCompatible, err)
		}
		if ourVer.LT(minVer) {
			return fmt.Errorf(
				"this build's schema version %s is below the stored minimum compatible version %s",
				Version, storedMinCompatible)
		}
	}

	return nil
}
