package control

import (
	"context"
	"testing"
	"time"
)

// These run against the compose stack:
//
//	docker compose -f deploy/docker-compose.yml up -d aerospike roster-init
//
// They skip rather than fail when nothing is listening, so `go test ./...` on a
// machine without the stack up is not a wall of red.
func testBrowser(t *testing.T) *Browser {
	t.Helper()

	cfg := ConfigFromEnv()
	if cfg.AerospikeHost == "aerospike:3000" {
		// The in-cluster default is not reachable from a test run; point at the
		// published port and take the alternate address with it.
		cfg.AerospikeHost = "127.0.0.1:3000"
		cfg.UseServicesAlternate = true
	}

	b := NewBrowser(cfg)
	t.Cleanup(b.Close)

	if _, err := b.connect(); err != nil {
		t.Skipf("no Aerospike node at %s (%v)\n"+
			"start one with: docker compose -f deploy/docker-compose.yml up -d aerospike roster-init",
			cfg.AerospikeHost, err)
	}
	return b
}

// TestParseInfoPairsSeparators pins the thing the info protocol is inconsistent
// about: `namespace/<ns>` separates its fields with ';' and each `sets/<ns>`
// entry separates its fields with ':'. Using one separator for both parses
// without error and yields nonsense, so it has to be asserted rather than
// eyeballed.
func TestParseInfoPairsSeparators(t *testing.T) {
	const namespaceLine = "ns_cluster_size=1;objects=93;strong-consistency=true;dead_partitions=0;unavailable_partitions=0"
	ns := parseInfoPairs(namespaceLine, ";")
	if ns["strong-consistency"] != "true" || ns["objects"] != "93" {
		t.Fatalf("namespace fields parsed wrong: %v", ns)
	}

	const setEntry = "ns=temporal:set=exec:objects=2:tombstones=0:data_used_bytes=1952"
	set := parseInfoPairs(setEntry, ":")
	if set["set"] != "exec" || set["objects"] != "2" {
		t.Fatalf("set fields parsed wrong: %v", set)
	}

	// The wrong separator does not fail; it produces one unusable field.
	if wrong := parseInfoPairs(setEntry, ";"); wrong["set"] != "" {
		t.Fatalf("expected the wrong separator to yield no 'set' field, got %v", wrong)
	}
}

// TestDescribeValueBlobMapKey covers the shape trap documented in
// store/aerospike/client.go: a 16-byte value arrives as a fixed-size array
// inside a MapPair, and a plain []byte assertion silently drops it.
func TestDescribeValueBlobMapKey(t *testing.T) {
	var key [16]uint8
	for i := range key {
		key[i] = byte(i)
	}
	if got := formatScalar(key); got != "000102030405060708090a0b0c0d0e0f" {
		t.Fatalf("array-shaped BLOB key rendered as %q", got)
	}

	typ, _, size := describeValue([]byte{0x0a, 0x24, 0xff})
	if typ != "blob" || size != 3 {
		t.Fatalf("slice-shaped BLOB rendered as type=%q size=%d", typ, size)
	}
}

func TestBrowserHealth(t *testing.T) {
	b := testBrowser(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	health := b.Health(ctx)
	if !health.Reachable {
		t.Fatal("namespace info returned nothing; the namespace name may be wrong")
	}
	if !health.StrongConsistency {
		t.Error("namespace is not strong-consistency; the store's transactions require it")
	}
	t.Logf("health: %+v", health)
}

func TestBrowserSetsAndRecords(t *testing.T) {
	b := testBrowser(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sets, err := b.Sets(ctx)
	if err != nil {
		t.Fatalf("listing sets: %v", err)
	}
	if len(sets) == 0 {
		t.Skip("namespace has no sets yet; run the conformance suites or the e2e demo first")
	}
	for _, s := range sets {
		t.Logf("set %-24s objects=%-6d %s", s.Name, s.Objects, s.Description)
	}

	// Scan whichever set actually holds records, so the assertion does not
	// depend on which suite last ran.
	var target string
	for _, s := range sets {
		if s.Objects > 0 {
			target = s.Name
			break
		}
	}
	if target == "" {
		t.Skip("every set is empty")
	}

	records, err := b.Records(ctx, target, 3)
	if err != nil {
		t.Fatalf("scanning set %s: %v", target, err)
	}
	if len(records) == 0 {
		t.Fatalf("set %s reports objects but the scan returned nothing", target)
	}
	for _, r := range records {
		t.Logf("record key=%q digest=%s", r.Key, r.Digest)
		if r.Digest == "" {
			t.Error("record has no digest; the scan is not returning keys")
		}
		for _, bin := range r.Bins {
			t.Logf("    %-12s %-8s size=%-6d %s", bin.Name, bin.Type, bin.Size, bin.Preview)
			if bin.Type == "" {
				t.Errorf("bin %s has no rendered type", bin.Name)
			}
		}
	}
}
