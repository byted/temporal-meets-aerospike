package aerospike

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// These tests pin the Aerospike behaviours the Temporal data model depends on.
// They are not tests of our code -- they are executable assertions about the
// server, so that running against the wrong edition, the wrong server version,
// or an AP namespace fails here with a clear message rather than surfacing as a
// baffling correctness bug three phases later.
//
// See docs/03-data-model.md for why each one matters.
//
// Requires the compose stack:
//     docker compose -f deploy/docker-compose.yml up -d aerospike roster-init

const (
	testNamespace = "temporal"
	testSet       = "captest"
)

func testClient(t *testing.T) *as.Client {
	t.Helper()

	host := os.Getenv("AEROSPIKE_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	policy := as.NewClientPolicy()
	// The node advertises its container IP, which is unreachable from the host.
	// deploy/aerospike.conf sets `alternate-access-address 127.0.0.1` for
	// exactly this path.
	policy.UseServicesAlternate = true

	client, err := as.NewClientWithPolicy(policy, host, 3000)
	if err != nil {
		t.Skipf("no Aerospike at %s:3000 (%v) -- start it with: docker compose -f deploy/docker-compose.yml up -d aerospike roster-init", host, err)
	}
	t.Cleanup(client.Close)
	return client
}

// scWritePolicy returns the write policy every caller must use against a
// strong-consistency namespace.
func scWritePolicy() *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.CommitLevel = as.COMMIT_ALL // mandatory in SC; COMMIT_MASTER is rejected
	return wp
}

func uniqueKey(t *testing.T, suffix string) *as.Key {
	t.Helper()
	k, err := as.NewKey(testNamespace, testSet, fmt.Sprintf("%s-%d-%d", suffix, os.Getpid(), rand.Int63()))
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	return k
}

// TestNamespaceIsStrongConsistency fails fast if the namespace is running in AP
// mode. Multi-record transactions require SC, and history-task reads must be
// quorum-consistent or tasks are silently lost.
func TestNamespaceIsStrongConsistency(t *testing.T) {
	client := testClient(t)

	nodes := client.GetNodes()
	if len(nodes) == 0 {
		t.Fatal("no nodes in cluster")
	}

	info, err := nodes[0].RequestInfo(as.NewInfoPolicy(), "namespace/"+testNamespace)
	if err != nil {
		t.Fatalf("info request: %v", err)
	}

	raw := info["namespace/"+testNamespace]
	if !hasField(raw, "strong-consistency=true") {
		t.Fatalf("namespace %q is NOT in strong-consistency mode; MRT will not work.\ninfo: %s", testNamespace, raw)
	}
	if !hasField(raw, "unavailable_partitions=0") {
		t.Fatalf("namespace %q has unavailable partitions -- was the roster staged?\ninfo: %s", testNamespace, raw)
	}
}

func hasField(info, want string) bool {
	for _, f := range splitSemi(info) {
		if f == want {
			return true
		}
	}
	return false
}

func splitSemi(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ';' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// TestMultiRecordTransactionCommits is the premise of the whole execution
// store: Cassandra's single-partition conditional batch becomes one MRT
// spanning several records.
func TestMultiRecordTransactionCommits(t *testing.T) {
	client := testClient(t)

	keys := []*as.Key{
		uniqueKey(t, "mrt-a"),
		uniqueKey(t, "mrt-b"),
		uniqueKey(t, "mrt-c"),
	}

	txn := as.NewTxnWithCapacity(16, 16)
	wp := scWritePolicy()
	wp.Txn = txn

	for i, k := range keys {
		if err := client.PutBins(wp, k, as.NewBin("v", i)); err != nil {
			_, _ = client.Abort(txn)
			t.Fatalf("write %d inside txn: %v", i, err)
		}
	}

	if _, err := client.Commit(txn); err != nil {
		t.Fatalf("commit: %v", err)
	}

	for i, k := range keys {
		rec, err := client.Get(nil, k, "v")
		if err != nil {
			t.Fatalf("read back %d: %v", i, err)
		}
		if got := rec.Bins["v"]; got != i {
			t.Errorf("record %d: got v=%v want %v", i, got, i)
		}
	}

	t.Cleanup(func() {
		dp := scWritePolicy()
		dp.DurableDelete = true
		for _, k := range keys {
			_, _ = client.Delete(dp, k)
		}
	})
}

// TestMultiRecordTransactionAborts proves the all-or-nothing half of the
// contract. Without this, a failed conditional update could leave a half-written
// mutable state behind.
func TestMultiRecordTransactionAborts(t *testing.T) {
	client := testClient(t)

	keys := []*as.Key{uniqueKey(t, "abort-a"), uniqueKey(t, "abort-b")}

	txn := as.NewTxnWithCapacity(16, 16)
	wp := scWritePolicy()
	wp.Txn = txn

	for i, k := range keys {
		if err := client.PutBins(wp, k, as.NewBin("v", i)); err != nil {
			t.Fatalf("write %d inside txn: %v", i, err)
		}
	}

	if _, err := client.Abort(txn); err != nil {
		t.Fatalf("abort: %v", err)
	}

	for i, k := range keys {
		exists, err := client.Exists(nil, k)
		if err != nil {
			t.Fatalf("exists %d: %v", i, err)
		}
		if exists {
			t.Errorf("record %d survived an aborted transaction", i)
		}
	}
}

// TestGenerationCAS underpins shard-lease fencing: Temporal's rangeID protocol
// is an optimistic lock, and EXPECT_GEN_EQUAL is how we express it.
func TestGenerationCAS(t *testing.T) {
	client := testClient(t)
	key := uniqueKey(t, "cas")

	wp := scWritePolicy()
	if err := client.PutBins(wp, key, as.NewBin("range_id", 1)); err != nil {
		t.Fatalf("initial put: %v", err)
	}
	t.Cleanup(func() {
		dp := scWritePolicy()
		dp.DurableDelete = true
		_, _ = client.Delete(dp, key)
	})

	rec, err := client.Get(nil, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	staleGen := rec.Generation

	// Someone else bumps the lease.
	if err := client.PutBins(wp, key, as.NewBin("range_id", 2)); err != nil {
		t.Fatalf("concurrent put: %v", err)
	}

	// Our write, guarded on the generation we read, must now fail.
	cas := scWritePolicy()
	cas.GenerationPolicy = as.EXPECT_GEN_EQUAL
	cas.Generation = staleGen
	cas.MaxRetries = 0 // never blind-retry a conditional write

	err = client.PutBins(cas, key, as.NewBin("range_id", 99))
	if err == nil {
		t.Fatal("stale-generation write succeeded; shard fencing would be unsound")
	}
	if !err.Matches(types.GENERATION_ERROR) {
		t.Fatalf("got %v, want GENERATION_ERROR", err)
	}
}

// TestKOrderedMapIntKeyRange checks the range semantics we rely on for
// immediate task categories: begin-inclusive, end-exclusive, ascending -- the
// same shape as Temporal's [InclusiveMinTaskKey, ExclusiveMaxTaskKey).
func TestKOrderedMapIntKeyRange(t *testing.T) {
	client := testClient(t)
	key := uniqueKey(t, "kmap-int")

	mp := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	wp := scWritePolicy()

	// Insert deliberately out of order.
	for _, id := range []int{5000, 1000, 3000, 2000, 4000} {
		if _, err := client.Operate(wp, key, as.MapPutOp(mp, "t", id, fmt.Sprintf("task-%d", id))); err != nil {
			t.Fatalf("map put %d: %v", id, err)
		}
	}
	t.Cleanup(func() {
		dp := scWritePolicy()
		dp.DurableDelete = true
		_, _ = client.Delete(dp, key)
	})

	rec, err := client.Operate(nil, key,
		as.MapGetByKeyRangeOp("t", 2000, 5000, as.MapReturnType.KEY))
	if err != nil {
		t.Fatalf("range read: %v", err)
	}

	got := toIntSlice(t, rec.Bins["t"])
	want := []int{2000, 3000, 4000} // 2000 included, 5000 excluded
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v (server did not return ascending order)", got, want)
		}
	}
}

// TestKOrderedMapBlobKeyOrdering is the load-bearing one.
//
// Temporal orders scheduled (timer) tasks by (visibility_ts, task_id). Aerospike
// has no compound key, but CDT ordering documents that BYTES keys compare
// bytewise -- so a 16-byte big-endian fireTime||taskID blob should sort exactly
// as Cassandra's compound clustering key does.
//
// The entire timer-task model depends on that being true. This test is the
// proof.
func TestKOrderedMapBlobKeyOrdering(t *testing.T) {
	client := testClient(t)
	key := uniqueKey(t, "kmap-blob")

	type tk struct {
		fireTime int64
		taskID   int64
	}
	// Insertion order is scrambled; the second field must break ties on the first.
	inserts := []tk{
		{fireTime: 200, taskID: 1},
		{fireTime: 100, taskID: 7},
		{fireTime: 200, taskID: 0},
		{fireTime: 100, taskID: 2},
		{fireTime: 300, taskID: 5},
	}
	// (fireTime, taskID) ascending.
	want := []tk{
		{100, 2}, {100, 7}, {200, 0}, {200, 1}, {300, 5},
	}

	mp := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	wp := scWritePolicy()

	for _, e := range inserts {
		if _, err := client.Operate(wp, key,
			as.MapPutOp(mp, "t", encodeTimerKey(e.fireTime, e.taskID), "payload")); err != nil {
			t.Fatalf("map put %v: %v", e, err)
		}
	}
	t.Cleanup(func() {
		dp := scWritePolicy()
		dp.DurableDelete = true
		_, _ = client.Delete(dp, key)
	})

	// Read the whole range: [ (0,0), (max,max) ).
	rec, err := client.Operate(nil, key, as.MapGetByKeyRangeOp("t",
		encodeTimerKey(0, 0), encodeTimerKey(1<<62, 0), as.MapReturnType.KEY))
	if err != nil {
		t.Fatalf("range read: %v", err)
	}

	keys, ok := rec.Bins["t"].([]interface{})
	if !ok {
		t.Fatalf("unexpected bin shape %T: %v", rec.Bins["t"], rec.Bins["t"])
	}
	if len(keys) != len(want) {
		t.Fatalf("got %d keys, want %d", len(keys), len(want))
	}

	for i, raw := range keys {
		b, ok := raw.([]byte)
		if !ok {
			t.Fatalf("key %d: unexpected type %T", i, raw)
		}
		ft, id := decodeTimerKey(t, b)
		if ft != want[i].fireTime || id != want[i].taskID {
			t.Fatalf("position %d: got (%d,%d) want (%d,%d) -- bytewise blob ordering does NOT match (fireTime, taskID)",
				i, ft, id, want[i].fireTime, want[i].taskID)
		}
	}
}

// encodeTimerKey packs a scheduled task's sort key into 16 big-endian bytes.
// Big-endian encoding of a non-negative int64 is order-preserving under
// bytewise comparison, which is how Aerospike compares BYTES map keys.
func encodeTimerKey(fireTime, taskID int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(fireTime))
	binary.BigEndian.PutUint64(b[8:16], uint64(taskID))
	return b
}

func decodeTimerKey(t *testing.T, b []byte) (fireTime, taskID int64) {
	t.Helper()
	if len(b) != 16 {
		t.Fatalf("timer key must be 16 bytes, got %d", len(b))
	}
	return int64(binary.BigEndian.Uint64(b[0:8])), int64(binary.BigEndian.Uint64(b[8:16]))
}

func toIntSlice(t *testing.T, v interface{}) []int {
	t.Helper()
	raw, ok := v.([]interface{})
	if !ok {
		t.Fatalf("unexpected shape %T: %v", v, v)
	}
	out := make([]int, len(raw))
	for i, e := range raw {
		n, ok := e.(int)
		if !ok {
			t.Fatalf("element %d: unexpected type %T", i, e)
		}
		out[i] = n
	}
	return out
}
