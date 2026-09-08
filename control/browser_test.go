package control

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"go.temporal.io/server/service/history/tasks"
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

// TestCompareNatural pins the ordering a viewer actually reads. Every composite
// key in this store is colon-joined numbers, and a plain string comparison
// files shard 10 between shard 1 and shard 2 -- which makes the bucket list, the
// one thing the history-task view exists to show in order, look wrong.
func TestCompareNatural(t *testing.T) {
	ordered := []string{
		"2:2:0", "3:1:0", "3:2:0", "3:2:1", "3:2:9", "3:2:10", "3:2:100",
		"10:1:0", "10:2:0", "100:1:0",
	}
	for i := 0; i+1 < len(ordered); i++ {
		if compareNatural(ordered[i], ordered[i+1]) >= 0 {
			t.Errorf("compareNatural(%q, %q) did not order them ascending", ordered[i], ordered[i+1])
		}
		if compareNatural(ordered[i+1], ordered[i]) <= 0 {
			t.Errorf("compareNatural is not antisymmetric for %q / %q", ordered[i], ordered[i+1])
		}
	}
	if compareNatural("3:2:0", "3:2:0") != 0 {
		t.Error("compareNatural says a key differs from itself")
	}
	// Leading zeros are a value, not a prefix.
	if compareNatural("3:2:007", "3:2:7") != 0 {
		t.Error("leading zeros changed the ordering of a digit run")
	}
	// A digit run wider than int64 must still order, not overflow.
	if compareNatural("99999999999999999999", "100000000000000000000") >= 0 {
		t.Error("digit runs beyond int64 did not compare by value")
	}
}

// TestSortRecordsDeterministic is the core of change 1: ScanAll returns records
// in digest order, which is effectively random and differs between calls when
// the server splits the scan across partitions differently. Sorting has to turn
// any arrival order into the same output order, or the panel reshuffles while
// nothing is happening.
func TestSortRecordsDeterministic(t *testing.T) {
	input := []RecordView{
		{Key: "10:2:0", Digest: "dd", hasKey: true},
		{Key: "#0badf00d", Digest: "bb"},
		{Key: "3:2:1", Digest: "cc", hasKey: true},
		{Key: "#0aaaaaaa", Digest: "aa"},
		{Key: "3:2:0", Digest: "ee", hasKey: true},
		{Key: "2:2:0", Digest: "ff", hasKey: true},
	}
	want := []string{"2:2:0", "3:2:0", "3:2:1", "10:2:0", "#0aaaaaaa", "#0badf00d"}

	// Every rotation of the input stands in for a different scan order.
	for shift := range input {
		shuffled := make([]RecordView, 0, len(input))
		shuffled = append(shuffled, input[shift:]...)
		shuffled = append(shuffled, input[:shift]...)

		sortRecords(shuffled)

		got := make([]string, len(shuffled))
		for i, r := range shuffled {
			got[i] = r.Key
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("rotation %d sorted to %v, want %v", shift, got, want)
			}
		}
	}
}

// TestSortRecordsKeylessRecordsDoNotBreakOrdering covers the case the store
// documents in docs/04-open-questions.md R9: a record written before sendKey was
// enabled carries only a digest, so displayKey substitutes one. Those must not
// interleave with real keys on a value that means nothing, and must still order
// deterministically among themselves.
func TestSortRecordsKeylessRecordsDoNotBreakOrdering(t *testing.T) {
	records := []RecordView{
		{Key: "#ffffffff", Digest: "ff"},
		{Key: "9:1:0", Digest: "22", hasKey: true},
		{Key: "#00000000", Digest: "00"},
		{Key: "1:1:0", Digest: "11", hasKey: true},
	}
	sortRecords(records)

	if !records[0].hasKey || !records[1].hasKey {
		t.Fatalf("keyed records did not sort first: %+v", records)
	}
	if records[2].hasKey || records[3].hasKey {
		t.Fatalf("keyless records did not sort last: %+v", records)
	}
	if records[0].Key != "1:1:0" || records[1].Key != "9:1:0" {
		t.Errorf("keyed records out of order: %q, %q", records[0].Key, records[1].Key)
	}
	if records[2].Digest != "00" || records[3].Digest != "ff" {
		t.Errorf("keyless records did not fall back to digest order: %q, %q",
			records[2].Digest, records[3].Digest)
	}
}

// TestDescribeValuePreservesServerOrder is the within-record half of change 1.
// A K-ordered map arrives as []as.MapPair already in key order -- that ordering
// is the data model, standing in for Cassandra's clustering key -- so the
// renderer must not drop it into a Go map on the way to the screen. The
// unordered shape has no such order, and is sorted only so two consecutive
// polls render it identically.
func TestDescribeValuePreservesServerOrder(t *testing.T) {
	ordered := []as.MapPair{
		{Key: 1024, Value: []any{[]byte{1, 2, 3}, "Proto3"}},
		{Key: 1025, Value: []any{[]byte{4}, "Proto3"}},
		{Key: 1026, Value: []any{[]byte{5}, "Proto3"}},
		{Key: 1027, Value: []any{[]byte{6}, "Proto3"}},
	}
	typ, preview, size := describeValue(ordered)
	if typ != "map" || size != 4 {
		t.Fatalf("K-ordered map rendered as type=%q size=%d", typ, size)
	}
	if !strings.HasPrefix(preview, "4 entries: 1024 → ") {
		t.Fatalf("K-ordered map preview did not lead with the first key: %q", preview)
	}
	if strings.Index(preview, "1024") > strings.Index(preview, "1025") {
		t.Errorf("server key order was not preserved: %q", preview)
	}

	// An unordered map: Go randomises iteration, so the same value must still
	// render the same way twice.
	unordered := map[any]any{1: "a", 2: "b", 10: "c", 3: "d"}
	_, first, _ := describeValue(unordered)
	for range 20 {
		if _, again, _ := describeValue(unordered); again != first {
			t.Fatalf("unordered map rendered two ways: %q vs %q", first, again)
		}
	}
	if !strings.Contains(first, "1 → a, 2 → b, 3 → d") {
		t.Errorf("unordered map preview is not in a readable order: %q", first)
	}
}

// TestParseHistoryTaskKey pins the decomposition the history-task view is
// built on: the record key *is* the model.
func TestParseHistoryTaskKey(t *testing.T) {
	shard, category, bucket, ok := parseHistoryTaskKey("3:2:29348280")
	if !ok || shard != 3 || category != 2 || bucket != 29348280 {
		t.Fatalf("parsed 3:2:29348280 as shard=%d category=%d bucket=%d ok=%v",
			shard, category, bucket, ok)
	}
	for _, bad := range []string{"", "3:2", "3:2:0:1", "#0badf00d", "a:2:0", "3:b:0", "3:2:c"} {
		if _, _, _, ok := parseHistoryTaskKey(bad); ok {
			t.Errorf("parseHistoryTaskKey accepted %q", bad)
		}
	}
}

// TestDescribeCategoryUsesTemporalRegistry checks that names come from
// Temporal's own registry rather than a list here that would drift, and that an
// id the registry does not know is still described from the shape of its keys.
func TestDescribeCategoryUsesTemporalRegistry(t *testing.T) {
	cases := []struct {
		id        int
		name      string
		scheduled bool
	}{
		{tasks.CategoryIDTransfer, "transfer", false},
		{tasks.CategoryIDTimer, "timer", true},
		{tasks.CategoryIDVisibility, "visibility", false},
		{tasks.CategoryIDReplication, "replication", false},
		{tasks.CategoryIDOutbound, "outbound", false},
		{tasks.CategoryIDArchival, "archival", true},
		{tasks.CategoryIDMemoryTimer, "memory-timer", true},
	}
	for _, c := range cases {
		name, scheduled := describeCategory(c.id, nil)
		if name != c.name || scheduled != c.scheduled {
			t.Errorf("category %d described as (%q, scheduled=%v), want (%q, %v)",
				c.id, name, scheduled, c.name, c.scheduled)
		}
	}

	// An unregistered category, inferred from the key encoding: a 16-byte BLOB
	// key means scheduled, an int64 means immediate. The BLOB arrives as a
	// fixed-size array inside a MapPair, which is the shape a plain []byte
	// assertion drops -- docs/04-open-questions.md R10.
	var blobKey [16]uint8
	name, scheduled := describeCategory(99, []as.MapPair{{Key: blobKey}})
	if name != "category-99" || !scheduled {
		t.Errorf("unknown category with BLOB keys described as (%q, scheduled=%v)", name, scheduled)
	}
	name, scheduled = describeCategory(99, []as.MapPair{{Key: 1024}})
	if name != "category-99" || scheduled {
		t.Errorf("unknown category with int keys described as (%q, scheduled=%v)", name, scheduled)
	}
}

// TestRenderTaskEntries covers both key encodings and the entry cap.
func TestRenderTaskEntries(t *testing.T) {
	immediate := []as.MapPair{
		{Key: 1024, Value: []any{make([]byte, 141), "Proto3"}},
		{Key: 1025, Value: []any{make([]byte, 7), "Proto3"}},
	}
	entries := renderTaskEntries(immediate, false)
	if len(entries) != 2 {
		t.Fatalf("rendered %d immediate entries, want 2", len(entries))
	}
	if entries[0].Label != "task 1024" || entries[0].TaskID != 1024 ||
		entries[0].FireTime != nil || entries[0].Bytes != 141 {
		t.Errorf("immediate entry rendered as %+v", entries[0])
	}

	// A scheduled key: bigendian(fireTimeNanos) || bigendian(taskID), arriving
	// as the array shape a MapPair uses.
	fire := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	var key [16]uint8
	binary.BigEndian.PutUint64(key[0:8], uint64(fire.UnixNano()))
	binary.BigEndian.PutUint64(key[8:16], 4242)
	entries = renderTaskEntries([]as.MapPair{{Key: key, Value: []any{make([]byte, 88), "Proto3"}}}, true)
	if len(entries) != 1 {
		t.Fatalf("rendered %d scheduled entries, want 1", len(entries))
	}
	if entries[0].TaskID != 4242 || entries[0].Bytes != 88 {
		t.Errorf("scheduled entry rendered as %+v", entries[0])
	}
	if entries[0].FireTime == nil || *entries[0].FireTime != "2026-09-07T12:00:00Z" {
		t.Errorf("scheduled entry fireTime = %v", entries[0].FireTime)
	}
	if !strings.Contains(entries[0].Label, "2026-09-07T12:00:00Z") ||
		!strings.Contains(entries[0].Label, "task 4242") {
		t.Errorf("scheduled label = %q", entries[0].Label)
	}

	// The cap bounds the response; Count, set by the caller, still reports the
	// true size.
	many := make([]as.MapPair, historyTaskEntryCap+10)
	for i := range many {
		many[i] = as.MapPair{Key: i, Value: []any{[]byte{0}, "Proto3"}}
	}
	if got := len(renderTaskEntries(many, false)); got != historyTaskEntryCap {
		t.Errorf("entry cap returned %d entries, want %d", got, historyTaskEntryCap)
	}
}

// TestBrowserRecordsOrderIsStable is change 1 against the live node: two scans
// of the same set must present the records the same way round. Records may be
// written or completed between the calls, so the assertion is on the relative
// order of the records both calls saw, not on the lists being identical.
func TestBrowserRecordsOrderIsStable(t *testing.T) {
	b := testBrowser(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sets, err := b.Sets(ctx)
	if err != nil {
		t.Fatalf("listing sets: %v", err)
	}
	// The busiest of the store's own sets: the more records, the more scan
	// order has a chance to differ between calls. Conformance runs leave
	// prefixed sets behind, and those carry no description, which is what
	// distinguishes them here.
	var target string
	var best int
	for _, s := range sets {
		if s.Description != "" && s.Objects > best {
			target, best = s.Name, s.Objects
		}
	}
	if best < 2 {
		t.Skip("no populated set to scan; run the e2e demo first")
	}

	first, err := b.Records(ctx, target, 200)
	if err != nil {
		t.Fatalf("scanning %s: %v", target, err)
	}
	second, err := b.Records(ctx, target, 200)
	if err != nil {
		t.Fatalf("re-scanning %s: %v", target, err)
	}
	t.Logf("set %s: %d then %d records", target, len(first), len(second))

	if !sort.SliceIsSorted(first, func(i, j int) bool {
		return recordLess(first[i], first[j])
	}) {
		t.Error("first scan is not in sorted order")
	}

	// Relative order of the records common to both calls.
	inSecond := map[string]int{}
	for i, r := range second {
		inSecond[r.Digest] = i
	}
	prev := -1
	common := 0
	for _, r := range first {
		pos, ok := inSecond[r.Digest]
		if !ok {
			continue
		}
		common++
		if pos < prev {
			t.Fatalf("record %q moved backwards between calls; the scan order is leaking through", r.Key)
		}
		prev = pos
	}
	if common < 2 {
		t.Skipf("only %d records common to both scans; nothing to compare", common)
	}
	t.Logf("%d records common to both scans, in the same relative order", common)
}

// recordLess mirrors the comparator sortRecords uses, so the test asserts the
// property rather than re-running the same call.
func recordLess(a, b RecordView) bool {
	if a.hasKey != b.hasKey {
		return a.hasKey
	}
	if a.hasKey {
		if c := compareNatural(a.Key, b.Key); c != 0 {
			return c < 0
		}
	}
	return a.Digest < b.Digest
}

// TestBrowserHistoryTasks prints the view against the live node so its shape can
// be checked by eye, and asserts the four orderings that are the lesson: shards
// by id, categories by id, buckets ascending, and entries in the server's own
// key order within each bucket.
func TestBrowserHistoryTasks(t *testing.T) {
	b := testBrowser(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	view, err := b.HistoryTasks(ctx)
	if err != nil {
		t.Fatalf("reading history tasks: %v", err)
	}

	if view.Bucketing.ImmediateShift != 12 || view.Bucketing.ScheduledBucketSeconds != 60 {
		t.Errorf("bucketing reported as %+v", view.Bucketing)
	}

	encoded, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		t.Fatalf("marshalling the view: %v", err)
	}
	t.Logf("HistoryTasks:\n%s", encoded)

	if len(view.Shards) == 0 {
		t.Skip("htask set is empty; run the e2e demo first")
	}

	var buckets, entries int
	for i, shard := range view.Shards {
		if i > 0 && view.Shards[i-1].ShardID >= shard.ShardID {
			t.Errorf("shards are not ascending by id: %d then %d",
				view.Shards[i-1].ShardID, shard.ShardID)
		}
		for j, category := range shard.Categories {
			if j > 0 && shard.Categories[j-1].ID >= category.ID {
				t.Errorf("shard %d categories are not ascending by id", shard.ShardID)
			}
			if category.Name == "" {
				t.Errorf("category %d has no name", category.ID)
			}
			for k, bucket := range category.Buckets {
				buckets++
				if k > 0 && category.Buckets[k-1].Bucket >= bucket.Bucket {
					t.Errorf("%s buckets are not ascending: %d then %d",
						bucket.RecordKey, category.Buckets[k-1].Bucket, bucket.Bucket)
				}
				if want := fmt.Sprintf("%d:%d:%d", shard.ShardID, category.ID, bucket.Bucket); bucket.RecordKey != want {
					t.Errorf("recordKey %q does not match its decomposition %q", bucket.RecordKey, want)
				}
				if bucket.Truncated != (bucket.Count > historyTaskEntryCap) {
					t.Errorf("%s truncated=%v with count=%d", bucket.RecordKey, bucket.Truncated, bucket.Count)
				}
				if len(bucket.Entries) > historyTaskEntryCap {
					t.Errorf("%s returned %d entries, above the cap", bucket.RecordKey, len(bucket.Entries))
				}
				entries += len(bucket.Entries)
				assertEntriesOrdered(t, bucket, category.Scheduled)
			}
		}
	}
	t.Logf("%d shards, %d buckets, %d entries", len(view.Shards), buckets, entries)
	if buckets == 0 {
		t.Error("htask records exist but no buckets were decomposed; sendKey may be off")
	}
}

// assertEntriesOrdered checks the half of the ordering the *server* supplies.
// Entries come out of a K-ordered map already in key order -- ascending task id
// for an immediate category, ascending (fireTime, taskID) for a scheduled one --
// and this view must hand that through untouched.
func assertEntriesOrdered(t *testing.T, bucket TaskBucket, scheduled bool) {
	t.Helper()
	for i := 1; i < len(bucket.Entries); i++ {
		prev, cur := bucket.Entries[i-1], bucket.Entries[i]
		if !scheduled {
			if prev.TaskID >= cur.TaskID {
				t.Errorf("%s entries are not in task-id order: %d then %d",
					bucket.RecordKey, prev.TaskID, cur.TaskID)
			}
			if cur.FireTime != nil {
				t.Errorf("%s is immediate but entry %d carries a fireTime", bucket.RecordKey, i)
			}
			continue
		}
		if prev.FireTime == nil || cur.FireTime == nil {
			t.Errorf("%s is scheduled but entry %d has no fireTime", bucket.RecordKey, i)
			continue
		}
		if *prev.FireTime > *cur.FireTime ||
			(*prev.FireTime == *cur.FireTime && prev.TaskID >= cur.TaskID) {
			t.Errorf("%s entries are not in (fireTime, taskID) order: %s/%d then %s/%d",
				bucket.RecordKey, *prev.FireTime, prev.TaskID, *cur.FireTime, cur.TaskID)
		}
	}
}

// TestBrowserTruncateUnusedPrefix exercises Truncate without destroying the
// demo's data.
//
// Truncate empties every set the store writes, under the configured set prefix.
// Pointing it at a prefix nothing has ever written to makes every set in its
// list absent, which is the interesting edge anyway: Aerospike has no DDL, so a
// set that has not been written to does not exist, and "nothing to empty" has to
// read as success rather than as a wall of errors.
//
// The real data lives under the empty prefix. The guard below is what keeps this
// test from becoming the thing that wipes it.
func TestBrowserTruncateUnusedPrefix(t *testing.T) {
	b := testBrowser(t)

	const prefix = "brwsrtest_"
	if prefix == "" || prefix == b.cfg.AerospikeSetPrefix {
		t.Fatal("refusing to truncate the configured set prefix")
	}
	b.cfg.AerospikeSetPrefix = prefix

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := b.Truncate(ctx); err != nil {
		t.Fatalf("truncating absent sets under prefix %q: %v", prefix, err)
	}

	// The sets it just truncated must not now show up as data.
	sets, err := b.Sets(ctx)
	if err != nil {
		t.Fatalf("listing sets: %v", err)
	}
	for _, s := range sets {
		if strings.HasPrefix(s.Name, prefix) && s.Objects > 0 {
			t.Errorf("set %s holds %d objects after truncation", s.Name, s.Objects)
		}
	}
}
