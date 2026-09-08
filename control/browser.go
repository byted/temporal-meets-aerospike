package control

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"go.temporal.io/server/service/history/tasks"
)

// Browser is read-only introspection over the Aerospike node holding Temporal's
// data. It exists so the demo can answer "where did my workflow actually go?"
// by showing the records, rather than asking the audience to take it on trust.
//
// Nothing here writes. Deliberately: this runs against the same live namespace
// a Temporal server is serving from, during a demo, in front of people.
type Browser struct {
	cfg Config

	// mu guards lazy connection. The Aerospike node may be down or still
	// rostering when this process starts, and that must not be fatal -- the
	// browser tab is one part of the demo and the rest has to keep working.
	mu     sync.Mutex
	client *as.Client
	info   *as.InfoPolicy
	scan   *as.ScanPolicy
}

// Health is the namespace-level answer to "is Aerospike serving, and is it
// serving *correctly*". The partition counts are the ones that matter in a
// strong-consistency namespace: dead or unavailable partitions mean reads and
// writes for part of the key space are being refused, which looks exactly like
// a Temporal bug from the outside.
type Health struct {
	Reachable             bool `json:"reachable"`
	StrongConsistency     bool `json:"strongConsistency"`
	DeadPartitions        int  `json:"deadPartitions"`
	UnavailablePartitions int  `json:"unavailablePartitions"`
	Objects               int  `json:"objects"`
}

// SetInfo is one Aerospike set with its role in Temporal's data model.
type SetInfo struct {
	Name        string `json:"name"`
	Objects     int    `json:"objects"`
	Description string `json:"description"`
}

// RecordView is one record rendered for display.
type RecordView struct {
	Key    string    `json:"key"`
	Digest string    `json:"digest"`
	Bins   []BinView `json:"bins"`

	// hasKey records whether Key is the record's real user key or the digest
	// stand-in displayKey falls back to. It is what keeps keyless records from
	// interleaving with keyed ones during the sort; see sortRecords.
	hasKey bool
}

// BinView is one bin, rendered rather than decoded. Most of the interesting
// bins hold serialized protos that only Temporal can interpret; see previewBlob.
type BinView struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Preview string `json:"preview"`
	Size    int    `json:"size"`
}

// setDescriptions maps each of the store's sets to what it holds. The order is
// the order the UI lists them in: the path a workflow actually takes through
// persistence, from the shard lease it is fenced by to the matching tasks that
// dispatch it. docs/03-data-model.md is the long version.
var setDescriptions = []struct {
	name string
	desc string
}{
	{"shard", "Shard lease: the range_id fencing token every other write is conditioned on"},
	{"exec", "Mutable state: one record per workflow run, with activities and timers as K-ordered map bins"},
	{"curr", "Current-execution pointer: which run is current for a workflow id"},
	{"htask", "History tasks: transfer, timer and visibility tasks, bucketed into K-ordered maps"},
	{"htaskidx", "History task index: per-category bucket bookkeeping for range scans"},
	{"dlq", "Replication DLQ: tasks that could not be applied"},
	{"hnode", "History events: the immutable event batches a workflow's history is made of"},
	{"hbranch", "History branch: the node index for one branch of a history tree"},
	{"htree", "History tree: branches of one workflow's history, for reset and conflict resolution"},
	{"tq", "Task queues (matching): the queue record and its lease"},
	{"task", "Matching tasks: workflow and activity tasks waiting for a worker to poll"},
	{"taskidx", "Task index: bucket bookkeeping for the matching task queues"},
	{"tqdata", "Task queue user data: versioning and build-id metadata"},
	{"buildidx", "Build-id index: worker versioning lookups"},
	{"ns", "Namespaces, keyed by name -- this is what the backend switch empties"},
	{"nsid", "Namespace id to name pointer"},
	{"nsmeta", "Namespace metadata: the single notification_version record"},
	{"cmeta", "Cluster metadata and its optimistic-lock version"},
	{"cmember", "Cluster membership: host heartbeats, the one place TTLs are load-bearing"},
	{"q2meta", "Queue V2 metadata: one record per queue"},
	{"q2msg", "Queue V2 messages: bucketed K-ordered maps of enqueued messages"},
	{"nexus", "Nexus endpoints"},
	{"nexusver", "Nexus endpoint table version"},
	{"schema", "Store schema version"},
}

func NewBrowser(cfg Config) *Browser {
	return &Browser{cfg: cfg}
}

func (b *Browser) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		b.client.Close()
		b.client = nil
	}
}

// connect returns a connected client, dialling on first use and after a
// failure. Caller must not hold b.mu.
func (b *Browser) connect() (*as.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.client != nil && b.client.IsConnected() {
		return b.client, nil
	}
	if b.client != nil {
		b.client.Close()
		b.client = nil
	}

	policy := as.NewClientPolicy()
	policy.Timeout = 5 * time.Second
	// The node advertises the address it is configured with, not the one we
	// reached it on. See Config.UseServicesAlternate.
	policy.UseServicesAlternate = b.cfg.UseServicesAlternate

	host, port, err := splitHostPort(b.cfg.AerospikeHost)
	if err != nil {
		return nil, err
	}
	client, err := as.NewClientWithPolicyAndHost(policy, as.NewHost(host, port))
	if err != nil {
		return nil, fmt.Errorf("connecting to Aerospike at %s: %w", b.cfg.AerospikeHost, err)
	}

	b.client = client
	b.info = as.NewInfoPolicy()
	b.info.Timeout = 5 * time.Second
	b.scan = as.NewScanPolicy()
	b.scan.TotalTimeout = 10 * time.Second
	b.scan.SocketTimeout = 10 * time.Second
	return client, nil
}

// Health reads namespace statistics. A node that is not reachable is reported
// as unreachable rather than as an error: "Aerospike is down" is a legitimate
// state for this panel to display, including immediately after a switch away
// from it.
func (b *Browser) Health(ctx context.Context) Health {
	fields, err := b.namespaceInfo(ctx)
	if err != nil {
		return Health{Reachable: false}
	}
	return Health{
		Reachable:             true,
		StrongConsistency:     fields["strong-consistency"] == "true",
		DeadPartitions:        atoiOr(fields["dead_partitions"], 0),
		UnavailablePartitions: atoiOr(fields["unavailable_partitions"], 0),
		Objects:               atoiOr(fields["objects"], 0),
	}
}

// namespaceInfo runs the `namespace/<ns>` info command, whose response is a
// single line of `key=value` pairs separated by semicolons.
func (b *Browser) namespaceInfo(ctx context.Context) (map[string]string, error) {
	cmd := "namespace/" + b.cfg.AerospikeNamespace
	raw, err := b.requestInfo(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return parseInfoPairs(raw[cmd], ";"), nil
}

// Sets lists the sets that exist in the namespace with their object counts,
// annotated with what each one means to Temporal.
//
// Only sets that have been written to appear: Aerospike has no DDL, so a set
// comes into existence on its first write and there is nothing to list before
// that. Running the demo on SQLite and then switching therefore shows the list
// growing, which is the point.
func (b *Browser) Sets(ctx context.Context) ([]SetInfo, error) {
	cmd := "sets/" + b.cfg.AerospikeNamespace
	raw, err := b.requestInfo(ctx, cmd)
	if err != nil {
		return nil, err
	}

	counts := map[string]int{}
	// One entry per set, separated by ';'. The fields *within* an entry are
	// separated by ':', not ';' -- the info protocol is not consistent about
	// this, and splitting `sets/` the way `namespace/` splits yields one
	// enormous unparseable field.
	for _, entry := range strings.Split(raw[cmd], ";") {
		fields := parseInfoPairs(entry, ":")
		name := firstOf(fields, "set", "set_name")
		if name == "" {
			continue
		}
		counts[name] = atoiOr(fields["objects"], 0)
	}

	return b.annotate(counts), nil
}

// annotate orders the sets by their place in the data model and attaches a
// description, appending anything unrecognised at the end so a set added later
// still shows up instead of vanishing.
func (b *Browser) annotate(counts map[string]int) []SetInfo {
	out := make([]SetInfo, 0, len(counts))
	seen := map[string]bool{}

	for _, d := range setDescriptions {
		physical := b.cfg.AerospikeSetPrefix + d.name
		n, ok := counts[physical]
		if !ok {
			continue
		}
		seen[physical] = true
		out = append(out, SetInfo{Name: physical, Objects: n, Description: d.desc})
	}

	rest := make([]string, 0)
	for name := range counts {
		if !seen[name] {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	for _, name := range rest {
		out = append(out, SetInfo{Name: name, Objects: counts[name], Description: foreignSets[name]})
	}
	return out
}

// foreignSets are sets in the namespace that this store did not create. They
// are never prefixed, so they are matched after the store's own sets rather
// than alongside them.
//
// <ERO~MRT is the server's own multi-record-transaction monitor set. It is the
// physical trace of the store's transactions -- worth naming rather than
// leaving in the list as an unexplained control character.
var foreignSets = map[string]string{
	"<ERO~MRT": "Aerospike internal: multi-record transaction monitor records",
	"captest":  "Scratch set used by the Aerospike capability tests",
}

// Records scans a set and renders up to limit records.
//
// A scan, not a query: Aerospike has no ordered range scan across records, and
// the store never needs one, so there is no secondary index to query. That
// makes this the only way to enumerate a set -- and the reason limit is bounded
// hard rather than politely.
func (b *Browser) Records(ctx context.Context, set string, limit int) ([]RecordView, error) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 200 {
		limit = 200
	}

	client, err := b.connect()
	if err != nil {
		return nil, err
	}

	policy := *b.scan
	policy.MaxRecords = int64(limit)
	applyDeadline(ctx, &policy.TotalTimeout)

	recordset, err := client.ScanAll(&policy, b.cfg.AerospikeNamespace, set)
	if err != nil {
		return nil, fmt.Errorf("scanning set %s in namespace %s: %w", set, b.cfg.AerospikeNamespace, err)
	}
	defer recordset.Close()

	out := make([]RecordView, 0, limit)
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("scanning set %s: %w", set, res.Err)
		}
		out = append(out, renderRecord(res.Record))
		if len(out) >= limit {
			// MaxRecords is approximate -- the server divides it across
			// partitions -- so the client-side count is what actually bounds
			// the response.
			break
		}
	}

	// The scan hands these back in digest order. Nothing downstream re-sorts
	// them, so without this the panel reshuffles on every poll.
	sortRecords(out)
	return out, nil
}

// --- History tasks: the bucketed model, made visible ---
//
// This is the one part of the store a viewer cannot guess from the record list,
// so it gets its own view rather than being left as another set to scroll.
//
// Cassandra keeps every task for a shard in one partition, ordered by a
// clustering key, and answers "tasks in [min, max) ascending" with a single
// range scan. Aerospike has neither a partition key we choose nor any ordering
// across records. What it does have is ordering *inside* a K-ordered map. So
// the clustering key becomes a map key, and the partition becomes a run of
// bucketed records:
//
//	htask/<shard>:<category>:<bucket>   bin "t": K-ordered map taskKey -> blob
//
// Bucket indices are monotonic in the sort key, so walking buckets in order and
// concatenating each one's server-ordered entries reproduces the single ordered
// stream Cassandra gets for free. Both halves of that sentence are visible in
// this view, which is the reason it exists: the bucket list is ordered by us,
// the entry list inside each bucket is ordered by the server.
//
// store/aerospike/tasks.go is the code being described.

const (
	// setHistoryTask mirrors the store's set name. The store's constant is
	// unexported, so this is a copy; docs/03-data-model.md lists the sets.
	setHistoryTask = "htask"
	// binTaskMap is the K-ordered map bin inside a bucket record.
	binTaskMap = "t"

	// These mirror immediateBucketShift and scheduledBucketNanos in
	// store/aerospike/tasks.go. They are reported rather than used: the view
	// decomposes keys the store already wrote, it does not recompute buckets.
	// If the store retunes them (docs/04-open-questions.md Q2), these move too.
	immediateBucketShift   = 12
	scheduledBucketSeconds = 60

	// historyTaskEntryCap bounds entries rendered per bucket. An immediate
	// bucket holds up to 4096 tasks and a busy queue has many buckets, so the
	// uncapped response is unbounded in a way a browser tab will notice.
	// TaskBucket.Count still reports the true size.
	historyTaskEntryCap = 50

	// historyTaskRecordCap bounds the scan itself, for the same reason.
	historyTaskRecordCap = 500
)

// HistoryTaskView is the htask set decomposed into the three parts of its
// record key. Serialisation shape is fixed: the UI is built against it.
type HistoryTaskView struct {
	Bucketing BucketingInfo `json:"bucketing"`
	Shards    []ShardTasks  `json:"shards"`
}

// BucketingInfo reports the two bucket functions, so the UI can state the rule
// rather than leaving a viewer to infer it from the numbers.
type BucketingInfo struct {
	// ImmediateShift is the right shift applied to a task id: bucket =
	// taskID >> 12, so 4096 task ids per record.
	ImmediateShift int `json:"immediateShift"`
	// ScheduledBucketSeconds is the fire-time window per record.
	ScheduledBucketSeconds int `json:"scheduledBucketSeconds"`
}

// ShardTasks is one history shard's queues.
type ShardTasks struct {
	ShardID    int64           `json:"shardId"`
	Categories []CategoryTasks `json:"categories"`
}

// CategoryTasks is one task category within a shard -- one Cassandra
// partition's worth of queue, spread over the buckets below.
type CategoryTasks struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	// Scheduled distinguishes the two key encodings. Immediate categories key
	// on an int64 task id; scheduled ones on a 16-byte big-endian
	// fireTime||taskID blob, because BYTES map keys compare bytewise and a
	// big-endian non-negative int64 is order-preserving under that comparison.
	Scheduled bool         `json:"scheduled"`
	Buckets   []TaskBucket `json:"buckets"`
}

// TaskBucket is one Aerospike record.
type TaskBucket struct {
	Bucket int64 `json:"bucket"`
	// RecordKey is the actual primary key, "<shard>:<category>:<bucket>".
	RecordKey string `json:"recordKey"`
	// Count is the number of entries in the map, before the entry cap.
	Count int `json:"count"`
	// Truncated reports that Entries is a prefix of the bucket rather than the
	// whole of it. Count minus len(Entries) is how many are missing.
	Truncated bool        `json:"truncated"`
	Entries   []TaskEntry `json:"entries"`
}

// TaskEntry is one map entry: one task.
type TaskEntry struct {
	Label  string `json:"label"`
	TaskID int64  `json:"taskId"`
	// FireTime is RFC3339 for a scheduled category and null for an immediate
	// one, mirroring the two key encodings.
	FireTime *string `json:"fireTime"`
	// Bytes is the size of the stored task blob. The blob is a Temporal proto
	// serialized above the persistence seam; the store never looks inside it
	// and neither does this, so its size is the honest thing to show.
	Bytes int `json:"bytes"`
}

// HistoryTasks returns the htask set decomposed into shard, category and
// bucket.
//
// A scan, because that is the only way to enumerate a set -- but note the
// asymmetry with how the store reads the same data. The store never scans: it
// computes the covering bucket span from the requested key range and consults
// the htaskidx index, so it touches only populated buckets by known key. This
// view scans precisely because it wants the records the store would *not* have
// looked at, including buckets emptied but not yet retired.
//
// Every ordering in the result is deliberate; see the sorts at the bottom.
func (b *Browser) HistoryTasks(ctx context.Context) (*HistoryTaskView, error) {
	client, err := b.connect()
	if err != nil {
		return nil, err
	}

	set := b.cfg.AerospikeSetPrefix + setHistoryTask

	policy := *b.scan
	policy.MaxRecords = historyTaskRecordCap
	applyDeadline(ctx, &policy.TotalTimeout)

	recordset, err := client.ScanAll(&policy, b.cfg.AerospikeNamespace, set)
	if err != nil {
		return nil, fmt.Errorf("scanning set %s in namespace %s: %w", set, b.cfg.AerospikeNamespace, err)
	}
	defer recordset.Close()

	view := &HistoryTaskView{
		Bucketing: BucketingInfo{
			ImmediateShift:         immediateBucketShift,
			ScheduledBucketSeconds: scheduledBucketSeconds,
		},
		Shards: []ShardTasks{},
	}

	// Keyed by shard then category id while accumulating; ordered on the way
	// out. Grouping in a Go map is safe here and only here, because what it
	// holds are whole buckets that get sorted afterwards -- not map entries,
	// whose order came from the server and cannot be reconstructed.
	byShard := map[int64]map[int]*CategoryTasks{}

	seen := 0
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("scanning set %s: %w", set, res.Err)
		}
		if seen >= historyTaskRecordCap {
			break
		}
		seen++

		rec := res.Record
		shardID, categoryID, bucket, ok := parseHistoryTaskKey(userKeyString(rec.Key))
		if !ok {
			// No user key: the record predates sendKey, and the shard,
			// category and bucket live only in the digest. Nothing to show.
			continue
		}

		pairs := recordMapPairs(rec, binTaskMap)
		name, scheduled := describeCategory(categoryID, pairs)

		categories, hasShard := byShard[shardID]
		if !hasShard {
			categories = map[int]*CategoryTasks{}
			byShard[shardID] = categories
		}
		category, hasCategory := categories[categoryID]
		if !hasCategory {
			category = &CategoryTasks{ID: categoryID, Name: name, Scheduled: scheduled, Buckets: []TaskBucket{}}
			categories[categoryID] = category
		}

		category.Buckets = append(category.Buckets, TaskBucket{
			Bucket:    bucket,
			RecordKey: fmt.Sprintf("%d:%d:%d", shardID, categoryID, bucket),
			Count:     len(pairs),
			Truncated: len(pairs) > historyTaskEntryCap,
			// Emitted even when empty: a bucket drained by
			// RangeCompleteHistoryTasks but not yet retired is a real state,
			// and seeing it is how the retirement step becomes visible.
			Entries: renderTaskEntries(pairs, scheduled),
		})
	}

	for shardID, categories := range byShard {
		shard := ShardTasks{ShardID: shardID, Categories: make([]CategoryTasks, 0, len(categories))}
		for _, category := range categories {
			// Bucket order is the model's claim: buckets are monotonic in the
			// sort key, so this ascending walk is exactly the order a range
			// read visits them in.
			sort.Slice(category.Buckets, func(i, j int) bool {
				return category.Buckets[i].Bucket < category.Buckets[j].Bucket
			})
			shard.Categories = append(shard.Categories, *category)
		}
		sort.Slice(shard.Categories, func(i, j int) bool {
			return shard.Categories[i].ID < shard.Categories[j].ID
		})
		view.Shards = append(view.Shards, shard)
	}
	sort.Slice(view.Shards, func(i, j int) bool {
		return view.Shards[i].ShardID < view.Shards[j].ShardID
	})

	return view, nil
}

// parseHistoryTaskKey splits "<shard>:<category>:<bucket>".
func parseHistoryTaskKey(key string) (shardID int64, categoryID int, bucket int64, ok bool) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return 0, 0, 0, false
	}
	shardID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	categoryID, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, false
	}
	bucket, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	return shardID, categoryID, bucket, true
}

// taskCategories is Temporal's own category registry, so the id -> name and
// id -> immediate/scheduled mappings come from the source of truth rather than
// from a list here that would drift the moment a category is added.
var taskCategories = buildTaskCategories()

func buildTaskCategories() map[int]tasks.Category {
	registry := tasks.NewDefaultTaskCategoryRegistry()
	// Archival is registered conditionally by the history service rather than
	// unconditionally by the default registry, but its records look identical
	// in the store, so add it if it is absent. AddCategory panics on a
	// duplicate, hence the check rather than an unconditional add.
	if _, ok := registry.GetCategoryByID(tasks.CategoryArchival.ID()); !ok {
		registry.AddCategory(tasks.CategoryArchival)
	}
	return registry.GetCategories()
}

// describeCategory names a category and reports whether it is scheduled.
//
// A category id read out of a record key may not be in the registry: it can be
// a custom category some other build registered, or data written by a newer
// server. The key encoding is still recoverable in that case, because the two
// encodings are distinguishable by shape -- a scheduled key is a 16-byte BLOB,
// an immediate one an int64 -- so an unknown category is described from its
// data instead of being dropped.
func describeCategory(id int, pairs []as.MapPair) (name string, scheduled bool) {
	if category, ok := taskCategories[id]; ok {
		return category.Name(), category.Type() == tasks.CategoryTypeScheduled
	}
	name = fmt.Sprintf("category-%d", id)
	if len(pairs) == 0 {
		// No entries to judge by. Immediate is the safer guess: it decodes an
		// int64 key, and a scheduled key read as one simply fails to render
		// rather than producing a wrong fire time.
		return name, false
	}
	return name, len(asBytes(pairs[0].Key)) == scheduledTaskKeyLen
}

// scheduledTaskKeyLen is the width of a scheduled category's map key:
// bigendian(fireTimeNanos) || bigendian(taskID).
const scheduledTaskKeyLen = 16

// renderTaskEntries decodes up to historyTaskEntryCap map entries, in the order
// the server returned them.
//
// The slice is walked front to back and never re-sorted. That is the point of
// the view: these arrived in key order from a K-ordered map, and any sort here
// -- even one that happened to agree -- would replace the server's guarantee
// with our own assertion about it.
func renderTaskEntries(pairs []as.MapPair, scheduled bool) []TaskEntry {
	limit := min(len(pairs), historyTaskEntryCap)
	out := make([]TaskEntry, 0, limit)
	for _, pair := range pairs[:limit] {
		if entry, ok := renderTaskEntry(pair, scheduled); ok {
			out = append(out, entry)
		}
	}
	return out
}

func renderTaskEntry(pair as.MapPair, scheduled bool) (TaskEntry, bool) {
	size := taskBlobSize(pair.Value)

	if !scheduled {
		taskID, ok := asInt64(pair.Key)
		if !ok {
			return TaskEntry{}, false
		}
		return TaskEntry{
			Label:  fmt.Sprintf("task %d", taskID),
			TaskID: taskID,
			Bytes:  size,
		}, true
	}

	// asBytes, not a []byte assertion: the same 16-byte key is a slice via
	// MapReturnType.KEY but a [16]uint8 *array* inside a MapPair, and the
	// assertion fails silently on the array form. docs/04-open-questions.md R10.
	raw := asBytes(pair.Key)
	if len(raw) != scheduledTaskKeyLen {
		return TaskEntry{}, false
	}
	fireTime := time.Unix(0, int64(binary.BigEndian.Uint64(raw[0:8]))).UTC()
	taskID := int64(binary.BigEndian.Uint64(raw[8:16]))
	stamp := fireTime.Format(time.RFC3339)
	return TaskEntry{
		Label:    fmt.Sprintf("%s · task %d", stamp, taskID),
		TaskID:   taskID,
		FireTime: &stamp,
		Bytes:    size,
	}, true
}

// taskBlobSize measures the stored task blob. The store writes a DataBlob as
// the two-element list {data, encoding} -- see blobValue in
// store/aerospike/mutable_state.go -- so the payload is the first element.
func taskBlobSize(v any) int {
	if parts, ok := v.([]any); ok {
		if len(parts) == 0 {
			return 0
		}
		return len(asBytes(parts[0]))
	}
	return len(asBytes(v))
}

// recordMapPairs normalises the two shapes an Aerospike map bin takes on read,
// mirroring the helper of the same name in store/aerospike/mutable_state.go.
//
// A K-ordered map deserializes to []as.MapPair, preserving the server's key
// order; an unordered one deserializes to map[any]any. Asserting only the
// latter yields an empty collection rather than an error, which is exactly how
// this fails silently: the view would render every bucket as empty and look
// like a bug in the store.
func recordMapPairs(rec *as.Record, bin string) []as.MapPair {
	switch m := rec.Bins[bin].(type) {
	case []as.MapPair:
		return m
	case map[any]any:
		out := make([]as.MapPair, 0, len(m))
		for k, v := range m {
			out = append(out, as.MapPair{Key: k, Value: v})
		}
		return out
	default:
		return nil
	}
}

// asInt64 normalises the integer shapes the client hands back. Aerospike has
// one integer type, but which Go type it lands in depends on the read path.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case uint64:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

// truncateSettle is how long Truncate waits before returning.
//
// Truncate is asynchronous with respect to reads already in flight: the server
// acknowledges it immediately and records disappear shortly afterwards. A
// caller that reset the demo and then immediately re-read would otherwise see
// records it just cleared and conclude the reset failed. The store's own test
// harness pauses for the same reason and the same duration
// (store/aerospike/testcluster.go). It is a settling delay, not a guarantee --
// nothing in the protocol offers one.
const truncateSettle = 100 * time.Millisecond

// Truncate empties every set the store uses, so the demo can be reset.
//
// Server-side truncation, not a scan and delete: it is a single info command
// per set that the server applies by advancing the set's truncate epoch, so it
// costs the same whether the set holds one record or a million. A scan-and-
// delete would also have to durable-delete every record to be correct under
// strong consistency, which is orders of magnitude more work for the same
// result.
//
// This is the one thing in this file that writes, and it destroys data: it is
// the reset button, not part of the browsing path.
//
// The set list mirrors store/aerospike/keys.go allSets, via setDescriptions.
// The store's list is unexported, so the two are kept in step by hand; a set
// missing from setDescriptions would also be missing a description in the set
// panel, which is the visible symptom.
func (b *Browser) Truncate(ctx context.Context) error {
	client, err := b.connect()
	if err != nil {
		return err
	}

	policy := *b.info
	applyDeadline(ctx, &policy.Timeout)

	var failures []string
	for _, d := range setDescriptions {
		set := b.cfg.AerospikeSetPrefix + d.name
		if err := client.Truncate(&policy, b.cfg.AerospikeNamespace, set, nil); err != nil {
			if isMissingSet(err) {
				// Aerospike has no DDL, so a set the store has not written to
				// yet does not exist. Nothing to empty is success.
				continue
			}
			failures = append(failures, fmt.Sprintf("%s: %v", set, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("truncating sets in namespace %s: %s",
			b.cfg.AerospikeNamespace, strings.Join(failures, "; "))
	}

	// Give the truncation a moment to take effect before telling the caller it
	// is done; see truncateSettle.
	timer := time.NewTimer(truncateSettle)
	defer timer.Stop()
	if ctx == nil {
		<-timer.C
		return nil
	}
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		// The truncation was accepted; only the wait was cut short.
		return nil
	}
}

// isMissingSet reports whether an error means "that set does not exist".
func isMissingSet(err error) bool {
	var asErr as.Error
	return errors.As(err, &asErr) &&
		asErr.Matches(types.KEY_NOT_FOUND_ERROR, types.INVALID_NAMESPACE)
}

// requestInfo sends an info command to one node.
//
// One node is the right scope: every statistic here is per-node, and the demo
// cluster is a single node because a strong-consistency namespace needs
// replication-factor == cluster size (deploy/aerospike.conf).
func (b *Browser) requestInfo(ctx context.Context, command string) (map[string]string, error) {
	client, err := b.connect()
	if err != nil {
		return nil, err
	}
	nodes := client.GetNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no Aerospike nodes are reachable at %s", b.cfg.AerospikeHost)
	}

	policy := *b.info
	applyDeadline(ctx, &policy.Timeout)

	res, err := nodes[0].RequestInfo(&policy, command)
	if err != nil {
		return nil, fmt.Errorf("info command %q on node %s: %w", command, nodes[0].GetName(), err)
	}
	return res, nil
}

// applyDeadline folds a context deadline into a policy timeout.
//
// The Aerospike client has no context.Context anywhere in its command API --
// deadlines are policy timeouts and nothing else. store/aerospike/context.go
// makes the same translation for the persistence store, with the same caveat:
// a context cancelled mid-command cannot interrupt it, because the client has
// nothing to observe. The command runs to its timeout.
func applyDeadline(ctx context.Context, timeout *time.Duration) {
	if ctx == nil {
		return
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return
	}
	if remaining := time.Until(deadline); remaining > 0 && remaining < *timeout {
		*timeout = remaining
	}
}

func renderRecord(rec *as.Record) RecordView {
	view := RecordView{Bins: make([]BinView, 0, len(rec.Bins))}
	if rec.Key != nil {
		view.Digest = hex.EncodeToString(rec.Key.Digest())
		view.Key, view.hasKey = displayKeyOf(rec.Key)
	}

	names := make([]string, 0, len(rec.Bins))
	for name := range rec.Bins {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		typ, preview, size := describeValue(rec.Bins[name])
		view.Bins = append(view.Bins, BinView{Name: name, Type: typ, Preview: preview, Size: size})
	}
	return view
}

// displayKey renders the record's primary key.
//
// Aerospike stores only the 20-byte digest unless a write asks for the key to
// be kept, so whether there is anything to show here depends on the store's
// sendKey option. When there is not, a truncated digest is at least a stable
// handle a viewer can match across the list.
func displayKey(key *as.Key) string {
	s, _ := displayKeyOf(key)
	return s
}

// displayKeyOf is displayKey plus the fact the caller needs to sort: whether
// what came back is the real user key or the digest fallback.
func displayKeyOf(key *as.Key) (string, bool) {
	if v := key.Value(); v != nil {
		if s := fmt.Sprintf("%v", v.GetObject()); s != "" && s != "<nil>" {
			return s, true
		}
	}
	return "#" + hex.EncodeToString(key.Digest())[:8], false
}

// userKeyString returns a record's stored user key, or "" when the write did
// not set sendKey. Unlike displayKeyOf it never invents a substitute, because
// the history-task view has to parse this and a digest is not parseable.
func userKeyString(key *as.Key) string {
	if key == nil {
		return ""
	}
	if s, ok := displayKeyOf(key); ok {
		return s
	}
	return ""
}

// sortRecords puts a scan's results into an order a viewer can follow.
//
// ScanAll returns records in digest order, which is a hash of the key: stable
// only in the sense that it does not change while the data does not, and
// effectively random with respect to anything a person can see. Worse, the
// server divides a scan across partitions, so two calls that return different
// subsets reshuffle the list -- the panel appears to churn while nothing is
// happening.
//
// Key order is what a viewer expects, and the demo sets sendKey so the key is
// there to sort on. Records written before that option was turned on have only
// a digest; they sort after every keyed record rather than being mixed in on a
// value that means nothing, and among themselves by digest, which is at least
// deterministic. Digest is the final tiebreak throughout, so the order is a
// total one and repeated calls agree.
func sortRecords(records []RecordView) {
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.hasKey != b.hasKey {
			return a.hasKey // keyed records first
		}
		if a.hasKey {
			if c := compareNatural(a.Key, b.Key); c != 0 {
				return c < 0
			}
		}
		return a.Digest < b.Digest
	})
}

// compareNatural compares two keys with digit runs compared as numbers.
//
// Every composite key in this store is colon-joined numbers and ids --
// "3:2:0", "10:2:1" -- and a plain byte comparison files shard 10 between
// shard 1 and shard 2. Comparing digit runs numerically puts the buckets of a
// shard in the order the store walks them, which is the whole point of showing
// them.
func compareNatural(a, b string) int {
	for len(a) > 0 && len(b) > 0 {
		if isDigit(a[0]) && isDigit(b[0]) {
			an, aRest := leadingDigits(a)
			bn, bRest := leadingDigits(b)
			if c := compareDigitRun(an, bn); c != 0 {
				return c
			}
			a, b = aRest, bRest
			continue
		}
		if a[0] != b[0] {
			if a[0] < b[0] {
				return -1
			}
			return 1
		}
		a, b = a[1:], b[1:]
	}
	switch {
	case len(a) == len(b):
		return 0
	case len(a) == 0:
		return -1
	default:
		return 1
	}
}

// compareDigitRun orders two runs of digits by value without parsing them.
// Task ids reach the top of int64 and a bucket index is a task id shifted, so
// strconv.ParseInt on an arbitrary run risks an overflow error on a path that
// has no way to report one; length-then-lexicographic is exact for any length.
func compareDigitRun(a, b string) int {
	a, b = strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0")
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return strings.Compare(a, b)
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func leadingDigits(s string) (run, rest string) {
	i := 0
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	return s[:i], s[i:]
}

// describeValue renders one bin value as (type, preview, size).
//
// Rendering, not decoding. Most of the bins that matter hold protos serialized
// by Temporal above the persistence seam, and this service has no business
// interpreting them -- a proto forced through a string conversion produces
// mojibake that looks like corruption. Type, size and a hex prefix are honest.
func describeValue(v any) (typ string, preview string, size int) {
	if v == nil {
		return "nil", "", 0
	}
	if b := asBytes(v); b != nil {
		return "blob", previewBlob(b), len(b)
	}

	switch x := v.(type) {
	case string:
		return "string", truncate(x, 96), len(x)
	case int:
		return "int", strconv.Itoa(x), 8
	case int64:
		return "int", strconv.FormatInt(x, 10), 8
	case float64:
		return "float", strconv.FormatFloat(x, 'g', -1, 64), 8
	case bool:
		return "bool", strconv.FormatBool(x), 1
	case []as.MapPair:
		// A K-ordered map -- the store's collection bins (activities, timers,
		// history tasks, matching tasks). The server returns these already in
		// key order and that order is the data model, not a presentation
		// choice: it is what replaces Cassandra's clustering key. So the slice
		// is handed to the preview untouched. Copying it into a Go map here,
		// or sorting it by anything of our own devising, would silently throw
		// away the one guarantee the bucketed model is built on.
		return "map", previewPairs(x), len(x)
	case map[any]any:
		// An unordered map reads back in this shape instead. Handling only one
		// of the two is the trap documented in store/aerospike/mutable_state.go:
		// the wrong assertion yields an empty collection, not an error.
		//
		// There is no server-side order to preserve here, and Go randomises map
		// iteration, so the preview is sorted by rendered key. That is a
		// display decision -- it exists only so the same record does not render
		// differently on two consecutive polls.
		pairs := make([]as.MapPair, 0, len(x))
		for k, val := range x {
			pairs = append(pairs, as.MapPair{Key: k, Value: val})
		}
		sort.Slice(pairs, func(i, j int) bool {
			return compareNatural(formatScalar(pairs[i].Key), formatScalar(pairs[j].Key)) < 0
		})
		return "map", previewPairs(pairs), len(pairs)
	case []any:
		return "list", previewList(x), len(x)
	default:
		return fmt.Sprintf("%T", v), truncate(fmt.Sprintf("%v", v), 96), 0
	}
}

// previewBlob renders bytes as a short hex prefix, unless the bytes happen to
// be printable text -- some bins hold encoding names rather than payloads.
func previewBlob(b []byte) string {
	if len(b) == 0 {
		return "0 bytes"
	}
	if printable(b) {
		return truncate(string(b), 96)
	}
	n := min(len(b), 16)
	preview := hex.EncodeToString(b[:n])
	if n < len(b) {
		preview += "…"
	}
	return fmt.Sprintf("%d bytes · %s", len(b), preview)
}

func previewPairs(pairs []as.MapPair) string {
	if len(pairs) == 0 {
		return "empty"
	}
	parts := make([]string, 0, 3)
	for _, p := range pairs[:min(len(pairs), 3)] {
		_, valPreview, _ := describeValue(p.Value)
		parts = append(parts, fmt.Sprintf("%s → %s",
			truncate(formatScalar(p.Key), 32), truncate(valPreview, 40)))
	}
	s := fmt.Sprintf("%d entries: %s", len(pairs), strings.Join(parts, ", "))
	if len(pairs) > 3 {
		s += ", …"
	}
	return s
}

func previewList(items []any) string {
	if len(items) == 0 {
		return "empty"
	}
	parts := make([]string, 0, 3)
	for _, item := range items[:min(len(items), 3)] {
		typ, valPreview, _ := describeValue(item)
		if valPreview == "" {
			valPreview = typ
		}
		parts = append(parts, truncate(valPreview, 40))
	}
	s := fmt.Sprintf("%d items: %s", len(items), strings.Join(parts, ", "))
	if len(items) > 3 {
		s += ", …"
	}
	return s
}

// formatScalar renders a map key. Map keys in this store are int64 task ids,
// strings, or 16-byte BLOBs -- and a BLOB key is exactly where the shape trap
// bites, so it goes through asBytes like everything else.
func formatScalar(v any) string {
	if b := asBytes(v); b != nil {
		return hex.EncodeToString(b)
	}
	switch x := v.(type) {
	case string:
		return x
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// asBytes normalises the two shapes the Aerospike client uses for a BLOB.
//
// This mirrors the helper of the same name in store/aerospike/client.go, and it
// is not defensive programming. The same 16-byte value arrives as a []byte
// slice from MapReturnType.KEY but as a fixed-size [16]uint8 *array* inside a
// MapPair. A plain `v.([]byte)` assertion silently fails on the array form,
// producing an empty result rather than an error.
func asBytes(v any) []byte {
	switch b := v.(type) {
	case nil:
		return nil
	case []byte:
		return b
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Array || rv.Type().Elem().Kind() != reflect.Uint8 {
		return nil
	}
	out := make([]byte, rv.Len())
	reflect.Copy(reflect.ValueOf(out), rv)
	return out
}

// parseInfoPairs splits an info response into key=value pairs. The separator
// varies by command, which is why it is a parameter: `namespace/<ns>` uses ';'
// and `sets/<ns>` uses ':' within each set entry.
func parseInfoPairs(raw, sep string) map[string]string {
	out := make(map[string]string)
	for _, field := range strings.Split(raw, sep) {
		key, value, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok || key == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func firstOf(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return ""
}

func printable(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return string(runes[:n]) + "…"
}

func atoiOr(s string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return n
}

func splitHostPort(hostPort string) (string, int, error) {
	host, portStr, ok := strings.Cut(hostPort, ":")
	if !ok {
		// Bare hostname: the standard client port.
		return hostPort, 3000, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid Aerospike address %q: %w", hostPort, err)
	}
	return host, port, nil
}
