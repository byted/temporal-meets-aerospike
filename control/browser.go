package control

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	as "github.com/aerospike/aerospike-client-go/v8"
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
	return out, nil
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
		view.Key = displayKey(rec.Key)
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
	if v := key.Value(); v != nil {
		if s := fmt.Sprintf("%v", v.GetObject()); s != "" && s != "<nil>" {
			return s
		}
	}
	return "#" + hex.EncodeToString(key.Digest())[:8]
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
		// history tasks, matching tasks). Order is meaningful here, so it is
		// preserved in the preview.
		return "map", previewPairs(x), len(x)
	case map[any]any:
		// An unordered map reads back in this shape instead. Handling only one
		// of the two is the trap documented in store/aerospike/mutable_state.go:
		// the wrong assertion yields an empty collection, not an error.
		pairs := make([]as.MapPair, 0, len(x))
		for k, val := range x {
			pairs = append(pairs, as.MapPair{Key: k, Value: val})
		}
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
