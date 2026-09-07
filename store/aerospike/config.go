package aerospike

import (
	"fmt"
	"time"

	"go.temporal.io/server/common/config"
)

// StoreName is the value expected in `datastores.<name>.customDatastore.name`.
// Temporal dispatches on the presence of the customDatastore key rather than on
// this string (common/persistence/client/fx.go), so it is advisory -- but we
// reject a mismatch to catch a misconfigured deployment early.
const StoreName = "aerospike"

// Config is the parsed form of customDatastore.options.
type Config struct {
	// Hosts is a list of "host:port" seeds. The client tends the cluster from
	// these and then talks to every node directly; there is no proxy in the
	// data path.
	Hosts []string

	// Namespace is the Aerospike namespace. It must be configured for strong
	// consistency -- multi-record transactions require it, and history-task
	// reads must be quorum-consistent.
	Namespace string

	// SetPrefix namespaces our set names. Empty in production; the conformance
	// harness sets it so a test run cannot collide with real data.
	SetPrefix string

	// UseServicesAlternate makes the client use each node's
	// `alternate-access-address` instead of its normal service address. Needed
	// when reaching a containerised node from outside its network -- see the
	// comment in deploy/aerospike.conf.
	UseServicesAlternate bool

	// SendKey stores the user key alongside the digest on every record this
	// store writes. Off by default, and deliberately so: Aerospike keeps only
	// the 20-byte digest unless asked otherwise, so enabling this adds the
	// full key -- e.g. "4:namespace-id:workflow-id:run-id" -- to every record,
	// on every write, forever.
	//
	// Nothing in the store reads the key back. That is the rule established by
	// R9 in docs/04-open-questions.md ("never recover identity from a record's
	// key"; carry anything you need in a bin), and it still holds with this
	// option on. Turning it on must never become a correctness dependency --
	// the store has to behave identically either way.
	//
	// It exists purely as a demo affordance: the record browser scans sets and
	// can only render digests otherwise, which tells a viewer nothing about
	// the data model. With the key stored, a record identifies itself.
	SendKey bool

	User     string
	Password string

	MinConnsPerNode int
	MaxConnsPerNode int

	// ConnectTimeout bounds initial cluster tending.
	ConnectTimeout time.Duration
	// SocketTimeout and TotalTimeout are the per-command defaults.
	SocketTimeout time.Duration
	TotalTimeout  time.Duration
	// TxnTimeout bounds a multi-record transaction. The server caps this at
	// 120s and defaults to 10s; the clock starts on the first write.
	TxnTimeout time.Duration
}

// NewConfig parses and validates a customDatastore options block.
func NewConfig(cfg config.CustomDatastoreConfig) (*Config, error) {
	if cfg.Name != "" && cfg.Name != StoreName {
		return nil, fmt.Errorf("customDatastore.name is %q, expected %q", cfg.Name, StoreName)
	}

	o := options(cfg.Options)
	c := &Config{
		// Defaults chosen for a single-node PoC. The connection pool numbers
		// are deliberately modest: (client instances x maxConnsPerNode) must
		// stay under the server's proto-fd-max.
		MinConnsPerNode: 0,
		MaxConnsPerNode: 64,
		ConnectTimeout:  10 * time.Second,
		SocketTimeout:   10 * time.Second,
		TotalTimeout:    15 * time.Second,
		TxnTimeout:      30 * time.Second,
	}

	var err error
	if c.Hosts, err = o.stringSlice("hosts"); err != nil {
		return nil, err
	}
	if len(c.Hosts) == 0 {
		return nil, fmt.Errorf("customDatastore.options.hosts must list at least one seed")
	}
	if c.Namespace, err = o.str("namespace", ""); err != nil {
		return nil, err
	}
	if c.Namespace == "" {
		return nil, fmt.Errorf("customDatastore.options.namespace is required")
	}
	if c.SetPrefix, err = o.str("setPrefix", ""); err != nil {
		return nil, err
	}
	if c.UseServicesAlternate, err = o.boolean("useServicesAlternate", false); err != nil {
		return nil, err
	}
	if c.SendKey, err = o.boolean("sendKey", false); err != nil {
		return nil, err
	}
	if c.User, err = o.str("user", ""); err != nil {
		return nil, err
	}
	if c.Password, err = o.str("password", ""); err != nil {
		return nil, err
	}
	if c.MinConnsPerNode, err = o.integer("minConnsPerNode", c.MinConnsPerNode); err != nil {
		return nil, err
	}
	if c.MaxConnsPerNode, err = o.integer("maxConnsPerNode", c.MaxConnsPerNode); err != nil {
		return nil, err
	}
	if c.ConnectTimeout, err = o.duration("connectTimeout", c.ConnectTimeout); err != nil {
		return nil, err
	}
	if c.SocketTimeout, err = o.duration("socketTimeout", c.SocketTimeout); err != nil {
		return nil, err
	}
	if c.TotalTimeout, err = o.duration("totalTimeout", c.TotalTimeout); err != nil {
		return nil, err
	}
	if c.TxnTimeout, err = o.duration("txnTimeout", c.TxnTimeout); err != nil {
		return nil, err
	}
	if c.TxnTimeout > 120*time.Second {
		return nil, fmt.Errorf("customDatastore.options.txnTimeout is %s; the server caps transactions at 120s", c.TxnTimeout)
	}

	return c, nil
}

// options is a small typed reader over the untyped YAML map. Hand-rolled
// rather than round-tripped through a marshaller so that a misconfiguration
// names the offending key.
type options map[string]any

func (o options) str(key, def string) (string, error) {
	v, ok := o[key]
	if !ok || v == nil {
		return def, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("customDatastore.options.%s must be a string, got %T", key, v)
	}
	return s, nil
}

func (o options) boolean(key string, def bool) (bool, error) {
	v, ok := o[key]
	if !ok || v == nil {
		return def, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("customDatastore.options.%s must be a bool, got %T", key, v)
	}
	return b, nil
}

func (o options) integer(key string, def int) (int, error) {
	v, ok := o[key]
	if !ok || v == nil {
		return def, nil
	}
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64: // JSON-shaped config
		return int(n), nil
	default:
		return 0, fmt.Errorf("customDatastore.options.%s must be an int, got %T", key, v)
	}
}

func (o options) duration(key string, def time.Duration) (time.Duration, error) {
	s, err := o.str(key, "")
	if err != nil {
		return 0, err
	}
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("customDatastore.options.%s: %w", key, err)
	}
	return d, nil
}

func (o options) stringSlice(key string) ([]string, error) {
	v, ok := o[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch xs := v.(type) {
	case []string:
		return xs, nil
	case []any:
		out := make([]string, 0, len(xs))
		for i, e := range xs {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("customDatastore.options.%s[%d] must be a string, got %T", key, i, e)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("customDatastore.options.%s must be a list of strings, got %T", key, v)
	}
}
