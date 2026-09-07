package aerospike

import (
	"context"
	"fmt"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"go.temporal.io/server/common/config"
)

// These tests cover the sendKey option: its parsing, the policies it reaches,
// and -- the only proof that actually matters -- whether the server stores the
// user key alongside the record when it is on.
//
// Context: R9 in docs/04-open-questions.md established that Aerospike keeps
// only the digest unless a write sets sendKey, and made "never recover identity
// from a record's key" a rule of this store. The option does not bend that
// rule; it exists so the demo's record browser can render a record as
// "4:namespace-id:workflow-id:run-id" instead of twenty bytes of hash.

// TestConfigSendKeyParsing pins the parse behaviour, including the default.
// The default is the load-bearing case: every existing deployment omits the
// key and must keep behaving exactly as it did.
func TestConfigSendKeyParsing(t *testing.T) {
	base := func(extra map[string]any) config.CustomDatastoreConfig {
		opts := map[string]any{
			"hosts":     []string{"127.0.0.1:3000"},
			"namespace": "temporal",
		}
		for k, v := range extra {
			opts[k] = v
		}
		return config.CustomDatastoreConfig{Name: StoreName, Options: opts}
	}

	t.Run("absent defaults to false", func(t *testing.T) {
		cfg, err := NewConfig(base(nil))
		if err != nil {
			t.Fatalf("NewConfig: %v", err)
		}
		if cfg.SendKey {
			t.Fatal("SendKey defaulted to true; storing the key must be opt-in")
		}
	})

	t.Run("explicit false", func(t *testing.T) {
		cfg, err := NewConfig(base(map[string]any{"sendKey": false}))
		if err != nil {
			t.Fatalf("NewConfig: %v", err)
		}
		if cfg.SendKey {
			t.Fatal("SendKey is true after sendKey: false")
		}
	})

	t.Run("explicit true", func(t *testing.T) {
		cfg, err := NewConfig(base(map[string]any{"sendKey": true}))
		if err != nil {
			t.Fatalf("NewConfig: %v", err)
		}
		if !cfg.SendKey {
			t.Fatal("SendKey is false after sendKey: true")
		}
	})

	t.Run("wrong type is rejected by key name", func(t *testing.T) {
		_, err := NewConfig(base(map[string]any{"sendKey": "yes"}))
		if err == nil {
			t.Fatal("a non-bool sendKey was accepted")
		}
		if want := "customDatastore.options.sendKey"; !sendKeyContains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	})
}

// TestSendKeyReachesEveryWritePolicy checks the wiring structurally, with no
// server involved. The base policies are copied by casWrite, condWrite,
// condWriteTxn and txnPolicies, so a flag set in the wrong place would store
// the key on some records and not others -- worse than not storing it at all.
func TestSendKeyReachesEveryWritePolicy(t *testing.T) {
	for _, sendKey := range []bool{false, true} {
		t.Run(fmt.Sprintf("sendKey=%v", sendKey), func(t *testing.T) {
			c := &client{cfg: &Config{
				Namespace:     "temporal",
				SendKey:       sendKey,
				SocketTimeout: time.Second,
				TotalTimeout:  time.Second,
			}}
			c.buildPolicies()

			txn := as.NewTxn()
			expr := as.ExpEq(as.ExpIntBin(binRangeID), as.ExpIntVal(1))
			txnRead, txnWrite, txnDelete := c.txnPolicies(txn)

			for name, wp := range map[string]*as.WritePolicy{
				"write":        c.write,
				"create":       c.create,
				"replace":      c.replace,
				"delete":       c.delete,
				"casWrite":     c.casWrite(1),
				"condWrite":    c.condWrite(expr),
				"condWriteTxn": c.condWriteTxn(expr, txn),
				"txnWrite":     txnWrite,
				"txnDelete":    txnDelete,
			} {
				if wp.SendKey != sendKey {
					t.Errorf("policy %q: SendKey=%v, want %v", name, wp.SendKey, sendKey)
				}
			}

			// Reads must never send the key: on a read the flag asks the server
			// to re-derive and verify the digest, which buys nothing here.
			for name, bp := range map[string]*as.BasePolicy{
				"read":          c.read,
				"readLinearize": c.readLinearize,
				"txnRead":       txnRead,
				"batch":         &c.batch.BasePolicy,
			} {
				if bp.SendKey {
					t.Errorf("read policy %q sends the key; it should not", name)
				}
			}
		})
	}
}

// TestSendKeyStoresUserKey is the end-to-end proof. A record read back through
// a *scan* is the only way to see what the server actually stored: on a Get the
// client already holds the key it was given, so rec.Key.Value() is non-nil
// regardless. That is precisely the trap R9 describes.
//
// Requires the compose stack:
//
//	docker compose -f deploy/docker-compose.yml up -d aerospike roster-init
func TestSendKeyStoresUserKey(t *testing.T) {
	if err := Ping(); err != nil {
		t.Skipf("no Aerospike node (%v) -- start it with: docker compose -f deploy/docker-compose.yml up -d aerospike roster-init", err)
	}

	for _, tc := range []struct {
		name      string
		sendKey   bool
		wantStore bool
	}{
		{name: "enabled", sendKey: true, wantStore: true},
		{name: "disabled", sendKey: false, wantStore: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSendKeyTestClient(t, tc.sendKey)
			ctx := context.Background()

			// One record per write path that creates records, so the assertion
			// covers the plain policy, the CREATE_ONLY policy and a write made
			// inside a multi-record transaction.
			plainKey := sendKeyExecKey(t, c, "plain")
			createKey := sendKeyExecKey(t, c, "create")
			txnKey := sendKeyExecKey(t, c, "txn")

			if err := c.as.PutBins(withCtxW(ctx, c.write), plainKey, as.NewBin(binData, []byte("p"))); err != nil {
				t.Fatalf("write via c.write: %v", err)
			}
			if err := c.as.PutBins(withCtxW(ctx, c.create), createKey, as.NewBin(binData, []byte("c"))); err != nil {
				t.Fatalf("write via c.create: %v", err)
			}

			txn := as.NewTxn()
			_, txnWrite, _ := c.txnPolicies(txn)
			if err := c.as.PutBins(withCtxW(ctx, txnWrite), txnKey, as.NewBin(binData, []byte("t"))); err != nil {
				_, _ = c.as.Abort(txn)
				t.Fatalf("write inside transaction: %v", err)
			}
			if _, err := c.as.Commit(txn); err != nil {
				t.Fatalf("commit: %v", err)
			}

			want := map[string]bool{}
			for _, k := range []*as.Key{plainKey, createKey, txnKey} {
				want[k.Value().String()] = false
			}

			scanned := sendKeyScanSet(t, c, c.keys.set(setExecution))
			if len(scanned) != len(want) {
				t.Fatalf("scan returned %d records, want %d", len(scanned), len(want))
			}

			for _, rec := range scanned {
				v := rec.Key.Value()
				if !tc.wantStore {
					if v != nil {
						t.Errorf("sendKey off but the server stored key %q -- default behaviour changed", v.String())
					}
					continue
				}
				if v == nil {
					t.Fatalf("sendKey on but record %x came back with no key", rec.Key.Digest())
				}
				seen, known := want[v.String()]
				if !known {
					t.Errorf("scan returned an unexpected key %q", v.String())
					continue
				}
				if seen {
					t.Errorf("key %q returned twice", v.String())
				}
				want[v.String()] = true
			}

			if tc.wantStore {
				for k, seen := range want {
					if !seen {
						t.Errorf("key %q was never returned by the scan", k)
					}
				}
			}
		})
	}
}

// newSendKeyTestClient builds a store client against the local node with its
// own set prefix, so the scan below sees only this test's records.
func newSendKeyTestClient(t *testing.T, sendKey bool) *client {
	t.Helper()

	cfg := &Config{
		Hosts:     []string{fmt.Sprintf("%s:%s", envOr("AEROSPIKE_HOST", "127.0.0.1"), envOr("AEROSPIKE_PORT", "3000"))},
		Namespace: envOr("AEROSPIKE_NAMESPACE", "temporal"),
		SetPrefix: newTestSetPrefix(),
		// The node advertises its container IP; deploy/aerospike.conf sets
		// alternate-access-address so the host can reach it.
		UseServicesAlternate: true,
		SendKey:              sendKey,
		MaxConnsPerNode:      8,
		ConnectTimeout:       10 * time.Second,
		SocketTimeout:        10 * time.Second,
		TotalTimeout:         15 * time.Second,
		TxnTimeout:           30 * time.Second,
	}

	c, err := newClient(cfg)
	if err != nil {
		t.Fatalf("connecting to Aerospike: %v", err)
	}
	t.Cleanup(func() {
		// Truncate rather than delete: the records are durable and the set name
		// is unique to this test.
		_ = c.as.Truncate(nil, cfg.Namespace, c.keys.set(setExecution), nil)
		c.Close()
	})
	return c
}

// sendKeyExecKey builds a real execution key, so the stored value is the identity the
// demo browser is meant to display: "<shard>:<namespaceID>:<workflowID>:<runID>".
func sendKeyExecKey(t *testing.T, c *client, suffix string) *as.Key {
	t.Helper()
	k, err := c.keys.executionKey(4, "namespace-id-"+suffix, "workflow-id-"+suffix, "run-id-"+suffix)
	if err != nil {
		t.Fatalf("executionKey: %v", err)
	}
	return k
}

// sendKeyScanSet reads a whole set back. Scans are what the demo's record browser
// uses, and what R9 was discovered through.
func sendKeyScanSet(t *testing.T, c *client, set string) []*as.Record {
	t.Helper()

	sp := as.NewScanPolicy()
	sp.TotalTimeout = 15 * time.Second
	sp.ReadModeSC = as.ReadModeSCSession

	rs, err := c.as.ScanAll(sp, c.cfg.Namespace, set)
	if err != nil {
		t.Fatalf("scan %s: %v", set, err)
	}
	defer func() { _ = rs.Close() }()

	var out []*as.Record
	for res := range rs.Results() {
		if res.Err != nil {
			t.Fatalf("scan %s: %v", set, res.Err)
		}
		out = append(out, res.Record)
	}
	return out
}

func sendKeyContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
