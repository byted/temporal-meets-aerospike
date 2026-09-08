package control

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// The UI is owned by control/web and shipped inside the binary, because the
// deployment is one container with one ServiceAccount and adding a volume mount
// for three static files is not worth the manifest.
//
// `all:` so that dotfiles are included; the default embed pattern skips them.
//
//go:embed all:web
var webAssets embed.FS

// switchTimeout bounds a whole switch -- worker down, patch, rollout, namespace,
// worker up -- and, with it, a reset, which is that sequence plus a truncate.
// It exists so a wedged switch eventually releases the lock and the operator
// can try again, rather than needing a restart mid-demo.
const switchTimeout = 10 * time.Minute

// Server wires the three subsystems to the HTTP contract the UI is built
// against, and owns the one piece of sequencing that is not obvious: the order
// of operations in a backend switch. See runSwitch.
type Server struct {
	cfg     Config
	backend *BackendController
	runner  *WorkflowRunner
	browser *Browser
	events  *sseHub
	logger  *slog.Logger

	// backendErr records a failure to reach Kubernetes at startup. Not fatal:
	// the record browser and the workflow button still work without it, and a
	// control plane that refuses to start because of one missing RBAC rule is
	// no help at all when you are trying to diagnose that rule.
	backendErr error

	// closed stops the background readiness retry when the server shuts down.
	closed    chan struct{}
	closeOnce sync.Once

	mu        sync.Mutex
	switching bool
	// lastBackend is what /api/state reports when Kubernetes cannot be read.
	lastBackend Backend
}

func NewServer(cfg Config, logger *slog.Logger) *Server {
	s := &Server{
		cfg:         cfg,
		runner:      NewWorkflowRunner(cfg, logger),
		browser:     NewBrowser(cfg),
		events:      newSSEHub(),
		logger:      logger,
		lastBackend: BackendSQLite,
		closed:      make(chan struct{}),
	}

	backend, err := NewBackendController(cfg)
	if err != nil {
		s.backendErr = err
		logger.Error("Kubernetes access unavailable; the backend switch will not work", "error", err)
	} else {
		s.backend = backend
	}
	return s
}

// Start brings the demo worker up so the run button works before anyone clicks
// switch.
//
// This retries in the background rather than attempting once, because attempting
// once reliably loses. `kubectl apply -f` starts the control plane and Temporal
// at the same moment, so the first EnsureNamespace hits a frontend that is not
// listening yet. A single attempt then leaves the demo permanently reporting
// temporalReady=false, and the run button returning "worker is not running; the
// backend switch may still be in progress" -- an error that blames the switch
// for a startup race. Observed on 2 of 2 cold installs, so it is the normal
// path, not an edge case.
//
// Returns immediately; the caller's context bounds only the first attempt.
func (s *Server) Start(ctx context.Context) {
	go s.ensureReady()
}

// ensureReady keeps trying to register the namespace and start the worker until
// it succeeds or the server closes. Backs off to avoid hammering a frontend
// that is still booting, and reports the outcome so the UI shows why the run
// button is not ready yet.
func (s *Server) ensureReady() {
	const (
		firstDelay = 500 * time.Millisecond
		maxDelay   = 5 * time.Second
	)

	delay := firstDelay
	for attempt := 1; ; attempt++ {
		select {
		case <-s.closed:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := s.runner.EnsureNamespace(ctx)
		if err == nil {
			err = s.runner.StartWorker(ctx)
		}
		cancel()

		if err == nil {
			if attempt > 1 {
				s.logger.Info("demo worker ready", "attempts", attempt)
				s.events.publish(Event{
					Type:    EventProgress,
					Message: "Demo worker is ready.",
					Ts:      time.Now(),
				})
			}
			return
		}

		// Only the first failure is worth a line in the log; after that it is
		// just noise while Temporal boots.
		if attempt == 1 {
			s.logger.Warn("demo worker not ready yet, retrying in the background", "error", err)
		}

		select {
		case <-s.closed:
			return
		case <-time.After(delay):
		}
		if delay < maxDelay {
			delay *= 2
		}
	}
}

func (s *Server) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
	s.runner.Close()
	s.browser.Close()
	s.events.close()
}

// Handler returns the full routing tree, wrapped in basic auth.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("POST /api/switch", s.handleSwitch)
	mux.HandleFunc("POST /api/reset", s.handleReset)
	mux.HandleFunc("POST /api/workflow/run", s.handleRunWorkflow)
	mux.HandleFunc("POST /api/workflow/run-batch", s.handleRunBatch)
	mux.HandleFunc("GET /api/aerospike/health", s.handleAerospikeHealth)
	mux.HandleFunc("GET /api/aerospike/sets", s.handleAerospikeSets)
	mux.HandleFunc("GET /api/aerospike/records", s.handleAerospikeRecords)
	mux.HandleFunc("GET /api/aerospike/history-tasks", s.handleAerospikeHistoryTasks)
	mux.HandleFunc("GET /api/events", s.handleEvents)

	web, err := fs.Sub(webAssets, "web")
	if err != nil {
		// Impossible unless the embed directive and the directory disagree,
		// which is a build-time fact, not a runtime one.
		panic(fmt.Sprintf("embedded web assets are malformed: %v", err))
	}
	mux.Handle("/", http.FileServerFS(web))

	return s.withBasicAuth(mux)
}

// State is the payload of GET /api/state and of every "state" SSE event, so the
// UI can render from one shape regardless of how it arrived.
type State struct {
	Backend       string `json:"backend"`
	TemporalReady bool   `json:"temporalReady"`
	Switching     bool   `json:"switching"`
	Namespace     string `json:"namespace"`
	// Error is non-empty when the backend could not be read. The UI can ignore
	// it; it exists so a misconfigured cluster says so instead of silently
	// reporting a stale backend.
	Error string `json:"error,omitempty"`
}

func (s *Server) state(ctx context.Context) State {
	s.mu.Lock()
	switching := s.switching
	backend := s.lastBackend
	s.mu.Unlock()

	st := State{
		Backend:   string(backend),
		Switching: switching,
		Namespace: s.cfg.DemoNamespace,
	}

	switch {
	case s.backend == nil:
		st.Error = s.backendErr.Error()
	default:
		current, err := s.backend.Current(ctx)
		if err != nil {
			st.Error = err.Error()
		} else {
			st.Backend = string(current)
			s.mu.Lock()
			s.lastBackend = current
			s.mu.Unlock()
		}
	}

	// Skip the readiness probe mid-switch: Temporal is expected to be
	// unreachable then, and a five-second gRPC timeout on every poll would
	// stall the UI exactly when it is meant to be showing progress.
	if !switching {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		st.TemporalReady = s.runner.Ready(probeCtx)
		cancel()
	}
	return st
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.state(r.Context()))
}

func (s *Server) handleSwitch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Backend string `json:"backend"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decoding request body: %w", err))
		return
	}
	target, err := ParseBackend(body.Backend)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !s.begin(w, fmt.Sprintf("backend switch to %s", target),
		func(ctx context.Context) error { return s.runSwitch(ctx, target) }) {
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"backend": string(target)})
}

// handleReset returns the demo to step 1: Temporal back on SQLite and an empty
// Aerospike namespace, so the whole narrative can be run again.
//
// It is the same kind of operation as a switch -- long, disruptive, and
// restarting Temporal -- so it answers the same way: 202 and progress on the
// event stream, sharing the one in-flight flag /api/state exposes as
// `switching`. That flag is what keeps the UI's buttons disabled and lets it
// tolerate Temporal going away mid-operation, and it is why a reset and a
// switch cannot overlap.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if !s.begin(w, "demo reset", s.runReset) {
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"backend": string(BackendSQLite)})
}

// begin claims the in-flight flag and runs op in the background, or writes the
// refusal itself and reports false.
//
// Shared by /api/switch and /api/reset so there is exactly one place that
// decides what "already busy" means. Both need Kubernetes, so both fail the
// same way without it: without the ability to move Temporal off Aerospike, a
// reset could only wipe the store out from under a running server.
func (s *Server) begin(w http.ResponseWriter, name string, op func(context.Context) error) bool {
	if s.backend == nil {
		writeError(w, http.StatusServiceUnavailable, s.backendErr)
		return false
	}

	s.mu.Lock()
	if s.switching {
		s.mu.Unlock()
		writeError(w, http.StatusConflict,
			errors.New("a backend switch or reset is already in progress"))
		return false
	}
	s.switching = true
	s.mu.Unlock()

	// Detached from the request: the handler returns 202 immediately and the
	// work outlives it. Using r.Context() here would cancel the rollout the
	// moment the browser got its response.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), switchTimeout)
		defer cancel()
		defer func() {
			s.mu.Lock()
			s.switching = false
			s.mu.Unlock()
			s.events.publish(newStateEvent(s.state(ctx)))
		}()

		if err := op(ctx); err != nil {
			s.logger.Error(name+" failed", "error", err)
			s.events.publish(Event{Type: EventError, Message: err.Error(), Ts: time.Now()})

			// Bring the worker back.
			//
			// Every one of these sequences stops the worker as its first step,
			// so a failure part-way leaves the demo with no worker at all and
			// the run button answering 503 -- not just for this attempt, but
			// until someone happens to run a *successful* switch. A failed
			// switch should cost you the switch, not the demo.
			//
			// ensureReady already retries with backoff and is a no-op if the
			// worker is running, so this is safe whether the failure happened
			// before or after the restart step.
			go s.ensureReady()
		}
	}()
	return true
}

// runSwitch performs the switch in the only order that leaves a working demo.
//
// The naive version is patch-and-wait, and it fails on the click after: the
// store Temporal has just been pointed at is a different, empty database, so
// the demo namespace registered against the old one does not exist. Step 4 is
// the whole reason this function is not three lines.
//
// Stopping the worker first (step 1) is the other non-obvious part. A worker
// polling a task queue through a rollout produces a stream of connection
// errors, and after the switch it would be polling a namespace that no longer
// exists -- so it comes down before the patch and is rebuilt at the end.
func (s *Server) runSwitch(ctx context.Context, target Backend) error {
	report := func(msg string) {
		s.logger.Info("switch", "target", target, "step", msg)
		s.events.publish(Event{Type: EventProgress, Message: msg, Ts: time.Now()})
	}

	report(fmt.Sprintf("switching persistence backend to %s", target))

	report("stopping the demo worker")
	s.runner.StopWorker()

	if err := s.backend.Switch(ctx, target, report); err != nil {
		return err
	}

	report(fmt.Sprintf("registering namespace %q in the %s store", s.cfg.DemoNamespace, target))
	if err := s.runner.EnsureNamespace(ctx); err != nil {
		return err
	}

	report("restarting the demo worker")
	if err := s.runner.StartWorker(ctx); err != nil {
		return err
	}

	// Absorb the stall. This is a bandage over an unsolved bug, and is kept
	// only so the demo stays usable.
	//
	// The first workflow after a switch to Aerospike intermittently stalls for
	// tens of seconds -- measured at 31.6s, 43.1s, 52.2s, 53.1s, 58.6s and once
	// past 60s, always bounded by matching's 60s long-poll cycle. It reproduces
	// under CPU contention and essentially never on an idle box.
	//
	// What is known, and what is not:
	//   - The gap is WorkflowTaskScheduled -> WorkflowTaskStarted, attempt 1,
	//     with no timeout events. The task is scheduled and simply not handed
	//     to the poller.
	//   - It is Aerospike-only. The same switch to SQLite delivers in ~83ms,
	//     including under the same load.
	//   - It is silent: the worker starts, then nothing is logged by either
	//     side until the task is finally delivered.
	//   - Pinning the task queue to one partition did NOT fix it, though the
	//     setting is correct for a single-worker deployment and is kept.
	//
	// So the mechanism is still unknown, and the evidence now points at the
	// store's own matching task path rather than at Temporal's configuration.
	// See docs/04-open-questions.md.
	//
	// 90s because the stall is bounded by the 60s poll cycle; a shorter timeout
	// just moves the wait in front of the presenter's button instead.
	report("checking the task queue")
	warmCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	if _, err := s.runner.Run(warmCtx); err != nil {
		s.logger.Warn("post-switch smoke test failed; the first run may be slow", "error", err)
		report("smoke test did not complete; the first run may be slow")
	}
	cancel()

	report(fmt.Sprintf("now running on %s", target))
	return nil
}

// runReset puts the demo back to step 1: Temporal on SQLite, Aerospike empty.
//
// The order is switch first, wipe second, and the other order is broken in a
// way that is worth spelling out. While Aerospike is the active backend,
// Temporal is *using* it continuously -- it holds shard leases there, and every
// range-ID check, task-queue poll and history append reads or writes those
// sets. Truncating underneath a live server does not reset it; it leaves a
// running Temporal serving from a store whose contents vanished, which surfaces
// as shard-ownership-lost and missing-namespace errors from a process that
// still believes it owns everything. The subsequent switch would then be a
// rollout out of an already-broken state.
//
// Switching to SQLite first ends that relationship completely: the pod is
// replaced and the new one never opens Aerospike at all, so by the time
// Truncate runs, nothing is reading or writing the sets it empties. The cost is
// that Aerospike keeps the old data for the length of the rollout, which is
// harmless -- it is about to be deleted and nothing points at it any more.
//
// runSwitch is reused rather than reimplemented: stopping the worker, patching,
// waiting for the rollout, re-registering the namespace in the now-empty SQLite
// store and restarting the worker are all required here for exactly the same
// reasons, and a second copy of that sequence is a second thing to get wrong.
func (s *Server) runReset(ctx context.Context) error {
	report := func(msg string) {
		s.logger.Info("reset", "step", msg)
		s.events.publish(Event{Type: EventProgress, Message: msg, Ts: time.Now()})
	}

	report("resetting the demo: moving Temporal back to SQLite, then wiping Aerospike")

	if err := s.runSwitch(ctx, BackendSQLite); err != nil {
		return err
	}

	report("wiping every Aerospike set the store uses")
	if err := s.browser.Truncate(ctx); err != nil {
		return fmt.Errorf("wiping Aerospike: %w", err)
	}

	report("reset complete: Temporal is on sqlite and Aerospike is empty")
	return nil
}

// defaultBatchCount is what the UI's bulk button sends when it sends nothing.
const defaultBatchCount = 100

// maxBatchCount bounds what a caller can ask for. The endpoint is behind basic
// auth on a 4 vCPU box; an unbounded count is a self-inflicted outage.
const maxBatchCount = 1000

func (s *Server) handleRunBatch(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	switching := s.switching
	s.mu.Unlock()
	if switching {
		writeError(w, http.StatusConflict, errors.New("a backend switch or reset is in progress"))
		return
	}

	count := defaultBatchCount
	// An empty body is fine and means the default, so a decode failure is only
	// an error when there was something to decode.
	if r.ContentLength > 0 {
		var body struct {
			Count int `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("decoding request: %w", err))
			return
		}
		if body.Count > 0 {
			count = body.Count
		}
	}
	if count > maxBatchCount {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("batch of %d exceeds the maximum of %d", count, maxBatchCount))
		return
	}

	s.events.publish(Event{
		Type:    EventProgress,
		Message: fmt.Sprintf("running %d workflows, %d at a time", count, batchConcurrency),
		Ts:      time.Now(),
	})

	// Progress is throttled to every 10 completions: a hundred SSE frames in
	// two seconds would bury the switch log the panel shares.
	var lastReported int
	var progressMu sync.Mutex
	result, err := s.runner.RunBatch(r.Context(), count, func(done, failed int) {
		progressMu.Lock()
		defer progressMu.Unlock()
		if done+failed-lastReported < 10 {
			return
		}
		lastReported = done + failed
		s.events.publish(Event{
			Type:    EventProgress,
			Message: fmt.Sprintf("%d/%d complete", done+failed, count),
			Ts:      time.Now(),
		})
	})
	if err != nil {
		s.events.publish(Event{Type: EventError, Message: err.Error(), Ts: time.Now()})
		status := http.StatusInternalServerError
		if errors.Is(err, ErrWorkerNotRunning) {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, err)
		return
	}

	s.events.publish(Event{
		Type: EventProgress,
		Message: fmt.Sprintf("%d/%d workflows completed in %dms (fastest %dms, slowest %dms)",
			result.Completed, result.Requested, result.DurationMs, result.FastestMs, result.SlowestMs),
		Ts: time.Now(),
	})
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleRunWorkflow(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	switching := s.switching
	s.mu.Unlock()
	if switching {
		writeError(w, http.StatusConflict, errors.New("a backend switch or reset is in progress"))
		return
	}

	result, err := s.runner.Run(r.Context())
	if err != nil {
		s.events.publish(Event{Type: EventError, Message: err.Error(), Ts: time.Now()})
		status := http.StatusInternalServerError
		if errors.Is(err, ErrWorkerNotRunning) {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, err)
		return
	}

	s.events.publish(Event{
		Type:    EventProgress,
		Message: fmt.Sprintf("workflow %s completed in %dms: %s", result.WorkflowID, result.DurationMs, result.Result),
		Ts:      time.Now(),
	})
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleAerospikeHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.browser.Health(r.Context()))
}

// handleAerospikeHistoryTasks serves the bucket view: the htask set decomposed
// into shard, category and bucket. This is what the UI renders in place of a
// flat record list, because the bucketing is the part of the data model worth
// explaining.
func (s *Server) handleAerospikeHistoryTasks(w http.ResponseWriter, r *http.Request) {
	view, err := s.browser.HistoryTasks(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleAerospikeSets(w http.ResponseWriter, r *http.Request) {
	sets, err := s.browser.Sets(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, sets)
}

func (s *Server) handleAerospikeRecords(w http.ResponseWriter, r *http.Request) {
	set := r.URL.Query().Get("set")
	if set == "" {
		writeError(w, http.StatusBadRequest, errors.New("query parameter 'set' is required"))
		return
	}
	limit := 25
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid limit %q: %w", v, err))
			return
		}
		limit = n
	}

	records, err := s.browser.Records(r.Context(), set, limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, records)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming is not supported by this connection"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Without this, a buffering reverse proxy holds the whole stream until the
	// switch finishes -- which is precisely the wrong moment to deliver it.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	sub := s.events.subscribe()
	defer s.events.unsubscribe(sub)

	// Open with the current state so a tab that connects mid-demo renders
	// something immediately instead of waiting for the next click.
	s.writeEvent(w, flusher, newStateEvent(s.state(r.Context())))

	// Idle connections through an ingress are reaped; a comment line is the
	// SSE-native keepalive and clients ignore it.
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-sub:
			if !ok {
				return
			}
			if !s.writeEvent(w, flusher, ev) {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) writeEvent(w http.ResponseWriter, flusher http.Flusher, ev Event) bool {
	payload, err := json.Marshal(ev)
	if err != nil {
		s.logger.Error("encoding SSE event", "error", err)
		return true
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// withBasicAuth gates every route, including the static assets -- the demo UI
// can restart Temporal, so there is nothing here worth leaving open. Auth is
// skipped entirely when either variable is unset, which is what makes `go run`
// against a local cluster bearable.
func (s *Server) withBasicAuth(next http.Handler) http.Handler {
	if s.cfg.BasicAuthUser == "" || s.cfg.BasicAuthPassword == "" {
		s.logger.Warn("basic auth is disabled; set BASIC_AUTH_USER and BASIC_AUTH_PASSWORD to enable it")
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		// Both comparisons always run: short-circuiting on the username would
		// leak which half was wrong through response timing.
		userOK := subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.BasicAuthUser)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.BasicAuthPassword)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="temporal-meets-aerospike control plane"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
