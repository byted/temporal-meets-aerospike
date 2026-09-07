// Command control-plane runs the live demo: a small web service that switches
// Temporal's persistence backend between SQLite and Aerospike, runs a workflow
// on whichever is active, and browses the records Aerospike ends up holding.
//
// It runs in k3s as a Deployment with a ServiceAccount scoped to get/patch the
// Temporal Deployment and get/list Pods. The switch itself is a patch of one
// environment variable on that Deployment -- Kubernetes performs the rolling
// restart, so nothing here has to.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stefanselent/temporal-meets-aerospike/control"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := control.ConfigFromEnv()
	logger.Info("starting control plane",
		"port", cfg.Port,
		"temporal", cfg.TemporalAddress,
		"demoNamespace", cfg.DemoNamespace,
		"aerospike", cfg.AerospikeHost,
		"aerospikeNamespace", cfg.AerospikeNamespace,
		"deployment", cfg.KubeNamespace+"/"+cfg.TemporalDeployment,
	)

	server := control.NewServer(cfg, logger)
	defer server.Close()

	// Best-effort: Temporal may still be coming up. The switch path retries
	// both the namespace registration and the worker start.
	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	server.Start(startCtx)
	cancelStart()

	httpServer := &http.Server{
		Addr:    net.JoinHostPort("", cfg.Port),
		Handler: server.Handler(),
		// No write timeout: /api/events is a long-lived SSE stream and any
		// finite value would cut it off mid-demo. Read timeouts still apply,
		// which is what protects against a slow-loris client.
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		logger.Error("http server failed", "error", err)
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}
