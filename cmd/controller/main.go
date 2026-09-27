package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/paddo-tech/ushr/internal/api"
	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/dispatch/pg"
	"github.com/paddo-tech/ushr/internal/enroll"
	"github.com/paddo-tech/ushr/internal/ledger"
	"github.com/paddo-tech/ushr/internal/telemetry"
	"github.com/paddo-tech/ushr/internal/version"
	"github.com/paddo-tech/ushr/internal/webhook"
)

func main() {
	configPath := flag.String("config", "controller.yaml", "path to controller config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		slog.Info("ushr-controller", "version", version.Version)
		return
	}

	if err := run(*configPath); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("controller exited cleanly")
			return
		}
		slog.Error("controller failed", "err", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.LoadController(configPath)
	if err != nil {
		return err
	}
	slog.Info("controller starting",
		"version", version.Version,
		"listen", cfg.Listen,
		"policy", cfg.Policy.Type,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, enr, rec, closeStores, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeStores()
	if n := len(store.Snapshot()); n > 0 {
		slog.Info("dispatch store recovered claimed dispatches", "count", n)
	}

	srv := api.NewServer(cfg.Policy.Aging.BoostPerMinute, cfg.Token, store, enr)
	srv.WithTelemetry(rec)
	if err := srv.RequireScopedAuth(); err != nil {
		return err
	}
	if err := srv.RequireAuthOrLoopback(cfg.Listen); err != nil {
		return err
	}

	handler, err := withWebhook(srv.Routes(), cfg.Webhook, rec)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	slog.Info("listening", "addr", cfg.Listen)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// openStores selects the dispatch store and, when Neon-backed, the enrollment
// store and telemetry recorder — all sharing one pgxpool. Returns a close func
// for the pool/WAL. Postgres (Neon) is used when dispatch_dsn is set (hosted
// control plane, with enrollment); otherwise the local JSONL ledger with no
// enrollment and no telemetry.
func openStores(ctx context.Context, cfg *config.Controller) (dispatch.Store, enroll.Store, telemetry.Recorder, func(), error) {
	if cfg.DispatchDSN != "" {
		pool, err := pgxpool.New(ctx, cfg.DispatchDSN)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("connect postgres: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, nil, nil, nil, fmt.Errorf("ping postgres: %w", err)
		}
		ds, err := pg.NewWithPool(ctx, pool)
		if err != nil {
			pool.Close()
			return nil, nil, nil, nil, fmt.Errorf("open dispatch store: %w", err)
		}
		en, err := enroll.NewWithPool(ctx, pool)
		if err != nil {
			pool.Close()
			return nil, nil, nil, nil, fmt.Errorf("open enroll store: %w", err)
		}
		rec, err := telemetry.NewWithPool(ctx, pool)
		if err != nil {
			pool.Close()
			return nil, nil, nil, nil, fmt.Errorf("open telemetry: %w", err)
		}
		slog.Info("dispatch store: postgres; enrollment and telemetry enabled")
		return ds, en, rec, pool.Close, nil
	}
	path := cfg.DispatchLedgerPath
	if path == "" {
		path = dispatch.DefaultPath()
	}
	led, err := dispatch.Open(path)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open dispatch ledger: %w", err)
	}
	slog.Info("dispatch store: jsonl", "path", path)
	return led, nil, telemetry.Noop{}, func() { _ = led.Close() }, nil
}

// withWebhook mounts the workflow_job receiver at /webhook ahead of the agent
// API when a secret is configured. The webhook authenticates by HMAC signature,
// so it sits outside the bearer-token middleware. Reaching it from GitHub is a
// deployment concern (front the loopback controller with a tunnel/reverse proxy).
func withWebhook(api http.Handler, cfg config.WebhookConfig, rec telemetry.Recorder) (http.Handler, error) {
	if cfg.Secret == "" {
		return api, nil
	}
	path := ledger.PathOrDefault(cfg.LedgerPath)
	led, err := ledger.Open(path)
	if err != nil {
		return nil, err
	}
	slog.Info("webhook enabled", "path", "/webhook", "ledger", path)
	sink := func(r ledger.Record) {
		if !r.CompletedAt.IsZero() {
			if err := led.Append(r); err != nil {
				slog.Warn("ledger append failed", "job", r.JobID, "err", err)
			}
		}
		// Hosted: merge the GitHub-truth columns into job_runs (Noop on OSS).
		rec.JobUpdated(r)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /webhook", webhook.Handler([]byte(cfg.Secret), sink))
	mux.Handle("/", api)
	return mux, nil
}
