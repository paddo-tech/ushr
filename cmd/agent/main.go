package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/paddo-tech/ushr/internal/agent"
	"github.com/paddo-tech/ushr/internal/api"
	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/driver/factory"
	"github.com/paddo-tech/ushr/internal/jit"
	gh "github.com/paddo-tech/ushr/internal/source/github"
	"github.com/paddo-tech/ushr/internal/source/scaleset"
	"github.com/paddo-tech/ushr/internal/update"
	"github.com/paddo-tech/ushr/internal/version"
)

func main() {
	configPath := flag.String("config", filepath.Join(os.Getenv("HOME"), ".config", "ushr", "agent.yaml"), "path to agent config")
	supervised := flag.Bool("supervised", false, "run the supervised worker manager")
	worker := flag.Bool("worker", false, "run the supervised agent worker")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		slog.Info("ushr-agent", "version", version.Version)
		return
	}

	var err error
	if *worker {
		err = run(*configPath)
	} else if *supervised {
		err = update.SuperviseWorker(*configPath)
	} else {
		err = update.Supervise(*configPath)
	}
	if err != nil {
		if errors.Is(err, update.ErrReady) {
			os.Exit(update.ExitReady)
		}
		if errors.Is(err, context.Canceled) {
			slog.Info("agent exited cleanly")
			return
		}
		slog.Error("agent failed", "err", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.LoadAgent(configPath)
	if err != nil {
		return err
	}
	slog.Info("agent starting",
		"version", version.Version,
		"name", cfg.Name,
		"controller", cfg.ControllerURL,
		"driver", cfg.Driver.Type,
		"capacity", cfg.Driver.Capacity,
	)

	drv, err := factory.New(cfg.Driver)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	jobs, minter, prio, err := newSource(ctx, cfg)
	if err != nil {
		return err
	}

	client := api.NewClient(cfg.ControllerURL, cfg.Token)
	a := agent.New(cfg.Name, cfg.Labels, drv, client, minter, prio, jobs)
	a.Version = version.Version
	a.Update = update.Prepare
	a.Healthy = update.Healthy
	a.FailedVersion, a.FailedRequest, a.UpdateError = update.Status()
	a.MinFreeDisk = factory.MinFreeBytes(cfg.Driver)
	a.ReclaimFloor = factory.ReclaimFloorBytes(cfg.Driver)

	// Reap any VMs the previous process left behind (launchd SIGKILL/OOM, crash).
	if err := a.ReconcileOrphans(ctx); err != nil {
		slog.Warn("orphan reconcile failed", "err", err)
	}

	return a.Run(ctx)
}

// newSource builds the job source and the JIT minter from the agent's org
// config. Model B: the agent — not the control plane — holds the App keys,
// watches GitHub (poll or scale-set message sessions), and mints. An unset
// type defaults to poll.
func newSource(ctx context.Context, cfg *config.Agent) (<-chan domain.Job, agent.Minter, map[string]int, error) {
	switch cfg.Source.Type {
	case config.SourceTypePoll, "":
	case config.SourceTypeScaleSet:
		return newScaleSetSource(ctx, cfg)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported source type %q", cfg.Source.Type)
	}
	auths := make([]gh.AppAuth, 0, len(cfg.Orgs)+len(cfg.Repos))
	minter := jit.New()
	prio := make(map[string]int, len(cfg.Orgs)+len(cfg.Repos))
	for _, o := range cfg.Orgs {
		auth := gh.AppAuth{AppID: o.AppID, PrivateKeyPath: o.PrivateKeyPath, Org: o.Name, Repos: o.Repos, BaseURL: o.BaseURL}
		auths = append(auths, auth)
		prio[o.Name] = o.Priority
		if err := minter.Add(ctx, auth, o.RunnerGroupID); err != nil {
			return nil, nil, nil, err
		}
	}
	// Repo targets (personal-account / per-repo). No runner group — repo runners
	// always join the default group (handled in jit.mintRepo).
	for _, r := range cfg.Repos {
		auth := gh.AppAuth{AppID: r.AppID, PrivateKeyPath: r.PrivateKeyPath, Owner: r.Owner, Repo: r.Repo, BaseURL: r.BaseURL}
		auths = append(auths, auth)
		prio[r.Scope()] = r.Priority
		if err := minter.Add(ctx, auth, 0); err != nil {
			return nil, nil, nil, err
		}
	}
	source, err := gh.New(ctx, auths, cfg.Source.Interval)
	if err != nil {
		return nil, nil, nil, err
	}
	jobs, err := source.Subscribe(ctx)
	return jobs, minter, prio, err
}

// newScaleSetSource wires the Runner Scale Set source agent-side: GitHub
// pushes desired counts over message sessions, the source emits one job per
// missing runner, and it mints the runner into the owning set at dispatch —
// the same Source the standalone controller uses, with the agent's terminal
// reports feeding its supply ledger (see agent.DispatchObserver).
func newScaleSetSource(ctx context.Context, cfg *config.Agent) (<-chan domain.Job, agent.Minter, map[string]int, error) {
	if len(cfg.Repos) > 0 {
		return nil, nil, nil, fmt.Errorf("repo targets aren't supported with the scaleset source (scale sets are org-level)")
	}
	src, err := scaleset.New(cfg.Orgs, version.Version)
	if err != nil {
		return nil, nil, nil, err
	}
	prio := make(map[string]int, len(cfg.Orgs))
	for _, o := range cfg.Orgs {
		prio[o.Name] = o.Priority
	}
	jobs, err := src.Subscribe(ctx)
	return jobs, src, prio, err
}
