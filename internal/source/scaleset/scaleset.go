// Package scaleset implements a Source backed by GitHub's Runner Scale Set
// API (the mechanism behind actions-runner-controller, via the standalone
// actions/scaleset client).
//
// Instead of polling repos for queued jobs, each scale set holds a long-poll
// message session; GitHub pushes the set's desired runner count
// (statistics.TotalAssignedJobs) and per-job lifecycle events. The source
// emits one credential-free job per missing runner, labelled with the set
// name — the scheduler and agents downstream are unchanged — and mints the
// runner's JIT config only when the server dispatches the job (the Source is
// the controller's JITMinter in this mode, and its DispatchObserver: agent
// done-reports clear the supply ledger within seconds, and idle scale-down
// is controller-driven — agents never idle-reap managed dispatches).
// GitHub then assigns queued work to whichever runner registers. This removes
// the whole polling cost class: no repo list scans, no rate-limit breaker, no
// dedup of re-listed jobs.
//
// A scale set's name doubles as the runs-on label workflows target, and repo
// scoping is enforced GitHub-side by the runner group the set lives in.
package scaleset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
	gh "github.com/paddo-tech/ushr/internal/source/github"
)

// listenerRestartDelay paces reconnects after a listener/session failure so a
// GitHub outage doesn't turn into a tight retry loop.
const listenerRestartDelay = 30 * time.Second

// Source streams jobs derived from scale set desired-count messages and mints
// their JIT configs at dispatch (it implements the server's JITMinter and
// DispatchObserver).
type Source struct {
	orgs    []config.Org
	version string
	ids     atomic.Int64
	scalers []*scaler
}

// New validates the org/set configuration. Credentials are only exercised on
// Subscribe.
func New(orgs []config.Org, version string) (*Source, error) {
	if len(orgs) == 0 {
		return nil, errors.New("scaleset source: no orgs configured")
	}
	for _, o := range orgs {
		if len(o.ScaleSets) == 0 {
			return nil, fmt.Errorf("scaleset source: org %s has no scale_sets", o.Name)
		}
		for _, s := range o.ScaleSets {
			if s.Name == "" {
				return nil, fmt.Errorf("scaleset source: org %s has a scale set without a name", o.Name)
			}
			if s.MaxRunners <= 0 {
				return nil, fmt.Errorf("scaleset source: scale set %s/%s needs max_runners > 0", o.Name, s.Name)
			}
		}
	}
	return &Source{orgs: orgs, version: version}, nil
}

// Subscribe ensures every configured scale set exists, then runs one message
// listener per set. All clients and scale sets are set up before any listener
// starts, so a failure can't strand live message sessions behind an error
// return. The channel closes when ctx is cancelled and all listeners have
// wound down.
func (s *Source) Subscribe(ctx context.Context) (<-chan domain.Job, error) {
	ch := make(chan domain.Job, 64)
	type target struct {
		session *scaleset.Client
		sc      *scaler
	}
	var targets []target
	for _, o := range s.orgs {
		// Separate clients for the session listeners and for dispatch-time
		// mints: the lib client's mutex is held across whole HTTPS round-trips
		// (including retry backoff), so sharing one would let a slow mint stall
		// session refreshes and serialize every concurrent dispatch behind it.
		session, err := newClient(ctx, o, s.version)
		if err != nil {
			return nil, err
		}
		mint, err := newClient(ctx, o, s.version)
		if err != nil {
			return nil, err
		}
		for _, set := range o.ScaleSets {
			id, err := ensureScaleSet(ctx, session, o, set)
			if err != nil {
				return nil, err
			}
			sc := newScaler(o.Name, set, id, mint, ch, &s.ids)
			s.scalers = append(s.scalers, sc)
			targets = append(targets, target{session: session, sc: sc})
			slog.Info("scale set ready", "org", o.Name, "set", set.Name, "id", id, "max_runners", set.MaxRunners)
		}
	}

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runListener(ctx, t.session, t.sc)
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch, nil
}

// Name implements api.JITMinter: scale-set runner names are random per
// dispatch (a re-dispatched job gets a fresh runner, never a name collision),
// prefixed with the owning set for attribution.
func (s *Source) Name(job domain.Job, _ string) (string, error) {
	for _, sc := range s.scalers {
		if sc.owns(job.JobID) {
			return domain.RandomName(domain.RunnerNamePrefix + sc.set.Name), nil
		}
	}
	return "", fmt.Errorf("no scale set owns job %d", job.JobID)
}

// Mint implements api.JITMinter: it registers a fresh runner under the
// offered name in the scale set that emitted the job. Called on server HTTP
// goroutines.
func (s *Source) Mint(ctx context.Context, name string, job domain.Job, _ []string) (string, error) {
	for _, sc := range s.scalers {
		if sc.owns(job.JobID) {
			return sc.mintRunner(ctx, name, job.JobID)
		}
	}
	return "", fmt.Errorf("no scale set owns job %d", job.JobID)
}

// DispatchDone implements api.DispatchObserver: every terminal agent report
// clears the runner's ledger slot within seconds, so supply reconciliation
// runs on real events rather than TTL guesswork.
func (s *Source) DispatchDone(handle string, failed bool) {
	for _, sc := range s.scalers {
		if sc.dispatchDone(handle, failed) {
			return
		}
	}
}

// newClient builds a scale set API client authenticated as the App
// installation in the org, sharing the poll source's installation discovery.
func newClient(ctx context.Context, o config.Org, version string) (*scaleset.Client, error) {
	key, err := os.ReadFile(o.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read app key: %w", err)
	}
	installID, err := gh.InstallationID(ctx, o.AppID, key, gh.AppAuth{Org: o.Name})
	if err != nil {
		return nil, err
	}
	return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: "https://github.com/" + o.Name,
		GitHubAppAuth: scaleset.GitHubAppAuth{
			ClientID:       strconv.FormatInt(o.AppID, 10),
			InstallationID: installID,
			PrivateKey:     string(key),
		},
		SystemInfo: scaleset.SystemInfo{
			System:    "ushr",
			Version:   version,
			Subsystem: "controller",
		},
	})
}

// ensureScaleSet returns the scale set's ID, creating it if it doesn't exist
// yet. Existing sets are matched by name within the org's runner group.
func ensureScaleSet(ctx context.Context, client *scaleset.Client, o config.Org, set config.ScaleSet) (int, error) {
	groupID := int(o.RunnerGroupID)
	if groupID == 0 {
		groupID = int(config.DefaultRunnerGroupID)
	}
	existing, err := client.GetRunnerScaleSet(ctx, groupID, set.Name)
	if err != nil {
		return 0, fmt.Errorf("get scale set %s/%s: %w", o.Name, set.Name, err)
	}
	if existing != nil {
		return existing.ID, nil
	}
	created, err := client.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          set.Name,
		RunnerGroupID: groupID,
		Labels:        []scaleset.Label{{Name: set.Name}},
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	})
	if err != nil {
		return 0, fmt.Errorf("create scale set %s/%s: %w", o.Name, set.Name, err)
	}
	return created.ID, nil
}

// runListener holds a message session for one scale set until ctx cancels,
// recreating the session after failures. The scaler (and its runner ledger)
// survives restarts; startedTTL backstops any JobCompleted lost in the gap.
func runListener(ctx context.Context, client *scaleset.Client, sc *scaler) {
	owner, _ := os.Hostname()
	for {
		err := runSession(ctx, client, owner, sc)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("scale set listener exited, restarting",
			"org", sc.org, "set", sc.set.Name, "delay", listenerRestartDelay, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(listenerRestartDelay):
		}
	}
}

func runSession(ctx context.Context, client *scaleset.Client, owner string, sc *scaler) error {
	sess, err := client.MessageSessionClient(ctx, sc.id, owner)
	if err != nil {
		return fmt.Errorf("create message session: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := sess.Close(closeCtx); err != nil {
			slog.Warn("close message session failed", "set", sc.set.Name, "err", err)
		}
	}()
	l, err := listener.New(sess, listener.Config{
		ScaleSetID: sc.id,
		MaxRunners: sc.set.MaxRunners,
		Logger:     slog.Default(),
	})
	if err != nil {
		return fmt.Errorf("build listener: %w", err)
	}
	return l.Run(ctx, sc)
}
