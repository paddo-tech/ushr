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
// A scale set's name doubles as the runs-on label workflows target. An org
// set's repo scoping is enforced GitHub-side by the runner group the set lives
// in; a repo set (the personal-account path) serves only its repo.
package scaleset

import (
	"context"
	"crypto/rand"
	"encoding/binary"
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
	// Completed, when set before Subscribe, receives every job completion a
	// set reports: the job's scope, its repo, its workflow run and the runner
	// it ran on. It runs on the listener goroutine, so it must not block.
	Completed func(scope, owner, repo string, runID int64, runner string)

	targets []target
	version string
	ids     atomic.Int64
	scalers []*scaler
}

// target is one org or repo whose scale sets this source serves.
type target struct {
	auth    gh.AppAuth
	groupID int
	sets    []config.ScaleSet
}

// New validates the org, repo and set configuration. Credentials are only
// exercised on Subscribe.
func New(orgs []config.Org, repos []config.RepoTarget, version string) (*Source, error) {
	if len(orgs) == 0 && len(repos) == 0 {
		return nil, errors.New("scaleset source: no orgs or repos configured")
	}
	var targets []target
	for _, o := range orgs {
		groupID := o.RunnerGroupID
		if groupID == 0 {
			groupID = config.DefaultRunnerGroupID
		}
		targets = append(targets, target{
			auth:    gh.AppAuth{AppID: o.AppID, PrivateKeyPath: o.PrivateKeyPath, Org: o.Name, BaseURL: o.BaseURL},
			groupID: int(groupID),
			sets:    o.ScaleSets,
		})
	}
	// Runner groups are org-only: a repo's sets live in the default group.
	for _, r := range repos {
		targets = append(targets, target{
			auth:    gh.AppAuth{AppID: r.AppID, PrivateKeyPath: r.PrivateKeyPath, Owner: r.Owner, Repo: r.Repo, BaseURL: r.BaseURL},
			groupID: int(config.DefaultRunnerGroupID),
			sets:    r.ScaleSets,
		})
	}
	for _, t := range targets {
		kind := "org"
		if t.auth.Repo != "" {
			kind = "repo"
		}
		if len(t.sets) == 0 {
			return nil, fmt.Errorf("scaleset source: %s %s has no scale_sets", kind, t.auth.Scope())
		}
		for _, s := range t.sets {
			if s.Name == "" {
				return nil, fmt.Errorf("scaleset source: %s %s has a scale set without a name", kind, t.auth.Scope())
			}
			if s.MaxRunners <= 0 {
				return nil, fmt.Errorf("scaleset source: scale set %s/%s needs max_runners > 0", t.auth.Scope(), s.Name)
			}
		}
	}
	s := &Source{targets: targets, version: version}
	var seed [8]byte
	_, _ = rand.Read(seed[:])
	// Negative request IDs cannot collide with GitHub job IDs or restart from one.
	s.ids.Store(-int64(binary.LittleEndian.Uint64(seed[:])&((1<<52)-1)) - 1)
	return s, nil
}

// Subscribe ensures every configured scale set exists, then runs one message
// listener per set. All clients and scale sets are set up before any listener
// starts, so a failure can't strand live message sessions behind an error
// return. The channel closes when ctx is cancelled and all listeners have
// wound down.
func (s *Source) Subscribe(ctx context.Context) (<-chan domain.Job, error) {
	ch := make(chan domain.Job, 64)
	type listen struct {
		session *scaleset.Client
		sc      *scaler
	}
	var listens []listen
	for _, t := range s.targets {
		// Separate clients for the session listeners and for dispatch-time
		// mints: the lib client's mutex is held across whole HTTPS round-trips
		// (including retry backoff), so sharing one would let a slow mint stall
		// session refreshes and serialize every concurrent dispatch behind it.
		session, err := newClient(ctx, t.auth, s.version)
		if err != nil {
			return nil, err
		}
		mint, err := newClient(ctx, t.auth, s.version)
		if err != nil {
			return nil, err
		}
		scope := t.auth.Scope()
		for _, set := range t.sets {
			id, err := ensureScaleSet(ctx, session, scope, t.groupID, set)
			if err != nil {
				return nil, err
			}
			sc := newScaler(scope, set, id, mint, ch, &s.ids)
			sc.completed = s.Completed
			s.scalers = append(s.scalers, sc)
			listens = append(listens, listen{session: session, sc: sc})
			slog.Info("scale set ready", "scope", scope, "set", set.Name, "id", id, "max_runners", set.MaxRunners)
		}
	}

	var wg sync.WaitGroup
	for _, t := range listens {
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
// installation on a's org or repo, sharing the poll source's installation
// discovery. The config URL's path (org, or owner/repo) sets the registration
// scope.
func newClient(ctx context.Context, a gh.AppAuth, version string) (*scaleset.Client, error) {
	key, err := os.ReadFile(a.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read app key: %w", err)
	}
	installID, err := gh.InstallationID(ctx, a.AppID, key, a)
	if err != nil {
		return nil, err
	}
	return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: gh.WebURL(a.BaseURL) + "/" + a.Scope(),
		GitHubAppAuth: scaleset.GitHubAppAuth{
			ClientID:       strconv.FormatInt(a.AppID, 10),
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
// yet. Existing sets are matched by name within the scope's runner group.
func ensureScaleSet(ctx context.Context, client *scaleset.Client, scope string, groupID int, set config.ScaleSet) (int, error) {
	existing, err := client.GetRunnerScaleSet(ctx, groupID, set.Name)
	if err != nil {
		return 0, fmt.Errorf("get scale set %s/%s: %w", scope, set.Name, err)
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
		return 0, fmt.Errorf("create scale set %s/%s: %w", scope, set.Name, err)
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
			"scope", sc.scope, "set", sc.set.Name, "delay", listenerRestartDelay, "err", err)
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
