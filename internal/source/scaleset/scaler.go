package scaleset

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
)

const (
	// provisionTTL is the backstop for a lost done-report on a never-started
	// runner (agent host died and never restarted): normally the agent reports
	// every container exit within seconds and dispatchDone clears the entry.
	provisionTTL = 30 * time.Minute

	// scaleDownAfter is how long an idle runner must have been minted before a
	// demand drop may retire it — young runners are likely mid-boot or about to
	// be assigned, and retiring them would thrash on transient count dips.
	scaleDownAfter = 2 * time.Minute

	// startedTTL is the backstop for a lost JobCompleted: the lib acks messages
	// before handling and a recreated session never replays lifecycle events,
	// so a completion that lands in a session gap would otherwise pin the slot
	// forever. Sized past the 6h default workflow timeout-minutes.
	startedTTL = 8 * time.Hour

	// workFolder is the runner's working directory, relative to the runner
	// root. Matches the actions/runner default.
	workFolder = "_work"

	// reapTimeout budgets the best-effort delete of a failed runner's GitHub
	// registration.
	reapTimeout = 30 * time.Second
)

// scalesetAPI is the slice of the scale set client the scaler needs; narrowed
// so tests can fake it.
type scalesetAPI interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(ctx context.Context, runnerName string) (*scaleset.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

// scaler reconciles GitHub's desired-runner count for one scale set. The
// message loop only emits credential-free jobs (a failure there can't kill the
// session, and nothing single-use ever waits in the queue); the JIT config is
// minted by mintRunner when the server dispatches the job to an agent.
//
// The ledger tracks every unit of runner-intent so deficit never
// double-provisions: pending (job emitted, waiting in the scheduler — no
// expiry; the heap never drops jobs, so the entry is exact until dispatch)
// and live (runner minted at dispatch). Live entries clear on real events —
// JobCompleted messages, and the agent's done/failed/lost reports routed in
// via dispatchDone (slot names are dispatch IDs, so every report matches) —
// with provisionTTL/startedTTL only as backstops for lost reports. Entries
// are recorded before the job is visible downstream, so a dispatch can never
// observe an un-ledgered job. The mutex is real: mintRunner and dispatchDone
// run on server HTTP goroutines while the Handle* callbacks run on the
// listener goroutine.
//
// Known, accepted tradeoffs (bounded and self-healing): a controller restart
// zeroes the ledger, so the first reconcile can briefly over-provision
// runners for jobs already running (the surplus idles and is scaled down); a
// JobStarted lost in a session gap leaves its runner on the provisionTTL
// backstop instead of startedTTL. Fixing either needs reconciliation against
// GitHub's live runner list — not worth it until observed in practice.
type scaler struct {
	org string
	set config.ScaleSet
	id  int // scale set ID
	api scalesetAPI
	out chan<- domain.Job
	now func() time.Time
	ids *atomic.Int64

	mu      sync.Mutex
	pending map[int64]struct{}     // jobID emitted, not yet dispatched
	live    map[string]*liveRunner // runner name -> minted at dispatch
}

type liveRunner struct {
	mintedAt time.Time
	started  bool
}

func newScaler(org string, set config.ScaleSet, id int, api scalesetAPI, out chan<- domain.Job, ids *atomic.Int64) *scaler {
	return &scaler{
		org:     org,
		set:     set,
		id:      id,
		api:     api,
		out:     out,
		now:     time.Now,
		ids:     ids,
		pending: make(map[int64]struct{}),
		live:    make(map[string]*liveRunner),
	}
}

// HandleDesiredRunnerCount reconciles supply with GitHub's desired count:
// deficit emits one credential-free job per missing runner (capped at
// MaxRunners); over-supply retires seasoned idle runners (agents don't
// idle-reap managed dispatches — the controller owns idle lifecycle).
// Queued pending jobs can't be retracted from the heap; they dispatch,
// idle, and get retired here on a later round.
func (s *scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	s.mu.Lock()
	now := s.now()
	var reap []string
	for name, l := range s.live {
		ttl := provisionTTL
		if l.started {
			ttl = startedTTL
		}
		if now.Sub(l.mintedAt) > ttl {
			slog.Warn("expiring stale runner from ledger (lost done-report?)",
				"org", s.org, "set", s.set.Name, "runner", name, "started", l.started)
			delete(s.live, name)
			reap = append(reap, name)
		}
	}
	want := min(count, s.set.MaxRunners)
	if excess := len(s.pending) + len(s.live) - want; excess > 0 {
		for name, l := range s.live {
			if excess == 0 {
				break
			}
			if l.started || now.Sub(l.mintedAt) < scaleDownAfter {
				continue
			}
			// Deleting the registration makes the idle runner exit; the agent
			// then reports done and frees its slot. A runner assigned a job in
			// this instant is protected GitHub-side (and GitHub reassigns jobs
			// whose runner vanishes), so the race is benign.
			slog.Info("scaling down idle runner",
				"org", s.org, "set", s.set.Name, "runner", name)
			delete(s.live, name)
			reap = append(reap, name)
			excess--
		}
	}
	deficit := want - len(s.pending) - len(s.live)
	jobs := make([]domain.Job, 0, max(deficit, 0))
	for range deficit {
		id := s.ids.Add(1)
		s.pending[id] = struct{}{}
		jobs = append(jobs, domain.Job{
			Org:      s.org,
			JobID:    id,
			Labels:   []string{s.set.Name},
			QueuedAt: now,
		})
	}
	outstanding := len(s.pending) + len(s.live)
	s.mu.Unlock()

	for _, name := range reap {
		go s.reapRegistration(name)
	}

	// The listener wraps message handling in context.WithoutCancel, so the
	// ctx.Done escape only fires on the initial-stats and idle-tick paths.
	// That's fine: the consumer (scheduler ingest) never blocks, so a send
	// can only park momentarily on the channel buffer.
	for i, job := range jobs {
		select {
		case <-ctx.Done():
			// Shutdown mid-emit: retract the never-sent jobs so the ledger
			// stays exact.
			s.mu.Lock()
			for _, j := range jobs[i:] {
				delete(s.pending, j.JobID)
			}
			s.mu.Unlock()
			return outstanding - (len(jobs) - i), ctx.Err()
		case s.out <- job:
			slog.Info("scale set runner requested", "org", s.org, "set", s.set.Name, "job", job.JobID)
		}
	}
	return outstanding, nil
}

func (s *scaler) HandleJobStarted(_ context.Context, info *scaleset.JobStarted) error {
	s.mu.Lock()
	if l, ok := s.live[info.RunnerName]; ok {
		l.started = true
	}
	s.mu.Unlock()
	slog.Info("job started",
		"org", s.org, "set", s.set.Name, "runner", info.RunnerName,
		"repo", info.RepositoryName, "job", info.JobDisplayName)
	return nil
}

func (s *scaler) HandleJobCompleted(_ context.Context, info *scaleset.JobCompleted) error {
	s.mu.Lock()
	delete(s.live, info.RunnerName)
	s.mu.Unlock()
	slog.Info("job completed",
		"org", s.org, "set", s.set.Name, "runner", info.RunnerName,
		"repo", info.RepositoryName, "job", info.JobDisplayName, "result", info.Result)
	return nil
}

// owns reports whether this set emitted (and still awaits dispatch of) jobID.
func (s *scaler) owns(jobID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[jobID]
	return ok
}

// mintRunner registers a fresh runner for the dispatched job under the
// offered name (random per offer, so a retry can never collide with a
// previously consumed registration). A mint error leaves the ledger
// untouched — the job stays re-enqueueable and the next dispatch retries
// cleanly.
func (s *scaler) mintRunner(ctx context.Context, name string, jobID int64) (string, error) {
	cfg, err := s.api.GenerateJitRunnerConfig(ctx,
		&scaleset.RunnerScaleSetJitRunnerSetting{Name: name, WorkFolder: workFolder}, s.id)
	if err != nil {
		return "", fmt.Errorf("generate jit config for %s/%s: %w", s.org, s.set.Name, err)
	}
	s.mu.Lock()
	delete(s.pending, jobID)
	s.live[name] = &liveRunner{mintedAt: s.now()}
	s.mu.Unlock()
	return cfg.EncodedJITConfig, nil
}

// dispatchDone clears a runner the agent reported terminal — its container
// exited (done) or never came up / was orphaned (failed/lost). The next
// reconcile re-supplies if demand persists. On failure the registration
// likely lingers (a completed ephemeral runner deregisters itself), so it is
// reaped best-effort. Returns false if the runner isn't ours.
func (s *scaler) dispatchDone(handle string, failed bool) bool {
	s.mu.Lock()
	_, ok := s.live[handle]
	delete(s.live, handle)
	s.mu.Unlock()
	if !ok {
		return false
	}
	if failed {
		go s.reapRegistration(handle)
	}
	return true
}

func (s *scaler) reapRegistration(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), reapTimeout)
	defer cancel()
	r, err := s.api.GetRunnerByName(ctx, name)
	if err != nil || r == nil {
		if err != nil {
			slog.Warn("lookup failed runner registration", "runner", name, "err", err)
		}
		return
	}
	if err := s.api.RemoveRunner(ctx, int64(r.ID)); err != nil {
		slog.Warn("reap failed runner registration", "runner", name, "err", err)
	}
}
