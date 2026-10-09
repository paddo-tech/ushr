package github

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/metrics"
	"github.com/paddo-tech/ushr/internal/source"
)

// orgClient pairs a poll target with an authenticated client. It is either
// org-scoped (Org set; Repos an optional in-org allowlist) or repo-scoped
// (Owner+Repo set, for personal-account/per-repo targets).
type orgClient struct {
	Org    string
	Owner  string // repo target owner (empty for org targets)
	Repo   string // repo target repo name (empty for org targets)
	Client *github.Client
	Repos  []string
}

// scope returns the tenant scope this client polls under: the org login, or
// "owner/repo" for a repo target. Matches domain.Job.Org.
func (c orgClient) scope() string {
	if c.Repo != "" {
		return c.Owner + "/" + c.Repo
	}
	return c.Org
}

// Poller polls every configured org's repos for queued workflow runs and emits
// each queued job as a domain.Job on every pass, from a fresh listing or from
// the run cache. A re-emission is what keeps a waiting job in the agent's
// queue; the agent and the control plane reject a repeat while a dispatch for
// the job is live.
type Poller struct {
	clients  []orgClient
	interval time.Duration

	// breakers pause a misbehaving org's polling; keyed by org, accessed only
	// from the poll goroutine.
	breakers map[string]*breaker

	metrics *metrics.Agent

	mu sync.Mutex
	// seenRuns caches each run's last list state so we can skip re-listing its
	// jobs while unchanged: unchanged in_progress runs are the dominant
	// rate-limit cost on a busy repo.
	seenRuns map[int64]runState
	// horizon is the start of each scope's last clean pass: one in which every
	// list call succeeded, so every job it did not emit is no longer queued.
	horizon map[string]time.Time
}

// runState records what we knew about a run at its last jobs-listing.
type runState struct {
	updated  time.Time    // run.updated_at we last processed
	listedAt time.Time    // wall clock of that listing, for the re-list floor
	queued   []domain.Job // the run's queued jobs at that listing
}

const (
	// relistPolls forces a run's jobs to be re-listed at least every N polls even
	// when its updated_at is unchanged. Bounding it in polls (not absolute time)
	// keeps the cache effective at any interval — an absolute floor <= the poll
	// interval would skip nothing. It guards against a needs-gated job that
	// becomes queued without bumping the run's updated_at: picked up within N
	// polls rather than never. With ETag the forced re-list is a free 304 when
	// genuinely unchanged.
	relistPolls = 6
	// seenTTL evicts run cache entries this long after they were last listed,
	// bounding map growth on a long-lived poller.
	seenTTL = 24 * time.Hour
)

// Horizon returns the start of scope's last clean pass, zero before the first.
// A job this source last emitted before it is no longer queued.
func (p *Poller) Horizon(scope string) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.horizon[scope]
}

// RelistGap bounds how stale an emitted job's state can be: a run is re-listed
// at least this often, and its cached jobs are re-emitted until then.
func (p *Poller) RelistGap() time.Duration {
	return relistPolls * p.interval
}

func newBreakers(clients []orgClient) map[string]*breaker {
	b := make(map[string]*breaker, len(clients))
	for _, c := range clients {
		b[c.scope()] = &breaker{}
	}
	return b
}

var _ source.Source = (*Poller)(nil)

// New authenticates as each AppAuth's installation and constructs a Poller.
// interval must be > 0; auths must be non-empty. m receives the per-org API
// error and breaker series.
func New(ctx context.Context, auths []AppAuth, interval time.Duration, m *metrics.Agent) (*Poller, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be > 0, got %s", interval)
	}
	if len(auths) == 0 {
		return nil, errors.New("at least one AppAuth required")
	}
	var clients []orgClient
	for _, a := range auths {
		c, err := NewAppClient(ctx, a)
		if err != nil {
			return nil, fmt.Errorf("auth %s: %w", a.Scope(), err)
		}
		clients = append(clients, orgClient{Org: a.Org, Owner: a.Owner, Repo: a.Repo, Client: c, Repos: a.Repos})
	}
	return &Poller{
		clients:  clients,
		interval: interval,
		breakers: newBreakers(clients),
		metrics:  m,
		seenRuns: make(map[int64]runState),
		horizon:  make(map[string]time.Time),
	}, nil
}

// newWithClients constructs a Poller from already-authenticated clients,
// for tests that don't want to hit GitHub.
func newWithClients(clients []orgClient, interval time.Duration) *Poller {
	return &Poller{
		clients:  clients,
		interval: interval,
		breakers: newBreakers(clients),
		metrics:  metrics.NewAgent(""),
		seenRuns: make(map[int64]runState),
		horizon:  make(map[string]time.Time),
	}
}

// Subscribe begins polling and returns a channel that closes when ctx is done.
// The first poll happens immediately so callers don't wait an interval to see
// already-queued jobs at startup.
func (p *Poller) Subscribe(ctx context.Context) (<-chan domain.Job, error) {
	out := make(chan domain.Job)
	go p.run(ctx, out)
	return out, nil
}

func (p *Poller) run(ctx context.Context, out chan<- domain.Job) {
	defer close(out)

	tick := time.NewTicker(p.interval)
	defer tick.Stop()

	p.pollAll(ctx, out)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.pollAll(ctx, out)
		}
	}
}

func (p *Poller) pollAll(ctx context.Context, out chan<- domain.Job) {
	p.prune(time.Now())
	for _, c := range p.clients {
		if ctx.Err() != nil {
			return
		}
		b := p.breakers[c.scope()]
		if b.ready(time.Now()) {
			if err := p.pollOrg(ctx, c, b, out); err != nil {
				slog.Warn("poll target failed", "scope", c.scope(), "err", err)
			}
		}
		open := 0.0
		if !b.ready(time.Now()) {
			open = 1
		}
		p.metrics.BreakerOpen.WithLabelValues(c.scope()).Set(open)
		p.metrics.GitHubAPIErrors.WithLabelValues(c.scope()) // export 0 before the first error
	}
}

// observe feeds one API outcome to the org's breaker and counts it if it failed.
func (p *Poller) observe(c orgClient, b *breaker, err error, resp *github.Response) {
	b.observe(time.Now(), err, resp)
	if err != nil {
		p.metrics.GitHubAPIErrors.WithLabelValues(c.scope()).Inc()
	}
}

// prune evicts run cache entries older than the TTL, bounding map growth on a
// long-lived poller. Runs once per tick; cheap relative to the API calls. The
// TTL is floored above the relist gap: evicting a live run early only costs one
// extra jobs listing, but there is no reason to pay it.
func (p *Poller) prune(now time.Time) {
	ttl := seenTTL
	if gap := 2 * relistPolls * p.interval; gap > ttl {
		ttl = gap
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, st := range p.seenRuns {
		if now.Sub(st.listedAt) > ttl {
			delete(p.seenRuns, id)
		}
	}
}

// pollOrg polls queued/in_progress workflow runs and emits Jobs for queued
// jobs. A pass with no failed list call advances the scope's horizon. It polls c.Repos when set (an explicit allowlist, which also
// avoids the list-installation-repos call); otherwise every repo the App
// installation can access — which doesn't scale past a handful of repos against
// the API rate limit.
func (p *Poller) pollOrg(ctx context.Context, c orgClient, b *breaker, out chan<- domain.Job) error {
	start := time.Now()
	clean := true
	type repoRef struct{ owner, name string }
	var refs []repoRef
	switch {
	case c.Repo != "":
		// Repo target: one fixed repo, no installation-repo discovery.
		refs = append(refs, repoRef{owner: c.Owner, name: c.Repo})
	case len(c.Repos) > 0:
		for _, name := range c.Repos {
			refs = append(refs, repoRef{owner: c.Org, name: name})
		}
	default:
		repos, err := ListInstallationRepos(ctx, c.Client)
		if err != nil {
			p.observe(c, b, err, nil)
			return fmt.Errorf("list repos: %w", err)
		}
		for _, r := range repos {
			refs = append(refs, repoRef{owner: r.GetOwner().GetLogin(), name: r.GetName()})
		}
	}
	for _, ref := range refs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Both queued AND in_progress runs can hold queued jobs: a run flips to
		// in_progress the moment its first job starts, while later jobs gated on
		// `needs:` stay queued until their deps finish. Polling only queued runs
		// would strand every job after the first in a multi-job workflow.
		for _, status := range []string{"queued", "in_progress"} {
			// A run missing from the page would read as gone, so take the
			// largest page GitHub serves.
			runs, resp, err := c.Client.Actions.ListRepositoryWorkflowRuns(ctx, ref.owner, ref.name,
				&github.ListWorkflowRunsOptions{Status: status, ListOptions: github.ListOptions{PerPage: 100}})
			p.observe(c, b, err, resp)
			// Stop the org's tick the moment the breaker trips — whether from
			// this error (every remaining repo would fail the same way) or a
			// low-water/secondary pause set on a successful observe — so we
			// don't keep spending an exhausted budget this same tick.
			if !b.ready(time.Now()) {
				if err != nil {
					return fmt.Errorf("org paused after %s: %w", status, err)
				}
				return nil
			}
			if err != nil {
				slog.Warn("list runs failed", "repo", ref.owner+"/"+ref.name, "status", status, "err", err)
				clean = false
				continue
			}
			now := time.Now()
			for _, run := range runs.WorkflowRuns {
				if !b.ready(time.Now()) {
					return nil // a jobs-list call tripped the breaker mid-loop
				}
				// Skip the per-run jobs call when the run is unchanged AND we
				// listed it recently. updated_at catches the common case; the
				// relistInterval floor guards against a needs-gated job becoming
				// queued without bumping updated_at — picked up one interval
				// late at worst, rather than never.
				updated := run.GetUpdatedAt().Time
				p.mu.Lock()
				st, ok := p.seenRuns[run.GetID()]
				p.mu.Unlock()
				if !ok || !st.updated.Equal(updated) || now.Sub(st.listedAt) >= relistPolls*p.interval {
					jobs, listed := p.listRunJobs(ctx, c, b, ref.owner, ref.name, run)
					if !listed {
						clean = false
						continue
					}
					st = runState{updated: updated, listedAt: time.Now(), queued: jobs}
					p.mu.Lock()
					p.seenRuns[run.GetID()] = st
					p.mu.Unlock()
				}
				p.emit(ctx, out, st.queued)
			}
		}
	}
	if clean && ctx.Err() == nil {
		p.mu.Lock()
		p.horizon[c.scope()] = start
		p.mu.Unlock()
	}
	return nil
}

// listRunJobs fetches a run's jobs and returns one domain.Job per queued job,
// carrying that job's runs-on labels. Per-job (not per-run)
// granularity is required for two reasons: a runner is ephemeral and serves
// exactly one job, so a multi-job run needs one runner each; and label routing
// needs each job's own labels, which only the jobs API exposes.
//
// ok is false on a list error, leaving the run uncached so the next poll
// retries it.
func (p *Poller) listRunJobs(ctx context.Context, c orgClient, b *breaker, owner, repo string, run *github.WorkflowRun) ([]domain.Job, bool) {
	jobs, resp, err := c.Client.Actions.ListWorkflowJobs(ctx, owner, repo, run.GetID(),
		&github.ListWorkflowJobsOptions{Filter: "latest", ListOptions: github.ListOptions{PerPage: 100}})
	// This per-run call is the dominant rate-limit cost on a busy repo, so feed
	// its outcome to the breaker too — otherwise an abuse/rate trip here would
	// never pause the org.
	p.observe(c, b, err, resp)
	if err != nil {
		slog.Warn("list jobs failed", "repo", owner+"/"+repo, "run", run.GetID(), "err", err)
		return nil, false
	}
	return queuedJobs(c.scope(), repo, run, jobs.Jobs), true
}

// emit sends each job to out. Every pass re-sends a still-queued job, even one
// a runner was minted for: GitHub may have given that runner another job.
// A cancelled ctx aborts the send rather than blocking on an unread channel.
func (p *Poller) emit(ctx context.Context, out chan<- domain.Job, jobs []domain.Job) {
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return
		case out <- job:
		}
	}
}

// queuedJobs builds a domain.Job for each queued job in a run, carrying its
// runs-on labels. scope is the job's tenant scope (org login or "owner/repo").
// Non-queued jobs (in-progress, completed) are skipped — only queued work needs
// a runner.
func queuedJobs(scope, repo string, run *github.WorkflowRun, jobs []*github.WorkflowJob) []domain.Job {
	var out []domain.Job
	for _, j := range jobs {
		if j.GetStatus() != "queued" {
			continue
		}
		out = append(out, domain.Job{
			Org:      scope,
			Repo:     repo,
			RunID:    run.GetID(),
			JobID:    j.GetID(),
			Labels:   j.Labels,
			QueuedAt: run.GetCreatedAt().Time,
		})
	}
	return out
}

// ListInstallationRepos paginates through all repos the App installation can access.
func ListInstallationRepos(ctx context.Context, c *github.Client) ([]*github.Repository, error) {
	var out []*github.Repository
	opt := &github.ListOptions{PerPage: 100}
	for {
		page, resp, err := c.Apps.ListRepos(ctx, opt)
		if err != nil {
			return nil, err
		}
		out = append(out, page.Repositories...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return out, nil
}
