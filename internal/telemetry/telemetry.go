// Package telemetry persists the dispatch story the control plane otherwise
// drops: finished jobs (job_runs), agent liveness (agent_status), and a live
// transition feed (dispatch_events). Everything here is best-effort — a
// telemetry failure must never fail dispatch — and hosted-only: the OSS
// single-host controller gets the no-op Recorder.
package telemetry

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/ledger"
)

// Recorder receives lifecycle signals from the server and webhook.
type Recorder interface {
	// JobResolved records the terminal dispatch state (done|failed|lost|
	// offer-expired) with the scheduling timestamps carried on the record.
	JobResolved(rec dispatch.Record, status string)
	// JobCompleted merges the GitHub webhook's view of a finished job — repo,
	// workflow, run id, conclusion, queue/start/complete times — into the same
	// job_runs row (either side may land first).
	JobCompleted(r ledger.Record)
	// AgentSeen upserts agent liveness, capacity and host state under the
	// caller's liveness key. Called on every poll; implementations throttle writes.
	AgentSeen(b Beat)
	// Event appends to the live transition feed and notifies listeners.
	Event(org, dispatchID, event string, payload map[string]any)
}

// Beat is one agent heartbeat: its liveness identity plus the capacity and
// image-store state carried on the poll. Blocked means the agent is refusing
// work because its store is under the host's free-space floor.
type Beat struct {
	Key      string
	Name     string
	Orgs     []string
	Labels   []string
	Capacity int
	Busy     int
	// QueueDepth is how many jobs the agent reports as pending. On a blocked
	// host it is the difference between "idle" and "stalled with work waiting".
	QueueDepth int
	DiskFree   uint64
	DiskTotal  uint64
	Blocked    bool
}

// Noop is the OSS default: telemetry disabled.
type Noop struct{}

func (Noop) JobResolved(dispatch.Record, string)          {}
func (Noop) JobCompleted(ledger.Record)                   {}
func (Noop) AgentSeen(Beat)                               {}
func (Noop) Event(string, string, string, map[string]any) {}

const opTimeout = 5 * time.Second

// queueDepth bounds the async write queue. Telemetry writes run on a single
// drain goroutine so they never add latency to the dispatch path; when the
// queue is full (DB stalled) new writes are dropped, not queued unboundedly.
const queueDepth = 256

// heartbeatEvery throttles agent_status writes: polls arrive up to every 25s
// per agent but claim bursts can be much hotter, and liveness at 5s grain is
// plenty for a dashboard.
const heartbeatEvery = 5 * time.Second

// retention bounds dispatch_events; job_runs and agent_status are small and
// keep forever.
const retention = 30 * 24 * time.Hour

const schema = `
CREATE TABLE IF NOT EXISTS job_runs (
	id             text PRIMARY KEY,
	account_org_id text,
	org            text NOT NULL,
	repo           text,
	workflow       text,
	run_id         bigint,
	job_id         bigint NOT NULL,
	agent          text NOT NULL,
	labels         text[] NOT NULL DEFAULT '{}',
	status         text,
	conclusion     text,
	queued_at      timestamptz,
	offered_at     timestamptz,
	claimed_at     timestamptz,
	started_at     timestamptz,
	completed_at   timestamptz,
	resolved_at    timestamptz
);
CREATE INDEX IF NOT EXISTS job_runs_account_resolved ON job_runs (account_org_id, resolved_at DESC);
CREATE INDEX IF NOT EXISTS job_runs_org_resolved ON job_runs (lower(org), resolved_at DESC);
CREATE TABLE IF NOT EXISTS agent_status (
	key            text PRIMARY KEY,
	name           text NOT NULL,
	account_org_id text,
	orgs           text[] NOT NULL DEFAULT '{}',
	labels         text[] NOT NULL DEFAULT '{}',
	capacity       int NOT NULL DEFAULT 0,
	busy           int NOT NULL DEFAULT 0,
	last_seen      timestamptz NOT NULL
);
ALTER TABLE agent_status ADD COLUMN IF NOT EXISTS disk_free_bytes bigint;
ALTER TABLE agent_status ADD COLUMN IF NOT EXISTS disk_total_bytes bigint;
ALTER TABLE agent_status ADD COLUMN IF NOT EXISTS blocked boolean NOT NULL DEFAULT false;
ALTER TABLE agent_status ADD COLUMN IF NOT EXISTS queue_depth int NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS dispatch_events (
	seq            bigserial PRIMARY KEY,
	account_org_id text,
	dispatch_id    text,
	event          text NOT NULL,
	payload        jsonb,
	at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dispatch_events_at ON dispatch_events (at);`

// accountFor resolves an org scope to its owning workspace via the verified
// installation registry. NULL when the scope isn't verified (a null row is
// invisible to every workspace-scoped dashboard query — fail closed).
const accountFor = `(SELECT account_org_id FROM org_installations WHERE lower(github_org) = lower($1) LIMIT 1)`

// canonicalOrg normalizes the org column to the registry's github_org: the
// dispatch side carries operator-typed casing and the webhook side only the
// owner login, so without this the same job's org would depend on which side
// wrote first. Falls back to the raw value for unverified scopes.
const canonicalOrg = `COALESCE((SELECT github_org FROM org_installations WHERE lower(github_org) = lower($1) LIMIT 1), $1)`

// PG is the Neon-backed Recorder, sharing the control plane's pool. All
// writes are handed to a single drain goroutine so a slow or stalled DB
// never adds latency to the dispatch handlers that emit telemetry.
type PG struct {
	pool  *pgxpool.Pool
	tasks chan func(ctx context.Context)

	mu       sync.Mutex
	lastBeat map[string]time.Time
}

var _ Recorder = (*PG)(nil)

// NewWithPool applies the telemetry schema and starts the drain worker and
// daily retention sweep, both stopping when ctx is done.
func NewWithPool(ctx context.Context, pool *pgxpool.Pool) (*PG, error) {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, err
	}
	p := &PG{
		pool:     pool,
		tasks:    make(chan func(context.Context), queueDepth),
		lastBeat: make(map[string]time.Time),
	}
	go p.drain(ctx)
	go p.retentionLoop(ctx)
	return p, nil
}

func (p *PG) drain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-p.tasks:
			opCtx, cancel := context.WithTimeout(context.Background(), opTimeout)
			task(opCtx)
			cancel()
		}
	}
}

// enqueue hands a write to the drain worker, dropping it when the queue is
// full — losing a telemetry row beats stalling dispatch.
func (p *PG) enqueue(task func(ctx context.Context)) {
	select {
	case p.tasks <- task:
	default:
		slog.Warn("telemetry queue full, dropping write")
	}
}

func (p *PG) retentionLoop(ctx context.Context) {
	t := time.NewTicker(24 * time.Hour)
	defer t.Stop()
	for {
		p.sweepRetention()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *PG) sweepRetention() {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if _, err := p.pool.Exec(ctx,
		`DELETE FROM dispatch_events WHERE at < now() - $1::interval`,
		retention.String()); err != nil {
		slog.Warn("telemetry retention sweep failed", "err", err)
	}
}

// notNil keeps pgx from encoding a nil slice as SQL NULL, which a NOT NULL
// array column rejects (DEFAULT does not apply to an explicit NULL).
func notNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (p *PG) JobResolved(rec dispatch.Record, status string) {
	var claimedAt *time.Time
	if !rec.ClaimedAt.IsZero() {
		claimedAt = &rec.ClaimedAt
	}
	labels := notNil(rec.Pending.Labels)
	p.enqueue(func(ctx context.Context) {
		_, err := p.pool.Exec(ctx, `
			INSERT INTO job_runs (id, account_org_id, org, job_id, agent, labels, status, offered_at, claimed_at, resolved_at)
			VALUES ($2, `+accountFor+`, `+canonicalOrg+`, $3, $4, $5, $6, $7, $8, now())
			ON CONFLICT (id) DO UPDATE SET
				account_org_id = COALESCE(job_runs.account_org_id, EXCLUDED.account_org_id),
				org = EXCLUDED.org,
				agent = EXCLUDED.agent,
				status = EXCLUDED.status,
				offered_at = EXCLUDED.offered_at,
				claimed_at = EXCLUDED.claimed_at,
				resolved_at = EXCLUDED.resolved_at`,
			rec.Pending.Org, rec.ID, rec.Pending.JobID, rec.Agent, labels,
			status, rec.OfferedAt, claimedAt)
		if err != nil {
			slog.Warn("telemetry job_runs insert failed", "id", rec.ID, "err", err)
		}
	})
}

func (p *PG) JobCompleted(r ledger.Record) {
	labels := notNil(r.Labels)
	p.enqueue(func(ctx context.Context) {
		// The webhook may land before or after the agent's done report; whichever
		// arrives second fills only its own columns. The dispatch side owns
		// status/agent/org; this side owns the GitHub-truth columns. The webhook
		// carries the owner login, but a personal-account install is verified as
		// "owner/repo" — resolve the workspace against either scope shape.
		_, err := p.pool.Exec(ctx, `
			INSERT INTO job_runs (id, account_org_id, org, repo, workflow, run_id, job_id, agent, labels, conclusion, queued_at, started_at, completed_at)
			VALUES ($2,
				(SELECT account_org_id FROM org_installations
				 WHERE lower(github_org) IN (lower($1), lower($1 || '/' || $3)) LIMIT 1),
				COALESCE((SELECT github_org FROM org_installations
				 WHERE lower(github_org) IN (lower($1), lower($1 || '/' || $3)) LIMIT 1), $1),
				$3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (id) DO UPDATE SET
				account_org_id = COALESCE(job_runs.account_org_id, EXCLUDED.account_org_id),
				repo = EXCLUDED.repo,
				workflow = EXCLUDED.workflow,
				run_id = EXCLUDED.run_id,
				conclusion = EXCLUDED.conclusion,
				queued_at = EXCLUDED.queued_at,
				started_at = EXCLUDED.started_at,
				completed_at = EXCLUDED.completed_at`,
			r.Org, r.RunnerName, r.Repo, r.Workflow, r.RunID, r.JobID, "", labels,
			r.Conclusion, nullTime(r.CreatedAt), nullTime(r.StartedAt), nullTime(r.CompletedAt))
		if err != nil {
			slog.Warn("telemetry job completion merge failed", "runner", r.RunnerName, "err", err)
		}
	})
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (p *PG) AgentSeen(b Beat) {
	now := time.Now()
	p.mu.Lock()
	last, known := p.lastBeat[b.Key]
	if known && now.Sub(last) < heartbeatEvery {
		p.mu.Unlock()
		return
	}
	p.lastBeat[b.Key] = now
	// Evict beats for agents gone an hour so re-enrollments and renames don't
	// grow the map forever on an always-on process.
	for k, seen := range p.lastBeat {
		if now.Sub(seen) > time.Hour {
			delete(p.lastBeat, k)
		}
	}
	p.mu.Unlock()

	org := ""
	if len(b.Orgs) > 0 {
		org = b.Orgs[0]
	}
	orgsArg, labelsArg := notNil(b.Orgs), notNil(b.Labels)
	// An agent that can't measure its store — or one still on a build that
	// doesn't report disk at all — sends zeros. Store NULL for those so a
	// reader can tell "unknown" from "empty", which is the difference between
	// a healthy host and one that looks full.
	var diskFree, diskTotal *int64
	if b.DiskTotal > 0 {
		f, t := int64(b.DiskFree), int64(b.DiskTotal)
		diskFree, diskTotal = &f, &t
	}
	p.enqueue(func(ctx context.Context) {
		_, err := p.pool.Exec(ctx, `
			INSERT INTO agent_status (key, name, account_org_id, orgs, labels, capacity, busy, queue_depth, disk_free_bytes, disk_total_bytes, blocked, last_seen)
			VALUES ($2, $3, `+accountFor+`, $4, $5, $6, $7, $8, $9, $10, $11, now())
			ON CONFLICT (key) DO UPDATE SET
				orgs = EXCLUDED.orgs, labels = EXCLUDED.labels,
				capacity = EXCLUDED.capacity, busy = EXCLUDED.busy,
				queue_depth = EXCLUDED.queue_depth,
				disk_free_bytes = EXCLUDED.disk_free_bytes,
				disk_total_bytes = EXCLUDED.disk_total_bytes,
				blocked = EXCLUDED.blocked,
				account_org_id = COALESCE(EXCLUDED.account_org_id, agent_status.account_org_id),
				last_seen = EXCLUDED.last_seen`,
			org, b.Key, b.Name, orgsArg, labelsArg, b.Capacity, b.Busy, b.QueueDepth,
			diskFree, diskTotal, b.Blocked)
		if err != nil {
			slog.Warn("telemetry agent_status upsert failed", "agent", b.Key, "err", err)
		}
	})
	if !known {
		p.Event(org, "", "agent-online", map[string]any{"agent": b.Key})
	}
}

func (p *PG) Event(org, dispatchID, event string, payload map[string]any) {
	body, _ := json.Marshal(payload)
	p.enqueue(func(ctx context.Context) {
		// One statement: append the event and notify dashboard listeners. The
		// channel payload is just a wake-up; listeners read rows by seq.
		_, err := p.pool.Exec(ctx, `
			WITH ins AS (
				INSERT INTO dispatch_events (account_org_id, dispatch_id, event, payload)
				VALUES (`+accountFor+`, $2, $3, $4) RETURNING seq
			) SELECT pg_notify('ushr_events', seq::text) FROM ins`,
			org, dispatchID, event, body)
		if err != nil {
			slog.Warn("telemetry event insert failed", "event", event, "err", err)
		}
	})
}
