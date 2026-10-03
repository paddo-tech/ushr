// Package agent runs the long-poll loop on the runner host. In model B it owns
// the GitHub relationship: it polls GitHub with the customer's App key, reports
// its queue metadata to the keyless control plane, and — once the control plane
// picks a job — mints the JIT config itself and drives the Driver to provision.
package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paddo-tech/ushr/internal/api"
	"github.com/paddo-tech/ushr/internal/disk"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/metrics"
	"github.com/paddo-tech/ushr/internal/update"
	"github.com/paddo-tech/ushr/internal/version"
)

// Minter mints a single-use JIT runner config with the customer's own App key.
// *jit.Minter is the production implementation; the scaleset Source is the
// other (it registers the runner into its scale set at mint time).
type Minter interface {
	Mint(ctx context.Context, name string, job domain.Job, labels []string) (string, error)
}

// DispatchObserver is implemented by sources that keep a per-dispatch supply
// ledger (scale sets): every terminal report clears the runner's entry
// immediately, so supply reconciliation runs on real events rather than TTL
// backstops. Handles the source never minted are a no-op.
type DispatchObserver interface {
	DispatchDone(handle string, failed bool)
}

const (
	pollRetryDelay = 5 * time.Second
	statusTimeout  = 8 * time.Second  // shorter than statusInterval so a hung Status counts as an error
	statusMaxFails = 6                // ~1 min of consecutive Status errors before treating as Failed
	destroyBudget  = 60 * time.Second // tart stop+delete; can stall on hung VM
	reportBudget   = 10 * time.Second // control-plane HTTP call after destroy
	claimBudget    = 15 * time.Second // claim ack
	mintBudget     = 15 * time.Second // local GitHub generate-jitconfig
)

// reclaimInterval is how often the image store is sampled for pressure. A
// statfs costs nothing; the reclaim it may trigger runs on this same goroutine,
// so a long sweep delays the next sample rather than overlapping it.
var reclaimInterval = time.Minute

// reclaimCooldown holds off after a sweep that freed nothing worth having.
// Space held by live job data isn't reclaimable, and without the cooldown such
// a host re-prunes every warm cache on every tick for as long as it stays full.
var reclaimCooldown = 30 * time.Minute

// drainWait bounds how long a host stays deselected waiting to go quiet before
// a drain-gated sweep. A long job outlasts it; giving up and taking work beats
// idling the host indefinitely for a prune.
var drainWait = 20 * time.Minute

// reconcileInterval is how often orphaned slots are reaped. An orphan is a bug
// whether or not the disk is short — a Destroy that failed mid-dispatch — so it
// is collected on its own schedule rather than only under pressure.
var reconcileInterval = 10 * time.Minute

// queueTTL evicts a pending job the source has stopped re-reporting — one that
// was cancelled, or that another host ran. GitHub never says a job left the
// queue, so silence is the only signal there is.
//
// It must exceed the source's worst-case re-report period — the redispatch
// window rounded up to the relist gap, which scales with the poll interval —
// or a job genuinely still waiting would be dropped mid-wait. Re-reports
// refresh the entry, so the value only bounds how long a dead job stays
// pending; the slack costs at most one wasted VM.
var queueTTL = 26 * time.Hour

// unblockDwell is how many consecutive clear samples end a block. One sample at
// the boundary would flap: unblock, take a job, re-block, hand it back.
const unblockDwell = 3

// Loop timings, overridable in tests.
var (
	statusInterval = 10 * time.Second
	// startupGrace bounds how long a freshly provisioned runner may sit idle —
	// registered but never assigned a job — before the agent reaps it. A healthy
	// runner picks up its job within seconds; only a runner whose job was
	// cancelled before pickup idles longer, and it would otherwise hold its slot
	// forever (deadlocking capacity). Requires the driver to implement JobWatcher.
	startupGrace = 2 * time.Minute
)

// Agent runs the dispatch loop and tracks busy slots.
type Agent struct {
	Version       string
	FailedRequest string
	FailedVersion string
	UpdateError   string
	Update        func(context.Context, string, string) error
	Healthy       func(context.Context) error
	Name          string
	Labels        []string
	Driver        driver.Driver
	Client        *api.Client

	// MinFreeDisk is the free-space floor on the driver's image store, in bytes.
	// Under it the agent deselects itself — it keeps polling but reports no free
	// capacity, so the work stays schedulable on a host that can actually run it
	// instead of failing here. Zero disables the gate, as does a driver that
	// can't locate its store.
	MinFreeDisk uint64

	// ReclaimFloor is the free-space mark reclaim works to keep clear. It is
	// separate from MinFreeDisk so that disabling the gate leaves upkeep
	// running: an operator who never wants work refused still wants the caches
	// collected. Zero disables reclaim.
	ReclaimFloor uint64

	// Metrics receives the agent's series; served only when the operator sets
	// a metrics listen address.
	Metrics *metrics.Agent

	minter      Minter
	orgPriority map[string]int
	jobs        <-chan domain.Job // pending jobs polled from GitHub

	wake chan struct{} // nudges the poll loop to re-poll when new work arrives

	diskLow   bool // last reported disk state; written only by the poll loop, to log transitions
	clearRuns int  // consecutive clear samples, for unblockDwell

	// draining deselects the host so it can go quiet for a drain-gated reclaim.
	// Read by the poll loop and every dispatch; written by the reclaim loop.
	draining atomic.Bool

	mu         sync.Mutex
	busy       map[string]struct{}
	unsent     map[string]api.DoneRequest // done-reports that failed to send, latest per handle
	queue      map[int64]queued           // known-pending jobs, reported to the control plane
	dispatched map[int64]bool             // jobs currently claimed/running, excluded from the report
	wg         sync.WaitGroup             // tracks in-flight handleOffer goroutines
}

// queued is a pending job and when the source last reported it. The timestamp
// is the eviction clock: an entry nothing refreshes is one the queue has
// outlived.
type queued struct {
	job  domain.Job
	seen time.Time
}

// New constructs an Agent. minter mints JIT configs with the customer's App
// key; jobs is the stream of pending jobs from the GitHub source; orgPriority
// is the advisory per-org priority reported to the control plane.
func New(name string, labels []string, drv driver.Driver, client *api.Client, minter Minter, orgPriority map[string]int, jobs <-chan domain.Job) *Agent {
	return &Agent{
		Name:        name,
		Labels:      labels,
		Driver:      drv,
		Client:      client,
		Metrics:     metrics.NewAgent(""),
		minter:      minter,
		orgPriority: orgPriority,
		jobs:        jobs,
		wake:        make(chan struct{}, 1),
		busy:        make(map[string]struct{}),
		unsent:      make(map[string]api.DoneRequest),
		queue:       make(map[int64]queued),
		dispatched:  make(map[int64]bool),
	}
}

// ReconcileOrphans destroys slots the Driver still owns that this agent isn't
// accounting for: what a previous process lifetime left behind (launchd
// SIGKILL/OOM), and what a Destroy that failed mid-dispatch stranded. Called at
// startup before Run, and on reconcileInterval thereafter.
//
// The listing is read before the busy set, and never the other way around: a
// slot that goes busy in between was created after the listing, so it can't be
// in it. Reversing that would let a runner provisioned mid-sweep look like an
// orphan and be destroyed under its job.
//
// Driver.List filters by NamePrefix, so unrelated user VMs are safe. Running
// two ushr-agent instances on the same host with the default prefix would
// reap each other's VMs — single-agent assumption holds for v0.1.
func (a *Agent) ReconcileOrphans(ctx context.Context) error {
	slots, err := a.Driver.List(ctx)
	if err != nil {
		return err
	}
	busy := a.busySet()
	for _, h := range slots {
		if busy[string(h)] {
			continue
		}
		slog.Warn("destroying orphan slot", "handle", h)
		if err := a.Driver.Destroy(ctx, h); err != nil {
			slog.Error("destroy orphan failed", "handle", h, "err", err)
		}
		// Slot names are dispatch IDs, so the report clears the control plane's
		// ledger immediately. Report even when Destroy fails: the dispatch is
		// dead either way, and the leaked local slot is already logged above.
		reportCtx, cancel := context.WithTimeout(ctx, reportBudget)
		a.report(reportCtx, string(h), api.DoneRequest{Status: "lost", Error: "orphaned by agent restart"})
		cancel()
	}
	return nil
}

// Run blocks until ctx is cancelled, then waits for in-flight dispatches to
// clean up before returning.
func (a *Agent) Run(ctx context.Context) error {
	slog.Info("agent starting", "name", a.Name, "capacity", a.Driver.Capacity(), "labels", a.Labels)
	a.Metrics.SlotsTotal.Set(float64(a.Driver.Capacity()))
	defer a.wg.Wait()
	target, request := "", ""
	var updateDone chan error
	healthy := false

	go a.drainJobs(ctx)
	go a.reclaimLoop(ctx)
	go a.reconcileLoop(ctx)
	if m, ok := a.Driver.(driver.Maintainer); ok {
		go m.Maintain(ctx)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case err := <-updateDone:
			if errors.Is(err, update.ErrReady) {
				return err
			}
			a.FailedVersion, a.FailedRequest, a.UpdateError = target, request, "Agent update failed. Previous version retained."
			slog.Error("agent update failed", "version", target, "err", err)
			target, updateDone = "", nil
		default:
		}
		a.flushReports(ctx)
		usage, blocked := a.gateState(ctx)
		phase := ""
		if target != "" {
			phase = "draining"
			if updateDone != nil {
				phase = "downloading"
			}
		} else if a.FailedVersion != "" {
			phase = "failed"
		}
		req := api.PollRequest{
			UpdateProtocol: 2, Version: a.Version, UpdateState: phase, UpdateError: a.UpdateError, FailedVersion: a.FailedVersion, FailedRequest: a.FailedRequest,
			Capacity:       a.Driver.Capacity(),
			Busy:           a.busyHandles(),
			Labels:         a.Labels,
			Queues:         a.reportQueues(),
			DiskFreeBytes:  usage.Free,
			DiskTotalBytes: usage.Total,
			Blocked:        blocked,
		}
		// Cancel the server-held long-poll the moment new work arrives, so a job
		// discovered mid-hold is reported on an immediate re-poll instead of
		// after the poll window elapses. watchDone gates the next iteration on
		// the watcher exiting, so only one waits on a.wake at a time.
		pollCtx, cancel := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-a.wake:
				cancel()
			case <-pollCtx.Done():
			}
		}()
		offer, err := a.Client.Poll(pollCtx, a.Name, req)
		cancel()
		<-watchDone

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // parent cancelled — shutting down
			}
			if errors.Is(err, context.Canceled) {
				continue // woken by new work — re-poll immediately with it
			}
			slog.Warn("poll failed", "err", err)
			a.Metrics.PollErrors.Inc()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollRetryDelay):
			}
			continue
		}
		if !healthy && a.Healthy != nil {
			if err := a.Healthy(ctx); err != nil {
				return err
			}
			healthy = true
		}
		if desired := a.Client.UpdateVersion; target == "" && a.Update != nil && version.Newer(desired, a.Version) && (desired != a.FailedVersion || a.Client.UpdateRequest != a.FailedRequest) {
			target, request = desired, a.Client.UpdateRequest
			a.UpdateError = ""
		}
		if target != "" {
			if len(a.busyHandles()) == 0 {
				a.mu.Lock()
				reportsPending := len(a.unsent) > 0
				a.mu.Unlock()
				if !reportsPending && updateDone == nil {
					updateDone = make(chan error, 1)
					done := updateDone
					go func(version, request string) {
						done <- a.Update(ctx, version, request)
						select {
						case a.wake <- struct{}{}:
						default:
						}
					}(target, request)
				}
			}
			continue
		}
		if offer == nil {
			continue
		}
		// Reserve the slot here, before Claim/Provision: marking busy only
		// after the runner is up lets the next poll report stale FreeCapacity,
		// so the control plane over-dispatches past Capacity.
		a.wg.Add(1)
		a.markBusy(offer.ID)
		go func(o *api.Offer) {
			defer a.wg.Done()
			defer a.markFree(o.ID)
			a.handleOffer(ctx, o)
		}(offer)
	}
}

// drainJobs folds the GitHub source stream into the reportable queue.
func (a *Agent) drainJobs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-a.jobs:
			if !ok {
				return
			}
			// Only work this host could actually be given. The control plane
			// applies the same test before offering, so reporting anything else
			// — a GitHub-hosted job, or one for a differently-labelled host —
			// is noise that would never be offered, never complete, and so
			// never leave the queue.
			if !domain.LabelsSatisfied(j.Labels, a.Labels) {
				continue
			}
			a.mu.Lock()
			_, known := a.queue[j.JobID]
			a.queue[j.JobID] = queued{job: j, seen: time.Now()}
			a.mu.Unlock()
			if !known {
				// New job: nudge the poll loop to re-poll now. Non-blocking —
				// a pending nudge already covers this one.
				select {
				case a.wake <- struct{}{}:
				default:
				}
			}
		}
	}
}

// handleOffer claims the control plane's dispatch decision, then mints and
// provisions locally. A gone offer just frees the slot; the job stays queued
// and is re-reported next poll.
func (a *Agent) handleOffer(ctx context.Context, o *api.Offer) {
	claimCtx, cancel := context.WithTimeout(ctx, claimBudget)
	err := a.Client.Claim(claimCtx, a.Name, o.ID)
	cancel()
	if err != nil {
		if errors.Is(err, api.ErrOfferGone) {
			slog.Info("offer gone before claim", "id", o.ID)
			return
		}
		slog.Warn("claim failed, dropping offer", "id", o.ID, "err", err)
		// The claim may have committed server-side before the response was lost.
		// Report it done(lost) so that record is resolved rather than blocking
		// the job forever (there's no wall-clock TTL on claimed records); an
		// unknown handle is a server-side no-op if the claim never landed.
		a.reportBudgeted(ctx, o.ID, api.DoneRequest{Status: "lost", Error: "claim response lost"})
		a.backoff(ctx) // don't hot-loop a persistently failing claim
		return
	}
	a.markDispatched(o.JobID)
	a.handleDispatch(ctx, o)
}

// reclaimLoop keeps the image store clear of the floor. Steps that only touch
// our own idle state run while the host works; steps that delete what a job may
// still be using run only after the host has been deselected and gone quiet.
// That drain is the move a scheduler which can reroute work has available and a
// single-node runtime does not — it trades this host's throughput for the
// certainty that nothing live is destroyed.
func (a *Agent) reclaimLoop(ctx context.Context) {
	r, ok := a.Driver.(driver.Reclaimer)
	if !ok || a.ReclaimFloor == 0 {
		return
	}
	t := time.NewTicker(reclaimInterval)
	defer t.Stop()
	var cooldownUntil time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		path := a.storePath(ctx)
		if path == "" {
			continue // store can't be located; there is nothing to measure or prune
		}
		u, err := disk.Stat(path)
		if err != nil || time.Now().Before(cooldownUntil) {
			continue
		}
		target := a.reclaimTarget(u.Total)
		if u.Free >= a.reclaimTrigger(target) {
			continue
		}
		slog.Info("image store under pressure, reclaiming",
			"free_gb", u.Free>>30, "target_gb", target>>30)

		tiers := r.ReclaimTiers(ctx)
		free := a.runTiers(path, tiers, u.Free, target, false)
		// Drain only once under the floor, never merely near it. There the host
		// is already refusing work, so deselecting it to go quiet costs nothing
		// that isn't already lost; above the floor it would trade real throughput
		// for a prune.
		if free < a.ReclaimFloor && hasDrainTier(tiers) {
			free = a.drainAndReclaim(ctx, path, tiers, free, target)
		}

		// Measured from here, not from the tick: a sweep can outlast the
		// interval, and a ticker holds a tick ready for the moment it returns.
		if free < target {
			cooldownUntil = time.Now().Add(reclaimCooldown)
			slog.Info("reclaim fell short of target, holding off",
				"free_gb", free>>30, "target_gb", target>>30, "cooldown", reclaimCooldown)
			continue
		}
		slog.Info("reclaim finished", "free_gb", free>>30)
	}
}

// reclaimTarget is the free-space mark a sweep works toward: clear of the floor
// so the next sweep isn't immediate, but scaled to the disk. Twice the floor is
// unreachable on a host whose disk is only a few times the floor, and a target
// that can never be met escalates to the most destructive step every time.
func (a *Agent) reclaimTarget(total uint64) uint64 {
	headroom := a.ReclaimFloor
	if tenth := total / 10; tenth < headroom {
		headroom = tenth
	}
	return a.ReclaimFloor + headroom
}

// reclaimTrigger is the mark a sweep starts at: comfortably above the floor so
// upkeep happens before work is refused, but never above the target a sweep can
// reach, which would re-trigger a sweep every interval on a small disk.
func (a *Agent) reclaimTrigger(target uint64) uint64 {
	trigger := a.ReclaimFloor + a.ReclaimFloor/2
	if target < trigger {
		return target
	}
	return trigger
}

// drainAndReclaim deselects the host, waits for it to go quiet, and runs the
// drain-gated steps. A host that never goes quiet within drainWait keeps its
// caches: idling it further to prune would cost more than the disk does.
func (a *Agent) drainAndReclaim(ctx context.Context, path string, tiers []driver.Tier, free, target uint64) uint64 {
	a.draining.Store(true)
	defer a.draining.Store(false)
	slog.Info("deselecting host to drain for reclaim", "busy", len(a.busyHandles()))

	deadline := time.After(drainWait)
	for len(a.busyHandles()) > 0 {
		select {
		case <-ctx.Done():
			return free
		case <-deadline:
			slog.Warn("host still busy at drain deadline, skipping drain-gated reclaim",
				"busy", len(a.busyHandles()))
			return free
		case <-time.After(time.Second):
		}
	}
	return a.runTiers(path, tiers, free, target, true)
}

// runTiers works through the steps of one class in order, stopping as soon as
// the store holds target free bytes: a mild shortfall must not cost what a
// severe one would. A step that fails is logged and skipped — a later one may
// still free enough. path and free are the caller's — the store it measured and
// what it read — so a sweep can't act on a different filesystem than the one it
// decided on, nor read an unmeasurable store as zero free and escalate straight
// to the most destructive step. Returns the free bytes it ended on.
func (a *Agent) runTiers(path string, tiers []driver.Tier, free, target uint64, drain bool) uint64 {
	for _, t := range tiers {
		if t.Drain != drain || free >= target {
			continue
		}
		if err := t.Run(); err != nil {
			slog.Warn("reclaim tier failed", "tier", t.Name, "err", err)
			continue
		}
		u, err := disk.Stat(path)
		if err != nil {
			continue
		}
		var freed uint64
		if u.Free > free {
			freed = u.Free - free // a concurrent job can fill faster than a tier frees
		}
		slog.Info("reclaim tier ran", "tier", t.Name,
			"freed_gb", freed>>30, "free_gb", u.Free>>30)
		free = u.Free
	}
	return free
}

func hasDrainTier(tiers []driver.Tier) bool {
	for _, t := range tiers {
		if t.Drain {
			return true
		}
	}
	return false
}

// reconcileLoop reaps orphaned slots on a schedule. ReconcileOrphans reads the
// driver's slots before the busy set, so a slot that goes busy mid-sweep is
// either absent from the listing or already accounted for — never both.
func (a *Agent) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.ReconcileOrphans(ctx); err != nil {
				slog.Warn("orphan reconcile failed", "err", err)
			}
		}
	}
}

// storePath is the filesystem the driver's images live on, "" when the driver
// can't name one.
func (a *Agent) storePath(ctx context.Context) string {
	w, ok := a.Driver.(driver.DiskWatcher)
	if !ok {
		return ""
	}
	return w.DiskPath(ctx)
}

// diskState samples the image store and reports whether it is under the gate's
// floor. The sample is taken whenever the store can be located, so disabling the
// gate costs the operator the blocking, not the reported free space. A store
// that can't be located, or a sample that fails, reads as clear: a measurement
// problem must not stop a working host from taking jobs.
func (a *Agent) diskState(ctx context.Context) (disk.Usage, bool) {
	path := a.storePath(ctx)
	if path == "" {
		return disk.Usage{}, false
	}
	u, err := disk.Stat(path)
	if err != nil {
		slog.Warn("disk check failed", "path", path, "err", err)
		return disk.Usage{}, false
	}
	return u, a.MinFreeDisk > 0 && u.Free < a.MinFreeDisk
}

// gateState is the reported disk verdict: the raw sample, held blocked until it
// reads clear unblockDwell times running, plus any drain in progress. Only the
// poll loop may call it — it owns the dwell counter.
func (a *Agent) gateState(ctx context.Context) (disk.Usage, bool) {
	u, low := a.diskState(ctx)
	if low {
		a.clearRuns = 0
	} else {
		a.clearRuns++
	}
	blocked := low || (a.diskLow && a.clearRuns < unblockDwell)
	if blocked != a.diskLow {
		a.diskLow = blocked
		if blocked {
			slog.Warn("image store below free-space floor, refusing dispatches",
				"free_gb", u.Free>>30, "floor_gb", a.MinFreeDisk>>30)
		} else {
			slog.Info("image store recovered, accepting dispatches", "free_gb", u.Free>>30)
		}
	}
	blocked = blocked || a.draining.Load()
	if blocked {
		a.Metrics.DiskBlocked.Set(1)
	} else {
		a.Metrics.DiskBlocked.Set(0)
	}
	return u, blocked
}

// declineReason names why this host can't take a dispatch right now, "" when it
// can. Checked after the claim and before the mint, so a dispatch that can't
// succeed costs no single-use credential.
func (a *Agent) declineReason(ctx context.Context) string {
	if a.draining.Load() {
		return "host draining for disk reclaim"
	}
	if _, low := a.diskState(ctx); low {
		return "insufficient disk space"
	}
	return ""
}

// backoff waits pollRetryDelay or until ctx is done, so a persistently failing
// claim/mint/provision can't spin the poll loop at HTTP speed.
func (a *Agent) backoff(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(pollRetryDelay):
	}
}

// handleDispatch mints the JIT with the agent's own key and provisions the
// runner. Mint/provision failures re-open the job for re-reporting.
func (a *Agent) handleDispatch(ctx context.Context, o *api.Offer) {
	// The offer carries only Org+JobID; the repo lives in the local queue the
	// agent built while polling. Look it up so the driver can isolate per-repo.
	a.mu.Lock()
	repo := a.queue[o.JobID].job.Repo
	a.mu.Unlock()
	job := domain.Job{Org: o.Org, Repo: repo, JobID: o.JobID, Labels: o.Labels}

	// The host's state can change between the poll that produced this offer and
	// now — a sibling dispatch fills the store, or a drain starts. Report "lost"
	// rather than "failed": the job never ran, and calling that a failure would
	// put a fault in the dashboards where a reschedule belongs.
	if reason := a.declineReason(ctx); reason != "" {
		slog.Warn("returning dispatch", "id", o.ID, "reason", reason)
		a.undispatch(o.JobID)
		a.reportBudgeted(ctx, o.ID, api.DoneRequest{Status: "lost", Error: reason})
		a.backoff(ctx) // don't re-poll into the same refusal at HTTP speed
		return
	}

	mintCtx, cancel := context.WithTimeout(ctx, mintBudget)
	jitConfig, err := a.minter.Mint(mintCtx, o.ID, job, o.Labels)
	cancel()
	if err != nil {
		slog.Error("mint failed", "id", o.ID, "org", o.Org, "err", err)
		a.undispatch(o.JobID)
		a.reportBudgeted(ctx, o.ID, api.DoneRequest{Status: "failed", Error: err.Error()})
		a.backoff(ctx) // a bad/revoked key would otherwise hot-loop poll->claim->mint
		return
	}

	slog.Info("dispatch received", "id", o.ID, "org", o.Org)
	start := time.Now()
	handle, err := a.Driver.Provision(ctx, driver.ProvisionRequest{
		Name:     o.ID,
		Org:      o.Org,
		Repo:     repo,
		JITToken: jitConfig,
		Labels:   o.Labels,
	})
	if err != nil {
		slog.Error("provision failed", "id", o.ID, "err", err)
		a.Metrics.ProvisionFailures.Inc()
		a.undispatch(o.JobID)
		a.reportBudgeted(ctx, o.ID, api.DoneRequest{Status: "failed", Error: err.Error()})
		a.backoff(ctx)
		return
	}
	a.Metrics.ProvisionSeconds.Observe(time.Since(start).Seconds())
	slog.Info("provisioned", "id", o.ID, "handle", handle)
	a.waitForDone(ctx, handle)

	destroyCtx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), destroyBudget)
	defer dcancel()
	if err := a.Driver.Destroy(destroyCtx, handle); err != nil {
		slog.Warn("destroy failed", "handle", handle, "err", err)
	}
	a.complete(o.JobID)
	a.reportBudgeted(ctx, o.ID, api.DoneRequest{Status: "done"})
	slog.Info("dispatch finished", "id", o.ID, "handle", handle)
}

// waitForDone polls Status until terminal or the consecutive-error budget is
// exhausted. Each Status call gets its own short timeout so a wedged VM (which
// can hang the underlying SSH/pgrep) trips the budget instead of starving the
// loop forever.
func (a *Agent) waitForDone(ctx context.Context, h driver.SlotHandle) {
	t := time.NewTicker(statusInterval)
	defer t.Stop()
	watcher, canWatch := a.Driver.(driver.JobWatcher)
	startupDeadline := time.Now().Add(startupGrace)
	jobStarted := false
	consecutiveErrs := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			callCtx, cancel := context.WithTimeout(ctx, statusTimeout)
			s, err := a.Driver.Status(callCtx, h)
			cancel()
			if err != nil {
				consecutiveErrs++
				slog.Warn("status check failed", "handle", h, "consecutive_errs", consecutiveErrs, "err", err)
				if consecutiveErrs >= statusMaxFails {
					slog.Error("status budget exhausted, treating as failed", "handle", h)
					return
				}
				continue
			}
			consecutiveErrs = 0
			if s == driver.StatusDone || s == driver.StatusFailed {
				return
			}
			// A runner whose job was cancelled before it could pick it up idles
			// at "Listening for Jobs" forever, holding its slot. Once a job has
			// started the runner runs to completion (no deadline); until then,
			// reap it if the startup grace elapses.
			if canWatch && !jobStarted {
				if a.startedJob(ctx, watcher, h) {
					jobStarted = true
				} else if time.Now().After(startupDeadline) {
					slog.Warn("runner idle past startup grace, reaping stranded slot", "handle", h, "grace", startupGrace)
					return
				}
			}
		}
	}
}

// startedJob reports whether the runner has begun a job. A check error logs and
// counts as not-yet-started, so a persistently unreadable runner still trips
// the startup deadline rather than hanging the slot forever.
func (a *Agent) startedJob(ctx context.Context, w driver.JobWatcher, h driver.SlotHandle) bool {
	callCtx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	started, err := w.StartedJob(callCtx, h)
	if err != nil {
		slog.Warn("job-start check failed", "handle", h, "err", err)
		return false
	}
	return started
}

// report sends a terminal dispatch report. On failure the report is buffered
// and retried at the top of every poll iteration — a control plane that was
// down at completion time otherwise never learns the slot freed. The local
// source's ledger is notified here regardless of delivery: the runner is
// terminal either way, and only the control plane needs the retry.
func (a *Agent) report(ctx context.Context, handle string, req api.DoneRequest) {
	if obs, ok := a.minter.(DispatchObserver); ok {
		obs.DispatchDone(handle, req.Status != "done")
	}
	if err := a.Client.ReportDone(ctx, a.Name, handle, req); err != nil {
		slog.Warn("report done failed, buffering for retry", "handle", handle, "err", err)
		a.mu.Lock()
		a.unsent[handle] = req
		a.mu.Unlock()
	}
}

// reportBudgeted reports with a fresh budget detached from ctx, so a terminal
// report still reaches the control plane even if the parent ctx is cancelled
// (SIGTERM mid-dispatch).
func (a *Agent) reportBudgeted(ctx context.Context, handle string, req api.DoneRequest) {
	rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportBudget)
	defer cancel()
	a.report(rc, handle, req)
}

// flushReports retries buffered done-reports. Reports are idempotent
// server-side (an unknown handle resolves to a no-op), so a duplicate send
// after an ambiguous failure is safe.
func (a *Agent) flushReports(ctx context.Context) {
	a.mu.Lock()
	queued := make(map[string]api.DoneRequest, len(a.unsent))
	for h, r := range a.unsent {
		queued[h] = r
	}
	a.mu.Unlock()
	for h, r := range queued {
		callCtx, cancel := context.WithTimeout(ctx, reportBudget)
		err := a.Client.ReportDone(callCtx, a.Name, h, r)
		cancel()
		if err != nil {
			slog.Warn("report retry failed", "handle", h, "err", err)
			continue
		}
		slog.Info("buffered report delivered", "handle", h, "status", r.Status)
		a.mu.Lock()
		delete(a.unsent, h)
		a.mu.Unlock()
	}
}

// reportQueues snapshots the pending jobs to report, grouped by org. Jobs
// currently claimed/running are excluded.
func (a *Agent) reportQueues() []api.OrgQueue {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	byOrg := map[string][]api.QueuedJob{}
	for id, q := range a.queue {
		if a.dispatched[id] {
			continue // in flight: the source stopped reporting it because it is running
		}
		if now.Sub(q.seen) > queueTTL {
			delete(a.queue, id)
			continue
		}
		byOrg[q.job.Org] = append(byOrg[q.job.Org], api.QueuedJob{
			JobID:       id,
			Labels:      q.job.Labels,
			WaitingSecs: int(now.Sub(q.job.QueuedAt).Seconds()),
		})
	}
	out := make([]api.OrgQueue, 0, len(byOrg))
	for org, jobs := range byOrg {
		out = append(out, api.OrgQueue{Org: org, Priority: a.orgPriority[org], Jobs: jobs})
	}
	return out
}

func (a *Agent) markDispatched(jobID int64) {
	a.mu.Lock()
	a.dispatched[jobID] = true
	a.mu.Unlock()
}

// undispatch reopens a job for reporting after a mint/provision failure.
func (a *Agent) undispatch(jobID int64) {
	a.mu.Lock()
	delete(a.dispatched, jobID)
	a.mu.Unlock()
}

// complete drops a finished job from the queue entirely.
func (a *Agent) complete(jobID int64) {
	a.mu.Lock()
	delete(a.queue, jobID)
	delete(a.dispatched, jobID)
	a.mu.Unlock()
}

func (a *Agent) busySet() map[string]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]bool, len(a.busy))
	for h := range a.busy {
		out[h] = true
	}
	return out
}

func (a *Agent) busyHandles() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.busy))
	for h := range a.busy {
		out = append(out, h)
	}
	return out
}

func (a *Agent) markBusy(h string) {
	a.mu.Lock()
	a.busy[h] = struct{}{}
	a.Metrics.SlotsBusy.Set(float64(len(a.busy)))
	a.mu.Unlock()
}

func (a *Agent) markFree(h string) {
	a.mu.Lock()
	delete(a.busy, h)
	a.Metrics.SlotsBusy.Set(float64(len(a.busy)))
	a.mu.Unlock()
}
