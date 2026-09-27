package agent

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paddo-tech/ushr/internal/api"
	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/driver"
)

// stuckDriver reports Running (until doneAfter status polls, if set);
// StartedJob returns whatever `started` is set to, modelling a runner that
// either picked up a job or sits idle.
type stuckDriver struct {
	started     bool
	doneAfter   int64 // 0 = run forever
	diskPath    string
	statusCalls atomic.Int64
}

func (d *stuckDriver) Capacity() int { return 1 }
func (d *stuckDriver) Provision(context.Context, driver.ProvisionRequest) (driver.SlotHandle, error) {
	return "h", nil
}
func (d *stuckDriver) Status(context.Context, driver.SlotHandle) (driver.Status, error) {
	if n := d.statusCalls.Add(1); d.doneAfter > 0 && n >= d.doneAfter {
		return driver.StatusDone, nil
	}
	return driver.StatusRunning, nil
}
func (d *stuckDriver) Destroy(context.Context, driver.SlotHandle) error  { return nil }
func (d *stuckDriver) List(context.Context) ([]driver.SlotHandle, error) { return nil, nil }
func (d *stuckDriver) StartedJob(context.Context, driver.SlotHandle) (bool, error) {
	return d.started, nil
}
func (d *stuckDriver) DiskPath(context.Context) string { return d.diskPath }

// e2eMinter stands in for the local JIT minter; the control plane is keyless so
// the credential is fabricated agent-side.
type e2eMinter struct{}

func (e2eMinter) Mint(context.Context, string, domain.Job, []string) (string, error) {
	return "jit-cfg", nil
}

// observerMinter records DispatchDone notifications (the scaleset Source's
// supply-ledger contract).
type observerMinter struct {
	e2eMinter
	handles []string
	failed  []bool
}

func (o *observerMinter) DispatchDone(handle string, failed bool) {
	o.handles = append(o.handles, handle)
	o.failed = append(o.failed, failed)
}

// Every terminal report must clear the source's ledger entry — delivered or
// not — with failed mapping to any non-done status.
func TestReport_NotifiesDispatchObserver(t *testing.T) {
	led, err := dispatch.Open("")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewServer(0, "", led, nil).Routes())
	defer srv.Close()

	obs := &observerMinter{}
	a := New("t1", nil, &stuckDriver{}, api.NewClient(srv.URL, ""), obs, nil, nil)

	a.report(context.Background(), "d1", api.DoneRequest{Status: "done"})
	a.report(context.Background(), "d2", api.DoneRequest{Status: "failed", Error: "x"})

	if len(obs.handles) != 2 || obs.handles[0] != "d1" || obs.handles[1] != "d2" {
		t.Fatalf("observer handles = %v, want [d1 d2]", obs.handles)
	}
	if obs.failed[0] != false || obs.failed[1] != true {
		t.Fatalf("observer failed flags = %v, want [false true]", obs.failed)
	}
}

func fastTimings(t *testing.T) {
	si, sg := statusInterval, startupGrace
	t.Cleanup(func() { statusInterval, startupGrace = si, sg })
	statusInterval = 5 * time.Millisecond
	startupGrace = 20 * time.Millisecond
}

// A runner that never starts a job must be reaped once the startup grace
// elapses, so waitForDone returns and the slot frees.
func TestWaitForDone_ReapsStrandedRunner(t *testing.T) {
	fastTimings(t)
	a := New("test", nil, &stuckDriver{started: false}, nil, nil, nil, nil)

	done := make(chan struct{})
	go func() { a.waitForDone(context.Background(), "h"); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitForDone did not reap a stranded runner within 1s")
	}
}

// A runner that has started a job must not be reaped, however long it runs.
func TestWaitForDone_DoesNotReapWorkingRunner(t *testing.T) {
	fastTimings(t)
	a := New("test", nil, &stuckDriver{started: true}, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.waitForDone(ctx, "h"); close(done) }()

	select {
	case <-done:
		t.Fatal("waitForDone reaped a runner that had started a job")
	case <-time.After(100 * time.Millisecond): // well past the 20ms grace
	}
	cancel()
	<-done
}

// TestRunHappyPath drives the full keyless loop: report -> poll -> claim ->
// mint -> provision -> status done -> destroy -> report, against a real server.
func TestRunHappyPath(t *testing.T) {
	fastTimings(t)
	led, err := dispatch.Open("")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewServer(0, "", led, nil).Routes())
	defer srv.Close()

	drv := &stuckDriver{started: true, doneAfter: 1}
	a := New("t1", []string{"self-hosted"}, drv, api.NewClient(srv.URL, ""),
		e2eMinter{}, map[string]int{"acme": 0}, nil)
	// Seed the reportable queue directly (no live GitHub source in the test).
	a.mu.Lock()
	a.queue[7] = queued{
		job:  domain.Job{Org: "acme", JobID: 7, Labels: []string{"self-hosted"}},
		seen: time.Now(),
	}
	a.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = a.Run(ctx); close(runDone) }()

	deadline := time.After(5 * time.Second)
	for drv.statusCalls.Load() == 0 || len(led.Snapshot()) != 0 || len(a.busyHandles()) != 0 {
		select {
		case <-deadline:
			t.Fatalf("dispatch never completed: status_calls=%d ledger=%d busy=%v",
				drv.statusCalls.Load(), len(led.Snapshot()), a.busyHandles())
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on cancel")
	}
}

// A host whose image store is under the floor must keep polling — the
// heartbeat and buffered reports still have to flow — but report itself blocked
// so the control plane offers it nothing.
func TestPollReportsBlockedBelowDiskFloor(t *testing.T) {
	polls := make(chan api.PollRequest, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req api.PollRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		select {
		case polls <- req:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	a := New("t1", nil, &stuckDriver{diskPath: t.TempDir()}, api.NewClient(srv.URL, ""), nil, nil, nil)
	a.MinFreeDisk = math.MaxUint64 // no real filesystem clears this floor

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	select {
	case req := <-polls:
		if !req.Blocked {
			t.Fatal("agent under the disk floor should report blocked")
		}
		if req.FreeCapacity() != 0 {
			t.Fatalf("blocked agent free capacity = %d, want 0", req.FreeCapacity())
		}
		if req.DiskTotalBytes == 0 {
			t.Fatal("poll carried no disk sample")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent never polled")
	}
}

// Nothing but running a job clears a queue entry, so anything the control
// plane would never offer this host must not be taken in at all — otherwise it
// accumulates for the life of the process and, with aging, outranks real work.
func TestDrainJobsDropsUnservableWork(t *testing.T) {
	jobs := make(chan domain.Job, 2)
	a := New("t1", []string{"self-hosted", "Linux", "X64"}, &stuckDriver{}, nil, nil, nil, jobs)

	jobs <- domain.Job{Org: "acme", JobID: 1, Labels: []string{"ubuntu-latest"}}
	jobs <- domain.Job{Org: "acme", JobID: 2, Labels: []string{"self-hosted", "Linux"}}
	close(jobs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.drainJobs(ctx)

	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.queue[1]; ok {
		t.Fatal("a GitHub-hosted job must not enter the queue")
	}
	if _, ok := a.queue[2]; !ok {
		t.Fatal("a servable job must be queued")
	}
}

// A job the source has stopped reporting is gone — cancelled, or run by another
// host. GitHub never says so, so silence past the TTL is the only signal.
func TestReportQueuesEvictsStaleEntries(t *testing.T) {
	a := New("t1", nil, &stuckDriver{}, nil, nil, nil, nil)
	a.mu.Lock()
	a.queue[1] = queued{
		job:  domain.Job{Org: "acme", JobID: 1, Labels: []string{"self-hosted"}},
		seen: time.Now().Add(-2 * queueTTL),
	}
	a.queue[2] = queued{
		job:  domain.Job{Org: "acme", JobID: 2, Labels: []string{"self-hosted"}},
		seen: time.Now(),
	}
	a.mu.Unlock()

	out := a.reportQueues()
	if len(out) != 1 || len(out[0].Jobs) != 1 || out[0].Jobs[0].JobID != 2 {
		t.Fatalf("reported %+v, want only the fresh job", out)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.queue[1]; ok {
		t.Fatal("the stale entry should have been evicted, not just hidden")
	}
}

// runReclaimLoop starts the loop and tears it down before the test's timing
// vars are restored: a loop still reading them races the next test's write.
// Cleanups run last-registered-first, so call this after overriding them.
func runReclaimLoop(t *testing.T, a *Agent) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.reclaimLoop(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// reclaimDriver offers one online tier and optionally one drain-gated tier,
// recording which ran.
type reclaimDriver struct {
	*stuckDriver
	withDrain bool

	mu  sync.Mutex
	ran []string
}

func (d *reclaimDriver) ReclaimTiers(context.Context) []driver.Tier {
	tiers := []driver.Tier{{Name: "online", Run: func() error { return d.record("online") }}}
	if d.withDrain {
		tiers = append(tiers, driver.Tier{Name: "drain", Drain: true,
			Run: func() error { return d.record("drain") }})
	}
	return tiers
}

func (d *reclaimDriver) record(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ran = append(d.ran, name)
	return nil
}

func (d *reclaimDriver) ranTiers() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.ran...)
}

// waitFor polls cond until it holds, failing the test on timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// Steps that only touch our own idle state run while the host works — no drain,
// no deselection.
func TestReclaimRunsOnlineTiersWhileWorking(t *testing.T) {
	prev := reclaimInterval
	t.Cleanup(func() { reclaimInterval = prev })
	reclaimInterval = 5 * time.Millisecond

	drv := &reclaimDriver{stuckDriver: &stuckDriver{diskPath: t.TempDir()}}
	a := New("t1", nil, drv, nil, nil, nil, nil)
	a.ReclaimFloor = math.MaxUint64 / 4 // no filesystem clears 1.5x this
	a.markBusy("ushr-t1-7")
	runReclaimLoop(t, a)

	waitFor(t, "the online tier to run", func() bool { return contains(drv.ranTiers(), "online") })
	if a.draining.Load() {
		t.Fatal("a driver with no drain-gated tier should never deselect the host")
	}
}

// A drain-gated step must wait for the host to go quiet, and the host must
// refuse work while it waits — that wait is the whole point of the drain.
func TestDrainGatedTierWaitsForQuietHost(t *testing.T) {
	prev := reclaimInterval
	t.Cleanup(func() { reclaimInterval = prev })
	reclaimInterval = 5 * time.Millisecond

	drv := &reclaimDriver{stuckDriver: &stuckDriver{diskPath: t.TempDir()}, withDrain: true}
	a := New("t1", nil, drv, nil, nil, nil, nil)
	a.ReclaimFloor = math.MaxUint64 / 4
	a.markBusy("ushr-t1-7")
	runReclaimLoop(t, a)

	waitFor(t, "the host to deselect itself", func() bool { return a.draining.Load() })
	if reason := a.declineReason(context.Background()); reason == "" {
		t.Fatal("a draining host must decline dispatches")
	}
	time.Sleep(50 * time.Millisecond) // ~10 ticks with the slot still busy
	if contains(drv.ranTiers(), "drain") {
		t.Fatal("drain-gated tier ran beside a live job")
	}

	a.markFree("ushr-t1-7")
	waitFor(t, "the drain-gated tier to run", func() bool { return contains(drv.ranTiers(), "drain") })
}

// A sweep that can't reach its target holds off: the space is live job data, and
// re-running the ladder every tick would just keep the caches cold.
func TestReclaimHoldsOffAfterShortfall(t *testing.T) {
	prevInterval, prevCooldown := reclaimInterval, reclaimCooldown
	t.Cleanup(func() { reclaimInterval, reclaimCooldown = prevInterval, prevCooldown })
	reclaimInterval, reclaimCooldown = 5*time.Millisecond, time.Hour

	drv := &reclaimDriver{stuckDriver: &stuckDriver{diskPath: t.TempDir()}}
	a := New("t1", nil, drv, nil, nil, nil, nil)
	a.ReclaimFloor = math.MaxUint64 / 4
	runReclaimLoop(t, a)

	waitFor(t, "the first sweep", func() bool { return len(drv.ranTiers()) > 0 })
	time.Sleep(100 * time.Millisecond) // ~20 ticks
	if got := len(drv.ranTiers()); got != 1 {
		t.Fatalf("ran %d tiers across the cooldown, want 1", got)
	}
}

// Twice the floor is unreachable on a disk only a few times the floor, and a
// target that can never be met escalates to the most destructive step every
// sweep — so the target scales to the disk.
func TestReclaimTargetClampsToDiskSize(t *testing.T) {
	a := &Agent{ReclaimFloor: 60 << 30}
	if got, want := a.reclaimTarget(2000<<30), uint64(120)<<30; got != want {
		t.Fatalf("large disk target = %d GB, want %d GB", got>>30, want>>30)
	}
	if got, want := a.reclaimTarget(500<<30), uint64(110)<<30; got != want {
		t.Fatalf("small disk target = %d GB, want %d GB", got>>30, want>>30)
	}
}

// listDriver owns slots and records what gets destroyed.
type listDriver struct {
	*stuckDriver
	slots []driver.SlotHandle

	mu        sync.Mutex
	destroyed []string
}

func (d *listDriver) List(context.Context) ([]driver.SlotHandle, error) { return d.slots, nil }
func (d *listDriver) Destroy(_ context.Context, h driver.SlotHandle) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.destroyed = append(d.destroyed, string(h))
	return nil
}

// The reconciler runs while the host works, so a slot the agent is accounting
// for must survive it.
func TestReconcileOrphansSparesBusySlots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	drv := &listDriver{stuckDriver: &stuckDriver{}, slots: []driver.SlotHandle{"ushr-a-1", "ushr-b-2"}}
	a := New("t1", nil, drv, api.NewClient(srv.URL, ""), nil, nil, nil)
	a.markBusy("ushr-a-1")

	if err := a.ReconcileOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	drv.mu.Lock()
	defer drv.mu.Unlock()
	if len(drv.destroyed) != 1 || drv.destroyed[0] != "ushr-b-2" {
		t.Fatalf("destroyed %v, want only the orphan", drv.destroyed)
	}
}

// The gate stays open when the driver can't locate its store: a measurement
// failure must not idle a working host.
func TestDiskGateOpenWithoutStorePath(t *testing.T) {
	a := New("t1", nil, &stuckDriver{}, nil, nil, nil, nil)
	a.MinFreeDisk = math.MaxUint64
	if _, blocked := a.diskState(context.Background()); blocked {
		t.Fatal("unknown store path should leave the gate open")
	}
}

// TestReportBuffersAndFlushes verifies a done-report that fails is retried by
// flushReports once the control plane is reachable again.
func TestReportBuffersAndFlushes(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := New("t1", nil, &stuckDriver{}, api.NewClient(srv.URL, ""), nil, nil, nil)
	ctx := context.Background()
	a.report(ctx, "slot-1", api.DoneRequest{Status: "done"})
	a.mu.Lock()
	buffered := len(a.unsent)
	a.mu.Unlock()
	if buffered != 1 {
		t.Fatalf("failed report should buffer, got %d", buffered)
	}
	a.flushReports(ctx)
	a.mu.Lock()
	buffered = len(a.unsent)
	a.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("flush should clear the buffer, got %d", buffered)
	}
}
