package scaleset

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
)

// mintTest mirrors the server's offer step: pick the runner name, then mint
// under it.
func mintTest(sc *scaler, jobID int64) (string, string, error) {
	name := domain.RandomName(domain.RunnerNamePrefix + sc.set.Name)
	jit, err := sc.mintRunner(context.Background(), name, jobID)
	return name, jit, err
}

var testBase = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

type fakeAPI struct {
	mu      sync.Mutex
	minted  int
	reaped  []string
	runners map[string]int // name -> id, for GetRunnerByName
	err     error
}

func (f *fakeAPI) GenerateJitRunnerConfig(_ context.Context, s *scaleset.RunnerScaleSetJitRunnerSetting, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.minted++
	if f.runners == nil {
		f.runners = map[string]int{}
	}
	f.runners[s.Name] = f.minted
	return &scaleset.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "jit-" + s.Name}, nil
}

func (f *fakeAPI) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.runners[name]
	if !ok {
		return nil, nil
	}
	return &scaleset.RunnerReference{ID: id, Name: name}, nil
}

func (f *fakeAPI) RemoveRunner(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, rid := range f.runners {
		if int64(rid) == id {
			delete(f.runners, name)
			f.reaped = append(f.reaped, name)
		}
	}
	return nil
}

func newTestScaler(maxRunners int) (*scaler, *fakeAPI, chan domain.Job, *time.Time) {
	api := &fakeAPI{}
	out := make(chan domain.Job, 32)
	var ids atomic.Int64
	sc := newScaler("paddo-tech", config.ScaleSet{Name: "pba-arc-runners", MaxRunners: maxRunners}, 7, api, out, &ids)
	now := testBase
	sc.now = func() time.Time { return now }
	return sc, api, out, &now
}

func TestSourceNameRejectsUnownedJob(t *testing.T) {
	sc, _, out, _ := newTestScaler(8)
	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	owned := drain(out)[0]
	s := &Source{scalers: []*scaler{sc}}

	name, err := s.Name(owned, "")
	if err != nil {
		t.Fatalf("owned job: unexpected err %v", err)
	}
	if !strings.HasPrefix(name, domain.RunnerNamePrefix+"pba-arc-runners") {
		t.Fatalf("owned job name %q missing set prefix", name)
	}
	if _, err := s.Name(domain.Job{JobID: owned.JobID + 999}, ""); err == nil {
		t.Fatal("unowned job must be rejected at Name, not fabricated")
	}
}

func drain(ch chan domain.Job) []domain.Job {
	var out []domain.Job
	for {
		select {
		case j := <-ch:
			out = append(out, j)
		default:
			return out
		}
	}
}

func TestDesiredCountEmitsCredentialFreeJobs(t *testing.T) {
	sc, api, out, _ := newTestScaler(8)

	n, err := sc.HandleDesiredRunnerCount(context.Background(), 3)
	if err != nil || n != 3 {
		t.Fatalf("got n=%d err=%v, want 3", n, err)
	}
	jobs := drain(out)
	if len(jobs) != 3 {
		t.Fatalf("emitted %d jobs, want 3", len(jobs))
	}
	if api.minted != 0 {
		t.Fatalf("emission minted %d configs; minting must only happen at dispatch", api.minted)
	}
	for _, j := range jobs {
		if j.Org != "paddo-tech" || len(j.Labels) != 1 || j.Labels[0] != "pba-arc-runners" {
			t.Errorf("bad job routing fields: %+v", j)
		}
		if !sc.owns(j.JobID) {
			t.Errorf("job %d emitted but not pending in the ledger", j.JobID)
		}
	}

	// Same desired count again: all three are pending in the queue — pending
	// entries never expire, so queued jobs are never double-emitted.
	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if extra := drain(out); len(extra) != 0 {
		t.Fatalf("second call emitted %d duplicate jobs, want 0", len(extra))
	}
}

func TestPendingRecordedBeforeSend(t *testing.T) {
	// The dispatch path can mint the instant a job is visible on the channel;
	// the ledger entry must already exist or the mint's pending-delete is a
	// no-op and the entry resurrects as a phantom. Simulate the tightest
	// interleaving: mint each job the moment it arrives, from a concurrent
	// consumer, and verify the ledger converges to live-only.
	api := &fakeAPI{}
	out := make(chan domain.Job)
	var ids atomic.Int64
	sc := newScaler("paddo-tech", config.ScaleSet{Name: "pba-arc-runners", MaxRunners: 8}, 7, api, out, &ids)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for job := range out {
			if _, _, err := mintTest(sc, job.JobID); err != nil {
				t.Errorf("mint: %v", err)
			}
		}
	}()

	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	close(out)
	<-done

	sc.mu.Lock()
	pending, live := len(sc.pending), len(sc.live)
	sc.mu.Unlock()
	if pending != 0 || live != 5 {
		t.Fatalf("ledger pending=%d live=%d, want 0/5 (phantom pending = permanent under-provision)", pending, live)
	}
}

func TestDesiredCountCapsAtMaxRunners(t *testing.T) {
	sc, _, out, _ := newTestScaler(2)
	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if jobs := drain(out); len(jobs) != 2 {
		t.Fatalf("emitted %d jobs, want MaxRunners=2", len(jobs))
	}
}

func TestMintMovesPendingToLive(t *testing.T) {
	sc, _, out, _ := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	job := drain(out)[0]

	name, jit, err := mintTest(sc, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(name, domain.RunnerNamePrefix+"pba-arc-runners-") {
		t.Errorf("runner name %q missing prefix contract", name)
	}
	if jit != "jit-"+name {
		t.Errorf("jit %q doesn't match runner %q", jit, name)
	}
	if sc.owns(job.JobID) {
		t.Error("job still pending after mint")
	}
	// Desired unchanged: no refill while the runner is live.
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	if extra := drain(out); len(extra) != 0 {
		t.Fatalf("emitted %d duplicates for a live runner", len(extra))
	}
}

func TestDispatchDoneFailedFreesSlotAndReapsRegistration(t *testing.T) {
	sc, api, out, _ := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	job := drain(out)[0]
	name, _, _ := mintTest(sc, job.JobID)

	if !sc.dispatchDone(name, true) {
		t.Fatal("dispatchDone should recognise its own runner")
	}
	if sc.dispatchDone("ushr-other-deadbeef", true) {
		t.Fatal("dispatchDone claimed a foreign runner")
	}

	// Slot freed: the same desired count refills with a fresh job.
	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if refills := drain(out); len(refills) != 1 {
		t.Fatalf("emitted %d jobs after failed dispatch, want 1", len(refills))
	}

	// The consumed registration is reaped best-effort (async).
	deadline := time.Now().Add(2 * time.Second)
	for {
		api.mu.Lock()
		reaped := len(api.reaped) == 1 && api.reaped[0] == name
		api.mu.Unlock()
		if reaped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registration %q not reaped", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestJobCompletedFreesSlot(t *testing.T) {
	sc, _, out, _ := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 2)
	jobs := drain(out)
	name, _, _ := mintTest(sc, jobs[0].JobID)

	_ = sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name, Result: "succeeded"})

	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if refills := drain(out); len(refills) != 1 {
		t.Fatalf("emitted %d jobs after completion, want 1", len(refills))
	}
}

func TestProvisionTTLExpiresNeverStartedRunner(t *testing.T) {
	sc, _, out, now := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	job := drain(out)[0]
	_, _, _ = mintTest(sc, job.JobID)

	*now = testBase.Add(provisionTTL + time.Minute)
	n, err := sc.HandleDesiredRunnerCount(context.Background(), 1)
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want 1", n, err)
	}
	if refills := drain(out); len(refills) != 1 {
		t.Fatalf("emitted %d jobs, want 1 (never-started live runner expired)", len(refills))
	}
}

func TestStartedRunnerSurvivesProvisionTTLButNotBackstop(t *testing.T) {
	sc, _, out, now := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	job := drain(out)[0]
	name, _, _ := mintTest(sc, job.JobID)
	_ = sc.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: name})

	*now = testBase.Add(provisionTTL + time.Hour)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	if extra := drain(out); len(extra) != 0 {
		t.Fatalf("started runner expired at provisionTTL; emitted %d duplicates", len(extra))
	}

	*now = testBase.Add(startedTTL + time.Minute)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	if refills := drain(out); len(refills) != 1 {
		t.Fatalf("emitted %d jobs, want 1 (startedTTL backstop)", len(refills))
	}
}

func TestMintErrorLeavesLedgerUntouched(t *testing.T) {
	sc, api, out, _ := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	job := drain(out)[0]

	api.err = fmt.Errorf("boom")
	if _, _, err := mintTest(sc, job.JobID); err == nil {
		t.Fatal("want error from mint failure")
	}
	if !sc.owns(job.JobID) {
		t.Fatal("job should still be pending (retryable) after mint error")
	}
	api.err = nil
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	if extra := drain(out); len(extra) != 0 {
		t.Fatalf("emitted %d duplicates after mint error", len(extra))
	}
}

func TestDispatchDoneCleanExitFreesSlotWithoutReap(t *testing.T) {
	sc, api, out, _ := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 1)
	job := drain(out)[0]
	name, _, _ := mintTest(sc, job.JobID)

	// Agent reports a clean container exit: slot frees on the event (no TTL
	// wait), and no reap — a completed ephemeral runner deregisters itself.
	if !sc.dispatchDone(name, false) {
		t.Fatal("dispatchDone should recognise its own runner")
	}
	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if refills := drain(out); len(refills) != 1 {
		t.Fatalf("emitted %d jobs after done report, want 1", len(refills))
	}
	time.Sleep(50 * time.Millisecond)
	api.mu.Lock()
	reaped := len(api.reaped)
	api.mu.Unlock()
	if reaped != 0 {
		t.Fatalf("clean done must not reap the registration, reaped=%d", reaped)
	}
}

func TestOverSupplyScalesDownSeasonedIdleRunners(t *testing.T) {
	sc, api, out, now := newTestScaler(8)
	_, _ = sc.HandleDesiredRunnerCount(context.Background(), 3)
	jobs := drain(out)
	var names []string
	for _, j := range jobs {
		n, _, _ := mintTest(sc, j.JobID)
		names = append(names, n)
	}
	// One runner picked up a job; demand then drops to 1.
	_ = sc.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: names[0]})

	// Too young: transient dips must not thrash freshly-minted runners.
	if _, err := sc.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	sc.mu.Lock()
	got := len(sc.pending) + len(sc.live)
	sc.mu.Unlock()
	if got != 3 {
		t.Fatalf("outstanding=%d, want 3 (young idle runners kept)", got)
	}

	// Seasoned: the two idle runners retire, the busy one stays.
	*now = testBase.Add(scaleDownAfter + time.Minute)
	if n, err := sc.HandleDesiredRunnerCount(context.Background(), 1); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want 1 after scale-down", n, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		api.mu.Lock()
		reaped := len(api.reaped)
		api.mu.Unlock()
		if reaped == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scale-down reaped %d registrations, want 2", reaped)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if extra := drain(out); len(extra) != 0 {
		t.Fatalf("scale-down emitted %d jobs, want 0", len(extra))
	}
}
