package github

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/domain"
)

func TestNew_Validation(t *testing.T) {
	// AppAuth values are minimal — we never actually hit GitHub because
	// validation errors fire before NewAppClient is called.
	someAuth := []AppAuth{{Org: "x"}}
	cases := []struct {
		name     string
		auths    []AppAuth
		interval time.Duration
		wantErr  bool
	}{
		{"zero interval", someAuth, 0, true},
		{"negative interval", someAuth, -time.Second, true},
		{"no auths", nil, time.Second, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(context.Background(), c.auths, c.interval, nil)
			if (err != nil) != c.wantErr {
				t.Errorf("err=%v wantErr=%v", err, c.wantErr)
			}
		})
	}
}

func TestQueuedJobs_FiltersToQueuedAndCarriesLabels(t *testing.T) {
	created := github.Timestamp{Time: time.Now()}
	run := &github.WorkflowRun{ID: github.Ptr(int64(42)), CreatedAt: &created}
	jobs := []*github.WorkflowJob{
		{ID: github.Ptr(int64(1)), Status: github.Ptr("queued"), Labels: []string{"self-hosted", "pba-arc-runners"}},
		{ID: github.Ptr(int64(2)), Status: github.Ptr("completed"), Labels: []string{"self-hosted", "pba-arc-runners"}},
		{ID: github.Ptr(int64(3)), Status: github.Ptr("queued"), Labels: []string{"self-hosted", "macos"}},
	}

	got := queuedJobs("paddo-tech", "repo", run, jobs)

	if len(got) != 2 {
		t.Fatalf("got %d jobs, want 2 (queued only)", len(got))
	}
	if got[0].JobID != 1 || got[0].RunID != 42 || got[0].Org != "paddo-tech" {
		t.Errorf("job0: %+v", got[0])
	}
	if len(got[0].Labels) != 2 || got[0].Labels[1] != "pba-arc-runners" {
		t.Errorf("job0 labels = %v, want [self-hosted pba-arc-runners]", got[0].Labels)
	}
	if got[1].JobID != 3 {
		t.Errorf("job1 JobID = %d, want 3 (the queued macos job)", got[1].JobID)
	}
}

func TestEmit_RespectsCtxCancel(t *testing.T) {
	p := newWithClients([]orgClient{{Org: "paddo-tech"}}, time.Second)

	// Unbuffered channel with no reader => the send blocks; cancelled ctx unblocks.
	out := make(chan domain.Job)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before emit

	done := make(chan struct{})
	go func() {
		p.emit(ctx, out, []domain.Job{{Org: "paddo-tech", JobID: 1}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emit did not honor ctx.Done() within 1s")
	}
}

func TestPrune_EvictsExpiredEntries(t *testing.T) {
	p := newWithClients(nil, time.Second)
	now := time.Now()
	p.seenRuns[10] = runState{listedAt: now.Add(-25 * time.Hour)}
	p.seenRuns[20] = runState{listedAt: now.Add(-1 * time.Hour)}

	p.prune(now)

	if _, ok := p.seenRuns[10]; ok {
		t.Error("seenRuns[10] should have been evicted")
	}
	if _, ok := p.seenRuns[20]; !ok {
		t.Error("seenRuns[20] should survive")
	}
}

// A runner minted for a job can take another one, leaving the first queued.
// Only a re-emission puts it back in front of the scheduler, so every listing
// must re-send it, not just the first.
func TestEmit_ReemitsStillQueuedJobOnEveryListing(t *testing.T) {
	p := newWithClients(nil, time.Second)
	out := make(chan domain.Job, 3)
	job := domain.Job{Org: "o", JobID: 7}

	for range 3 {
		p.emit(context.Background(), out, []domain.Job{job})
	}
	if len(out) != 3 {
		t.Fatalf("emitted %d times over 3 listings, want 3", len(out))
	}
}

// The consumer's TTL must outlast the longest gap between re-emissions of a
// waiting job (an unchanged run is re-listed every relistPolls polls), or the
// job drops out of the report while it still waits.
func TestQueueTTL_OutlastsRelistGap(t *testing.T) {
	for _, interval := range []time.Duration{10 * time.Second, 30 * time.Second, time.Minute} {
		gap := relistPolls * interval
		if ttl := QueueTTL(interval); ttl < 2*gap {
			t.Errorf("interval %s: TTL %s does not cover two relist gaps of %s", interval, ttl, gap)
		}
	}
	if got := QueueTTL(30 * time.Second); got > 10*time.Minute {
		t.Errorf("TTL at a 30s interval = %s, want a few minutes so a gone job ages out quickly", got)
	}
}
