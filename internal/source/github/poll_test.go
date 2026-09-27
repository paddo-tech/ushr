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
			_, err := New(context.Background(), c.auths, c.interval)
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
	p.seen[1] = now.Add(-25 * time.Hour) // older than seenTTL
	p.seen[2] = now.Add(-1 * time.Hour)  // fresh
	p.seenRuns[10] = runState{listedAt: now.Add(-25 * time.Hour)}
	p.seenRuns[20] = runState{listedAt: now.Add(-1 * time.Hour)}

	p.prune(now)

	if _, ok := p.seen[1]; ok {
		t.Error("seen[1] should have been evicted")
	}
	if _, ok := p.seen[2]; !ok {
		t.Error("seen[2] should survive")
	}
	if _, ok := p.seenRuns[10]; ok {
		t.Error("seenRuns[10] should have been evicted")
	}
	if _, ok := p.seenRuns[20]; !ok {
		t.Error("seenRuns[20] should survive")
	}
}

func TestEmit_SuppressesDuplicateWithinRedispatchWindow(t *testing.T) {
	p := newWithClients(nil, time.Second)
	out := make(chan domain.Job, 2)
	job := domain.Job{Org: "o", JobID: 7}

	p.emit(context.Background(), out, []domain.Job{job})
	<-out // first sighting sends and records seen

	// Still queued inside the window: a repeat is filtered downstream while
	// the dispatch is live, so suppressing it here only saves churn.
	p.emit(context.Background(), out, []domain.Job{job})

	if len(out) != 0 {
		t.Fatal("re-emitted inside the redispatch window")
	}
}

func TestEmit_RedispatchesJobStillQueuedAfterWindow(t *testing.T) {
	p := newWithClients(nil, time.Second)
	out := make(chan domain.Job, 2)
	job := domain.Job{Org: "o", JobID: 7}

	p.emit(context.Background(), out, []domain.Job{job})
	<-out

	// A job still queued past the window must be offered again — nothing else
	// will provision for it.
	p.mu.Lock()
	p.seen[7] = time.Now().Add(-redispatchAfter - time.Minute)
	p.mu.Unlock()

	p.emit(context.Background(), out, []domain.Job{job})

	select {
	case got := <-out:
		if got.JobID != 7 {
			t.Fatalf("re-emitted the wrong job: %d", got.JobID)
		}
	default:
		t.Fatal("still-queued job was never re-emitted — it would wait forever")
	}

	// The re-emission also refreshes seen, so prune cannot evict a job that is
	// still waiting.
	p.prune(time.Now())
	p.mu.Lock()
	_, ok := p.seen[7]
	p.mu.Unlock()
	if !ok {
		t.Fatal("seen entry evicted immediately after re-emission")
	}
}
