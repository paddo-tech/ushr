package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
// Only a re-emission puts it back in front of the scheduler, so every pass
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

// fakeActions serves one repo's queued runs and their jobs. runs is the list of
// queued run ids; failJobs makes every jobs listing fail.
type fakeActions struct {
	mu       sync.Mutex
	runs     []int64
	failJobs bool
	jobCalls int
}

func (f *fakeActions) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.URL.Path == "/api/v3/repos/o/r/actions/runs":
		var runs []map[string]any
		if r.URL.Query().Get("status") == "queued" {
			for _, id := range f.runs {
				runs = append(runs, map[string]any{"id": id, "updated_at": "2026-01-01T00:00:00Z"})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": len(runs), "workflow_runs": runs})
	case strings.HasSuffix(r.URL.Path, "/jobs"):
		f.jobCalls++
		if f.failJobs {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 1, "jobs": []map[string]any{
			{"id": 7, "status": "queued", "labels": []string{"self-hosted"}},
		}})
	default:
		http.NotFound(w, r)
	}
}

func fakePoller(t *testing.T, f *fakeActions) (*Poller, orgClient) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	gc, err := NewClient(srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := orgClient{Owner: "o", Repo: "r", Client: gc}
	return newWithClients([]orgClient{c}, time.Hour), c
}

func pollOnce(t *testing.T, p *Poller, c orgClient) []int64 {
	t.Helper()
	out := make(chan domain.Job, 8)
	if err := p.pollOrg(context.Background(), c, p.breakers[c.scope()], out); err != nil {
		t.Fatal(err)
	}
	close(out)
	var ids []int64
	for j := range out {
		ids = append(ids, j.JobID)
	}
	return ids
}

// A clean pass re-sends every queued job, cached runs included, and advances
// the scope's horizon. A run that leaves the listing stops being sent, so its
// job falls behind the horizon: that is how the agent learns it is gone.
func TestPollOrg_CleanPassReemitsAndAdvancesHorizon(t *testing.T) {
	f := &fakeActions{runs: []int64{1}}
	p, c := fakePoller(t, f)

	before := time.Now()
	if got := pollOnce(t, p, c); len(got) != 1 || got[0] != 7 {
		t.Fatalf("first pass emitted %v, want [7]", got)
	}
	h1 := p.Horizon(c.scope())
	if h1.Before(before) {
		t.Fatal("a clean pass must advance the horizon")
	}
	if got := pollOnce(t, p, c); len(got) != 1 || got[0] != 7 {
		t.Fatalf("cached pass emitted %v, want [7]", got)
	}
	if f.jobCalls != 1 {
		t.Fatalf("jobs listed %d times, want 1: an unchanged run is served from the cache", f.jobCalls)
	}
	if !p.Horizon(c.scope()).After(h1) {
		t.Fatal("each clean pass must advance the horizon")
	}

	f.mu.Lock()
	f.runs = nil
	f.mu.Unlock()
	if got := pollOnce(t, p, c); len(got) != 0 {
		t.Fatalf("a run gone from the listing still emitted %v", got)
	}
}

// A pass with a failed list call cannot prove a job gone, so it must leave the
// horizon where it was.
func TestPollOrg_FailedListingHoldsHorizon(t *testing.T) {
	p, c := fakePoller(t, &fakeActions{runs: []int64{1}, failJobs: true})
	if got := pollOnce(t, p, c); len(got) != 0 {
		t.Fatalf("emitted %v from a failed listing", got)
	}
	if h := p.Horizon(c.scope()); !h.IsZero() {
		t.Fatalf("horizon = %v after a failed pass, want zero", h)
	}
}
