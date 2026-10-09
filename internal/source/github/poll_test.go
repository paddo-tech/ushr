package github

import (
	"context"
	"encoding/json"
	"fmt"
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

// fakeActions serves one repo's queued runs and their jobs, two pages of each.
// Run ids in runs are split one per page when there are two; run N has jobs
// 10N on page 1 and 10N+1 on page 2. failJobs fails every jobs listing and
// failPage2 fails the second page of the run listing.
type fakeActions struct {
	mu        sync.Mutex
	runs      []int64
	failJobs  bool
	failPage2 bool
	jobCalls  int
}

func (f *fakeActions) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	page2 := r.URL.Query().Get("page") == "2"
	next := func() {
		u := *r.URL
		q := u.Query()
		q.Set("page", "2")
		u.RawQuery = q.Encode()
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, u.String()))
	}
	switch {
	case r.URL.Path == "/api/v3/repos/o/r/actions/runs":
		var ids []int64
		if r.URL.Query().Get("status") == "queued" {
			ids = f.runs
		}
		if len(ids) > 1 {
			if page2 {
				if f.failPage2 {
					http.Error(w, "boom", http.StatusInternalServerError)
					return
				}
				ids = ids[1:]
			} else {
				ids = ids[:1]
				next()
			}
		}
		runs := []map[string]any{}
		for _, id := range ids {
			runs = append(runs, map[string]any{"id": id, "updated_at": "2026-01-01T00:00:00Z"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": len(f.runs), "workflow_runs": runs})
	case strings.HasSuffix(r.URL.Path, "/jobs"):
		f.jobCalls++
		if f.failJobs {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		var run int64
		_, _ = fmt.Sscanf(r.URL.Path, "/api/v3/repos/o/r/actions/runs/%d/jobs", &run)
		id := run * 10
		if page2 {
			id++
		} else {
			next()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 2, "jobs": []map[string]any{
			{"id": id, "status": "queued", "labels": []string{"self-hosted"}},
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

// pollOnce runs one pass and returns the job ids it sent, in order, and the
// pass marker's start (zero when the pass sent none).
func pollOnce(t *testing.T, p *Poller, c orgClient) ([]int64, time.Time) {
	t.Helper()
	out := make(chan domain.Job, 16)
	if err := p.pollOrg(context.Background(), c, p.breakers[c.scope()], out); err != nil {
		t.Fatal(err)
	}
	close(out)
	var ids []int64
	var marker time.Time
	for j := range out {
		if !j.PassStart.IsZero() {
			if j.Org != c.scope() {
				t.Fatalf("marker for scope %q, want %q", j.Org, c.scope())
			}
			marker = j.PassStart
			continue
		}
		if !marker.IsZero() {
			t.Fatal("a job was sent after the pass marker")
		}
		ids = append(ids, j.JobID)
	}
	return ids, marker
}

// A clean pass reads every page, re-sends every queued job (cached runs
// included), and ends with a marker the agent applies after the jobs. A run
// that leaves the listing stops being sent, so its job falls behind the marker:
// that is how the agent learns it is gone.
func TestPollOrg_CleanPassReemitsAndMarks(t *testing.T) {
	f := &fakeActions{runs: []int64{1, 2}}
	p, c := fakePoller(t, f)

	before := time.Now()
	ids, m1 := pollOnce(t, p, c)
	if fmt.Sprint(ids) != "[10 11 20 21]" {
		t.Fatalf("first pass sent %v, want both pages of runs and jobs [10 11 20 21]", ids)
	}
	if m1.Before(before) {
		t.Fatal("a clean pass must end with a marker")
	}
	ids, m2 := pollOnce(t, p, c)
	if fmt.Sprint(ids) != "[10 11 20 21]" {
		t.Fatalf("cached pass sent %v, want [10 11 20 21]", ids)
	}
	if f.jobCalls != 4 {
		t.Fatalf("jobs pages read %d times, want 4: an unchanged run is served from the cache", f.jobCalls)
	}
	if !m2.After(m1) {
		t.Fatal("each clean pass must mark a later start")
	}

	f.mu.Lock()
	f.runs = nil
	f.mu.Unlock()
	if ids, _ := pollOnce(t, p, c); len(ids) != 0 {
		t.Fatalf("runs gone from the listing still sent %v", ids)
	}
}

// A pass with a failed list call cannot prove a job gone, so it must end
// without a marker. A failed later page counts: the runs it hid would
// otherwise read as gone.
func TestPollOrg_FailedListingSendsNoMarker(t *testing.T) {
	for _, f := range []*fakeActions{
		{runs: []int64{1}, failJobs: true},
		{runs: []int64{1, 2}, failPage2: true},
	} {
		p, c := fakePoller(t, f)
		ids, marker := pollOnce(t, p, c)
		if !marker.IsZero() {
			t.Fatalf("failJobs=%v failPage2=%v: a failed pass sent a marker", f.failJobs, f.failPage2)
		}
		if f.failPage2 && fmt.Sprint(ids) != "[10 11]" {
			t.Fatalf("the page read before the failure should still be sent, got %v", ids)
		}
	}
}
