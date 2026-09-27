package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/enroll"
	"github.com/paddo-tech/ushr/internal/telemetry"
)

// dupOnceStore returns ErrDuplicate on the first Offer (simulating losing an
// offer race), then delegates — so a poll must re-pick rather than hold.
type dupOnceStore struct {
	dispatch.Store
	raced bool
}

func (d *dupOnceStore) Offer(r dispatch.Record) error {
	if !d.raced {
		d.raced = true
		return fmt.Errorf("raced: %w", dispatch.ErrDuplicate)
	}
	return d.Store.Offer(r)
}

func TestPollRePicksAfterOfferRace(t *testing.T) {
	store := &dupOnceStore{Store: memLedger(t)}
	srv := httptest.NewServer(NewServer(0, "secret", store, nil).Routes())
	defer srv.Close()
	c := NewClient(srv.URL, "secret")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Job 7 (waited longer) is picked first; its Offer races and loses, so the
	// poll must fall through to job 8 instead of holding for the poll window.
	o, err := c.Poll(ctx, "a1", PollRequest{Capacity: 1, Labels: []string{"self-hosted"},
		Queues: []OrgQueue{{Org: "o", Jobs: []QueuedJob{
			{JobID: 7, Labels: []string{"self-hosted"}, WaitingSecs: 100},
			{JobID: 8, Labels: []string{"self-hosted"}, WaitingSecs: 0},
		}}}})
	if err != nil || o == nil {
		t.Fatalf("poll: err=%v offer=%v", err, o)
	}
	if o.JobID != 8 {
		t.Fatalf("want job 8 offered after job 7 raced, got %d", o.JobID)
	}
}

// fakeEnroll is an in-memory enroll.Store for tests.
type fakeEnroll struct {
	tokens    map[string]agentIdentity  // token -> (orgs, name)
	challenge map[string]string         // session id -> challenge
	approved  map[string]enroll.Session // session id -> minted session
}

func newFakeEnroll() *fakeEnroll {
	return &fakeEnroll{tokens: map[string]agentIdentity{}, challenge: map[string]string{}, approved: map[string]enroll.Session{}}
}
func (f *fakeEnroll) ValidateToken(t string) ([]string, string, bool) {
	id, ok := f.tokens[t]
	return id.orgs, id.name, ok
}
func (f *fakeEnroll) CreateSession(id, ch string, _ enroll.SessionMeta) error {
	f.challenge[id] = ch
	return nil
}
func (f *fakeEnroll) FetchSession(id, verifier string) (enroll.Session, enroll.SessionStatus, error) {
	ch, ok := f.challenge[id]
	if !ok || ch != enroll.Challenge(verifier) {
		return enroll.Session{}, enroll.Expired, nil
	}
	if s, ok := f.approved[id]; ok {
		return s, enroll.Ready, nil
	}
	return enroll.Session{}, enroll.Pending, nil
}

func TestEnrolledTokenAuthenticates(t *testing.T) {
	fe := newFakeEnroll()
	// Token's org matches the reported queue below; its name matches the agent
	// "a1" polling with it (a token is bound to one agent identity).
	fe.tokens["enrolled-tok"] = agentIdentity{orgs: []string{"o"}, name: "a1"}
	srv := httptest.NewServer(NewServer(0, "static-tok", memLedger(t), fe).Routes())
	defer srv.Close()

	// Poll with a matching job so a passing auth returns an instant 200 offer
	// (a work-less poll would be held for the full PollTimeout).
	poll := func(agent, bearer string, jobID int64) int {
		body := `{"capacity":1,"labels":["self-hosted"],"queues":[{"org":"o","jobs":[{"job_id":` +
			strconv.Itoa(int(jobID)) + `,"labels":["self-hosted"]}]}]}`
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/agents/"+agent+"/poll", strings.NewReader(body))
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := poll("a1", "enrolled-tok", 1); code != http.StatusOK {
		t.Fatalf("enrolled token: want 200, got %d", code)
	}
	if code := poll("a2", "static-tok", 2); code != http.StatusOK {
		t.Fatalf("static token: want 200, got %d", code)
	}
	if code := poll("a3", "bogus", 3); code != http.StatusUnauthorized {
		t.Fatalf("bad token: want 401, got %d", code)
	}
}

func TestEmptyOrgsTokenRejected(t *testing.T) {
	fe := newFakeEnroll()
	// An enrolled token that somehow carries no orgs must NOT be treated as an
	// unscoped (static-equivalent) master key — it must fail closed.
	fe.tokens["empty"] = agentIdentity{orgs: nil, name: "a1"}
	srv := httptest.NewServer(NewServer(0, "", memLedger(t), fe).Routes())
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/agents/a1/poll", strings.NewReader(`{"capacity":1}`))
	req.Header.Set("Authorization", "Bearer empty")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("empty-orgs enrolled token must be 401, got %d", resp.StatusCode)
	}
}

func TestCrossTenantClaimDoneRejected(t *testing.T) {
	fe := newFakeEnroll()
	fe.tokens["tok-a"] = agentIdentity{orgs: []string{"orgA"}, name: "a1"}
	fe.tokens["tok-b"] = agentIdentity{orgs: []string{"orgB"}, name: "b1"}
	led := memLedger(t)
	srv := httptest.NewServer(NewServer(0, "", led, fe).Routes())
	defer srv.Close()

	// Agent a1 (orgA) claims an orgA job.
	victim := NewClient(srv.URL, "tok-a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := victim.Poll(ctx, "a1", PollRequest{Capacity: 1, Labels: []string{"self-hosted"},
		Queues: oneJob("orgA", 7, 0, "self-hosted")})
	if err != nil || o == nil {
		t.Fatalf("victim poll: err=%v offer=%v", err, o)
	}
	if err := victim.Claim(ctx, "a1", o.ID); err != nil {
		t.Fatalf("victim claim: %v", err)
	}

	// Attacker b1 (orgB) knows the guessable handle but its token is scoped to
	// orgB: a done for the orgA dispatch must not resolve it.
	attacker := NewClient(srv.URL, "tok-b")
	if err := attacker.ReportDone(ctx, "b1", o.ID, DoneRequest{Status: "lost"}); err != nil {
		t.Fatalf("done returns 200 (resolve is a no-op), got %v", err)
	}
	if got := len(led.Snapshot()); got != 1 {
		t.Fatalf("cross-tenant done must not clear the victim's dispatch, len=%d", got)
	}

	// A token acting under another agent's name is rejected outright.
	if err := attacker.ReportDone(ctx, "a1", o.ID, DoneRequest{Status: "lost"}); err == nil {
		t.Fatal("a token used under another agent's name must be rejected")
	}
	if got := len(led.Snapshot()); got != 1 {
		t.Fatalf("victim's dispatch must survive, len=%d", got)
	}
}

func TestRequireScopedAuth(t *testing.T) {
	fe := newFakeEnroll()
	led := memLedger(t)
	// Enrollment + a static token: refuse (the static token is an unscoped
	// cross-tenant master key on a multi-tenant deploy).
	if err := NewServer(0, "static", led, fe).RequireScopedAuth(); err == nil {
		t.Fatal("want error when enrollment and a static token are both set")
	}
	// Enrollment only, and OSS single-host (static token, no enrollment): allow.
	if err := NewServer(0, "", led, fe).RequireScopedAuth(); err != nil {
		t.Fatalf("enrollment-only should be allowed: %v", err)
	}
	if err := NewServer(0, "static", led, nil).RequireScopedAuth(); err != nil {
		t.Fatalf("OSS single-host static token should be allowed: %v", err)
	}
}

func TestClaimDoneOrgCaseInsensitive(t *testing.T) {
	fe := newFakeEnroll()
	// Token carries GitHub's canonical casing; the agent reports the org as the
	// operator typed it in agent.yaml — a different case. Claim/done must still
	// resolve the agent's own dispatch (else the job livelocks).
	fe.tokens["tok"] = agentIdentity{orgs: []string{"paddo-tech"}, name: "a1"}
	led := memLedger(t)
	srv := httptest.NewServer(NewServer(0, "", led, fe).Routes())
	defer srv.Close()

	c := NewClient(srv.URL, "tok")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.Poll(ctx, "a1", PollRequest{Capacity: 1, Labels: []string{"self-hosted"},
		Queues: oneJob("Paddo-Tech", 7, 0, "self-hosted")})
	if err != nil || o == nil {
		t.Fatalf("poll: err=%v offer=%v", err, o)
	}
	if err := c.Claim(ctx, "a1", o.ID); err != nil {
		t.Fatalf("claim must succeed across org casing: %v", err)
	}
	if err := c.ReportDone(ctx, "a1", o.ID, DoneRequest{Status: "done"}); err != nil {
		t.Fatalf("done: %v", err)
	}
	if got := len(led.Snapshot()); got != 0 {
		t.Fatalf("done must clear the dispatch, len=%d", got)
	}
}

func TestMultiOrgTokenServesItsOrgs(t *testing.T) {
	fe := newFakeEnroll()
	// One token, one agent name, covering two orgs — a shared runner host.
	fe.tokens["tok"] = agentIdentity{orgs: []string{"orgA", "orgB"}, name: "shared"}
	led := memLedger(t)
	srv := httptest.NewServer(NewServer(0, "", led, fe).Routes())
	defer srv.Close()
	c := NewClient(srv.URL, "tok")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A dispatch in each covered org — offered, claimed, and resolved all under
	// the one token (its claim/done guard admits both orgs).
	for _, tc := range []struct {
		org   string
		jobID int64
	}{{"orgA", 1}, {"orgB", 2}} {
		o, err := c.Poll(ctx, "shared", PollRequest{Capacity: 4, Labels: []string{"self-hosted"},
			Queues: oneJob(tc.org, tc.jobID, 0, "self-hosted")})
		if err != nil || o == nil || o.Org != tc.org {
			t.Fatalf("%s: err=%v offer=%v", tc.org, err, o)
		}
		if err := c.Claim(ctx, "shared", o.ID); err != nil {
			t.Fatalf("claim %s: %v", tc.org, err)
		}
		if err := c.ReportDone(ctx, "shared", o.ID, DoneRequest{Status: "done"}); err != nil {
			t.Fatalf("done %s: %v", tc.org, err)
		}
	}
	if got := len(led.Snapshot()); got != 0 {
		t.Fatalf("both orgs' dispatches should be resolved, len=%d", got)
	}
}

func TestQueuesForOrgs(t *testing.T) {
	qs := []OrgQueue{{Org: "a"}, {Org: "b"}, {Org: "c"}, {Org: "a"}}
	got := queuesForOrgs(qs, []string{"A", "c"}) // case-insensitive set membership
	if len(got) != 3 {
		t.Fatalf("want 3 queues (a, c, a), got %d", len(got))
	}
	for _, q := range got {
		if q.Org == "b" {
			t.Fatalf("leaked queue for uncovered org %q", q.Org)
		}
	}
}

func TestCLISessionHandshake(t *testing.T) {
	fe := newFakeEnroll()
	srv := httptest.NewServer(NewServer(0, "static-tok", memLedger(t), fe).Routes())
	defer srv.Close()

	verifier, sid := "verifier-xyz", "sess-1"
	body := `{"session_id":"` + sid + `","challenge":"` + enroll.Challenge(verifier) + `"}`
	resp, err := http.Post(srv.URL+"/v1/cli/session", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: err=%v code=%d", err, resp.StatusCode)
	}
	_ = resp.Body.Close()

	fetch := func(v string) int {
		r, err := http.Post(srv.URL+"/v1/cli/session/"+sid+"/fetch", "application/json",
			strings.NewReader(`{"verifier":"`+v+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		return r.StatusCode
	}
	if code := fetch(verifier); code != http.StatusAccepted {
		t.Fatalf("pending: want 202, got %d", code)
	}
	fe.approved[sid] = enroll.Session{Token: "minted-tok", Orgs: []string{"paddo-tech"}, Name: "linux-runner-1"}
	if code := fetch(verifier); code != http.StatusOK {
		t.Fatalf("ready: want 200, got %d", code)
	}
	if code := fetch("wrong"); code != http.StatusGone {
		t.Fatalf("bad verifier: want 410, got %d", code)
	}
}

func memLedger(t *testing.T) *dispatch.Ledger {
	t.Helper()
	l, err := dispatch.Open("")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// oneJob builds a single-org report of one queued job.
func oneJob(org string, jobID int64, waitingSecs int, labels ...string) []OrgQueue {
	return []OrgQueue{{Org: org, Jobs: []QueuedJob{{JobID: jobID, Labels: labels, WaitingSecs: waitingSecs}}}}
}

func TestPollClaimDone(t *testing.T) {
	led := memLedger(t)
	srv := httptest.NewServer(NewServer(0, "secret", led, nil).Routes())
	defer srv.Close()
	c := NewClient(srv.URL, "secret")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	o, err := c.Poll(ctx, "agent-1", PollRequest{
		Capacity: 2, Labels: []string{"self-hosted", "Linux", "X64"},
		Queues: oneJob("paddo-tech", 7, 0, "self-hosted", "Linux", "X64"),
	})
	if err != nil || o == nil {
		t.Fatalf("poll: err=%v offer=%v", err, o)
	}
	if o.ID != "ushr-agent-1-7" || o.Org != "paddo-tech" || o.JobID != 7 {
		t.Fatalf("unexpected offer: %+v", o)
	}

	// Claim is a keyless ack — no credential comes back.
	if err := c.Claim(ctx, "agent-1", o.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got := len(led.Snapshot()); got != 1 {
		t.Fatalf("claimed dispatch should be live, len=%d", got)
	}
	if err := c.ReportDone(ctx, "agent-1", o.ID, DoneRequest{Status: "done"}); err != nil {
		t.Fatal(err)
	}
	if got := len(led.Snapshot()); got != 0 {
		t.Fatalf("done should clear the ledger, len=%d", got)
	}
}

func TestClaimUnknownOfferIsGone(t *testing.T) {
	srv := httptest.NewServer(NewServer(0, "secret", memLedger(t), nil).Routes())
	defer srv.Close()
	c := NewClient(srv.URL, "secret")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Claim(ctx, "agent-1", "ushr-agent-1-99"); err != ErrOfferGone {
		t.Fatalf("want ErrOfferGone, got %v", err)
	}
}

func TestAuthRejected(t *testing.T) {
	srv := httptest.NewServer(NewServer(0, "secret", memLedger(t), nil).Routes())
	defer srv.Close()
	c := NewClient(srv.URL, "wrong-token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Poll(ctx, "agent-1", PollRequest{Capacity: 1}); err == nil {
		t.Fatal("expected auth failure")
	}
}

// A blocked agent (image store under its floor) must be held, not offered its
// own queued job — the job stays schedulable on a host that can run it.
func TestPollHoldsBlockedAgent(t *testing.T) {
	srv := httptest.NewServer(NewServer(0, "", memLedger(t), nil).Routes())
	defer srv.Close()
	c := NewClient(srv.URL, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	o, err := c.Poll(ctx, "a1", PollRequest{Capacity: 4, Blocked: true,
		Labels: []string{"self-hosted"}, Queues: oneJob("paddo-tech", 7, 0, "self-hosted")})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("poll: %v", err)
	}
	if o != nil {
		t.Fatalf("blocked agent was offered %+v", o)
	}
}

func TestPickSkipsLiveJob(t *testing.T) {
	s := NewServer(0, "", memLedger(t), nil)
	req := PollRequest{Capacity: 1, Labels: []string{"self-hosted"}, Queues: oneJob("paddo-tech", 7, 0, "self-hosted")}

	rec, ok := s.pick("agent-1", nil, req, liveJobs(s.ledger.Snapshot()))
	if !ok {
		t.Fatal("first pick should offer job 7")
	}
	if err := s.ledger.Offer(rec); err != nil {
		t.Fatal(err)
	}
	// Same job re-reported (by this or another agent) must not be picked again.
	if _, ok := s.pick("agent-2", nil, req, liveJobs(s.ledger.Snapshot())); ok {
		t.Fatal("a job already live must not be re-offered")
	}
}

func TestPickLabelFilter(t *testing.T) {
	s := NewServer(0, "", memLedger(t), nil)
	req := PollRequest{Capacity: 1, Labels: []string{"self-hosted", "Linux"},
		Queues: oneJob("paddo-tech", 7, 0, "self-hosted", "Linux", "X64")}
	if _, ok := s.pick("agent-1", nil, req, liveJobs(s.ledger.Snapshot())); ok {
		t.Fatal("job requiring X64 must not match an agent without it")
	}
}

func TestPickPriorityOrder(t *testing.T) {
	s := NewServer(10, "", memLedger(t), nil) // boost 10/min
	req := PollRequest{Capacity: 1, Labels: []string{"self-hosted"}, Queues: []OrgQueue{
		{Org: "low", Priority: 0, Jobs: []QueuedJob{{JobID: 1, Labels: []string{"self-hosted"}, WaitingSecs: 0}}},
		{Org: "high", Priority: 5, Jobs: []QueuedJob{{JobID: 2, Labels: []string{"self-hosted"}, WaitingSecs: 0}}},
	}}
	rec, ok := s.pick("agent-1", nil, req, liveJobs(s.ledger.Snapshot()))
	if !ok || rec.Pending.JobID != 2 {
		t.Fatalf("expected the higher-priority job 2, got %+v ok=%v", rec, ok)
	}
}

func TestSweepExpiresOffer(t *testing.T) {
	led := memLedger(t)
	s := NewServer(0, "", led, nil)
	base := time.Now()
	s.now = func() time.Time { return base }
	if err := led.Offer(dispatch.Record{ID: "ushr-a-7", Agent: "a", OfferedAt: base}); err != nil {
		t.Fatal(err)
	}
	base = base.Add(offerTTL + time.Second)
	s.sweep(led.Snapshot())
	if got := len(led.Snapshot()); got != 0 {
		t.Fatalf("expired offer should be swept, len=%d", got)
	}
}

func TestSweepLostOnDeadAgent(t *testing.T) {
	led := memLedger(t)
	s := NewServer(0, "", led, nil)
	base := time.Now()
	s.now = func() time.Time { return base }
	if err := led.Offer(dispatch.Record{ID: "ushr-a-7", Agent: "a", OfferedAt: base}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := led.Claim("ushr-a-7", base, nil); !ok {
		t.Fatal("claim")
	}
	s.heartbeat("a")
	base = base.Add(agentDeadAfter + time.Second)
	s.sweep(led.Snapshot())
	if got := len(led.Snapshot()); got != 0 {
		t.Fatalf("dead agent's claim should be swept lost, len=%d", got)
	}
}

// resolveFailStore makes every Resolve fail, simulating a transient store error.
type resolveFailStore struct{ dispatch.Store }

func (resolveFailStore) Resolve(_, _ string, _ []string) (dispatch.Record, bool, error) {
	return dispatch.Record{}, false, fmt.Errorf("boom")
}

func TestSweepKeepsClaimWhenResolveFails(t *testing.T) {
	led := memLedger(t)
	s := NewServer(0, "", resolveFailStore{led}, nil)
	base := time.Now()
	s.now = func() time.Time { return base }
	if err := led.Offer(dispatch.Record{ID: "ushr-a-7", Agent: "a", OfferedAt: base}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := led.Claim("ushr-a-7", base, nil); !ok {
		t.Fatal("claim")
	}
	s.heartbeat("a")
	base = base.Add(agentDeadAfter + time.Second)

	survivors := s.sweep(s.ledger.Snapshot())
	// The record's Resolve failed, so it must stay live (not offerable to
	// another agent) and its liveness must be retained so a later sweep retries.
	if len(survivors) != 1 {
		t.Fatalf("record must survive a failed Resolve, survivors=%d", len(survivors))
	}
	s.mu.Lock()
	_, present := s.lastSeen["a"]
	s.mu.Unlock()
	if !present {
		t.Fatal("liveness must be retained on failed Resolve, or the claim orphans")
	}
}

func TestSweepPrunesStaleLiveness(t *testing.T) {
	led := memLedger(t)
	s := NewServer(0, "", led, nil)
	base := time.Now()
	s.now = func() time.Time { return base }
	s.heartbeat("idle-agent") // polled once, never again, holds no dispatch
	base = base.Add(agentDeadAfter + time.Second)
	s.sweep(led.Snapshot())
	s.mu.Lock()
	_, present := s.lastSeen["idle-agent"]
	s.mu.Unlock()
	if present {
		t.Fatal("a long-gone agent's liveness entry should be pruned")
	}
}

type updateRecorder struct {
	telemetry.Noop
	key string
}

func (u *updateRecorder) AgentUpdateTarget(_ context.Context, key string) (string, error) {
	u.key = key
	return "v0.2.5", nil
}

func TestUpdateUsesAuthenticatedHostIdentity(t *testing.T) {
	fe := newFakeEnroll()
	fe.tokens["token"] = agentIdentity{orgs: []string{"example-org"}, name: "host"}
	rec := &updateRecorder{}
	server := NewServer(0, "", memLedger(t), fe)
	server.WithTelemetry(rec)
	for _, name := range []string{"other-host", "host"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := httptest.NewRequest(http.MethodPost, "/v1/agents/"+name+"/poll", strings.NewReader(`{"version":"0.2.4","capacity":1}`)).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer token")
		w := httptest.NewRecorder()
		server.Routes().ServeHTTP(w, req)
		if name == "other-host" {
			if rec.key != "" || w.Header().Get("X-Ushr-Agent-Version") != "" {
				t.Fatal("another host accessed update target")
			}
			continue
		}
		if rec.key != "example-org/host" {
			t.Fatalf("lookup key=%q", rec.key)
		}
		if w.Header().Get("X-Ushr-Agent-Version") != "v0.2.5" {
			t.Fatal("missing update instruction")
		}
	}
}
