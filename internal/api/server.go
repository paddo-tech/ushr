package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/enroll"
	"github.com/paddo-tech/ushr/internal/metrics"
	"github.com/paddo-tech/ushr/internal/telemetry"
	"github.com/paddo-tech/ushr/internal/version"
)

// PollTimeout is how long the server holds a poll connection open before
// returning 204 if no work surfaces. Agents immediately re-poll afterward,
// which makes the poll a heartbeat with a guaranteed cadence — a full-capacity
// poll is held open too, not bounced.
const PollTimeout = 25 * time.Second

// offerTTL bounds how long an unclaimed offer is held before it is dropped. A
// healthy agent claims within one round-trip; anything slower means the offer
// response was lost. Nothing was minted, so expiry is free — the agent
// re-reports the still-queued job on its next poll.
const offerTTL = 2 * PollTimeout

// agentDeadAfter marks an agent dead when it hasn't been heard from for this
// long. Poll cadence is at most PollTimeout plus turnaround, so several
// consecutive missed windows mean the process is gone, and its claimed
// dispatches are declared lost. It is the ONLY expiry for claimed records: a
// claim stays open for the job's whole runtime (the agent reports done only
// after the runner finishes), so any wall-clock TTL would declare healthy
// long-running jobs lost.
const agentDeadAfter = 4 * PollTimeout

// maxOfferAttempts bounds how many jobs a single poll tries to offer before
// giving up and holding — so an agent reporting many jobs that all race to
// other agents can't spin the offer path within one request.
const maxOfferAttempts = 8

// Server is the keyless control plane. It schedules on the queue metadata each
// agent reports (the agent holds the GitHub key and mints JIT itself) and never
// sees a credential.
type Server struct {
	boostPerMinute int
	token          string
	ledger         dispatch.Store
	enroll         enroll.Store       // nil = OSS single-host (static token / loopback only)
	telemetry      telemetry.Recorder // Noop unless WithTelemetry is called
	// Session create is unauthenticated; without a rate cap a flood keeps
	// ~rate×TTL live rows in the store (the prune only clears expired ones).
	sessionLimiter *ipLimiter
	// Fetch is also unauthenticated (PKCE-gated) and each hit is a DB query;
	// the cap is sized for a legitimate login's 2s poll (~30/min) with room
	// for a shared NAT.
	fetchLimiter *ipLimiter

	metrics *metrics.Controller

	mu       sync.Mutex
	lastSeen map[string]time.Time
	reported map[string][]string // job keys in each live agent's latest poll
	now      func() time.Time    // test seam
}

// WithTelemetry swaps in a live Recorder (hosted control plane). Call before
// serving; the default is telemetry.Noop.
func (s *Server) WithTelemetry(t telemetry.Recorder) {
	s.telemetry = t
}

// NewServer wires the dependencies. boostPerMinute is the priority aging rate.
// token is the static shared secret (OSS single-host and the current trial);
// enr, when non-nil, adds Neon-backed per-agent enrollment tokens and the CLI
// login handshake. If both are empty/nil, RequireAuthOrLoopback refuses to bind
// beyond loopback.
func NewServer(boostPerMinute int, token string, led dispatch.Store, enr enroll.Store) *Server {
	if led == nil {
		panic("api: NewServer requires a ledger")
	}
	s := &Server{
		boostPerMinute: boostPerMinute,
		token:          token,
		ledger:         led,
		enroll:         enr,
		telemetry:      telemetry.Noop{},
		sessionLimiter: newIPLimiter(10, time.Minute),
		fetchLimiter:   newIPLimiter(120, time.Minute),
		lastSeen:       make(map[string]time.Time),
		reported:       make(map[string][]string),
		now:            time.Now,
	}
	s.metrics = metrics.NewController(s.activeAgents, s.queuedJobs)
	// Claimed records recovered from the WAL belong to agents this process has
	// never heard from. Count recovery as contact so the liveness sweep applies
	// to them uniformly: a dead agent's claims resolve lost after agentDeadAfter
	// instead of never.
	for _, rec := range led.Snapshot() {
		if rec.State == dispatch.StateClaimed {
			s.lastSeen[rec.Agent] = s.now()
		}
	}
	return s
}

// RequireScopedAuth refuses a config that enables enrollment while also setting
// a static token. The static token is unscoped — it bypasses queue scoping and
// the per-tenant claim/resolve guard — so on a multi-tenant deploy it is a
// cross-tenant master key. Enroll every agent instead; its token carries an org
// and name. OSS single-host (no enrollment) keeps the static token.
func (s *Server) RequireScopedAuth() error {
	if s.enroll != nil && s.token != "" {
		return errors.New("enrollment enabled with a static token set: the static token is an unscoped cross-tenant master key — unset USHR_TOKEN and enroll agents instead")
	}
	return nil
}

// RequireAuthOrLoopback returns an error if listenAddr binds beyond loopback
// while the server has an empty auth token. Call before binding to fail loudly.
//
// "loopback" means literally `localhost` or an IP that satisfies
// net.IP.IsLoopback. Other hostnames are rejected because they may resolve to
// public addresses and we can't tell at config-load time.
func (s *Server) RequireAuthOrLoopback(listenAddr string) error {
	if s.token != "" || s.enroll != nil {
		return nil
	}
	reject := func() error {
		return fmt.Errorf("refusing to listen on %q with empty token: set listen to a loopback address (127.0.0.1, ::1, localhost) or set a token", listenAddr)
	}
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fmt.Errorf("parse listen %q: %w", listenAddr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() || !ip.IsLoopback() {
		return reject()
	}
	return nil
}

// Routes returns an http.Handler with all endpoints mounted.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/{name}/poll", s.handlePoll)
	mux.HandleFunc("POST /v1/agents/{name}/dispatches/{id}/claim", s.handleClaim)
	mux.HandleFunc("POST /v1/agents/{name}/slots/{handle}/done", s.handleDone)
	mux.HandleFunc("POST /v1/cli/session", s.handleCLISessionCreate)
	mux.HandleFunc("POST /v1/cli/session/{id}/fetch", s.handleCLISessionFetch)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", s.metrics.Handler())
	return s.authMiddleware(mux)
}

// metricsAllowed gates /metrics. The series are fleet-wide, so an enrolled
// agent token, which is scoped to one tenant, must not read them. A static
// token is the operator's own key and unlocks them. Without one (loopback OSS
// and the enrolled hosted plane) only a direct loopback peer may scrape; a
// proxy-forwarding header means a tunnel or edge relayed an outside request.
func (s *Server) metricsAllowed(r *http.Request) bool {
	if s.token != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		return subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
	}
	for _, h := range []string{"X-Forwarded-For", "Forwarded", "Fly-Client-IP"} {
		if r.Header.Get(h) != "" {
			return false
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// Health and the pre-login handshake are unauthenticated (the CLI has no
		// token yet; the handshake is gated by session entropy + the PKCE verifier).
		if p == "/healthz" || strings.HasPrefix(p, "/v1/cli/") {
			next.ServeHTTP(w, r)
			return
		}
		if p == "/metrics" {
			if !s.metricsAllowed(r) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		// No auth configured at all: loopback OSS single-host.
		if s.token == "" && s.enroll == nil {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if s.token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1 {
			// Static token is unscoped (OSS single-host / trial): no tenant binding.
			next.ServeHTTP(w, r)
			return
		}
		if s.enroll != nil {
			// Require a non-empty org set: an empty set is the unscoped
			// (static-token) semantics, so an enrolled token with no orgs would
			// otherwise become a cross-tenant master key. Fail closed instead.
			if orgs, name, ok := s.enroll.ValidateToken(got); ok && len(orgs) > 0 {
				// Bind the request to the token's tenant identity so the handlers
				// scope the agent to the orgs it may serve and its own agent name.
				ctx := context.WithValue(r.Context(), identityKey{}, agentIdentity{orgs: orgs, name: name})
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

// agentIdentity is the tenant an enrolled token belongs to: the set of orgs it
// may serve and the agent name it is bound to. An absent identity means
// unscoped (static token) — no tenant restriction.
type agentIdentity struct {
	orgs []string
	name string
}

type identityKey struct{}

// livenessKey namespaces an agent's heartbeat so two tenants that both name an
// agent "linux-runner-1" don't share (and mask) each other's liveness. It keys by the
// token's primary org (each org belongs to exactly one account, so this stays
// unique per account) plus the name. An unscoped static token has no orgs, so
// it keys by name alone.
func livenessKey(orgs []string, name string) string {
	if len(orgs) == 0 {
		return name
	}
	return orgs[0] + "/" + name
}

func (s *Server) handleCLISessionCreate(w http.ResponseWriter, r *http.Request) {
	if s.enroll == nil {
		http.Error(w, "enrollment not enabled", http.StatusNotFound)
		return
	}
	if !s.sessionLimiter.allow(limiterIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req struct {
		SessionID string `json:"session_id"`
		Challenge string `json:"challenge"`
		AgentName string `json:"agent_name"`
		UserCode  string `json:"user_code"`
	}
	// Unauthenticated endpoint: cap the body so decode-time memory is bounded.
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SessionID == "" || req.Challenge == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	meta := enroll.SessionMeta{
		AgentName:   clip(req.AgentName, 64),
		UserCode:    clip(req.UserCode, 16),
		RequesterIP: clientIP(r),
	}
	if err := s.enroll.CreateSession(req.SessionID, req.Challenge, meta); err != nil {
		slog.Error("create cli session failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// clientIP is the requester's address for display on the approval page. The
// client controls any X-Forwarded-For prefix it sends, so only proxy-appended
// values count: Fly-Client-IP (set by the Fly edge), else the LAST forwarded
// hop, else the socket address. Leftmost-XFF here would let an enrolling
// machine forge the exact recognition signal the page exists to show. The
// last-hop fallback is only trustworthy when a trusted proxy appends the
// final hop — on a bare deploy with no proxy, treat it as client-claimed.
// limiterIP keys the rate limiter. Unlike the display-oriented clientIP it
// never reads X-Forwarded-For: the client controls that header, and a spoofed
// value per request would mint a fresh bucket each time — bypassing the limit
// and growing the counts map at will.
func limiterIP(r *http.Request) string {
	if ip := r.Header.Get("Fly-Client-IP"); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func clientIP(r *http.Request) string {
	if ip := r.Header.Get("Fly-Client-IP"); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) handleCLISessionFetch(w http.ResponseWriter, r *http.Request) {
	if s.enroll == nil {
		http.Error(w, "enrollment not enabled", http.StatusNotFound)
		return
	}
	if !s.fetchLimiter.allow(limiterIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	// Verifier in the body, not the URL: a query-string secret leaks into the
	// control plane's HTTP access logs, defeating PKCE.
	var req struct {
		Verifier string `json:"verifier"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Verifier == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	sess, status, err := s.enroll.FetchSession(r.PathValue("id"), req.Verifier)
	if err != nil {
		slog.Error("fetch cli session failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	switch status {
	case enroll.Ready:
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(struct {
			Token string   `json:"token"`
			Orgs  []string `json:"orgs"`
			Name  string   `json:"agent_name"`
		}{sess.Token, sess.Orgs, sess.Name}); err != nil {
			slog.Warn("encode session failed", "err", err)
		}
	case enroll.Pending:
		w.WriteHeader(http.StatusAccepted)
	default:
		http.Error(w, "session expired", http.StatusGone)
	}
}

func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req PollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	orgs, ok := s.tenantFor(w, r, name)
	if !ok {
		return
	}
	// Scope an enrolled agent to its token's orgs: drop any reported queue whose
	// org the token can't serve, so a token for account A can't be dispatched
	// account B's work. A static (unscoped) token has no orgs and keeps all.
	if len(orgs) > 0 {
		req.Queues = queuesForOrgs(req.Queues, orgs)
	}
	s.heartbeat(livenessKey(orgs, name))
	s.recordReported(livenessKey(orgs, name), req.Queues)
	// Queue depth is recorded before the free-capacity check below: a blocked
	// agent's report is dropped for scheduling, but it is the only evidence that
	// work exists on a host that can't take it, which is what makes a fleet-wide
	// stall visible rather than silent.
	s.telemetry.AgentSeen(telemetry.Beat{
		Key: livenessKey(orgs, name), Name: name, Orgs: orgs, Labels: req.Labels,
		Capacity: req.Capacity, Busy: len(req.Busy), QueueDepth: queueDepth(req.Queues),
		DiskFree: req.DiskFreeBytes, DiskTotal: req.DiskTotalBytes, Blocked: req.Blocked,
		UpdateProtocol: req.UpdateProtocol, Version: req.Version, UpdateState: req.UpdateState, UpdateError: req.UpdateError,
	})
	// One snapshot per poll, shared by the sweep and the pick: on the Postgres
	// store each Snapshot is a full-table read, so a second scan is pure waste.
	live := liveJobs(s.sweep(s.ledger.Snapshot()))

	if req.UpdateProtocol == 2 {
		target, err := s.telemetry.AgentUpdateTarget(r.Context(), livenessKey(orgs, name))
		if err != nil {
			http.Error(w, "update target unavailable", http.StatusServiceUnavailable)
			return
		}
		if version.Newer(target.Version, req.Version) && (req.FailedVersion != target.Version || req.FailedRequest != target.Request) {
			w.Header().Set("X-Ushr-Agent-Version", target.Version)
			w.Header().Set("X-Ushr-Agent-Request", target.Request)
			s.hold(w, r)
			return
		}
	}
	if req.FreeCapacity() <= 0 {
		s.hold(w, r)
		return
	}

	// Try the best runnable job; if another agent raced us to it (ErrDuplicate),
	// mark it live and try the next best rather than holding for a full poll
	// window while the agent's other jobs wait. Bounded so a flood of racing
	// reports can't spin the offer path.
	for attempt := 0; attempt < maxOfferAttempts; attempt++ {
		best, waited, ok := s.pick(name, orgs, req, live)
		if !ok {
			break
		}
		err := s.ledger.Offer(best)
		// A failed WAL append still leaves the offer live, so it counts too.
		if !errors.Is(err, dispatch.ErrDuplicate) {
			s.metrics.Offers.Inc()
			s.metrics.DispatchLatency.Observe(float64(waited))
		}
		if err == nil {
			s.telemetry.Event(best.Pending.Org, best.ID, "offered",
				map[string]any{"agent": best.Agent, "job_id": best.Pending.JobID})
			writeOffer(w, best)
			return
		}
		if errors.Is(err, dispatch.ErrDuplicate) {
			live[jobKey(best.Pending.Org, best.Pending.JobID)] = true
			continue
		}
		// WAL append failed but the record is live — degraded persistence must
		// not abort dispatch, or the job double-dispatches when the offer expires.
		slog.Warn("ledger offer append failed", "id", best.ID, "err", err)
		writeOffer(w, best)
		return
	}
	// Nothing to dispatch: hold the connection for the poll window as a
	// heartbeat instead of bouncing, so a full-capacity spin can't happen.
	s.hold(w, r)
}

func writeOffer(w http.ResponseWriter, best dispatch.Record) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(Offer{
		ID:     best.ID,
		Org:    best.Pending.Org,
		JobID:  best.Pending.JobID,
		Labels: best.Pending.Labels,
	}); err != nil {
		slog.Warn("encode offer failed", "err", err)
	}
}

// pick chooses the highest-priority reported job this agent can run that isn't
// in the live set (already offered/claimed, or raced this poll). Score is the
// org's advisory priority plus aging from wait time; ties break toward the
// longer-waiting job. orgs is the token's tenant set (empty for the unscoped
// static token), used to namespace the record's liveness identity. It also
// returns the chosen job's clamped wait in seconds.
func (s *Server) pick(name string, orgs []string, req PollRequest, live map[string]bool) (dispatch.Record, int, bool) {
	var best dispatch.Record
	var bestJob QueuedJob
	bestWait := 0
	bestScore := math.MinInt
	found := false
	for _, q := range req.Queues {
		for _, j := range q.Jobs {
			if live[jobKey(q.Org, j.JobID)] {
				continue
			}
			if !domain.LabelsSatisfied(j.Labels, req.Labels) {
				continue
			}
			// Clamp negative waits: WaitingSecs is agent-computed from GitHub's
			// created_at against the agent's local clock, so skew can make it
			// negative, which would otherwise score a job below its base priority.
			waited := max(j.WaitingSecs, 0)
			score := q.Priority + (waited/60)*s.boostPerMinute
			better := !found || score > bestScore || (score == bestScore && j.WaitingSecs > bestJob.WaitingSecs)
			if !better {
				continue
			}
			found, bestScore, bestJob, bestWait = true, score, j, waited
			best = dispatch.Record{
				ID:        domain.RunnerName(name),
				Agent:     livenessKey(orgs, name),
				Pending:   domain.Job{Org: q.Org, JobID: j.JobID, Labels: j.Labels},
				OfferedAt: s.now(),
			}
		}
	}
	return best, bestWait, found
}

// liveJobs is the set of (org, job_id) already offered or claimed, so a
// re-reported job isn't dispatched twice.
func liveJobs(snapshot []dispatch.Record) map[string]bool {
	out := make(map[string]bool, len(snapshot))
	for _, rec := range snapshot {
		out[jobKey(rec.Pending.Org, rec.Pending.JobID)] = true
	}
	return out
}

// jobKey dedups a job across agents. It lowercases org because the agent
// reports the operator-typed casing: two same-tenant agents configured with
// different casing must not each get the same job dispatched (double-provision).
func jobKey(org string, jobID int64) string {
	return fmt.Sprintf("%s/%d", strings.ToLower(org), jobID)
}

// queuesForOrgs keeps only the reported queues whose org the token may serve.
// GitHub org logins are case-insensitive, and the token carries GitHub's
// canonical casing while the agent reports whatever the operator typed, so
// compare case-insensitively or a casing mismatch silently drops the work.
func queuesForOrgs(queues []OrgQueue, orgs []string) []OrgQueue {
	out := queues[:0]
	for _, q := range queues {
		if orgMatches(orgs, q.Org) {
			out = append(out, q)
		}
	}
	return out
}

// orgMatches reports whether org is in the case-insensitive set.
func orgMatches(orgs []string, org string) bool {
	for _, o := range orgs {
		if strings.EqualFold(o, org) {
			return true
		}
	}
	return false
}

func (s *Server) hold(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-time.After(PollTimeout):
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	id := r.PathValue("id")
	orgs, ok := s.tenantFor(w, r, name)
	if !ok {
		return
	}
	s.heartbeat(livenessKey(orgs, name))

	rec, ok, err := s.ledger.Claim(id, s.now(), orgs)
	if err != nil {
		slog.Warn("ledger claim append failed", "id", id, "err", err)
	}
	if !ok {
		// Expired, resolved, never offered, or another tenant's dispatch. The
		// agent drops the offer and re-reports the still-queued job next poll.
		http.Error(w, "offer gone", http.StatusGone)
		return
	}
	s.metrics.Claims.Inc()
	s.telemetry.Event(rec.Pending.Org, id, "claimed",
		map[string]any{"agent": rec.Agent, "job_id": rec.Pending.JobID})
	// No mint here — the agent holds the key and mints the JIT itself.
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDone(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	handle := r.PathValue("handle")
	var req DoneRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	orgs, ok := s.tenantFor(w, r, name)
	if !ok {
		return
	}
	s.heartbeat(livenessKey(orgs, name))

	// Resolve clears the ledger entry, scoped to the token's tenant so one
	// tenant can't resolve another's dispatch by its guessable handle. A
	// non-"done" status needs no re-enqueue: the job is still queued on GitHub,
	// so the agent re-reports it next poll.
	rec, resolved, err := s.ledger.Resolve(handle, req.Status, orgs)
	if err != nil {
		slog.Warn("ledger resolve append failed", "id", handle, "err", err)
	}
	if resolved {
		s.recordResolved(rec, req.Status)
	}
	slog.Info("dispatch resolved", "agent", name, "handle", handle, "status", req.Status, "err", req.Error)
	w.WriteHeader(http.StatusOK)
}

// tenantFor returns the orgs the token may serve for a request against agent
// {name}, rejecting an enrolled token whose bound name isn't the one in the
// path. The unscoped static token returns no orgs (no tenant restriction).
func (s *Server) tenantFor(w http.ResponseWriter, r *http.Request, name string) ([]string, bool) {
	id, scoped := r.Context().Value(identityKey{}).(agentIdentity)
	if scoped && id.name != name {
		http.Error(w, "token is not for this agent", http.StatusForbidden)
		return nil, false
	}
	return id.orgs, true
}

func (s *Server) heartbeat(key string) {
	s.mu.Lock()
	s.lastSeen[key] = s.now()
	s.mu.Unlock()
}

func (s *Server) recordReported(key string, queues []OrgQueue) {
	keys := make([]string, 0, queueDepth(queues))
	for _, q := range queues {
		for _, j := range q.Jobs {
			keys = append(keys, jobKey(q.Org, j.JobID))
		}
	}
	s.mu.Lock()
	s.reported[key] = keys
	s.mu.Unlock()
}

// activeAgents reads the deadline itself: the sweep that prunes lastSeen runs
// only on a poll, so a fleet that stops polling would otherwise read as live.
func (s *Server) activeAgents() float64 {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, seen := range s.lastSeen {
		if now.Sub(seen) <= agentDeadAfter {
			n++
		}
	}
	return float64(n)
}

// queuedJobs counts each job once even when several agents of one org report
// it, and skips agents past the liveness deadline.
func (s *Server) queuedJobs() float64 {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := make(map[string]struct{})
	for agent, keys := range s.reported {
		if now.Sub(s.lastSeen[agent]) > agentDeadAfter {
			continue
		}
		for _, k := range keys {
			jobs[k] = struct{}{}
		}
	}
	return float64(len(jobs))
}

// recordResolved emits the terminal-telemetry pair — history row + live event
// — so every terminal path (done, lost, offer-expired) stays in lockstep.
func (s *Server) recordResolved(rec dispatch.Record, status string) {
	s.telemetry.JobResolved(rec, status)
	s.telemetry.Event(rec.Pending.Org, rec.ID, "resolved",
		map[string]any{"agent": rec.Agent, "job_id": rec.Pending.JobID, "status": status})
}

// sweep expires stale records from the given snapshot and returns the records
// that survive (used by pick for dedup, saving a second snapshot):
//   - offered past offerTTL: the offer response never reached the agent.
//   - claimed whose agent is dead: the runner may exist but nobody will report.
//
// Neither re-enqueues: the owning agent re-reports the still-queued job itself.
func (s *Server) sweep(snapshot []dispatch.Record) []dispatch.Record {
	now := s.now()
	survivors := make([]dispatch.Record, 0, len(snapshot))
	// retry holds agents whose dead-claim Resolve failed this pass: their record
	// survives and their liveness must NOT be pruned, so the next sweep retries
	// instead of orphaning the still-claimed record.
	retry := make(map[string]bool)
	for _, rec := range snapshot {
		switch rec.State {
		case dispatch.StateOffered:
			if now.Sub(rec.OfferedAt) > offerTTL {
				// State-guarded: if the agent claimed it since the snapshot, the
				// expiry is a no-op and the claimed record survives.
				if s.ledger.ExpireOffer(rec.ID) {
					slog.Warn("offer expired", "id", rec.ID, "agent", rec.Agent)
					s.recordResolved(rec, "offer-expired")
					continue
				}
			}
		case dispatch.StateClaimed:
			s.mu.Lock()
			seen, polled := s.lastSeen[rec.Agent]
			s.mu.Unlock()
			if polled && now.Sub(seen) > agentDeadAfter {
				_, ok, err := s.ledger.Resolve(rec.ID, "lost", nil)
				if err != nil {
					// Transient store error: keep the record live (so it isn't
					// offered to another agent while still claimed) and protect
					// the agent's liveness so a later sweep can retry.
					slog.Warn("resolve of dead agent's claim failed, will retry", "id", rec.ID, "err", err)
					retry[rec.Agent] = true
					break
				}
				if ok {
					slog.Warn("claimed dispatch lost, agent dead", "id", rec.ID, "agent", rec.Agent)
					s.recordResolved(rec, "lost")
				}
				continue
			}
		}
		survivors = append(survivors, rec)
	}
	// Prune liveness for agents gone longer than agentDeadAfter, except those a
	// failed Resolve left with a still-claimed record. A returning agent re-adds
	// its entry on the next poll. Bounds lastSeen on the always-on plane.
	s.mu.Lock()
	var lost []string
	for k, seen := range s.lastSeen {
		if now.Sub(seen) > agentDeadAfter && !retry[k] {
			delete(s.lastSeen, k)
			delete(s.reported, k)
			lost = append(lost, k)
		}
	}
	s.mu.Unlock()
	s.metrics.AgentsLost.Add(float64(len(lost)))
	for _, k := range lost {
		// The liveness key is "{org}/{name}" for enrolled agents, where org may
		// itself be an "owner/repo" scope — the name is after the LAST slash.
		var org string
		if i := strings.LastIndex(k, "/"); i >= 0 {
			org = k[:i]
		}
		s.telemetry.Event(org, "", "agent-lost", map[string]any{"agent": k})
	}
	return survivors
}
