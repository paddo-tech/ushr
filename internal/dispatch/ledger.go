// Package dispatch is the controller's persistent record of in-flight
// dispatches. Every offer, claim, and terminal report appends an event to a
// JSONL write-ahead log, so a controller restart knows which dispatches were
// outstanding instead of losing all state (the v0.1 failure mode).
//
// Lifecycle: offered -> claimed -> [started] -> resolved (done/failed/lost/expired).
// Offered records do not survive a restart: no credential was minted, the job
// is still queued on GitHub, and the poller will re-emit it. Claimed records
// do survive: a runner may exist, so the server's sweeps decide their fate.
package dispatch

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paddo-tech/ushr/internal/domain"
)

// ErrDuplicate is returned by Offer when the job already has a live dispatch.
// Callers must distinguish it from WAL I/O errors: a duplicate means the offer
// was rejected, any other error means the record is live but unpersisted.
var ErrDuplicate = errors.New("dispatch already live")

// States a live record can be in.
const (
	StateOffered = "offered"
	StateClaimed = "claimed"
	// StateStarted is a claimed dispatch whose runner has taken a job. GitHub
	// gives a JIT runner any queued job with matching labels, so the offered
	// job may still be queued: a started record holds its slot but no longer
	// holds that job.
	StateStarted = "started"
)

// Record is one in-flight dispatch. ID is the runner name (fixed at offer
// time, before any mint) and doubles as the agent's slot handle.
type Record struct {
	ID        string     `json:"id"`
	Agent     string     `json:"agent"`
	State     string     `json:"state"`
	Pending   domain.Job `json:"pending"`
	OfferedAt time.Time  `json:"offered_at"`
	ClaimedAt time.Time  `json:"claimed_at,omitzero"`
}

// event is one WAL line.
type event struct {
	Op     string    `json:"op"` // offer | claim | start | resolve
	At     time.Time `json:"at"`
	Record *Record   `json:"record,omitempty"` // offer only
	ID     string    `json:"id,omitempty"`     // claim/start/resolve
	Status string    `json:"status,omitempty"` // resolve only
}

// Ledger tracks live dispatch records, optionally backed by a JSONL WAL.
type Ledger struct {
	mu   sync.Mutex
	live map[string]Record
	f    *os.File // nil when in-memory only
}

// DefaultPath is where the WAL lives when config doesn't override it.
func DefaultPath() string {
	return filepath.Join(os.Getenv("HOME"), ".local", "share", "ushr", "dispatches.jsonl")
}

// Open loads the ledger at path, replaying the WAL and compacting it down to
// the surviving records. Offered records are dropped during replay (see the
// package comment). An empty path yields a purely in-memory ledger.
func Open(path string) (*Ledger, error) {
	l := &Ledger{live: make(map[string]Record)}
	if path == "" {
		return l, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir ledger dir: %w", err)
	}
	if err := l.replay(path); err != nil {
		return nil, err
	}
	for id, r := range l.live {
		if r.State == StateOffered {
			delete(l.live, id)
		}
	}
	// Compact: rewrite only the survivors, then append from there. One offer
	// event per survivor is enough — its Record serializes State and ClaimedAt,
	// so replay restores the claimed state without a follow-up claim event.
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("compact ledger: %w", err)
	}
	for _, r := range l.live {
		rec := r
		if err := writeEvent(f, event{Op: "offer", At: r.OfferedAt, Record: &rec}); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, fmt.Errorf("compact ledger: %w", err)
	}
	if l.f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	return l, nil
}

func (l *Ledger) replay(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			// Events are appended without fsync, so a crash mid-append leaves a
			// torn tail. Stop replaying rather than refuse to start; the compaction
			// pass in Open rewrites the file clean.
			slog.Warn("dispatch ledger line unparseable, truncating replay", "err", err)
			return nil
		}
		switch e.Op {
		case "offer":
			if e.Record != nil {
				l.live[e.Record.ID] = *e.Record
			}
		case "claim":
			if r, ok := l.live[e.ID]; ok {
				r.State = StateClaimed
				r.ClaimedAt = e.At
				l.live[e.ID] = r
			}
		case "start":
			if r, ok := l.live[e.ID]; ok {
				r.State = StateStarted
				l.live[e.ID] = r
			}
		case "resolve":
			delete(l.live, e.ID)
		}
	}
	return sc.Err()
}

func writeEvent(f *os.File, e event) error {
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append ledger: %w", err)
	}
	return nil
}

// append writes e to the WAL if one is open. WAL write failures are returned
// so callers can log them, but the in-memory state has already advanced: a
// full disk degrades persistence, not dispatching.
func (l *Ledger) append(e event) error {
	if l.f == nil {
		return nil
	}
	return writeEvent(l.f, e)
}

// Offer records a new offered dispatch. Returns ErrDuplicate if the ID is
// taken or the job already has an unstarted dispatch under another ID. IDs
// embed the agent name (poll source) or are random (scaleset), so a re-emitted
// job offered to a different agent would otherwise slip past an ID-only check
// and double-provision.
func (l *Ledger) Offer(r Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.live[r.ID]; exists {
		return fmt.Errorf("dispatch %q: %w", r.ID, ErrDuplicate)
	}
	for _, live := range l.live {
		// Case-insensitive on org: the agent reports operator-typed casing, so a
		// case-sensitive match would let differing casings double-dispatch a job.
		if live.State != StateStarted && strings.EqualFold(live.Pending.Org, r.Pending.Org) && live.Pending.JobID == r.Pending.JobID {
			return fmt.Errorf("job %d live as %q: %w", r.Pending.JobID, live.ID, ErrDuplicate)
		}
	}
	r.State = StateOffered
	l.live[r.ID] = r
	return l.append(event{Op: "offer", At: r.OfferedAt, Record: &r})
}

// orgMatches reports whether a tenant guard passes: an empty wantOrgs disables
// scoping; otherwise the record's org must be one of them, case-insensitively.
// The agent reports the operator-typed org casing while the token carries
// GitHub's canonical casing, so a case-sensitive check would livelock claims.
func orgMatches(wantOrgs []string, recordOrg string) bool {
	if len(wantOrgs) == 0 {
		return true
	}
	for _, o := range wantOrgs {
		if strings.EqualFold(o, recordOrg) {
			return true
		}
	}
	return false
}

// Claim transitions an offered record to claimed, returning it. ok is false
// if the record is unknown, not in the offered state (double claim), or owned
// by another tenant (wantOrg set and mismatched).
func (l *Ledger) Claim(id string, at time.Time, wantOrgs []string) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.live[id]
	if !ok || r.State != StateOffered || !orgMatches(wantOrgs, r.Pending.Org) {
		return Record{}, false, nil
	}
	r.State = StateClaimed
	r.ClaimedAt = at
	l.live[id] = r
	return r, true, l.append(event{Op: "claim", At: at, ID: id})
}

// Start transitions a claimed record to started, returning it. ok is false if
// the record is unknown, not claimed, or owned by another agent or tenant.
func (l *Ledger) Start(id, agent string, wantOrgs []string) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.live[id]
	if !ok || r.State != StateClaimed || r.Agent != agent || !orgMatches(wantOrgs, r.Pending.Org) {
		return Record{}, false, nil
	}
	r.State = StateStarted
	l.live[id] = r
	return r, true, l.append(event{Op: "start", At: time.Now(), ID: id})
}

// Resolve removes a record in any live state, returning it. ok is false if the
// record is unknown (already resolved, or a report for a pre-restart offered
// record) or owned by another tenant (wantOrg set and mismatched).
func (l *Ledger) Resolve(id, status string, wantOrgs []string) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.live[id]
	if !ok || !orgMatches(wantOrgs, r.Pending.Org) {
		return Record{}, false, nil
	}
	delete(l.live, id)
	return r, true, l.append(event{Op: "resolve", At: time.Now(), ID: id, Status: status})
}

// ExpireOffer removes id only if it is still offered (see Store.ExpireOffer).
func (l *Ledger) ExpireOffer(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.live[id]
	if !ok || r.State != StateOffered {
		return false
	}
	delete(l.live, id)
	_ = l.append(event{Op: "resolve", At: time.Now(), ID: id, Status: "offer-expired"})
	return true
}

// Snapshot returns a copy of every live record.
func (l *Ledger) Snapshot() []Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Record, 0, len(l.live))
	for _, r := range l.live {
		out = append(out, r)
	}
	return out
}

// Close closes the WAL file, if any.
func (l *Ledger) Close() error {
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}
