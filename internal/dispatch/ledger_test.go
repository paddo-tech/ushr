package dispatch

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paddo-tech/ushr/internal/domain"
)

func rec(id, agent string, jobID int64) Record {
	return Record{
		ID:        id,
		Agent:     agent,
		Pending:   domain.Job{Org: "acme", JobID: jobID, Labels: []string{"macos"}},
		OfferedAt: time.Now().UTC().Truncate(time.Second),
	}
}

func TestLifecycleInMemory(t *testing.T) {
	l, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Offer(rec("ushr-a-42", "mac1", 42)); err != nil {
		t.Fatal(err)
	}
	if err := l.Offer(rec("ushr-a-42", "mac1", 42)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("double offer: want ErrDuplicate, got %v", err)
	}
	// Same job re-offered under a different ID (another agent) must also dedup.
	if err := l.Offer(rec("ushr-b-42", "mac2", 42)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("same job, new ID: want ErrDuplicate, got %v", err)
	}
	r, ok, err := l.Claim("ushr-a-42", time.Now(), nil)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if r.State != StateClaimed || r.Pending.Org != "acme" {
		t.Fatalf("claimed record wrong: %+v", r)
	}
	if _, ok, _ := l.Claim("ushr-a-42", time.Now(), nil); ok {
		t.Fatal("double claim should not be ok")
	}
	if _, ok, _ := l.Resolve("ushr-a-42", "done", nil); !ok {
		t.Fatal("resolve should find record")
	}
	if got := len(l.Snapshot()); got != 0 {
		t.Fatalf("snapshot after resolve: %d records", got)
	}
}

func TestReplayDropsOfferedKeepsClaimed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Offer(rec("offered-only", "mac1", 41)); err != nil {
		t.Fatal(err)
	}
	if err := l.Offer(rec("claimed", "mac1", 42)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := l.Claim("claimed", time.Now(), nil); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := l.Offer(rec("resolved", "mac1", 43)); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l.Claim("resolved", time.Now(), nil); !ok {
		t.Fatal("claim resolved")
	}
	if _, ok, _ := l.Resolve("resolved", "done", nil); !ok {
		t.Fatal("resolve")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	snap := l2.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 survivor, got %d: %+v", len(snap), snap)
	}
	got := snap[0]
	if got.ID != "claimed" || got.State != StateClaimed {
		t.Fatalf("survivor wrong: %+v", got)
	}
	if got.Pending.JobID != 42 || got.Pending.Org != "acme" {
		t.Fatalf("pending not round-tripped: %+v", got.Pending)
	}

	// Compaction: a third open still sees exactly one record.
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	l3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(l3.Snapshot()) != 1 {
		t.Fatal("compacted replay lost the claimed record")
	}
}

func TestExpireOfferIsStateGuarded(t *testing.T) {
	l, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Offer(rec("ushr-a-7", "mac1", 7)); err != nil {
		t.Fatal(err)
	}
	// A claimed record must not be expirable — mirrors a claim landing between a
	// sweep's snapshot and its expiry call.
	if _, ok, _ := l.Claim("ushr-a-7", time.Now(), nil); !ok {
		t.Fatal("claim")
	}
	if l.ExpireOffer("ushr-a-7") {
		t.Fatal("ExpireOffer must not remove a claimed record")
	}
	if got := len(l.Snapshot()); got != 1 {
		t.Fatalf("claimed record should survive, len=%d", got)
	}
	// An offered record is expirable.
	if err := l.Offer(rec("ushr-a-8", "mac1", 8)); err != nil {
		t.Fatal(err)
	}
	if !l.ExpireOffer("ushr-a-8") {
		t.Fatal("ExpireOffer should remove an offered record")
	}
}

func TestReplayToleratesTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Offer(rec("claimed", "mac1", 42)); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l.Claim("claimed", time.Now(), nil); !ok {
		t.Fatal("claim")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: partial JSON on the last line.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"op":"resolve","id":"cl`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(path)
	if err != nil {
		t.Fatalf("torn tail must not fail Open: %v", err)
	}
	snap := l2.Snapshot()
	if len(snap) != 1 || snap[0].ID != "claimed" {
		t.Fatalf("records before the torn line should survive: %+v", snap)
	}
}
