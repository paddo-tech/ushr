package pg

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/domain"
)

// Set USHR_TEST_PG_DSN to a throwaway Postgres to exercise the store; skipped
// otherwise so CI without a DB stays green.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("USHR_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set USHR_TEST_PG_DSN to run the Postgres dispatch store tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), "TRUNCATE dispatches")
		s.Close()
	})
	_, _ = s.pool.Exec(ctx, "TRUNCATE dispatches")
	return s
}

func rec(id, org string, jobID int64) dispatch.Record {
	return dispatch.Record{
		ID:        id,
		Agent:     "agent-1",
		Pending:   domain.Job{Org: org, JobID: jobID, Labels: []string{"self-hosted"}},
		OfferedAt: time.Now().UTC().Truncate(time.Second),
	}
}

func TestLifecycle(t *testing.T) {
	s := testStore(t)
	if err := s.Offer(rec("ushr-a-7", "acme", 7)); err != nil {
		t.Fatal(err)
	}
	if err := s.Offer(rec("ushr-a-7", "acme", 7)); !errors.Is(err, dispatch.ErrDuplicate) {
		t.Fatalf("duplicate id: want ErrDuplicate, got %v", err)
	}
	if err := s.Offer(rec("ushr-b-7", "acme", 7)); !errors.Is(err, dispatch.ErrDuplicate) {
		t.Fatalf("same (org,job): want ErrDuplicate, got %v", err)
	}
	r, ok, err := s.Claim("ushr-a-7", time.Now(), nil)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if r.State != dispatch.StateClaimed || r.Pending.Org != "acme" || r.Pending.JobID != 7 {
		t.Fatalf("claimed record wrong: %+v", r)
	}
	if _, ok, _ := s.Claim("ushr-a-7", time.Now(), nil); ok {
		t.Fatal("double claim should not be ok")
	}
	if got := len(s.Snapshot()); got != 1 {
		t.Fatalf("snapshot: want 1 live, got %d", got)
	}
	if _, ok, _ := s.Resolve("ushr-a-7", "done", nil); !ok {
		t.Fatal("resolve should find record")
	}
	if got := len(s.Snapshot()); got != 0 {
		t.Fatalf("snapshot after resolve: want 0, got %d", got)
	}
}
