// Package pg is a Postgres-backed dispatch.Store for the hosted control plane.
// The OSS single-host controller uses the in-memory + JSONL *dispatch.Ledger;
// the hosted control plane points at Neon so dispatch state survives Fly
// machine restarts and the web app can read it. Same contract either way.
package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/paddo-tech/ushr/internal/dispatch"
	"github.com/paddo-tech/ushr/internal/domain"
)

// opTimeout bounds each query. The Store interface has no context, so each op
// gets its own short deadline off the background context.
const opTimeout = 5 * time.Second

const schema = `
CREATE TABLE IF NOT EXISTS dispatches (
	id          text PRIMARY KEY,
	agent       text NOT NULL,
	state       text NOT NULL,
	org         text NOT NULL,
	job_id      bigint NOT NULL,
	labels      text[] NOT NULL DEFAULT '{}',
	offered_at  timestamptz NOT NULL,
	claimed_at  timestamptz
);
-- One live dispatch per (org, job_id), matched case-insensitively: the agent
-- reports operator-typed org casing, so a case-sensitive key would let two
-- same-tenant agents with differing casing double-provision a job. Replaces the
-- older case-sensitive UNIQUE(org, job_id) constraint where present.
ALTER TABLE dispatches DROP CONSTRAINT IF EXISTS dispatches_org_job_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS dispatches_org_lower_job_id ON dispatches (lower(org), job_id);`

// Store implements dispatch.Store against Postgres. Resolving deletes the row,
// so every row is a live dispatch and the UNIQUE(org, job_id) constraint gives
// the same one-live-dispatch-per-job dedup as the JSONL ledger.
type Store struct {
	pool *pgxpool.Pool
}

var _ dispatch.Store = (*Store)(nil)

// Open connects to dsn, applies the schema, and returns the Store.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{pool: pool}, nil
}

// NewWithPool applies the schema on an existing pool and returns a Store sharing
// it (the caller owns the pool's lifetime — used to share one pool with enroll).
func NewWithPool(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

const cols = "id, agent, state, org, job_id, labels, offered_at, claimed_at"

// Offer inserts a new offered record. A duplicate id or (org, job_id) returns
// dispatch.ErrDuplicate so the server can distinguish it from an I/O error.
func (s *Store) Offer(r dispatch.Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO dispatches (id, agent, state, org, job_id, labels, offered_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		r.ID, r.Agent, dispatch.StateOffered, r.Pending.Org, r.Pending.JobID, r.Pending.Labels, r.OfferedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return fmt.Errorf("dispatch %q: %w", r.ID, dispatch.ErrDuplicate)
	}
	return err
}

// Claim transitions offered -> claimed. ok is false if the record is unknown,
// not offered (double claim), or owned by another tenant (org not in wantOrgs).
func (s *Store) Claim(id string, at time.Time, wantOrgs []string) (dispatch.Record, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	row := s.pool.QueryRow(ctx,
		`UPDATE dispatches SET state = $2, claimed_at = $3
		 WHERE id = $1 AND state = $4 AND (cardinality($5::text[]) = 0 OR lower(org) = ANY($5)) RETURNING `+cols,
		id, dispatch.StateClaimed, at, dispatch.StateOffered, lowered(wantOrgs))
	return scanOne(row)
}

// Resolve removes a live record in any state. ok is false if it is unknown
// (already resolved) or owned by another tenant (org not in wantOrgs). status
// is not persisted — the row is deleted.
func (s *Store) Resolve(id, _ string, wantOrgs []string) (dispatch.Record, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	row := s.pool.QueryRow(ctx,
		`DELETE FROM dispatches WHERE id = $1 AND (cardinality($2::text[]) = 0 OR lower(org) = ANY($2)) RETURNING `+cols,
		id, lowered(wantOrgs))
	return scanOne(row)
}

// lowered returns wantOrgs lowercased (a non-nil empty slice when empty, so it
// encodes as an empty array and cardinality(...) = 0 disables the scope check).
func lowered(wantOrgs []string) []string {
	out := make([]string, len(wantOrgs))
	for i, o := range wantOrgs {
		out[i] = strings.ToLower(o)
	}
	return out
}

// ExpireOffer deletes id only if it is still offered (see Store.ExpireOffer).
func (s *Store) ExpireOffer(id string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `DELETE FROM dispatches WHERE id = $1 AND state = $2`, id, dispatch.StateOffered)
	if err != nil {
		slog.Error("dispatch expire-offer failed", "id", id, "err", err)
		return false
	}
	return tag.RowsAffected() > 0
}

// Snapshot returns every live record. On a query error it logs and returns nil
// (the interface has no error channel); the sweep simply finds nothing to do.
func (s *Store) Snapshot() []dispatch.Record {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT `+cols+` FROM dispatches`)
	if err != nil {
		slog.Error("dispatch snapshot query failed", "err", err)
		return nil
	}
	defer rows.Close()
	var out []dispatch.Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			slog.Error("dispatch snapshot scan failed", "err", err)
			return nil
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		slog.Error("dispatch snapshot iteration failed", "err", err)
		return nil
	}
	return out
}

type scanner interface {
	Scan(dest ...any) error
}

// scanOne scans a single RETURNING row, mapping no-rows to ok=false.
func scanOne(row scanner) (dispatch.Record, bool, error) {
	rec, err := scanRecord(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return dispatch.Record{}, false, nil
	}
	if err != nil {
		return dispatch.Record{}, false, err
	}
	return rec, true, nil
}

func scanRecord(row scanner) (dispatch.Record, error) {
	var (
		rec       dispatch.Record
		org       string
		jobID     int64
		labels    []string
		claimedAt *time.Time
	)
	if err := row.Scan(&rec.ID, &rec.Agent, &rec.State, &org, &jobID, &labels, &rec.OfferedAt, &claimedAt); err != nil {
		return dispatch.Record{}, err
	}
	rec.Pending = domain.Job{Org: org, JobID: jobID, Labels: labels}
	if claimedAt != nil {
		rec.ClaimedAt = *claimedAt
	}
	return rec, nil
}
