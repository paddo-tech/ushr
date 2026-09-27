package enroll

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const opTimeout = 5 * time.Second

// migrations run once each, in order, recorded in enroll_schema_migrations.
// Append-only: never edit an entry that has shaped the live database. All
// entries share ONE transaction, so statements that can't run in a tx block
// (CREATE INDEX CONCURRENTLY, VACUUM) are off-limits here.
var migrations = []string{schemaV1}

const schemaV1 = `
CREATE TABLE IF NOT EXISTS agent_tokens (
	token_hash     text PRIMARY KEY,
	orgs           text[] NOT NULL DEFAULT '{}',
	account_org_id text,
	name           text NOT NULL,
	created_at     timestamptz NOT NULL DEFAULT now(),
	revoked_at     timestamptz
);
CREATE TABLE IF NOT EXISTS cli_sessions (
	id           text PRIMARY KEY,
	challenge    text NOT NULL,
	token        text,
	orgs         text[],
	name         text,
	agent_name   text,
	requester_ip text,
	user_code    text,
	created_at   timestamptz NOT NULL DEFAULT now(),
	expires_at   timestamptz NOT NULL
);
ALTER TABLE cli_sessions ADD COLUMN IF NOT EXISTS agent_name text;
ALTER TABLE cli_sessions ADD COLUMN IF NOT EXISTS requester_ip text;
ALTER TABLE cli_sessions ADD COLUMN IF NOT EXISTS user_code text;
ALTER TABLE cli_sessions ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
-- Token provenance: who approved the enrollment and from where.
ALTER TABLE agent_tokens ADD COLUMN IF NOT EXISTS approved_by text;
ALTER TABLE agent_tokens ADD COLUMN IF NOT EXISTS approved_ip text;
-- Session create is unauthenticated and prunes on every insert; without this
-- index a create-flood makes each prune an O(n) seq scan over its own spam.
CREATE INDEX IF NOT EXISTS cli_sessions_expires ON cli_sessions (expires_at);
-- Migrate an older single-org shape (org text) to the multi-org columns.
ALTER TABLE agent_tokens ADD COLUMN IF NOT EXISTS orgs text[] NOT NULL DEFAULT '{}';
ALTER TABLE agent_tokens ADD COLUMN IF NOT EXISTS account_org_id text;
ALTER TABLE cli_sessions ADD COLUMN IF NOT EXISTS orgs text[];
DO $$ BEGIN
	IF EXISTS (SELECT FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'agent_tokens' AND column_name = 'org') THEN
		UPDATE agent_tokens SET orgs = ARRAY[org] WHERE cardinality(orgs) = 0 AND org IS NOT NULL;
	END IF;
END $$;
ALTER TABLE agent_tokens DROP COLUMN IF EXISTS org;
ALTER TABLE cli_sessions DROP COLUMN IF EXISTS org;
-- Every token must belong to an account: a null-account row would be invisible
-- to the owner's dashboard and immune to the re-enroll revoke sweep (NULL never
-- matches the account-scoped queries) while still authenticating. Tokens from
-- before hosted enrollment have no account and cannot be scoped — drop them so
-- the constraint can hold; their agents re-enroll.
DELETE FROM agent_tokens WHERE account_org_id IS NULL;
ALTER TABLE agent_tokens ALTER COLUMN account_org_id SET NOT NULL;
-- At most one live token per (account, agent name): re-enrolling an agent (the
-- expected way to change its org set) rotates its token instead of leaving the
-- prior one valid. The mint flow revokes the old row in the same transaction.
CREATE UNIQUE INDEX IF NOT EXISTS agent_tokens_active_account_name
	ON agent_tokens (account_org_id, name) WHERE revoked_at IS NULL;`

// PG is the Neon-backed enroll.Store. The hosted web app web app writes agent_tokens
// (mint) and approves cli_sessions; this reads/validates and serves the CLI poll.
// It shares the control plane's pgxpool (see NewWithPool).
type PG struct {
	pool *pgxpool.Pool
}

var _ Store = (*PG)(nil)

// NewWithPool applies any unapplied enrollment migrations on an existing pool
// and returns a store sharing it (the control plane's dispatch store owns the
// pool's lifetime). An advisory lock serializes concurrent boots.
func NewWithPool(ctx context.Context, pool *pgxpool.Pool) (*PG, error) {
	if err := migrate(ctx, pool); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &PG{pool: pool}, nil
}

// enrollMigrateLockKey is an arbitrary constant namespacing this package's
// advisory lock against other users of the shared database.
const enrollMigrateLockKey = 0x75736872 // "ushr"

// migrate runs everything in ONE transaction under pg_advisory_xact_lock: a
// session-level lock spread over autocommit statements is a no-op behind a
// transaction-pooling proxy (each statement may hit a different backend), and
// the xact lock auto-releases on commit/rollback so cancellation can't leak it.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, enrollMigrateLockKey); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS enroll_schema_migrations (
		version    int PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	var applied int
	if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM enroll_schema_migrations`).Scan(&applied); err != nil {
		return err
	}
	for i := applied; i < len(migrations); i++ {
		version := i + 1
		if _, err := tx.Exec(ctx, migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO enroll_schema_migrations (version) VALUES ($1)`, version); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if applied < len(migrations) {
		slog.Info("enroll migrations applied", "from", applied+1, "through", len(migrations))
	}
	return nil
}

// ValidateToken looks up an active enrollment token by its hash. A DB error
// fails closed (ok=false) so a blip rejects rather than admits.
func (p *PG) ValidateToken(token string) ([]string, string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	var orgs []string
	var name string
	err := p.pool.QueryRow(ctx,
		`SELECT orgs, name FROM agent_tokens WHERE token_hash = $1 AND revoked_at IS NULL`,
		HashToken(token)).Scan(&orgs, &name)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Error("validate token failed", "err", err)
		}
		return nil, "", false
	}
	return orgs, name, true
}

// CreateSession records a pending login session and prunes expired ones.
func (p *PG) CreateSession(id, challenge string, meta SessionMeta) error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if _, err := p.pool.Exec(ctx, `DELETE FROM cli_sessions WHERE expires_at < now()`); err != nil {
		slog.Warn("prune cli_sessions failed", "err", err)
	}
	_, err := p.pool.Exec(ctx,
		`INSERT INTO cli_sessions (id, challenge, agent_name, requester_ip, user_code, expires_at)
		 VALUES ($1, $2, nullif($3, ''), nullif($4, ''), nullif($5, ''), now() + interval '10 minutes')
		 ON CONFLICT (id) DO NOTHING`,
		id, challenge, meta.AgentName, meta.RequesterIP, meta.UserCode)
	return err
}

// FetchSession consumes an approved session and returns its token. The delete
// and read are one statement, so a session is single-use: two racing fetches
// can't both receive the token. A wrong verifier fails the challenge match and
// reads as expired — no oracle for pending vs unknown.
func (p *PG) FetchSession(id, verifier string) (Session, SessionStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	challenge := Challenge(verifier)
	var token, name string
	var orgs []string
	err := p.pool.QueryRow(ctx,
		`DELETE FROM cli_sessions
		 WHERE id = $1 AND challenge = $2 AND token IS NOT NULL AND expires_at > now()
		 RETURNING token, orgs, name`,
		id, challenge).Scan(&token, &orgs, &name)
	if err == nil {
		return Session{Token: token, Orgs: orgs, Name: name}, Ready, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Session{}, Expired, err
	}
	// Nothing consumed: distinguish an unapproved-but-live session (keep polling)
	// from an unknown/expired one, without deleting either.
	var pending bool
	err = p.pool.QueryRow(ctx,
		`SELECT true FROM cli_sessions
		 WHERE id = $1 AND challenge = $2 AND token IS NULL AND expires_at > now()`,
		id, challenge).Scan(&pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, Expired, nil
	}
	if err != nil {
		return Session{}, Expired, err
	}
	return Session{}, Pending, nil
}
