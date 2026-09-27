# Agent enrollment (`ushr login`) — cross-repo contract

Replaces the shared bearer token with **per-agent enrollment tokens** obtained
through a browser-session + PKCE flow (the `fly auth login` model). Humans log in
to ushr.io (Neon Auth); the agent carries a token that login minted.

## Flow

```
CLI (ushr login)                 ushr.io (web, closed)          cp.ushr.io (Go, this repo)
  verifier = rand
  challenge = sha256(verifier)
  session   = rand
  POST /v1/cli/session ─────────────────────────────────────▶  create pending row
                                                                (session, challenge, exp)
  open browser: ushr.io/cli/auth?session=…  ──▶ Neon Auth login
                                               pick org + agent name, approve
                                               mint token; hash→agent_tokens;
                                               write token+org+name into cli_sessions
  GET /v1/cli/session/{session}?verifier ───────────────────▶  sha256(verifier)==challenge?
                                                                200 {token,org,agent_name}, consume
  write token+org → agent.yaml
```

The **verifier never touches the browser**, so a leaked session id (history,
referrer) can't retrieve the token. Sessions are single-use with a short TTL.

The confirmation code the CLI prints is **display-only** — the server never
checks it. Its entire value is the human comparing terminal vs page (a
forwarded phishing link shows a code the victim's terminal never printed); an
approver who ignores a mismatch is not protected by anything server-side.

## cp.ushr.io endpoints (this repo)

- `POST /v1/cli/session` — body `{session_id, challenge}` (`challenge` =
  hex(sha256(verifier))). Creates a pending session (TTL 10m). `201`.
  Unauthenticated: it is the pre-login handshake, gated by session entropy.
- `POST /v1/cli/session/{session_id}/fetch` — body `{verifier}` (in the body,
  not the URL, so the PKCE secret doesn't land in access logs). `202` pending,
  `410` expired/unknown, `200 {token, org, agent_name}` once approved (then
  consumed, `Cache-Control: no-store`).
- Agent requests (`/v1/agents/...`) authenticate with the enrolment token as
  `Authorization: Bearer`. The control plane validates `sha256(token)` against
  `agent_tokens`; the static `USHR_TOKEN` is still accepted (OSS single-host and
  the current trial).

## Neon schema (owned by hosted web app Drizzle; cp creates IF NOT EXISTS to match)

```sql
CREATE TABLE agent_tokens (
  token_hash text PRIMARY KEY,     -- sha256(token), hex
  org        text NOT NULL,
  name       text NOT NULL,        -- agent name
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz           -- null = active
);
CREATE TABLE cli_sessions (
  id         text PRIMARY KEY,
  challenge  text NOT NULL,        -- hex(sha256(verifier))
  token      text,                 -- minted token, null until approved; cleared on fetch
  org        text,
  name       text,
  expires_at timestamptz NOT NULL
);
```

Token at rest: `agent_tokens` stores only the hash. `cli_sessions.token` holds
the plaintext token transiently (short TTL, deleted on fetch, retrieval gated by
the PKCE verifier) — the standard session-park tradeoff.

## hosted web app half (closed repo, separate)

- `ushr.io/cli/auth` page: Neon Auth gate, org picker + agent-name field, approve
  action that mints a token, inserts `agent_tokens`, and updates the
  `cli_sessions` row (token/org/name).
- Own the two tables in the Drizzle schema (cp's `IF NOT EXISTS` aligns to it).

## Compatibility

OSS single-host keeps the static-token (or loopback-no-token) path — no accounts,
no Neon. Enrollment is the hosted/multi-tenant path, active only when the control
plane is Neon-backed.
