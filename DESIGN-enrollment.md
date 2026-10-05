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

Token at rest: `agent_tokens` stores only the hash. The CLI makes an ephemeral
X25519 key pair per login and sends the public key as `public_key` on session
create. The web app then writes `cli_sessions.token` sealed to that key
(`internal/enroll/seal.go`, `server/seal.ts`), and only the CLI can open it.
CLIs v0.2.10 and older send no key, so their row holds the plaintext token until
fetch or expiry.

## hosted web app half (closed repo, separate)

- `ushr.io/cli/auth` page: Neon Auth gate, org picker + agent-name field, approve
  action that mints a token, inserts `agent_tokens`, and updates the
  `cli_sessions` row (token/org/name).
- Own the two tables in the Drizzle schema (cp's `IF NOT EXISTS` aligns to it).

## Compatibility

OSS single-host keeps the static-token (or loopback-no-token) path — no accounts,
no Neon. Enrollment is the hosted/multi-tenant path, active only when the control
plane is Neon-backed.

## Hosted first-run setup

The CLI reuses a valid enrollment when the customer runs `ushr login` again.
The browser then guides GitHub App creation through `/setup/github`.
This flow works when the browser and runner use different machines.

The host creates a random setup ID and sends a verifier hash to `/api/runner-setup`.
The web app binds that request to the enrolled host and a verified GitHub scope.
A workspace administrator must approve the GitHub flow.
The callback checks its stored state before recording GitHub's temporary exchange code.
The host sends a per-setup X25519 public key, and the web app seals the code to it.
Setups from CLIs v0.2.10 and older send no key, and their code stays plaintext.
The host authenticates and presents its verifier to retrieve that code.
Only the host exchanges the code for its private key.
The control plane receives the webhook secret, but never the private key.

The controller migration owns `runner_apps`.
Each app receives a separate `/webhook/{id}` endpoint and secret.
The receiver checks the signature, active enrollment, verified scope, and runner name.
Revoking enrollment disables that app's telemetry access.
Token renewal preserves the host's webhook connection within its workspace.

The CLI saves setup progress locally with mode `0600`.
A failed webhook registration leaves the saved key available for retry.
The CLI verifies GitHub installation before starting the agent.
The dashboard requires a heartbeat before showing machine readiness.
It requires a successful GitHub result before showing onboarding completion.

Deploy the controller migration before the web changes.
Deploy the web changes before releasing the CLI.
Verify signup, remote setup, retries, and the first workflow using a new workspace before public release.
