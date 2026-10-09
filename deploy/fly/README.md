# Deploy the control plane to Fly (cp.ushr.io)

The hosted, **keyless** control plane: it schedules on agent-reported metadata
and never holds a GitHub key. Always-on in `iad`, Neon-backed dispatch store.

## One-time

```bash
fly auth login
fly apps create ushr-cp                      # or edit `app` in fly.toml

# Secrets (not baked into the image):
fly secrets set USHR_DISPATCH_DSN="postgres://…neon…/ushr?sslmode=require" -a ushr-cp
```

The Neon DSN enables per-agent **enrollment**, which satisfies the non-loopback
auth requirement — so leave `USHR_TOKEN` **unset** here. Do not set a static
token alongside enrollment: it is an unscoped cross-tenant master key, and the
control plane refuses to start with both (see `RequireScopedAuth`). Agents
authenticate by running `ushr login`, not a shared token.

## Deploy

```bash
fly deploy -c deploy/fly/fly.toml --dockerfile deploy/fly/Dockerfile
```

Health: `https://ushr-cp.fly.dev/healthz` → 200. Point DNS `cp.ushr.io` at the
app (`fly certs add cp.ushr.io`).

## Roll back past the `started` dispatch state

A control plane with the `started` state keeps a started row and a new row for
the same job. An older control plane rebuilds a full unique index on
`(lower(org), job_id)` at boot, and it fails to start if such pairs exist. It
also does not know the `started` state. Run this against the Neon database
before you deploy the older image:

```sql
BEGIN;
-- A started row that shares its job with another row: drop the started one.
-- Its runner still finishes; the done report for it becomes a no-op.
DELETE FROM dispatches s
WHERE s.state = 'started'
  AND EXISTS (SELECT 1 FROM dispatches o
              WHERE o.id <> s.id AND lower(o.org) = lower(s.org) AND o.job_id = s.job_id);
UPDATE dispatches SET state = 'claimed' WHERE state = 'started';
COMMIT;
```

Deploy the older image straight after, so no new started rows appear between
the two steps.

## Point the Linux-box agent at it

Enroll the box from its terminal:

```bash
ushr login --control-plane https://cp.ushr.io
```

This writes `~/.config/ushr/credentials.yaml` (controller URL, per-agent token,
agent name). Restart the agent (`systemctl --user restart ushr-agent`). It polls GitHub with
its own key, reports queues to cp.ushr.io, mints JIT locally, and provisions —
the control plane never sees a credential.

## Neon

Any Postgres works; Neon is the chosen host (owner role, same region `iad`). The
store creates its `dispatches` table on first connect — no migration step. To
verify state: `select id, agent, state, org, job_id from dispatches;`.
