# Model B — M1 keyless control plane (build spec)

Implementation spec for the first trialable slice of the hosted control plane.
Goal: run the **agent** on the Linux box holding the GitHub App key, and a **keyless
control plane** it dials — the control plane schedules on reported metadata and never
sees a credential. Trial target: cp.ushr.io on Fly `iad` + Neon.

## Scope

**In M1:** move GitHub poll + App keys + JIT mint from controller to agent; invert the
protocol so the agent reports queue metadata and mints locally; control plane becomes a
scheduler over reports; keep the persistent dispatch ledger, dedup, and liveness from
#22/#24.

**Deferred:** dashboard, cost/chargeback UI, Stripe (step 5); per-agent enrollment tokens
(M3 — bearer token until then); multi-tenant isolation/RLS (single logical tenant for the
trial); repo split (stays in-tree). Neon-backed ledger is M2; Fly deploy is M3.

## Config split

Orgs, App keys, and the source move from the controller to the agent.

**agent.yaml gains** (from controller.yaml):
```yaml
orgs:
  - name: paddo-tech
    app_id: 0
    private_key_path: ~/.secrets/paddo-tech-app.pem
    priority: 100          # advisory; the control plane owns final priority
source:
  type: poll               # poll | scaleset — now runs on the agent
  interval: 30s
```

**controller.yaml keeps** listen/token, `policy` (aging), `dispatch_ledger` (or Neon DSN in
M2). It **loses** `orgs` and `source`: the control plane no longer authenticates to GitHub.

## Protocol

Reuses the two-phase dispatch shape (#22); the change is *what crosses the wire* and *who
mints*. Endpoints stay `POST /v1/agents/{name}/{poll,dispatches/{id}/claim,slots/{h}/done}`.

**1. REPORT** — `POST .../poll` (long-poll; also the heartbeat). Body carries the agent's
own queue, derived from its GitHub polling:
```json
{
  "capacity": 16,
  "busy": ["ushr-linux-runner-1-7"],
  "queues": [
    { "org": "paddo-tech",
      "jobs": [{ "job_id": 7, "labels": ["self-hosted","Linux","X64"], "waiting_secs": 42 }] }
  ]
}
```

**2. PENDING** — long-poll response. The control plane merges every agent's reported queues,
applies priority + aging, and returns the one job this agent should claim (no JIT, no keys):
```json
{ "dispatch_id": "ushr-linux-runner-1-7", "org": "paddo-tech", "job_id": 7,
  "labels": ["self-hosted","Linux","X64"] }
```
`204` when nothing is scheduled for this agent this window.

**3. CLAIM** — `POST .../dispatches/{id}/claim`. Control plane records the dispatch in the
ledger (dedup by `(org, job_id)`, per #24) and returns the decision echo — **no JITConfig**.
`410 Gone` if expired/unknown.

**4. (local)** — the agent mints the JIT with its own App key (`internal/jit`) and provisions
via the driver. The runner talks to GitHub directly; the control plane never sees the token.

**5. DONE** — `POST .../slots/{handle}/done`, unchanged (done/failed/lost).

### Wire type changes (`internal/api/types.go`)

- `PollRequest` gains `Queues []OrgQueue`; `OrgQueue{Org string; Jobs []QueuedJob}`;
  `QueuedJob{JobID int64; Labels []string; WaitingSecs int}`.
- `Offer` becomes the PENDING decision (already keyless — drop nothing).
- **`Dispatch.JITConfig` is removed**: claim no longer returns a credential. The agent mints
  from the `Offer` it already holds.

## Scheduler over reports

The control plane no longer owns a queue fed by its own polling. Instead:

- Keep, per agent, its latest reported `queues` (in-memory, replaced each REPORT; TTL'd by
  the existing `agentDeadAfter` liveness).
- On a REPORT from an agent with free capacity: build the candidate set = **that agent's own
  reported jobs** (an agent can only run what it can mint for) minus any `(org, job_id)`
  already live in the ledger. Rank by `orgPriority + aging(waiting_secs)` (reuse
  `scheduler.Pending.EffectivePriority`). Return the top candidate as PENDING; `204` if empty.
- Cross-org fairness across *multiple* agents falls out because priority/aging is computed on
  the merged reported set; with one agent (the trial) it degenerates to "highest-priority job
  the box reported." The `scheduler.Heap` enqueue/dequeue is replaced by a per-report ranking
  pass; aging math and the dispatch ledger are reused verbatim.

## Ledger as an interface (M2 seam)

Extract `dispatch.Store` (Offer/Claim/Resolve/Snapshot). JSONL impl exists; add a Neon impl
in M2 (`dispatches` table keyed on dispatch_id, `(org, job_id)` unique among live rows for
the dedup guarantee). OSS single-host keeps JSONL; hosted uses Neon.

## Auth & deploy (M2/M3)

- **M1:** shared bearer token (existing), agent → control plane over HTTPS.
- **M3:** per-agent enrollment tokens minted by the dashboard, validated by the control plane.
- **Deploy:** cp.ushr.io on Fly `iad`, `auto_stop_machines=false` (agents hold long-polls);
  Neon owner role over pgx; same region as the DB.

## Reused unchanged

Two-phase dispatch lifecycle, ledger dedup + liveness sweep (#22/#24), driver runtime
autodetect (#24), the poll/scaleset sources (now invoked agent-side), `internal/jit`.

## Build order (one PR each, all in-tree, each green)

1. **Move source + mint to the agent.** `cmd/agent` builds the source (gh poll / scaleset)
   and a `jit.Minter` from agent-side org config; agent maintains a local queue. Agent config
   gains `orgs`/`source`. Controller still authoritative — no protocol change yet; agent just
   holds the GitHub relationship. (Largest PR.)
2. **Invert the protocol.** `PollRequest.Queues`; scheduler-over-reports on the control plane;
   drop `Dispatch.JITConfig`; agent mints on claim. Strip GitHub/keys/source from the
   controller; slim `controller.yaml`.
3. **`dispatch.Store` interface** + keep JSONL as the default impl. (Prep for Neon.)
4. **Neon store impl** + config DSN.
5. **Fly deploy** (`fly.toml`, Dockerfile for the controller) + point the box agent at
   cp.ushr.io. **Trial.**
6. Enrollment tokens; then dashboard/billing.
