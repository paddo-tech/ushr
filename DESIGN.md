# ushr — design

Cross-org priority scheduler for self-hosted GitHub Actions runners.

## Problem

GitHub's runner hierarchy stops at the org level (or Enterprise, $$). Self-hosters with multiple unrelated orgs have no native way to share a runner pool with priority rules.

Existing tools:
- **ARC** — autoscales within one org, no cross-org pooling
- **Tart / sand / Tartelet / Cilicon** — provision macOS VMs, no scheduling logic
- **GitHub Enterprise** — solves it, but ~$21/user/mo and requires consolidation

`ushr` fills the missing layer: a scheduler that watches queued jobs across N orgs, applies priority + aging policy, and dispatches to free runner slots via pluggable drivers.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                       Controller                        │
│                                                         │
│   ┌────────┐    ┌──────────────┐    ┌──────────────┐  │
│   │ Source │───▶│  Scheduler   │───▶│ Slot Manager │  │
│   │ poll/wh│    │ priority+age │    │   (M slots)  │  │
│   └────────┘    └──────────────┘    └──────┬───────┘  │
│                                            │           │
│                          (dispatch via gRPC/HTTP)      │
└────────────────────────────────────────────┼───────────┘
                                             ▼
                              ┌──────────────────────────┐
                              │          Agent           │
                              │   (dials out to ctrl)    │
                              │                          │
                              │   ┌──────────────────┐  │
                              │   │     Driver       │  │
                              │   │  Tart│K8s│Docker │  │
                              │   └──────────────────┘  │
                              └──────────────────────────┘
                                             │ provision
                                             ▼
                                    Ephemeral runner
                              actions/runner --jit --org X
```

### Components

- **Source** — produces "job needed" events. v1: poll `/repos/.../actions/runs?status=queued` per configured org. v2: GitHub webhook receiver.
- **Scheduler** — priority queue with aging. Picks the next job for a free slot. No preemption (running jobs always finish).
- **Slot Manager** — tracks M concurrent slots. Knows free vs occupied. Sends dispatch requests to agents.
- **Agent** — long-lived process on the runner host. Dials out to the controller (NAT-friendly). Hosts a Driver.
- **Driver** — provisions a single runner for a single job, then destroys it.

### Driver interface

```go
type Driver interface {
    Provision(ctx, org, jitToken, labels) (SlotHandle, error)
    Status(SlotHandle) (Running|Done|Failed, error)
    Destroy(SlotHandle) error
    Capacity() int
}
```

Implementations:
- **TartDriver** (v1) — `tart clone base ephemeral-N`, `tart run`, SSH in, `actions/runner --jit`, watch, destroy
- **K8sDriver** (v2+) — `kubectl create job` with runner image, watch pod, delete
- **DockerDriver** (v2+) — `docker run` for Linux self-host without k8s
- **SystemdDriver** (niche) — bare-metal via `systemd-run`

The scheduler doesn't know which Driver an agent uses.

## Controller / agent split

Even in self-hosted single-host mode, controller and agent are **separate processes** (or one binary, two modes) communicating over HTTP/gRPC with token auth.

### Why

- **NAT-friendly** — agent dials out, no inbound exposure required
- **Multi-host** — one controller, N agents on different machines (your Mac + a Linux box)
- **SaaS-ready** — controller can run on hosted infra later; agents stay on user iron
- **Failure isolation** — agent crash doesn't kill scheduler state

### Cost

~20% extra code in v1 vs in-process coupling. Worth it; refactoring the seam later is significantly more painful than designing for it now.

## Scheduling policy

### Priority

Each org has a static priority (integer). Higher wins for slot allocation when multiple orgs have queued jobs.

### Aging

Low-priority jobs get a `boost_per_minute` added to their effective priority while waiting. Prevents starvation. Effective priority = base_priority + (minutes_waiting × boost).

### No preemption

Running jobs always finish. Priority only affects queue order, never kills work in progress.

## Config sketch

```yaml
controller:
  listen: 127.0.0.1:7080
  token: ${USHR_TOKEN}

orgs:
  - name: paddo-tech
    app_id: 12345
    private_key_path: ~/.secrets/paddo-tech.pem
    priority: 100
  - name: example-org-secondary
    priority: 50
  - name: example-org-tertiary
    priority: 20

policy:
  type: priority
  aging:
    boost_per_minute: 1

source:
  type: poll
  interval: 10s
```

```yaml
# agent.yaml (separate file, on the runner host)
agent:
  controller_url: http://127.0.0.1:7080
  token: ${USHR_TOKEN}
  name: mac-runner-1

driver:
  type: tart
  image: ghcr.io/cirruslabs/macos-runner:tahoe
  capacity: 2  # Apple's macOS VM cap
```

## Deployment modes

### v1 — self-hosted single host

Controller + agent run on the same Mac. One yaml each, launchd plists for both. Communication over loopback.

### v2 — self-hosted multi host

One controller (could be on a small Linux VPS or one of the runner hosts). N agents on different physical machines, each with its own Driver. Useful for "Mac for iOS + Linux box for Android backend builds, pooled."

### v3 — hosted control plane (only if validated)

`ushr.io` runs the controller. Users install the agent on their iron. GitHub OAuth/App for org connection. Web UI, billing, multi-tenant.

## Roadmap

| Version | Scope |
|---|---|
| **v0.1** | Tart driver, polling source, static priority, single-host, no UI. Ship for our own use. |
| **v0.2** | Aging, retries, basic CLI for queue inspection. Public OSS release. |
| **v0.3** | Webhook source, multi-host (multiple agents one controller), Prometheus metrics. |
| **v1.0** | Web UI, K8sDriver, hardened auth, docs site at ushr.io. |
| **v2.0** | Hosted control plane. Only if v1 gets traction. |

## Non-goals

- **Hosting runners** — the whole point is "you bring the iron"
- **Replacing ARC** — ARC stays better at within-org k8s autoscaling. `ushr` adds the cross-org layer above it.
- **Job-level routing rules** beyond org+labels (e.g. "this PR title matches X, route to Y") — out of scope, GitHub Actions has its own routing
- **Preemption / killing in-progress jobs** — explicitly off the table

## Implementation language

**Go.** Decisive factors: `go-github` is the most complete GitHub SDK; `actions/runner` and ARC ecosystem are Go; goroutines map naturally to scheduler/dispatch; cross-compile for darwin/linux × arm64/amd64 is trivial; iteration speed matters for solo OSS work. Performance is irrelevant — ushr is I/O bound on GitHub API.

## Open questions

- ~~Auth between controller and agent.~~ Done (v0.2): per-agent enrollment tokens scoped to the agent's orgs; self-hosted single-host keeps a shared token or loopback with no token. mTLS is still open.
- ~~Agent identity / reconnection semantics.~~ Done (v0.2): the long poll is the heartbeat. The server marks a silent agent's claimed dispatches lost after four missed poll windows.
- JIT registration token caching — GitHub limits token request rate; need to handle gracefully.
- **Org auth: PAT fallback alongside GitHub App.** Real users will start with a PAT for testing before setting up an App. Need to either support per-org `token:` field or document the App-only path.
- ~~**Agent capability advertisement.**~~ Done: each poll carries the agent's label set, and the server offers a job only to an agent whose labels satisfy it (`domain.LabelsSatisfied`).

## v0.1.1 follow-up — operational maturity

v0.1 ships runnable but stateless about in-flight work. These are the known gaps to close before any second host or shared usage:

### High priority (real ops issues, not theoretical)

- **Orphan JIT runner reaper.** Every Mint-then-fail (controller restart between Mint and dispatch return, agent crash, network drop) leaks a runner registration in GitHub that sits `offline` forever. Add a controller-side reaper goroutine polling `/orgs/{org}/actions/runners` and deleting `offline` runners matching the `ushr-*` prefix older than ~5 minutes. Distinct from the agent-side `ReconcileOrphans` already in v0.1, which destroys local Tart VMs left by a SIGKILL'd previous process — these two reapers are complementary, not duplicative.
- ~~**Controller dispatch ledger.**~~ Done (v0.2): `internal/dispatch` persists every offer/claim/resolve to a JSONL WAL (`dispatch_ledger_path`, default `~/.local/share/ushr/dispatches.jsonl`). Restart replays claimed dispatches; offered ones drop (the job is still queued on GitHub and the poller re-emits it).
- ~~**`ReportDone` with retry.**~~ Done (v0.2): the agent buffers failed done-reports in memory and retries them at the top of every poll iteration. Reports are idempotent server-side.
- ~~**Two-phase dispatch.**~~ Done (v0.2): poll returns a credential-free `Offer{ID}`; the agent claims via `/v1/agents/{name}/dispatches/{id}/claim`, and only the claim mints. An unclaimed offer expires (2x poll window) with nothing leaked. `JITMinter` split into `Name` (offer time; deterministic for the poll source so re-dispatch 409-reaps its own stale registration) and `Mint` (claim time). The poll doubles as a heartbeat — a zero-capacity poll is held open, not bounced — and the server marks a silent agent's claimed dispatches lost after four missed poll windows instead of waiting out the 30-minute TTL backstop.

### Medium priority (degrades correctness over time)

- **Per-org circuit breaker for mint failures.** A bad org config (revoked installation) currently re-enqueues immediately and burns the App's 5000/hr rate limit for ALL orgs in seconds. Add a per-org backoff that pauses the org's polling on consecutive 401/403.
- **Source `seen` map eviction.** Currently unbounded. At 100 jobs/day this is ~880KB/year — defer. Add age-based eviction (drop IDs older than 24h) when the map matters.
- **ETag-aware source polling.** `Apps.ListRepos` and per-repo run lists send full bodies every tick. ETag returns 304 with no body cost AND doesn't decrement the rate limit. ~10-100× fewer effective calls at idle.
- **Status check optimization.** `Driver.Status` SSH-execs `pgrep` every 10s per running slot. SSH ControlMaster multiplexing or having the runner write a sentinel file would drop VM CPU notably during long jobs.

### Lower priority (polish / forward compat)

- ~~**Agent unit tests.**~~ Done (v0.2): `TestRunHappyPath` drives poll -> claim -> provision -> done against the real server and ledger.
- **Protocol version negotiation.** `/v1/` in the path is theatre without `X-Ushr-Protocol` headers and an agent-side mismatch check.
- **Log rotation + correlation IDs.** launchd doesn't rotate; `controller.log` grows unbounded. A `dispatch_id` field threaded through scheduler→server→agent→driver makes post-hoc investigation tractable.
- **App private key reload.** Today rotation requires a `launchctl kickstart -k`. SIGHUP handler that re-runs `minter.Add` per org would be cleaner.
- **Build provenance.** `install.sh` runs `make build` from local checkout — running ushr is whatever git state was present. Wire `ushr-controller -version` to `runtime/debug.ReadBuildInfo` and tag releases.
