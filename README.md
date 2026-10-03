# ushr

Cross-org priority scheduler for self-hosted GitHub Actions runners. Architecture in [DESIGN.md](DESIGN.md).

**Status:** v0.2.8, Apache-2.0. Multi-host, keyless: the agent holds your GitHub
App key and the control plane never sees a credential.

Run it two ways. Self-host the controller alongside the agent (`ushr setup`) and
nothing leaves your network. Or point the agent at the hosted control plane at
[ushr.io](https://ushr.io) (`ushr login`) for cross-machine scheduling and a
dashboard — the key still never leaves your box.

## Components

| Binary | Role |
|---|---|
| `ushr` | User-facing CLI. `ushr login` sets a host up end to end for the hosted plane; `ushr setup --org NAME` creates the GitHub App and records it in `agent.yaml`; `ushr doctor` checks host health; `ushr init [dir]` rewrites a repo's workflow `runs-on` targets to ushr labels. |
| `ushr-controller` | Schedules queued jobs across agents over a long-poll HTTP API (keyless: agents report metadata, the controller holds no GitHub credentials) |
| `ushr-agent` | Runs on the runner host. Holds the GitHub App keys, watches GitHub for queued jobs, mints JIT configs, and uses the configured Driver (Tart on macOS, docker/podman on Linux) to provision/destroy ephemeral runners |

## Quick start — one command per host

Install the binaries:

```bash
curl -fsSL https://get.ushr.io | sh
# or, from a checkout: ./deploy/install.sh
```

Then, on the runner host:

```bash
# Hosted plane (ushr.io): guided end-to-end setup —
# config, prereq installs (tart / podman, runner image), browser
# enrollment, GitHub App creation, service install + start.
ushr login

# Local/OSS single host, no account: GitHub App + local
# controller/agent services.
ushr setup --org YOUR_ORG
```

Both are interactive but zero-edit: prereqs are offered as guided installs
(`brew install tart`, image pulls, podman), the GitHub App flow is two browser
clicks, and the org block is written into `~/.config/ushr/agent.yaml` for you.
On Linux the per-repo docker build cache is on by default.

Check a host any time:

```bash
ushr doctor
```

## Managed agent updates

Workspace admins can select **Update agent** on a host in the Fleet dashboard.
The host finishes active jobs before it installs the latest stable agent release.
It downloads the official release and verifies the checksum published with that release.
A supervisor restores the previous agent if the new process cannot contact the control plane within 90 seconds.
The dashboard shows the installed version, update progress, and failures.

Existing hosts need one manual installation of v0.2.7 to enable the recovery protocol.
The recovery process stays running until the service restarts.
Each update starts a new supervisor and worker under that recovery process.
A new dashboard request can retry a failed release. Older targets cannot downgrade an agent.
Managed updates replace the agent binary. Use the installer to update the CLI and a local controller.

## Job telemetry

The dashboard combines runner dispatch records with signed GitHub `workflow_job` events.
Set `USHR_WEBHOOK_SECRET` on the control plane and configure the GitHub App's webhook with the same secret.
Point the webhook at `/webhook` on the control plane and subscribe to **Workflow job** events.
Start events supply live job links. Completion events supply workflow names, results, and execution times.
The dashboard identifies missing results and estimated slot durations.

## Metrics

The controller and the agent export Prometheus metrics at `/metrics`.

The controller serves `/metrics` on its normal listener.
With a static token (`USHR_TOKEN`), a scrape must send `Authorization: Bearer <token>`.
Without a static token, only a direct loopback peer can scrape.
The controller refuses scrapes that carry `X-Forwarded-For`, `Forwarded` or `Fly-Client-IP`.
Enrolled agent tokens never unlock `/metrics`, because the series cover every tenant.

| Controller series | Type | Meaning |
|---|---|---|
| `ushr_controller_offers_total` | counter | Dispatch offers made to agents |
| `ushr_controller_claims_total` | counter | Offers that agents claimed |
| `ushr_controller_dispatch_latency_seconds` | histogram | Job wait from GitHub queue to offer, from the agent-reported wait |
| `ushr_controller_agents_lost_total` | counter | Agents dropped after missed polls |
| `ushr_controller_agents_active` | gauge | Agents heard from within the liveness window |
| `ushr_controller_queued_jobs_seen` | gauge | Distinct queued jobs in the latest poll of each active agent |

The agent serves no metrics by default.
Set `metrics_listen` in `agent.yaml` to serve them:

```yaml
metrics_listen: 127.0.0.1:9464
```

The agent listener has no auth. Bind it to loopback or a private interface.

| Agent series | Type | Labels | Meaning |
|---|---|---|---|
| `ushr_agent_slots_busy` | gauge | | Slots reserved or running a job |
| `ushr_agent_slots_total` | gauge | | Slots the driver offers |
| `ushr_agent_provision_duration_seconds` | histogram | `driver` | Time to provision a runner, successes only |
| `ushr_agent_provision_failures_total` | counter | `driver` | Provision attempts that failed |
| `ushr_agent_disk_blocked` | gauge | | 1 while the disk gate or a reclaim drain refuses work |
| `ushr_agent_github_api_errors_total` | counter | `org` | Failed GitHub API calls by the poll source |
| `ushr_agent_github_breaker_open` | gauge | `org` | 1 while the poll source pauses an org |
| `ushr_agent_poll_errors_total` | counter | | Failed polls to the controller |

The GitHub series come from the `poll` source only. The `scaleset` source does not export them.
Both processes also export the standard Go runtime and process series.

## Development

```bash
make build          # bin/ushr, bin/ushr-controller, bin/ushr-agent
make test           # go test ./...
make lint           # golangci-lint run
```

End-to-end Tart driver smoke (requires real Tart + an org with the App installed):
```bash
go run ./cmd/integration-test -trigger-repo=example-repo
```

> Note: mints a real org-level runner registration in GitHub, dispatches a real workflow on `-trigger-repo`, and deletes both on success. Don't point at a production org for casual testing.

## Roadmap

Shipped: Tart / Lume / Docker / SSH drivers, polling and scaleset sources,
priority + aging, multi-host, two-phase keyless dispatch, per-agent enrollment,
persistent dispatch ledger, `ushr cost`, one-command host setup, disk-pressure
gating and reclaim, Prometheus metrics.

Next: Kubernetes driver, docs site.

## Auth model

- **Controller ↔ Agent:** per-agent enrollment tokens minted by `ushr login`, scoped to the orgs the agent may serve. Self-hosted single-host instead uses a shared bearer token, or no token at all when bound to loopback.
- **Agent ↔ GitHub:** GitHub App per org via `ghinstallation/v2`. App ID + private key path in `agent.yaml`; installation ID auto-discovered. The control plane never sees a GitHub credential.
- **JIT runner registration:** the agent mints a fresh JIT config per dispatch via `Actions.GenerateOrgJITConfig`. Runner is single-use and self-removes from GitHub when the job finishes.

## License

[Apache-2.0](LICENSE).
