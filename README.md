# ushr

Cross-org priority scheduler for self-hosted GitHub Actions runners. Architecture in [DESIGN.md](DESIGN.md).

**Status:** v0.2.10, Apache-2.0. Multi-host, keyless: the agent holds your GitHub
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

If cosign is on `PATH`, the installer also verifies the release signature.
Otherwise it verifies the sha256 checksums only. It prints which mode ran.

Check a host any time:

```bash
ushr doctor
```

## macOS drivers: Tart and Lume

Tart is the default macOS driver. Tart's maintainers left with Cirrus Labs,
and Tart is to be relicensed more permissively. ushr also ships a Lume driver
as a fallback. Set `driver.type: lume` in `agent.yaml` to use it.

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

## Priority policy

The control plane now owns each scope's priority. A scope is a GitHub org login or `owner/repo`.
Before this change, each agent sent the `priority` from its own `agent.yaml`.
That value still applies when no policy exists for the scope.
A policy value replaces the agent value for every agent that reports the scope.
Aging still adds `boost_per_minute` for each minute a job waits.

Self-hosted controllers read the policy from `controller.yaml`:

```yaml
policy:
  type: priority
  aging:
    boost_per_minute: 1
  priorities:
    example-org: 100
    example-owner/example-repo: 20
```

The hosted control plane reads the `priority_policies` table.
Workspace admins set it in the Workspace settings page.
A row applies only while its workspace owns the scope.
If the table read fails, the control plane uses the agent values for that poll.

## Job telemetry

The dashboard combines runner dispatch records with signed GitHub `workflow_job` events.
Set `USHR_WEBHOOK_SECRET` on the control plane and configure the GitHub App's webhook with the same secret.
Point the webhook at `/webhook` on the control plane and subscribe to **Workflow job** events.
Start events supply live job links. Completion events supply workflow names, results, and execution times.
The dashboard identifies missing results and estimated slot durations.

## Verify a release

Each release signs `checksums.txt` with keyless cosign through the release
workflow's GitHub OIDC identity. Each archive also has an SPDX SBOM from syft.

```bash
cosign verify-blob \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/paddo-tech/ushr/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
sha256sum --ignore-missing -c checksums.txt
```

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
gating and reclaim, shared runner pools, managed agent updates, job telemetry,
GitHub App setup from any browser.

Next: Kubernetes driver, Prometheus metrics, docs site.

## Auth model

- **Controller ↔ Agent:** per-agent enrollment tokens minted by `ushr login`, scoped to the orgs the agent may serve. Self-hosted single-host instead uses a shared bearer token, or no token at all when bound to loopback.
- **Agent ↔ GitHub:** GitHub App per org via `ghinstallation/v2`. App ID + private key path in `agent.yaml`; installation ID auto-discovered. The control plane never sees a GitHub credential.
- **JIT runner registration:** the agent mints a fresh JIT config per dispatch via `Actions.GenerateOrgJITConfig`. Runner is single-use and self-removes from GitHub when the job finishes.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CHANGELOG.md](CHANGELOG.md).
Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

[Apache-2.0](LICENSE).
