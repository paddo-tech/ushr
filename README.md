# ushr

Cross-org priority scheduler for self-hosted GitHub Actions runners. Architecture in [DESIGN.md](DESIGN.md).

**Status:** v0.2.4, Apache-2.0. Multi-host, keyless: the agent holds your GitHub
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
gating and reclaim.

Next: Kubernetes driver, Prometheus metrics, docs site.

## Auth model

- **Controller ↔ Agent:** per-agent enrollment tokens minted by `ushr login`, scoped to the orgs the agent may serve. Self-hosted single-host instead uses a shared bearer token, or no token at all when bound to loopback.
- **Agent ↔ GitHub:** GitHub App per org via `ghinstallation/v2`. App ID + private key path in `agent.yaml`; installation ID auto-discovered. The control plane never sees a GitHub credential.
- **JIT runner registration:** the agent mints a fresh JIT config per dispatch via `Actions.GenerateOrgJITConfig`. Runner is single-use and self-removes from GitHub when the job finishes.

## License

[Apache-2.0](LICENSE).
