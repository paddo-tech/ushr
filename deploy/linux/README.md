# ushr on Linux (rootless Podman)

Runs the controller + agent as systemd **user** services. The agent uses the
`docker` driver, which **autodetects the container runtime** — Docker or Podman,
no `podman-docker` shim needed. k3s is **not** a runner backend yet (the `k8s`
driver is v2+/unbuilt), so runners execute as containers on the host, not pods.

## Prerequisites

1. **A container runtime** — Podman (rootless, recommended) or Docker. If
   neither is installed, `ushr login` / `ushr doctor` detect your distro and
   offer to install Podman for you. The driver prefers `docker` when both are
   present; pin it with `driver.runtime: podman` in `agent.yaml`.

2. **Rootless plumbing** (usually already present on a modern systemd distro):
   cgroup v2 with delegation (needed only if you set a per-runner `memory` cap),
   and `/etc/subuid`+`/etc/subgid` entries for your user (`podman info` warns if
   missing).

3. **Go** to build the binaries (`make build`), and **GitHub App** perms —
   created for you by `ushr setup`.

4. **A linux/amd64 runner image** whose entrypoint accepts `--jitconfig`. The
   default `ghcr.io/actions/actions-runner:latest` works; roll your own if your
   jobs need extra tooling baked in.

## Install

```bash
./deploy/linux/install.sh   # or deploy/get.sh for release binaries (no Go)
```

Then one command: `ushr login` (hosted) or `ushr setup --org YOUR_ORG`
(local/OSS). Both write `agent.yaml`, offer prereq installs, and install +
start the systemd user services. `ushr doctor` checks the host afterwards.

## Notes

- **Labels** in `agent.yaml` must match your workflows' `runs-on`. The example
  ships `self-hosted, Linux, X64`.
- **Capacity** is pure config — the Strix Halo's 128 GB comfortably runs 16–32
  concurrent runner containers. Start conservative and raise it.
- **Docker-in-job**: workflows that build images inside the runner need either a
  Docker-enabled runner image or rootful Podman. Rootless can't nest containers
  out of the box.
- **Host devices**: `driver.devices` passes each listed device to every runner
  container, so every job on the agent can use it. For a hardware-accelerated
  emulator, set `devices: [/dev/kvm]`. The runner user also needs permission
  on the device:
  - Docker: add the host's numeric kvm GID (`getent group kvm`) to
    `group_add`, after the docker GID. The first `group_add` entry owns the
    per-repo build cache config, so keep the docker GID first.
  - Rootless Podman: a numeric GID does not carry the host group into the
    container. Podman documents `group_add: [keep-groups]` for this case. It
    needs the `crun` runtime and cannot be combined with other entries, and
    SELinux can still deny the device. With `keep-groups` there is no numeric
    GID, so the build cache config becomes world-writable.
- **Not HA**: the controller keeps its dispatch ledger on the local filesystem
  and liveness state in memory — run a single controller. Multiple replicas
  split-brain.
