# Security policy

## Report a vulnerability

Report it privately through GitHub:
[open a security advisory](https://github.com/paddo-tech/ushr/security/advisories/new).
Do not open a public issue or pull request.

Include the affected version, the steps to reproduce, and the impact you expect.
We tell you when a fix ships.

## Supported versions

Only the latest release gets security fixes. Upgrade before you report.

| Version | Supported |
|---|---|
| Latest `v0.2.x` release | Yes |
| Older releases | No |

## Scope

In scope:

- The `ushr`, `ushr-controller` and `ushr-agent` binaries in this repository.
- The installer (`deploy/get.sh`, served at `get.ushr.io`) and the release
  artifacts, signatures and SBOMs.
- The hosted control plane at `cp.ushr.io` and the dashboard at `ushr.io`.

Out of scope:

- GitHub, GitHub Actions and the `actions/runner` binary.
- Tart, Lume, Docker, Podman and other tools that the drivers call.
- Runner images that you build or pull from third parties.
- Volumetric denial of service, social engineering and physical attacks.
- Hosts where an attacker already has local administrator access.

## Verify a release

Each release signs `checksums.txt` with keyless cosign. See
[Verify a release](README.md#verify-a-release) in the README.
