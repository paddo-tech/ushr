# Changelog

This file lists each tagged release, from the git tags and commit history.
The history in this repository starts at v0.2.4.

## Unreleased

- Releases sign `checksums.txt` with keyless cosign and publish an SBOM for
  each archive.
- The installer verifies the cosign signature when cosign is installed.

## v0.2.10 — 2026-09-29

- fix(docker): reclaim the default builder's cache on exclusive hosts.
  Scale-set jobs carry no repo, so builds used the daemon's default builder.
  Its cache grew until the host refused work.

## v0.2.9 — 2026-09-28

- feat(setup): let hosted hosts finish GitHub setup from any browser.
  The browser and the runner host can be different machines. Each GitHub App
  gets its own webhook endpoint, bound to the host's token and scope.
- fix(setup): retry transient install checks and surface webhook lookup
  faults. Network errors and GitHub rate limits no longer end the install wait.
  A database fault no longer answers webhooks as a bad signature.

## v0.2.8 — 2026-09-28

- fix(telemetry): keep dispatch attempts and job events distinct.

## v0.2.7 — 2026-09-28

- fix(update): preserve recovery and allow explicit retries.

## v0.2.6 — 2026-09-27

- fix(agent): retry temporary release server failures.

## v0.2.5 — 2026-09-27

- feat(agent): update hosts without interrupting active jobs.

## v0.2.4 — 2026-09-27

- feat: enable shared self-hosted runner pools.
