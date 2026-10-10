# Changelog

This file lists each tagged release, from the git tags and commit history.
The history in this repository starts at v0.2.4.

## Unreleased

## v0.2.14 — 2026-10-10

- Each host caches public GitHub Action archives and shares them read-only
  with every runner through `ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE`, so jobs
  skip re-downloading actions. The poll source fills the cache from each
  job's log. v0.2.12 and v0.2.13 shipped the cache, but every fill failed.
- fix(actioncache): take the job owner from its scope. Fill errors now log
  as warnings.

## v0.2.13 — 2026-10-09

- Releases sign `checksums.txt` with keyless cosign and publish an SBOM for
  each archive.
- The installer verifies the cosign signature when cosign is installed.
- fix: re-offer a job when GitHub gives its runner a different job.
  GitHub assigns a JIT runner to any queued job with matching labels. The
  job the runner was minted for then stayed queued with no runner. The agent
  now reports when its runner takes a job, and the control plane frees the
  offered job for a new offer. The poller re-reports every queued job on
  each pass. The agent drops a job only when a pass with no failed GitHub
  call did not report it. Upgrade the control plane before the agents. To
  roll the control plane back, see deploy/fly/README.md.

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
