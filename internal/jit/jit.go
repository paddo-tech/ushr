// Package jit mints GitHub Actions runner JIT configs per org installation.
package jit

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
	gh "github.com/paddo-tech/ushr/internal/source/github"
)

// Minter generates a JIT runner config for a tenant scope (an org, or an
// "owner/repo" for repo-scoped targets). It must be pre-loaded with an
// authenticated client per scope via Add.
type Minter struct {
	mu      sync.Mutex
	clients map[string]*github.Client
	groups  map[string]int64
}

// New constructs an empty Minter.
func New() *Minter {
	return &Minter{
		clients: make(map[string]*github.Client),
		groups:  make(map[string]int64),
	}
}

// Add authenticates as the App installation for a's target (org or repo) and
// registers the resulting client under a.Scope() with the runner group it
// should mint into. runnerGroupID == 0 falls back to DefaultRunnerGroupID.
func (m *Minter) Add(ctx context.Context, a gh.AppAuth, runnerGroupID int64) error {
	c, err := gh.NewAppClient(ctx, a)
	if err != nil {
		return fmt.Errorf("auth %s: %w", a.Scope(), err)
	}
	if runnerGroupID == 0 {
		runnerGroupID = config.DefaultRunnerGroupID
	}
	m.mu.Lock()
	m.clients[a.Scope()] = c
	m.groups[a.Scope()] = runnerGroupID
	m.mu.Unlock()
	return nil
}

// Client returns the App client registered for scope, nil if none.
func (m *Minter) Client(scope string) *github.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[scope]
}

// Mint requests a JIT runner config for the job's scope under the previously
// offered name. Repo scopes ("owner/repo") mint a repo-level runner; org scopes
// mint an org-level one.
func (m *Minter) Mint(ctx context.Context, name string, job domain.Job, labels []string) (string, error) {
	m.mu.Lock()
	c, ok := m.clients[job.Org]
	groupID := m.groups[job.Org]
	m.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("no client registered for scope %q", job.Org)
	}
	if owner, repo, isRepo := domain.SplitRepoScope(job.Org); isRepo {
		return mintRepo(ctx, c, owner, repo, name, groupID, labels)
	}
	return mintOrg(ctx, c, job.Org, name, groupID, labels)
}

// mintOrg generates an org-level JIT config. On a 409 it reaps the stale
// same-named runner and retries once: a killed runner container (agent
// restart/crash) leaves its JIT registration behind — the ephemeral runner only
// self-removes on job completion — so re-minting the same job (name is the job
// id) collides until the orphan is deleted.
func mintOrg(ctx context.Context, c *github.Client, org, name string, groupID int64, labels []string) (string, error) {
	req := &github.GenerateJITConfigRequest{Name: name, RunnerGroupID: groupID, Labels: labels}

	cfg, resp, err := c.Actions.GenerateOrgJITConfig(ctx, org, req)
	if err != nil {
		if resp == nil || resp.StatusCode != http.StatusConflict {
			return "", fmt.Errorf("generate jit: %w", err)
		}
		if rerr := reapOrgRunner(ctx, c, org, name); rerr != nil {
			return "", fmt.Errorf("generate jit: %w; reaping stale runner %q failed: %v", err, name, rerr)
		}
		if cfg, _, err = c.Actions.GenerateOrgJITConfig(ctx, org, req); err != nil {
			return "", fmt.Errorf("generate jit after reaping stale runner %q: %w", name, err)
		}
	}
	return cfg.GetEncodedJITConfig(), nil
}

// mintRepo generates a repo-level JIT config (the personal-account/per-repo
// path). Runner groups are an org-only concept, so a repo runner always joins
// the implicit default group (DefaultRunnerGroupID). Same 409-reap semantics as
// mintOrg.
func mintRepo(ctx context.Context, c *github.Client, owner, repo, name string, groupID int64, labels []string) (string, error) {
	req := &github.GenerateJITConfigRequest{Name: name, RunnerGroupID: groupID, Labels: labels}

	cfg, resp, err := c.Actions.GenerateRepoJITConfig(ctx, owner, repo, req)
	if err != nil {
		if resp == nil || resp.StatusCode != http.StatusConflict {
			return "", fmt.Errorf("generate repo jit: %w", err)
		}
		if rerr := reapRepoRunner(ctx, c, owner, repo, name); rerr != nil {
			return "", fmt.Errorf("generate repo jit: %w; reaping stale runner %q failed: %v", err, name, rerr)
		}
		if cfg, _, err = c.Actions.GenerateRepoJITConfig(ctx, owner, repo, req); err != nil {
			return "", fmt.Errorf("generate repo jit after reaping stale runner %q: %w", name, err)
		}
	}
	return cfg.GetEncodedJITConfig(), nil
}

// reapOrgRunner deletes the org runner registered under name, if one exists. The
// exact-name filter makes this a single lookup; a missing runner is a no-op
// (the 409 may have been a transient GitHub-side race that has since cleared).
func reapOrgRunner(ctx context.Context, c *github.Client, org, name string) error {
	runners, _, err := c.Actions.ListOrganizationRunners(ctx, org, &github.ListRunnersOptions{
		Name:        &name,
		ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		return fmt.Errorf("list runners: %w", err)
	}
	for _, r := range runners.Runners {
		if r.GetName() != name {
			continue
		}
		if _, err := c.Actions.RemoveOrganizationRunner(ctx, org, r.GetID()); err != nil {
			return fmt.Errorf("remove runner %d: %w", r.GetID(), err)
		}
		return nil
	}
	return nil
}

// reapRepoRunner is reapOrgRunner for a repo-level runner.
func reapRepoRunner(ctx context.Context, c *github.Client, owner, repo, name string) error {
	runners, _, err := c.Actions.ListRunners(ctx, owner, repo, &github.ListRunnersOptions{
		Name:        &name,
		ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		return fmt.Errorf("list runners: %w", err)
	}
	for _, r := range runners.Runners {
		if r.GetName() != name {
			continue
		}
		if _, err := c.Actions.RemoveRunner(ctx, owner, repo, r.GetID()); err != nil {
			return fmt.Errorf("remove runner %d: %w", r.GetID(), err)
		}
		return nil
	}
	return nil
}
