// Package github implements a polling Source backed by the GitHub Actions API.
package github

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/domain"
)

// AppAuth identifies a GitHub App installation. It is either org-scoped (Org
// set) or repo-scoped (Owner+Repo set, for personal-account repos that have no
// org-level runner pool). Exactly one form is populated.
type AppAuth struct {
	AppID          int64
	PrivateKeyPath string
	Org            string
	// Owner/Repo name a single repo target (personal-account or per-repo).
	Owner string
	Repo  string
	// Repos optionally restricts an org target's polling to these repo names;
	// empty polls all installation repos. Unused for repo targets.
	Repos []string
}

// Scope returns the tenant scope this auth mints under: the org login, or
// "owner/repo" for a repo target. Matches domain.Job.Org.
func (a AppAuth) Scope() string {
	if a.Repo != "" {
		return a.Owner + "/" + a.Repo
	}
	return a.Org
}

// ParseScope is Scope's inverse: a valid "owner/repo" → repo target, else org
// target. Kept beside Scope, built on the canonical grammar in domain.
func ParseScope(scope string) AppAuth {
	if owner, repo, ok := domain.SplitRepoScope(scope); ok {
		return AppAuth{Owner: owner, Repo: repo}
	}
	return AppAuth{Org: scope}
}

// InstallationID resolves the App's installation for a's target (org or repo)
// via a JWT-only client. Shared by every source that authenticates as the App.
func InstallationID(ctx context.Context, appID int64, key []byte, a AppAuth) (int64, error) {
	jwtTransport, err := ghinstallation.NewAppsTransport(http.DefaultTransport, appID, key)
	if err != nil {
		return 0, fmt.Errorf("load app key: %w", err)
	}
	jwtClient := github.NewClient(&http.Client{Transport: jwtTransport})
	if a.Repo != "" {
		install, _, err := jwtClient.Apps.FindRepositoryInstallation(ctx, a.Owner, a.Repo)
		if err != nil {
			return 0, fmt.Errorf("find installation for %s/%s: %w", a.Owner, a.Repo, err)
		}
		return install.GetID(), nil
	}
	install, _, err := jwtClient.Apps.FindOrganizationInstallation(ctx, a.Org)
	if err != nil {
		return 0, fmt.Errorf("find installation in %s: %w", a.Org, err)
	}
	return install.GetID(), nil
}

// NewAppClient returns a go-github client authenticated as the App installation
// for a's target (org or repo). The installation ID is discovered automatically.
func NewAppClient(ctx context.Context, a AppAuth) (*github.Client, error) {
	key, err := os.ReadFile(a.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read app key: %w", err)
	}
	installID, err := InstallationID(ctx, a.AppID, key, a)
	if err != nil {
		return nil, err
	}

	// Conditional-request cache under the auth transport: idle list polls return
	// 304 (no body, no rate-limit quota) and replay from cache. 512 entries
	// bounds the per-run job-list URLs that accumulate over time.
	cache := newETagTransport(http.DefaultTransport, 512)
	tr, err := ghinstallation.New(cache, a.AppID, installID, key)
	if err != nil {
		return nil, fmt.Errorf("installation transport: %w", err)
	}
	return github.NewClient(&http.Client{Transport: tr}), nil
}
