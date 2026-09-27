// Package domain holds neutral types shared across components.
package domain

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// RunnerNamePrefix identifies jobs served by this fleet.
const RunnerNamePrefix = "ushr-"

// RunnerName identifies an attempt; retries must never overwrite an earlier attempt's telemetry.
func RunnerName(agent string) string {
	var nonce [16]byte
	_, _ = rand.Read(nonce[:])
	return RunnerNamePrefix + agent + "-" + hex.EncodeToString(nonce[:])
}

// RandomName keeps short names for local runtime resources.
func RandomName(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

// Job is a queued unit of work that needs a runner.
type Job struct {
	// Org is the tenant scope: a GitHub org login, or "owner/repo" for a
	// repo-scoped target (personal accounts have no org-level runner pool, so
	// their runners are per-repo). Everything downstream — dispatch dedup, the
	// enrollment-token grant set, tenant matching — treats it as an opaque
	// string; only the agent's GitHub calls interpret org vs repo. See SplitRepoScope.
	Org      string
	Repo     string
	RunID    int64
	JobID    int64
	Labels   []string
	QueuedAt time.Time
}

// SplitRepoScope splits an "owner/repo" scope. ok is false for an org scope (a
// '/' is legal in neither an org login nor a bare repo name).
func SplitRepoScope(scope string) (owner, repo string, ok bool) {
	owner, repo, found := strings.Cut(scope, "/")
	return owner, repo, found && owner != "" && repo != "" && !strings.Contains(repo, "/")
}

// LabelsSatisfied reports whether every label a job requires is advertised by a
// runner. Case-insensitive: GitHub preserves the case a workflow wrote and an
// operator's config rarely matches it.
//
// Both the control plane (deciding what to offer an agent) and the agent
// (deciding what is worth reporting) must apply the same test — an agent that
// reports work it can never be offered leaks that work into its queue forever,
// because nothing but running a job to completion clears an entry.
func LabelsSatisfied(required, advertised []string) bool {
	have := make(map[string]bool, len(advertised))
	for _, l := range advertised {
		have[strings.ToLower(l)] = true
	}
	for _, l := range required {
		if !have[strings.ToLower(l)] {
			return false
		}
	}
	return true
}
