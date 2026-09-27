// Package domain holds neutral types shared across components.
package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// RunnerNamePrefix is the prefix the controller gives every JIT runner it mints
// ("ushr-{agent}-{jobid}"). It's the contract that lets cost attribution and the
// webhook ledger recognise a job as ushr-served, so minting and attribution must
// share this one constant.
const RunnerNamePrefix = "ushr-"

// RunnerName is the dispatch id == slot handle == runner name for a job an
// agent will run: "ushr-{agent}-{jobid}". The single source of truth, shared by
// the control plane (which assigns it) and the agent (which mints under it).
// JobID, not RunID: a multi-job run shares a RunID, so a RunID-based name
// collides on the second job; JobID is unique per job, which also makes a
// re-dispatch of the same job hit the 409-reap path rather than leak a runner.
func RunnerName(agent string, jobID int64) string {
	return fmt.Sprintf("%s%s-%d", RunnerNamePrefix, agent, jobID)
}

// RandomName returns prefix plus a random 4-byte hex suffix — the one name
// generator for runners, VMs, and containers, so entropy/format changes land
// everywhere at once.
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
