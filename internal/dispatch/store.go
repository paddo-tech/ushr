package dispatch

import "time"

// Store is the dispatch persistence the control plane depends on. *Ledger is
// the in-memory + JSONL implementation used by the OSS single-host controller;
// the hosted control plane swaps in a Postgres-backed implementation with the
// same contract, so the server never knows which is behind it.
type Store interface {
	Offer(r Record) error
	// Claim and Resolve take wantOrgs to scope the operation to one tenant: a
	// non-empty wantOrgs only matches a record whose org is one of them (case-
	// insensitively), so an enrolled agent can't claim or resolve another
	// tenant's dispatch by its (guessable) id. Empty wantOrgs skips the check
	// (OSS single-host / sweeps).
	Claim(id string, at time.Time, wantOrgs []string) (Record, bool, error)
	// Start marks a claimed dispatch's runner as working, which frees its
	// offered job to be offered again (see StateStarted).
	Start(id string, wantOrgs []string) (Record, bool, error)
	Resolve(id, status string, wantOrgs []string) (Record, bool, error)
	Snapshot() []Record
	// ExpireOffer removes a record only if it is still in the offered state,
	// returning true if it did. State-guarded so a claim landing between a
	// sweep's snapshot and its expiry call can't delete a live claimed record.
	ExpireOffer(id string) bool
}
