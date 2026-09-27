// Package api defines the control-plane<->agent HTTP protocol.
//
// Model B is keyless: the agent holds the GitHub App key, polls GitHub, and
// mints JIT configs itself. The control plane sees only metadata and never a
// credential. Agents dial out (NAT-friendly). Dispatch is two-phase:
//
//  1. POST /v1/agents/{name}/poll with capacity, busy slots, labels, AND the
//     agent's own queued jobs (derived from its GitHub polling). The control
//     plane merges every agent's report, ranks by priority + aging, and
//     returns the one job this agent should run as an Offer (200) or empty
//     (204). No credential crosses the wire.
//  2. POST /v1/agents/{name}/dispatches/{id}/claim to accept. The control
//     plane records the dispatch in the ledger and returns 200; 410 means the
//     offer expired or is unknown — drop it. The agent then mints the JIT
//     locally (its own key) and provisions.
//
// When a dispatch finishes, the agent POSTs its terminal status to
// /v1/agents/{name}/slots/{handle}/done. The poll doubles as the agent's
// heartbeat: the control plane holds even a zero-capacity poll open, so a
// healthy agent is never silent for longer than the poll window.
package api

// PollRequest is what the agent sends on each long-poll iteration. Queues is
// the agent's own view of pending work, polled from GitHub with its own key —
// the control plane schedules on this metadata, never touching a credential.
type PollRequest struct {
	Capacity int        `json:"capacity"` // total concurrent slots this agent supports
	Busy     []string   `json:"busy"`     // slot handles currently in use
	Labels   []string   `json:"labels"`   // labels this agent's runners advertise
	Queues   []OrgQueue `json:"queues"`   // per-org queued jobs this agent can serve

	// Disk* describe the filesystem backing the agent's image store, zero when
	// it can't be measured. Blocked means the agent is refusing work because
	// that store is under its free-space floor; it keeps polling so its
	// heartbeat, queue report and buffered done-reports still flow.
	DiskFreeBytes  uint64 `json:"disk_free_bytes,omitempty"`
	DiskTotalBytes uint64 `json:"disk_total_bytes,omitempty"`
	Blocked        bool   `json:"blocked,omitempty"`
}

// OrgQueue is one org's pending jobs as reported by an agent. Priority is the
// agent's advisory org priority; the control plane owns the final ordering.
type OrgQueue struct {
	Org      string      `json:"org"`
	Priority int         `json:"priority"`
	Jobs     []QueuedJob `json:"jobs"`
}

// QueuedJob is one pending job's scheduling metadata. No repo, no secrets.
type QueuedJob struct {
	JobID       int64    `json:"job_id"`
	Labels      []string `json:"labels"`
	WaitingSecs int      `json:"waiting_secs"`
}

// queueDepth totals the jobs an agent reports as pending across its orgs.
func queueDepth(qs []OrgQueue) int {
	n := 0
	for _, q := range qs {
		n += len(q.Jobs)
	}
	return n
}

// FreeCapacity returns how many more slots the agent can accept. A blocked
// agent has none: that is what makes the scheduler route the work to a host
// that can actually run it, instead of one that would fail every dispatch.
func (r PollRequest) FreeCapacity() int {
	if r.Blocked {
		return 0
	}
	return r.Capacity - len(r.Busy)
}

// Offer names the job the agent should run. It carries no credential: the
// agent already holds the full job (it polled GitHub) and mints the JIT itself
// after claiming. JobID lets the agent match the decision to its local queue.
type Offer struct {
	ID     string   `json:"id"`     // dispatch id == slot name == runner name
	Org    string   `json:"org"`    // GitHub org the runner will register to
	JobID  int64    `json:"job_id"` // which job — the agent mints for this
	Labels []string `json:"labels"` // labels to advertise to GitHub
}

// DoneRequest is what the agent posts when a dispatch finishes.
// Status is one of: "done" (clean exit), "failed" (provision error), "lost"
// (the slot was found orphaned after an agent restart and destroyed — its
// runner never reported, so its registration may linger).
type DoneRequest struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
