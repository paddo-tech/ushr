package driver

import "context"

// SlotHandle identifies a provisioned runner slot. Format is driver-specific.
type SlotHandle string

type Status int

const (
	StatusUnknown Status = iota
	StatusRunning
	StatusDone
	StatusFailed
)

type ProvisionRequest struct {
	// Name is the slot's name — the dispatch ID, and (for scale-set runners)
	// the GitHub runner name. Drivers must name the underlying container/VM
	// with it so the returned handle, the dispatch ID, and the runner name are
	// one identifier: done-reports then match the controller's books exactly.
	Name string
	Org  string
	// Repo is the "owner/repo" the job belongs to, when known. Drivers may use
	// it to isolate per-repo state (e.g. a per-repo build cache). Empty when the
	// agent can't resolve it from its local queue.
	Repo     string
	JITToken string
	Labels   []string
}

// JobStartMarker is logged by the actions runner the moment it picks up a job.
// Its absence means the runner is still idle at "Listening for Jobs" (e.g. its
// job was cancelled before pickup). The contract behind every StartedJob
// implementation; keep it in one place so the drivers can't drift.
const JobStartMarker = "Running job:"

// Driver provisions ephemeral runners for one job each.
//
// List exists for crash recovery: on agent restart, the controller asks for
// existing slots so it can reconcile state and destroy orphans.
type Driver interface {
	Provision(ctx context.Context, req ProvisionRequest) (SlotHandle, error)
	Status(ctx context.Context, h SlotHandle) (Status, error)
	Destroy(ctx context.Context, h SlotHandle) error
	List(ctx context.Context) ([]SlotHandle, error)
	Capacity() int
}

// JobWatcher is an optional Driver capability. StartedJob reports whether a
// provisioned slot has begun executing a job (vs still idling, waiting for
// one). The agent uses it to reap a runner whose job was cancelled before
// pickup: such a runner idles indefinitely and would otherwise hold its slot
// forever, starving capacity. Drivers that can't tell idle from working (or
// don't need to) simply don't implement it.
type JobWatcher interface {
	StartedJob(ctx context.Context, h SlotHandle) (bool, error)
}

// DiskWatcher is an optional Driver capability naming the filesystem whose free
// space bounds provisioning — the VM or image store. A host that can't clone a
// VM or pull an image fails every dispatch it accepts, and the control plane can
// only route that work to another host if this one stops accepting, so the agent
// gates on it. Returning "" means the store can't be located, which leaves the
// gate open rather than blocking dispatch on a measurement failure.
type DiskWatcher interface {
	DiskPath(ctx context.Context) string
}

// Reclaimer is an optional Driver capability: the steps that free space in the
// image store, least destructive first. The driver only describes and executes
// them; the agent decides when to run one, because the decision needs state
// only the agent has — the free-space floor, and whether a job is running.
type Reclaimer interface {
	ReclaimTiers(ctx context.Context) []Tier
}

// Tier is one reclaim step: what it frees, and how.
type Tier struct {
	Name string
	// Drain marks a step that must not run beside a live job — it deletes
	// objects ushr doesn't own or can't prove are idle. The agent deselects the
	// host and waits for it to go quiet before running these, which is the
	// option a scheduler that can reroute work has and a single-node runtime
	// doesn't. Steps that only touch our own idle state leave it false.
	Drain bool
	Run   func() error
}

// Maintainer is an optional Driver capability: long-running background upkeep
// (e.g. build-cache pruning and reaping idle per-repo builders) that runs until
// ctx is cancelled. The agent starts it once at boot. Drivers with no upkeep
// simply don't implement it.
type Maintainer interface {
	Maintain(ctx context.Context)
}
