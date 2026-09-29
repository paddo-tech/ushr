// Package docker implements the Driver interface by running each ephemeral
// runner as a container via the Docker or Podman CLI (autodetected, or pinned
// via Options.Runtime — the two are drop-in for the subcommands used here). It
// is deliberately much thinner than the Tart driver: no VM clone, no boot wait,
// no SSH. A slot is one `run -d` of the runner image launched with
// `run.sh --jitconfig`, status is one `inspect`, teardown is one `rm -f`.
//
// Unlike Tart (Apple caps macOS guests at 2 per host), container capacity is
// bounded only by host CPU/memory, so Capacity is purely config-driven.
//
// Containers carry the label ushr=runner so List() can find ours and reconcile
// orphans on agent restart without touching unrelated containers on the host.
package docker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paddo-tech/ushr/internal/driver"
)

const (
	DefaultEntrypoint          = "/home/runner/run.sh"
	DefaultCapacity            = 4
	DefaultBuildkitImage       = "moby/buildkit:buildx-stable-1"
	DefaultBuildCacheGB        = 20
	DefaultBuildCacheIdleHours = 168 // reap a repo's builder after a week idle
	maintenanceInterval        = 30 * time.Minute
	// runnerPullInterval is well inside the 30 days GitHub gives a runner to
	// update after a new release.
	runnerPullInterval = 6 * time.Hour
	infoTimeout        = 10 * time.Second
	// pressureCacheDivisor is how much of a repo's steady-state cache budget it
	// keeps when the host is short of disk.
	pressureCacheDivisor = 2
	// DefaultMinFreeGB is the free-space floor under which the agent stops
	// accepting work. Containers are thin next to a VM clone, but one job's
	// image pulls and build cache still land in the same data root.
	DefaultMinFreeGB = 25

	// ownerLabel tags every container we create so List() can filter to ours.
	ownerLabel = "ushr=runner"
)

// Options configures a Driver. Zero values fall back to Default* constants.
type Options struct {
	Image      string
	Capacity   int
	Runtime    string // "docker" | "podman"; empty autodetects
	Network    string
	Volumes    []string
	ExtraHosts []string
	GroupAdd   []string
	Memory     string
	Entrypoint string
	// Per-repo persistent build cache (docker driver).
	BuildCache        bool
	BuildkitImage     string
	BuildCacheGB      int
	BuildCacheIdleHrs int
	StateDir          string
	// Exclusive declares that no workload other than ushr's runners uses this
	// daemon, which is what lets reclaim collect the containers, volumes and
	// images jobs leave behind through the shared socket. Off by default: on a
	// daemon someone else also uses, those objects are not ours to delete.
	Exclusive bool
}

// Driver is the Docker implementation of driver.Driver.
type Driver struct {
	image      string
	capacity   int
	runtime    string
	network    string
	volumes    []string
	extraHosts []string
	groupAdd   []string
	memory     string
	entrypoint string

	buildCache     bool
	buildkitImage  string
	buildCacheGB   int
	buildCacheIdle time.Duration
	stateDir       string
	sharedGID      int                    // gid shared by agent + runner for buildx config; -1 if none
	buildMu        sync.Mutex             // guards repoLocks
	repoLocks      map[string]*sync.Mutex // serializes builder creation per repo

	rootMu   sync.Mutex // guards dataRoot
	dataRoot string

	// gcMu serializes the two things that mutate builder state — the maintenance
	// sweep and pressure reclaim — which hold opposite objectives and would
	// otherwise prune a builder the other is removing.
	gcMu sync.Mutex

	exclusive bool
}

var (
	_ driver.Driver      = (*Driver)(nil)
	_ driver.JobWatcher  = (*Driver)(nil)
	_ driver.Maintainer  = (*Driver)(nil)
	_ driver.DiskWatcher = (*Driver)(nil)
	_ driver.Reclaimer   = (*Driver)(nil)
)

// New constructs a Driver. Empty Options fields fall back to Default*.
func New(opts Options) *Driver {
	if opts.Capacity == 0 {
		opts.Capacity = DefaultCapacity
	}
	if opts.Entrypoint == "" {
		opts.Entrypoint = DefaultEntrypoint
	}
	if opts.Runtime == "" {
		opts.Runtime = detectRuntime()
	}
	if opts.BuildkitImage == "" {
		opts.BuildkitImage = DefaultBuildkitImage
	}
	if opts.BuildCacheGB == 0 {
		opts.BuildCacheGB = DefaultBuildCacheGB
	}
	if opts.BuildCacheIdleHrs == 0 {
		opts.BuildCacheIdleHrs = DefaultBuildCacheIdleHours
	}
	if opts.StateDir == "" {
		home, _ := os.UserHomeDir()
		opts.StateDir = filepath.Join(home, ".local", "share", "ushr")
	}
	sharedGID := -1
	if len(opts.GroupAdd) > 0 {
		if g, err := strconv.Atoi(opts.GroupAdd[0]); err == nil {
			sharedGID = g
		}
	}
	if opts.BuildCache && sharedGID < 0 {
		slog.Warn("build_cache: group_add has no numeric gid, so the per-repo buildx config will be world-writable; set group_add to the host docker gid to lock it to that group")
	}
	return &Driver{
		image:          opts.Image,
		capacity:       opts.Capacity,
		runtime:        opts.Runtime,
		network:        opts.Network,
		volumes:        opts.Volumes,
		extraHosts:     opts.ExtraHosts,
		groupAdd:       opts.GroupAdd,
		memory:         opts.Memory,
		entrypoint:     opts.Entrypoint,
		buildCache:     opts.BuildCache,
		buildkitImage:  opts.BuildkitImage,
		buildCacheGB:   opts.BuildCacheGB,
		buildCacheIdle: time.Duration(opts.BuildCacheIdleHrs) * time.Hour,
		stateDir:       opts.StateDir,
		exclusive:      opts.Exclusive,
		sharedGID:      sharedGID,
	}
}

func (d *Driver) Capacity() int { return d.capacity }

// DiskPath resolves the runtime's data root — images, container layers and the
// build cache all live there. Docker and podman name it differently in `info`,
// and a daemon that is down at boot must not poison the answer, so only a
// successful lookup is cached.
func (d *Driver) DiskPath(ctx context.Context) string {
	d.rootMu.Lock()
	defer d.rootMu.Unlock()
	if d.dataRoot != "" {
		return d.dataRoot
	}
	// The agent asks on every poll until this succeeds, so it must be bounded:
	// `info` against a wedged daemon otherwise stalls the poll loop — and with
	// it the heartbeat and buffered done-reports — for the process's lifetime.
	ctx, cancel := context.WithTimeout(ctx, infoTimeout)
	defer cancel()
	for _, tmpl := range []string{"{{.DockerRootDir}}", "{{.Store.GraphRoot}}"} {
		out, err := d.dockerOut(ctx, "info", "-f", tmpl)
		if err != nil {
			continue
		}
		p := strings.TrimSpace(string(out))
		if p == "" || strings.Contains(p, "<no value>") {
			continue
		}
		// The path must exist here, not merely be reported: a VM-backed runtime
		// (Docker Desktop, colima, podman machine) names a path inside its VM,
		// and measuring the host directory it resolves to would report a
		// filesystem that has nothing to do with where images land.
		if _, err := os.Stat(p); err != nil {
			slog.Warn("container data root is not visible from the agent; disk gating disabled",
				"path", p, "err", err)
			continue
		}
		d.dataRoot = p
		return p
	}
	return ""
}

// detectRuntime prefers docker and falls back to podman. Both speak the same
// CLI for the subcommands this driver uses, and isNotFound already handles
// podman's error wording. Falls back to docker when neither is on PATH so the
// failure surfaces at first use as a clear "executable not found".
func detectRuntime() string { return detectRuntimeWith(exec.LookPath) }

func detectRuntimeWith(lookPath func(string) (string, error)) string {
	for _, c := range []string{"docker", "podman"} {
		if _, err := lookPath(c); err == nil {
			return c
		}
	}
	return "docker"
}

// Provision starts a detached runner container. The JIT config carries the
// runner name, labels, and registration — the controller already baked those
// in via GenerateOrgJITConfig, so the driver only has to hand it to run.sh.
func (d *Driver) Provision(ctx context.Context, req driver.ProvisionRequest) (driver.SlotHandle, error) {
	name := req.Name

	args := []string{"run", "-d", "--name", name, "--label", ownerLabel}
	if d.network != "" {
		args = append(args, "--network", d.network)
	}
	for _, v := range d.volumes {
		args = append(args, "-v", v)
	}
	for _, h := range d.extraHosts {
		args = append(args, "--add-host", h)
	}
	for _, g := range d.groupAdd {
		args = append(args, "--group-add", g)
	}
	if d.memory != "" {
		args = append(args, "--memory", d.memory)
	}
	// Per-repo persistent build cache: point the runner's default buildx builder
	// at this repo's long-lived buildkit, so its cache survives between jobs
	// while staying isolated from other repos. Best-effort: a failure here just
	// means a cold build, never a failed job.
	if d.buildCache && req.Repo != "" {
		if dir, builder, err := d.ensureRepoBuilder(ctx, req.Repo); err == nil {
			// Mount outside ~/.docker so bind-creating the mount point doesn't
			// root-own the runner's .docker and break `docker`/gcloud writes.
			args = append(args,
				"-v", dir+":/ushr/buildx",
				"-e", "BUILDX_CONFIG=/ushr/buildx",
				"-e", "BUILDX_BUILDER="+builder)
		}
	}
	args = append(args, "--entrypoint", d.entrypoint, d.image, "--jitconfig", req.JITToken)

	if err := d.docker(ctx, args...); err != nil {
		// Best-effort cleanup: a failed `run` can still leave a created container.
		_ = d.Destroy(context.Background(), driver.SlotHandle(name))
		return "", fmt.Errorf("%s run %s: %w", d.runtime, name, err)
	}
	return driver.SlotHandle(name), nil
}

// Maintain runs the driver's upkeep until ctx is cancelled. It re-pulls the
// runner image because `run` never refreshes a local tag, and GitHub rejects a
// runner whose version it has deprecated.
func (d *Driver) Maintain(ctx context.Context) {
	go d.maintainBuildCache(ctx)
	t := time.NewTicker(runnerPullInterval)
	defer t.Stop()
	for {
		pullCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err := d.docker(pullCtx, "pull", d.image)
		cancel()
		if err != nil {
			slog.Warn("runner image pull failed", "image", d.image, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Status maps the container state to a driver.Status. A gone container (the
// runner is ephemeral and may have been reaped) is treated as Done; any other
// docker error is surfaced so the agent's retry budget applies.
func (d *Driver) Status(ctx context.Context, h driver.SlotHandle) (driver.Status, error) {
	out, err := d.dockerOut(ctx, "inspect", "-f", "{{.State.Status}}", string(h))
	if err != nil {
		if isNotFound(err) {
			return driver.StatusDone, nil
		}
		return driver.StatusUnknown, err
	}
	return statusFromState(strings.TrimSpace(string(out))), nil
}

// Destroy force-removes the container. Treated as success if it is already gone.
func (d *Driver) Destroy(ctx context.Context, h driver.SlotHandle) error {
	if err := d.docker(ctx, "rm", "-f", string(h)); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("%s rm %s: %w", d.runtime, string(h), err)
	}
	return nil
}

// List returns every container we own (running or exited) for orphan reaping.
func (d *Driver) List(ctx context.Context) ([]driver.SlotHandle, error) {
	out, err := d.dockerOut(ctx, "ps", "-a", "--filter", "label="+ownerLabel, "--format", "{{.Names}}")
	if err != nil {
		return nil, err
	}
	return parseNames(out), nil
}

// StartedJob reports whether the runner has begun a job, read from its console
// log. A gone container counts as not-started (Status reports it Done); the
// runner's output can land on either stream depending on the image entrypoint,
// so both are scanned.
func (d *Driver) StartedJob(ctx context.Context, h driver.SlotHandle) (bool, error) {
	// --since bounds each poll to recent output so an idle chatty container
	// doesn't cost a full-log copy per poll. The window is sized far above the
	// 10s poll interval: a stall between polls (host suspend, wedged docker
	// call) must not scroll the marker out of view, or a working runner would
	// read as idle and be reaped mid-job.
	cmd := exec.CommandContext(ctx, d.runtime, "logs", "--since", "5m", string(h))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		werr := fmt.Errorf("%s logs %s: %w (stderr: %s)", d.runtime, h, err, strings.TrimSpace(stderr.String()))
		if isNotFound(werr) {
			return false, nil
		}
		return false, werr
	}
	marker := []byte(driver.JobStartMarker)
	return bytes.Contains(stdout.Bytes(), marker) || bytes.Contains(stderr.Bytes(), marker), nil
}

// ReclaimTiers frees data-root space. What a job creates through the shared
// docker socket carries none of our labels, so collecting it means deleting
// objects we can't prove are idle: those steps are drain-gated, and further
// gated on the operator declaring the daemon ours alone. Without that
// declaration reclaim is limited to our own build cache and unreferenced
// layers, and a shared daemon's disk stays the operator's to manage.
func (d *Driver) ReclaimTiers(ctx context.Context) []driver.Tier {
	tiers := []driver.Tier{
		// Buildkit prunes its own store under lease, so trimming beside a live
		// build costs that build cache, not correctness.
		{Name: "build cache", Run: func() error { return d.pruneBuilders(ctx) }},
		{Name: "dangling images", Run: func() error { return d.docker(ctx, "image", "prune", "-f") }},
	}
	if !d.exclusive {
		return tiers
	}
	if d.runtime == "docker" {
		// Jobs without a repo (scale-set demand) and jobs that bypass their
		// per-repo builder build on the daemon's default builder, which
		// pruneBuilders never sees. Buildkit prunes under lease, so no drain.
		// --keep-storage, not --reserved-space: native `builder prune` and
		// older buildx accept only the former. Podman has no `builder prune`.
		tiers = append(tiers, driver.Tier{Name: "default build cache", Run: func() error {
			return d.docker(ctx, "builder", "prune", "-f",
				fmt.Sprintf("--keep-storage=%dGB", max(d.buildCacheGB/pressureCacheDivisor, 1)))
		}})
	}
	return append(tiers,
		// `until` filters on creation time, not last use, so it can't express
		// "idle for an hour" — the drain is what makes these safe, not a filter.
		driver.Tier{Name: "exited containers", Drain: true, Run: func() error {
			return d.docker(ctx, "container", "prune", "-f", "--filter", "label!="+ownerLabel)
		}},
		// A stopped builder's state volume holds a whole repo's warm cache and
		// reads as unused, so this has to sit below the build-cache trim, which
		// keeps what fits.
		driver.Tier{Name: "unused volumes", Drain: true, Run: func() error {
			return d.docker(ctx, "volume", "prune", "-f")
		}},
		// Evicts the runner and buildkit images too when nothing holds them; the
		// drain means no job is waiting on the re-pull that costs.
		driver.Tier{Name: "unused images", Drain: true, Run: func() error {
			return d.docker(ctx, "image", "prune", "-a", "-f")
		}},
	)
}

// pruneBuilders trims each repo's build cache to a fraction of its steady-state
// cap. The budget stays per-repo under pressure: a filesystem-level target would
// make the first builder flush its entire cache reaching for a mark the other
// repos' caches hold down, then the next, until the whole host is cold.
func (d *Driver) pruneBuilders(ctx context.Context) error {
	entries, err := os.ReadDir(filepath.Join(d.stateDir, "buildx"))
	if os.IsNotExist(err) {
		return nil // build_cache off, or no build has run yet
	}
	if err != nil {
		return err
	}
	d.gcMu.Lock() // the maintenance sweep rms builders; don't prune one mid-remove
	defer d.gcMu.Unlock()
	keepGB := max(d.buildCacheGB/pressureCacheDivisor, 1)
	var firstErr error
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		env := append(os.Environ(), "BUILDX_CONFIG="+filepath.Join(d.stateDir, "buildx", e.Name()))
		if err := d.buildx(ctx, env, "prune", "--builder", "ushr-"+e.Name(), "--force",
			fmt.Sprintf("--reserved-space=%dGB", keepGB)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (d *Driver) docker(ctx context.Context, args ...string) error {
	_, err := d.dockerOut(ctx, args...)
	return err
}

func (d *Driver) dockerOut(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, d.runtime, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%s %s: %w (stderr: %s)",
			d.runtime, strings.Join(redact(args), " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// redact hides the JIT config blob from logged command lines — it is a
// single-use credential and there is no value in having it in the logs.
func redact(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	for i, a := range out {
		if a == "--jitconfig" && i+1 < len(out) {
			out[i+1] = "<redacted>"
		}
	}
	return out
}
