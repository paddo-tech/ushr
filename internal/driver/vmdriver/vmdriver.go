// Package vmdriver provides a generic ephemeral-VM Driver shared by the macOS
// drivers (tart, lume). The provision/status/teardown flow and the SSH runner
// bootstrap are identical across them; only the VM CLI differs, so that part
// lives behind the Commands interface and everything else is shared here.
package vmdriver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/driver/sshrunner"
)

const bootTimeout = 90 * time.Second

// runnerRefreshInterval is well inside the 30 days GitHub gives a runner to
// update after a new release.
const runnerRefreshInterval = 6 * time.Hour

// Run executes a VM CLI command and returns its stdout. Shared by the tart and
// lume bindings so their exec/stderr/error handling can't drift.
func Run(ctx context.Context, bin string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%s %s: %w (stderr: %s)",
			bin, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// Commands abstracts a VM CLI (tart, lume). All methods operate on VM names.
type Commands interface {
	// Clone makes a copy-on-write copy of base as name.
	Clone(ctx context.Context, base, name string) error
	// Start boots name headless, detached so it survives the parent process
	// (a controller restart can then reconcile live VMs via List). A non-empty
	// share is a host directory to expose read-only, at guestShare(share) in a
	// macOS guest.
	Start(name, share string) error
	Stop(ctx context.Context, name string) error
	Delete(ctx context.Context, name string) error
	// List returns our VMs — those whose name matches prefix.
	List(ctx context.Context, prefix string) ([]driver.SlotHandle, error)
	// State returns name's status and, when the CLI exposes it cheaply in the
	// same call (e.g. lume ls), its IP — empty otherwise, in which case the
	// generic layer falls back to IP().
	State(ctx context.Context, name string) (driver.Status, string, error)
	// IP returns name's current IP. Before a lease it returns "" or a non-nil
	// error; callers retry on both.
	IP(ctx context.Context, name string) (string, error)
	// StorePath is the directory the CLI keeps VM images and its download cache
	// in — the filesystem that fills up.
	StorePath() string
	// PruneCaches drops cached image downloads last used more than
	// olderThanDays ago; zero drops them all. It must never touch local VMs —
	// the base image the driver clones from is one of them.
	PruneCaches(ctx context.Context, olderThanDays int) error
}

// Config carries the runtime knobs shared by all VM drivers.
type Config struct {
	Base     string
	SSHUser  string
	Capacity int
	// ActionCache is the host's action archive cache directory, "" for none.
	ActionCache string
}

// guestShare is where a macOS guest automounts a share: both CLIs use the
// automount tag and name the share after the host folder.
func guestShare(host string) string {
	return filepath.Join("/Volumes/My Shared Files", filepath.Base(host))
}

// Driver is the generic VM driver, parameterized by a Commands implementation.
type Driver struct {
	cmd Commands
	cfg Config

	mu     sync.RWMutex
	runner sshrunner.Runner
}

var (
	_ driver.Driver      = (*Driver)(nil)
	_ driver.JobWatcher  = (*Driver)(nil)
	_ driver.DiskWatcher = (*Driver)(nil)
	_ driver.Reclaimer   = (*Driver)(nil)
	_ driver.Maintainer  = (*Driver)(nil)
)

// staleCacheDays is the age at which a cached image download is written off as
// unlikely to be pulled again.
const staleCacheDays = 7

// New constructs a Driver from a CLI binding and config.
func New(cmd Commands, cfg Config) *Driver {
	return &Driver{cmd: cmd, cfg: cfg}
}

func (d *Driver) Capacity() int { return d.cfg.Capacity }

func (d *Driver) DiskPath(context.Context) string { return d.cmd.StorePath() }

func (d *Driver) Provision(ctx context.Context, req driver.ProvisionRequest) (driver.SlotHandle, error) {
	name := req.Name
	handle := driver.SlotHandle(name)

	if err := d.cmd.Clone(ctx, d.cfg.Base, name); err != nil {
		return "", fmt.Errorf("clone %s -> %s: %w", d.cfg.Base, name, err)
	}
	share, guestCache := "", ""
	if fi, err := os.Stat(d.cfg.ActionCache); err == nil && fi.IsDir() {
		share, guestCache = d.cfg.ActionCache, guestShare(d.cfg.ActionCache)
	}
	if err := d.cmd.Start(name, share); err != nil {
		// Nothing booted yet, so a plain delete is enough cleanup.
		_ = d.cmd.Delete(context.Background(), name)
		return "", fmt.Errorf("start %s: %w", name, err)
	}
	ip, err := d.waitForIP(ctx, name)
	if err != nil {
		_ = d.Destroy(context.Background(), handle)
		return "", fmt.Errorf("wait for IP: %w", err)
	}
	if err := sshrunner.WaitForSSH(ctx, d.cfg.SSHUser, ip); err != nil {
		_ = d.Destroy(context.Background(), handle)
		return "", fmt.Errorf("wait for SSH: %w", err)
	}
	d.mu.RLock()
	if d.runner.Version == "" {
		d.mu.RUnlock()
		if err := d.refreshRunner(ctx); err != nil {
			_ = d.Destroy(context.Background(), handle)
			return "", fmt.Errorf("fetch runner: %w", err)
		}
		d.mu.RLock()
	}
	// Cache cleanup must wait until the guest has received its archive.
	err = sshrunner.InstallAndStart(ctx, d.cfg.SSHUser, ip, d.runner, req.JITToken, guestCache)
	d.mu.RUnlock()
	if err != nil {
		_ = d.Destroy(context.Background(), handle)
		return "", fmt.Errorf("install runner: %w", err)
	}
	return handle, nil
}

// Maintain keeps the host's runner at GitHub's latest release until ctx is
// cancelled. Provision copies it into any clone whose baked runner differs, so
// a stale base image still runs jobs.
func (d *Driver) Maintain(ctx context.Context) {
	t := time.NewTicker(runnerRefreshInterval)
	defer t.Stop()
	for {
		if err := d.refreshRunner(ctx); err != nil {
			slog.Warn("runner refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (d *Driver) refreshRunner(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cache, err := os.UserCacheDir()
	if err != nil {
		return err
	}
	r, err := sshrunner.FetchLatest(ctx, filepath.Join(cache, "ushr", "runner"), d.runner.Tar)
	if err != nil {
		return err
	}
	d.runner = r
	return nil
}

func (d *Driver) Status(ctx context.Context, h driver.SlotHandle) (driver.Status, error) {
	name := string(h)
	st, ip, err := d.cmd.State(ctx, name)
	if err != nil {
		return driver.StatusUnknown, err
	}
	if st != driver.StatusRunning {
		return st, nil
	}
	// Running: confirm the runner process is alive inside. Absence => the job
	// finished (cleanly or otherwise).
	if ip == "" {
		if ip, err = d.cmd.IP(ctx, name); err != nil {
			return driver.StatusUnknown, fmt.Errorf("ip lookup: %w", err)
		}
		if ip == "" {
			return driver.StatusUnknown, fmt.Errorf("running vm %q has no IP yet", name)
		}
	}
	alive, err := sshrunner.Alive(ctx, d.cfg.SSHUser, ip)
	if err != nil {
		return driver.StatusUnknown, err
	}
	if alive {
		return driver.StatusRunning, nil
	}
	return driver.StatusDone, nil
}

// StartedJob reports whether the runner has begun a job (vs idling after a
// pre-pickup cancel), letting the agent reap a stranded slot.
func (d *Driver) StartedJob(ctx context.Context, h driver.SlotHandle) (bool, error) {
	ip, err := d.cmd.IP(ctx, string(h))
	if err != nil {
		return false, err
	}
	if ip == "" {
		return false, fmt.Errorf("no IP for %s", string(h))
	}
	return sshrunner.StartedJob(ctx, d.cfg.SSHUser, ip)
}

func (d *Driver) Destroy(ctx context.Context, h driver.SlotHandle) error {
	name := string(h)
	// Stop is best-effort: the VM may already be stopped or never have started.
	_ = d.cmd.Stop(ctx, name)
	if err := d.cmd.Delete(ctx, name); err != nil {
		return fmt.Errorf("delete %s: %w", name, err)
	}
	return nil
}

func (d *Driver) List(ctx context.Context) ([]driver.SlotHandle, error) {
	return d.cmd.List(ctx, domain.RunnerNamePrefix)
}

// ReclaimTiers frees the CLI's image download caches. Neither step needs a
// drain: the caches hold pulled images, not the local VMs, so a running clone
// is untouched — the worst outcome is a re-pull the next time the operator
// refreshes the base image.
func (d *Driver) ReclaimTiers(ctx context.Context) []driver.Tier {
	return []driver.Tier{
		{Name: "stale image cache", Run: func() error { return d.cmd.PruneCaches(ctx, staleCacheDays) }},
		{Name: "image cache", Run: func() error { return d.cmd.PruneCaches(ctx, 0) }},
	}
}

func (d *Driver) waitForIP(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, bootTimeout)
	defer cancel()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-t.C:
			if ip, err := d.cmd.IP(ctx, name); err == nil && ip != "" {
				return ip, nil
			}
		}
	}
}
