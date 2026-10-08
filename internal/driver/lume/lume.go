// Package lume binds the lume CLI (MIT, Apple Virtualization.framework) to the
// generic VM driver in internal/driver/vmdriver. It is the macOS driver for
// distributed builds, where Tart's license forbids competing-use redistribution.
package lume

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/driver/vmdriver"
)

// Defaults reflect our M4 Pro setup. Override via Options.
const (
	DefaultBaseImage = "paddo-runner-mac"
	DefaultSSHUser   = "admin"
	DefaultCapacity  = 2 // Apple's macOS guest VM cap per host
	// DefaultMinFreeGB is the free-space floor under which the agent stops
	// accepting work — same shape as tart: a clone is cheap, the job that runs
	// in it is not.
	DefaultMinFreeGB = 60
)

// Options configures a Driver. Zero values fall back to Default* constants.
type Options struct {
	BaseImage string
	SSHUser   string
	Capacity  int
	// ActionCache is the host action archive cache to share into each VM.
	ActionCache string
}

// New constructs a lume-backed VM driver. Empty Options fields fall back to Default*.
func New(opts Options) *vmdriver.Driver {
	if opts.BaseImage == "" {
		opts.BaseImage = DefaultBaseImage
	}
	if opts.SSHUser == "" {
		opts.SSHUser = DefaultSSHUser
	}
	if opts.Capacity == 0 {
		opts.Capacity = DefaultCapacity
	}
	return vmdriver.New(commands{}, vmdriver.Config{
		Base:        opts.BaseImage,
		SSHUser:     opts.SSHUser,
		Capacity:    opts.Capacity,
		ActionCache: opts.ActionCache,
	})
}

// commands implements vmdriver.Commands via the lume CLI.
type commands struct{}

var _ vmdriver.Commands = commands{}

func (commands) Clone(ctx context.Context, base, name string) error {
	_, err := vmdriver.Run(ctx, "lume", "clone", base, name)
	return err
}

// Start launches `lume run --no-display` in the background.
func (commands) Start(name, share string) error {
	args := []string{"run", name, "--no-display"}
	if share != "" {
		args = append(args, "--shared-dir", share+":ro")
	}
	return exec.Command("lume", args...).Start()
}

func (commands) Stop(ctx context.Context, name string) error {
	_, err := vmdriver.Run(ctx, "lume", "stop", name)
	return err
}

// Delete passes --force: without it `lume delete` prompts interactively and hangs.
func (commands) Delete(ctx context.Context, name string) error {
	_, err := vmdriver.Run(ctx, "lume", "delete", name, "--force")
	return err
}

func (commands) List(ctx context.Context, prefix string) ([]driver.SlotHandle, error) {
	out, err := vmdriver.Run(ctx, "lume", "ls", "-f", "json")
	if err != nil {
		return nil, err
	}
	return parseList(out, prefix)
}

// State reads status AND IP from a single `lume ls` payload, so the generic
// driver needs no separate IP() call while the VM is running.
func (commands) State(ctx context.Context, name string) (driver.Status, string, error) {
	out, err := vmdriver.Run(ctx, "lume", "ls", "-f", "json")
	if err != nil {
		return driver.StatusUnknown, "", err
	}
	return parseVM(out, name)
}

// PruneCaches is a no-op: lume's CLI exposes no cache prune, so reclaiming on a
// lume host is limited to deleting orphaned VMs.
func (commands) PruneCaches(context.Context, int) error { return nil }

// StorePath is lume's default VM store.
func (commands) StorePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "" // unknown store, not a relative path that would measure the cwd
	}
	return filepath.Join(home, ".lume")
}

func (commands) IP(ctx context.Context, name string) (string, error) {
	out, err := vmdriver.Run(ctx, "lume", "get", name, "-f", "json")
	if err != nil {
		return "", err
	}
	return parseIP(out) // "" (no error) until the guest gets a DHCP lease
}
