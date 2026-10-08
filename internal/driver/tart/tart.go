// Package tart binds the tart CLI (Apple Virtualization.framework) to the
// generic VM driver in internal/driver/vmdriver.
package tart

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/driver/vmdriver"
)

// Defaults reflect our M4 Pro setup. Override via Options.
const (
	DefaultBaseImage = "paddo-runner-mac"
	DefaultSSHUser   = "admin"
	DefaultCapacity  = 2 // Apple's macOS guest VM cap per host
	// DefaultMinFreeGB is the free-space floor under which the agent stops
	// accepting work. Sized for macOS: a clone starts near-free (APFS
	// copy-on-write) but a full Xcode job writes tens of GB into it, and two run
	// at once.
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

// New constructs a tart-backed VM driver. Empty Options fields fall back to Default*.
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

// commands implements vmdriver.Commands via the tart CLI.
type commands struct{}

var _ vmdriver.Commands = commands{}

func (commands) Clone(ctx context.Context, base, name string) error {
	_, err := vmdriver.Run(ctx, "tart", "clone", base, name)
	return err
}

// Start launches `tart run --no-graphics` in the background. Naming the share
// after its folder mounts it at /Volumes/My Shared Files/<folder>, as lume does.
func (commands) Start(name, share string) error {
	args := []string{"run", name, "--no-graphics"}
	if share != "" {
		args = append(args, "--dir="+filepath.Base(share)+":"+share+":ro")
	}
	return exec.Command("tart", args...).Start()
}

func (commands) Stop(ctx context.Context, name string) error {
	_, err := vmdriver.Run(ctx, "tart", "stop", name)
	return err
}

func (commands) Delete(ctx context.Context, name string) error {
	_, err := vmdriver.Run(ctx, "tart", "delete", name)
	return err
}

func (commands) List(ctx context.Context, prefix string) ([]driver.SlotHandle, error) {
	out, err := vmdriver.Run(ctx, "tart", "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	return parseList(out, prefix)
}

// State maps `tart list` status. tart's list carries no IP, so it returns "";
// the generic driver falls back to IP().
func (commands) State(ctx context.Context, name string) (driver.Status, string, error) {
	out, err := vmdriver.Run(ctx, "tart", "list", "--format", "json")
	if err != nil {
		return driver.StatusUnknown, "", err
	}
	st, err := parseVMState(out, name)
	return st, "", err
}

// StorePath is tart's home: VM images plus the OCI and IPSW caches.
func (commands) StorePath() string {
	if h := os.Getenv("TART_HOME"); h != "" {
		return h
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "" // unknown store, not a relative path that would measure the cwd
	}
	return filepath.Join(home, ".tart")
}

// PruneCaches trims the OCI and IPSW caches only — `--entries=vms` would judge
// local VMs by last access and could delete the base image we clone from.
// olderThanDays zero flushes the caches entirely, costing a re-pull the next
// time the operator refreshes the image.
func (commands) PruneCaches(ctx context.Context, olderThanDays int) error {
	args := []string{"prune", "--entries", "caches"}
	if olderThanDays > 0 {
		args = append(args, "--older-than", strconv.Itoa(olderThanDays))
	} else {
		args = append(args, "--space-budget", "0")
	}
	_, err := vmdriver.Run(ctx, "tart", args...)
	return err
}

// IP reports the VM's IP. Before a lease `tart ip` exits non-zero, surfaced as
// an error (callers retry on it), so a genuine failure stays visible rather
// than being masked as a not-ready empty.
func (commands) IP(ctx context.Context, name string) (string, error) {
	out, err := vmdriver.Run(ctx, "tart", "ip", name)
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("empty IP for %s", name)
	}
	return ip, nil
}
