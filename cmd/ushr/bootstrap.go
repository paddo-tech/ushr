package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/term"

	"github.com/paddo-tech/ushr/internal/config"
)

// confirm asks a y/n question on the terminal. Without a TTY (piped stdin,
// CI) only -y answers yes — some prompts guard multi-GB downloads, so a
// silent default-yes is never safe non-interactively.
func confirm(prompt string, def, assumeYes bool) bool {
	if assumeYes {
		return true
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		fmt.Printf("%s — no TTY, assuming no (pass -y to accept)\n", prompt)
		return false
	}
	hint := "[y/N]"
	if def {
		hint = "[Y/n]"
	}
	fmt.Printf("%s %s ", prompt, hint)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		// EOF mid-prompt gets the no-TTY treatment: never yes by accident.
		fmt.Println("no (input closed)")
		return false
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		return def
	}
}

// runStream runs a command with the user's terminal attached, so installs and
// image pulls show their own progress and can prompt (sudo, brew).
func runStream(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// defaultAgentYAML is the platform-default agent config written on first run.
func defaultAgentYAML(name string) string {
	arch := "X64"
	if runtime.GOARCH == "arm64" {
		arch = "ARM64"
	}
	if runtime.GOOS == "darwin" {
		return fmt.Sprintf(`version: "1"
# controller_url/token here serve a local OSS controller. The hosted plane
# (`+"`ushr login`"+`) writes credentials.yaml alongside, which overrides both.
controller_url: http://127.0.0.1:7080
token: ""
name: %s
labels:
  - self-hosted
  - macOS
  - %s

driver:
  type: tart
  image: paddo-runner-mac     # base VM image cloned for each ephemeral runner
  capacity: 2                 # Apple's macOS guest VM cap

# The agent holds the GitHub App keys and mints JIT runner configs itself.
# `+"`ushr setup`"+` and `+"`ushr login`"+` fill in orgs/repos entries here.
source:
  type: poll
  interval: 30s
`, name, arch)
	}
	return fmt.Sprintf(`version: "1"
# controller_url/token here serve a local OSS controller. The hosted plane
# (`+"`ushr login`"+`) writes credentials.yaml alongside, which overrides both.
controller_url: http://127.0.0.1:7080
token: ""
name: %s
labels:
  - self-hosted
  - Linux
  - %s

driver:
  type: docker                # autodetects docker, then podman
  image: ghcr.io/actions/actions-runner:latest
  capacity: 4                 # size to host CPU/RAM
  # Per-repo persistent build caches are on by default (needs the host
  # docker-buildx plugin; jobs run uncached without it). Opt out with:
  # build_cache: false

# The agent holds the GitHub App keys and mints JIT runner configs itself.
# `+"`ushr setup`"+` and `+"`ushr login`"+` fill in orgs/repos entries here.
source:
  type: poll
  interval: 30s
`, name, arch)
}

// ensureAgentConfig writes the platform-default agent config when none exists.
func ensureAgentConfig(path string) (created bool, err error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	name := ""
	if host, err := os.Hostname(); err == nil {
		name = sanitizeName(host)
	}
	if name == "" {
		name = "ushr-agent"
	}
	if err := os.WriteFile(path, []byte(defaultAgentYAML(name)), 0o644); err != nil {
		return false, err
	}
	return true, nil
}

// agentBinaryPath is where the service definitions expect the agent binary.
func agentBinaryPath() string {
	return filepath.Join(os.Getenv("HOME"), ".local", "bin", "ushr-agent")
}

// preflight verifies (and offers to install) what the configured driver needs
// before the agent starts, so a missing prereq is a guided prompt here instead
// of an exec error buried in the daemon log.
func preflight(ctx context.Context, cfg *config.Agent, assumeYes bool) error {
	switch cfg.Driver.Type {
	case config.DriverTypeTart, "":
		return preflightTart(ctx, cfg.Driver.Image, assumeYes)
	case config.DriverTypeDocker:
		return preflightDocker(ctx, cfg.Driver, assumeYes)
	case config.DriverTypeLume:
		if _, err := exec.LookPath("lume"); err != nil {
			return errors.New("lume not found on PATH — install it (https://github.com/trycua/lume), then re-run")
		}
		return nil
	default:
		return nil
	}
}

// cirrusBaseImage is a ready-to-use macOS runner base (ssh admin/admin, dev
// tools preinstalled); the agent installs the Actions runner into each clone
// itself, so it works as a runner image out of the box.
const cirrusBaseImage = "ghcr.io/cirruslabs/macos-runner:tahoe"

func preflightTart(ctx context.Context, image string, assumeYes bool) error {
	if _, err := exec.LookPath("tart"); err != nil {
		if _, brewErr := exec.LookPath("brew"); brewErr != nil {
			return errors.New("tart not found, and Homebrew isn't installed to get it.\n  Install Homebrew (https://brew.sh), then re-run — or install tart manually: https://tart.run")
		}
		fmt.Println("==> tart (the macOS VM engine) is not installed.")
		if !confirm("    Install it now with `brew install openai/tools/tart`?", true, assumeYes) {
			return errors.New("tart is required for the tart driver — install it and re-run")
		}
		if err := runStream(ctx, "brew", "install", "openai/tools/tart"); err != nil {
			return fmt.Errorf("brew install tart: %w", err)
		}
	}
	if image == "" {
		image = "paddo-runner-mac"
	}
	ok, err := tartImageExists(ctx, image)
	if err != nil {
		return fmt.Errorf("tart list: %w", err)
	}
	if ok {
		fmt.Printf("==> tart image %q present ✓\n", image)
		return nil
	}
	fmt.Printf("==> Runner VM image %q not found.\n", image)
	fmt.Printf("    The %s base works out of the box (the agent installs the\n", cirrusBaseImage)
	fmt.Println("    Actions runner into each clone). This is a large download (tens of GB).")
	if !confirm(fmt.Sprintf("    Fetch it now with `tart clone %s %s`?", cirrusBaseImage, image), true, assumeYes) {
		return errors.New("setup paused: the runner image is required; run ushr login to resume")
	}
	if err := runStream(ctx, "tart", "clone", cirrusBaseImage, image); err != nil {
		return fmt.Errorf("tart clone: %w", err)
	}
	fmt.Printf("==> Image %q ready ✓\n", image)
	return nil
}

func tartImageExists(ctx context.Context, image string) (bool, error) {
	out, err := exec.CommandContext(ctx, "tart", "list", "--format", "json").Output()
	if err != nil {
		return false, err
	}
	var entries []struct {
		Name   string `json:"Name"`
		Source string `json:"Source"`
	}
	if err := json.Unmarshal(out, &entries); err != nil {
		return false, err
	}
	for _, e := range entries {
		if e.Source == "local" && e.Name == image {
			return true, nil
		}
	}
	return false, nil
}

func preflightDocker(ctx context.Context, drv config.DriverConfig, assumeYes bool) error {
	rt := drv.Runtime
	if rt == "" {
		for _, c := range []string{"docker", "podman"} {
			if _, err := exec.LookPath(c); err == nil {
				rt = c
				break
			}
		}
	} else if _, err := exec.LookPath(rt); err != nil {
		return fmt.Errorf("configured runtime %q not found on PATH", rt)
	}
	if rt == "" {
		installCmd := podmanInstallCmd()
		if installCmd == "" {
			return errors.New("no container runtime (docker/podman) and no supported package manager — install one manually, then re-run")
		}
		fmt.Println("==> No container runtime (docker/podman) found.")
		if !confirm(fmt.Sprintf("    Install podman now with `%s`?", installCmd), true, assumeYes) {
			return errors.New("a container runtime is required for the docker driver — install docker or podman and re-run")
		}
		if err := runStream(ctx, "sh", "-c", installCmd); err != nil {
			return fmt.Errorf("install podman: %w", err)
		}
		rt = "podman"
	}
	if err := exec.CommandContext(ctx, rt, "info").Run(); err != nil {
		return fmt.Errorf("container runtime is not ready: start %s, then run ushr login again", rt)
	}
	fmt.Printf("==> Container runtime: %s ready ✓\n", rt)
	if drv.BuildCache == nil || *drv.BuildCache {
		if exec.CommandContext(ctx, rt, "buildx", "version").Run() != nil {
			fmt.Println("    note: buildx plugin not found — builds run uncached until it's installed")
			fmt.Println("    (debian/ubuntu: sudo apt-get install docker-buildx)")
		}
	}
	return nil
}

func podmanInstallCmd() string {
	switch {
	case commandExists("dnf"):
		return "sudo dnf install -y podman"
	case commandExists("apt-get"):
		return "sudo apt-get update && sudo apt-get install -y podman"
	case commandExists("pacman"):
		return "sudo pacman -S --noconfirm podman"
	case commandExists("zypper"):
		return "sudo zypper install -y podman"
	}
	return ""
}

func commandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}
