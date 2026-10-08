// Package sshrunner holds the SSH-based runner bootstrap shared by the macOS
// drivers (tart, lume). Every function is driver-agnostic: it operates on a
// guest user+IP, independent of how the VM was provisioned or named.
package sshrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/paddo-tech/ushr/internal/actioncache"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/driver"
)

// SSHTimeout bounds WaitForSSH: how long to wait for a freshly booted guest to
// accept connections before giving up.
const SSHTimeout = 60 * time.Second

const runnerLog = "/tmp/runner.log"

// RandomName returns a unique VM name with the given prefix. The prefix lets
// List() reconcile our VMs from everything else on the host.
func RandomName(prefix string) string {
	return domain.RandomName(prefix)
}

// WaitForSSH blocks until an SSH session to the guest succeeds or SSHTimeout
// elapses.
func WaitForSSH(ctx context.Context, user, ip string) error {
	ctx, cancel := context.WithTimeout(ctx, SSHTimeout)
	defer cancel()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := ssh(ctx, user, ip, "true"); err == nil {
				return nil
			}
		}
	}
}

// Alive reports whether the runner's Listener process is up inside the guest.
// Absence => the job finished (cleanly or otherwise).
func Alive(ctx context.Context, user, ip string) (bool, error) {
	err := ssh(ctx, user, ip, "pgrep -f Runner.Listener > /dev/null")
	if err == nil {
		return true, nil
	}
	// pgrep returns 1 when no matching process exists — treat as alive=false.
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// StartedJob reports whether the runner has begun a job, read from its log.
// A missing log or absent marker counts as not-started (exit 1), so a runner
// idling after a pre-pickup cancel is reported false rather than erroring.
func StartedJob(ctx context.Context, user, ip string) (bool, error) {
	cmd := fmt.Sprintf("test -f %s && grep -q %s %s", runnerLog, shellQuote(driver.JobStartMarker), runnerLog)
	err := ssh(ctx, user, ip, cmd)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// A login shell loads the guest's toolchain environment before starting the runner.
//
// jitConfig is passed as a positional parameter ($1), not interpolated into the
// script body, so the token can't break out of the shell quoting. The outer
// shell receives it shell-quoted (one ssh-level reparse) and threads it into
// the inner login shell via `_ "$1"`.
//
// A non-empty actionCache is the guest path of the action archive cache.
func InstallAndStart(ctx context.Context, user, ip string, runner Runner, jitConfig, actionCache string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := install(ctx, user, ip, runner); err != nil {
		return err
	}
	export := ""
	if actionCache != "" {
		export = fmt.Sprintf("export %s=%s\n", actioncache.EnvVar, shellQuote(actionCache))
	}
	script := export + fmt.Sprintf(`set -e
cd ~/actions-runner
nohup bash -lc './run.sh --jitconfig "$1"' _ "$1" > %s 2>&1 < /dev/null & disown
sleep 2
pgrep -f Runner.Listener > /dev/null
`, runnerLog)

	cmd := exec.CommandContext(ctx, "ssh",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		fmt.Sprintf("%s@%s", user, ip),
		"bash -s "+shellQuote(jitConfig),
	)
	cmd.Stdin = strings.NewReader(script)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ssh %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// install replaces the guest's runner binaries when their version differs from
// runner's. It keeps the rest of ~/actions-runner, so a tool cache the image
// bakes into _work survives.
func install(ctx context.Context, user, ip string, runner Runner) error {
	check := fmt.Sprintf(`test "$(actions-runner/bin/Runner.Listener --version 2>/dev/null)" = %s`, shellQuote(runner.Version))
	if ssh(ctx, user, ip, check) == nil {
		return nil
	}
	cmd := exec.CommandContext(ctx, "scp",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		runner.Tar, fmt.Sprintf("%s@%s:runner.tar.gz", user, ip),
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("scp runner %s: %w (stderr: %s)", runner.Version, err, strings.TrimSpace(stderr.String()))
	}
	if err := ssh(ctx, user, ip, "mkdir -p actions-runner && rm -rf actions-runner/bin actions-runner/externals && tar xzf runner.tar.gz -C actions-runner && rm runner.tar.gz"); err != nil {
		return fmt.Errorf("extract runner %s: %w", runner.Version, err)
	}
	return nil
}

// shellQuote wraps s in single quotes so a remote shell treats it as one
// literal token, escaping any embedded single quote as '\”.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func ssh(ctx context.Context, user, ip, command string) error {
	cmd := exec.CommandContext(ctx, "ssh",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=5",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		fmt.Sprintf("%s@%s", user, ip),
		command,
	)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}
