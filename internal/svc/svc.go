// Package svc installs and controls the ushr launchd/systemd user services,
// so the CLI can take a host from enrolled to running without a repo checkout
// or hand-run launchctl/systemctl incantations.
package svc

import (
	"embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

//go:embed templates
var templates embed.FS

// Unit is one of the two ushr daemons.
type Unit string

const (
	Agent      Unit = "agent"
	Controller Unit = "controller"
)

func launchdLabel(u Unit) string { return "tech.paddo.ushr-" + string(u) }
func systemdUnit(u Unit) string  { return "ushr-" + string(u) + ".service" }

// Install writes the service definition for u (launchd plist or systemd user
// unit) and creates the log/work directories it references. It does not start
// the service; call Restart. Overwrites an existing definition so template
// updates ship with the binary.
func Install(u Unit) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	switch runtime.GOOS {
	case "darwin":
		for _, d := range []string{
			filepath.Join(home, "Library", "Logs", "ushr"),
			filepath.Join(home, "Library", "Application Support", "ushr"),
			filepath.Join(home, "Library", "LaunchAgents"),
		} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				return "", err
			}
		}
		b, err := templates.ReadFile("templates/launchd/" + launchdLabel(u) + ".plist")
		if err != nil {
			return "", err
		}
		dst := filepath.Join(home, "Library", "LaunchAgents", launchdLabel(u)+".plist")
		body := strings.ReplaceAll(string(b), "__HOME__", home)
		if err := os.WriteFile(dst, []byte(body), 0o644); err != nil {
			return "", err
		}
		return dst, nil
	case "linux":
		dir := filepath.Join(home, ".config", "systemd", "user")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		b, err := templates.ReadFile("templates/systemd/" + systemdUnit(u))
		if err != nil {
			return "", err
		}
		dst := filepath.Join(dir, systemdUnit(u))
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			return "", err
		}
		return dst, nil
	default:
		return "", fmt.Errorf("service install not supported on %s", runtime.GOOS)
	}
}

// Restart (re)starts u, loading/enabling it first if needed.
func Restart(u Unit) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	switch runtime.GOOS {
	case "darwin":
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		plist := filepath.Join(home, "Library", "LaunchAgents", launchdLabel(u)+".plist")
		// bootout+bootstrap is the idempotent (re)load: bootout fails harmlessly
		// when the service isn't loaded, bootstrap picks up plist changes and
		// transiently fails while the previous daemon is still exiting — retry.
		_ = exec.Command("launchctl", "bootout", domain+"/"+launchdLabel(u)).Run()
		var lastOut []byte
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Second)
			}
			lastOut, lastErr = exec.Command("launchctl", "bootstrap", domain, plist).CombinedOutput()
			if lastErr == nil {
				return nil
			}
		}
		return fmt.Errorf("launchctl bootstrap: %w (%s) — note: launchd user services need a logged-in GUI session on macOS", lastErr, strings.TrimSpace(string(lastOut)))
	case "linux":
		if _, err := exec.LookPath("systemctl"); err != nil {
			return fmt.Errorf("systemctl not found — ushr manages services via systemd user units; on a non-systemd distro run ushr-agent under your init system manually")
		}
		// Linger keeps user services alive after logout — a headless runner box
		// is useless without it.
		_ = exec.Command("loginctl", "enable-linger").Run()
		if out, err := exec.Command("systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl daemon-reload: %w (%s)", err, strings.TrimSpace(string(out)))
		}
		if out, err := exec.Command("systemctl", "--user", "enable", "--now", systemdUnit(u)).CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl enable: %w (%s)", err, strings.TrimSpace(string(out)))
		}
		if out, err := exec.Command("systemctl", "--user", "restart", systemdUnit(u)).CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl restart: %w (%s)", err, strings.TrimSpace(string(out)))
		}
		return nil
	default:
		return fmt.Errorf("service control not supported on %s", runtime.GOOS)
	}
}

// Running reports whether u is loaded and running.
func Running(u Unit) bool {
	switch runtime.GOOS {
	case "darwin":
		target := fmt.Sprintf("gui/%d/%s", os.Getuid(), launchdLabel(u))
		out, err := exec.Command("launchctl", "print", target).Output()
		return err == nil && strings.Contains(string(out), "state = running")
	case "linux":
		return exec.Command("systemctl", "--user", "is-active", "--quiet", systemdUnit(u)).Run() == nil
	default:
		return false
	}
}

// LogHint is the command a user runs to watch u's logs.
func LogHint(u Unit) string {
	if runtime.GOOS == "darwin" {
		return fmt.Sprintf("tail -f ~/Library/Logs/ushr/%s.log", u)
	}
	return fmt.Sprintf("journalctl --user -u ushr-%s -f", u)
}
