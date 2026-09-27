package update

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var healthTimeout = 90 * time.Second

type State struct {
	Target        string `json:"target"`
	Request       string `json:"request"`
	Pending       bool   `json:"pending"`
	FailedVersion string `json:"failed_version"`
	FailedRequest string `json:"failed_request"`
	Error         string `json:"error"`
}

func readState(exe string) (State, error) {
	var s State
	b, err := os.ReadFile(exe + ".update.json")
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	err = json.Unmarshal(b, &s)
	return s, err
}

func syncDirectory(exe string) error {
	dir, err := os.Open(filepath.Dir(exe))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func writeState(exe string, s State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(exe+".update.tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(exe+".update.tmp", exe+".update.json"); err != nil {
		return err
	}
	return syncDirectory(exe)
}

func Status() (string, string, string) {
	return os.Getenv("USHR_UPDATE_FAILED_VERSION"), os.Getenv("USHR_UPDATE_FAILED_REQUEST"), os.Getenv("USHR_UPDATE_ERROR")
}

func Healthy(ctx context.Context) error {
	path := os.Getenv("USHR_UPDATE_HEALTH_FILE")
	if path == "" {
		return nil
	}
	if err := os.WriteFile(path+".worker", []byte("ready"), 0o600); err != nil {
		return err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path + ".accepted"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// The recovery process survives updates; each candidate supervisor owns its own worker.
func Supervise(config string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	state, err := readState(exe)
	if err != nil {
		return err
	}
	if state.Pending {
		if err = rollback(exe, &state); err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	for {
		health := exe + ".healthy"
		for _, path := range []string{health, health + ".worker", health + ".accepted"} {
			if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		cmd := exec.Command(exe, "-supervised", "-config", config)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		failedVersion, failedRequest, updateError := state.FailedVersion, state.FailedRequest, state.Error
		if state.Pending {
			failedVersion, failedRequest, updateError = "", "", ""
		}
		cmd.Env = append(os.Environ(), "USHR_UPDATE_HEALTH_FILE="+health, "USHR_UPDATE_FAILED_VERSION="+failedVersion, "USHR_UPDATE_FAILED_REQUEST="+failedRequest, "USHR_UPDATE_ERROR="+updateError)
		childErr := cmd.Start()
		if childErr == nil {
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			timer := time.NewTimer(healthTimeout)
			tick := time.NewTicker(250 * time.Millisecond)
			ready, running := false, true
			for running {
				select {
				case <-ctx.Done():
					stopCandidate(cmd, done)
					timer.Stop()
					tick.Stop()
					return ctx.Err()
				case childErr = <-done:
					running = false
				case <-tick.C:
					if !ready {
						if _, err = os.Stat(health); err == nil {
							pending := state
							if state.Pending {
								state.Pending = false
								state.FailedVersion, state.FailedRequest, state.Error = "", "", ""
								err = writeState(exe, state)
							}
							if err == nil {
								err = os.WriteFile(health+".accepted", []byte("ready"), 0o600)
							}
							if err != nil {
								state, childErr = pending, err
								stopCandidate(cmd, done)
								running = false
								continue
							}
							_ = os.Remove(exe + ".previous")
							ready = true
						}
					}
				case <-timer.C:
					if state.Pending {
						stopCandidate(cmd, done)
						childErr = errors.New("candidate health timeout")
						running = false
					}
				}
			}
			timer.Stop()
			tick.Stop()
			// A crashed supervisor must not leave an unacknowledged worker behind.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		if state.Pending {
			if err = rollback(exe, &state); err != nil {
				return err
			}
			continue
		}
		var status *exec.ExitError
		if !errors.As(childErr, &status) || status.ExitCode() != ExitReady {
			return childErr
		}
		state, err = readState(exe)
		if err != nil {
			return err
		}
		if !releaseVersion.MatchString(state.Target) {
			return errors.New("invalid staged update version")
		}
		if err = install(exe, &state); err != nil {
			if state.Pending {
				if err = rollback(exe, &state); err != nil {
					return err
				}
			} else {
				state.FailedVersion, state.FailedRequest = state.Target, state.Request
				state.Error = "Agent installation failed. Previous version retained."
				_ = os.Remove(exe + ".next")
				if writeErr := writeState(exe, state); writeErr != nil {
					slog.Error("could not persist installation failure", "err", writeErr)
				}
			}
		}
	}
}

func stopCandidate(cmd *exec.Cmd, done <-chan error) {
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }()
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func SuperviseWorker(config string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "-worker", "-config", config)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err = cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	ready := false
	for {
		select {
		case <-ctx.Done():
			_ = cmd.Process.Signal(syscall.SIGTERM)
			<-done
			return ctx.Err()
		case err = <-done:
			var status *exec.ExitError
			if errors.As(err, &status) && status.ExitCode() == ExitReady {
				return ErrReady
			}
			return err
		case <-tick.C:
			if !ready {
				health := os.Getenv("USHR_UPDATE_HEALTH_FILE")
				if _, err = os.Stat(health + ".worker"); err == nil {
					if err = os.WriteFile(health, []byte("ready"), 0o600); err != nil {
						_ = cmd.Process.Signal(syscall.SIGTERM)
						<-done
						return err
					}
					ready = true
				}
			}
		}
	}
}

func install(exe string, state *State) error {
	if err := os.Remove(exe + ".previous"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Link(exe, exe+".previous"); err != nil {
		return err
	}
	if err := syncDirectory(exe); err != nil {
		return err
	}
	state.Pending = true
	if err := writeState(exe, *state); err != nil {
		return err
	}
	if err := os.Rename(exe+".next", exe); err != nil {
		return err
	}
	return syncDirectory(exe)
}

func rollback(exe string, state *State) error {
	if err := os.Remove(exe + ".restore"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Keep the backup until the recovery journal durably records the restored binary.
	if err := os.Link(exe+".previous", exe+".restore"); err != nil {
		return fmt.Errorf("restore previous agent: %w", err)
	}
	if err := os.Rename(exe+".restore", exe); err != nil {
		return err
	}
	if err := syncDirectory(exe); err != nil {
		return err
	}
	state.Pending = false
	state.FailedVersion, state.FailedRequest = state.Target, state.Request
	state.Error = "New agent did not become healthy. Previous version restored."
	if err := writeState(exe, *state); err != nil {
		return err
	}
	_ = os.Remove(exe + ".previous")
	return nil
}
