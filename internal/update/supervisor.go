package update

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var healthTimeout = 90 * time.Second

type State struct {
	Target        string `json:"target"`
	Pending       bool   `json:"pending"`
	FailedVersion string `json:"failed_version"`
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

func writeState(exe string, s State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if err = os.WriteFile(exe+".update.tmp", b, 0o600); err != nil {
		return err
	}
	return os.Rename(exe+".update.tmp", exe+".update.json")
}

func Status() (string, string) {
	return os.Getenv("USHR_UPDATE_FAILED_VERSION"), os.Getenv("USHR_UPDATE_ERROR")
}

func Healthy(ctx context.Context) error {
	path := os.Getenv("USHR_UPDATE_HEALTH_FILE")
	if path == "" {
		return nil
	}
	if err := os.WriteFile(path, []byte("ready"), 0o600); err != nil {
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

// The supervisor retains the old executable until the candidate completes an authenticated poll.
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
	adoptPID := os.Getenv("USHR_UPDATE_CHILD_PID")
	_ = os.Unsetenv("USHR_UPDATE_CHILD_PID")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	for {
		health := exe + ".healthy"
		ready := adoptPID != ""
		if !ready {
			_ = os.Remove(health)
			_ = os.Remove(health + ".accepted")
		}
		cmd := exec.Command(exe, "-worker", "-config", config)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		failedVersion, updateError := state.FailedVersion, state.Error
		if state.Pending {
			failedVersion, updateError = "", ""
		}
		cmd.Env = append(os.Environ(), "USHR_UPDATE_HEALTH_FILE="+health, "USHR_UPDATE_FAILED_VERSION="+failedVersion, "USHR_UPDATE_ERROR="+updateError)
		if adoptPID != "" {
			pid, parseErr := strconv.Atoi(adoptPID)
			if parseErr != nil || pid <= 0 {
				return errors.New("invalid supervised worker PID")
			}
			cmd.Process, err = os.FindProcess(pid)
			adoptPID = ""
			if err == nil {
				err = os.WriteFile(health+".accepted", []byte("ready"), 0o600)
			}
		} else {
			err = cmd.Start()
		}
		if err != nil {
			if state.Pending {
				if err = rollback(exe, &state); err != nil {
					return err
				}
				continue
			}
			return err
		}
		done := make(chan error, 1)
		go func() {
			status, waitErr := cmd.Process.Wait()
			if waitErr == nil && !status.Success() {
				waitErr = &exec.ExitError{ProcessState: status}
			}
			done <- waitErr
		}()

		timer := time.NewTimer(healthTimeout)
		tick := time.NewTicker(250 * time.Millisecond)
		var childErr error
		running := true
		for running {
			select {
			case <-ctx.Done():
				_ = cmd.Process.Signal(syscall.SIGTERM)
				<-done
				timer.Stop()
				tick.Stop()
				return ctx.Err()
			case childErr = <-done:
				running = false
			case <-tick.C:
				if !ready {
					if _, e := os.Stat(health); e == nil {
						if state.Pending {
							state.Pending = false
							state.FailedVersion, state.Error = "", ""
							if err = writeState(exe, state); err != nil {
								_ = cmd.Process.Signal(syscall.SIGTERM)
								<-done
								timer.Stop()
								tick.Stop()
								return err
							}
							_ = os.Remove(exe + ".previous")
							// Exec preserves the worker while the new supervisor accepts its health check.
							env := append(os.Environ(), "USHR_UPDATE_CHILD_PID="+strconv.Itoa(cmd.Process.Pid))
							if err := syscall.Exec(exe, []string{exe, "-config", config}, env); err != nil {
								slog.Error("could not replace update supervisor", "err", err)
							}
						}
						if err = os.WriteFile(health+".accepted", []byte("ready"), 0o600); err != nil {
							_ = cmd.Process.Signal(syscall.SIGTERM)
							<-done
							timer.Stop()
							tick.Stop()
							return err
						}
						ready = true
					}
				}
			case <-timer.C:
				if state.Pending {
					if _, e := os.Stat(health); e == nil {
						continue
					}
					_ = cmd.Process.Kill()
					childErr = <-done
					running = false
				}
			}
		}
		timer.Stop()
		tick.Stop()
		if state.Pending {
			if err = rollback(exe, &state); err != nil {
				return err
			}
			continue
		}
		var exit *exec.ExitError
		if !errors.As(childErr, &exit) || exit.ExitCode() != ExitReady {
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
			return err
		}
	}
}

func install(exe string, state *State) error {
	src, err := os.Open(exe)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(exe+".previous", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, src)
	if err == nil {
		err = dst.Sync()
	}
	if closeErr := dst.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	state.Pending = true
	if err = writeState(exe, *state); err != nil {
		return err
	}
	return os.Rename(exe+".next", exe)
}

func rollback(exe string, state *State) error {
	if err := os.Rename(exe+".previous", exe); err != nil {
		return fmt.Errorf("restore previous agent: %w", err)
	}
	state.Pending = false
	state.FailedVersion = state.Target
	state.Error = "New agent did not become healthy. Previous version restored."
	return writeState(exe, *state)
}
