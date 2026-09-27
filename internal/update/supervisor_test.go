package update

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("USHR_TEST_SUPERVISOR") != "" {
		healthTimeout = 2 * time.Second
		exe, _ := os.Executable()
		if len(os.Args) > 1 && os.Args[1] == "-worker" {
			state, err := readState(exe)
			if err != nil {
				os.Exit(2)
			}
			if state.Target == "" {
				state.Target = "v99.0.0"
				if writeState(exe, state) != nil {
					os.Exit(3)
				}
				os.Exit(ExitReady)
			}
			if state.FailedVersion != "" {
				os.Exit(0)
			}
			if Healthy(context.Background()) != nil {
				os.Exit(4)
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				b, _ := os.ReadFile(exe + ".adopted")
				if string(b) == strconv.Itoa(os.Getpid()) {
					os.Exit(0)
				}
				time.Sleep(10 * time.Millisecond)
			}
			os.Exit(5)
		}
		if pid := os.Getenv("USHR_UPDATE_CHILD_PID"); pid != "" {
			if os.WriteFile(exe+".adopted", []byte(pid), 0o600) != nil {
				os.Exit(6)
			}
		}
		if Supervise("") != nil {
			os.Exit(7)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSupervisorRecoveryAndHandoff(t *testing.T) {
	original, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"startup failure", "health timeout", "healthy handoff"} {
		t.Run(scenario, func(t *testing.T) {
			exe := filepath.Join(t.TempDir(), "agent")
			if err := os.WriteFile(exe, binary, 0o755); err != nil {
				t.Fatal(err)
			}
			candidate := binary
			if scenario == "startup failure" {
				candidate = []byte("#!/bin/sh\nexit 1\n")
			}
			if scenario == "health timeout" {
				candidate = []byte("#!/bin/sh\nexec sleep 30\n")
			}
			if err := os.WriteFile(exe+".next", candidate, 0o755); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe)
			cmd.Env = append(os.Environ(), "USHR_TEST_SUPERVISOR=1")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("supervisor: %v\n%s", err, out)
			}
			state, err := readState(exe)
			if err != nil {
				t.Fatal(err)
			}
			if state.Pending {
				t.Fatal("candidate stayed pending")
			}
			if scenario == "healthy handoff" {
				if state.FailedVersion != "" {
					t.Fatalf("healthy update failed: %+v", state)
				}
				if _, err := os.Stat(exe + ".adopted"); err != nil {
					t.Fatal("supervisor did not preserve its worker")
				}
			} else if state.FailedVersion != "v99.0.0" {
				t.Fatalf("rollback not reported: %+v", state)
			}
		})
	}
}
