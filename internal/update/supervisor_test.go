package update

import (
	"context"
	"errors"
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
		state, err := readState(exe)
		if err != nil {
			os.Exit(2)
		}
		if len(os.Args) > 1 && os.Args[1] == "-worker" {
			if state.Target == "" {
				state.Target, state.Request = "v99.0.0", "request-1"
				if writeState(exe, state) != nil {
					os.Exit(3)
				}
				os.Exit(ExitReady)
			}
			failed, request, _ := Status()
			if failed != "" {
				if request != "request-1" {
					os.Exit(8)
				}
				os.Exit(0)
			}
			if Healthy(context.Background()) != nil {
				os.Exit(4)
			}
			state, err = readState(exe)
			if err != nil || state.Pending {
				os.Exit(5)
			}
			if os.WriteFile(exe+".accepted-worker", []byte(strconv.Itoa(os.Getppid())), 0o600) != nil {
				os.Exit(6)
			}
			os.Exit(0)
		}
		if len(os.Args) > 1 && os.Args[1] == "-supervised" {
			if state.Pending && os.Getenv("USHR_TEST_MODE") == "supervisor failure" {
				cmd := exec.Command(exe, "-worker")
				if cmd.Start() != nil {
					os.Exit(9)
				}
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					if _, err := os.Stat(os.Getenv("USHR_UPDATE_HEALTH_FILE") + ".worker"); err == nil {
						os.Exit(10)
					}
					time.Sleep(10 * time.Millisecond)
				}
				os.Exit(11)
			}
			err = SuperviseWorker("")
		} else {
			err = Supervise("")
		}
		if errors.Is(err, ErrReady) {
			os.Exit(ExitReady)
		}
		if err != nil {
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
	for _, scenario := range []string{"startup failure", "health timeout", "supervisor failure", "installation failure", "healthy handoff"} {
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
			if scenario == "installation failure" {
				if err := os.Mkdir(exe+".previous", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(exe+".previous/occupied", nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(exe+".next", candidate, 0o755); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe)
			cmd.Env = append(os.Environ(), "USHR_TEST_SUPERVISOR=1", "USHR_TEST_MODE="+scenario)
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
				b, err := os.ReadFile(exe + ".accepted-worker")
				if err != nil {
					t.Fatal("candidate worker was not accepted")
				}
				if string(b) == strconv.Itoa(cmd.Process.Pid) {
					t.Fatal("worker bypassed candidate supervisor")
				}
				if _, err := os.Stat(exe + ".previous"); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("successful update retained backup")
				}
			} else {
				if state.FailedVersion != "v99.0.0" || state.FailedRequest != "request-1" {
					t.Fatalf("failure not reported: %+v", state)
				}
				if _, err := os.Stat(exe + ".accepted-worker"); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed candidate accepted work")
				}
			}
		})
	}
}
