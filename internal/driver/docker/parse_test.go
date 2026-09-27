package docker

import (
	"errors"
	"testing"

	"github.com/paddo-tech/ushr/internal/driver"
)

func TestStatusFromState(t *testing.T) {
	cases := map[string]driver.Status{
		"created":    driver.StatusRunning,
		"running":    driver.StatusRunning,
		"restarting": driver.StatusRunning,
		"paused":     driver.StatusRunning,
		"exited":     driver.StatusDone,
		"dead":       driver.StatusDone,
		"removing":   driver.StatusDone,
		"":           driver.StatusUnknown,
		"weird":      driver.StatusUnknown,
	}
	for state, want := range cases {
		if got := statusFromState(state); got != want {
			t.Errorf("statusFromState(%q) = %v, want %v", state, got, want)
		}
	}
}

func TestParseNames(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []driver.SlotHandle
	}{
		{"empty", "", nil},
		{"whitespace only", "  \n\n  ", nil},
		{"single", "ushr-runner-ab12cd34\n", []driver.SlotHandle{"ushr-runner-ab12cd34"}},
		{
			"multiple with trailing blank",
			"ushr-runner-aaaa\nushr-runner-bbbb\n",
			[]driver.SlotHandle{"ushr-runner-aaaa", "ushr-runner-bbbb"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseNames([]byte(tc.in))
			if len(got) != len(tc.want) {
				t.Fatalf("parseNames(%q) = %v, want %v", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("parseNames(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestIsNotFound(t *testing.T) {
	if !isNotFound(errors.New("docker inspect: exit status 1 (stderr: Error: No such container: ushr-runner-x)")) {
		t.Error("expected docker 'No such container' to be detected")
	}
	if !isNotFound(errors.New(`docker inspect: exit status 125 (stderr: Error: no such object: "ushr-runner-x")`)) {
		t.Error("expected podman 'no such object' to be detected")
	}
	if !isNotFound(errors.New(`docker rm: Error: no container with name or ID "ushr-runner-x" found`)) {
		t.Error("expected podman 'no container with name' to be detected")
	}
	if isNotFound(errors.New("docker run: Cannot connect to the Docker daemon")) {
		t.Error("daemon error must not be treated as not-found")
	}
	if isNotFound(nil) {
		t.Error("nil error is not not-found")
	}
}
