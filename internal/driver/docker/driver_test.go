package docker

import (
	"errors"
	"testing"
)

func TestNewHonorsRuntimeOverride(t *testing.T) {
	if d := New(Options{Runtime: "podman"}); d.runtime != "podman" {
		t.Fatalf("runtime override ignored: got %q", d.runtime)
	}
}

func TestDetectRuntime(t *testing.T) {
	found := func(present ...string) func(string) (string, error) {
		set := map[string]bool{}
		for _, p := range present {
			set[p] = true
		}
		return func(name string) (string, error) {
			if set[name] {
				return "/usr/bin/" + name, nil
			}
			return "", errors.New("not found")
		}
	}
	cases := map[string]struct {
		lookPath func(string) (string, error)
		want     string
	}{
		"prefers docker when both present": {found("docker", "podman"), "docker"},
		"falls back to podman":             {found("podman"), "podman"},
		"defaults to docker when neither":  {found(), "docker"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := detectRuntimeWith(tc.lookPath); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
