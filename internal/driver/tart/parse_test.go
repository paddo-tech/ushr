package tart

import (
	"testing"

	"github.com/paddo-tech/ushr/internal/driver"
)

const sampleListJSON = `[
  {"Name": "base", "Source": "local", "State": "stopped"},
  {"Name": "paddo-runner-mac", "Source": "local", "State": "stopped"},
  {"Name": "ushr-runner-abc123", "Source": "local", "State": "running"},
  {"Name": "ushr-runner-def456", "Source": "local", "State": "stopped"},
  {"Name": "ghcr.io/cirruslabs/macos-runner:tahoe", "Source": "OCI", "State": "stopped"}
]`

func TestParseList_FiltersByPrefixAndLocalSource(t *testing.T) {
	got, err := parseList([]byte(sampleListJSON), "ushr-runner")
	if err != nil {
		t.Fatal(err)
	}
	want := []driver.SlotHandle{"ushr-runner-abc123", "ushr-runner-def456"}
	if len(got) != len(want) {
		t.Fatalf("got %d slots, want %d: %v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseList_EmptyOnNoMatches(t *testing.T) {
	got, err := parseList([]byte(sampleListJSON), "no-such-prefix")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

func TestParseList_BadJSON(t *testing.T) {
	if _, err := parseList([]byte("not json"), "x"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestParseVMState(t *testing.T) {
	cases := []struct {
		name    string
		want    driver.Status
		wantErr bool
	}{
		{"ushr-runner-abc123", driver.StatusRunning, false},
		{"ushr-runner-def456", driver.StatusDone, false},
		{"missing", driver.StatusUnknown, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseVMState([]byte(sampleListJSON), c.name)
			if (err != nil) != c.wantErr {
				t.Errorf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}
