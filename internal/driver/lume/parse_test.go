package lume

import (
	"testing"

	"github.com/paddo-tech/ushr/internal/driver"
)

const sampleListJSON = `[
  {"name": "base", "status": "stopped", "ipAddress": null},
  {"name": "paddo-runner-mac", "status": "stopped", "ipAddress": null},
  {"name": "ushr-runner-abc123", "status": "running", "ipAddress": "192.168.64.5"},
  {"name": "ushr-runner-def456", "status": "stopped", "ipAddress": null}
]`

func TestParseList_FiltersByPrefix(t *testing.T) {
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

func TestParseVM(t *testing.T) {
	cases := []struct {
		name    string
		want    driver.Status
		wantIP  string
		wantErr bool
	}{
		{"ushr-runner-abc123", driver.StatusRunning, "192.168.64.5", false},
		{"ushr-runner-def456", driver.StatusDone, "", false},
		{"missing", driver.StatusUnknown, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ip, err := parseVM([]byte(sampleListJSON), c.name)
			if (err != nil) != c.wantErr {
				t.Errorf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
			if ip != c.wantIP {
				t.Errorf("ip=%q, want %q", ip, c.wantIP)
			}
		})
	}
}

func TestParseIP(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{"assigned", `{"name": "ushr-runner-abc123", "status": "running", "ipAddress": "192.168.64.5"}`, "192.168.64.5"},
		{"no lease yet", `{"name": "ushr-runner-abc123", "status": "running", "ipAddress": null}`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseIP([]byte(c.json))
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestParseIP_BadJSON(t *testing.T) {
	if _, err := parseIP([]byte("not json")); err == nil {
		t.Fatal("expected parse error")
	}
}
