package actioncache

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	shaA = "08eba0b27e820071cde6df949e0beb9ba4906955"
	shaB = "0c366fd6a839edf440554fa01a7085ccba70ac98"
)

func TestParse(t *testing.T) {
	log := strings.Join([]string{
		"2026-10-09T01:02:03.4567890Z Prepare all required actions",
		"2026-10-09T01:02:03.4567890Z Getting action download info",
		"2026-10-09T01:02:03.4567890Z Download action repository 'actions/checkout@v4' (SHA:" + shaA + ")",
		"2026-10-09T01:02:03.4567890Z Download action repository 'github/codeql-action/init@v3' (SHA:" + strings.ToUpper(shaB) + ")",
		"2026-10-09T01:02:03.4567890Z Download action repository 'actions/checkout@v4' (SHA:" + shaA + ")",
		"2026-10-09T01:02:03.4567890Z Download action repository 'actions/checkout@" + shaA + "' (SHA:" + shaA + ")",
		"Download action repository 'nope@v1' (SHA:" + shaA + ")",
		"Download action repository 'owner/repo@v1' (SHA:short)",
	}, "\n")
	got := Parse(strings.NewReader(log))
	want := []Action{
		{Repo: "actions/checkout", SHA: shaA},
		{Repo: "github/codeql-action", SHA: shaB},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Parse = %+v, want %+v", got, want)
	}
}

func TestPath(t *testing.T) {
	got := Path("/c", "actions/checkout", shaA)
	if want := "/c/actions_checkout/" + shaA + ".tar.gz"; got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

func TestEvictRemovesOldestFirst(t *testing.T) {
	dir := t.TempDir()
	c := &Cache{Dir: dir, MaxBytes: 25}
	now := time.Now()
	for i, name := range []string{"old", "mid", "new"} {
		p := Path(dir, "o/"+name, shaA)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, 10), 0o644); err != nil {
			t.Fatal(err)
		}
		mod := now.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(p, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.evict(); err != nil {
		t.Fatal(err)
	}
	for name, kept := range map[string]bool{"old": false, "mid": true, "new": true} {
		_, err := os.Stat(Path(dir, "o/"+name, shaA))
		if (err == nil) != kept {
			t.Errorf("%s kept=%v, want %v", name, err == nil, kept)
		}
	}
}

func TestReclaimKeepsDir(t *testing.T) {
	dir := t.TempDir()
	p := Path(dir, "actions/checkout", shaA)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := (&Cache{Dir: dir}).Reclaim(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reclaim removed the mounted dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("reclaim left %d entries", len(entries))
	}
}
