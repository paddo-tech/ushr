package disk

import (
	"path/filepath"
	"testing"
)

func TestStat(t *testing.T) {
	u, err := Stat("/")
	if err != nil {
		t.Fatal(err)
	}
	if u.Total == 0 || u.Free == 0 || u.Free > u.Total {
		t.Fatalf("implausible usage for /: %+v", u)
	}
}

// "" is how a driver says it can't locate its store. It must error rather than
// walk up to ".": callers gate and prune on this number, and the process's cwd
// is not the filesystem any of that is about.
func TestStatEmptyPathErrors(t *testing.T) {
	if u, err := Stat(""); err == nil {
		t.Fatalf("Stat(\"\") returned %+v, want an error", u)
	}
}

// A store directory that doesn't exist yet must still measure the filesystem it
// will land on, not error.
func TestStatMissingPathUsesAncestor(t *testing.T) {
	dir := t.TempDir()
	want, err := Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Stat(filepath.Join(dir, "cache", "oci"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != want.Total {
		t.Fatalf("total = %d, want %d (same filesystem)", got.Total, want.Total)
	}
}
