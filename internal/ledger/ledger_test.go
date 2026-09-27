package ledger

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAppendRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{JobID: 1, Org: "acme", Repo: "api", RunnerName: "ushr-a-1", StartedAt: time.Unix(0, 0).UTC(), CompletedAt: time.Unix(60, 0).UTC()},
		{JobID: 2, Org: "acme", Repo: "web", RunnerName: "ushr-a-2", StartedAt: time.Unix(0, 0).UTC(), CompletedAt: time.Unix(120, 0).UTC()},
	}
	for _, r := range want {
		if err := l.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].JobID != want[i].JobID || !got[i].CompletedAt.Equal(want[i].CompletedAt) {
			t.Errorf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestReadMissingIsEmpty(t *testing.T) {
	got, err := Read(filepath.Join(t.TempDir(), "absent.jsonl"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}
