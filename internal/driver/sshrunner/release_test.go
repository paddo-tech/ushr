package sshrunner

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// A new release downloads once, then replaces every cached tarball except the
// one a provision may still be copying.
func TestFetchLatest(t *testing.T) {
	tag := "v2.337.0"
	downloads := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/latest" {
			_, _ = fmt.Fprintf(w, `{"tag_name":%q,"assets":[{"name":"actions-runner-osx-arm64-%s.tar.gz","digest":"sha256:%x"}]}`, tag, tag[1:], sha256.Sum256([]byte("tar /"+tag[1:])))
			return
		}
		downloads++
		_, _ = w.Write([]byte("tar " + r.URL.Path))
	}))
	defer srv.Close()
	oldLatest, oldDownload := latestURL, downloadURL
	t.Cleanup(func() { latestURL, downloadURL = oldLatest, oldDownload })
	latestURL = srv.URL + "/latest"
	downloadURL = srv.URL + "/%[1]s"

	dir := t.TempDir()
	unrelated := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "actions-runner-2.335.1.tar.gz")
	if err := os.WriteFile(stale, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := FetchLatest(context.Background(), dir, stale)
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != "2.337.0" {
		t.Fatalf("version %q, want 2.337.0", r.Version)
	}
	if got, _ := os.ReadFile(r.Tar); string(got) != "tar /2.337.0" {
		t.Fatalf("tarball holds %q", got)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("in-use tarball removed: %v", err)
	}

	if _, err := FetchLatest(context.Background(), dir, r.Tar); err != nil {
		t.Fatal(err)
	}
	if downloads != 1 {
		t.Fatalf("%d downloads, want 1 for an unchanged release", downloads)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale tarball kept: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated cache file removed: %v", err)
	}
	tag = "v2.338.0"
	updated, err := FetchLatest(context.Background(), dir, r.Tar)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != "2.338.0" || downloads != 2 {
		t.Fatalf("release update: version %q, downloads %d", updated.Version, downloads)
	}
	if _, err := os.Stat(r.Tar); err != nil {
		t.Fatalf("previous archive removed during update: %v", err)
	}
}

func TestFetchLatestRejectsIncompleteRelease(t *testing.T) {
	for _, failure := range []string{"metadata", "checksum missing", "checksum mismatch", "truncated", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/latest" {
					if failure == "metadata" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					if failure == "checksum missing" {
						_, _ = w.Write([]byte(`{"tag_name":"v2.337.0"}`))
						return
					}
					_, _ = fmt.Fprintf(w, `{"tag_name":"v2.337.0","assets":[{"name":"actions-runner-osx-arm64-2.337.0.tar.gz","digest":"sha256:%x"}]}`, sha256.Sum256([]byte("complete archive")))
					return
				}
				if failure == "truncated" {
					w.Header().Set("Content-Length", "100")
				}
				_, _ = w.Write([]byte("broken archive"))
			}))
			defer srv.Close()
			oldLatest, oldDownload := latestURL, downloadURL
			t.Cleanup(func() { latestURL, downloadURL = oldLatest, oldDownload })
			latestURL, downloadURL = srv.URL+"/latest", srv.URL+"/%[1]s"
			dir := t.TempDir()
			previous := filepath.Join(dir, "actions-runner-2.336.0.tar.gz")
			if err := os.WriteFile(previous, []byte("previous archive"), 0o644); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			}
			if _, err := FetchLatest(ctx, dir, previous); err == nil {
				t.Fatal("accepted an incomplete release")
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Name() != filepath.Base(previous) {
				t.Fatalf("failed update changed the cache: %v", entries)
			}
		})
	}
}
