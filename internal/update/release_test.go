package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDownload(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	binary := []byte("candidate agent")
	if err := tw.WriteHeader(&tar.Header{Name: "ushr-agent", Mode: 0o755, Size: int64(len(binary)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(binary); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	archive := buf.Bytes()
	sum := sha256.Sum256(archive)
	for _, tt := range []struct {
		name, version       string
		badHash, prerelease bool
		wantError           bool
	}{
		{name: "verified release", version: "v0.2.5"},
		{name: "checksum failure", version: "v0.2.5", badHash: true, wantError: true},
		{name: "prerelease refused", version: "v0.2.5", prerelease: true, wantError: true},
		{name: "path injection refused", version: "../../evil", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Scheme != "https" {
					t.Fatal("release request is not HTTPS")
				}
				var data []byte
				switch r.URL.Host {
				case "api.github.com":
					digest := fmt.Sprintf("sha256:%x", sum)
					if tt.badHash {
						digest = "sha256:" + strings.Repeat("0", 64)
					}
					data, _ = json.Marshal(map[string]any{"tag_name": tt.version, "prerelease": tt.prerelease, "assets": []map[string]string{{"name": "ushr_0.2.5_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz", "digest": digest}}})
				case "github.com":
					data = archive
				default:
					t.Fatalf("unexpected release host %s", r.URL.Host)
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data))}, nil
			})}
			dst := filepath.Join(t.TempDir(), "agent.next")
			err := download(context.Background(), client, tt.version, dst)
			if (err != nil) != tt.wantError {
				t.Fatalf("download error=%v", err)
			}
			if tt.wantError {
				if _, err := os.Stat(dst); !os.IsNotExist(err) {
					t.Fatal("failed download left an executable")
				}
				return
			}
			data, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, binary) {
				t.Fatal("downloaded wrong binary")
			}
		})
	}
}

func TestInstallRollback(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(exe, []byte("previous"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe+".next", []byte("candidate"), 0o755); err != nil {
		t.Fatal(err)
	}
	state := State{Target: "v0.2.5"}
	if err := install(exe, &state); err != nil {
		t.Fatal(err)
	}
	state, err := readState(exe)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Pending {
		t.Fatal("missing recovery journal")
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "candidate" {
		t.Fatal("candidate not installed")
	}
	if err := rollback(exe, &state); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(exe)
	if string(got) != "previous" {
		t.Fatal("previous version not restored")
	}
	state, err = readState(exe)
	if err != nil {
		t.Fatal(err)
	}
	if state.Pending || state.FailedVersion != "v0.2.5" || state.Error == "" {
		t.Fatalf("bad rollback state: %+v", state)
	}
}
