package sshrunner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Fresh VM clones must not depend on the runner version baked into their base image.
var (
	latestURL   = "https://api.github.com/repos/actions/runner/releases/latest"
	downloadURL = "https://github.com/actions/runner/releases/download/v%[1]s/actions-runner-osx-arm64-%[1]s.tar.gz"
)

// Runner is a runner release cached on the host.
type Runner struct {
	Version string
	Tar     string
}

// Callers must exclude cache updates while copying a cached archive into a guest.
func FetchLatest(ctx context.Context, dir, keep string) (Runner, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	version, digest, err := latestRelease(ctx)
	if err != nil {
		return Runner{}, err
	}
	r := Runner{Version: version, Tar: filepath.Join(dir, "actions-runner-"+version+".tar.gz")}
	if _, err := os.Stat(r.Tar); err != nil {
		if err := download(ctx, fmt.Sprintf(downloadURL, version), r.Tar, digest); err != nil {
			return Runner{}, err
		}
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		if strings.HasPrefix(e.Name(), "actions-runner-") && strings.HasSuffix(e.Name(), ".tar.gz") && p != r.Tar && p != keep {
			_ = os.Remove(p)
		}
	}
	return r, nil
}

func latestRelease(ctx context.Context) (string, string, error) {
	body, err := get(ctx, latestURL)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = body.Close() }()
	var rel struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name   string `json:"name"`
			Digest string `json:"digest"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(body).Decode(&rel); err != nil {
		return "", "", fmt.Errorf("decode runner release: %w", err)
	}
	if rel.TagName == "" {
		return "", "", fmt.Errorf("runner release has no tag")
	}
	version := strings.TrimPrefix(rel.TagName, "v")
	for _, asset := range rel.Assets {
		if asset.Name == "actions-runner-osx-arm64-"+version+".tar.gz" && strings.HasPrefix(asset.Digest, "sha256:") {
			return version, strings.TrimPrefix(asset.Digest, "sha256:"), nil
		}
	}
	return "", "", fmt.Errorf("runner release %s has no macOS ARM64 checksum", rel.TagName)
}

// Only complete archives with the published checksum may enter the cache.
func download(ctx context.Context, url, dst, digest string) error {
	body, err := get(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(dst), ".runner-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, hash), body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != digest {
		return fmt.Errorf("runner archive checksum mismatch")
	}
	return os.Rename(tmp, dst)
}

func get(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return resp.Body, nil
}
