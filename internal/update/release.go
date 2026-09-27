// Package update installs official agent releases after the host drains.
package update

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
)

var ErrReady = errors.New("agent update ready")
var releaseVersion = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

const ExitReady = 75

func Prepare(ctx context.Context, version string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := download(ctx, http.DefaultClient, version, exe+".next"); err != nil {
		return err
	}
	state, err := readState(exe)
	if err != nil {
		return err
	}
	state.Target = version
	if err := writeState(exe, state); err != nil {
		return err
	}
	return ErrReady
}

func download(ctx context.Context, client *http.Client, version, dst string) error {
	if !releaseVersion.MatchString(version) {
		return errors.New("invalid release version")
	}
	get := func(url string) (io.ReadCloser, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		for attempt := 0; ; attempt++ {
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode == http.StatusOK {
				return resp.Body, nil
			}
			_ = resp.Body.Close()
			if attempt > 0 || resp.StatusCode < 500 || resp.StatusCode >= 600 {
				return nil, fmt.Errorf("release download returned HTTP %d", resp.StatusCode)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	body, err := get("https://api.github.com/repos/paddo-tech/ushr/releases/tags/" + version)
	if err != nil {
		return err
	}
	var rel struct {
		Tag        string `json:"tag_name"`
		Draft      bool   `json:"draft"`
		Prerelease bool   `json:"prerelease"`
		Assets     []struct {
			Name   string `json:"name"`
			Digest string `json:"digest"`
		} `json:"assets"`
	}
	err = json.NewDecoder(io.LimitReader(body, 2<<20)).Decode(&rel)
	_ = body.Close()
	if err != nil {
		return err
	}
	if rel.Tag != version || rel.Draft || rel.Prerelease {
		return errors.New("release is not a published stable version")
	}
	name := "ushr_" + strings.TrimPrefix(version, "v") + "_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
	digest := ""
	for _, a := range rel.Assets {
		if a.Name == name {
			digest = a.Digest
		}
	}
	if !regexp.MustCompile(`^sha256:[a-f0-9]{64}$`).MatchString(digest) {
		return errors.New("release has no archive checksum for this host")
	}
	body, err = get("https://github.com/paddo-tech/ushr/releases/download/" + version + "/" + name)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	archive, err := os.CreateTemp("", "ushr-release-*.tar.gz")
	if err != nil {
		return err
	}
	defer func() { _ = archive.Close(); _ = os.Remove(archive.Name()) }()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(archive, hash), io.LimitReader(body, (256<<20)+1))
	if err != nil {
		return err
	}
	if n > 256<<20 || "sha256:"+fmt.Sprintf("%x", hash.Sum(nil)) != digest {
		return errors.New("release archive checksum mismatch")
	}
	if _, err = archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	gz, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return errors.New("release archive has no agent binary")
		}
		if err != nil {
			return err
		}
		if h.Name != "ushr-agent" {
			continue
		}
		if h.Typeflag != tar.TypeReg || h.Size <= 0 || h.Size > 128<<20 {
			return errors.New("invalid agent archive entry")
		}
		f, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
		if err != nil {
			return err
		}
		_, err = io.CopyN(f, tr, h.Size)
		if err == nil {
			err = f.Sync()
		}
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(dst)
			return err
		}
		return nil
	}
}
