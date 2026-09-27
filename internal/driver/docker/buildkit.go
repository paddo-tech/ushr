package docker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// containerPrefix is how buildx names the buildkit container backing a builder
// created as "ushr-<key>": buildx_buildkit_ushr-<key>0.
const containerPrefix = "buildx_buildkit_ushr-"

// containerKey extracts the repo key from a buildkit container name, or "" if
// the name isn't one of ours. Strips the prefix and the trailing node index.
func containerKey(name string) string {
	s := strings.TrimPrefix(name, containerPrefix)
	if s == name {
		return ""
	}
	return strings.TrimSuffix(s, "0")
}

// repoKey turns "owner/repo" into a stable, filesystem- and container-safe id.
func repoKey(repo string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(repo)))
	return hex.EncodeToString(sum[:])[:12]
}

// ensureRepoBuilder makes sure a persistent buildx builder exists for one repo,
// returning its host BUILDX_CONFIG dir (to bind-mount into the runner) and its
// builder name (to set as BUILDX_BUILDER). Each repo gets a distinct builder
// name, so buildx backs it with a distinct buildkit container: the layer cache
// and RUN --mount=type=cache persist between that repo's jobs in a cache store
// separate from every other repo's. This is cache isolation, not a security
// boundary: every job mounts the host docker socket, so a job can reach any
// other repo's buildkit container. Serialized per repo so concurrent dispatches
// for the same repo don't race to create it.
func (d *Driver) ensureRepoBuilder(ctx context.Context, repo string) (dir, builder string, err error) {
	key := repoKey(repo)
	dir = filepath.Join(d.stateDir, "buildx", key)
	builder = "ushr-" + key

	mu := d.repoLock(key)
	mu.Lock()
	defer mu.Unlock()

	if err = os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("buildx config dir: %w", err)
	}
	env := append(os.Environ(), "BUILDX_CONFIG="+dir)

	// Reuse a healthy builder; otherwise clear a stale config and (re)create.
	if d.buildx(ctx, env, "inspect", "--bootstrap", builder) == nil {
		makeShared(dir, d.sharedGID)
		return dir, builder, nil
	}
	_ = d.buildx(ctx, env, "rm", "--force", builder)
	if err = d.buildx(ctx, env, "create",
		"--name", builder,
		"--driver", "docker-container",
		"--driver-opt", "image="+d.buildkitImage,
		"--bootstrap"); err != nil {
		return "", "", fmt.Errorf("create builder %s: %w", builder, err)
	}
	makeShared(dir, d.sharedGID)
	return dir, builder, nil
}

// makeShared lets the runner user (a different uid than the agent) read and
// update buildx's lock/activity files under the mount. When a shared gid is
// known — the group the runner joins via group_add, which the agent is also in
// — it group-owns the tree with 0660 files and setgid 2770 dirs (so files the
// runner creates inherit the group too). Without a shared gid it falls back to
// world-writable; set driver.group_add so it doesn't have to.
func makeShared(root string, gid int) {
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if gid > 0 {
			_ = os.Chown(p, -1, gid)
			if info.IsDir() {
				_ = os.Chmod(p, os.ModeSetgid|0o770)
			} else {
				_ = os.Chmod(p, 0o660)
			}
			return nil
		}
		if info.IsDir() {
			_ = os.Chmod(p, 0o777)
		} else {
			_ = os.Chmod(p, 0o666)
		}
		return nil
	})
}

// buildx runs `<runtime> buildx <args>` with env carrying BUILDX_CONFIG so each
// repo's builder is created and read under its own isolated config directory.
func (d *Driver) buildx(ctx context.Context, env []string, args ...string) error {
	cmd := exec.CommandContext(ctx, d.runtime, append([]string{"buildx"}, args...)...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s buildx %s: %w (%s)", d.runtime, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// maintainBuildCache runs the per-repo build-cache upkeep loop until ctx is
// cancelled: it caps each repo's cache at buildCacheGB and reaps builders idle
// past buildCacheIdle. It is filesystem-driven (a repo's activity mtime is its last
// build), so it survives agent restarts and reconciles builders left over from
// a previous run. No-op unless the build cache is enabled.
func (d *Driver) maintainBuildCache(ctx context.Context) {
	if !d.buildCache {
		return
	}
	// Pre-pull the buildkit image so the first cold-repo builder create doesn't
	// block a runner's Provision on the image pull.
	_ = d.docker(ctx, "pull", d.buildkitImage)
	t := time.NewTicker(maintenanceInterval)
	defer t.Stop()
	d.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.sweep(ctx)
		}
	}
}

// sweep prunes each live repo builder to its size cap, reaps the ones idle past
// the TTL, and removes any orphaned buildkit container whose config is gone.
func (d *Driver) sweep(ctx context.Context) {
	d.gcMu.Lock() // pressure reclaim prunes the same builders to a tighter budget
	defer d.gcMu.Unlock()
	base := filepath.Join(d.stateDir, "buildx")
	entries, _ := os.ReadDir(base)
	live := make(map[string]bool, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		key := e.Name()
		dir := filepath.Join(base, key)
		builder := "ushr-" + key
		env := append(os.Environ(), "BUILDX_CONFIG="+dir)
		if d.builderIdle(dir) {
			_ = d.buildx(ctx, env, "rm", "--force", builder)
			d.removeDir(ctx, dir)
			continue
		}
		live[key] = true
		_ = d.buildx(ctx, env, "prune", "--builder", builder, "--force",
			fmt.Sprintf("--reserved-space=%dGB", d.buildCacheGB))
	}
	// Reap buildkit containers orphaned by a failed rm (their config is gone).
	out, err := d.dockerOut(ctx, "ps", "-a", "--filter", "name="+containerPrefix, "--format", "{{.Names}}")
	if err != nil {
		return
	}
	for _, name := range strings.Fields(string(out)) {
		if key := containerKey(name); key != "" && !live[key] {
			_ = d.docker(ctx, "rm", "-f", name)
		}
	}
}

// removeDir deletes a per-repo config dir. A build leaves a root-only refs/
// owned by the runner uid that the agent user can't unlink, so when a plain
// RemoveAll fails it retries from inside a root container mounted on the parent,
// which (with rootful docker) always has permission.
func (d *Driver) removeDir(ctx context.Context, dir string) {
	if os.RemoveAll(dir) == nil {
		return
	}
	base, key := filepath.Dir(dir), filepath.Base(dir)
	_ = d.docker(ctx, "run", "--rm", "--user", "0", "--entrypoint", "rm",
		"-v", base+":/x", d.image, "-rf", "/x/"+key)
}

// builderIdle reports whether a repo's builder hasn't been used within the TTL,
// judged by the mtime of its buildx activity dir, which buildx bumps on every
// build. Falls back to the config dir's mtime.
func (d *Driver) builderIdle(dir string) bool {
	fi, err := os.Stat(filepath.Join(dir, "activity"))
	if err != nil {
		if fi, err = os.Stat(dir); err != nil {
			return false
		}
	}
	return time.Since(fi.ModTime()) > d.buildCacheIdle
}

// repoLock returns the per-repo creation mutex, lazily allocated.
func (d *Driver) repoLock(key string) *sync.Mutex {
	d.buildMu.Lock()
	defer d.buildMu.Unlock()
	if d.repoLocks == nil {
		d.repoLocks = make(map[string]*sync.Mutex)
	}
	mu, ok := d.repoLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		d.repoLocks[key] = mu
	}
	return mu
}
