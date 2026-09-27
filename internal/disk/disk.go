// Package disk reports free space on a runner host's image store. It stats the
// filesystem rather than summing file sizes: VM disk images are sparse, so what
// they appear to hold bears no relation to the blocks they occupy.
package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Usage is a filesystem's available and total capacity in bytes. Free counts
// only blocks available to an unprivileged writer — the reserve root can still
// dip into can't run a job.
type Usage struct {
	Free  uint64
	Total uint64
}

// Stat reports usage for the filesystem holding path. A path that doesn't exist
// yet — an image store before the first pull — is measured at its nearest
// existing ancestor.
func Stat(path string) (Usage, error) {
	target, err := nearest(path)
	if err != nil {
		return Usage{}, err
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(target, &st); err != nil {
		return Usage{}, fmt.Errorf("statfs %s: %w", target, err)
	}
	bs := uint64(st.Bsize)
	return Usage{Free: st.Bavail * bs, Total: st.Blocks * bs}, nil
}

func nearest(path string) (string, error) {
	if path == "" {
		// filepath.Dir("") is ".", so walking up from empty would silently
		// measure the process's cwd — a filesystem unrelated to any image store.
		return "", fmt.Errorf("empty path")
	}
	for p := path; ; {
		_, err := os.Stat(p)
		if err == nil {
			return p, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", fmt.Errorf("no existing ancestor of %s", path)
		}
		p = parent
	}
}
