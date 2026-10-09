// Package actioncache keeps a host-local cache of GitHub Action archives in the
// layout the actions runner reads through ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE:
// <dir>/<owner>_<repo>/<sha>.tar.gz. The runner only reads it, so the agent
// fills it after each job from the actions that job's log says it downloaded.
package actioncache

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/domain"
)

// EnvVar points the runner at the cache directory.
const EnvVar = "ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE"

// DirName is the cache's directory under the agent state dir. The VM CLIs name
// a guest share after the host folder, so this is also its name in the guest.
const DirName = "actions"

// DefaultMaxBytes caps the cache; the oldest archives go first past it.
const DefaultMaxBytes = 2 << 30

const (
	// logDelay gives GitHub time to finish ingesting the job log after the
	// runner exits.
	logDelay     = 30 * time.Second
	queueSize    = 64
	fillTimeout  = 10 * time.Minute
	maxRedirects = 3
	// maxActions bounds the downloads one job can trigger.
	maxActions = 32
)

// Action is one action repository at a resolved commit.
type Action struct {
	Repo string // owner/repo
	SHA  string
}

var downloadLine = regexp.MustCompile(`Download action repository '([^'@]+)@[^']*' \(SHA:([0-9a-fA-F]{40})\)`)

// firstStep marks the end of the runner's "Set up job" section. Lines after it
// are step output a workflow controls, so a fake download line there must not
// make the agent fetch arbitrary repositories.
const firstStep = "##[group]Run "

// Parse returns the distinct actions a job log's setup section reports
// downloading, in order, at most maxActions. A subpath action (owner/repo/path)
// is cached under its repository.
func Parse(r io.Reader) ([]Action, error) {
	var out []Action
	seen := map[Action]bool{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for len(out) < maxActions && sc.Scan() {
		line := sc.Text()
		if strings.Contains(line, firstStep) {
			break
		}
		m := downloadLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		parts := strings.Split(m[1], "/")
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			continue
		}
		a := Action{Repo: parts[0] + "/" + parts[1], SHA: strings.ToLower(m[2])}
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out, sc.Err()
}

// Path is where the runner looks for repo's archive at sha.
func Path(dir, repo, sha string) string {
	return filepath.Join(dir, strings.ReplaceAll(repo, "/", "_"), sha+".tar.gz")
}

// Cache fills and trims one host's archive directory.
type Cache struct {
	Dir      string
	MaxBytes int64

	client func(scope string) *github.Client
	http   *http.Client
	queue  chan domain.Job

	mu sync.Mutex // serializes renames, eviction and reclaim
}

// New creates dir and returns a cache whose fills authenticate with the job
// scope's App client.
func New(dir string, client func(scope string) *github.Client) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("action cache dir: %w", err)
	}
	return &Cache{
		Dir:      dir,
		MaxBytes: DefaultMaxBytes,
		client:   client,
		http:     &http.Client{Timeout: fillTimeout},
		queue:    make(chan domain.Job, queueSize),
	}, nil
}

// Fill schedules a cache fill from a finished job. It never blocks: a full
// queue drops the job, which costs a later download, not a job.
func (c *Cache) Fill(job domain.Job) {
	if job.Repo == "" || job.JobID == 0 {
		return
	}
	time.AfterFunc(logDelay, func() {
		select {
		case c.queue <- job:
		default:
		}
	})
}

// Run works the fill queue until ctx is cancelled.
func (c *Cache) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.queue:
			fctx, cancel := context.WithTimeout(ctx, fillTimeout)
			if err := c.fill(fctx, job); err != nil {
				slog.Debug("action cache fill failed", "job", job.JobID, "repo", job.Repo, "err", err)
			}
			cancel()
		}
	}
}

func (c *Cache) fill(ctx context.Context, job domain.Job) error {
	gc := c.client(job.Org)
	if gc == nil {
		return fmt.Errorf("no client for scope %q", job.Org)
	}
	// Paths carry no server, so a GHES repo could share a github.com repo's
	// cache entry under the same name. Only github.com fills the cache.
	if gc.BaseURL.Host != "api.github.com" {
		return nil
	}
	owner, repo, ok := strings.Cut(job.Repo, "/")
	if !ok {
		return fmt.Errorf("bad repo %q", job.Repo)
	}
	u, _, err := gc.Actions.GetWorkflowJobLogs(ctx, owner, repo, job.JobID, maxRedirects)
	if err != nil {
		return fmt.Errorf("job log url: %w", err)
	}
	body, err := c.get(ctx, u.String())
	if err != nil {
		return fmt.Errorf("job log: %w", err)
	}
	actions, err := Parse(body)
	if cerr := body.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("job log: %w", err)
	}
	for _, a := range actions {
		if err := c.store(ctx, gc, a); err != nil {
			slog.Debug("action cache store failed", "action", a.Repo, "sha", a.SHA, "err", err)
		}
	}
	return nil
}

// store downloads one archive unless it is cached or not public.
func (c *Cache) store(ctx context.Context, gc *github.Client, a Action) error {
	// The cache is shared by every org on the host, so only public actions go
	// in: a private action's archive must never reach another tenant's runner.
	name, err := c.publicName(ctx, gc, a.Repo)
	if err != nil || name == "" {
		return err
	}
	dst := Path(c.Dir, name, a.SHA)
	if _, err := os.Stat(dst); err == nil {
		return nil
	}
	owner, repo, _ := strings.Cut(name, "/")
	u, _, err := gc.Repositories.GetArchiveLink(ctx, owner, repo, github.Tarball,
		&github.RepositoryContentGetOptions{Ref: a.SHA}, maxRedirects)
	if err != nil {
		return fmt.Errorf("archive url: %w", err)
	}
	body, err := c.get(ctx, u.String())
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return err
	}
	// The download runs outside c.mu so a slow fill never stalls disk reclaim.
	// An archive over the cap would evict the whole cache and then itself.
	n, err := io.Copy(tmp, io.LimitReader(body, c.MaxBytes+1))
	if err == nil && n > c.MaxBytes {
		err = fmt.Errorf("archive exceeds %d bytes", c.MaxBytes)
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	// Runner users have other uids than the agent; CreateTemp makes it 0600.
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o644)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.Rename(tmp.Name(), dst); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return c.evict()
}

// publicName returns the canonical owner/repo for a public repository and ""
// for any other. The canonical case matters: the runner names the cache entry
// from the resolved repository, not from what the workflow wrote. It asks
// GitHub on every call: a repo can turn private at any time.
func (c *Cache) publicName(ctx context.Context, gc *github.Client, repo string) (string, error) {
	owner, r, _ := strings.Cut(repo, "/")
	info, resp, err := gc.Repositories.Get(ctx, owner, r)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return "", nil
		}
		return "", err
	}
	// Internal repos report private; a repo this App can't see is not public.
	if info.GetPrivate() {
		return "", nil
	}
	return info.GetFullName(), nil
}

func (c *Cache) get(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s", req.URL.Redacted(), resp.Status)
	}
	return resp.Body, nil
}

// evict removes the oldest archives until the cache fits MaxBytes. The runner
// reads without touching mtimes, so age is time since download.
func (c *Cache) evict() error {
	type entry struct {
		path string
		size int64
		mod  time.Time
	}
	var all []entry
	var total int64
	err := filepath.WalkDir(c.Dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".tar.gz") {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		all = append(all, entry{p, fi.Size(), fi.ModTime()})
		total += fi.Size()
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(all, func(i, j int) bool { return all[i].mod.Before(all[j].mod) })
	for _, e := range all {
		if total <= c.MaxBytes {
			break
		}
		if err := os.Remove(e.path); err != nil {
			return err
		}
		total -= e.size
	}
	return nil
}

// Reclaim empties the cache for disk-pressure reclaim. It removes the entries,
// not the directory, so live runner mounts of it stay valid.
func (c *Cache) Reclaim() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := os.ReadDir(c.Dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(c.Dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}
