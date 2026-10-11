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
	// Client returns the job scope's App client. Set it before the first fill.
	Client func(scope string) *github.Client

	http  *http.Client
	queue chan fill

	mu sync.Mutex // serializes renames, eviction and reclaim
}

// fill is one queued fill. A scale-set completion names the runner, not the
// job, so runner is set and the job id is resolved from the run.
type fill struct {
	job    domain.Job
	runner string
}

// New creates dir and returns a cache. Fills authenticate with Client.
func New(dir string) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("action cache dir: %w", err)
	}
	return &Cache{
		Dir:      dir,
		MaxBytes: DefaultMaxBytes,
		http:     &http.Client{Timeout: fillTimeout},
		queue:    make(chan fill, queueSize),
	}, nil
}

// Fill schedules a cache fill from a finished job. It never blocks: a full
// queue drops the job, which costs a later download, not a job.
func (c *Cache) Fill(job domain.Job) {
	if job.Repo == "" || job.JobID == 0 {
		return
	}
	c.enqueue(fill{job: job})
}

// FillRun schedules a cache fill from the job that runner ran in run runID of
// owner/repo, under scope's App client. Scale-set completions carry the Actions
// service job id, not the REST one, so the fill looks the job up by runner. It
// never blocks.
func (c *Cache) FillRun(scope, owner, repo string, runID int64, runner string) {
	if owner == "" || repo == "" || runID == 0 || runner == "" {
		return
	}
	c.enqueue(fill{job: domain.Job{Org: scope, Repo: owner + "/" + repo, RunID: runID}, runner: runner})
}

func (c *Cache) enqueue(f fill) {
	time.AfterFunc(logDelay, func() {
		select {
		case c.queue <- f:
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
		case f := <-c.queue:
			fctx, cancel := context.WithTimeout(ctx, fillTimeout)
			if err := c.fill(fctx, f); err != nil {
				slog.Warn("action cache fill failed", "job", f.job.JobID, "run", f.job.RunID, "runner", f.runner, "repo", f.job.Repo, "err", err)
			}
			cancel()
		}
	}
}

func (c *Cache) fill(ctx context.Context, f fill) error {
	job := f.job
	gc := c.Client(job.Org)
	if gc == nil {
		return fmt.Errorf("no client for scope %q", job.Org)
	}
	// Paths carry no server, so a GHES repo could share a github.com repo's
	// cache entry under the same name. Only github.com fills the cache.
	if gc.BaseURL.Host != "api.github.com" {
		return nil
	}
	owner, repo := jobRepo(job)
	if f.runner != "" {
		id, err := runnerJob(ctx, gc, owner, repo, job.RunID, f.runner)
		if err != nil {
			return err
		}
		if id == 0 {
			return fmt.Errorf("no job in run %d ran on %s", job.RunID, f.runner)
		}
		job.JobID = id
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
			slog.Warn("action cache store failed", "action", a.Repo, "sha", a.SHA, "err", err)
		}
	}
	return nil
}

// jobRepo returns the job's repository owner and name. The poll source sets
// Repo to the bare name; the owner is the scope's org or owner/repo prefix.
func jobRepo(job domain.Job) (owner, repo string) {
	if o, r, ok := domain.SplitRepoScope(job.Repo); ok {
		return o, r
	}
	owner, _, _ = strings.Cut(job.Org, "/")
	return owner, job.Repo
}

// runnerJob returns the id of the job in run runID that ran on runner, 0 if
// none did.
func runnerJob(ctx context.Context, gc *github.Client, owner, repo string, runID int64, runner string) (int64, error) {
	opts := &github.ListWorkflowJobsOptions{Filter: "all", ListOptions: github.ListOptions{PerPage: 100}}
	for {
		jobs, resp, err := gc.Actions.ListWorkflowJobs(ctx, owner, repo, runID, opts)
		if err != nil {
			return 0, fmt.Errorf("list run jobs: %w", err)
		}
		if id := jobOnRunner(jobs.Jobs, runner); id != 0 {
			return id, nil
		}
		if resp.NextPage == 0 {
			return 0, nil
		}
		opts.Page = resp.NextPage
	}
}

// jobOnRunner returns the id of the job that ran on runner, 0 if none did.
// Runner names are random per dispatch, so at most one job matches.
func jobOnRunner(jobs []*github.WorkflowJob, runner string) int64 {
	for _, j := range jobs {
		if j.GetRunnerName() == runner {
			return j.GetID()
		}
	}
	return 0
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
	slog.Info("action cached", "action", name, "sha", a.SHA, "bytes", n)
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
