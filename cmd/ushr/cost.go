package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/google/go-github/v84/github"
	"golang.org/x/sync/errgroup"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/ledger"
	gh "github.com/paddo-tech/ushr/internal/source/github"
)

// macosRate is GitHub-hosted Actions' per-minute price for a standard macOS
// runner — the headline number self-hosting avoids, and the cost report's
// default. Overridable with --rate for other runner classes. GitHub list
// price as of 2026-01-01; they revise it, so treat it as a snapshot.
const macosRate = 0.062

// scanWorkers bounds concurrent per-repo scans during --backfill. A whole
// installation scanned serially is impractically slow (one round-trip per run's
// job list); a small pool cuts wall-clock without risking secondary rate limits.
const scanWorkers = 8

func runCost(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cost", flag.ExitOnError)
	configPath := fs.String("config", defaultControllerConfig(), "path to controller.yaml")
	days := fs.Int("days", 30, "look back this many days")
	rate := fs.Float64("rate", macosRate, "GitHub-hosted price per runner-minute (USD); default is the standard macOS rate")
	backfill := fs.Bool("backfill", false, "scan GitHub for past jobs and seed the ledger (slow); default reads the ledger")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.LoadController(*configPath)
	if err != nil {
		return err
	}
	since := time.Now().AddDate(0, 0, -*days)
	path := ledger.PathOrDefault(cfg.Webhook.LedgerPath)

	if *backfill {
		if err := backfillLedger(ctx, cfg, since, path); err != nil {
			return err
		}
	}

	records, err := ledger.Read(path)
	if err != nil {
		return err
	}
	rows := aggregate(records, since)
	if len(rows) == 0 && !*backfill {
		fmt.Fprintln(os.Stderr, "no jobs in ledger — run `ushr cost --backfill` to scan GitHub history, or enable the webhook to populate it live")
	}
	printCost(rows, *days, *rate)
	return nil
}

type orgCost struct {
	org     string
	jobs    int
	minutes int
}

// aggregate totals billed minutes per org from ledger records, deduping by job
// ID (webhook redelivery and re-run backfills can repeat a job) and counting
// only jobs completed since the cutoff. Each job rounds up to the whole minute,
// matching GitHub's per-job billing.
func aggregate(records []ledger.Record, since time.Time) []orgCost {
	seen := make(map[int64]struct{}, len(records))
	byOrg := map[string]*orgCost{}
	for _, r := range records {
		if r.CompletedAt.Before(since) {
			continue
		}
		if _, dup := seen[r.JobID]; dup {
			continue
		}
		dur := r.CompletedAt.Sub(r.StartedAt)
		if dur <= 0 {
			continue
		}
		seen[r.JobID] = struct{}{}
		o := byOrg[r.Org]
		if o == nil {
			o = &orgCost{org: r.Org}
			byOrg[r.Org] = o
		}
		o.jobs++
		o.minutes += int(math.Ceil(dur.Minutes()))
	}
	rows := make([]orgCost, 0, len(byOrg))
	for _, o := range byOrg {
		rows = append(rows, *o)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].org < rows[j].org })
	return rows
}

func printCost(rows []orgCost, days int, rate float64) {
	fmt.Printf("ushr cost — last %d days (rate $%.3f/runner-min)\n\n", days, rate)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight)
	_, _ = fmt.Fprintln(w, "ORG\tJOBS\tRUNNER-MIN\tSAVED\t")
	var totJobs, totMin int
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "%s\t%d\t%d\t$%.2f\t\n", r.org, r.jobs, r.minutes, float64(r.minutes)*rate)
		totJobs += r.jobs
		totMin += r.minutes
	}
	_, _ = fmt.Fprintf(w, "TOTAL\t%d\t%d\t$%.2f\t\n", totJobs, totMin, float64(totMin)*rate)
	_ = w.Flush()
}

// backfillLedger scans each org's repos for completed jobs run on ushr runners
// since the cutoff and appends them to the ledger. Dedup on read means a repeat
// backfill is idempotent.
func backfillLedger(ctx context.Context, cfg *config.Controller, since time.Time, path string) error {
	led, err := ledger.Open(path)
	if err != nil {
		return err
	}
	for _, o := range cfg.Orgs {
		c, err := gh.NewAppClient(ctx, gh.AppAuth{
			AppID:          o.AppID,
			PrivateKeyPath: o.PrivateKeyPath,
			Org:            o.Name,
			Repos:          o.Repos,
			BaseURL:        o.BaseURL,
		})
		if err != nil {
			return fmt.Errorf("auth %s: %w", o.Name, err)
		}
		recs, err := scanOrg(ctx, c, o, since)
		if err != nil {
			return fmt.Errorf("%s: %w", o.Name, err)
		}
		for _, r := range recs {
			if err := led.Append(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// scanOrg collects ushr job records across the org's repos since the cutoff,
// scanning repos concurrently.
func scanOrg(ctx context.Context, c *github.Client, o config.Org, since time.Time) ([]ledger.Record, error) {
	repos, err := repoNames(ctx, c, o)
	if err != nil {
		return nil, err
	}
	created := ">=" + since.Format("2006-01-02")

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(scanWorkers)
	var mu sync.Mutex
	var out []ledger.Record
	for _, repo := range repos {
		g.Go(func() error {
			recs, err := scanRepo(ctx, c, o.Name, repo, created)
			if err != nil {
				return err
			}
			mu.Lock()
			out = append(out, recs...)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func scanRepo(ctx context.Context, c *github.Client, org, repo, created string) ([]ledger.Record, error) {
	runOpt := &github.ListWorkflowRunsOptions{
		Status:      "completed",
		Created:     created,
		ListOptions: github.ListOptions{PerPage: 100},
	}
	var out []ledger.Record
	for {
		runs, resp, err := c.Actions.ListRepositoryWorkflowRuns(ctx, org, repo, runOpt)
		if err != nil {
			return nil, fmt.Errorf("list runs %s: %w", repo, err)
		}
		for _, run := range runs.WorkflowRuns {
			recs, err := scanRun(ctx, c, org, repo, run.GetID())
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
		if resp.NextPage == 0 {
			break
		}
		runOpt.Page = resp.NextPage
	}
	return out, nil
}

func scanRun(ctx context.Context, c *github.Client, org, repo string, runID int64) ([]ledger.Record, error) {
	opt := &github.ListWorkflowJobsOptions{Filter: "latest", ListOptions: github.ListOptions{PerPage: 100}}
	var out []ledger.Record
	for {
		list, resp, err := c.Actions.ListWorkflowJobs(ctx, org, repo, runID, opt)
		if err != nil {
			return nil, fmt.Errorf("list jobs run %d: %w", runID, err)
		}
		for _, j := range list.Jobs {
			if !strings.HasPrefix(j.GetRunnerName(), domain.RunnerNamePrefix) {
				continue
			}
			out = append(out, ledger.Record{
				JobID:       j.GetID(),
				Org:         org,
				Repo:        repo,
				Labels:      j.Labels,
				RunnerName:  j.GetRunnerName(),
				StartedAt:   j.GetStartedAt().Time,
				CompletedAt: j.GetCompletedAt().Time,
			})
		}
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return out, nil
}

// repoNames returns the org's configured repo allowlist, or every repo the App
// installation can access when no allowlist is set.
func repoNames(ctx context.Context, c *github.Client, o config.Org) ([]string, error) {
	if len(o.Repos) > 0 {
		return o.Repos, nil
	}
	repos, err := gh.ListInstallationRepos(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("list repos: %w", err)
	}
	names := make([]string, len(repos))
	for i, r := range repos {
		names[i] = r.GetName()
	}
	return names, nil
}

func defaultControllerConfig() string {
	return filepath.Join(os.Getenv("HOME"), ".config", "ushr", "controller.yaml")
}
