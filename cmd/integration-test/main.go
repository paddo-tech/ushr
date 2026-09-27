// Command integration-test exercises the Tart driver end-to-end against a
// real paddo-runner-mac image. Not a unit test — requires Tart, the saved
// image, the gh CLI authenticated to the org, and an Apple Silicon host.
//
// Usage:
//
//	go run ./cmd/integration-test -org=paddo-tech [-trigger-repo=example-repo]
//
// What it does:
//  1. Mints a JIT config via gh api .../runners/generate-jitconfig
//  2. Provision: clones paddo-runner-mac, boots, installs+starts actions/runner
//  3. Waits for the runner to appear "online" in GitHub
//  4. (Optional) Dispatches a workflow on -trigger-repo and waits for completion
//  5. Status check (should be Done after job, since JIT runner is single-use)
//  6. Destroy: tart stop+delete
//  7. Verifies VM is gone via List
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/driver/tart"
)

func main() {
	var (
		org          = flag.String("org", "paddo-tech", "GitHub org to register the runner with")
		triggerRepo  = flag.String("trigger-repo", "", "if set, dispatch smoke.yml on this repo and wait")
		runnerName   = flag.String("name", fmt.Sprintf("ushr-itest-%d", time.Now().Unix()), "runner name")
		overallTimeo = flag.Duration("timeout", 10*time.Minute, "overall timeout")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *overallTimeo)
	defer cancel()

	// Cancel on Ctrl-C so we don't leak VMs.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; log.Println("interrupt — cancelling"); cancel() }()

	step("Minting JIT config for", *org)
	jit, ghRunnerID, err := mintJIT(ctx, *org, *runnerName)
	if err != nil {
		log.Fatalf("mintJIT: %v", err)
	}
	log.Printf("  GitHub runner id %d, jit length %d bytes", ghRunnerID, len(jit))

	d := tart.New(tart.Options{})

	step("Provision")
	t0 := time.Now()
	handle, err := d.Provision(ctx, driver.ProvisionRequest{
		Org:      *org,
		JITToken: jit,
		Labels:   []string{"self-hosted", "macOS", "ARM64", "paddo-runner-mac"},
	})
	if err != nil {
		// Best-effort cleanup of the GitHub runner record.
		_ = githubDeleteRunner(context.Background(), *org, ghRunnerID)
		log.Fatalf("Provision: %v", err)
	}
	defer func() {
		log.Println("Final cleanup — Destroy + GitHub delete")
		_ = d.Destroy(context.Background(), handle)
		_ = githubDeleteRunner(context.Background(), *org, ghRunnerID)
	}()
	log.Printf("  provisioned %q in %s", handle, time.Since(t0).Round(time.Second))

	step("Wait for runner to register as online in GitHub")
	if err := waitGithubOnline(ctx, *org, ghRunnerID); err != nil {
		log.Fatalf("waitGithubOnline: %v", err)
	}

	if *triggerRepo != "" {
		step("Dispatch smoke.yml on", *triggerRepo)
		if err := triggerWorkflow(ctx, *org, *triggerRepo); err != nil {
			log.Fatalf("triggerWorkflow: %v", err)
		}
		step("Wait for workflow run to complete")
		runID, err := waitLatestRun(ctx, *org, *triggerRepo)
		if err != nil {
			log.Fatalf("waitLatestRun: %v", err)
		}
		log.Printf("  workflow run %d completed", runID)
	}

	step("Status check (expect Done after JIT runner exits)")
	// After the job, the JIT runner exits and Status should report Done.
	// Give it a moment for the runner process to actually exit.
	time.Sleep(5 * time.Second)
	status, err := d.Status(ctx, handle)
	log.Printf("  status=%v err=%v", status, err)

	step("Destroy")
	if err := d.Destroy(ctx, handle); err != nil {
		log.Fatalf("Destroy: %v", err)
	}

	step("Verify List shows our prefix is empty")
	slots, err := d.List(ctx)
	if err != nil {
		log.Fatalf("List: %v", err)
	}
	if len(slots) != 0 {
		log.Fatalf("expected 0 slots remaining, got %d: %v", len(slots), slots)
	}
	log.Println("  ✓ no orphan slots")

	step("All checks passed")
}

func step(parts ...string) {
	log.Printf(">>> %s", strings.Join(parts, " "))
}

// mintJIT shells out to `gh api` to generate a JIT config and returns the
// encoded config plus the new runner's GitHub ID (for cleanup on failure).
func mintJIT(ctx context.Context, org, name string) (string, int64, error) {
	args := []string{
		"api", "-X", "POST",
		fmt.Sprintf("/orgs/%s/actions/runners/generate-jitconfig", org),
		"-f", "name=" + name,
		"-F", "runner_group_id=1",
		"-f", "labels[]=self-hosted",
		"-f", "labels[]=macOS",
		"-f", "labels[]=ARM64",
		"-f", "labels[]=paddo-runner-mac",
	}
	out, err := exec.CommandContext(ctx, "gh", args...).Output()
	if err != nil {
		return "", 0, fmt.Errorf("gh api generate-jitconfig: %w", err)
	}
	var resp struct {
		Runner struct {
			ID int64 `json:"id"`
		} `json:"runner"`
		EncodedJITConfig string `json:"encoded_jit_config"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return "", 0, fmt.Errorf("decode jitconfig response: %w", err)
	}
	return resp.EncodedJITConfig, resp.Runner.ID, nil
}

func githubDeleteRunner(ctx context.Context, org string, runnerID int64) error {
	return exec.CommandContext(ctx, "gh", "api", "-X", "DELETE",
		fmt.Sprintf("/orgs/%s/actions/runners/%d", org, runnerID)).Run()
}

func waitGithubOnline(ctx context.Context, org string, runnerID int64) error {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		out, err := exec.CommandContext(ctx, "gh", "api",
			fmt.Sprintf("/orgs/%s/actions/runners/%d", org, runnerID),
			"--jq", ".status").Output()
		if err == nil && strings.TrimSpace(string(out)) == "online" {
			log.Printf("  ✓ runner %d online", runnerID)
			return nil
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("runner %d did not become online within 2m", runnerID)
}

func triggerWorkflow(ctx context.Context, org, repo string) error {
	cmd := exec.CommandContext(ctx, "gh", "workflow", "run", "smoke.yml",
		"--repo", fmt.Sprintf("%s/%s", org, repo), "--ref", "main")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gh workflow run: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func waitLatestRun(ctx context.Context, org, repo string) (int64, error) {
	// Give GitHub a moment to register the dispatched run.
	time.Sleep(3 * time.Second)
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		out, err := exec.CommandContext(ctx, "gh", "run", "list",
			"--repo", fmt.Sprintf("%s/%s", org, repo),
			"--workflow", "smoke.yml",
			"--limit", "1",
			"--json", "databaseId,status,conclusion").Output()
		if err == nil {
			var runs []struct {
				ID         int64  `json:"databaseId"`
				Status     string `json:"status"`
				Conclusion string `json:"conclusion"`
			}
			if err := json.Unmarshal(out, &runs); err == nil && len(runs) > 0 {
				r := runs[0]
				if r.Status == "completed" {
					log.Printf("  conclusion: %s", r.Conclusion)
					if r.Conclusion != "success" {
						return r.ID, fmt.Errorf("workflow concluded %q (not success)", r.Conclusion)
					}
					return r.ID, nil
				}
				log.Printf("  workflow %d status=%s", r.ID, r.Status)
			}
		}
		time.Sleep(5 * time.Second)
	}
	return 0, fmt.Errorf("workflow did not complete within 5m")
}
