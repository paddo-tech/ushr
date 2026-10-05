package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/disk"
	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/driver/factory"
	gh "github.com/paddo-tech/ushr/internal/source/github"
	"github.com/paddo-tech/ushr/internal/svc"
)

// runDoctor checks everything between this host and a working runner and
// prints a fix hint for each failure, so "the runner isn't picking up jobs"
// is diagnosable without reading daemon logs.
func runDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	configPath := fs.String("config", filepath.Join(os.Getenv("HOME"), ".config", "ushr", "agent.yaml"), "agent config path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	failed := 0
	check := func(ok bool, label, hint string) {
		if ok {
			fmt.Printf("  ✓ %s\n", label)
			return
		}
		failed++
		fmt.Printf("  ✗ %s\n", label)
		if hint != "" {
			fmt.Printf("      → %s\n", hint)
		}
	}

	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		check(false, fmt.Sprintf("config %s", *configPath), err.Error()+" — run `ushr login` (hosted) or `ushr setup` (local) to create it")
		return fmt.Errorf("%d check(s) failed", failed)
	}
	check(true, fmt.Sprintf("config %s", *configPath), "")

	switch cfg.Driver.Type {
	case config.DriverTypeTart, "":
		_, tartErr := exec.LookPath("tart")
		check(tartErr == nil, "tart installed", "brew install openai/tools/tart")
		if tartErr == nil {
			image := cfg.Driver.Image
			if image == "" {
				image = "paddo-runner-mac"
			}
			ok, err := tartImageExists(ctx, image)
			check(err == nil && ok, fmt.Sprintf("runner VM image %q", image),
				fmt.Sprintf("tart clone %s %s", cirrusBaseImage, image))
		}
	case config.DriverTypeDocker:
		rt := cfg.Driver.Runtime
		if rt == "" {
			for _, c := range []string{"docker", "podman"} {
				if commandExists(c) {
					rt = c
					break
				}
			}
		}
		check(rt != "" && commandExists(rt), "container runtime (docker/podman)", "install docker or podman, or re-run `ushr login`")
		if rt != "" && (cfg.Driver.BuildCache == nil || *cfg.Driver.BuildCache) {
			check(exec.CommandContext(ctx, rt, "buildx", "version").Run() == nil,
				"buildx plugin (build cache)", "sudo apt-get install docker-buildx — or set driver.build_cache: false")
		}
	case config.DriverTypeLume:
		check(commandExists("lume"), "lume installed", "https://github.com/trycua/lume")
	}

	// Free space on the driver's image store. Below the floor the agent takes no
	// work at all, which presents as "the runner never picks up jobs" — the one
	// symptom this command exists to explain.
	if drv, derr := factory.New(cfg.Driver); derr == nil {
		if w, ok := drv.(driver.DiskWatcher); ok {
			if path := w.DiskPath(ctx); path != "" {
				floor := factory.MinFreeBytes(cfg.Driver)
				u, serr := disk.Stat(path)
				switch {
				case serr != nil:
					check(false, "disk space for "+path, serr.Error())
				case floor > 0 && u.Total < 3*floor:
					// Reclaim aims above the floor, so a disk only a couple of
					// floors wide can never reach its target and would prune on
					// every sweep.
					check(false, fmt.Sprintf("disk space for %s (%d GB free of %d GB)", path, u.Free>>30, u.Total>>30),
						fmt.Sprintf("this disk is too small for a %d GB floor — set driver.min_free_gb to at most %d", floor>>30, u.Total/3>>30))
				default:
					check(floor == 0 || u.Free >= floor,
						fmt.Sprintf("disk space for %s (%d GB free)", path, u.Free>>30),
						fmt.Sprintf("free up space or lower driver.min_free_gb — the agent refuses jobs under %d GB free", floor>>30))
				}
			}
		}
	}

	// App keys: every configured target needs a readable private key AND an
	// App that is actually installed on the scope — key-on-disk alone still
	// can't mint runner configs. A transport error is reported as unverified,
	// not failed: offline doctor must not flag a correct setup.
	checkTarget := func(appID int64, keyPath, scope, baseURL, setupHint string) {
		if appID == 0 {
			check(false, fmt.Sprintf("GitHub App key for %s", scope), setupHint)
			return
		}
		key, err := os.ReadFile(keyPath)
		check(err == nil, fmt.Sprintf("GitHub App key for %s", scope), setupHint)
		if err != nil {
			return
		}
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		installed, ierr := appInstalled(checkCtx, appID, key, scope, baseURL)
		cancel()
		switch {
		case installed:
			check(true, fmt.Sprintf("GitHub App installed on %s", scope), "")
		case isNotInstalled(ierr):
			// 404 for a repo target also fires on a mistyped owner/repo.
			check(false, fmt.Sprintf("GitHub App installed on %s", scope),
				fmt.Sprintf("install App id %d on %s (%s → Settings → Developer settings → GitHub Apps) — or fix the scope if it's mistyped", appID, scope, gh.WebURL(baseURL)))
		default:
			// Not proven either way: could be network, a bad/corrupt key, or
			// revoked credentials — don't fail a possibly-correct setup, but
			// don't claim a cause the error doesn't establish.
			fmt.Printf("  ? GitHub App install on %s could not be verified: %v\n", scope, ierr)
		}
	}
	// A GHES instance that is down or blocked fails every target on it, so name
	// the instance once before the per-target checks.
	checked := map[string]bool{}
	checkBase := func(baseURL string) {
		if baseURL == "" || checked[baseURL] {
			return
		}
		checked[baseURL] = true
		check(reachable(ctx, gh.APIURL(baseURL)), fmt.Sprintf("GitHub Enterprise Server reachable (%s)", baseURL),
			"check base_url in agent.yaml, network and TLS trust for the instance")
	}
	for _, o := range cfg.Orgs {
		checkBase(o.BaseURL)
		checkTarget(o.AppID, o.PrivateKeyPath, o.Name, o.BaseURL, setupCmd("--org "+o.Name, o.BaseURL))
	}
	for _, r := range cfg.Repos {
		checkBase(r.BaseURL)
		checkTarget(r.AppID, r.PrivateKeyPath, r.Scope(), r.BaseURL, setupCmd("--repo "+r.Scope(), r.BaseURL))
	}
	if len(cfg.Orgs) == 0 && len(cfg.Repos) == 0 {
		check(false, "orgs/repos configured", "run `ushr login` (hosted) or `ushr setup --org NAME` (local)")
	}

	_, binErr := os.Stat(agentBinaryPath())
	check(binErr == nil, "ushr-agent binary at "+agentBinaryPath(),
		"re-run the installer (get.ushr.io) — it ships ushr, ushr-agent, ushr-controller together")

	check(svc.Running(svc.Agent), "ushr-agent service running",
		"re-run `ushr login`, or watch "+svc.LogHint(svc.Agent))

	// Reachability of whatever controller the agent actually talks to
	// (credentials.yaml overrides agent.yaml).
	if cfg.ControllerURL != "" {
		check(reachable(ctx, cfg.ControllerURL+"/healthz"), fmt.Sprintf("control plane reachable (%s)", cfg.ControllerURL),
			"check the URL, network, and (local OSS) that ushr-controller is running")
	}

	if failed > 0 {
		return fmt.Errorf("%d check(s) failed", failed)
	}
	fmt.Println("\nAll checks passed.")
	return nil
}

// reachable reports whether url answers below 500 within 10s.
func reachable(ctx context.Context, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode < 500
}

func setupCmd(target, baseURL string) string {
	if baseURL == "" {
		return "ushr setup " + target
	}
	return "ushr setup " + target + " --base-url " + baseURL
}
