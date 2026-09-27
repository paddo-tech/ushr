package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/svc"
)

const (
	manifestName = "ushr"
	homepageURL  = "https://ushr.io"
	priorityHelp = "scheduling priority for this target (higher wins)"
	// appSetupTimeout bounds each browser-dependent wait (App creation, App
	// install): an abandoned tab must not hang login forever.
	appSetupTimeout = 10 * time.Minute
)

func runSetup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	org := fs.String("org", "", "GitHub organization to create the App in (org-level runners)")
	repo := fs.String("repo", "", "GitHub owner/repo to create the App for (repo-level runners, e.g. personal accounts)")
	priority := fs.Int("priority", 100, priorityHelp)
	keyDir := fs.String("key-dir", filepath.Join(os.Getenv("HOME"), ".secrets"), "directory to write the App private key")
	configPath := fs.String("config", filepath.Join(os.Getenv("HOME"), ".config", "ushr", "agent.yaml"), "agent config to record the App in")
	yes := fs.Bool("y", false, "answer yes to prompts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*org == "") == (*repo == "") {
		return errors.New("exactly one of --org or --repo is required")
	}
	scope := *org
	if scope == "" {
		scope = *repo
	}

	created, err := ensureAgentConfig(*configPath)
	if err != nil {
		return err
	}
	if created {
		fmt.Println("==> Wrote default agent config:", *configPath)
	}
	// Every run of the manifest flow registers a real GitHub App; re-running
	// setup for an already-configured scope should not silently mint another.
	if cfg, err := config.LoadAgent(*configPath); err == nil {
		if id, ok := configuredTarget(cfg, scope); ok && id != 0 {
			fmt.Printf("==> %s is already configured (App id %d).\n", scope, id)
			// -y keeps the existing App: assume-yes reruns are provisioning
			// scripts, and auto-answering yes here would mint a duplicate
			// GitHub App on every converge.
			if *yes || !confirm("    Create ANOTHER GitHub App for it?", false, false) {
				fmt.Println("    Keeping the existing App.")
				return offerServices(*configPath, *yes)
			}
		}
	}
	if err := appSetup(ctx, scope, *keyDir, *configPath, *priority); err != nil {
		return err
	}
	return offerServices(*configPath, *yes)
}

// offerServices installs and starts the daemons after setup: the agent always,
// plus the local controller when the config points at loopback (the OSS
// single-host layout — a hosted controller_url needs no local controller).
func offerServices(configPath string, assumeYes bool) error {
	cfg, err := config.LoadAgent(configPath)
	if err != nil {
		return err
	}
	units := []svc.Unit{svc.Agent}
	if u, err := neturl.Parse(cfg.ControllerURL); err == nil {
		switch u.Hostname() {
		case "127.0.0.1", "localhost", "::1":
			units = []svc.Unit{svc.Controller, svc.Agent}
		}
	}
	fmt.Println()
	if !confirm("==> Install and start the ushr service(s) now?", true, assumeYes) {
		fmt.Println("    Skipped. Start later by re-running, or check state with `ushr doctor`.")
		return nil
	}
	for _, u := range units {
		if _, err := svc.Install(u); err != nil {
			return fmt.Errorf("install %s service: %w", u, err)
		}
		if err := svc.Restart(u); err != nil {
			return fmt.Errorf("start %s service: %w", u, err)
		}
		fmt.Printf("==> ushr-%s service running ✓  (logs: %s)\n", u, svc.LogHint(u))
	}
	return nil
}

// appSetup runs the GitHub App manifest flow for scope (an org name or an
// "owner/repo"), writes the private key, opens the App install page, and
// records the target in the agent config — no hand-editing.
func appSetup(ctx context.Context, scope, keyDir, configPath string, priority int) error {
	// The manifest form-POST target, the App permissions, and the key filename
	// all differ between an org App (org-level runner pool) and a repo App
	// (repo-level runners — the only option for personal accounts).
	owner, repoName, isRepo := domain.SplitRepoScope(scope)
	if !isRepo && strings.Contains(scope, "/") {
		return fmt.Errorf("repo scope must be owner/repo, got %q", scope)
	}
	var actionURL string
	var perms map[string]string
	if !isRepo {
		actionURL = fmt.Sprintf("https://github.com/organizations/%s/settings/apps/new", scope)
		perms = map[string]string{
			"actions":                          "read",
			"metadata":                         "read",
			"organization_self_hosted_runners": "write",
		}
	} else {
		// Created under the signed-in user, not an org. Repo-level runner
		// management rides on the repo "Administration" permission — there is no
		// dedicated repo self-hosted-runners scope.
		actionURL = "https://github.com/settings/apps/new"
		perms = map[string]string{
			"actions":        "read",
			"metadata":       "read",
			"administration": "write",
		}
	}

	state, err := randomHex(16)
	if err != nil {
		return fmt.Errorf("generate state: %w", err)
	}
	suffix, err := randomHex(3)
	if err != nil {
		return fmt.Errorf("generate suffix: %w", err)
	}
	// Ephemeral port: a fixed one can be held hostage by a previous hung login
	// or any other listener, failing every future setup on the host.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen on callback port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	// App names must be alphanumeric+dashes, so flatten "owner/repo".
	appName := fmt.Sprintf("%s-%s-%s", manifestName, strings.ReplaceAll(scope, "/", "-"), suffix)
	manifest := manifestJSON(appName, fmt.Sprintf("http://localhost:%d/callback", port), perms)

	localURL := fmt.Sprintf("http://localhost:%d/", port)
	fmt.Println()
	fmt.Println("==> Open this URL in your browser:")
	fmt.Println("    " + localURL)
	fmt.Println()
	fmt.Println("    (browser will auto-POST the manifest to GitHub, prefilling the App form)")
	fmt.Println()
	tryOpen(localURL)
	fmt.Printf("==> Waiting for App-creation callback ...\n")

	waitCtx, cancelWait := context.WithTimeout(ctx, appSetupTimeout)
	cb, err := waitForCallback(waitCtx, listener, actionURL, manifest, state)
	cancelWait()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("timed out waiting for the App to be created in the browser — re-run when ready")
		}
		return err
	}

	cfg, err := exchangeCode(ctx, cb.code)
	if err != nil {
		return fmt.Errorf("exchange code: %w", err)
	}
	fmt.Printf("==> App created: %s (id %d)\n", cfg.GetSlug(), cfg.GetID())

	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", keyDir, err)
	}
	// Named after the App (unique per creation), not the scope: each host gets
	// its own App, and a scope-named file would clobber another App's key.
	keyPath := filepath.Join(keyDir, appName+".pem")
	if err := os.WriteFile(keyPath, []byte(cfg.GetPEM()), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", keyPath, err)
	}
	fmt.Printf("==> Wrote private key %s (mode 0600)\n", keyPath)

	installURL := fmt.Sprintf("https://github.com/apps/%s/installations/new", cfg.GetSlug())
	fmt.Println()
	fmt.Printf("==> Install the App on %s (pick the repos your runners serve):\n", scope)
	fmt.Println("    " + installURL)
	fmt.Println()
	tryOpen(installURL)

	if !isRepo {
		err = config.AddOrg(configPath, config.Org{
			Name: scope, AppID: cfg.GetID(), PrivateKeyPath: keyPath, Priority: priority,
		})
	} else {
		err = config.AddRepo(configPath, config.RepoTarget{
			Owner: owner, Repo: repoName, AppID: cfg.GetID(), PrivateKeyPath: keyPath, Priority: priority,
		})
	}
	if err != nil {
		return fmt.Errorf("record %s in %s: %w", scope, configPath, err)
	}
	fmt.Printf("==> Recorded %s (app id %d) in %s\n", scope, cfg.GetID(), configPath)

	waitForAppInstall(ctx, cfg.GetID(), keyPath, scope, installURL)
	return nil
}

// configuredTarget reports whether scope is recorded in the agent config, and
// with which App id (0 = placeholder entry awaiting an App).
func configuredTarget(cfg *config.Agent, scope string) (int64, bool) {
	for _, o := range cfg.Orgs {
		if strings.EqualFold(o.Name, scope) {
			return o.AppID, true
		}
	}
	for _, r := range cfg.Repos {
		if strings.EqualFold(r.Scope(), scope) {
			return r.AppID, true
		}
	}
	return 0, false
}

type callback struct {
	code  string
	state string
}

// autoPostHTML renders a tiny page that immediately POSTs the manifest to
// GitHub. App Manifests don't accept GET — the form has to be submitted.
const autoPostHTML = `<!DOCTYPE html>
<html><head><title>ushr setup</title></head>
<body>
  <p>Posting App manifest to GitHub…</p>
  <form id="f" action="{{.ActionURL}}?state={{.State}}" method="POST">
    <input type="hidden" name="manifest" value='{{.Manifest}}'>
    <noscript><button type="submit">Continue to GitHub</button></noscript>
  </form>
  <script>document.getElementById('f').submit();</script>
</body></html>`

// callbackDoneHTML is the success page shown after GitHub redirects back, styled
// to match the ushr dashboard (dark, monospace, amber accent).
const callbackDoneHTML = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>ushr — setup complete</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:#0a0a0b; color:#ededed;
         font-family: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace; }
  .card { text-align:center; padding:2.75rem 3.25rem; border:1px solid #262629;
          border-radius:14px; background:#111114; box-shadow:0 1px 40px rgba(0,0,0,.4); }
  .check { font-size:2.25rem; line-height:1; color:#f0b429; }
  .title { margin-top:1rem; font-weight:600; font-size:1.05rem; letter-spacing:-0.01em; }
  .sub { margin-top:.6rem; color:#8b8b90; font-size:.85rem; }
  .kbd { color:#f0b429; }
</style></head>
<body>
  <div class="card">
    <div class="check">✓</div>
    <div class="title">App created</div>
    <div class="sub">Return to your terminal — <span class="kbd">ushr</span> is finishing setup.<br>You can close this tab.</div>
  </div>
</body></html>`

func waitForCallback(ctx context.Context, listener net.Listener, actionURL, manifest, expectedState string) (*callback, error) {
	tmpl, err := template.New("autopost").Parse(autoPostHTML)
	if err != nil {
		return nil, fmt.Errorf("parse autopost template: %w", err)
	}

	cbCh := make(chan *callback, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = tmpl.Execute(w, struct{ ActionURL, State, Manifest string }{
			ActionURL: actionURL,
			State:     expectedState,
			Manifest:  manifest,
		})
	})
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		got := q.Get("state")
		if got != expectedState {
			http.Error(w, "state mismatch", http.StatusBadRequest)
			errCh <- fmt.Errorf("callback state mismatch: got %q want %q", got, expectedState)
			return
		}
		code := q.Get("code")
		if code == "" {
			http.Error(w, "missing code", http.StatusBadRequest)
			errCh <- errors.New("callback missing ?code=")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(callbackDoneHTML))
		cbCh <- &callback{code: code, state: got}
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(listener) //nolint:errcheck // shutdown handled below

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	select {
	case cb := <-cbCh:
		return cb, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func exchangeCode(ctx context.Context, code string) (*github.AppConfig, error) {
	c := github.NewClient(nil)
	cfg, _, err := c.Apps.CompleteAppManifest(ctx, code)
	return cfg, err
}

func manifestJSON(name, redirectURL string, perms map[string]string) string {
	m := map[string]any{
		"name": name,
		"url":  homepageURL,
		// hook_attributes.url is required by GitHub even when active=false.
		// Point it at the homepage; nothing will be delivered.
		"hook_attributes": map[string]any{
			"url":    homepageURL,
			"active": false,
		},
		"redirect_url":        redirectURL,
		"public":              false,
		"default_permissions": perms,
		"default_events":      []string{},
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func tryOpen(rawURL string) {
	var bin string
	switch runtime.GOOS {
	case "darwin":
		bin = "open"
	case "linux":
		bin = "xdg-open"
	default:
		return
	}
	if _, err := exec.LookPath(bin); err != nil {
		return
	}
	_ = exec.Command(bin, rawURL).Start()
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
