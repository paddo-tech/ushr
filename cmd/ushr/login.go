package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/enroll"
	"github.com/paddo-tech/ushr/internal/svc"
)

const (
	defaultControlPlane = "https://cp.ushr.io"
	defaultWebURL       = "https://ushr.io"
	loginTimeout        = 5 * time.Minute
	pollInterval        = 2 * time.Second
)

// runLogin takes a fresh host to a working runner in one command: write a
// default agent config if none exists, preflight the driver's prereqs (guided
// installs), enroll via browser approval (PKCE), create GitHub App keys for
// any enrolled org that lacks one, then install and start the agent service.
func runLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	cp := fs.String("control-plane", defaultControlPlane, "control plane base URL")
	web := fs.String("web", defaultWebURL, "web app base URL for browser approval")
	configPath := fs.String("config", filepath.Join(os.Getenv("HOME"), ".config", "ushr", "agent.yaml"), "agent config path (credentials written alongside)")
	keyDir := fs.String("key-dir", filepath.Join(os.Getenv("HOME"), ".secrets"), "directory for GitHub App private keys")
	yes := fs.Bool("y", false, "answer yes to prompts")
	if err := fs.Parse(args); err != nil {
		return err
	}

	created, err := ensureAgentConfig(*configPath)
	if err != nil {
		return err
	}
	if created {
		fmt.Println("==> Wrote default agent config:", *configPath)
	}
	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		return err
	}
	if err := preflight(ctx, cfg, *yes); err != nil {
		return err
	}

	var creds sessionResult
	if cfg.Token != "" && cfg.ControllerURL == *cp {
		status, resumeErr := setupRequest(ctx, http.MethodGet, strings.TrimRight(*web, "/")+"/api/runner-setup", cfg.Token, nil, &creds)
		if resumeErr != nil && status != http.StatusUnauthorized {
			return resumeErr
		}
		if resumeErr == nil {
			creds.Token = cfg.Token
			fmt.Println("==> Resuming setup for", creds.AgentName)
			if len(missingKeyScopes(cfg, creds.Orgs)) == 0 && confirm("    Change this host's GitHub access?", false, false) {
				creds.Token = ""
			}
		}
	}
	if creds.Token == "" {
		verifier, err := randToken()
		if err != nil {
			return fmt.Errorf("generate verifier: %w", err)
		}
		session, err := randToken()
		if err != nil {
			return fmt.Errorf("generate session id: %w", err)
		}
		userCode, err := confirmCode()
		if err != nil {
			return fmt.Errorf("generate confirmation code: %w", err)
		}
		sealKey, err := enroll.NewSealKey()
		if err != nil {
			return fmt.Errorf("generate seal key: %w", err)
		}

		if err := postJSON(ctx, *cp+"/v1/cli/session", map[string]string{
			"session_id": session,
			"challenge":  enroll.Challenge(verifier),
			"agent_name": agentName(*configPath),
			"user_code":  userCode,
			"public_key": enroll.SealPublicKey(sealKey),
		}); err != nil {
			return fmt.Errorf("start login session: %w", err)
		}

		authURL := fmt.Sprintf("%s/cli/auth?session=%s", *web, session)
		fmt.Println("==> Opening", authURL)
		fmt.Println("    (headless box? open that URL on any device — phone, laptop)")
		fmt.Println()
		fmt.Println("==> Confirmation code:", userCode)
		fmt.Println("    Approve only if the page shows this exact code.")
		tryOpen(authURL)

		fmt.Print("==> Waiting for approval")
		creds, err = pollSession(ctx, *cp, session, verifier)
		if err != nil {
			fmt.Println()
			return err
		}
		creds.Token, err = enroll.OpenSealed(sealKey, creds.Token)
		if err != nil {
			fmt.Println()
			return fmt.Errorf("enrolled, but failed to open the token: %w — run `ushr login` again", err)
		}
		fmt.Println(" ✓")

		credPath := config.CredentialsPath(*configPath)
		if err := config.WriteCredentials(credPath, config.Credentials{
			ControllerURL: *cp,
			Token:         creds.Token,
			Name:          creds.AgentName,
			Orgs:          creds.Orgs,
		}); err != nil {
			return fmt.Errorf("write credentials: %w", err)
		}
		fmt.Printf("✓ Enrolled agent %q for org(s) %s\n", creds.AgentName, strings.Join(creds.Orgs, ", "))
		fmt.Println("  Wrote", credPath)
	}

	// Model B: the agent mints JIT runner configs with a GitHub App key it
	// holds locally, so each enrolled org needs one on this host. Chain the
	// App-manifest flow for any org that has none instead of leaving a
	// hand-edit gap between login and a working runner.
	for _, scope := range missingKeyScopes(cfg, creds.Orgs) {
		fmt.Printf("\n==> %s has no GitHub App key on this host yet — creating one.\n", scope)
		fmt.Println("    (each host runs its own GitHub App; hosts already serving this scope keep theirs)")
		if err := hostedAppSetup(ctx, scope, *keyDir, *configPath, *web, creds.Token); err != nil {
			return fmt.Errorf("GitHub App setup for %s: %w", scope, err)
		}
	}

	cfg, err = config.LoadAgent(*configPath)
	if err != nil {
		return err
	}
	for _, scope := range creds.Orgs {
		if err := checkInstalled(ctx, cfg, scope); err != nil {
			return err
		}
	}

	// The service definition execs this path; installing it with the binary
	// missing leaves launchd/systemd crash-looping with an empty log.
	if _, err := os.Stat(agentBinaryPath()); err != nil {
		return fmt.Errorf("%s not found — re-run the installer (get.ushr.io ships all three binaries)", agentBinaryPath())
	}
	if _, err := svc.Install(svc.Agent); err != nil {
		return fmt.Errorf("install agent service: %w", err)
	}
	if err := svc.Restart(svc.Agent); err != nil {
		return fmt.Errorf("start agent service: %w", err)
	}
	fmt.Println()
	fmt.Println("✓ Agent started. Waiting for its first heartbeat in the dashboard.")
	fmt.Println("  Finish setup:      " + strings.TrimRight(*web, "/") + "/dashboard")
	fmt.Printf("  Target jobs with:  runs-on: [%s]\n", strings.Join(cfg.Labels, ", "))
	fmt.Println("  Check health:      ushr doctor")
	fmt.Println("  Watch logs:        " + svc.LogHint(svc.Agent))
	return nil
}

// missingKeyScopes returns the enrolled org/repo scopes with no App key
// configured in agent.yaml.
func missingKeyScopes(cfg *config.Agent, enrolled []string) []string {
	var out []string
	for _, e := range enrolled {
		if _, _, _, ok := configuredTarget(cfg, e); !ok {
			out = append(out, e)
		}
	}
	return out
}

// agentName is what the approval page displays as the enrolling host's
// identity: the agent config's name when one exists, else the hostname.
// Sanitized either way — the approve flow locks this value into a read-only
// field, so a name outside the runner-name charset would leave the approver
// staring at an un-fixable validation error.
func agentName(configPath string) string {
	if cfg, err := config.LoadAgent(configPath); err == nil && cfg.Name != "" {
		return sanitizeName(cfg.Name)
	}
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return sanitizeName(host)
}

// sanitizeName maps a host/config name onto the runner-name charset the
// approve flow accepts ([a-zA-Z0-9][a-zA-Z0-9_-]*, ≤32): first DNS label
// only, other characters become '-', leading non-alphanumerics drop.
func sanitizeName(name string) string {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		name = name[:i]
	}
	isAlnum := func(c byte) bool {
		return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
	}
	b := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		if isAlnum(c) || c == '-' || c == '_' {
			b = append(b, c)
		} else {
			b = append(b, '-')
		}
	}
	for len(b) > 0 && !isAlnum(b[0]) {
		b = b[1:]
	}
	if len(b) > 32 {
		b = b[:32]
	}
	return string(b)
}

// confirmCode returns a short human-matchable code (e.g. "KTX-42P") from an
// alphabet without lookalike glyphs. It is a human check, not a server-enforced
// binding: the approval page displays it so a forwarded phishing link shows a
// code the victim's terminal never printed. Rejection sampling keeps the
// distribution uniform (240 = 8×30 is the largest multiple of the alphabet).
func confirmCode() (string, error) {
	const alphabet = "ABCDEFGHJKMNPQRSTVWXYZ23456789"
	out := make([]byte, 0, 7)
	for len(out) < 7 {
		var b [1]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		if b[0] >= 240 {
			continue
		}
		if len(out) == 3 {
			out = append(out, '-')
		}
		out = append(out, alphabet[int(b[0])%len(alphabet)])
	}
	return string(out), nil
}

type sessionResult struct {
	Token     string   `json:"token"`
	Orgs      []string `json:"orgs"`
	AgentName string   `json:"agent_name"`
}

func pollSession(ctx context.Context, cp, session, verifier string) (sessionResult, error) {
	url := fmt.Sprintf("%s/v1/cli/session/%s/fetch", cp, session)
	timeout := time.After(loginTimeout)
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		res, status, err := fetchSession(ctx, url, verifier)
		switch {
		case status == http.StatusOK && err != nil:
			// The server already consumed the session (200), so a decode failure
			// means the minted token is gone — terminal, not worth re-polling.
			return sessionResult{}, fmt.Errorf("enrolled, but failed to read the token: %w — run `ushr login` again", err)
		case err != nil:
			// transient — keep polling within the timeout
		case status == http.StatusOK:
			return res, nil
		case status == http.StatusGone:
			return sessionResult{}, fmt.Errorf("login session expired — run `ushr login` again")
		case status >= 500:
			// A control-plane blip (e.g. a momentary DB error) is transient; keep
			// polling within the timeout rather than aborting the whole login.
		case status != http.StatusAccepted:
			return sessionResult{}, fmt.Errorf("unexpected status %d from control plane", status)
		}
		fmt.Print(".")
		select {
		case <-ctx.Done():
			return sessionResult{}, ctx.Err()
		case <-timeout:
			return sessionResult{}, fmt.Errorf("timed out waiting for approval")
		case <-tick.C:
		}
	}
}

func fetchSession(ctx context.Context, url, verifier string) (sessionResult, int, error) {
	body, err := json.Marshal(map[string]string{"verifier": verifier})
	if err != nil {
		return sessionResult{}, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return sessionResult{}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return sessionResult{}, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return sessionResult{}, resp.StatusCode, nil
	}
	var res sessionResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return sessionResult{}, resp.StatusCode, err
	}
	return res, resp.StatusCode, nil
}

func postJSON(ctx context.Context, url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("control plane returned %d", resp.StatusCode)
	}
	return nil
}

// httpClient bounds each request so a blackholed control-plane connection can't
// hang login indefinitely (the login timeout is only checked between polls).
var httpClient = &http.Client{Timeout: 15 * time.Second}

func randToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
