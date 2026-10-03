package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validControllerYAML = `
version: "1"
listen: 127.0.0.1:7080
token: secret
orgs:
  - name: paddo-tech
    app_id: 12345
    private_key_path: /tmp/key.pem
    priority: 100
policy:
  type: priority
  aging:
    boost_per_minute: 1
`

const validAgentYAML = `
version: "1"
controller_url: http://127.0.0.1:7080
token: secret
name: mac-runner-1
labels: [self-hosted, macOS, ARM64, paddo-runner-mac]
driver:
  type: tart
  image: ghcr.io/cirruslabs/macos-runner:tahoe
  capacity: 2
orgs:
  - name: paddo-tech
    app_id: 12345
    private_key_path: /tmp/key.pem
    priority: 100
source:
  type: poll
  interval: 10s
`

func writeTemp(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadController(t *testing.T) {
	cfg, err := LoadController(writeTemp(t, "c.yaml", validControllerYAML))
	if err != nil {
		t.Fatalf("LoadController: %v", err)
	}
	if cfg.Listen != "127.0.0.1:7080" {
		t.Errorf("Listen = %q", cfg.Listen)
	}
	if len(cfg.Orgs) != 1 || cfg.Orgs[0].Name != "paddo-tech" || cfg.Orgs[0].Priority != 100 {
		t.Errorf("Orgs = %+v", cfg.Orgs)
	}
	if cfg.Policy.Type != PolicyTypePriority || cfg.Policy.Aging.BoostPerMinute != 1 {
		t.Errorf("Policy = %+v", cfg.Policy)
	}
}

func TestLoadAgent(t *testing.T) {
	cfg, err := LoadAgent(writeTemp(t, "a.yaml", validAgentYAML))
	if err != nil {
		t.Fatalf("LoadAgent: %v", err)
	}
	if cfg.Name != "mac-runner-1" {
		t.Errorf("Name = %q", cfg.Name)
	}
	if cfg.Driver.Type != DriverTypeTart || cfg.Driver.Capacity != 2 {
		t.Errorf("Driver = %+v", cfg.Driver)
	}
	if len(cfg.Orgs) != 1 || cfg.Orgs[0].Name != "paddo-tech" {
		t.Errorf("Orgs = %+v", cfg.Orgs)
	}
	if cfg.Source.Type != SourceTypePoll || cfg.Source.Interval != 10*time.Second {
		t.Errorf("Source = %+v", cfg.Source)
	}
}

// Every org in agent.yaml must be one the enrolment token covers, or its jobs
// would be silently dropped — so LoadAgent must reject an uncovered org. A
// case-different but covered org is fine (GitHub logins are case-insensitive).
func TestLoadAgent_ConfiguredOrgsMustBeCovered(t *testing.T) {
	writeCreds := func(t *testing.T, orgs ...string) string {
		path := writeTemp(t, "a.yaml", validAgentYAML) // agent.yaml org: paddo-tech
		if err := WriteCredentials(CredentialsPath(path), Credentials{
			Token: "ushr_tok", Name: "enrolled-1", Orgs: orgs,
		}); err != nil {
			t.Fatal(err)
		}
		return path
	}
	// Covered (case-insensitively), possibly alongside other enrolled orgs.
	if _, err := LoadAgent(writeCreds(t, "Paddo-Tech", "realworldadvertising")); err != nil {
		t.Fatalf("a covered org should load: %v", err)
	}
	// agent.yaml's paddo-tech is not in the token's org set.
	if _, err := LoadAgent(writeCreds(t, "other-org")); err == nil {
		t.Fatal("an org the enrolment token doesn't cover must be rejected")
	}
}

// The enrollment token is bound to one agent name server-side, so a name in
// credentials.yaml must override the one in agent.yaml — polling under the
// local name would be rejected as a token/agent mismatch.
func TestLoadAgent_CredentialsNameWins(t *testing.T) {
	path := writeTemp(t, "a.yaml", validAgentYAML) // agent.yaml sets name: mac-runner-1
	if err := WriteCredentials(CredentialsPath(path), Credentials{
		ControllerURL: "https://cp.example",
		Token:         "ushr_tok",
		Name:          "enrolled-1",
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAgent(path)
	if err != nil {
		t.Fatalf("LoadAgent: %v", err)
	}
	if cfg.Name != "enrolled-1" {
		t.Errorf("Name = %q, want the credentials name to win over agent.yaml", cfg.Name)
	}
	if cfg.Token != "ushr_tok" || cfg.ControllerURL != "https://cp.example" {
		t.Errorf("credentials not overlaid: %+v", cfg)
	}
}

func TestLoadController_VersionMismatch(t *testing.T) {
	body := strings.Replace(validControllerYAML, `version: "1"`, `version: "999"`, 1)
	_, err := LoadController(writeTemp(t, "c.yaml", body))
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestLoadController_MissingVersion(t *testing.T) {
	body := strings.Replace(validControllerYAML, `version: "1"`+"\n", "", 1)
	_, err := LoadController(writeTemp(t, "c.yaml", body))
	if err == nil {
		t.Fatal("expected error for missing version")
	}
}

func TestCheckBaseURL(t *testing.T) {
	for _, ok := range []string{"", "https://ghe.example.com", "https://ghe.example.com/", "http://10.0.0.5:8080"} {
		if err := CheckBaseURL(ok); err != nil {
			t.Errorf("CheckBaseURL(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{"ghe.example.com", "https://ghe.example.com/api/v3", "ftp://ghe.example.com", "https://github.com"} {
		if err := CheckBaseURL(bad); err == nil {
			t.Errorf("CheckBaseURL(%q) accepted", bad)
		}
	}
}

func TestLoadAgent_RejectsBadBaseURL(t *testing.T) {
	body := strings.Replace(validAgentYAML, "    priority: 100", "    priority: 100\n    base_url: ghe.example.com", 1)
	if _, err := LoadAgent(writeTemp(t, "a.yaml", body)); err == nil {
		t.Fatal("LoadAgent accepted a base_url without a scheme")
	}
}
