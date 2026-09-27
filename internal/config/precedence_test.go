package config

import (
	"os"
	"path/filepath"
	"testing"
)

// A hosted agent's identity comes from credentials.yaml (written by login);
// agent.yaml ships loopback defaults. If this precedence ever flips, enrolled
// agents silently point at 127.0.0.1 instead of the control plane.
func TestCredentialsOverrideAgentConfig(t *testing.T) {
	dir := t.TempDir()
	agentPath := filepath.Join(dir, "agent.yaml")
	src := `version: "1"
controller_url: http://127.0.0.1:7080
token: ""
name: local-name
driver:
  type: docker
`
	if err := os.WriteFile(agentPath, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteCredentials(CredentialsPath(agentPath), Credentials{
		ControllerURL: "https://cp.example.com",
		Token:         "tok",
		Name:          "enrolled-name",
	}); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadAgent(agentPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControllerURL != "https://cp.example.com" {
		t.Errorf("controller_url = %q, want credentials value", cfg.ControllerURL)
	}
	if cfg.Token != "tok" {
		t.Errorf("token = %q, want credentials value", cfg.Token)
	}
	if cfg.Name != "enrolled-name" {
		t.Errorf("name = %q, want credentials value", cfg.Name)
	}
}
