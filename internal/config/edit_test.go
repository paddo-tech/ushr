package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAddOrg_PreservesCommentsAndAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	src := `version: "1"
controller_url: http://127.0.0.1:7080
token: ""
name: box
# keep this comment
driver:
  type: docker
  image: ghcr.io/actions/actions-runner:latest
  capacity: 4
source:
  type: poll
  interval: 30s
`
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := AddOrg(path, Org{Name: "acme", AppID: 42, PrivateKeyPath: "/k.pem", Priority: 100}); err != nil {
		t.Fatal(err)
	}
	if err := AddRepo(path, RepoTarget{Owner: "me", Repo: "app", AppID: 7, PrivateKeyPath: "/r.pem", Priority: 50}); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "# keep this comment") {
		t.Errorf("comment lost:\n%s", out)
	}

	cfg, err := LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Orgs) != 1 || cfg.Orgs[0].Name != "acme" || cfg.Orgs[0].AppID != 42 || cfg.Orgs[0].Priority != 100 {
		t.Errorf("org not recorded: %+v", cfg.Orgs)
	}
	if len(cfg.Repos) != 1 || cfg.Repos[0].Scope() != "me/app" || cfg.Repos[0].AppID != 7 {
		t.Errorf("repo not recorded: %+v", cfg.Repos)
	}
}

func TestAddOrg_EmptyFlowList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	src := "version: \"1\"\norgs: []\n"
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := AddOrg(path, Org{Name: "acme", AppID: 1, PrivateKeyPath: "/k.pem"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Orgs) != 1 || cfg.Orgs[0].Name != "acme" {
		t.Errorf("org not recorded: %+v", cfg.Orgs)
	}
}
