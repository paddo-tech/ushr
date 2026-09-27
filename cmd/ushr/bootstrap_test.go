package main

import (
	"path/filepath"
	"testing"

	"github.com/paddo-tech/ushr/internal/config"
)

func TestEnsureAgentConfig_GeneratesLoadableConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")

	created, err := ensureAgentConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected config to be created")
	}

	cfg, err := config.LoadAgent(path)
	if err != nil {
		t.Fatalf("generated config does not load: %v", err)
	}
	if cfg.Name == "" {
		t.Error("generated config has no name")
	}
	if len(cfg.Labels) == 0 || cfg.Labels[0] != "self-hosted" {
		t.Errorf("unexpected labels %v", cfg.Labels)
	}

	// AddOrg must work on the generated file (login/setup append to it).
	if err := config.AddOrg(path, config.Org{Name: "acme", AppID: 1, PrivateKeyPath: "/k.pem", Priority: 100}); err != nil {
		t.Fatal(err)
	}
	cfg, err = config.LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Orgs) != 1 || cfg.Orgs[0].Name != "acme" {
		t.Errorf("org not recorded: %+v", cfg.Orgs)
	}

	created, err = ensureAgentConfig(path)
	if err != nil || created {
		t.Errorf("second call should be a no-op, got created=%v err=%v", created, err)
	}
}

func TestMissingKeyScopes(t *testing.T) {
	cfg := &config.Agent{
		Orgs:  []config.Org{{Name: "Acme"}},
		Repos: []config.RepoTarget{{Owner: "me", Repo: "app"}},
	}
	got := missingKeyScopes(cfg, []string{"acme", "me/app", "other-org"})
	if len(got) != 1 || got[0] != "other-org" {
		t.Errorf("got %v, want [other-org]", got)
	}
}
