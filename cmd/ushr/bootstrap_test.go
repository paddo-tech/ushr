package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/enroll"
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

func TestHostedResumeReadsEnrollmentIdentity(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer host-token" {
			t.Error("missing host authentication")
		}
		_, _ = w.Write([]byte(`{"agent_name":"test-host","orgs":["acme"]}`))
	})
	transport := http.DefaultTransport
	http.DefaultTransport = setupTransport(func(r *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response.Result(), nil
	})
	t.Cleanup(func() { http.DefaultTransport = transport })
	var session sessionResult
	status, err := setupRequest(context.Background(), http.MethodGet, "https://ushr.example", "host-token", nil, &session)
	if err != nil || status != http.StatusOK || session.AgentName != "test-host" || len(session.Orgs) != 1 {
		t.Fatalf("identity was not restored: %#v, %d, %v", session, status, err)
	}
}

func TestHostedSetupRetainsKeyWhenRegistrationFails(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "agent.yaml")
	state := hostedSetupState{ID: strings.Repeat("a", 64), Verifier: strings.Repeat("b", 43), AppID: 1, Slug: "example", Secret: "private-webhook-secret", Key: "saved-private-key"}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "setup-"+enroll.HashToken("acme")[:16]+".json")
	if err := os.WriteFile(statePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/runner-setup/"+state.ID {
			t.Errorf("retry created another app: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	transport := http.DefaultTransport
	http.DefaultTransport = setupTransport(func(r *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response.Result(), nil
	})
	t.Cleanup(func() { http.DefaultTransport = transport })
	if err := hostedAppSetup(context.Background(), "acme", filepath.Join(dir, "keys"), configPath, "https://ushr.example", "token"); err == nil {
		t.Fatal("expected failed registration")
	}
	saved, err := os.ReadFile(statePath)
	if err != nil || string(saved) != string(data) {
		t.Fatal("saved setup was lost")
	}
	keyPath := filepath.Join(dir, "keys", "ushr-"+state.ID+".pem")
	key, err := os.ReadFile(keyPath)
	if err != nil || string(key) != state.Key {
		t.Fatal("private key was lost")
	}
	info, err := os.Stat(keyPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("private key permissions are not 0600")
	}
}

type setupTransport func(*http.Request) (*http.Response, error)

func (f setupTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
