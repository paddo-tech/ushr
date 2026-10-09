package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/metrics"
	gh "github.com/paddo-tech/ushr/internal/source/github"
)

// The poll source must hand the agent its horizon, or the agent falls back to
// the 26 hour TTL and a job a runner took stays offered for a day.
func TestNewSourceWiresPollFreshness(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":42}`))
	}))
	defer srv.Close()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "app.pem")
	key := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Agent{
		Orgs:   []config.Org{{Name: "acme", AppID: 1, PrivateKeyPath: keyPath, BaseURL: srv.URL, Repos: []string{"r"}}},
		Source: config.SourceConfig{Type: config.SourceTypePoll, Interval: time.Hour},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, _, fresh, err := newSource(ctx, cfg, metrics.NewAgent(""))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fresh.(*gh.Poller); !ok {
		t.Fatalf("poll source freshness = %T, want *github.Poller", fresh)
	}
}
