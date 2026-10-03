package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestURLDerivation(t *testing.T) {
	cases := []struct {
		base, web, api, install string
	}{
		{"", "https://github.com", "https://api.github.com", "https://github.com/apps/ushr-x/installations/new"},
		{"https://ghe.example.com", "https://ghe.example.com", "https://ghe.example.com/api/v3", "https://ghe.example.com/github-apps/ushr-x/installations/new"},
		{"https://ghe.example.com/", "https://ghe.example.com", "https://ghe.example.com/api/v3", "https://ghe.example.com/github-apps/ushr-x/installations/new"},
	}
	for _, c := range cases {
		if got := WebURL(c.base); got != c.web {
			t.Errorf("WebURL(%q) = %q, want %q", c.base, got, c.web)
		}
		if got := APIURL(c.base); got != c.api {
			t.Errorf("APIURL(%q) = %q, want %q", c.base, got, c.api)
		}
		if got := AppInstallURL(c.base, "ushr-x"); got != c.install {
			t.Errorf("AppInstallURL(%q) = %q, want %q", c.base, got, c.install)
		}
	}
}

func TestNewClientEnterpriseURLs(t *testing.T) {
	c, err := NewClient(nil, "https://ghe.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got := c.BaseURL.String(); got != "https://ghe.example.com/api/v3/" {
		t.Errorf("BaseURL = %q", got)
	}
	if got := c.UploadURL.String(); got != "https://ghe.example.com/api/uploads/" {
		t.Errorf("UploadURL = %q", got)
	}
	dotcom, err := NewClient(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := dotcom.BaseURL.String(); got != "https://api.github.com/" {
		t.Errorf("default BaseURL = %q", got)
	}
}

// The App JWT lookup must go to the GHES API root, not api.github.com.
func TestInstallationIDUsesBaseURL(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = w.Write([]byte(`{"id":42}`))
	}))
	defer srv.Close()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	key := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})

	id, err := InstallationID(context.Background(), 1, key, AppAuth{Org: "acme", BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 || path != "/api/v3/orgs/acme/installation" {
		t.Fatalf("id %d via %q, want 42 via /api/v3/orgs/acme/installation", id, path)
	}
}
