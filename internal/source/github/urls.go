package github

import (
	"net/http"
	"strings"

	"github.com/google/go-github/v84/github"
)

// A base URL names a GitHub Enterprise Server instance (e.g.
// https://ghe.example.com); empty means github.com.

// WebURL returns the browser root of the instance at base.
func WebURL(base string) string {
	if base == "" {
		return "https://github.com"
	}
	return strings.TrimRight(base, "/")
}

// APIURL returns the REST API root of the instance at base, without a
// trailing slash. GHES serves its API under /api/v3.
func APIURL(base string) string {
	if base == "" {
		return "https://api.github.com"
	}
	return WebURL(base) + "/api/v3"
}

// AppInstallURL returns the page that installs the App with slug. GHES serves
// App pages under /github-apps, github.com under /apps.
func AppInstallURL(base, slug string) string {
	if base == "" {
		return "https://github.com/apps/" + slug + "/installations/new"
	}
	return WebURL(base) + "/github-apps/" + slug + "/installations/new"
}

// NewClient returns a go-github client over hc for the instance at base.
func NewClient(hc *http.Client, base string) (*github.Client, error) {
	c := github.NewClient(hc)
	if base == "" {
		return c, nil
	}
	return c.WithEnterpriseURLs(WebURL(base), WebURL(base))
}
