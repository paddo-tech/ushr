package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/config"
	gh "github.com/paddo-tech/ushr/internal/source/github"
)

// appInstalled reports whether the App has an installation covering scope.
// A false with a nil-or-404 distinction matters to callers: 404 means "not
// installed", anything else means "couldn't verify".
func appInstalled(ctx context.Context, appID int64, key []byte, scope, baseURL string) (bool, error) {
	a := gh.ParseScope(scope)
	a.BaseURL = baseURL
	if _, err := gh.InstallationID(ctx, appID, key, a); err != nil {
		return false, err
	}
	return true, nil
}

func githubStatus(err error) int {
	var ghErr *github.ErrorResponse
	if errors.As(err, &ghErr) && ghErr.Response != nil {
		return ghErr.Response.StatusCode
	}
	return 0
}

func isNotInstalled(err error) bool {
	return githubStatus(err) == http.StatusNotFound
}

// The agent must not start for a scope its App cannot serve.
func checkInstalled(ctx context.Context, cfg *config.Agent, scope string) error {
	appID, keyPath, baseURL, _ := configuredTarget(cfg, scope)
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := appInstalled(ctx, appID, key, scope, baseURL); err != nil {
		return fmt.Errorf("GitHub app is not ready for %s: %w", scope, err)
	}
	return nil
}

// An uninstalled app must not advance setup to agent startup.
func waitForAppInstall(ctx context.Context, appID int64, keyPath, scope, baseURL, installURL string) error {
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	fmt.Println("==> Waiting for GitHub installation. Continue in your browser.")
	waitCtx, cancel := context.WithTimeout(ctx, appSetupTimeout)
	defer cancel()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		checkCtx, checkCancel := context.WithTimeout(waitCtx, 10*time.Second)
		ok, err := appInstalled(checkCtx, appID, key, scope, baseURL)
		checkCancel()
		if ok {
			fmt.Println("==> GitHub app installed on", scope)
			return nil
		}
		// Network errors, rate limits, and GitHub failures retry; only a definite rejection stops setup.
		status := githubStatus(err)
		if status >= 400 && status < 500 && status != http.StatusNotFound && status != http.StatusTooManyRequests {
			return fmt.Errorf("check GitHub installation: %w", err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("GitHub installation is unfinished: %s; run ushr login to resume", installURL)
		case <-tick.C:
		}
	}
}
