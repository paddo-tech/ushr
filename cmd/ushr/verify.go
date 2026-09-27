package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/google/go-github/v84/github"
	"golang.org/x/term"

	gh "github.com/paddo-tech/ushr/internal/source/github"
)

// appInstalled reports whether the App has an installation covering scope.
// A false with a nil-or-404 distinction matters to callers: 404 means "not
// installed", anything else means "couldn't verify".
func appInstalled(ctx context.Context, appID int64, key []byte, scope string) (bool, error) {
	if _, err := gh.InstallationID(ctx, appID, key, gh.ParseScope(scope)); err != nil {
		return false, err
	}
	return true, nil
}

func isNotInstalled(err error) bool {
	var ghErr *github.ErrorResponse
	return errors.As(err, &ghErr) && ghErr.Response != nil && ghErr.Response.StatusCode == http.StatusNotFound
}

// waitForAppInstall polls until the App is installed on scope — an uninstalled
// App can't mint runner configs, a failure that would otherwise surface only
// in the agent log. Best-effort: on timeout it prints the install URL; doctor
// re-checks later.
func waitForAppInstall(ctx context.Context, appID int64, keyPath, scope, installURL string) {
	key, err := os.ReadFile(keyPath)
	if err != nil {
		fmt.Printf("    Could not read %s to verify the install: %v\n", keyPath, err)
		return
	}
	giveUp := func() {
		fmt.Printf("    App not installed on %s yet — runners can't start until it is.\n", scope)
		fmt.Println("    Finish here, then check with `ushr doctor`:")
		fmt.Println("    " + installURL)
	}
	check := func() bool {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		ok, _ := appInstalled(checkCtx, appID, key, scope)
		return ok
	}
	// No TTY means no browser to finish the install step — check once and
	// hand off to doctor instead of polling for minutes.
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		if check() {
			fmt.Printf("==> App installed on %s ✓\n", scope)
		} else {
			giveUp()
		}
		return
	}
	fmt.Print("==> Waiting for the App to be installed (finish the browser step)")
	waitCtx, cancel := context.WithTimeout(ctx, appSetupTimeout)
	defer cancel()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if check() {
			fmt.Println(" ✓")
			return
		}
		fmt.Print(".")
		select {
		case <-waitCtx.Done():
			fmt.Println()
			giveUp()
			return
		case <-tick.C:
		}
	}
}
