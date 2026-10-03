package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/enroll"
)

type hostedSetupState struct {
	ID       string `json:"id"`
	Verifier string `json:"verifier"`
	AppID    int64  `json:"app_id"`
	Slug     string `json:"slug"`
	Secret   string `json:"secret"`
	Key      string `json:"key"`
}

func setupRequest(ctx context.Context, method, url, token string, input, output any) (int, error) {
	var body io.Reader
	if input != nil {
		payload, err := json.Marshal(input)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return res.StatusCode, fmt.Errorf("setup request failed (%d)", res.StatusCode)
	}
	if output != nil && res.StatusCode != http.StatusNoContent {
		err = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(output)
	}
	return res.StatusCode, err
}

// hostedAppSetup resumes local progress and exchanges the GitHub code only on the host.
func hostedAppSetup(ctx context.Context, scope, keyDir, configPath, web, token string) error {
	statePath := filepath.Join(filepath.Dir(configPath), "setup-"+enroll.HashToken(strings.ToLower(scope))[:16]+".json")
	var state hostedSetupState
	b, err := os.ReadFile(statePath)
	if err == nil {
		if err := json.Unmarshal(b, &state); err != nil {
			return fmt.Errorf("read saved setup: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	save := func() error {
		b, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := os.WriteFile(statePath+".tmp", b, 0o600); err != nil {
			return err
		}
		return os.Rename(statePath+".tmp", statePath)
	}
	if state.ID == "" {
		state.ID, err = randomHex(32)
		if err != nil {
			return err
		}
		state.Verifier, err = randToken()
		if err != nil {
			return err
		}
		if err := save(); err != nil {
			return err
		}
	}
	endpoint := strings.TrimRight(web, "/") + "/api/runner-setup"
	if state.AppID == 0 {
		status, err := setupRequest(ctx, http.MethodPost, endpoint, token, map[string]string{
			"id": state.ID, "challenge": enroll.Challenge(state.Verifier), "scope": scope,
		}, nil)
		if status == http.StatusGone {
			fmt.Println("==> GitHub approval expired. Starting a fresh approval.")
			state.ID, err = randomHex(32)
			if err != nil {
				return err
			}
			state.Verifier, err = randToken()
			if err != nil {
				return err
			}
			if err := save(); err != nil {
				return err
			}
			_, err = setupRequest(ctx, http.MethodPost, endpoint, token, map[string]string{
				"id": state.ID, "challenge": enroll.Challenge(state.Verifier), "scope": scope,
			}, nil)
		}
		if err != nil {
			return fmt.Errorf("start GitHub setup: %w", err)
		}
		url := strings.TrimRight(web, "/") + "/setup/github?session=" + state.ID
		fmt.Println("==> Connect GitHub from any device:")
		fmt.Println("    " + url)
		tryOpen(url)
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		var result struct {
			Code string `json:"code"`
		}
		for result.Code == "" {
			status, err := setupRequest(waitCtx, http.MethodPost, endpoint+"/"+state.ID, token, map[string]string{"action": "fetch", "verifier": state.Verifier}, &result)
			// Network errors and server failures retry until the wait expires; a client error is final.
			if err != nil && status >= 400 && status < 500 {
				return err
			}
			if result.Code != "" {
				break
			}
			select {
			case <-waitCtx.Done():
				return fmt.Errorf("GitHub setup paused: run ushr login to resume")
			case <-tick.C:
			}
		}
		app, err := exchangeCode(ctx, "", result.Code)
		if err != nil {
			// The one-time code may be spent even when the response is lost, so the next login starts over.
			state = hostedSetupState{}
			if saveErr := save(); saveErr != nil {
				return saveErr
			}
			return fmt.Errorf("exchange GitHub code: %w; run ushr login to retry", err)
		}
		state.AppID, state.Slug, state.Secret, state.Key = app.GetID(), app.GetSlug(), app.GetWebhookSecret(), app.GetPEM()
		if err := save(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return err
	}
	keyPath := filepath.Join(keyDir, "ushr-"+state.ID+".pem")
	if err := os.WriteFile(keyPath, []byte(state.Key), 0o600); err != nil {
		return err
	}
	_, err = setupRequest(ctx, http.MethodPost, endpoint+"/"+state.ID, token, map[string]any{
		"action": "finish", "verifier": state.Verifier, "appId": state.AppID, "slug": state.Slug, "secret": state.Secret,
	}, nil)
	if err != nil {
		return fmt.Errorf("connect job results: %w; run ushr login to resume", err)
	}
	installURL := "https://github.com/apps/" + state.Slug + "/installations/new"
	fmt.Println("==> Install your runner app on GitHub:")
	fmt.Println("    " + installURL)
	if err := waitForAppInstall(ctx, state.AppID, keyPath, scope, "", installURL); err != nil {
		return err
	}
	owner, repo, isRepo := domain.SplitRepoScope(scope)
	if isRepo {
		err = config.AddRepo(configPath, config.RepoTarget{Owner: owner, Repo: repo, AppID: state.AppID, PrivateKeyPath: keyPath, Priority: 100})
	} else {
		err = config.AddOrg(configPath, config.Org{Name: scope, AppID: state.AppID, PrivateKeyPath: keyPath, Priority: 100})
	}
	if err != nil {
		return err
	}
	// The final config and key are durable before the temporary secret copy is removed.
	return os.Remove(statePath)
}
