package jit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/google/go-github/v84/github"
)

// TestMint_ReapsStaleRunnerOn409 covers the orphan-runner path: a killed
// runner leaves its JIT registration behind, so the first mint 409s; mint must
// delete that stale runner and retry, returning the config from the retry.
func TestMint_ReapsStaleRunnerOn409(t *testing.T) {
	const org = "test-org"
	const name = "ushr-agent-99"

	var genCalls, deletes int
	mux := http.NewServeMux()
	mux.HandleFunc("/orgs/"+org+"/actions/runners/generate-jitconfig", func(w http.ResponseWriter, r *http.Request) {
		genCalls++
		if genCalls == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"A runner with the name already exists."}`))
			return
		}
		_, _ = w.Write([]byte(`{"encoded_jit_config":"OK-JIT"}`))
	})
	mux.HandleFunc("/orgs/"+org+"/actions/runners", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":42,"name":"` + name + `","status":"offline"}]}`))
	})
	mux.HandleFunc("/orgs/"+org+"/actions/runners/42", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes++
			w.WriteHeader(http.StatusNoContent)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := github.NewClient(nil)
	c.BaseURL, _ = url.Parse(srv.URL + "/")

	jit, err := mintOrg(context.Background(), c, org, name, 1, []string{"self-hosted"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if jit != "OK-JIT" {
		t.Errorf("jit = %q, want OK-JIT", jit)
	}
	if genCalls != 2 {
		t.Errorf("generate-jitconfig called %d times, want 2 (409 then retry)", genCalls)
	}
	if deletes != 1 {
		t.Errorf("stale runner deleted %d times, want 1", deletes)
	}
}

// TestMintRepo_ReapsStaleRunnerOn409 is TestMint_ReapsStaleRunnerOn409 for the
// repo-level path: it must hit the /repos/{owner}/{repo}/... endpoints and reap
// the stale repo runner on 409 before retrying.
func TestMintRepo_ReapsStaleRunnerOn409(t *testing.T) {
	const owner, repo = "test-owner", "test-repo"
	const name = "ushr-agent-99"
	base := "/repos/" + owner + "/" + repo + "/actions/runners"

	var genCalls, deletes int
	mux := http.NewServeMux()
	mux.HandleFunc(base+"/generate-jitconfig", func(w http.ResponseWriter, r *http.Request) {
		genCalls++
		if genCalls == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"A runner with the name already exists."}`))
			return
		}
		_, _ = w.Write([]byte(`{"encoded_jit_config":"OK-REPO-JIT"}`))
	})
	mux.HandleFunc(base, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":42,"name":"` + name + `","status":"offline"}]}`))
	})
	mux.HandleFunc(base+"/42", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes++
			w.WriteHeader(http.StatusNoContent)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := github.NewClient(nil)
	c.BaseURL, _ = url.Parse(srv.URL + "/")

	jit, err := mintRepo(context.Background(), c, owner, repo, name, 1, []string{"self-hosted"})
	if err != nil {
		t.Fatalf("mintRepo: %v", err)
	}
	if jit != "OK-REPO-JIT" {
		t.Errorf("jit = %q, want OK-REPO-JIT", jit)
	}
	if genCalls != 2 {
		t.Errorf("generate-jitconfig called %d times, want 2 (409 then retry)", genCalls)
	}
	if deletes != 1 {
		t.Errorf("stale runner deleted %d times, want 1", deletes)
	}
}

// TestMint_NonConflictErrorNoReap ensures a non-409 failure surfaces directly
// without listing or deleting any runner.
func TestMint_NonConflictErrorNoReap(t *testing.T) {
	const org = "test-org"

	var listed, genCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/orgs/"+org+"/actions/runners/generate-jitconfig", func(w http.ResponseWriter, r *http.Request) {
		genCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/orgs/"+org+"/actions/runners", func(w http.ResponseWriter, r *http.Request) {
		listed++
		_, _ = w.Write([]byte(`{"total_count":0,"runners":[]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := github.NewClient(nil)
	c.BaseURL, _ = url.Parse(srv.URL + "/")

	if _, err := mintOrg(context.Background(), c, org, "ushr-agent-1", 1, nil); err == nil {
		t.Fatal("expected error from 500, got nil")
	}
	if genCalls != 1 {
		t.Errorf("generate called %d times, want 1 (no retry on 500)", genCalls)
	}
	if listed != 0 {
		t.Errorf("listed runners %d times, want 0 (no reap on non-409)", listed)
	}
}
