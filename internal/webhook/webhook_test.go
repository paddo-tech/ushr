package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/paddo-tech/ushr/internal/ledger"
)

const jobPayload = `{
  "action": "completed",
  "workflow_job": {
    "id": 42,
    "runner_name": "ushr-m4-42",
    "status": "completed",
    "labels": ["macos"],
    "started_at": "2024-01-01T00:00:00Z",
    "completed_at": "2024-01-01T00:05:00Z"
  },
  "repository": {"name": "api", "owner": {"login": "acme"}}
}`

func post(t *testing.T, secret, payload string, sign bool) (*httptest.ResponseRecorder, []ledger.Record) {
	t.Helper()
	var got []ledger.Record
	h := Handler([]byte(secret), func(r ledger.Record) { got = append(got, r) })

	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "workflow_job")
	if sign {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(payload))
		req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec, got
}

func TestCompletedUshrJobRecorded(t *testing.T) {
	rec, got := post(t, "s3cr3t", jobPayload, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	r := got[0]
	if r.JobID != 42 || r.Org != "acme" || r.Repo != "api" || r.RunnerName != "ushr-m4-42" {
		t.Errorf("unexpected record %+v", r)
	}
	if d := r.CompletedAt.Sub(r.StartedAt).Minutes(); d != 5 {
		t.Errorf("duration %v min, want 5", d)
	}
}

func TestBadSignatureRejected(t *testing.T) {
	rec, got := post(t, "s3cr3t", jobPayload, false)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status %d, want 401", rec.Code)
	}
	if len(got) != 0 {
		t.Errorf("sink called on bad signature")
	}
}

func TestNonUshrRunnerIgnored(t *testing.T) {
	payload := strings.Replace(jobPayload, "ushr-m4-42", "GitHub Actions 3", 1)
	rec, got := post(t, "s3cr3t", payload, true)
	if rec.Code != http.StatusOK {
		t.Errorf("status %d, want 200", rec.Code)
	}
	if len(got) != 0 {
		t.Errorf("non-ushr runner recorded")
	}
}

func TestNonCompletedActionIgnored(t *testing.T) {
	payload := strings.Replace(jobPayload, `"action": "completed"`, `"action": "queued"`, 1)
	_, got := post(t, "s3cr3t", payload, true)
	if len(got) != 0 {
		t.Errorf("non-completed action recorded")
	}
}
