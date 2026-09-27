// Package webhook receives GitHub workflow_job events and feeds completed jobs
// run on ushr runners into the ledger.
package webhook

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/ledger"
)

// Handler validates the webhook signature with secret and, for each completed
// workflow_job run on an ushr runner, calls sink with the job's record. Non-job
// events, non-completed actions, and non-ushr runners are acknowledged and
// ignored so GitHub doesn't retry them.
func Handler(secret []byte, sink func(ledger.Record)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payload, err := github.ValidatePayload(r, secret)
		if err != nil {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		event, err := github.ParseWebHook(github.WebHookType(r), payload)
		if err != nil {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		e, ok := event.(*github.WorkflowJobEvent)
		if !ok || e.GetAction() != "completed" {
			w.WriteHeader(http.StatusOK)
			return
		}
		j := e.GetWorkflowJob()
		if strings.HasPrefix(j.GetRunnerName(), domain.RunnerNamePrefix) {
			sink(ledger.Record{
				JobID:       j.GetID(),
				Org:         e.GetRepo().GetOwner().GetLogin(),
				Repo:        e.GetRepo().GetName(),
				Workflow:    j.GetWorkflowName(),
				RunID:       j.GetRunID(),
				Conclusion:  j.GetConclusion(),
				Labels:      j.Labels,
				RunnerName:  j.GetRunnerName(),
				CreatedAt:   j.GetCreatedAt().Time,
				StartedAt:   j.GetStartedAt().Time,
				CompletedAt: j.GetCompletedAt().Time,
			})
		} else {
			slog.Debug("ignoring non-ushr job", "runner", j.GetRunnerName())
		}
		w.WriteHeader(http.StatusOK)
	}
}
