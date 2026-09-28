// Package webhook receives GitHub lifecycle events for jobs served by this fleet.
package webhook

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/go-github/v84/github"

	"github.com/paddo-tech/ushr/internal/domain"
	"github.com/paddo-tech/ushr/internal/ledger"
)

type Resolver interface {
	Webhook(context.Context, string) (secret, scope, name string, err error)
}

// ScopedHandler binds each app secret to its verified scope and enrolled host.
func ScopedHandler(resolver Resolver, sink func(ledger.Record)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secret, scope, name, err := resolver.Webhook(r.Context(), r.PathValue("id"))
		// A 5xx marks the delivery failed in GitHub, so it can be redelivered.
		if err != nil {
			http.Error(w, "lookup failed", http.StatusServiceUnavailable)
			return
		}
		if secret == "" {
			http.Error(w, "unknown connection", http.StatusUnauthorized)
			return
		}
		Handler([]byte(secret), func(record ledger.Record) {
			matchesScope := strings.EqualFold(scope, record.Org) || strings.EqualFold(scope, record.Org+"/"+record.Repo)
			prefix := domain.RunnerNamePrefix + name + "-"
			if matchesScope && strings.HasPrefix(record.RunnerName, prefix) && len(record.RunnerName) == len(prefix)+domain.RunnerNameSuffixLength {
				sink(record)
			}
		})(w, r)
	}
}

// Handler accepts signed start and completion events for fleet runners.
func Handler(secret []byte, sink func(ledger.Record)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
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
		if !ok || (e.GetAction() != "in_progress" && e.GetAction() != "completed") {
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
