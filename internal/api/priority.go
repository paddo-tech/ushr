package api

import (
	"context"
	"log/slog"
	"strings"
)

// PriorityPolicy is the controller-side priority for a tenant scope (a GitHub
// org login, or "owner/repo" for a repo-level scope). A scope with a policy
// scores at that priority whatever the agent reports; a scope without one
// keeps the agent-reported value.
type PriorityPolicy interface {
	// Priorities returns the policy priority for each given scope that has
	// one, keyed by the lower-cased scope.
	Priorities(ctx context.Context, scopes []string) (map[string]int, error)
}

// StaticPriorities is the OSS policy, read from controller config.
type StaticPriorities map[string]int

// NewStaticPriorities lower-cases the keys: GitHub logins are case-insensitive
// and agents report operator-typed casing.
func NewStaticPriorities(m map[string]int) StaticPriorities {
	out := make(StaticPriorities, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = v
	}
	return out
}

func (p StaticPriorities) Priorities(_ context.Context, scopes []string) (map[string]int, error) {
	out := make(map[string]int)
	for _, s := range scopes {
		k := strings.ToLower(s)
		if v, ok := p[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

// WithPriorities sets the central priority policy. Call before serving; the
// default (nil) schedules on agent-reported priority alone.
func (s *Server) WithPriorities(p PriorityPolicy) {
	s.priorities = p
}

// applyPriorities overrides each queue's agent-reported priority with the
// policy value where one exists. A policy read failure keeps the reported
// values: a priority lookup must never stop dispatch.
func (s *Server) applyPriorities(ctx context.Context, queues []OrgQueue) {
	if s.priorities == nil || len(queues) == 0 {
		return
	}
	scopes := make([]string, len(queues))
	for i, q := range queues {
		scopes[i] = q.Org
	}
	pol, err := s.priorities.Priorities(ctx, scopes)
	if err != nil {
		slog.Warn("priority policy lookup failed, using agent-reported priority", "err", err)
		return
	}
	for i, q := range queues {
		if v, ok := pol[strings.ToLower(q.Org)]; ok {
			queues[i].Priority = v
		}
	}
}
