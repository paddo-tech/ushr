package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func families(t *testing.T, reg *prometheus.Registry) map[string]bool {
	t.Helper()
	got, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(got))
	for _, f := range got {
		out[f.GetName()] = true
	}
	return out
}

func TestControllerRegistersSeries(t *testing.T) {
	m := NewController(func() float64 { return 2 }, func() float64 { return 3 })
	m.DispatchLatency.Observe(4)
	got := families(t, m.Registry)
	for _, name := range []string{
		"ushr_controller_offers_total",
		"ushr_controller_claims_total",
		"ushr_controller_dispatch_latency_seconds",
		"ushr_controller_agents_lost_total",
		"ushr_controller_agents_active",
		"ushr_controller_queued_jobs_seen",
		"go_goroutines",
	} {
		if !got[name] {
			t.Errorf("missing %s", name)
		}
	}
}

func TestAgentRegistersSeries(t *testing.T) {
	m := NewAgent("tart")
	m.GitHubAPIErrors.WithLabelValues("acme")
	m.BreakerOpen.WithLabelValues("acme")
	got := families(t, m.Registry)
	for _, name := range []string{
		"ushr_agent_slots_busy",
		"ushr_agent_slots_total",
		"ushr_agent_provision_duration_seconds",
		"ushr_agent_provision_failures_total",
		"ushr_agent_disk_blocked",
		"ushr_agent_github_api_errors_total",
		"ushr_agent_github_breaker_open",
		"ushr_agent_poll_errors_total",
	} {
		if !got[name] {
			t.Errorf("missing %s", name)
		}
	}
}
