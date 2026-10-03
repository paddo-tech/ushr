// Package metrics defines the Prometheus series the controller and the agent
// export. Each owns a private registry rather than the global default, so tests
// and multiple servers in one process never collide on registration.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Controller holds the control plane's scheduling series.
type Controller struct {
	Registry        *prometheus.Registry
	Offers          prometheus.Counter
	Claims          prometheus.Counter
	DispatchLatency prometheus.Histogram
	AgentsLost      prometheus.Counter
}

// NewController registers the controller series. activeAgents and queuedJobs
// are read at scrape time from the server's own liveness state.
func NewController(activeAgents, queuedJobs func() float64) *Controller {
	reg := newRegistry()
	m := &Controller{
		Registry: reg,
		Offers: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ushr_controller_offers_total",
			Help: "Dispatch offers made to agents.",
		}),
		Claims: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ushr_controller_claims_total",
			Help: "Offers claimed by agents.",
		}),
		DispatchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ushr_controller_dispatch_latency_seconds",
			Help:    "Time from GitHub queueing a job to the controller offering it, from the agent-reported wait.",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
		AgentsLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ushr_controller_agents_lost_total",
			Help: "Agents dropped after missing polls for the liveness window.",
		}),
	}
	reg.MustRegister(m.Offers, m.Claims, m.DispatchLatency, m.AgentsLost,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "ushr_controller_agents_active",
			Help: "Agents heard from within the liveness window.",
		}, activeAgents),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "ushr_controller_queued_jobs_seen",
			Help: "Distinct queued jobs in the latest poll of each active agent.",
		}, queuedJobs),
	)
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *Controller) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// Agent holds the runner host's series. The driver label is fixed per process,
// so the per-driver series are curried at construction.
type Agent struct {
	Registry          *prometheus.Registry
	SlotsBusy         prometheus.Gauge
	SlotsTotal        prometheus.Gauge
	ProvisionSeconds  prometheus.Observer
	ProvisionFailures prometheus.Counter
	DiskBlocked       prometheus.Gauge
	GitHubAPIErrors   *prometheus.CounterVec
	BreakerOpen       *prometheus.GaugeVec
	PollErrors        prometheus.Counter
}

// NewAgent registers the agent series for the named driver.
func NewAgent(driver string) *Agent {
	reg := newRegistry()
	provision := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ushr_agent_provision_duration_seconds",
		Help:    "Time for the driver to provision a runner, successful provisions only.",
		Buckets: []float64{1, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"driver"})
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ushr_agent_provision_failures_total",
		Help: "Driver provision attempts that returned an error.",
	}, []string{"driver"})
	m := &Agent{
		Registry: reg,
		SlotsBusy: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ushr_agent_slots_busy",
			Help: "Runner slots reserved or running a job.",
		}),
		SlotsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ushr_agent_slots_total",
			Help: "Runner slots the driver offers.",
		}),
		ProvisionSeconds:  provision.WithLabelValues(driver),
		ProvisionFailures: failures.WithLabelValues(driver),
		DiskBlocked: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ushr_agent_disk_blocked",
			Help: "1 while the disk gate or a reclaim drain refuses dispatches, else 0.",
		}),
		GitHubAPIErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ushr_agent_github_api_errors_total",
			Help: "GitHub API calls by the poll source that returned an error.",
		}, []string{"org"}),
		BreakerOpen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ushr_agent_github_breaker_open",
			Help: "1 while the poll source pauses an org for auth or rate-limit trouble, else 0.",
		}, []string{"org"}),
		PollErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ushr_agent_poll_errors_total",
			Help: "Control-plane polls that failed.",
		}),
	}
	reg.MustRegister(m.SlotsBusy, m.SlotsTotal, provision, failures, m.DiskBlocked,
		m.GitHubAPIErrors, m.BreakerOpen, m.PollErrors)
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *Agent) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}
