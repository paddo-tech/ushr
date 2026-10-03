package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// SchemaVersion is the only config schema accepted by this binary.
// Bump when making backwards-incompatible config changes.
const SchemaVersion = "1"

// DefaultRunnerGroupID is GitHub's "Default" runner group; used when
// Org.RunnerGroupID is unset (zero).
const DefaultRunnerGroupID int64 = 1

type PolicyType string

const PolicyTypePriority PolicyType = "priority"

type SourceType string

const (
	SourceTypePoll     SourceType = "poll"
	SourceTypeScaleSet SourceType = "scaleset"
)

type DriverType string

const (
	DriverTypeTart   DriverType = "tart"
	DriverTypeLume   DriverType = "lume"
	DriverTypeDocker DriverType = "docker"
	DriverTypeK8s    DriverType = "k8s"
)

type Controller struct {
	Version string `yaml:"version"`
	Listen  string `yaml:"listen"`
	Token   string `yaml:"token"`
	// Orgs is read only by the local `ushr cost` report, which queries GitHub
	// directly; the keyless control plane schedules on agent-reported metadata
	// and needs no org config of its own.
	Orgs    []Org         `yaml:"orgs"`
	Policy  Policy        `yaml:"policy"`
	Webhook WebhookConfig `yaml:"webhook"`
	// DispatchDSN, when set, backs the dispatch ledger with Postgres (Neon) for
	// the hosted control plane instead of the local JSONL WAL. Empty = JSONL.
	DispatchDSN string `yaml:"dispatch_dsn"`
	// DispatchLedgerPath overrides where the controller persists in-flight
	// dispatch state across restarts. Empty means dispatch.DefaultPath().
	DispatchLedgerPath string `yaml:"dispatch_ledger_path"`
}

// WebhookConfig enables the workflow_job receiver that feeds the job ledger.
// Empty Secret disables it. LedgerPath defaults to ledger.DefaultPath().
type WebhookConfig struct {
	Secret     string `yaml:"secret"`
	LedgerPath string `yaml:"ledger_path"`
}

type Org struct {
	Name           string `yaml:"name"`
	AppID          int64  `yaml:"app_id"`
	PrivateKeyPath string `yaml:"private_key_path"`
	Priority       int    `yaml:"priority,omitempty"`
	// BaseURL is the GitHub Enterprise Server root (e.g.
	// https://ghe.example.com). Empty means github.com.
	BaseURL string `yaml:"base_url,omitempty"`
	// RunnerGroupID is the GitHub runner group to register JIT runners into.
	// Falls back to DefaultRunnerGroupID if zero.
	RunnerGroupID int64 `yaml:"runner_group_id,omitempty"`
	// Repos optionally restricts polling to these repo names within the org.
	// Empty means poll every repo the App installation can access — which blows
	// the API rate limit on a large installation. Set it to the handful that
	// actually use the runner; this also skips the list-installation-repos call.
	// Poll source only.
	Repos []string `yaml:"repos,omitempty"`
	// ScaleSets lists the runner scale sets to serve in this org. Scaleset
	// source only; repo scoping is enforced GitHub-side by the runner group.
	ScaleSets []ScaleSet `yaml:"scale_sets,omitempty"`
}

// RepoTarget serves a single repository's runners (the personal-account path:
// user accounts have no org-level runner pool, so runners are per-repo). The
// App must have repository "Administration: write" on the repo.
type RepoTarget struct {
	Owner          string `yaml:"owner"`
	Repo           string `yaml:"repo"`
	AppID          int64  `yaml:"app_id"`
	PrivateKeyPath string `yaml:"private_key_path"`
	Priority       int    `yaml:"priority,omitempty"`
	// BaseURL is the GitHub Enterprise Server root; empty means github.com.
	BaseURL string `yaml:"base_url,omitempty"`
}

// Scope is the tenant scope ("owner/repo") this target serves; matches the
// enrollment-token grant and domain.Job.Org.
func (r RepoTarget) Scope() string { return r.Owner + "/" + r.Repo }

// ScaleSet is one runner scale set. Its name doubles as the runs-on label
// workflows target.
type ScaleSet struct {
	Name string `yaml:"name"`
	// MaxRunners caps outstanding runners for this set. Size it to the
	// capacity of the agent(s) labelled to serve it: it is what the controller
	// reports to GitHub as the set's max capacity when polling for messages.
	MaxRunners int `yaml:"max_runners"`
}

type Policy struct {
	Type  PolicyType `yaml:"type"`
	Aging Aging      `yaml:"aging"`
}

type Aging struct {
	BoostPerMinute int `yaml:"boost_per_minute"`
}

type SourceConfig struct {
	Type     SourceType    `yaml:"type"`
	Interval time.Duration `yaml:"interval"`
}

type Agent struct {
	Version       string       `yaml:"version"`
	ControllerURL string       `yaml:"controller_url"`
	Token         string       `yaml:"token"`
	Name          string       `yaml:"name"`
	Labels        []string     `yaml:"labels"`
	Driver        DriverConfig `yaml:"driver"`
	// The agent holds the GitHub App keys, polls GitHub, and mints JIT configs,
	// so the orgs it serves and its poll source are configured here.
	Orgs []Org `yaml:"orgs"`
	// Repos serves individual repositories (personal-account / per-repo runners).
	Repos  []RepoTarget `yaml:"repos"`
	Source SourceConfig `yaml:"source"`
}

type DriverConfig struct {
	Type     DriverType `yaml:"type"`
	Image    string     `yaml:"image"`
	Capacity int        `yaml:"capacity"`

	// Runtime pins the container CLI for the docker driver ("docker" or
	// "podman"). Empty autodetects (docker, then podman). Ignored by other drivers.
	Runtime string `yaml:"runtime"`

	// MinFreeGB is the free-space floor on the driver's image store (tart's
	// home, the container data root). Under it the agent stops accepting
	// dispatches, so the work stays schedulable on a host that can run it rather
	// than failing on a full disk here. Zero takes the driver's default;
	// negative disables the gate but leaves reclaim working to the same mark.
	MinFreeGB int `yaml:"min_free_gb"`

	// ExclusiveDaemon declares that nothing but ushr's runners uses this host's
	// container daemon. Jobs mount the host docker socket, so what they leave
	// behind carries none of our labels and can only be collected by pruning
	// unowned objects — safe exactly when there is no other user. Docker driver
	// only; off by default.
	ExclusiveDaemon bool `yaml:"exclusive_daemon"`

	// Docker-driver-specific, ignored by other drivers. The load-bearing ones:
	// Volumes mounts the host docker socket for buildx, GroupAdd carries the
	// host docker GID so the runner user can use it, and Memory (e.g. "4g")
	// caps each runner container so a heavy build OOM-kills its own job rather
	// than the host.
	Network    string   `yaml:"network"`
	Volumes    []string `yaml:"volumes"`
	ExtraHosts []string `yaml:"extra_hosts"`
	GroupAdd   []string `yaml:"group_add"`
	Memory     string   `yaml:"memory"`
	Entrypoint string   `yaml:"entrypoint"`

	// BuildCache gives each repo its own persistent buildx builder, so image
	// layers and RUN --mount=type=cache survive between that repo's jobs in a
	// cache store separate from other repos'. This is cache isolation, not a
	// trust boundary (jobs share the host docker socket), and within a repo a
	// fork-PR build shares the warm cache with protected builds — keep the usual
	// "no untrusted PRs on self-hosted runners" rule. Docker driver only, on by
	// default (unset = enabled; needs the host buildx plugin, and each dispatch
	// degrades to uncached when buildx is unavailable). BuildkitImage overrides
	// the buildkit image; BuildCacheGB caps each repo's cache before a prune
	// (default 20). StateDir holds the per-repo buildx configs (default
	// ~/.local/share/ushr).
	BuildCache        *bool  `yaml:"build_cache"`
	BuildkitImage     string `yaml:"buildkit_image"`
	BuildCacheGB      int    `yaml:"build_cache_gb"`
	BuildCacheIdleHrs int    `yaml:"build_cache_idle_hours"` // reap a repo's builder after this idle (default 168 = 7d)
	StateDir          string `yaml:"state_dir"`
}

func LoadController(path string) (*Controller, error) {
	var c Controller
	if err := loadYAML(path, &c); err != nil {
		return nil, err
	}
	// Env overrides for hosted deploys: Fly injects secrets as env, not files.
	if v := os.Getenv("USHR_LISTEN"); v != "" {
		c.Listen = v
	}
	if v := os.Getenv("USHR_TOKEN"); v != "" {
		c.Token = v
	}
	if v := os.Getenv("USHR_DISPATCH_DSN"); v != "" {
		c.DispatchDSN = v
	}
	if v := os.Getenv("USHR_WEBHOOK_SECRET"); v != "" {
		c.Webhook.Secret = v
	}
	if err := checkVersion(c.Version); err != nil {
		return nil, err
	}
	return &c, nil
}

func LoadAgent(path string) (*Agent, error) {
	var a Agent
	if err := loadYAML(path, &a); err != nil {
		return nil, err
	}
	// Overlay credentials written by `ushr login` (kept out of agent.yaml so the
	// login-managed secret and the hand-edited driver/orgs config stay separate).
	c, err := loadCredentials(CredentialsPath(path))
	if err != nil {
		return nil, fmt.Errorf("load credentials: %w", err)
	}
	if c.ControllerURL != "" {
		a.ControllerURL = c.ControllerURL
	}
	if c.Token != "" {
		a.Token = c.Token
	}
	// The enrollment token is bound to one agent name server-side, so the
	// login-managed name must win over a hand-edited agent.yaml: keeping the
	// local name would poll under an identity the token doesn't authorize.
	if c.Name != "" {
		a.Name = c.Name
	}
	// An enrollment token covers a fixed set of orgs, so every org configured
	// here must be one the token serves — otherwise its jobs would be silently
	// dropped by the control plane. Fail loud instead. (A static-token/OSS agent
	// has no enrolled orgs and may serve any.)
	if len(c.Orgs) > 0 {
		for _, o := range a.Orgs {
			if !orgInSet(c.Orgs, o.Name) {
				return nil, fmt.Errorf("agent.yaml configures org %q, which this enrollment token does not cover (enrolled for [%s])", o.Name, strings.Join(c.Orgs, ", "))
			}
		}
		for _, r := range a.Repos {
			if !orgInSet(c.Orgs, r.Scope()) {
				return nil, fmt.Errorf("agent.yaml configures repo %q, which this enrollment token does not cover (enrolled for [%s])", r.Scope(), strings.Join(c.Orgs, ", "))
			}
		}
	}
	for _, o := range a.Orgs {
		if err := CheckBaseURL(o.BaseURL); err != nil {
			return nil, fmt.Errorf("org %s: %w", o.Name, err)
		}
	}
	for _, r := range a.Repos {
		if err := CheckBaseURL(r.BaseURL); err != nil {
			return nil, fmt.Errorf("repo %s: %w", r.Scope(), err)
		}
	}
	if err := checkVersion(a.Version); err != nil {
		return nil, err
	}
	return &a, nil
}

// CheckBaseURL accepts an empty base URL or the root of a GHES instance. The
// API and App URLs are derived from it, so it must carry no path, and
// github.com is spelled by leaving it empty.
func CheckBaseURL(base string) error {
	if base == "" {
		return nil
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || strings.Trim(u.Path, "/") != "" {
		return fmt.Errorf("base_url %q must be a GHES root such as https://ghe.example.com", base)
	}
	if strings.EqualFold(u.Hostname(), "github.com") {
		return fmt.Errorf("base_url %q: leave base_url empty for github.com", base)
	}
	return nil
}

func orgInSet(set []string, org string) bool {
	for _, s := range set {
		if strings.EqualFold(s, org) {
			return true
		}
	}
	return false
}

// Credentials are the login-managed connection secrets, written by `ushr login`
// to credentials.yaml alongside the agent config and overlaid by LoadAgent.
type Credentials struct {
	ControllerURL string   `yaml:"controller_url"`
	Token         string   `yaml:"token"`
	Name          string   `yaml:"name"`
	Orgs          []string `yaml:"orgs"`
}

// CredentialsPath is credentials.yaml in the same directory as the agent config.
func CredentialsPath(agentConfigPath string) string {
	return filepath.Join(filepath.Dir(agentConfigPath), "credentials.yaml")
}

// WriteCredentials writes c to path with 0600 perms (it holds a bearer token).
func WriteCredentials(path string, c Credentials) error {
	b, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// loadCredentials returns the credentials at path. A missing file is not an
// error (returns the zero value); a present-but-unparseable file IS, so a
// botched `ushr login` write surfaces instead of silently dropping the token.
func loadCredentials(path string) (Credentials, error) {
	var c Credentials
	if err := loadYAML(path, &c); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Credentials{}, nil
		}
		return Credentials{}, err
	}
	return c, nil
}

func checkVersion(v string) error {
	if v != SchemaVersion {
		return fmt.Errorf("config version %q not supported (expected %q)", v, SchemaVersion)
	}
	return nil
}

func loadYAML(path string, into any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, into); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}
