// Package enroll handles agent authentication for the hosted control plane:
// per-agent enrollment tokens (validated against Neon) and the browser-session +
// PKCE handshake behind `ushr login`. See DESIGN-enrollment.md.
//
// OSS single-host does not use this — it keeps the static shared token (or
// loopback with no token). Enrollment is the hosted, multi-tenant path.
package enroll

import (
	"crypto/sha256"
	"encoding/hex"
)

// SessionStatus is the state of a CLI login session when the CLI polls for it.
type SessionStatus int

const (
	// Pending: the browser side hasn't approved yet.
	Pending SessionStatus = iota
	// Ready: approved, token available.
	Ready
	// Expired: unknown or past its TTL.
	Expired
)

// Session is the approved result the CLI writes into its config.
type Session struct {
	// Token is sealed to SessionMeta.PublicKey when the CLI sent one.
	Token string
	Orgs  []string
	Name  string
}

// SessionMeta is requester context captured at session creation and shown on
// the browser approval page, so the approver can recognize (or reject) the
// machine asking to enroll — the defense against device-flow phishing.
type SessionMeta struct {
	// AgentName is the enrolling host's own declared name (its agent config
	// name or hostname). The approval page displays it read-only.
	AgentName string
	// RequesterIP is the CLI's egress address as seen by the control plane.
	RequesterIP string
	// UserCode is the short confirmation code the CLI prints; the approver
	// checks it matches the page before approving (RFC 8628-style binding).
	UserCode string
	// PublicKey is the CLI's base64url X25519 key for this login. The web app
	// seals the token to it. Empty from CLIs v0.2.10 and older.
	PublicKey string
}

// Store authenticates enrollment tokens and drives the CLI login handshake.
// The web app (hosted web app) writes agent_tokens and approves cli_sessions; the
// control plane only reads/validates here.
type Store interface {
	// ValidateToken reports the orgs an enrollment token may serve and the agent
	// name it is bound to. A token covers the set of orgs the enrolling account
	// selected (all verified by that account), so one agent can serve several.
	// ok is false for unknown or revoked tokens; errors are treated as not-ok.
	ValidateToken(token string) (orgs []string, name string, ok bool)
	// CreateSession records a pending login session (idempotent on id).
	CreateSession(id, challenge string, meta SessionMeta) error
	// FetchSession returns the approved Session iff verifier matches the stored
	// challenge, consuming it. Pending/expired sessions return no token.
	FetchSession(id, verifier string) (Session, SessionStatus, error)
}

// HashToken is the at-rest form of an enrollment token: hex sha256. Both this
// control plane and the hosted web app minter must hash identically.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Challenge is the PKCE challenge for a verifier: hex sha256(verifier). The CLI
// sends this; FetchSession recomputes it from the presented verifier.
func Challenge(verifier string) string {
	return HashToken(verifier)
}
