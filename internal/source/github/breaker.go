package github

import (
	"errors"
	"time"

	"github.com/google/go-github/v84/github"
)

const (
	authBackoffBase = time.Minute
	authBackoffMax  = 30 * time.Minute
	lowWaterMin     = 50
	// lowWaterPause throttles an org that dips below the low-water floor rather
	// than blacking it out: poll less often to conserve headroom for the
	// dispatch-path JIT mints, but keep discovering so a job queued during the
	// dip isn't stranded for up to a full rate window. Actual exhaustion (a
	// RateLimitError) still hard-pauses until reset.
	lowWaterPause = time.Minute
)

// breaker gates polling for one org, pausing it on auth failure or rate-limit
// pressure so a broken or throttled org stops burning shared secondary limits
// and CPU instead of re-failing every tick. The zero value is closed (polls
// normally). Driven from the single poll goroutine; not safe for concurrent use.
type breaker struct {
	until   time.Time
	strikes int
}

// ready reports whether the org may be polled now.
func (b *breaker) ready(now time.Time) bool {
	return !now.Before(b.until)
}

// observe folds one API outcome into the breaker state. resp may be nil (e.g. a
// transport-level error or a call site without a response); rate-limit errors
// carry their own reset time, so nil resp still trips correctly.
func (b *breaker) observe(now time.Time, err error, resp *github.Response) {
	var abuse *github.AbuseRateLimitError
	var rate *github.RateLimitError
	var errResp *github.ErrorResponse
	switch {
	case errors.As(err, &abuse):
		// Secondary (abuse) limit: respect Retry-After.
		b.strikes = 0
		d := time.Minute
		if abuse.RetryAfter != nil && *abuse.RetryAfter > 0 {
			d = *abuse.RetryAfter
		}
		b.until = now.Add(d)
	case errors.As(err, &rate):
		// Primary limit exhausted: wait for the window to roll over.
		b.strikes = 0
		b.until = resetOr(rate.Rate.Reset.Time, now)
	case errors.As(err, &errResp) && errResp.Response != nil && isAuthStatus(errResp.Response.StatusCode):
		// Revoked/misconfigured installation: exponential backoff.
		b.strikes++
		b.until = now.Add(authBackoff(b.strikes))
	case err != nil:
		// Transient (network, 5xx): don't trip — ordinary logging handles it.
	default:
		// Success. Clear strikes; near the low-water floor, throttle (a short
		// pause re-evaluated each cycle) to leave headroom for dispatch-path
		// mints without going dark on new work until the window resets.
		b.strikes = 0
		if resp != nil && resp.Rate.Limit > 0 && resp.Rate.Remaining <= lowWater(resp.Rate.Limit) {
			b.until = now.Add(lowWaterPause)
		}
	}
}

// resetOr returns the rate window reset time, or a short fallback pause when the
// reset is missing or already past, so a zero Reset never leaves the breaker
// open and re-hammering an exhausted limit.
func resetOr(reset, now time.Time) time.Time {
	if reset.After(now) {
		return reset
	}
	return now.Add(time.Minute)
}

func isAuthStatus(code int) bool {
	return code == 401 || code == 403
}

// authBackoff grows 1m, 2m, 4m… capped at 30m.
func authBackoff(strikes int) time.Duration {
	d := authBackoffBase
	for i := 1; i < strikes; i++ {
		d *= 2
		if d >= authBackoffMax {
			return authBackoffMax
		}
	}
	return d
}

// lowWater is the remaining-request floor (~5% of the bucket, min 50) below
// which the org pauses until its window resets.
func lowWater(limit int) int {
	if w := limit / 20; w > lowWaterMin {
		return w
	}
	return lowWaterMin
}
