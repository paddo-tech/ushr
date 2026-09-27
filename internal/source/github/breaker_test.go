package github

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func authErr(code int) error {
	return &github.ErrorResponse{
		Response: &http.Response{
			StatusCode: code,
			Request:    &http.Request{Method: http.MethodGet, URL: &url.URL{Scheme: "https", Host: "api.github.com"}},
		},
		Message: "forbidden",
	}
}

func rateErr(reset time.Time) error {
	return &github.RateLimitError{
		Rate:     github.Rate{Limit: 5000, Remaining: 0, Reset: github.Timestamp{Time: reset}},
		Response: &http.Response{StatusCode: 403, Request: &http.Request{Method: http.MethodGet, URL: &url.URL{}}},
		Message:  "rate limited",
	}
}

func abuseErr(d time.Duration) error {
	return &github.AbuseRateLimitError{
		Response:   &http.Response{StatusCode: 403, Request: &http.Request{Method: http.MethodGet, URL: &url.URL{}}},
		Message:    "secondary limit",
		RetryAfter: &d,
	}
}

func okResp(limit, remaining int, reset time.Time) *github.Response {
	return &github.Response{Rate: github.Rate{Limit: limit, Remaining: remaining, Reset: github.Timestamp{Time: reset}}}
}

func TestBreaker_Observe(t *testing.T) {
	reset := base.Add(30 * time.Minute)
	cases := []struct {
		name      string
		err       error
		resp      *github.Response
		wantReady bool          // ready immediately after observe (at base)?
		wantUntil time.Duration // expected pause from base; -1 = don't check
	}{
		{"success healthy", nil, okResp(5000, 4000, reset), true, -1},
		{"success low water throttles, not until reset", nil, okResp(5000, 10, reset), false, lowWaterPause},
		{"primary rate limit waits for reset", rateErr(reset), nil, false, 30 * time.Minute},
		{"secondary limit honors retry-after", abuseErr(90 * time.Second), nil, false, 90 * time.Second},
		{"auth failure backs off 1m", authErr(403), nil, false, time.Minute},
		{"transient error does not trip", errString("dial tcp: timeout"), nil, true, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b breaker
			b.observe(base, c.err, c.resp)
			if got := b.ready(base); got != c.wantReady {
				t.Errorf("ready=%v, want %v", got, c.wantReady)
			}
			if c.wantUntil >= 0 {
				if want := base.Add(c.wantUntil); !b.until.Equal(want) {
					t.Errorf("until=%v, want %v", b.until, want)
				}
			}
		})
	}
}

func TestBreaker_ZeroResetFallback(t *testing.T) {
	// A rate-limit error whose Reset is the zero time must still pause (fallback
	// to a short delay), not leave the breaker open re-hammering the limit.
	var b breaker
	b.observe(base, rateErr(time.Time{}), nil)
	if b.ready(base) {
		t.Fatal("zero Reset should still pause the breaker")
	}
	if got := b.until.Sub(base); got != time.Minute {
		t.Errorf("fallback pause = %v, want 1m", got)
	}
}

func TestBreaker_AuthBackoffEscalates(t *testing.T) {
	var b breaker
	for i, want := range []time.Duration{1, 2, 4, 8} {
		b.observe(base, authErr(401), nil)
		if got := b.until.Sub(base); got != want*time.Minute {
			t.Errorf("strike %d: backoff=%v, want %v", i+1, got, want*time.Minute)
		}
	}
}

func TestBreaker_AuthBackoffCaps(t *testing.T) {
	var b breaker
	for i := 0; i < 20; i++ {
		b.observe(base, authErr(401), nil)
	}
	if got := b.until.Sub(base); got != authBackoffMax {
		t.Errorf("capped backoff=%v, want %v", got, authBackoffMax)
	}
}

func TestBreaker_SuccessClearsStrikes(t *testing.T) {
	var b breaker
	b.observe(base, authErr(401), nil)
	b.observe(base, authErr(401), nil) // strikes=2
	b.observe(base, nil, okResp(5000, 4000, base))
	b.observe(base, authErr(401), nil) // back to strike 1 → 1m, not 4m
	if got := b.until.Sub(base); got != time.Minute {
		t.Errorf("post-success backoff=%v, want 1m (strikes reset)", got)
	}
}

func TestBreaker_ReadyAfterPause(t *testing.T) {
	var b breaker
	b.observe(base, authErr(403), nil) // pause 1m
	if b.ready(base.Add(59 * time.Second)) {
		t.Error("should still be paused at 59s")
	}
	if !b.ready(base.Add(61 * time.Second)) {
		t.Error("should be ready after 1m")
	}
}

func TestLowWater(t *testing.T) {
	cases := []struct{ limit, want int }{
		{5000, 250}, // 5%
		{1000, 50},  // 5% = 50, == floor
		{500, 50},   // 5% = 25 < floor → 50
		{0, 50},
	}
	for _, c := range cases {
		if got := lowWater(c.limit); got != c.want {
			t.Errorf("lowWater(%d)=%d, want %d", c.limit, got, c.want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestBreaker_LowWaterThrottleRecovers(t *testing.T) {
	// A low-water dip must throttle briefly and resume — not black the org out
	// until the (far-off) window reset, which would strand a job queued meanwhile.
	reset := base.Add(45 * time.Minute)
	var b breaker
	b.observe(base, nil, okResp(5000, 5, reset))
	if b.ready(base) {
		t.Fatal("low water should pause briefly")
	}
	if b.ready(base.Add(lowWaterPause - time.Second)) {
		t.Error("should still be throttled within the pause window")
	}
	if !b.ready(base.Add(lowWaterPause + time.Second)) {
		t.Error("should resume polling after the throttle, well before window reset")
	}
}
