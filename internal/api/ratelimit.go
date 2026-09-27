package api

import (
	"sync"
	"time"
)

// ipLimiter is a fixed-window per-IP rate limiter for unauthenticated
// endpoints. Per-process: multiple instances multiply the effective limit.
// The counts map is replaced at each window boundary and capped within a
// window (new IPs are denied once full — fail closed under a flood of
// distinct addresses).
type ipLimiter struct {
	limit    int
	interval time.Duration

	mu          sync.Mutex
	windowStart time.Time
	counts      map[string]int
	now         func() time.Time // test seam
}

func newIPLimiter(limit int, interval time.Duration) *ipLimiter {
	return &ipLimiter{
		limit:    limit,
		interval: interval,
		counts:   map[string]int{},
		now:      time.Now,
	}
}

const maxTrackedIPs = 100_000

func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Sub(l.windowStart) >= l.interval {
		l.windowStart = now
		l.counts = map[string]int{}
	}
	count, seen := l.counts[ip]
	if !seen && len(l.counts) >= maxTrackedIPs {
		return false
	}
	if count >= l.limit {
		return false
	}
	l.counts[ip] = count + 1
	return true
}
