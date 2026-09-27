package github

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// etagTransport makes conditional GET requests using stored ETags. GitHub does
// not charge primary rate-limit quota for a conditional request answered with
// 304 Not Modified, so idle list polls become effectively free: an unchanged
// run list returns 304 (no body, no quota) and we replay the cached body, so
// the poller above behaves identically — just cheaper.
//
// Only GET responses carrying an ETag are cached. The cache is a bounded LRU so
// per-run job-list URLs (each a distinct URL, polled only while the run is
// active) can't accumulate forever.
type etagTransport struct {
	inner http.RoundTripper
	mu    sync.Mutex
	cache *lru
}

func newETagTransport(inner http.RoundTripper, capacity int) *etagTransport {
	return &etagTransport{inner: inner, cache: newLRU(capacity)}
}

type cacheEntry struct {
	etag   string
	body   []byte
	header http.Header
}

func (t *etagTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return t.inner.RoundTrip(req)
	}
	key := req.URL.String()

	t.mu.Lock()
	cached, hit := t.cache.get(key)
	t.mu.Unlock()
	if hit {
		req.Header.Set("If-None-Match", cached.etag)
	}

	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	switch resp.StatusCode {
	case http.StatusNotModified:
		if !hit {
			return resp, nil // no cached body to serve; let caller handle it
		}
		_ = resp.Body.Close()
		return cached.replay(req, resp), nil
	case http.StatusOK:
		etag := resp.Header.Get("ETag")
		if etag == "" {
			return resp, nil
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		t.mu.Lock()
		t.cache.set(key, &cacheEntry{etag: etag, body: body, header: resp.Header.Clone()})
		t.mu.Unlock()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		return resp, nil
	default:
		return resp, nil
	}
}

// replay reconstructs a 200 response from the cached body, but carries the live
// 304's volatile headers forward: rate-limit counters (so accounting sees
// current values) and Link (so pagination follows the resource's current page
// set, not the stale links captured when the body was first cached). GitHub
// returns both on a 304.
func (e *cacheEntry) replay(req *http.Request, live *http.Response) *http.Response {
	h := e.header.Clone()
	for k := range h {
		if isVolatileHeader(k) {
			h.Del(k)
		}
	}
	for k, vs := range live.Header {
		if isVolatileHeader(k) {
			h[k] = vs
		}
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", http.StatusOK, http.StatusText(http.StatusOK)),
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(e.body)),
		ContentLength: int64(len(e.body)),
		Request:       req,
	}
}

func isVolatileHeader(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case "X-Ratelimit-Limit", "X-Ratelimit-Remaining", "X-Ratelimit-Reset",
		"X-Ratelimit-Used", "X-Ratelimit-Resource", "Retry-After", "Link":
		return true
	}
	return false
}

// lru is a bounded key->entry cache evicting least-recently-used on overflow.
// Not safe for concurrent use; etagTransport guards it with a mutex.
type lru struct {
	cap int
	ll  *list.List
	m   map[string]*list.Element
}

type lruItem struct {
	key string
	val *cacheEntry
}

func newLRU(capacity int) *lru {
	if capacity < 1 {
		capacity = 1
	}
	return &lru{cap: capacity, ll: list.New(), m: make(map[string]*list.Element, capacity)}
}

func (c *lru) get(key string) (*cacheEntry, bool) {
	el, ok := c.m[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*lruItem).val, true
}

func (c *lru) set(key string, val *cacheEntry) {
	if el, ok := c.m[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*lruItem).val = val
		return
	}
	c.m[key] = c.ll.PushFront(&lruItem{key: key, val: val})
	if c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.m, oldest.Value.(*lruItem).key)
		}
	}
}
