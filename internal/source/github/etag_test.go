package github

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func mkResp(status int, etag, remaining, body string, req *http.Request) *http.Response {
	h := http.Header{}
	if etag != "" {
		h.Set("ETag", etag)
	}
	if remaining != "" {
		h.Set("X-RateLimit-Remaining", remaining)
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func TestETagTransport_CachesAndReplaysOn304(t *testing.T) {
	var sawINM []string // If-None-Match seen by the inner transport per call
	calls := 0
	inner := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sawINM = append(sawINM, r.Header.Get("If-None-Match"))
		calls++
		if calls == 1 {
			return mkResp(http.StatusOK, `"abc"`, "100", `[{"id":1}]`, r), nil
		}
		// Second call: unchanged, fresh rate header, no body.
		return mkResp(http.StatusNotModified, `"abc"`, "99", "", r), nil
	})
	tr := newETagTransport(inner, 8)

	req1, _ := http.NewRequest(http.MethodGet, "https://api.github.com/x/runs", nil)
	resp1, err := tr.RoundTrip(req1)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := io.ReadAll(resp1.Body)
	if string(b1) != `[{"id":1}]` {
		t.Fatalf("first body = %q", b1)
	}
	if sawINM[0] != "" {
		t.Errorf("first call should not send If-None-Match, got %q", sawINM[0])
	}

	req2, _ := http.NewRequest(http.MethodGet, "https://api.github.com/x/runs", nil)
	resp2, err := tr.RoundTrip(req2)
	if err != nil {
		t.Fatal(err)
	}
	if sawINM[1] != `"abc"` {
		t.Errorf("second call If-None-Match = %q, want \"abc\"", sawINM[1])
	}
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("304 should be replayed as 200, got %d", resp2.StatusCode)
	}
	b2, _ := io.ReadAll(resp2.Body)
	if string(b2) != `[{"id":1}]` {
		t.Errorf("replayed body = %q, want cached body", b2)
	}
	// Fresh rate header from the live 304, not the stale 100 from the first 200.
	if got := resp2.Header.Get("X-RateLimit-Remaining"); got != "99" {
		t.Errorf("replayed rate remaining = %q, want fresh 99", got)
	}
}

func TestETagTransport_ForwardsLinkOn304(t *testing.T) {
	// GitHub returns a current Link header on a 304; replay must serve THAT,
	// not the stale Link cached with the original body, or pagination breaks.
	calls := 0
	inner := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		resp := mkResp(http.StatusOK, `"e"`, "", `[]`, r)
		if calls == 1 {
			resp.Header.Set("Link", `<https://api/x?page=2>; rel="next"`)
			return resp, nil
		}
		resp = mkResp(http.StatusNotModified, `"e"`, "", "", r)
		// Page 3 now exists; the live 304 reflects the new link set.
		resp.Header.Set("Link", `<https://api/x?page=2>; rel="next", <https://api/x?page=3>; rel="next"`)
		return resp, nil
	})
	tr := newETagTransport(inner, 8)

	req1, _ := http.NewRequest(http.MethodGet, "https://api/x", nil)
	resp1, _ := tr.RoundTrip(req1)
	_ = resp1.Body.Close()

	req2, _ := http.NewRequest(http.MethodGet, "https://api/x", nil)
	resp2, err := tr.RoundTrip(req2)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp2.Header.Get("Link"); got != `<https://api/x?page=2>; rel="next", <https://api/x?page=3>; rel="next"` {
		t.Errorf("replayed Link = %q, want the fresh live Link", got)
	}
}

func TestETagTransport_BypassesNonGET(t *testing.T) {
	inner := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("If-None-Match") != "" {
			t.Error("non-GET must not get If-None-Match")
		}
		return mkResp(http.StatusOK, `"x"`, "", "ok", r), nil
	})
	tr := newETagTransport(inner, 8)
	req, _ := http.NewRequest(http.MethodPost, "https://api.github.com/app/installations/1/access_tokens", nil)
	if _, err := tr.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
}

func TestETagTransport_RealServerNoETagNotCached(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No ETag emitted: never cached, every request reaches the handler.
		if r.Header.Get("If-None-Match") != "" {
			t.Error("should not send If-None-Match for an uncached URL")
		}
		_, _ = io.WriteString(w, "no-etag")
	}))
	defer srv.Close()

	tr := newETagTransport(http.DefaultTransport, 8)
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

func TestLRU_Eviction(t *testing.T) {
	c := newLRU(2)
	c.set("a", &cacheEntry{etag: "1"})
	c.set("b", &cacheEntry{etag: "2"})
	c.get("a")                         // a now most-recent
	c.set("c", &cacheEntry{etag: "3"}) // evicts b (least-recent)
	if _, ok := c.get("b"); ok {
		t.Error("b should have been evicted")
	}
	if _, ok := c.get("a"); !ok {
		t.Error("a should survive")
	}
	if _, ok := c.get("c"); !ok {
		t.Error("c should be present")
	}
}
