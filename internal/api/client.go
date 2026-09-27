package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is the agent-side HTTP client to the controller.
type Client struct {
	UpdateVersion string
	BaseURL       string
	Token         string
	HTTP          *http.Client
}

// NewClient constructs a Client. Default timeout is generous because /poll
// long-polls server-side.
func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    &http.Client{Timeout: PollTimeout + 10*time.Second},
	}
}

// ErrOfferGone reports a claim on an offer the server no longer holds (it
// expired or was resolved). The agent should drop the offer; the job has been
// re-enqueued server-side.
var ErrOfferGone = errors.New("offer gone")

// Poll sends one poll iteration. Returns nil offer on long-poll timeout.
func (c *Client) Poll(ctx context.Context, agent string, req PollRequest) (*Offer, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/v1/agents/%s/poll", c.BaseURL, agent)
	resp, err := c.do(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	c.UpdateVersion = resp.Header.Get("X-Ushr-Agent-Version")
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, nil
	case http.StatusOK:
		var o Offer
		if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
			return nil, fmt.Errorf("decode offer: %w", err)
		}
		return &o, nil
	default:
		return nil, errFromResponse(resp)
	}
}

// Claim accepts an offer: the control plane records the dispatch in its ledger
// and returns 200. No credential comes back — the agent mints the JIT locally.
// ErrOfferGone (410) means the offer expired or is unknown; drop it.
func (c *Client) Claim(ctx context.Context, agent, id string) error {
	url := fmt.Sprintf("%s/v1/agents/%s/dispatches/%s/claim", c.BaseURL, agent, id)
	resp, err := c.do(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusGone:
		return ErrOfferGone
	default:
		return errFromResponse(resp)
	}
}

// ReportDone notifies the controller that a dispatch has finished.
func (c *Client) ReportDone(ctx context.Context, agent, handle string, req DoneRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/v1/agents/%s/slots/%s/done", c.BaseURL, agent, handle)
	resp, err := c.do(ctx, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return errFromResponse(resp)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	return c.HTTP.Do(req)
}

func errFromResponse(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("http %d: %s", resp.StatusCode, bytes.TrimSpace(body))
}
