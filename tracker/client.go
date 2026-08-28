package tracker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client talks to a tracker: announces this peer's holdings and looks up
// which peers hold a file. Both agents and tests use it; tracker errors are
// for the caller to swallow — a missing tracker only degrades efficiency.
type Client struct {
	base string
	hc   *http.Client
	mint func(now time.Time) (token string, exp int64)
}

// ClientOption configures the Client.
type ClientOption func(*Client)

// WithMintToken makes announces carry a bearer token minted per request,
// matching a tracker running WithAuth on the same cluster secret.
func WithMintToken(mint func(now time.Time) (token string, exp int64)) ClientOption {
	return func(c *Client) { c.mint = mint }
}

// NewClient creates a tracker client for the given base URL.
func NewClient(base string, opts ...ClientOption) (*Client, error) {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		return nil, fmt.Errorf("tracker: base URL is required")
	}
	c := &Client{
		base: base,
		hc:   &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Announce registers or withdraws holdings for the peer named in req.
func (c *Client) Announce(ctx context.Context, req *AnnounceRequest) error {
	var resp AnnounceResponse
	if err := c.post(ctx, "/v1/announce", req, &resp, true); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("tracker: announce not acknowledged")
	}
	return nil
}

// Lookup resolves file identities to the peers holding them. Absent map
// entries are misses.
func (c *Client) Lookup(ctx context.Context, req *LookupRequest) (*LookupResponse, error) {
	resp := &LookupResponse{}
	if err := c.post(ctx, "/v1/lookup", req, resp, false); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) post(ctx context.Context, path string, in, out any, authed bool) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create %s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if authed && c.mint != nil {
		tok, _ := c.mint(time.Now())
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("tracker: %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxBodySize)).Decode(out)
}
