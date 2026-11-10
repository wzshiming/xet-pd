package apis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"time"
)

// responseHeaderTimeout bounds the wait for response headers on every xet-pd connection; callers bound whole exchanges through their contexts.
const responseHeaderTimeout = 30 * time.Second

// NewTransport returns a transport that never asks for gzip, so lengths and ranges stay byte-exact, and waits at most responseHeaderTimeout for response headers.
func NewTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DisableCompression = true
	t.ResponseHeaderTimeout = responseHeaderTimeout
	return t
}

// NewClient returns a client over rt that never follows redirects, so a secret-bearing request goes only where it was sent.
func NewClient(rt http.RoundTripper) *http.Client {
	return &http.Client{Transport: rt, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// PostJSON posts body as JSON to url, carrying the bearer secret when it is set.
func PostJSON(ctx context.Context, client *http.Client, url, secret string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	return client.Do(req)
}

// DecodeJSON decodes a reply of exactly one JSON value within limit bytes into v.
func DecodeJSON(r io.Reader, limit int64, v any) error {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return fmt.Errorf("exceeds %d bytes", limit)
	}
	return decode(bytes.NewReader(data), v)
}

// Announce posts req to the tracker's AnnouncePath and decodes its 200 reply.
func Announce(ctx context.Context, c *http.Client, tracker, secret string, req AnnounceRequest) (AnnounceResponse, error) {
	var out AnnounceResponse
	err := exchange(ctx, c, tracker+AnnouncePath, secret, req, MaxReplyBytes, &out)
	return out, err
}

// Lookup posts req to the tracker's LookupPath and decodes its 200 reply.
func Lookup(ctx context.Context, c *http.Client, tracker, secret string, req LookupRequest) (LookupResponse, error) {
	var out LookupResponse
	err := exchange(ctx, c, tracker+LookupPath, secret, req, MaxLookupBytes, &out)
	return out, err
}

// Pull posts req to the peer's PullPath; only 202 means the peer took it.
func Pull(ctx context.Context, c *http.Client, peer, secret string, req PullRequest) error {
	resp, err := PostJSON(ctx, c, peer+PullPath, secret, req)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

// GetShard fetches the ShardResponse of fileHash from peer, sending self as PeerHeader; a 404 yields an error wrapping fs.ErrNotExist.
func GetShard(ctx context.Context, c *http.Client, peer, secret, self, fileHash string) (ShardResponse, error) {
	var out ShardResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ShardURL(peer, fileHash), nil)
	if err != nil {
		return out, err
	}
	req.Header.Set(PeerHeader, self)
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	resp, err := c.Do(req)
	if err != nil {
		return out, err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		err = DecodeJSON(resp.Body, MaxShardBytes, &out)
	case http.StatusNotFound:
		err = fmt.Errorf("status %s: %w", resp.Status, fs.ErrNotExist)
	default:
		err = fmt.Errorf("status %s", resp.Status)
	}
	return out, err
}

// exchange posts body to url and decodes a 200 reply of at most limit bytes into out.
func exchange(ctx context.Context, c *http.Client, url, secret string, body any, limit int64, out any) error {
	resp, err := PostJSON(ctx, c, url, secret, body)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", resp.Status)
	}
	return DecodeJSON(resp.Body, limit, out)
}

// drain reads up to MaxReplyBytes of what is left of resp's body, so its connection can be reused, and closes it.
func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxReplyBytes))
	_ = resp.Body.Close()
}
