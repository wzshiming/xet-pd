package apis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// NewTransport returns a transport that never asks for gzip, so lengths and ranges stay byte-exact, waits at most 30s for response headers and keeps up to idlePerHost idle connections per host, http.DefaultMaxIdleConnsPerHost if it is below 1.
func NewTransport(idlePerHost int) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DisableCompression = true
	t.ResponseHeaderTimeout = 30 * time.Second
	if idlePerHost > 0 {
		t.MaxIdleConnsPerHost = idlePerHost
	}
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
