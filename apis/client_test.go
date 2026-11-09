package apis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewTransport(t *testing.T) {
	t.Parallel()
	tr := NewTransport()
	if tr == http.DefaultTransport || !tr.DisableCompression || tr.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("NewTransport: shared %v, DisableCompression %v, ResponseHeaderTimeout %v, want a clone, true, 30s", tr == http.DefaultTransport, tr.DisableCompression, tr.ResponseHeaderTimeout)
	}
}

func TestNewClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.RedirectHandler("/elsewhere", http.StatusFound))
	defer srv.Close()
	resp, err := NewClient(srv.Client().Transport).Get(srv.URL)
	if err != nil {
		t.Fatalf("NewClient followed the redirect: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("GET = %d, want the 302 itself", resp.StatusCode)
	}
}

func TestPostJSON(t *testing.T) {
	t.Parallel()
	type sent struct{ method, ctype, auth, body string }
	reqs := make(chan sent, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		reqs <- sent{r.Method, r.Header.Get("Content-Type"), r.Header.Get("Authorization"), string(body)}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	for secret, auth := range map[string]string{"s3cret": "Bearer s3cret", "": ""} {
		resp, err := PostJSON(t.Context(), srv.Client(), srv.URL, secret, map[string]int{"n": 1})
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		want := sent{http.MethodPost, "application/json", auth, `{"n":1}`}
		if got := <-reqs; got != want || resp.StatusCode != http.StatusAccepted {
			t.Errorf("PostJSON with secret %q sent %+v, answered %d, want %+v, 202", secret, got, resp.StatusCode, want)
		}
	}
}

func TestDecodeJSON(t *testing.T) {
	t.Parallel()
	const limit = 16
	tests := []struct {
		name, body string
		wantErr    bool
	}{
		{"valid", `{"N":1}`, false},
		{"whitespace up to the limit", `{"N":1}` + strings.Repeat(" ", limit-7), false},
		{"over the limit", `{"N":1}` + strings.Repeat(" ", limit-6), true},
		{"trailing value", `{"N":1}{}`, true},
		{"invalid json", `{"N":`, true},
	}
	for _, tt := range tests {
		var v struct{ N int }
		err := DecodeJSON(strings.NewReader(tt.body), limit, &v)
		if (err != nil) != tt.wantErr || err == nil && v.N != 1 {
			t.Errorf("%s: DecodeJSON = %v, N %d, want error %v", tt.name, err, v.N, tt.wantErr)
		}
	}
}

// seen is what a test server saw of one request.
type seen struct{ method, path, ctype, auth, peer, body string }

// replying answers every request with code and body and hands what it saw to the returned channel.
func replying(t *testing.T, code int, body string) (*httptest.Server, <-chan seen) {
	reqs := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		reqs <- seen{r.Method, r.URL.Path, r.Header.Get("Content-Type"), r.Header.Get("Authorization"), r.Header.Get(PeerHeader), string(b)}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, reqs
}

// calls runs every typed call against origin, keeping only its error.
var calls = map[string]func(ctx context.Context, c *http.Client, origin, secret string) error{
	"Announce": func(ctx context.Context, c *http.Client, origin, secret string) error {
		_, err := Announce(ctx, c, origin, secret, AnnounceRequest{})
		return err
	},
	"Lookup": func(ctx context.Context, c *http.Client, origin, secret string) error {
		_, err := Lookup(ctx, c, origin, secret, LookupRequest{})
		return err
	},
	"Pull": func(ctx context.Context, c *http.Client, origin, secret string) error {
		return Pull(ctx, c, origin, secret, PullRequest{})
	},
	"GetShard": func(ctx context.Context, c *http.Client, origin, secret string) error {
		_, err := GetShard(ctx, c, origin, secret, "self", "f1")
		return err
	},
}

func TestAnnounce(t *testing.T) {
	t.Parallel()
	srv, reqs := replying(t, http.StatusOK, `{"ttl":30,"min_seeds":2}`)
	req := AnnounceRequest{Peer: Peer{ID: "p1", URL: "http://p1"}, Epoch: "e1", Full: true}
	resp, err := Announce(t.Context(), srv.Client(), srv.URL, "s3cret", req)
	if err != nil || resp != (AnnounceResponse{TTL: 30, MinSeeds: 2}) {
		t.Fatalf("Announce = %+v, %v, want TTL 30 and MinSeeds 2", resp, err)
	}
	want := seen{http.MethodPost, AnnouncePath, "application/json", "Bearer s3cret", "", `{"peer":{"id":"p1","url":"http://p1"},"epoch":"e1","full":true}`}
	if got := <-reqs; got != want {
		t.Fatalf("Announce sent %+v, want %+v", got, want)
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()
	srv, reqs := replying(t, http.StatusOK, `{"xorbs":{"aa":[{"id":"p2","url":"http://p2"}]}}`)
	resp, err := Lookup(t.Context(), srv.Client(), srv.URL, "s3cret", LookupRequest{Peer: "p1", Xorbs: []string{"aa"}})
	if want := (LookupResponse{Xorbs: map[string][]Peer{"aa": {{ID: "p2", URL: "http://p2"}}}}); err != nil || !reflect.DeepEqual(resp, want) {
		t.Fatalf("Lookup = %+v, %v, want %+v", resp, err, want)
	}
	want := seen{http.MethodPost, LookupPath, "application/json", "Bearer s3cret", "", `{"peer":"p1","xorbs":["aa"]}`}
	if got := <-reqs; got != want {
		t.Fatalf("Lookup sent %+v, want %+v", got, want)
	}
}

func TestPull(t *testing.T) {
	t.Parallel()
	srv, reqs := replying(t, http.StatusAccepted, "")
	if err := Pull(t.Context(), srv.Client(), srv.URL, "s3cret", PullRequest{File: "f1", Seed: true}); err != nil {
		t.Fatalf("Pull answered 202 = %v, want nil", err)
	}
	want := seen{http.MethodPost, PullPath, "application/json", "Bearer s3cret", "", `{"hash":"","file":"f1","seed":true}`}
	if got := <-reqs; got != want {
		t.Fatalf("Pull sent %+v, want %+v", got, want)
	}
}

func TestPullAcceptsOnly202(t *testing.T) {
	t.Parallel()
	for _, code := range []int{http.StatusOK, http.StatusNoContent} {
		srv, _ := replying(t, code, "")
		err := Pull(t.Context(), srv.Client(), srv.URL, "", PullRequest{Hash: "aa"})
		if status := fmt.Sprintf("%d %s", code, http.StatusText(code)); err == nil || !strings.Contains(err.Error(), status) {
			t.Errorf("Pull answered %s = %v, want an error naming the status", status, err)
		}
	}
}

func TestGetShard(t *testing.T) {
	t.Parallel()
	srv, reqs := replying(t, http.StatusOK, `{"shard":"c2hhcmQ=","offsets":{"aa":[10,25]}}`)
	resp, err := GetShard(t.Context(), srv.Client(), srv.URL, "s3cret", "p1", "f1")
	if want := (ShardResponse{Shard: []byte("shard"), Offsets: map[string][]uint64{"aa": {10, 25}}}); err != nil || !reflect.DeepEqual(resp, want) {
		t.Fatalf("GetShard = %+v, %v, want %+v", resp, err, want)
	}
	want := seen{http.MethodGet, ShardPathPrefix + "f1", "", "Bearer s3cret", "p1", ""}
	if got := <-reqs; got != want {
		t.Fatalf("GetShard sent %+v, want %+v", got, want)
	}
}

func TestGetShardNotFound(t *testing.T) {
	t.Parallel()
	srv, _ := replying(t, http.StatusNotFound, "404 page not found\n")
	if _, err := GetShard(t.Context(), srv.Client(), srv.URL, "", "p1", "f1"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard answered 404 = %v, want fs.ErrNotExist", err)
	}
}

func TestCallsReportTheStatus(t *testing.T) {
	t.Parallel()
	for name, call := range calls {
		srv, _ := replying(t, http.StatusServiceUnavailable, "busy\n")
		err := call(t.Context(), srv.Client(), srv.URL, "")
		if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable") || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s answered 503 = %v, want an error naming the status", name, err)
		}
	}
}

func TestCallsWithoutSecretSendNoAuthorization(t *testing.T) {
	t.Parallel()
	for name, call := range calls {
		srv, reqs := replying(t, http.StatusOK, `{}`)
		_ = call(t.Context(), srv.Client(), srv.URL, "")
		if got := <-reqs; got.auth != "" {
			t.Errorf("%s without a secret sent Authorization %q", name, got.auth)
		}
	}
}

// The transport drains a reply closed early for only 50ms, so a slower reply is reused only if the call drains it.
func TestCallsDrainTheReply(t *testing.T) {
	t.Parallel()
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			addrs := make(chan string, 2)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				addrs <- r.RemoteAddr
				w.WriteHeader(http.StatusServiceUnavailable)
				w.(http.Flusher).Flush()
				time.Sleep(100 * time.Millisecond)
				_, _ = io.WriteString(w, "busy\n")
			}))
			defer srv.Close()
			for range 2 {
				_ = call(t.Context(), srv.Client(), srv.URL, "")
			}
			if first, second := <-addrs, <-addrs; first != second {
				t.Errorf("second call came from %s, first from %s; want the connection reused", second, first)
			}
		})
	}
}

func TestCallsLimitTheReply(t *testing.T) {
	t.Parallel()
	for name, limit := range map[string]int{"Announce": MaxReplyBytes, "Lookup": MaxLookupBytes, "GetShard": MaxShardBytes} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "{}")
			spaces := []byte(strings.Repeat(" ", 64<<10))
			for n := limit + 1 - 2; n > 0; n -= len(spaces) {
				_, _ = w.Write(spaces[:min(n, len(spaces))])
			}
		}))
		err := calls[name](t.Context(), srv.Client(), srv.URL, "")
		srv.Close()
		if want := fmt.Sprintf("exceeds %d bytes", limit); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s with a %d-byte reply = %v, want an error saying it %s", name, limit+1, err, want)
		}
	}
}
