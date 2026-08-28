package agent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"

	"github.com/wzshiming/xet-pd/agent"
)

// fakeHub is a plain upstream hub: resolve requests answer metadata headers
// and redirect to a /cdn path that serves the bytes with Range support.
type fakeHub struct {
	mu     sync.Mutex
	files  map[string][]byte
	commit string
	gate   *hubGate
}

func newFakeHub() *fakeHub {
	return &fakeHub{files: map[string][]byte{}, commit: "commit-s0"}
}

// hubGate makes the /cdn handler serve the first full GET (offset 0) of one
// path in two halves: it writes the first `after` bytes, flushes, then blocks
// until Release. This freezes the agent's ingest mid-body so a test can prove
// the agent streams from the spool instead of buffering the whole file.
type hubGate struct {
	path        string
	after       int
	hit         chan struct{} // closed once the prefix is flushed and the hub is blocking
	release     chan struct{}
	hitOnce     sync.Once
	releaseOnce sync.Once
	armed       atomic.Bool // only the first eligible request gates
}

func newHubGate(path string, after int) *hubGate {
	return &hubGate{path: path, after: after, hit: make(chan struct{}), release: make(chan struct{})}
}

func (g *hubGate) Release() { g.releaseOnce.Do(func() { close(g.release) }) }

func (u *fakeHub) serveGated(w http.ResponseWriter, r *http.Request, data []byte, g *hubGate) {
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data[:g.after])
	w.(http.Flusher).Flush()
	g.hitOnce.Do(func() { close(g.hit) })
	select {
	case <-g.release:
	case <-r.Context().Done():
		return
	}
	_, _ = w.Write(data[g.after:])
}

var hubResolveRe = regexp.MustCompile(`^(/.+?/resolve/)[^/]+(/.+)$`)

// set registers the file under the given resolve path and, like a real hub,
// under the same path with the revision replaced by the current commit.
func (u *fakeHub) set(path string, data []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[path] = data
	if m := hubResolveRe.FindStringSubmatch(path); m != nil {
		u.files[m[1]+u.commit+m[2]] = data
	}
}

func (u *fakeHub) get(path string) ([]byte, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	data, ok := u.files[path]
	return data, ok
}

func (u *fakeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/cdn/") {
		path := strings.TrimPrefix(r.URL.Path, "/cdn")
		data, ok := u.get(path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if g := u.gate; g != nil && path == g.path && r.Method == http.MethodGet &&
			r.Header.Get("Range") == "" && g.armed.CompareAndSwap(false, true) {
			u.serveGated(w, r, data, g)
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
		return
	}
	data, ok := u.get(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	sum := sha256.Sum256(data)
	etag := hex.EncodeToString(sum[:])
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Etag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
	w.Header().Set("X-Repo-Commit", u.commit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

// handlerBox keeps atomic.Value stores monomorphic across handler types.
type handlerBox struct{ h http.Handler }

// newSwappableServer returns a running server whose handler is installed
// later, so agents can be constructed knowing each other's URLs.
func newSwappableServer(t *testing.T) (*httptest.Server, func(http.Handler)) {
	t.Helper()
	var inner atomic.Value
	inner.Store(handlerBox{http.NotFoundHandler()})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.Load().(handlerBox).h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, func(h http.Handler) { inner.Store(handlerBox{h}) }
}

// newAgentAt builds an agent on srv with the given config, defaulting the
// external URL to the server's own.
func newAgentAt(t *testing.T, srv *httptest.Server, install func(http.Handler), cfg agent.Config) {
	t.Helper()
	if cfg.ExternalURL == "" {
		cfg.ExternalURL = srv.URL
	}
	a, err := agent.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	install(a)
}

// newAgentServer runs the composed agent handler on an httptest server whose
// URL doubles as the external URL. Optional peers become the agent's sibling
// list.
func newAgentServer(t *testing.T, upstreamURL string, revalidate *time.Duration, peers ...string) *httptest.Server {
	t.Helper()
	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:           upstreamURL,
		StorageDir:         t.TempDir(),
		RevalidateInterval: revalidate,
		Peers:              peers,
	})
	return srv
}

func deterministicData(n int) []byte {
	b := make([]byte, n)
	_, _ = mrand.New(mrand.NewSource(42)).Read(b)
	return b
}

func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// waitReady polls the resolve URL until the agent answers with the
// ready-state redirect and returns that response (body already consumed).
func waitReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := noRedirectClient()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusFound {
			return resp
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("agent never reached ready state")
	return nil
}

func getBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status = %d, want 200; body %q", url, resp.StatusCode, body)
	}
	return body
}

// readFullWithin fails fast instead of hanging when the agent stops streaming.
func readFullWithin(t *testing.T, r io.Reader, buf []byte, timeout time.Duration, what string) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(r, buf)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read %s: %v", what, err)
		}
	case <-time.After(timeout):
		t.Fatalf("timed out after %v reading %s", timeout, what)
	}
}

func readAllWithin(t *testing.T, r io.Reader, timeout time.Duration, what string) []byte {
	t.Helper()
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(r)
		done <- result{data, err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("read %s: %v", what, res.err)
		}
		return res.data
	case <-time.After(timeout):
		t.Fatalf("timed out after %v reading %s", timeout, what)
		return nil
	}
}

var linkRe = regexp.MustCompile(`<([^>]+)>;\s*rel="([^"]+)"`)

// parseLinks maps rel values to URLs across all Link headers.
func parseLinks(h http.Header) map[string]string {
	links := map[string]string{}
	for _, v := range h.Values("Link") {
		for _, m := range linkRe.FindAllStringSubmatch(v, -1) {
			links[m[2]] = m[1]
		}
	}
	return links
}

// TestAgentEndToEnd walks one file through the S0 states: cold streaming
// download, warm plain bridge, and warm xet-protocol path.
func TestAgentEndToEnd(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/model.bin"
	data := deterministicData(48 * 1024)
	wantSum := sha256.Sum256(data)
	hub.set(resolvePath, data)

	srv := newAgentServer(t, hubSrv.URL, nil)
	resolveURL := srv.URL + resolvePath

	t.Run("cold download streams while ingesting", func(t *testing.T) {
		body := getBody(t, resolveURL)
		if got := sha256.Sum256(body); got != wantSum {
			t.Fatalf("cold body sha256 = %x, want %x", got, wantSum)
		}
	})

	ready := waitReady(t, resolveURL)

	t.Run("warm plain bridge serves identical bytes", func(t *testing.T) {
		if ready.Header.Get("X-Xet-Hash") == "" {
			t.Fatal("ready response missing X-Xet-Hash")
		}
		links := parseLinks(ready.Header)
		for _, rel := range []string{"xet-auth", "xet-reconstruction-info"} {
			u, ok := links[rel]
			if !ok {
				t.Fatalf("Link headers missing rel %q: %q", rel, ready.Header.Values("Link"))
			}
			if !strings.HasPrefix(u, srv.URL) {
				t.Fatalf("rel %q URL = %q, want prefix %q (must point at the agent)", rel, u, srv.URL)
			}
		}

		wantLoc := srv.URL + "/xet-bridge/" + hex.EncodeToString(wantSum[:])
		if loc := ready.Header.Get("Location"); loc != wantLoc {
			t.Fatalf("ready Location = %q, want %q", loc, wantLoc)
		}
		if body := getBody(t, wantLoc); !bytes.Equal(body, data) {
			t.Fatal("bridge bytes differ from upstream data")
		}
	})

	t.Run("warm xet path serves reconstruction with minted token", func(t *testing.T) {
		var tok struct {
			CasURL      string `json:"casUrl"`
			AccessToken string `json:"accessToken"`
			Exp         int64  `json:"exp"`
		}
		if err := json.Unmarshal(getBody(t, srv.URL+"/xet-token"), &tok); err != nil {
			t.Fatalf("parse token response: %v", err)
		}
		if tok.AccessToken == "" || tok.CasURL != srv.URL {
			t.Fatalf("token response = %+v, want non-empty accessToken and casUrl %q", tok, srv.URL)
		}

		reconURL := parseLinks(ready.Header)["xet-reconstruction-info"]
		req, err := http.NewRequest(http.MethodGet, reconURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("reconstruction status = %d, want 200; body %q", resp.StatusCode, body)
		}
		var recon struct {
			Terms []json.RawMessage `json:"terms"`
		}
		if err := json.Unmarshal(body, &recon); err != nil {
			t.Fatalf("parse reconstruction response: %v", err)
		}
		if len(recon.Terms) == 0 {
			t.Fatalf("reconstruction terms empty: %s", body)
		}

		if got := downloadViaXet(t, resolveURL); !bytes.Equal(got, data) {
			t.Fatal("xet-protocol download bytes differ from upstream data")
		}
	})
}

// downloadViaXet performs a real xet-protocol download against the agent:
// resolve Link headers, minted token, reconstruction, and xorb fetches.
func downloadViaXet(t *testing.T, resolveURL string) []byte {
	t.Helper()
	ctx := context.Background()
	fileHash, provider, err := hf.ResolveDownload(ctx, nil, resolveURL)
	if err != nil {
		t.Fatalf("resolve download: %v", err)
	}
	c, err := client.NewClient(client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "dl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := c.DownloadFileWithAuthProvider(ctx, provider, fileHash, f); err != nil {
		t.Fatalf("xet download: %v", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestAnonymousReadOnly: every read is served without credentials — bare
// reconstructions and xorb fetches included — while every write path is
// rejected outright, minted token or not: CAS uploads and dedup queries, hub
// mutations through the proxy, and the write-token route. paths-info, the hub
// read query that uses POST, must keep flowing upstream.
func TestAnonymousReadOnly(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/auth.bin"
	data := deterministicData(48 * 1024)
	wantSum := sha256.Sum256(data)
	hub.set(resolvePath, data)

	srv := newAgentServer(t, hubSrv.URL, nil)
	resolveURL := srv.URL + resolvePath

	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("warm-up body mismatch")
	}
	ready := waitReady(t, resolveURL)

	do := func(method, url, bearer string, body []byte) (int, []byte) {
		t.Helper()
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, url, rd)
		if err != nil {
			t.Fatal(err)
		}
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}

	// Reads: fully anonymous end to end.
	reconURL := parseLinks(ready.Header)["xet-reconstruction-info"]
	if reconURL == "" {
		t.Fatal("missing xet-reconstruction-info link")
	}
	code, reconBody := do(http.MethodGet, reconURL, "", nil)
	if code != http.StatusOK {
		t.Fatalf("anonymous v1 reconstruction: status = %d, want 200; body %q", code, reconBody)
	}
	fileHash := ready.Header.Get("X-Xet-Hash")
	if code, _ := do(http.MethodGet, srv.URL+"/v2/reconstructions/"+fileHash, "", nil); code != http.StatusOK {
		t.Fatalf("anonymous v2 reconstruction: status = %d, want 200", code)
	}

	var recon struct {
		FetchInfo map[string][]struct {
			URL string `json:"url"`
		} `json:"fetch_info"`
	}
	if err := json.Unmarshal(reconBody, &recon); err != nil {
		t.Fatalf("parse reconstruction response: %v", err)
	}
	var xorbURL string
	for _, entries := range recon.FetchInfo {
		for _, e := range entries {
			xorbURL = e.URL
			break
		}
	}
	if xorbURL == "" {
		t.Fatalf("no fetch_info URL in reconstruction: %s", reconBody)
	}
	if code, _ := do(http.MethodGet, xorbURL, "", nil); code != http.StatusOK && code != http.StatusPartialContent {
		t.Fatalf("anonymous xorb fetch: status = %d, want 200/206", code)
	}

	// /xet-token keeps minting: the handshake token is protocol furniture,
	// not access control.
	var tok struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(getBody(t, srv.URL+"/xet-token"), &tok); err != nil {
		t.Fatalf("parse token response: %v", err)
	}
	if tok.AccessToken == "" {
		t.Fatal("empty accessToken")
	}

	bridgeURL := srv.URL + "/xet-bridge/" + hex.EncodeToString(wantSum[:])
	if body := getBody(t, bridgeURL); !bytes.Equal(body, data) {
		t.Fatal("bridge bytes differ from upstream data")
	}

	// Writes: rejected outright, with or without a minted token.
	xorbHash := strings.Repeat("0", 63) + "1"
	writes := []struct{ method, path string }{
		{http.MethodPost, "/v1/xorbs/default/" + xorbHash},
		{http.MethodPost, "/v1/shards"},
		{http.MethodPost, "/v2/shards"},
		{http.MethodPost, "/shards"},
		{http.MethodPost, "/v1/chunks/default:query"},
		{http.MethodPost, "/api/models/org/repo/commit/main"},
		{http.MethodPost, "/api/models/org/repo/preupload/main"},
		{http.MethodPut, resolvePath},
		{http.MethodDelete, "/api/models/org/repo"},
		{http.MethodPatch, "/api/models/org/repo/settings"},
	}
	for _, wr := range writes {
		for _, bearer := range []string{"", tok.AccessToken} {
			if code, _ := do(wr.method, srv.URL+wr.path, bearer, []byte("payload")); code != http.StatusForbidden {
				t.Fatalf("%s %s (bearer=%v): status = %d, want 403", wr.method, wr.path, bearer != "", code)
			}
		}
	}

	// The write-token route must not fall through to the credential-carrying
	// upstream proxy.
	if code, _ := do(http.MethodGet, srv.URL+"/api/models/org/repo/xet-write-token/main", "", nil); code != http.StatusForbidden {
		t.Fatalf("xet-write-token: status = %d, want 403", code)
	}

	// paths-info passes the gate and reaches the upstream (the fake hub
	// answers 404 for it, not the gate's 403).
	if code, _ := do(http.MethodPost, srv.URL+"/api/models/org/repo/paths-info/main", "", []byte(`{"paths":["x"]}`)); code == http.StatusForbidden {
		t.Fatal("paths-info POST blocked by the read-only gate")
	}
}

// TestAgentColdStreamsDuringIngest proves streaming-during-ingest: with the
// upstream frozen mid-body, the agent must already deliver the received
// prefix to the client; a buffer-everything implementation times out here.
func TestAgentColdStreamsDuringIngest(t *testing.T) {
	hub := newFakeHub()
	const resolvePath = "/org/repo/resolve/main/gated.bin"
	data := deterministicData(64 * 1024)
	hub.set(resolvePath, data)

	const gateAfter = 16 * 1024
	gate := newHubGate(resolvePath, gateAfter)
	hub.gate = gate

	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()
	// Registered after hubSrv.Close so it runs first: a failure mid-test must
	// unblock the gated hub handler or Close would wait on it forever (the
	// agent's background ingest keeps the request alive).
	defer gate.Release()

	srv := newAgentServer(t, hubSrv.URL, nil)
	resolveURL := srv.URL + resolvePath

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolveURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	type respResult struct {
		resp *http.Response
		err  error
	}
	respCh := make(chan respResult, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		respCh <- respResult{resp, err}
	}()
	var resp *http.Response
	select {
	case res := <-respCh:
		if res.err != nil {
			t.Fatal(res.err)
		}
		resp = res.resp
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for cold GET response headers while upstream is gated")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cold GET status = %d, want 200", resp.StatusCode)
	}

	select {
	case <-gate.hit:
	case <-time.After(10 * time.Second):
		t.Fatal("upstream gate never reached")
	}

	// The gate is still closed: only the flushed prefix exists upstream, so
	// these bytes can only come from the agent streaming its spool.
	prefix := make([]byte, 8*1024)
	readFullWithin(t, resp.Body, prefix, 10*time.Second, "prefix while upstream is gated")
	if !bytes.Equal(prefix, data[:len(prefix)]) {
		t.Fatal("streamed prefix differs from upstream data")
	}

	gate.Release()
	rest := readAllWithin(t, resp.Body, 10*time.Second, "remainder after gate release")
	if body := append(prefix, rest...); !bytes.Equal(body, data) {
		t.Fatalf("full body mismatch: got %d bytes, want %d", len(body), len(data))
	}

	waitReady(t, resolveURL)
}

// TestAgentServesWarmContentWithUpstreamDown: once a file is ready, killing
// the upstream must not break downloads even with revalidation due on every
// request.
func TestAgentServesWarmContentWithUpstreamDown(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/outage.bin"
	data := deterministicData(48 * 1024)
	hub.set(resolvePath, data)

	revalidateAlways := time.Duration(0)
	srv := newAgentServer(t, hubSrv.URL, &revalidateAlways)
	resolveURL := srv.URL + resolvePath

	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("initial body mismatch")
	}
	waitReady(t, resolveURL)

	hubSrv.Close()

	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("cached body not served while upstream is down")
	}
}
