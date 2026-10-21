package peer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

func hashes(hs ...xet.XorbHash) []string {
	out := make([]string, 0, len(hs))
	for _, h := range hs {
		out = append(out, h.String())
	}
	slices.Sort(out)
	return out
}

// hasAdd reports whether the announce adds hash.
func hasAdd(a clustertest.Announce, hash xet.XorbHash) bool {
	return slices.Contains(a.Req.Add, hash.String())
}

// hasRemove reports whether the announce removes hash.
func hasRemove(a clustertest.Announce, hash xet.XorbHash) bool {
	return slices.Contains(a.Req.Remove, hash.String())
}

// seedStore commits body into a data directory before a peer opens it.
func seedStore(t *testing.T, dir string, hash xet.XorbHash, body []byte) {
	t.Helper()
	store, err := local.NewStorage(local.WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

func xorbPath(dir string, hash xet.XorbHash) string {
	return filepath.Join(dir, filepath.FromSlash(storage.ObjectKey("xorbs", hash.String())))
}

// commitDirect stores body underneath a running peer and registers it as a finished pull would.
func commitDirect(t *testing.T, p *Peer, hash xet.XorbHash, body []byte) {
	t.Helper()
	if _, err := p.store.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.admit(hash, int64(len(body)), nil)
	p.mu.Unlock()
}

func newTestPeer(t *testing.T, gw string, mod func(*Config)) (*Peer, *httptest.Server) {
	t.Helper()
	cfg := Config{Advertise: "http://seed.test", Gateway: gw, CacheDir: t.TempDir()}
	if mod != nil {
		mod(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(func() {
		_ = p.Close()
		srv.Close()
	})
	return p, srv
}

func xorbURL(srv *httptest.Server, hash xet.XorbHash) string {
	return srv.URL + "/v1/xorbs/default/" + hash.String()
}

// do performs a request and returns the response, body and body read error.
func do(t *testing.T, method, url string, hdr ...string) (*http.Response, []byte, error) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp, body, err
}

func spoolEmpty(p *Peer) func() bool {
	return func() bool {
		left, _ := filepath.Glob(filepath.Join(p.spoolDir, "*"))
		return len(left) == 0
	}
}

func TestStartupFullAnnouncesSeededInventory(t *testing.T) {
	t.Setenv("HF_TOKEN", "hf_must_not_leak")
	a, bodyA := clustertest.MakeXorb(t, 20, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 21, 2, 8<<10)
	dir := t.TempDir()
	seedStore(t, dir, a, bodyA)
	seedStore(t, dir, b, bodyB)
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("seeded object reached the gateway")
		w.WriteHeader(http.StatusInternalServerError)
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) {
		c.CacheDir, c.Heartbeat, c.Secret = dir, time.Hour, "s3cret"
	})
	first := g.Announce(t, 0)
	want := apis.AnnounceRequest{Peer: apis.Peer{ID: p.id, URL: "http://seed.test"}, Epoch: p.epoch, Full: true}
	if got := first.Req; got.Peer != want.Peer || got.Epoch != want.Epoch || !got.Full || len(got.Remove) != 0 {
		t.Fatalf("startup announce = %+v, want a Full from %+v in epoch %s", got, want.Peer, want.Epoch)
	}
	if first.Header.Get("Authorization") != "Bearer s3cret" || first.Header.Get("Content-Type") != "application/json" || first.Header.Get(apis.PeerHeader) != "" {
		t.Fatalf("announce headers = %v", first.Header)
	}
	// Seeded objects reach the gateway as background validation admits them.
	clustertest.WaitFor(t, clustertest.WaitTimeout, "startup validation", p.Validated)
	waitView(t, g, a, b)
	n := len(g.Announces())
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, a))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, bodyA) {
		t.Fatalf("GET seeded = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if got := len(g.Announces()); got != n {
		t.Fatalf("announces after the GET = %d, want %d", got, n)
	}
}

func TestConfigValidation(t *testing.T) {
	base := Config{Advertise: "http://seed:8080/", Gateway: "https://gw.example/", CacheDir: t.TempDir()}
	bad := map[string]func(*Config){
		"missing cache dir":     func(c *Config) { c.CacheDir = "" },
		"gateway userinfo":      func(c *Config) { c.Gateway = "http://u:p@gw" },
		"gateway query":         func(c *Config) { c.Gateway = "http://gw?x=1" },
		"gateway fragment":      func(c *Config) { c.Gateway = "http://gw#f" },
		"gateway base path":     func(c *Config) { c.Gateway = "http://gw/base" },
		"gateway scheme":        func(c *Config) { c.Gateway = "ftp://gw" },
		"gateway no host":       func(c *Config) { c.Gateway = "http:///x" },
		"gateway empty host":    func(c *Config) { c.Gateway = "http://:1234" },
		"gateway port range":    func(c *Config) { c.Gateway = "http://gw:65536" },
		"gateway credentials":   func(c *Config) { c.Gateway = "http://bob:hunter2@gw/%zz" },
		"advertise relative":    func(c *Config) { c.Advertise = "seed:8080" },
		"advertise empty":       func(c *Config) { c.Advertise = "" },
		"advertise empty port":  func(c *Config) { c.Advertise = "http://seed:" },
		"advertise port zero":   func(c *Config) { c.Advertise = "http://seed:0" },
		"advertise credentials": func(c *Config) { c.Advertise = "http://bob:hunter2@seed/%zz" },
		"negative max pulls":    func(c *Config) { c.MaxPulls = -1 },
		"negative timeout":      func(c *Config) { c.PullTimeout = -time.Second },
		"negative heartbeat":    func(c *Config) { c.Heartbeat = -time.Second },
		"negative cache size":   func(c *Config) { c.XorbCacheSize = -1 },
	}
	for name, mutate := range bad {
		cfg := base
		mutate(&cfg)
		p, err := New(cfg)
		if err == nil {
			_ = p.Close()
			t.Errorf("%s: New accepted %+v", name, cfg)
		} else if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: New echoes the url: %v", name, err)
		}
	}
	p, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if p.cfg.Advertise != "http://seed:8080" || p.cfg.Gateway != "https://gw.example" {
		t.Fatalf("normalized URLs = %q %q", p.cfg.Advertise, p.cfg.Gateway)
	}
	if p.cfg.MaxPulls != 8 || p.cfg.PullTimeout != 10*time.Minute || p.cfg.Heartbeat != 10*time.Second {
		t.Fatalf("defaults = %+v", p.cfg)
	}
}

func TestNewIdentityAndSpoolCleanup(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "spool", "deadbeef-123.part")
	keep := filepath.Join(dir, "spool", "notes.txt")
	ready := filepath.Join(dir, "xorbs", "ab", "cd", "ef")
	for _, f := range []string{stale, keep, ready} {
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	a, err := New(Config{Advertise: "http://seed:8080", Gateway: "http://gw", CacheDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale spool file kept: %v", err)
	}
	for _, f := range []string{keep, ready} {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("New removed %s: %v", f, err)
		}
	}
	b, err := New(Config{Advertise: "http://seed:8080/", Gateway: "http://gw2", CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if a.id == "" || a.id != b.id {
		t.Fatalf("id not stable for the same advertise: %q vs %q", a.id, b.id)
	}
	c, err := New(Config{Advertise: "http://other:8080", Gateway: "http://gw", CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if c.id == a.id {
		t.Fatalf("different advertise shares id %q", a.id)
	}
	if a.epoch == "" || a.epoch == b.epoch {
		t.Fatalf("epoch must be random per start: %q vs %q", a.epoch, b.epoch)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestWarmServe(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 1, 4, 16<<10)
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("warm object reached the gateway")
		w.WriteHeader(http.StatusInternalServerError)
	})
	p, srv := newTestPeer(t, g.URL, nil)
	commitDirect(t, p, hash, body)
	u := xorbURL(srv, hash)
	etag := `"` + hash.String() + `"`

	resp, got, err := do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if resp.Header.Get("ETag") != etag || resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("headers = %v", resp.Header)
	}
	resp, got, _ = do(t, http.MethodHead, u)
	if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(body)) || len(got) != 0 {
		t.Fatalf("HEAD = %d, length %d, %d body bytes", resp.StatusCode, resp.ContentLength, len(got))
	}
	resp, got, _ = do(t, http.MethodGet, u, "Range", "bytes=10-19")
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[10:20]) {
		t.Fatalf("single range = %d, %q", resp.StatusCode, got)
	}
	if cr := resp.Header.Get("Content-Range"); cr != fmt.Sprintf("bytes 10-19/%d", len(body)) {
		t.Fatalf("Content-Range = %q", cr)
	}
	resp, got, _ = do(t, http.MethodGet, u, "Range", "bytes=-5")
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[len(body)-5:]) {
		t.Fatalf("suffix range = %d, %d bytes", resp.StatusCode, len(got))
	}
	resp, got, _ = do(t, http.MethodGet, u, "Range", "bytes=0-1,4-5")
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || resp.StatusCode != http.StatusPartialContent || mediaType != "multipart/byteranges" {
		t.Fatalf("multi range = %d %q %v", resp.StatusCode, resp.Header.Get("Content-Type"), err)
	}
	mr := multipart.NewReader(bytes.NewReader(got), params["boundary"])
	for _, want := range [][]byte{body[0:2], body[4:6]} {
		part, err := mr.NextPart()
		if err != nil {
			t.Fatal(err)
		}
		if data, _ := io.ReadAll(part); !bytes.Equal(data, want) {
			t.Fatalf("part = %q, want %q", data, want)
		}
	}
	resp, _, _ = do(t, http.MethodGet, u, "If-None-Match", etag)
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match = %d", resp.StatusCode)
	}
	resp, _, _ = do(t, http.MethodGet, srv.URL+"/v1/xorbs/default/nothex")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad hash = %d", resp.StatusCode)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		if resp, _, _ := do(t, method, srv.URL+"/healthz"); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s /healthz = %d", method, resp.StatusCode)
		}
	}
	resp, _, _ = do(t, http.MethodPut, u)
	if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("PUT = %d", resp.StatusCode)
	}
}

// Like xet's CAS server, the route accepts any prefix segment and selects the xorb by hash alone.
func TestXorbRouteIgnoresPrefix(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 12, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("stored object reached the gateway")
		w.WriteHeader(http.StatusInternalServerError)
	})
	p, srv := newTestPeer(t, g.URL, nil)
	commitDirect(t, p, hash, body)
	resp, got, err := do(t, http.MethodGet, srv.URL+"/v1/xorbs/x/"+hash.String())
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET under a foreign prefix = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
}

type result struct {
	resp *http.Response
	body []byte
	err  error
}

func doAsync(t *testing.T, url string, hdr ...string) <-chan result {
	t.Helper()
	out := make(chan result, 1)
	go func() {
		resp, body, err := do(t, http.MethodGet, url, hdr...)
		out <- result{resp, body, err}
	}()
	return out
}

func TestGatedPullServesPrefixBeforeUpstreamCompletes(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 2, 4, 16<<10)
	release := make(chan struct{})
	const prefix = 20 << 10
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, prefix, release))
	p, srv := newTestPeer(t, g.URL, nil)
	u := xorbURL(srv, hash)

	resp, got, err := do(t, http.MethodGet, u, "Range", "bytes=0-1023")
	if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[:1024]) {
		t.Fatalf("prefix range while gated = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	resp, _, _ = do(t, http.MethodHead, u)
	if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(body)) {
		t.Fatalf("HEAD while gated = %d, length %d", resp.StatusCode, resp.ContentLength)
	}
	late := doAsync(t, u, "Range", fmt.Sprintf("bytes=%d-%d", prefix, prefix))
	// The last flushed byte proves the peer has consumed all the gateway sent before release.
	resp, got, err = do(t, http.MethodGet, u, "Range", fmt.Sprintf("bytes=%d-%d", prefix-1, prefix-1))
	if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[prefix-1:prefix]) {
		t.Fatalf("last flushed byte while gated = %d, %q, %v", resp.StatusCode, got, err)
	}
	select {
	case r := <-late:
		t.Fatalf("range past the flushed prefix returned %d before release", r.resp.StatusCode)
	default:
	}
	if clustertest.HasXorb(t, p.store, hash) {
		t.Fatal("xorb published before upstream finished")
	}
	close(release)
	if r := <-late; r.err != nil || r.resp.StatusCode != http.StatusPartialContent || !bytes.Equal(r.body, body[prefix:prefix+1]) {
		t.Fatalf("late range = %d, %q, %v", r.resp.StatusCode, r.body, r.err)
	}
	resp, got, err = do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) || resp.ContentLength != int64(len(body)) {
		t.Fatalf("whole GET = %d, %d bytes, length %d, %v", resp.StatusCode, len(got), resp.ContentLength, err)
	}
	settled(t, p)
	resp, got, _ = do(t, http.MethodGet, u, apis.PeerHeader, "gw")
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("relay GET after commit = %d", resp.StatusCode)
	}
	if n := len(g.Requests()); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
	h := g.Requests()[0].Header
	if h.Get(apis.PeerHeader) != p.id || h.Get("Authorization") != "" || h.Get("Range") != "" {
		t.Fatalf("upstream request headers = %v", h)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
}

// Readers are not held back for validation: a mismatching body reaches the client as a clean 200, only the pull fails.
func TestStreamDoesNotWaitForValidation(t *testing.T) {
	hash, _ := clustertest.MakeXorb(t, 13, 4, 16<<10)
	_, served := clustertest.MakeXorb(t, 14, 4, 16<<10)
	release := make(chan struct{})
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(served, 4<<10, release))
	p, srv := newTestPeer(t, g.URL, nil)
	u := xorbURL(srv, hash)

	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	// The client is streaming before the pull can fail validation.
	close(release)
	got, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, served) {
		t.Fatalf("stream of a body failing validation = %d, %d of %d bytes, %v, want 200 with every byte", resp.StatusCode, len(got), len(served), err)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	if clustertest.HasXorb(t, p.store, hash) {
		t.Fatal("xorb failing validation published")
	}
	p.mu.Lock()
	_, cooling := p.cooldown[hash]
	p.mu.Unlock()
	if !cooling {
		t.Fatal("failed pull left no cooldown")
	}
	if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway || len(g.Requests()) != 1 {
		t.Fatalf("GET in cooldown = %d, gateway GETs = %d, want 502 and 1", resp.StatusCode, len(g.Requests()))
	}
	pr, err := http.Post(srv.URL+apis.PullPath, "application/json", strings.NewReader(`{"hash":"`+hash.String()+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = pr.Body.Close()
	if pr.StatusCode != http.StatusAccepted {
		t.Fatalf("pull in cooldown = %d, want 202", pr.StatusCode)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "forced pull exit", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.pulls) == 0
	})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	if n := len(g.Requests()); n != 2 {
		t.Fatalf("gateway GETs after the forced pull = %d, want 2", n)
	}
}

func TestConcurrentColdRequestsShareOnePull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 3, 4, 16<<10)
	release := make(chan struct{})
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, 4<<10, release))
	_, srv := newTestPeer(t, g.URL, nil)
	u := xorbURL(srv, hash)

	const n = 10
	resps := make([]*http.Response, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			resp, err := http.Get(u)
			if err != nil {
				t.Error(err)
				return
			}
			resps[i] = resp
		})
	}
	wg.Wait()
	if got := len(g.Requests()); got != 1 {
		t.Fatalf("gateway GETs = %d, want 1", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	dropped, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := io.ReadAll(dropped.Body); err == nil {
		t.Fatal("canceled waiter received a complete body")
	}
	close(release)
	for _, resp := range resps {
		got, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
			t.Fatalf("shared waiter = %d, %d bytes, %v", resp.StatusCode, len(got), err)
		}
	}
	if got := len(g.Requests()); got != 1 {
		t.Fatalf("gateway GETs = %d, want 1", got)
	}
}

func TestUpstreamFailures(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 4, 2, 8<<10)
	other, _ := clustertest.MakeXorb(t, 5, 2, 8<<10)
	status := func(code int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
	}
	t.Run("not found", func(t *testing.T) {
		g := clustertest.NewFakeGateway(t, status(http.StatusNotFound))
		_, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
		if resp, _, _ := do(t, http.MethodHead, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("HEAD in cooldown = %d", resp.StatusCode)
		}
		if n := len(g.Requests()); n != 1 {
			t.Fatalf("gateway GETs = %d, want 1 (cooldown)", n)
		}
	})
	t.Run("upstream error", func(t *testing.T) {
		g := clustertest.NewFakeGateway(t, status(http.StatusInternalServerError))
		_, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
	})
	t.Run("oversized", func(t *testing.T) {
		g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(129<<20))
			w.WriteHeader(http.StatusOK)
		})
		_, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
		if n := len(g.Requests()); n != 1 {
			t.Fatalf("gateway GETs = %d, want 1", n)
		}
	})
	t.Run("empty body", func(t *testing.T) {
		g := clustertest.NewFakeGateway(t, clustertest.ServeBody(nil))
		p, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode == http.StatusOK {
			t.Fatal("empty body served as 200")
		}
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if clustertest.HasXorb(t, p.store, hash) {
			t.Fatal("empty xorb published")
		}
		p.mu.Lock()
		err := p.cooldown[hash].err
		p.mu.Unlock()
		if !errors.Is(err, errEmpty) {
			t.Fatalf("cooldown error = %v, want %v", err, errEmpty)
		}
	})
	t.Run("empty chunked body", func(t *testing.T) {
		g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			// An early flush commits to chunked encoding, so the peer sees no Content-Length.
			w.(http.Flusher).Flush()
		})
		p, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode == http.StatusOK {
			t.Fatal("empty chunked body served as 200")
		}
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if clustertest.HasXorb(t, p.store, hash) {
			t.Fatal("empty xorb published")
		}
		p.mu.Lock()
		err := p.cooldown[hash].err
		p.mu.Unlock()
		if !errors.Is(err, errEmpty) {
			t.Fatalf("cooldown error = %v, want %v", err, errEmpty)
		}
	})
	t.Run("hash mismatch", func(t *testing.T) {
		g := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
		p, srv := newTestPeer(t, g.URL, nil)
		u := xorbURL(srv, other)
		_, _, _ = do(t, http.MethodGet, u)
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if clustertest.HasXorb(t, p.store, other) {
			t.Fatal("invalid xorb published")
		}
		if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET in cooldown = %d", resp.StatusCode)
		}
		if n := len(g.Requests()); n != 1 {
			t.Fatalf("gateway GETs = %d, want 1 (cooldown)", n)
		}
		if ok, _ := filepath.Glob(filepath.Join(p.cfg.CacheDir, "xorbs", "*", "*", ".xorb-*")); len(ok) != 0 {
			t.Fatalf("storage temp files left: %v", ok)
		}
	})
	t.Run("short body", func(t *testing.T) {
		release := make(chan struct{})
		g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body[:4<<10])
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		p, srv := newTestPeer(t, g.URL, nil)
		u := xorbURL(srv, hash)
		resp, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		// The upstream ends short only once this client is streaming.
		close(release)
		if got, err := io.ReadAll(resp.Body); err == nil {
			t.Fatalf("short body ended cleanly = %d, %d bytes, want a read error", resp.StatusCode, len(got))
		}
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if clustertest.HasXorb(t, p.store, hash) {
			t.Fatal("short xorb published")
		}
		if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway || len(g.Requests()) != 1 {
			t.Fatalf("GET in cooldown = %d, gateway GETs = %d, want 502 and 1", resp.StatusCode, len(g.Requests()))
		}
	})
}

func TestPullAndLocalOnlyRelay(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 6, 4, 16<<10)
	release := make(chan struct{})
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, 4<<10, release))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Secret = "s3cret" })
	u := xorbURL(srv, hash)
	pull := func(auth, payload string) int {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+apis.PullPath, strings.NewReader(payload))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	payload := fmt.Sprintf(`{"hash":"%s"}`, hash)

	if resp, _, _ := do(t, http.MethodGet, u, apis.PeerHeader, "gw"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("relay miss = %d", resp.StatusCode)
	}
	if code := pull("", payload); code != http.StatusUnauthorized {
		t.Fatalf("pull without secret = %d", code)
	}
	if code := pull("Bearer wrong", payload); code != http.StatusUnauthorized {
		t.Fatalf("pull with wrong secret = %d", code)
	}
	if code := pull("Bearer s3cret", `{"hash":"zz"}`); code != http.StatusBadRequest {
		t.Fatalf("pull with bad hash = %d", code)
	}
	if code := pull("Bearer s3cret", payload+`{}`); code != http.StatusBadRequest {
		t.Fatalf("pull with trailing data = %d", code)
	}
	if code := pull("Bearer s3cret", payload+strings.Repeat(" ", maxPullBytes+1-len(payload))); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("pull over the body limit = %d, want 413", code)
	}
	if n := len(g.Requests()); n != 0 {
		t.Fatalf("gateway GETs before any accepted pull = %d", n)
	}
	if code := pull("Bearer s3cret", payload); code != http.StatusAccepted {
		t.Fatalf("pull = %d", code)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(g.Requests()) == 1 })
	if resp, _, _ := do(t, http.MethodGet, u, apis.PeerHeader, "gw"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("relay miss while pulling = %d", resp.StatusCode)
	}
	if code := pull("Bearer s3cret", payload); code != http.StatusAccepted {
		t.Fatalf("repeated pull = %d", code)
	}
	close(release)
	resp, got, err := do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("public GET sharing the pull = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	settled(t, p)
	if resp, _, _ := do(t, http.MethodGet, u, apis.PeerHeader, "gw"); resp.StatusCode != http.StatusOK {
		t.Fatalf("relay hit = %d", resp.StatusCode)
	}
	if code := pull("Bearer s3cret", payload); code != http.StatusAccepted {
		t.Fatalf("pull for a complete xorb = %d", code)
	}
	if n := len(g.Requests()); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}

func TestUnknownContentLengthWaitsForValidation(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 7, 4, 16<<10)
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body[:100])
		w.(http.Flusher).Flush()
		_, _ = w.Write(body[100:])
	})
	_, srv := newTestPeer(t, g.URL, nil)
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) || resp.ContentLength != int64(len(body)) {
		t.Fatalf("GET = %d, %d bytes, length %d, %v", resp.StatusCode, len(got), resp.ContentLength, err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+apis.PullPath, strings.NewReader(fmt.Sprintf(`{"hash":"%s"}`, hash)))
	pr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = pr.Body.Close()
	if pr.StatusCode != http.StatusAccepted || len(g.Requests()) != 1 {
		t.Fatalf("pull without secret = %d, gateway GETs = %d", pr.StatusCode, len(g.Requests()))
	}
}

func TestCloseCancelsPullAndReaders(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 8, 4, 16<<10)
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, 4<<10, make(chan struct{})))
	p, srv := newTestPeer(t, g.URL, nil)
	u := xorbURL(srv, hash)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not drain the blocked pull")
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("reader completed after Close")
	}
	if clustertest.HasXorb(t, p.store, hash) {
		t.Fatal("canceled pull published the xorb")
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold GET after Close = %d", resp.StatusCode)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireAfterCommitReportsReady(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 9, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
	p, srv := newTestPeer(t, g.URL, nil)
	if resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash)); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("cold GET = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull exit", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.pulls) == 0
	})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	// A handler that missed the store before that commit now takes its pull decision.
	f, rd, err := p.acquire(hash, false)
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		f.release()
		t.Fatal("acquire started a redundant pull for a committed xorb")
	}
	if rd == nil || rd.Key != hash {
		t.Fatalf("acquire did not reserve the committed xorb: %+v", rd)
	}
	p.unpin(rd)
	if n := len(g.Requests()); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}

func TestDeduplicatedSpoolStillValidates(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 10, 4, 16<<10)
	corrupt := bytes.Clone(body)
	corrupt[len(corrupt)/2] ^= 0xff
	release := make(chan struct{})
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(corrupt, 4<<10, release))
	p, srv := newTestPeer(t, g.URL, nil)
	u := xorbURL(srv, hash)

	late := doAsync(t, u)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(g.Requests()) == 1 })
	// A valid copy is committed underneath the running pull, so its PutXorb deduplicates.
	commitDirect(t, p, hash, body)
	close(release)
	<-late
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	p.mu.Lock()
	_, cooling := p.cooldown[hash]
	p.mu.Unlock()
	if !cooling {
		t.Fatal("corrupt pull passed on the deduplicated copy: no cooldown")
	}
	if !clustertest.HasXorb(t, p.store, hash) {
		t.Fatal("failed pull removed the committed copy")
	}
	resp, got, err := do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("committed xorb after the failed pull = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := len(g.Requests()); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}

func TestDeadClientReleasesReaderAndKeepsPull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 11, 4, 16<<10)
	release := make(chan struct{})
	const prefix = 4 << 10
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, prefix, release))
	p, srv := newTestPeer(t, g.URL, nil)
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, xorbURL(srv, hash), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The first part is long enough to flush the headers; the second blocks on upstream.
	req.Header.Set("Range", fmt.Sprintf("bytes=0-1023,%d-%d", prefix, prefix))
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	held := p.pulls[hash] != nil && p.pulls[hash].refs == 2
	p.mu.Unlock()
	if resp.StatusCode != http.StatusPartialContent || !held {
		t.Fatalf("multi range = %d, streaming client holds a pull reference %v, want 206 and true", resp.StatusCode, held)
	}
	_ = conn.Close()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "dead client release", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.pulls[hash] != nil && p.pulls[hash].refs == 1
	})
	close(release)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "commit", func() bool { return clustertest.HasXorb(t, p.store, hash) })
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET after the dead client = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := len(g.Requests()); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}
