package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
)

// makeXorb packs deterministic incompressible chunks with the real encoder.
func makeXorb(t *testing.T, seed byte, chunks, chunkSize int) (xet.XorbHash, []byte) {
	t.Helper()
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, true)
	rng := rand.NewChaCha8([32]byte{seed})
	chunk := make([]byte, chunkSize)
	for range chunks {
		if _, err := rng.Read(chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := enc.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return enc.SummoryHash(), buf.Bytes()
}

type announced struct {
	req  apis.AnnounceRequest
	hdr  http.Header
	code int // 0 while the reply is still pending
}

// gateway is a fake upstream that counts data requests, keeps the last request headers and records announces in order.
type gateway struct {
	*httptest.Server
	gets    atomic.Int32
	headers atomic.Pointer[http.Header]

	mu        sync.Mutex
	announces []announced
	reply     func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse)
}

func newGateway(t *testing.T, handler http.HandlerFunc) *gateway {
	g := &gateway{}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == apis.AnnouncePath {
			g.serveAnnounce(w, r)
			return
		}
		g.gets.Add(1)
		h := r.Header.Clone()
		g.headers.Store(&h)
		handler(w, r)
	}))
	t.Cleanup(g.Close)
	return g
}

// serveAnnounce records the request after draining it, then answers through the reply hook, which may block.
func (g *gateway) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	var req apis.AnnounceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g.mu.Lock()
	i := len(g.announces)
	g.announces = append(g.announces, announced{req: req, hdr: r.Header.Clone()})
	reply := g.reply
	g.mu.Unlock()
	code, resp := http.StatusOK, apis.AnnounceResponse{TTL: 30}
	if reply != nil {
		code, resp = reply(w, r, req)
	}
	if r.Context().Err() != nil {
		return
	}
	g.mu.Lock()
	g.announces[i].code = code
	g.mu.Unlock()
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func (g *gateway) setReply(fn func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse)) {
	g.mu.Lock()
	g.reply = fn
	g.mu.Unlock()
}

func (g *gateway) announceCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.announces)
}

// announce waits for the i-th recorded announce.
func (g *gateway) announce(t *testing.T, i int) announced {
	t.Helper()
	waitFor(t, fmt.Sprintf("announce %d", i), func() bool { return g.announceCount() > i })
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.announces[i]
}

// findAnnounce waits for the first announce at or after index from that matches.
func (g *gateway) findAnnounce(t *testing.T, from int, match func(announced) bool) (announced, int) {
	t.Helper()
	var found announced
	idx := -1
	waitFor(t, "matching announce", func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		for i := from; i < len(g.announces); i++ {
			if match(g.announces[i]) {
				found, idx = g.announces[i], i
				return true
			}
		}
		return false
	})
	return found, idx
}

func hashes(hs ...xet.XorbHash) []string {
	out := make([]string, 0, len(hs))
	for _, h := range hs {
		out = append(out, h.String())
	}
	slices.Sort(out)
	return out
}

// hasAdd reports whether the announce adds hash.
func hasAdd(a announced, hash xet.XorbHash) bool {
	return slices.Contains(a.req.Add, hash.String())
}

// hasRemove reports whether the announce removes hash.
func hasRemove(a announced, hash xet.XorbHash) bool {
	return slices.Contains(a.req.Remove, hash.String())
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

// commitDirect stores body underneath a running peer and registers it as a finished fill would.
func commitDirect(t *testing.T, p *Peer, hash xet.XorbHash, body []byte) {
	t.Helper()
	if _, err := p.store.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.admit(hash, int64(len(body)), nil)
	p.mu.Unlock()
}

func serveBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}
}

// gatedBody flushes body[:prefix], then holds the rest until release or client disconnect.
func gatedBody(body []byte, prefix int, release <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:prefix])
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body[prefix:])
	}
}

func newTestPeer(t *testing.T, gw string, mod func(*Config)) (*Peer, *httptest.Server) {
	t.Helper()
	cfg := Config{Advertise: "http://seed.test", Gateway: gw, Dir: t.TempDir()}
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

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func spoolEmpty(p *Peer) func() bool {
	return func() bool {
		left, _ := filepath.Glob(filepath.Join(p.spoolDir, "*"))
		return len(left) == 0
	}
}

func hasXorb(t *testing.T, p *Peer, hash xet.XorbHash) bool {
	t.Helper()
	ok, err := p.store.HasXorb(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestStartupFullAnnouncesSeededInventory(t *testing.T) {
	t.Setenv("HF_TOKEN", "hf_must_not_leak")
	a, bodyA := makeXorb(t, 20, 2, 8<<10)
	b, bodyB := makeXorb(t, 21, 2, 8<<10)
	dir := t.TempDir()
	seedStore(t, dir, a, bodyA)
	seedStore(t, dir, b, bodyB)
	g := newGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("seeded object reached the gateway")
		w.WriteHeader(http.StatusInternalServerError)
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) {
		c.Dir, c.Heartbeat, c.Secret = dir, time.Hour, "s3cret"
	})
	first := g.announce(t, 0)
	want := apis.AnnounceRequest{Peer: apis.Peer{ID: p.id, URL: "http://seed.test"}, Epoch: p.epoch, Full: true, Add: hashes(a, b)}
	if got := first.req; !slices.Equal(got.Add, want.Add) || got.Peer != want.Peer || got.Epoch != want.Epoch || !got.Full || len(got.Remove) != 0 {
		t.Fatalf("startup announce = %+v, want %+v", got, want)
	}
	if first.hdr.Get("Authorization") != "Bearer s3cret" || first.hdr.Get("Content-Type") != "application/json" || first.hdr.Get(apis.PeerHeader) != "" {
		t.Fatalf("announce headers = %v", first.hdr)
	}
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, a))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, bodyA) {
		t.Fatalf("GET seeded = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := g.announceCount(); n != 1 {
		t.Fatalf("announces after startup = %d, want 1", n)
	}
}

func TestConfigValidation(t *testing.T) {
	base := Config{Advertise: "http://seed:8080/", Gateway: "https://gw.example/", Dir: t.TempDir()}
	bad := map[string]func(*Config){
		"missing dir":           func(c *Config) { c.Dir = "" },
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
		"negative max fills":    func(c *Config) { c.MaxFills = -1 },
		"negative timeout":      func(c *Config) { c.FillTimeout = -time.Second },
		"negative heartbeat":    func(c *Config) { c.Heartbeat = -time.Second },
		"negative cache size":   func(c *Config) { c.CacheSize = -1 },
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
	if p.cfg.MaxFills != 8 || p.cfg.FillTimeout != 10*time.Minute || p.cfg.Heartbeat != 10*time.Second {
		t.Fatalf("defaults = %+v", p.cfg)
	}
}

func TestNormalizeOrigin(t *testing.T) {
	good := map[string]string{
		"http://h":             "http://h",
		"https://h:65535/":     "https://h:65535",
		"http://127.0.0.1:1":   "http://127.0.0.1:1",
		"http://[::1]":         "http://[::1]",
		"http://[::1]:8080":    "http://[::1]:8080",
		"http://gw.test:08080": "http://gw.test:08080",
	}
	for raw, want := range good {
		if got, err := normalizeOrigin(raw); err != nil || got != want {
			t.Errorf("normalizeOrigin(%q) = %q, %v, want %q", raw, got, err, want)
		}
	}
	bad := []string{"", "h", "//h", "ftp://h", "http:h", "http://", "http://:1", "http://[]:1", "http://h:", "http://[::1]:", "http://h:0", "http://h:65536", "http://h:1x", "http://h/p", "http://h?q", "http://h#f", "http://bob:hunter2@h", "http://bob:hunter2@h/%zz", "hunter2://h"}
	for _, raw := range bad {
		got, err := normalizeOrigin(raw)
		if err == nil {
			t.Errorf("normalizeOrigin(%q) = %q, want error", raw, got)
		} else if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "@") {
			t.Errorf("normalizeOrigin(%q) error echoes the input: %v", raw, err)
		}
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
	a, err := New(Config{Advertise: "http://seed:8080", Gateway: "http://gw", Dir: dir})
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
	b, err := New(Config{Advertise: "http://seed:8080/", Gateway: "http://gw2", Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if a.id == "" || a.id != b.id {
		t.Fatalf("id not stable for the same advertise: %q vs %q", a.id, b.id)
	}
	c, err := New(Config{Advertise: "http://other:8080", Gateway: "http://gw", Dir: t.TempDir()})
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
	hash, body := makeXorb(t, 1, 4, 16<<10)
	g := newGateway(t, func(w http.ResponseWriter, _ *http.Request) {
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
	resp, _, _ = do(t, http.MethodGet, srv.URL+"/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	resp, _, _ = do(t, http.MethodPut, u)
	if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("PUT = %d", resp.StatusCode)
	}
}

// Like xet's CAS server, the route accepts any prefix segment and selects the xorb by hash alone.
func TestXorbRouteIgnoresPrefix(t *testing.T) {
	hash, body := makeXorb(t, 12, 2, 8<<10)
	g := newGateway(t, func(w http.ResponseWriter, _ *http.Request) {
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

func TestGatedFillServesPrefixBeforeUpstreamCompletes(t *testing.T) {
	hash, body := makeXorb(t, 2, 4, 16<<10)
	release := make(chan struct{})
	const prefix = 20 << 10
	g := newGateway(t, gatedBody(body, prefix, release))
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
	select {
	case r := <-late:
		t.Fatalf("range past the flushed prefix returned %d before release", r.resp.StatusCode)
	case <-time.After(100 * time.Millisecond):
	}
	if hasXorb(t, p, hash) {
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
	waitFor(t, "commit", func() bool { return hasXorb(t, p, hash) })
	resp, got, _ = do(t, http.MethodGet, u, apis.PeerHeader, "gw")
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("relay GET after commit = %d", resp.StatusCode)
	}
	if n := g.gets.Load(); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
	h := *g.headers.Load()
	if h.Get(apis.PeerHeader) != p.id || h.Get("Authorization") != "" || h.Get("Range") != "" {
		t.Fatalf("upstream request headers = %v", h)
	}
	waitFor(t, "spool cleanup", spoolEmpty(p))
}

func TestConcurrentColdRequestsShareOneFill(t *testing.T) {
	hash, body := makeXorb(t, 3, 4, 16<<10)
	release := make(chan struct{})
	g := newGateway(t, gatedBody(body, 4<<10, release))
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
	if got := g.gets.Load(); got != 1 {
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
	if got := g.gets.Load(); got != 1 {
		t.Fatalf("gateway GETs = %d, want 1", got)
	}
}

func TestUpstreamFailures(t *testing.T) {
	hash, body := makeXorb(t, 4, 2, 8<<10)
	other, _ := makeXorb(t, 5, 2, 8<<10)
	status := func(code int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
	}
	t.Run("not found", func(t *testing.T) {
		g := newGateway(t, status(http.StatusNotFound))
		_, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
		if resp, _, _ := do(t, http.MethodHead, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("HEAD in cooldown = %d", resp.StatusCode)
		}
		if n := g.gets.Load(); n != 1 {
			t.Fatalf("gateway GETs = %d, want 1 (cooldown)", n)
		}
	})
	t.Run("upstream error", func(t *testing.T) {
		g := newGateway(t, status(http.StatusInternalServerError))
		_, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
	})
	t.Run("oversized", func(t *testing.T) {
		g := newGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(129<<20))
			w.WriteHeader(http.StatusOK)
		})
		_, srv := newTestPeer(t, g.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
	})
	t.Run("hash mismatch", func(t *testing.T) {
		g := newGateway(t, serveBody(body))
		p, srv := newTestPeer(t, g.URL, nil)
		u := xorbURL(srv, other)
		resp, got, err := do(t, http.MethodGet, u)
		if resp.StatusCode == http.StatusOK && err == nil {
			t.Fatalf("invalid body served as a clean %d bytes", len(got))
		}
		if hasXorb(t, p, other) {
			t.Fatal("invalid xorb published")
		}
		waitFor(t, "spool cleanup", spoolEmpty(p))
		if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET in cooldown = %d", resp.StatusCode)
		}
		if n := g.gets.Load(); n != 1 {
			t.Fatalf("gateway GETs = %d, want 1 (cooldown)", n)
		}
		if ok, _ := filepath.Glob(filepath.Join(p.cfg.Dir, "xorbs", "*", "*", ".xorb-*")); len(ok) != 0 {
			t.Fatalf("storage temp files left: %v", ok)
		}
	})
	t.Run("short body", func(t *testing.T) {
		g := newGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body[:len(body)-100])
		})
		p, srv := newTestPeer(t, g.URL, nil)
		resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
		if resp.StatusCode == http.StatusOK && err == nil {
			t.Fatalf("short body served as a clean %d bytes", len(got))
		}
		if hasXorb(t, p, hash) {
			t.Fatal("short xorb published")
		}
		waitFor(t, "spool cleanup", spoolEmpty(p))
	})
}

func TestPullAndLocalOnlyRelay(t *testing.T) {
	hash, body := makeXorb(t, 6, 4, 16<<10)
	release := make(chan struct{})
	g := newGateway(t, gatedBody(body, 4<<10, release))
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
	if n := g.gets.Load(); n != 0 {
		t.Fatalf("gateway GETs before any accepted pull = %d", n)
	}
	if code := pull("Bearer s3cret", payload); code != http.StatusAccepted {
		t.Fatalf("pull = %d", code)
	}
	waitFor(t, "fill start", func() bool { return g.gets.Load() == 1 })
	if resp, _, _ := do(t, http.MethodGet, u, apis.PeerHeader, "gw"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("relay miss while filling = %d", resp.StatusCode)
	}
	if code := pull("Bearer s3cret", payload); code != http.StatusAccepted {
		t.Fatalf("repeated pull = %d", code)
	}
	close(release)
	resp, got, err := do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("public GET sharing the pull fill = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	waitFor(t, "commit", func() bool { return hasXorb(t, p, hash) })
	if resp, _, _ := do(t, http.MethodGet, u, apis.PeerHeader, "gw"); resp.StatusCode != http.StatusOK {
		t.Fatalf("relay hit = %d", resp.StatusCode)
	}
	if code := pull("Bearer s3cret", payload); code != http.StatusAccepted {
		t.Fatalf("pull for a complete xorb = %d", code)
	}
	if n := g.gets.Load(); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}

func TestUnknownContentLengthWaitsForValidation(t *testing.T) {
	hash, body := makeXorb(t, 7, 4, 16<<10)
	g := newGateway(t, func(w http.ResponseWriter, _ *http.Request) {
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
	if pr.StatusCode != http.StatusAccepted || g.gets.Load() != 1 {
		t.Fatalf("pull without secret = %d, gateway GETs = %d", pr.StatusCode, g.gets.Load())
	}
}

func TestCloseCancelsFillAndReaders(t *testing.T) {
	hash, body := makeXorb(t, 8, 4, 16<<10)
	g := newGateway(t, gatedBody(body, 4<<10, make(chan struct{})))
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
		t.Fatal("Close did not drain the blocked fill")
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("reader completed after Close")
	}
	if hasXorb(t, p, hash) {
		t.Fatal("canceled fill published the xorb")
	}
	waitFor(t, "spool cleanup", spoolEmpty(p))
	if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold GET after Close = %d", resp.StatusCode)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireAfterCommitReportsReady(t *testing.T) {
	hash, body := makeXorb(t, 9, 2, 8<<10)
	g := newGateway(t, serveBody(body))
	p, srv := newTestPeer(t, g.URL, nil)
	if resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash)); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("cold GET = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	waitFor(t, "fill exit", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.fills) == 0
	})
	// A handler that missed the store before that commit now takes its fill decision.
	f, rd, err := p.acquire(hash, false)
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		f.release()
		t.Fatal("acquire started a redundant fill for a committed xorb")
	}
	if rd == nil || rd.hash != hash {
		t.Fatalf("acquire did not reserve the committed xorb: %+v", rd)
	}
	p.unpin(rd)
	if n := g.gets.Load(); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}

func TestDeduplicatedSpoolStillValidates(t *testing.T) {
	hash, body := makeXorb(t, 10, 4, 16<<10)
	corrupt := bytes.Clone(body)
	corrupt[len(corrupt)/2] ^= 0xff
	release := make(chan struct{})
	g := newGateway(t, gatedBody(corrupt, 4<<10, release))
	p, srv := newTestPeer(t, g.URL, nil)
	u := xorbURL(srv, hash)

	late := doAsync(t, u)
	waitFor(t, "fill start", func() bool { return g.gets.Load() == 1 })
	// A valid copy is committed underneath the running fill, so its PutXorb deduplicates.
	commitDirect(t, p, hash, body)
	close(release)
	r := <-late
	if r.err == nil && r.resp.StatusCode == http.StatusOK {
		t.Fatalf("corrupt spool served as a clean %d bytes, matches body: %v", len(r.body), bytes.Equal(r.body, body))
	}
	waitFor(t, "spool cleanup", spoolEmpty(p))
	resp, got, err := do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("committed xorb after the failed fill = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := g.gets.Load(); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}

// deadClient is a ResponseWriter whose connection breaks after writes successful writes.
type deadClient struct {
	hdr    http.Header
	code   int
	writes int
}

func (d *deadClient) Header() http.Header  { return d.hdr }
func (d *deadClient) WriteHeader(code int) { d.code = code }
func (d *deadClient) Write(p []byte) (int, error) {
	if d.writes == 0 {
		return 0, syscall.EPIPE
	}
	d.writes--
	return len(p), nil
}

// blockedReaders counts goroutines parked inside spoolReader.Read.
func blockedReaders() int {
	buf := make([]byte, 1<<20)
	n := 0
	for g := range strings.SplitSeq(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
		if strings.Contains(g, "[select]") && strings.Contains(g, "(*spoolReader).Read(") {
			n++
		}
	}
	return n
}

func TestDeadClientMultipartReleasesReaderAndKeepsFill(t *testing.T) {
	hash, body := makeXorb(t, 11, 4, 16<<10)
	release := make(chan struct{})
	const prefix = 4 << 10
	g := newGateway(t, gatedBody(body, prefix, release))
	p, srv := newTestPeer(t, g.URL, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash.String(), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=0-1,%d-%d", prefix, prefix))
	// The connection breaks on the second part boundary while that part is blocked on upstream.
	w := &deadClient{hdr: http.Header{}, writes: 2}
	func() {
		defer func() {
			if r := recover(); r != nil && r != http.ErrAbortHandler {
				panic(r)
			}
		}()
		p.ServeHTTP(w, req)
	}()
	if w.code != http.StatusPartialContent {
		t.Fatalf("status = %d", w.code)
	}
	settle := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(settle) {
		if n := blockedReaders(); n != 0 {
			t.Fatalf("%d multipart reader(s) still blocked after the handler returned", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	waitFor(t, "commit", func() bool { return hasXorb(t, p, hash) })
	waitFor(t, "spool cleanup", spoolEmpty(p))
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET after the dead client = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := g.gets.Load(); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
}
