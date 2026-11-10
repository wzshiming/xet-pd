package peer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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
	store, err := local.NewStorage(local.WithBasePath(filepath.Join(dir, "data")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

func xorbPath(dir string, hash xet.XorbHash) string {
	return filepath.Join(dir, "data", filepath.FromSlash(storage.ObjectKey("xorbs", hash.String())))
}

// cluster starts a tracker and a peer it names as the holder of every xorb in bodies, each served whole.
func cluster(t *testing.T, bodies map[xet.XorbHash][]byte) (*clustertest.FakeTracker, *clustertest.FakePeer) {
	t.Helper()
	tracker, holder := clustertest.NewFakeTracker(t), clustertest.NewFakePeer(t, "holder")
	for hash, body := range bodies {
		holder.ServeXorb(hash, body)
		tracker.Hold(holder.Peer(), hash)
	}
	return tracker, holder
}

func newTestPeer(t *testing.T, tracker string, mod func(*Config)) (*Peer, *httptest.Server) {
	t.Helper()
	cfg := Config{Advertise: "http://seed.test", Tracker: tracker, Upstream: "http://upstream.test", DataDir: t.TempDir()}
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

// awaitSamples waits until every sample in want reads its value on the /metrics of srv.
func awaitSamples(t *testing.T, srv *httptest.Server, want map[string]float64) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("samples %v", want), func() bool {
		got := clustertest.Scrape(t, srv.URL)
		for key, v := range want {
			if got[key] != v {
				return false
			}
		}
		return true
	})
}

func TestStartupFullAnnouncesSeededInventory(t *testing.T) {
	t.Setenv("HF_TOKEN", "hf_must_not_leak")
	a, bodyA := clustertest.MakeXorb(t, 20, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 21, 2, 8<<10)
	dir := t.TempDir()
	seedStore(t, dir, a, bodyA)
	seedStore(t, dir, b, bodyB)
	tracker := clustertest.NewFakeTracker(t)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) {
		c.DataDir, c.Heartbeat, c.Secret = dir, time.Hour, "s3cret"
	})
	first := tracker.Announce(t, 0)
	want := apis.AnnounceRequest{Peer: apis.Peer{ID: peerID(p), URL: "http://seed.test", Upstream: true}, Epoch: peerEpoch(p), Full: true, Add: hashes(a, b)}
	if got := first.Req; !slices.Equal(got.Add, want.Add) || got.Peer != want.Peer || got.Epoch != want.Epoch || !got.Full || len(got.Remove) != 0 {
		t.Fatalf("startup announce = %+v, want %+v", got, want)
	}
	if first.Header.Get("Authorization") != "Bearer s3cret" || first.Header.Get("Content-Type") != "application/json" || first.Header.Get(apis.PeerHeader) != "" {
		t.Fatalf("announce headers = %v", first.Header)
	}
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, a))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, bodyA) {
		t.Fatalf("GET seeded = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := len(tracker.Announces()); n != 1 {
		t.Fatalf("announces after startup = %d, want 1", n)
	}
	if n := len(tracker.Lookups()); n != 0 {
		t.Fatalf("lookups for a seeded object = %d, want 0", n)
	}
}

func TestConfigValidation(t *testing.T) {
	base := Config{Advertise: "http://seed:8080/", Tracker: "https://tracker.example/", Upstream: "https://hub.example/", DataDir: t.TempDir()}
	bad := map[string]func(*Config){
		"missing data dir":      func(c *Config) { c.DataDir = "" },
		"tracker userinfo":      func(c *Config) { c.Tracker = "http://u:p@tracker" },
		"tracker query":         func(c *Config) { c.Tracker = "http://tracker?x=1" },
		"tracker fragment":      func(c *Config) { c.Tracker = "http://tracker#f" },
		"tracker base path":     func(c *Config) { c.Tracker = "http://tracker/base" },
		"tracker scheme":        func(c *Config) { c.Tracker = "ftp://tracker" },
		"tracker no host":       func(c *Config) { c.Tracker = "http:///x" },
		"tracker empty host":    func(c *Config) { c.Tracker = "http://:1234" },
		"tracker port range":    func(c *Config) { c.Tracker = "http://tracker:65536" },
		"tracker credentials":   func(c *Config) { c.Tracker = "http://bob:hunter2@tracker/%zz" },
		"upstream base path":    func(c *Config) { c.Upstream = "http://hub/base" },
		"upstream scheme":       func(c *Config) { c.Upstream = "ftp://hub" },
		"upstream credentials":  func(c *Config) { c.Upstream = "http://bob:hunter2@hub/%zz" },
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
	cfg := configOf(p)
	if cfg.Advertise != "http://seed:8080" || cfg.Tracker != "https://tracker.example" || cfg.Upstream != "https://hub.example" {
		t.Fatalf("normalized URLs = %q %q %q", cfg.Advertise, cfg.Tracker, cfg.Upstream)
	}
	if cfg.MaxPulls != 8 || cfg.PullTimeout != 10*time.Minute || cfg.Heartbeat != 10*time.Second {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestConfigRequiresUpstream(t *testing.T) {
	p, err := New(Config{Advertise: "http://seed:8080", Tracker: "http://tracker", DataDir: t.TempDir()})
	if err == nil {
		_ = p.Close()
		t.Fatal("New accepted a config without an upstream")
	}
	if want := "upstream is required"; err.Error() != want {
		t.Fatalf("New error = %q, want %q", err, want)
	}
}

func TestNewIdentityAndSpoolCleanup(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "spool", "deadbeef-123.part")
	keep := filepath.Join(dir, "spool", "notes.txt")
	ready := filepath.Join(dir, "data", "xorbs", "ab", "cd", "ef")
	for _, f := range []string{stale, keep, ready} {
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	a, err := New(Config{Advertise: "http://seed:8080", Tracker: "http://tracker", Upstream: "http://hub", DataDir: dir})
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
	b, err := New(Config{Advertise: "http://seed:8080/", Tracker: "http://tracker2", Upstream: "http://hub", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if peerID(a) == "" || peerID(a) != peerID(b) {
		t.Fatalf("id not stable for the same advertise: %q vs %q", peerID(a), peerID(b))
	}
	c, err := New(Config{Advertise: "http://other:8080", Tracker: "http://tracker", Upstream: "http://hub", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if peerID(c) == peerID(a) {
		t.Fatalf("different advertise shares id %q", peerID(a))
	}
	if peerEpoch(a) == "" || peerEpoch(a) == peerEpoch(b) {
		t.Fatalf("epoch must be random per start: %q vs %q", peerEpoch(a), peerEpoch(b))
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
	tracker := clustertest.NewFakeTracker(t)
	p, srv := newTestPeer(t, tracker.URL, nil)
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
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT = %d, want 403", resp.StatusCode)
	}
	if n := len(tracker.Lookups()); n != 0 {
		t.Fatalf("lookups for a warm object = %d, want 0", n)
	}
}

// Like xet's CAS server, the route accepts any prefix segment and selects the xorb by hash alone.
func TestXorbRouteIgnoresPrefix(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 12, 2, 8<<10)
	tracker := clustertest.NewFakeTracker(t)
	p, srv := newTestPeer(t, tracker.URL, nil)
	commitDirect(t, p, hash, body)
	resp, got, err := do(t, http.MethodGet, srv.URL+"/v1/xorbs/x/"+hash.String())
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET under a foreign prefix = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := len(tracker.Lookups()); n != 0 {
		t.Fatalf("lookups for a stored object = %d, want 0", n)
	}
}

func TestRelayAndHeadNeverPull(t *testing.T) {
	held, body := clustertest.MakeXorb(t, 16, 2, 8<<10)
	unknown, _ := clustertest.MakeXorb(t, 17, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{held: body})
	p, srv := newTestPeer(t, tracker.URL, nil)
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, held), apis.PeerHeader, "other"); resp.StatusCode != http.StatusNotFound || len(tracker.Lookups()) != 0 {
		t.Fatalf("relay GET of a xorb held elsewhere = %d after %d lookups, want 404 and none", resp.StatusCode, len(tracker.Lookups()))
	}
	if resp, got, _ := do(t, http.MethodHead, xorbURL(srv, held)); resp.StatusCode != http.StatusOK || resp.ContentLength != -1 || len(got) != 0 {
		t.Fatalf("HEAD of a xorb held elsewhere = %d, length %d, want 200 without a length", resp.StatusCode, resp.ContentLength)
	}
	if resp, _, _ := do(t, http.MethodHead, xorbURL(srv, unknown)); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("HEAD of a xorb nobody holds = %d, want 404", resp.StatusCode)
	}
	if n := len(holder.Requests()); n != 0 || stored(t, p, held) {
		t.Fatalf("holder GETs = %d, stored %v; want no pull", n, stored(t, p, held))
	}
}

func TestShardRouteServesKnownFilesToHoldersOfTheSecret(t *testing.T) {
	tf := makeFile(t, 93, 94)
	p, srv := newTestPeer(t, clustertest.NewFakeTracker(t).URL, func(c *Config) { c.Secret, c.Heartbeat = testSecret, time.Hour })
	ingest(t, storeOf(p), tf)
	auth := []string{"Authorization", "Bearer " + testSecret, apis.PeerHeader, "other"}
	resp, body, err := do(t, http.MethodGet, srv.URL+apis.ShardPathPrefix+tf.file.Hash, auth...)
	var got apis.ShardResponse
	if err != nil || resp.StatusCode != http.StatusOK || json.Unmarshal(body, &got) != nil {
		t.Fatalf("GET shard = %d %q, %v; want the file's ShardResponse", resp.StatusCode, body, err)
	}
	if sh, err := storage.DecodeStoredShard(bytes.NewReader(got.Shard)); err != nil || sh.File(tf.hash) == nil {
		t.Fatalf("served shard decodes to %v, want the shard of %s", err, tf.hash)
	}
	if !maps.EqualFunc(got.Offsets, tf.resp.Offsets, slices.Equal[[]uint64]) {
		t.Fatalf("served offsets = %v, want %v", got.Offsets, tf.resp.Offsets)
	}
	for _, tt := range []struct {
		file string
		hdr  []string
		want int
	}{
		{tf.file.Hash, nil, http.StatusUnauthorized},
		{strings.Repeat("cd", 32), auth, http.StatusNotFound},
		{"nothex", auth, http.StatusBadRequest},
	} {
		if resp, body, _ := do(t, http.MethodGet, srv.URL+apis.ShardPathPrefix+tt.file, tt.hdr...); resp.StatusCode != tt.want {
			t.Fatalf("GET shard of %s with %q = %d %q, want %d", tt.file, tt.hdr, resp.StatusCode, body, tt.want)
		}
	}
	awaitSamples(t, srv, map[string]float64{
		`xetpd_peer_shard_gets_total{status="200"}`: 1, `xetpd_peer_shard_gets_total{status="401"}`: 1,
		`xetpd_peer_shard_gets_total{status="404"}`: 1, `xetpd_peer_shard_gets_total{status="400"}`: 1,
	})
}

func TestShardRouteErrorNamesNoLocalPath(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
	tf := makeFile(t, 95)
	gone := tf.xorbHashes()[0]
	p, srv := newTestPeer(t, clustertest.NewFakeTracker(t).URL, nil)
	ingest(t, storeOf(p), tf)
	deleteStored(t, p, gone)
	path := sidecarPath(storeOf(p), gone)
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	resp, body, _ := do(t, http.MethodGet, srv.URL+apis.ShardPathPrefix+tf.file.Hash)
	if resp.StatusCode != http.StatusBadGateway || strings.Contains(string(body), configOf(p).DataDir) {
		t.Fatalf("GET shard with an unreadable sidecar = %d %q, want 502 naming no local path", resp.StatusCode, body)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 53, 2, 8<<10)
	cold, coldBody := clustertest.MakeXorb(t, 54, 2, 8<<10)
	tracker, _ := cluster(t, map[xet.XorbHash][]byte{cold: coldBody})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat, c.Secret, c.XorbCacheSize = time.Hour, "s3cret", 1<<20 })
	resp, text, err := do(t, http.MethodGet, srv.URL+"/metrics")
	if err != nil || resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("GET /metrics without credentials = %d %q, %v, want 200 text/plain", resp.StatusCode, resp.Header.Get("Content-Type"), err)
	}
	for _, typ := range []string{
		"xorb_gets_total counter", "pulls_total counter", "announces_total counter", "restore_dropped_total counter", "shard_gets_total counter",
		"shards gauge", "xorb_cache_bytes gauge", "xorb_cache_entries gauge", "xorb_cache_limit_bytes gauge", "pulls_in_flight gauge", "cooldowns gauge",
	} {
		if !strings.Contains("\n"+string(text), "\n# TYPE xetpd_peer_"+typ+"\n") {
			t.Fatalf("GET /metrics lacks # TYPE xetpd_peer_%s", typ)
		}
	}
	m := clustertest.Scrape(t, srv.URL)
	for _, key := range []string{"go_goroutines", `xetpd_peer_announces_total{result="ok"}`, `xetpd_peer_announces_total{result="resync"}`, `xetpd_peer_announces_total{result="error"}`} {
		if _, ok := m[key]; !ok {
			t.Fatalf("GET /metrics lacks %s", key)
		}
	}
	for _, key := range []string{
		`xetpd_peer_xorb_gets_total{requester="client",status="200"}`, `xetpd_peer_xorb_gets_total{requester="client",status="206"}`, `xetpd_peer_xorb_gets_total{requester="client",status="404"}`,
		`xetpd_peer_xorb_gets_total{requester="peer",status="200"}`, `xetpd_peer_xorb_gets_total{requester="peer",status="206"}`, `xetpd_peer_xorb_gets_total{requester="peer",status="404"}`,
		`xetpd_peer_pulls_total{result="ok"}`, `xetpd_peer_pulls_total{result="not_found"}`, `xetpd_peer_pulls_total{result="canceled"}`, `xetpd_peer_pulls_total{result="timeout"}`, `xetpd_peer_pulls_total{result="error"}`,
		`xetpd_peer_shard_gets_total{status="200"}`, `xetpd_peer_shard_gets_total{status="404"}`,
		"xetpd_peer_restore_dropped_total", "xetpd_peer_shards", "xetpd_peer_xorb_cache_bytes", "xetpd_peer_xorb_cache_entries", "xetpd_peer_pulls_in_flight", "xetpd_peer_cooldowns",
	} {
		if v, ok := m[key]; !ok || v != 0 {
			t.Fatalf("fresh %s = %v (present %v), want a visible 0", key, v, ok)
		}
	}

	commitDirect(t, p, hash, body)
	if resp, _, _ := do(t, http.MethodHead, xorbURL(srv, hash)); resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD of a stored xorb = %d, want 200", resp.StatusCode)
	}
	// net/http sends a HEAD's header only once the handler returned, so a count would already show.
	for key, v := range clustertest.Scrape(t, srv.URL) {
		if strings.HasPrefix(key, "xetpd_peer_xorb_gets_total{") && v != 0 {
			t.Errorf("%s = %v after a HEAD, want 0: only GETs count", key, v)
		}
	}
	for _, req := range []struct {
		url    string
		hdr    []string
		status int
	}{
		{xorbURL(srv, hash), nil, http.StatusOK},
		{xorbURL(srv, hash), []string{"Range", "bytes=0-9"}, http.StatusPartialContent},
		{xorbURL(srv, cold), []string{apis.PeerHeader, "other"}, http.StatusNotFound},
		{srv.URL + "/v1/xorbs/default/nothex", nil, http.StatusBadRequest},
	} {
		if resp, _, _ := do(t, http.MethodGet, req.url, req.hdr...); resp.StatusCode != req.status {
			t.Fatalf("GET %s %q = %d, want %d", req.url, req.hdr, resp.StatusCode, req.status)
		}
	}
	awaitSamples(t, srv, map[string]float64{
		`xetpd_peer_xorb_gets_total{requester="client",status="200"}`: 1,
		`xetpd_peer_xorb_gets_total{requester="client",status="206"}`: 1,
		`xetpd_peer_xorb_gets_total{requester="peer",status="404"}`:   1,
		`xetpd_peer_xorb_gets_total{requester="client",status="400"}`: 1,
	})
	m = clustertest.Scrape(t, srv.URL)
	if m["xetpd_peer_xorb_cache_bytes"] != float64(usage(p)) || m["xetpd_peer_xorb_cache_entries"] != 1 || m["xetpd_peer_xorb_cache_limit_bytes"] != float64(configOf(p).XorbCacheSize) {
		t.Fatalf("cache bytes %v, entries %v, limit %v, want %d, 1 and %d", m["xetpd_peer_xorb_cache_bytes"], m["xetpd_peer_xorb_cache_entries"], m["xetpd_peer_xorb_cache_limit_bytes"], usage(p), configOf(p).XorbCacheSize)
	}
	pullAndSettle(t, p, srv, cold, coldBody)
	awaitSamples(t, srv, map[string]float64{`xetpd_peer_pulls_total{result="ok"}`: 1, "xetpd_peer_pulls_in_flight": 0})
	ingest(t, storeOf(p), makeFile(t, 55))
	awaitSamples(t, srv, map[string]float64{"xetpd_peer_shards": 1})
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

// streamClient bounds the wait for a response header that must arrive while the pull behind it is held.
var streamClient = &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: clustertest.WaitTimeout}}

func TestGatedPullServesPrefixBeforeUpstreamCompletes(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 2, 4, 16<<10)
	release := make(chan struct{})
	const prefix = 20 << 10
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	holder.GateXorb(hash, body, prefix, release)
	p, srv := newTestPeer(t, tracker.URL, nil)
	u := xorbURL(srv, hash)

	resp, got, err := do(t, http.MethodGet, u, "Range", "bytes=0-1023")
	if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[:1024]) {
		t.Fatalf("prefix range while gated = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	resp, _, _ = do(t, http.MethodHead, u)
	if resp.StatusCode != http.StatusOK || resp.ContentLength != -1 {
		t.Fatalf("HEAD while gated = %d, length %d, want 200 without a length", resp.StatusCode, resp.ContentLength)
	}
	late := doAsync(t, u, "Range", fmt.Sprintf("bytes=%d-%d", prefix, prefix))
	// The last flushed byte proves the peer has consumed all the holder sent before release.
	resp, got, err = do(t, http.MethodGet, u, "Range", fmt.Sprintf("bytes=%d-%d", prefix-1, prefix-1))
	if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[prefix-1:prefix]) {
		t.Fatalf("last flushed byte while gated = %d, %q, %v", resp.StatusCode, got, err)
	}
	select {
	case r := <-late:
		t.Fatalf("range past the flushed prefix returned %d before release", r.resp.StatusCode)
	default:
	}
	if stored(t, p, hash) {
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
	if n := len(holder.Requests()); n != 1 {
		t.Fatalf("holder GETs = %d, want 1", n)
	}
	h := holder.Requests()[0].Header
	if h.Get(apis.PeerHeader) != peerID(p) || h.Get("Authorization") != "" || h.Get("Range") != "" {
		t.Fatalf("upstream request headers = %v", h)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
}

// A client reads the first bytes of a xorb while its only holder still holds back the rest.
func TestServeWhilePulling(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 18, 4, 64<<10)
	const prefix = 100 << 10
	release := make(chan struct{})
	tracker, holder := cluster(t, nil)
	holder.GateXorb(hash, body, prefix, release)
	tracker.Hold(holder.Peer(), hash)
	p, srv := newTestPeer(t, tracker.URL, nil)
	resp, err := streamClient.Get(xorbURL(srv, hash))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	head := make([]byte, prefix/2)
	if _, err := io.ReadFull(resp.Body, head); err != nil || resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(body)) || !bytes.Equal(head, body[:len(head)]) {
		t.Fatalf("GET while the holder holds the rest = %d, length %d, first bytes %v", resp.StatusCode, resp.ContentLength, err)
	}
	if stored(t, p, hash) {
		t.Fatal("xorb committed before the holder sent the rest")
	}
	close(release)
	if rest, err := io.ReadAll(resp.Body); err != nil || !bytes.Equal(rest, body[len(head):]) {
		t.Fatalf("rest after release = %d bytes, %v, want %d", len(rest), err, len(body)-len(head))
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the pulled xorb committed", admitted(p, hash))
	if n := len(holder.Requests()); n != 1 {
		t.Fatalf("holder GETs = %d, want 1", n)
	}
}

// Readers are not held back for validation: a mismatching body reaches the client as a clean 200, only the pull fails.
func TestStreamDoesNotWaitForValidation(t *testing.T) {
	hash, _ := clustertest.MakeXorb(t, 13, 4, 16<<10)
	_, served := clustertest.MakeXorb(t, 14, 4, 16<<10)
	release := make(chan struct{})
	tracker, holder := cluster(t, nil)
	holder.GateXorb(hash, served, 4<<10, release)
	tracker.Hold(holder.Peer(), hash)
	p, srv := newTestPeer(t, tracker.URL, nil)
	u := xorbURL(srv, hash)

	resp, err := streamClient.Get(u)
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
	if stored(t, p, hash) {
		t.Fatal("xorb failing validation published")
	}
	_, cooling := cooldownOf(p, hash)
	if !cooling {
		t.Fatal("failed pull left no cooldown")
	}
	if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway || len(holder.Requests()) != 1 {
		t.Fatalf("GET in cooldown = %d, holder GETs = %d, want 502 and 1", resp.StatusCode, len(holder.Requests()))
	}
	forcePull(t, p, hash)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "forced pull exit", noPulls(p))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs after the forced pull = %d, want 2", n)
	}
}

func TestConcurrentColdRequestsShareOnePull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 3, 4, 16<<10)
	release := make(chan struct{})
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	holder.GateXorb(hash, body, 4<<10, release)
	_, srv := newTestPeer(t, tracker.URL, nil)
	u := xorbURL(srv, hash)

	const n = 10
	resps := make([]*http.Response, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			resp, err := streamClient.Get(u)
			if err != nil {
				t.Error(err)
				return
			}
			resps[i] = resp
		})
	}
	wg.Wait()
	if got := len(holder.Requests()); got != 1 {
		t.Fatalf("holder GETs = %d, want 1", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	dropped, err := streamClient.Do(req)
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
	if got := len(holder.Requests()); got != 1 {
		t.Fatalf("holder GETs = %d, want 1", got)
	}
}

func TestUpstreamFailures(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 4, 2, 8<<10)
	other, _ := clustertest.MakeXorb(t, 5, 2, 8<<10)
	t.Run("not found", func(t *testing.T) {
		tracker, holder := cluster(t, nil)
		tracker.Hold(holder.Peer(), hash)
		_, srv := newTestPeer(t, tracker.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET in cooldown = %d", resp.StatusCode)
		}
		if n, m := len(holder.Requests()), len(tracker.Lookups()); n != 1 || m != 1 {
			t.Fatalf("holder GETs = %d, lookups = %d, want 1 and 1 (cooldown)", n, m)
		}
		awaitSamples(t, srv, map[string]float64{`xetpd_peer_pulls_total{result="not_found"}`: 1})
	})
	t.Run("holder error", func(t *testing.T) {
		tracker := clustertest.NewFakeTracker(t)
		rawHolder(t, tracker, "broken", hash, withStatus(http.StatusInternalServerError))
		_, srv := newTestPeer(t, tracker.URL, nil)
		// A holder failing before any byte is skipped; with none left the xorb has no copy.
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
	})
	t.Run("oversized", func(t *testing.T) {
		tracker := clustertest.NewFakeTracker(t)
		gets := rawHolder(t, tracker, "oversized", hash, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(129<<20))
			w.WriteHeader(http.StatusOK)
		})
		p, srv := newTestPeer(t, tracker.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET = %d", resp.StatusCode)
		}
		if n := gets.Load(); n != 1 {
			t.Fatalf("holder GETs = %d, want 1", n)
		}
		if stored(t, p, hash) {
			t.Fatal("oversized xorb published")
		}
	})
	t.Run("empty body", func(t *testing.T) {
		tracker, _ := cluster(t, map[xet.XorbHash][]byte{hash: nil})
		p, srv := newTestPeer(t, tracker.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode == http.StatusOK {
			t.Fatal("empty body served as 200")
		}
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if stored(t, p, hash) {
			t.Fatal("empty xorb published")
		}
		err, _ := cooldownOf(p, hash)
		if !errors.Is(err, errEmpty) {
			t.Fatalf("cooldown error = %v, want %v", err, errEmpty)
		}
	})
	t.Run("empty chunked body", func(t *testing.T) {
		tracker := clustertest.NewFakeTracker(t)
		rawHolder(t, tracker, "chunked", hash, func(w http.ResponseWriter, _ *http.Request) {
			// An early flush commits to chunked encoding, so the peer sees no Content-Length.
			w.(http.Flusher).Flush()
		})
		p, srv := newTestPeer(t, tracker.URL, nil)
		if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode == http.StatusOK {
			t.Fatal("empty chunked body served as 200")
		}
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if stored(t, p, hash) {
			t.Fatal("empty xorb published")
		}
		err, _ := cooldownOf(p, hash)
		if !errors.Is(err, errEmpty) {
			t.Fatalf("cooldown error = %v, want %v", err, errEmpty)
		}
	})
	t.Run("hash mismatch", func(t *testing.T) {
		tracker, holder := cluster(t, map[xet.XorbHash][]byte{other: body})
		p, srv := newTestPeer(t, tracker.URL, nil)
		u := xorbURL(srv, other)
		_, _, _ = do(t, http.MethodGet, u)
		clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
		if stored(t, p, other) {
			t.Fatal("invalid xorb published")
		}
		if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("GET in cooldown = %d", resp.StatusCode)
		}
		if n := len(holder.Requests()); n != 1 {
			t.Fatalf("holder GETs = %d, want 1 (cooldown)", n)
		}
		if ok, _ := filepath.Glob(filepath.Join(configOf(p).DataDir, "data", "xorbs", "*", "*", ".xorb-*")); len(ok) != 0 {
			t.Fatalf("storage temp files left: %v", ok)
		}
		awaitSamples(t, srv, map[string]float64{`xetpd_peer_pulls_total{result="error"}`: 1, "xetpd_peer_cooldowns": 1})
	})
	t.Run("short body", func(t *testing.T) {
		release := make(chan struct{})
		tracker := clustertest.NewFakeTracker(t)
		gets := rawHolder(t, tracker, "short", hash, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body[:4<<10])
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		p, srv := newTestPeer(t, tracker.URL, nil)
		u := xorbURL(srv, hash)
		resp, err := streamClient.Get(u)
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
		if stored(t, p, hash) {
			t.Fatal("short xorb published")
		}
		if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway || gets.Load() != 1 {
			t.Fatalf("GET in cooldown = %d, holder GETs = %d, want 502 and 1", resp.StatusCode, gets.Load())
		}
	})
	t.Run("tracker unreachable", func(t *testing.T) {
		tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				_ = conn.Close()
			}
		}))
		t.Cleanup(tracker.Close)
		p, srv := newTestPeer(t, tracker.URL, nil)
		resp, got, _ := do(t, http.MethodGet, xorbURL(srv, hash))
		if resp.StatusCode != http.StatusBadGateway || strings.Contains(string(got), tracker.URL) || strings.Contains(string(got), configOf(p).DataDir) {
			t.Fatalf("GET with the tracker unreachable = %d %q, want 502 naming neither the tracker nor a local path", resp.StatusCode, got)
		}
	})
}

func TestPullBatchPullsXorbsAsSeedsAndFetchesShards(t *testing.T) {
	tf := makeFile(t, 90, 91)
	xs := tf.xorbHashes()
	other, otherBody := clustertest.MakeXorb(t, 92, 2, 8<<10)
	tracker, holder := newTracker(t), newHolder(t, "holder")
	tracker.SetReply(replyMinSeeds(1))
	holder.ServeShard(tf.file, tf.resp)
	tracker.HoldShard(holder.Peer(), tf.file)
	for h, body := range tf.xorbs {
		holder.ServeXorb(h, body)
		tracker.Hold(holder.Peer(), h)
	}
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Secret, c.Heartbeat, c.XorbCacheSize = testSecret, time.Hour, int64(len(tf.xorbs[xs[0]])+len(tf.xorbs[xs[1]]))
	})
	payload := `{"xorbs":["` + xs[0].String() + `","` + xs[1].String() + `"],"shards":["` + tf.file.Hash + `"],"seed":true}`
	if resp, body := send(t, http.MethodPost, srv.URL+apis.PullPath, payload, "Authorization", "Bearer "+testSecret); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pull = %d %q, want 202", resp.StatusCode, body)
	}
	for _, h := range xs {
		clustertest.WaitFor(t, clustertest.WaitTimeout, "the pull of "+h.String(), admitted(p, h))
	}
	tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return slices.Contains(a.Req.AddShards, tf.file) })
	var paths []string
	for _, r := range holder.Requests() {
		paths = append(paths, r.Path)
	}
	if want := []string{apis.ShardPathPrefix + tf.file.Hash, apis.XorbPathPrefix + xs[0].String(), apis.XorbPathPrefix + xs[1].String()}; !slices.Equal(slices.Sorted(slices.Values(paths)), slices.Sorted(slices.Values(want))) {
		t.Fatalf("holder requests = %v, want %v", paths, want)
	}
	// With no other holder left, the seed copies outlast a younger xorb over the cap.
	tracker.Forget("holder")
	commitDirect(t, p, other, otherBody)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the cache back under its cap", func() bool { return usage(p) <= configOf(p).XorbCacheSize })
	if !stored(t, p, xs[0]) || !stored(t, p, xs[1]) || stored(t, p, other) {
		t.Fatal("want the seed xorbs kept and the younger xorb evicted")
	}
}

func TestPullOfAShardAloneFetchesNoXorb(t *testing.T) {
	tf := makeFile(t, 95, 96)
	tracker, holder := newTracker(t), newHolder(t, "holder")
	holder.ServeShard(tf.file, tf.resp)
	tracker.HoldShard(holder.Peer(), tf.file)
	for h, body := range tf.xorbs {
		holder.ServeXorb(h, body)
		tracker.Hold(holder.Peer(), h)
	}
	_, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Secret, c.Heartbeat = testSecret, time.Hour })
	auth := "Bearer " + testSecret
	if resp, body := send(t, http.MethodPost, srv.URL+apis.PullPath, `{"shards":["`+tf.file.Hash+`"]}`, "Authorization", auth); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pull = %d %q, want 202", resp.StatusCode, body)
	}
	added, _ := tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return len(a.Req.AddShards) > 0 })
	if !slices.Equal(added.Req.AddShards, []apis.File{tf.file}) || len(added.Req.Add) != 0 {
		t.Fatalf("announce after the pull = %+v, want only the shard of %+v", added.Req, tf.file)
	}
	if resp, body := send(t, http.MethodGet, srv.URL+apis.ShardPathPrefix+tf.file.Hash, "", "Authorization", auth); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET of the pulled shard = %d %q, want 200 from the cache", resp.StatusCode, body)
	}
	if reqs := holder.Requests(); len(reqs) != 1 || reqs[0].Path != apis.ShardPathPrefix+tf.file.Hash {
		t.Fatalf("holder requests = %+v, want only the shard GET", reqs)
	}
}

func TestPullRouteValidatesTheBatch(t *testing.T) {
	captureWarnings(t) // the accepted batch fails in the background
	tracker := newTracker(t)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Secret, c.Heartbeat = testSecret, time.Hour })
	entries := func(n int) string {
		list := make([]string, n)
		for i := range list {
			list[i] = fmt.Sprintf(`"%064x"`, i)
		}
		return "[" + strings.Join(list, ",") + "]"
	}
	valid := `{"xorbs":` + entries(200) + `}`
	auth := "Bearer " + testSecret
	for _, tt := range []struct {
		name, auth, payload string
		want                int
		body                string
	}{
		{"no secret", "", valid, http.StatusUnauthorized, "unauthorized\n"},
		{"wrong secret", "Bearer wrong", valid, http.StatusUnauthorized, "unauthorized\n"},
		{"empty", auth, `{}`, http.StatusBadRequest, "empty pull\n"},
		{"legacy file", auth, `{"file":"` + strings.Repeat("ab", 32) + `","seed":true}`, http.StatusBadRequest, "empty pull\n"},
		{"too many", auth, `{"xorbs":` + entries(200) + `,"shards":` + entries(apis.MaxPullBatch-199) + `}`, http.StatusBadRequest, "too many entries\n"},
		{"invalid xorb", auth, `{"xorbs":["zz"]}`, http.StatusBadRequest, "invalid hash\n"},
		{"invalid shard", auth, `{"xorbs":` + entries(1) + `,"shards":["zz"]}`, http.StatusBadRequest, "invalid hash\n"},
		{"trailing data", auth, valid + `{}`, http.StatusBadRequest, ""},
		{"oversized", auth, valid + strings.Repeat(" ", apis.MaxPullBytes+1-len(valid)), http.StatusRequestEntityTooLarge, ""},
	} {
		resp, body := send(t, http.MethodPost, srv.URL+apis.PullPath, tt.payload, "Authorization", tt.auth)
		if resp.StatusCode != tt.want || (tt.body != "" && body != tt.body) {
			t.Fatalf("%s pull = %d %q, want %d %q", tt.name, resp.StatusCode, body, tt.want, tt.body)
		}
	}
	if n := len(tracker.Lookups()); n != 0 {
		t.Fatalf("lookups after refused pulls = %d, want none", n)
	}
	if resp, body := send(t, http.MethodPost, srv.URL+apis.PullPath, valid+strings.Repeat(" ", 300<<10-len(valid)), "Authorization", auth); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pull of 300 KiB with 200 entries = %d %q, want 202", resp.StatusCode, body)
	}
	_ = p.Close()
	if resp, body := send(t, http.MethodPost, srv.URL+apis.PullPath, valid, "Authorization", auth); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("pull after Close = %d %q, want 503", resp.StatusCode, body)
	}
}

func TestUnknownContentLengthWaitsForValidation(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 7, 4, 16<<10)
	tracker := clustertest.NewFakeTracker(t)
	gets := rawHolder(t, tracker, "chunked", hash, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body[:100])
		w.(http.Flusher).Flush()
		_, _ = w.Write(body[100:])
	})
	_, srv := newTestPeer(t, tracker.URL, nil)
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) || resp.ContentLength != int64(len(body)) {
		t.Fatalf("GET = %d, %d bytes, length %d, %v", resp.StatusCode, len(got), resp.ContentLength, err)
	}
	if n := gets.Load(); n != 1 {
		t.Fatalf("holder GETs = %d, want 1", n)
	}
}

func TestCloseCancelsPullAndReaders(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 8, 4, 16<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	holder.GateXorb(hash, body, 4<<10, make(chan struct{}))
	p, srv := newTestPeer(t, tracker.URL, nil)
	u := xorbURL(srv, hash)
	resp, err := streamClient.Get(u)
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
	if stored(t, p, hash) {
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

func TestAcquireAfterCommitStartsNoPull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 9, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	p, srv := newTestPeer(t, tracker.URL, nil)
	if resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash)); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("cold GET = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull exit", noPulls(p))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	// A handler that missed the store before that commit now takes its pull decision.
	f, err := storeOf(p).acquire(hash, false)
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		f.release()
		t.Fatal("acquire started a redundant pull for a committed xorb")
	}
	if n := len(holder.Requests()); n != 1 {
		t.Fatalf("holder GETs = %d, want 1", n)
	}
}

func TestDeduplicatedSpoolStillValidates(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 10, 4, 16<<10)
	corrupt := bytes.Clone(body)
	corrupt[len(corrupt)/2] ^= 0xff
	release := make(chan struct{})
	tracker, holder := cluster(t, nil)
	holder.GateXorb(hash, corrupt, 4<<10, release)
	tracker.Hold(holder.Peer(), hash)
	p, srv := newTestPeer(t, tracker.URL, nil)
	u := xorbURL(srv, hash)

	late := doAsync(t, u)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(holder.Requests()) == 1 })
	// A valid copy is committed underneath the running pull, so its PutXorb deduplicates.
	commitDirect(t, p, hash, body)
	close(release)
	<-late
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	_, cooling := cooldownOf(p, hash)
	if !cooling {
		t.Fatal("corrupt pull passed on the deduplicated copy: no cooldown")
	}
	if !stored(t, p, hash) {
		t.Fatal("failed pull removed the committed copy")
	}
	resp, got, err := do(t, http.MethodGet, u)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("committed xorb after the failed pull = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := len(holder.Requests()); n != 1 {
		t.Fatalf("holder GETs = %d, want 1", n)
	}
}

func TestDeadClientReleasesReaderAndKeepsPull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 11, 4, 16<<10)
	release := make(chan struct{})
	const prefix = 4 << 10
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	holder.GateXorb(hash, body, prefix, release)
	p, srv := newTestPeer(t, tracker.URL, nil)
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
	if err := conn.SetReadDeadline(time.Now().Add(clustertest.WaitTimeout)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatalf("multi range header while the pull is held: %v", err)
	}
	f, refs, _, _ := pullStatus(p, hash)
	held := f != nil && refs == 2
	if resp.StatusCode != http.StatusPartialContent || !held {
		t.Fatalf("multi range = %d, streaming client holds a pull reference %v, want 206 and true", resp.StatusCode, held)
	}
	_ = conn.Close()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "dead client release", func() bool {
		f, refs, _, _ := pullStatus(p, hash)
		return f != nil && refs == 1
	})
	close(release)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "commit", func() bool { return stored(t, p, hash) })
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET after the dead client = %d, %d bytes, %v", resp.StatusCode, len(got), err)
	}
	if n := len(holder.Requests()); n != 1 {
		t.Fatalf("holder GETs = %d, want 1", n)
	}
}
