package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hf"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/hubtest"
	"github.com/wzshiming/xet-pd/peer"
)

// newTestGateway serves a gateway whose own origin is handed to mod before New runs.
func newTestGateway(t *testing.T, hub *hubtest.Hub, mod func(cfg *Config, self string)) (*Gateway, *httptest.Server) {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil)
	cfg := Config{StorageDir: t.TempDir(), Upstream: hub.URL}
	if mod != nil {
		mod(&cfg, "http://"+srv.Listener.Addr().String())
	}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = g
	srv.Start()
	t.Cleanup(func() {
		_ = g.Close()
		srv.Close()
	})
	return g, srv
}

// startPeer runs a real seed with a fresh directory on addr; "127.0.0.1:0" picks a port.
func startPeer(t *testing.T, gw, secret, addr string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.New(peer.Config{Advertise: "http://" + ln.Addr().String(), Gateway: gw, CacheDir: t.TempDir(), Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: p}
	go func() { _ = srv.Serve(ln) }()
	stop := func() {
		_ = p.Close()
		_ = srv.Close()
	}
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

func request(t *testing.T, method, url, body string, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(data)
}

func diag(t *testing.T, gw string) apis.PeersResponse {
	t.Helper()
	resp, body := request(t, http.MethodGet, gw+apis.PeersPath, "")
	var d apis.PeersResponse
	if resp.StatusCode != http.StatusOK || json.Unmarshal([]byte(body), &d) != nil {
		t.Fatalf("GET %s = %d %q", apis.PeersPath, resp.StatusCode, body)
	}
	return d
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testData(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{7}).Read(b)
	return b
}

// waitReady polls the resolve HEAD until the mirror advertises the xet hash and returns that response.
func waitReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	var resp *http.Response
	waitFor(t, "ingest ready", func() bool {
		resp, _ = request(t, http.MethodHead, resolveURL, "")
		return resp.Header.Get("X-Xet-Hash") != ""
	})
	return resp
}

func TestConfigValidation(t *testing.T) {
	hub := hubtest.New(t)
	tests := []struct {
		name    string
		mod     func(*Config)
		wantErr string
	}{
		{"missing upstream", func(c *Config) { c.Upstream = "" }, "upstream"},
		{"missing storage dir", func(c *Config) { c.StorageDir = "" }, "storage dir"},
		{"upstream path", func(c *Config) { c.Upstream += "/hub" }, "upstream"},
		{"upstream userinfo", func(c *Config) { c.Upstream = "http://u:p@127.0.0.1:1" }, "upstream"},
		{"upstream query", func(c *Config) { c.Upstream += "?x=1" }, "upstream"},
		{"upstream fragment", func(c *Config) { c.Upstream += "#f" }, "upstream"},
		{"upstream scheme", func(c *Config) { c.Upstream = "ftp://127.0.0.1:1" }, "upstream"},
		{"upstream empty host", func(c *Config) { c.Upstream = "http://:1234" }, "upstream"},
		{"upstream empty port", func(c *Config) { c.Upstream = "http://127.0.0.1:" }, "upstream"},
		{"upstream credentials unparsable", func(c *Config) { c.Upstream = "http://bob:hunter2@127.0.0.1:1/%zz" }, "upstream"},
		{"external url", func(c *Config) { c.ExternalURL = "http://gw.test/base" }, "external url"},
		{"external url credentials unparsable", func(c *Config) { c.ExternalURL = "http://bob:hunter2@gw.test/%zz" }, "external url"},
		{"sub-second ttl", func(c *Config) { c.PeerTTL = 500 * time.Millisecond }, "ttl"},
		{"negative ttl", func(c *Config) { c.PeerTTL = -time.Second }, "negative"},
		{"negative replicas", func(c *Config) { c.Replicas = -1 }, "negative"},
		{"negative min seeds", func(c *Config) { c.MinSeeds = -1 }, "negative"},
		{"negative cache", func(c *Config) { c.XorbCacheSize = -1 }, "negative"},
		{"min seeds over replicas", func(c *Config) { c.MinSeeds = 3 }, "replicas"},
		{"defaults", nil, ""},
		{"explicit", func(c *Config) {
			c.Upstream += "/"
			c.ExternalURL = "http://gw.test/"
			c.PeerTTL, c.Replicas, c.MinSeeds, c.XorbCacheSize = 2*time.Second, 2, 2, 1
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{StorageDir: t.TempDir(), Upstream: hub.URL}
			if tt.mod != nil {
				tt.mod(&cfg)
			}
			g, err := New(cfg)
			if tt.wantErr != "" {
				if err == nil || !strings.HasPrefix(err.Error(), "gateway: ") || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("New = %v, want gateway error mentioning %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "hunter2") {
					t.Fatalf("New echoes the url: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			wantTTL, wantReplicas, wantSeeds := 30*time.Second, 2, 1
			if tt.mod != nil {
				wantTTL, wantSeeds = 2*time.Second, 2
			}
			if g.reg.ttl != wantTTL || g.staging.cfg.replicas != wantReplicas || g.staging.cfg.minSeeds != wantSeeds || g.staging.cfg.cacheSize != cfg.XorbCacheSize {
				t.Fatalf("effective ttl %v replicas %d minSeeds %d cache %d", g.reg.ttl, g.staging.cfg.replicas, g.staging.cfg.minSeeds, g.staging.cfg.cacheSize)
			}
			for _, dir := range []string{"data/offsets", "mirror/index"} {
				if _, err := os.Stat(filepath.Join(cfg.StorageDir, dir)); err != nil {
					t.Fatalf("layout: %v", err)
				}
			}
		})
	}
}

func TestControlRoutes(t *testing.T) {
	hub := hubtest.New(t)
	_, srv := newTestGateway(t, hub, nil)
	tests := []struct {
		method, path string
		status       int
		allow        string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, ""},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, http.MethodGet},
		{http.MethodGet, apis.AnnouncePath, http.StatusMethodNotAllowed, http.MethodPost},
		{http.MethodPost, apis.PeersPath, http.StatusMethodNotAllowed, http.MethodGet},
		{http.MethodGet, apis.PullPath, http.StatusNotFound, ""},
		{http.MethodPost, "/xet-pd/v1/other", http.StatusNotFound, ""},
		{http.MethodGet, "/xet-pd/", http.StatusNotFound, ""},
	}
	for _, tt := range tests {
		resp, body := request(t, tt.method, srv.URL+tt.path, "")
		if resp.StatusCode != tt.status || resp.Header.Get("Allow") != tt.allow {
			t.Fatalf("%s %s = %d Allow %q %q, want %d Allow %q", tt.method, tt.path, resp.StatusCode, resp.Header.Get("Allow"), body, tt.status, tt.allow)
		}
	}
	if n := len(hub.Auth()); n != 0 {
		t.Fatalf("control paths reached the upstream %d times", n)
	}
	resp, body := request(t, http.MethodGet, srv.URL+apis.PeersPath, "")
	want := `{"peers":[],"xorb_bytes":0,"xorb_gets":{"peer":0,"direct":0}}` + "\n"
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" || body != want {
		t.Fatalf("GET %s = %d %q %q, want %q", apis.PeersPath, resp.StatusCode, resp.Header.Get("Content-Type"), body, want)
	}
}

func TestReadOnlySurface(t *testing.T) {
	hub := hubtest.New(t)
	_, srv := newTestGateway(t, hub, func(cfg *Config, _ string) { cfg.UpstreamToken = "up-tok" })
	hash := strings.Repeat("ab", 32)
	denied := []struct{ method, path string }{
		{http.MethodPost, "/v1/xorbs/default/" + hash},
		{http.MethodPut, "/v1/xorbs/default/" + hash},
		{http.MethodPost, "/v1/shards"},
		{http.MethodPost, "/v2/shards"},
		{http.MethodPost, "/v1/chunks/default:query"},
		{http.MethodDelete, "/api/models/org/repo"},
		{http.MethodOptions, "/api/models/org/repo/paths-info/main"},
		{http.MethodPost, "/api/models/org/repo/commit/main"},
		{http.MethodPost, "/api/models/org/paths-info/commit/main"},
		{http.MethodPost, "/api/models/paths-info/main"},
		{http.MethodPost, "/api/users/org/repo/paths-info/main"},
		{http.MethodPost, "/api/models/org/repo/paths-info/main/extra"},
		{http.MethodPost, "/api/models/org/repo/paths-info/"},
		{http.MethodPost, "/org/repo/paths-info/main"},
		{http.MethodGet, "/api/models/org/repo/xet-write-token/main"},
	}
	for _, tt := range denied {
		resp, _ := request(t, tt.method, srv.URL+tt.path, "paths=a", "Content-Type", "application/x-www-form-urlencoded")
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s = %d, want 403", tt.method, tt.path, resp.StatusCode)
		}
	}
	if n := len(hub.Auth()); n != 0 {
		t.Fatalf("denied requests reached the upstream %d times", n)
	}
	for _, path := range []string{"/api/models/org/repo/paths-info/main", "/api/datasets/org/paths-info/refs%2Fpr%2F1?expand=1"} {
		resp, body := request(t, http.MethodPost, srv.URL+path, "paths=a.bin", "Content-Type", "application/x-www-form-urlencoded", "Authorization", "Bearer client-tok")
		if resp.StatusCode != http.StatusOK || body != `[{"path":"a.bin","type":"file"}]`+"\n" {
			t.Fatalf("POST %s = %d %q", path, resp.StatusCode, body)
		}
	}
	if got := hub.Auth(); !reflect.DeepEqual(got, []string{"Bearer up-tok", "Bearer up-tok"}) {
		t.Fatalf("upstream saw Authorization %q, want the upstream token only", got)
	}
	resp, body := request(t, http.MethodGet, srv.URL+"/api/models/org/repo/xet-read-token/main", "")
	var tok struct {
		CASURL string `json:"casUrl"`
		Token  string `json:"accessToken"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal([]byte(body), &tok) != nil || tok.Token == "" || tok.CASURL != srv.URL {
		t.Fatalf("read token = %d %q", resp.StatusCode, body)
	}
}

func TestSigningKeyBindsReadTokens(t *testing.T) {
	hub := hubtest.New(t)
	data := testData(64 << 10)
	hub.Set("/org/repo/resolve/main/a.bin", data)
	_, srv := newTestGateway(t, hub, func(cfg *Config, _ string) { cfg.SigningKey = "k" })
	fileHash := waitReady(t, srv.URL+"/org/repo/resolve/main/a.bin").Header.Get("X-Xet-Hash")
	recon := srv.URL + "/v1/reconstructions/" + fileHash
	if resp, _ := request(t, http.MethodGet, recon, ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous reconstruction = %d, want 401", resp.StatusCode)
	}
	mint := func(hash string) string {
		resp, body := request(t, http.MethodGet, srv.URL+"/xet-token/"+hash, "")
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Xet-Access-Token") == "" {
			t.Fatalf("mint for %s = %d %q", hash, resp.StatusCode, body)
		}
		return resp.Header.Get("X-Xet-Access-Token")
	}
	if resp, body := request(t, http.MethodGet, recon, "", "Authorization", "Bearer "+mint(fileHash)); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"fetch_info"`) {
		t.Fatalf("bound reconstruction = %d %q", resp.StatusCode, body)
	}
	if resp, _ := request(t, http.MethodGet, recon, "", "Authorization", "Bearer "+mint(strings.Repeat("11", 32))); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("reconstruction with a token bound to another file = %d, want 403", resp.StatusCode)
	}
	if resp, _ := request(t, http.MethodHead, srv.URL+"/v1/xorbs/default/"+strings.Repeat("11", 32), ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous xorb probe = %d, want 401", resp.StatusCode)
	}
	if resp, _ := request(t, http.MethodGet, srv.URL+"/xet-bridge/"+sha(data), ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous bridge = %d, want 200", resp.StatusCode)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	g, srv := newTestGateway(t, hubtest.New(t), nil)
	for range 2 {
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if resp, _ := request(t, http.MethodGet, srv.URL+"/healthz", ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz after Close = %d", resp.StatusCode)
	}
}

// Two real seeds hold the only copies of an ingested file; a full loss of both re-ingests it from the hub.
func TestGatewayIntegration(t *testing.T) {
	const secret, upstreamToken = "s3cret", "up-tok"
	hub := hubtest.New(t)
	data := testData(300 << 10)
	hub.Set("/org/repo/resolve/main/model.bin", data)
	g, srv := newTestGateway(t, hub, func(cfg *Config, self string) {
		cfg.Secret, cfg.UpstreamToken, cfg.ExternalURL = secret, upstreamToken, self
		cfg.XorbCacheSize, cfg.MinSeeds = 1, 2
	})
	resolveURL := srv.URL + "/org/repo/resolve/main/model.bin"
	ctx := context.Background()

	var addrs []string
	var stops []func()
	for range 2 {
		addr, stop := startPeer(t, srv.URL, secret, "127.0.0.1:0")
		addrs, stops = append(addrs, addr), append(stops, stop)
	}
	peerURLs := func(d apis.PeersResponse) []string {
		var out []string
		for _, p := range d.Peers {
			out = append(out, p.URL)
		}
		return out
	}
	waitFor(t, "peers registered", func() bool { return len(diag(t, srv.URL).Peers) == 2 })
	for _, addr := range addrs {
		if !strings.Contains(strings.Join(peerURLs(diag(t, srv.URL)), " "), "http://"+addr) {
			t.Fatalf("peer %s missing from %v", addr, peerURLs(diag(t, srv.URL)))
		}
	}

	// Cold plain GET streams the bytes while the ingest runs.
	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || sha(body) != sha(data) {
		t.Fatalf("plain GET = %d %v, %d bytes", resp.StatusCode, err, len(body))
	}
	ready := waitReady(t, resolveURL)
	fileHash, etag := ready.Header.Get("X-Xet-Hash"), ready.Header.Get("ETag")
	if etag != `"`+sha(data)+`"` || ready.Header.Get("X-Repo-Commit") != hub.Commit {
		t.Fatalf("ready headers = %v", ready.Header)
	}
	if gets, n := hub.CDN(); gets != 1 || n != int64(len(data)) {
		t.Fatalf("hub CDN after ingest = %d gets, %d bytes", gets, n)
	}

	// Both seeds pull the xorb through the gateway, which then drops its own copy.
	seeded := func(d apis.PeersResponse) bool {
		if d.XorbBytes != 0 || len(d.Peers) != 2 {
			return false
		}
		for _, p := range d.Peers {
			if p.Holdings != 1 {
				return false
			}
		}
		return true
	}
	waitFor(t, "seeds hold the xorb", func() bool { return seeded(diag(t, srv.URL)) })
	if d := diag(t, srv.URL); d.XorbGets != (apis.XorbGets{Peer: 2}) {
		t.Fatalf("xorb gets after seeding = %+v", d.XorbGets)
	}

	// A fresh xet client downloads straight from the seeds.
	download := func() {
		t.Helper()
		repo, path, err := hf.ParseResolveURL(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		c, err := hf.NewClient(repo, hf.WithClientOptions(client.WithCache(client.NewCache(t.TempDir(), 0, 0))))
		if err != nil {
			t.Fatal(err)
		}
		rf, err := c.Resolve(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		fh := rf.Hash
		if fh.String() != fileHash {
			t.Fatalf("resolved hash %s, want %s", fh, fileHash)
		}
		f, err := os.Create(filepath.Join(t.TempDir(), "out"))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.DownloadFile(ctx, fh, f); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := os.ReadFile(f.Name())
		if err != nil || sha(out) != sha(data) {
			t.Fatalf("xet download: %v, %d bytes", err, len(out))
		}
		v1, err := c.GetReconstructionV1(ctx, fh, nil)
		if err != nil {
			t.Fatal(err)
		}
		v2, err := c.GetReconstructionV2(ctx, fh, nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := c.GetBatchReconstruction(ctx, []xet.FileHash{fh})
		if err != nil {
			t.Fatal(err)
		}
		if len(v1.FetchInfo) == 0 || !reflect.DeepEqual(batch.FetchInfo, v1.FetchInfo) || !reflect.DeepEqual(batch.Files[fileHash], v1.Terms) || !reflect.DeepEqual(v2.Terms, v1.Terms) {
			t.Fatalf("batch/v2 differ from v1:\n%+v\n%+v\n%+v", v1, v2, batch)
		}
		peers := peerURLs(diag(t, srv.URL))
		for xorbHash, entries := range v1.FetchInfo {
			multi := v2.Xorbs[xorbHash]
			if len(multi) != 1 || len(multi[0].Ranges) != len(entries) {
				t.Fatalf("v2 xorb %s = %+v, want one entry over %d ranges", xorbHash, multi, len(entries))
			}
			for i, e := range entries {
				var fromPeer bool
				for _, p := range peers {
					fromPeer = fromPeer || e.URL == p+"/v1/xorbs/default/"+xorbHash
				}
				if !fromPeer || multi[0].URL != e.URL || multi[0].Ranges[i].Bytes != e.URLRange || multi[0].Ranges[i].Chunks != e.Range {
					t.Fatalf("fetch entry %+v vs v2 %+v, peers %v", e, multi[0], peers)
				}
			}
		}
	}
	download()
	if gets, _ := hub.CDN(); gets != 1 {
		t.Fatalf("hub CDN gets after xet download = %d, want 1", gets)
	}
	if d := diag(t, srv.URL); d.XorbGets != (apis.XorbGets{Peer: 2}) {
		t.Fatalf("xorb gets after xet download = %+v, want no direct gets", d.XorbGets)
	}

	// Both seeds restart empty on the same addresses: every copy is gone.
	before := diag(t, srv.URL)
	for i, stop := range stops {
		stop()
		startPeer(t, srv.URL, secret, addrs[i])
	}
	waitFor(t, "seeds re-registered empty", func() bool {
		d := diag(t, srv.URL)
		if len(d.Peers) != 2 {
			return false
		}
		for i, p := range d.Peers {
			if p.Epoch == before.Peers[i].Epoch || p.Holdings != 0 {
				return false
			}
		}
		return true
	})
	if resp, _ := request(t, http.MethodGet, srv.URL+"/v1/reconstructions/"+fileHash, ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("reconstruction after total loss = %d, want 404", resp.StatusCode)
	}
	if resp, _ := request(t, http.MethodGet, srv.URL+"/xet-bridge/"+sha(data), ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bridge after total loss = %d, want 404", resp.StatusCode)
	}

	// The next resolve re-ingests from the hub under the same identity and seeds again.
	again := waitReady(t, resolveURL)
	if again.Header.Get("X-Xet-Hash") != fileHash || again.Header.Get("ETag") != etag {
		t.Fatalf("re-ingested headers %v, want hash %s etag %s", again.Header, fileHash, etag)
	}
	if gets, n := hub.CDN(); gets != 2 || n != 2*int64(len(data)) {
		t.Fatalf("hub CDN after re-ingest = %d gets, %d bytes", gets, n)
	}
	waitFor(t, "seeds hold the xorb again", func() bool { return seeded(diag(t, srv.URL)) })
	download()
	if gets, _ := hub.CDN(); gets != 2 {
		t.Fatalf("hub CDN gets after second xet download = %d, want 2", gets)
	}
	if d := diag(t, srv.URL); d.XorbGets != (apis.XorbGets{Peer: 4}) {
		t.Fatalf("xorb gets at the end = %+v", d.XorbGets)
	}
	for _, a := range hub.Auth() {
		if a != "Bearer "+upstreamToken {
			t.Fatalf("upstream saw Authorization %q", a)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}
