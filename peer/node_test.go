package peer

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const modelPath = "/org/repo/resolve/main/model.bin"

// startNode runs a peer as its node's endpoint, advertised at its own listener, announcing to trackerURL and mirroring upstream.
func startNode(t *testing.T, trackerURL, upstream string) (*Peer, string) {
	t.Helper()
	p, origin, _ := startRecordedNode(t, trackerURL, upstream)
	return p, origin
}

// startRecordedNode is startNode recording every request the peer receives.
func startRecordedNode(t *testing.T, trackerURL, upstream string) (*Peer, string, *recorder) {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil)
	origin := "http://" + srv.Listener.Addr().String()
	p, err := New(Config{Advertise: origin, Tracker: trackerURL, Upstream: upstream, DataDir: t.TempDir(), Secret: testSecret, Heartbeat: 50 * time.Millisecond})
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	rec := &recorder{}
	srv.Config.Handler = rec.wrap(p)
	srv.Start()
	t.Cleanup(func() {
		_ = p.Close()
		srv.Close()
	})
	return p, origin, rec
}

// recorded is a request as a recorder saw it; peer is its apis.PeerHeader.
type recorded struct{ method, path, peer string }

// recorder records the requests reaching the handlers it wraps.
type recorder struct {
	mu   sync.Mutex
	reqs []recorded
}

func (rec *recorder) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, recorded{r.Method, r.URL.Path, r.Header.Get(apis.PeerHeader)})
		rec.mu.Unlock()
		h.ServeHTTP(w, r)
	})
}

func (rec *recorder) requests() []recorded {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return slices.Clone(rec.reqs)
}

// send performs one request carrying body without following redirects and returns the response and its body.
func send(t *testing.T, method, url, body string, hdr ...string) (*http.Response, string) {
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

// waitReady polls the resolve HEAD at url until the mirror advertises the file's xet hash, which it returns.
func waitReady(t *testing.T, url string) string {
	t.Helper()
	var hash string
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the mirror to publish "+url, func() bool {
		resp, _ := send(t, http.MethodHead, url, "")
		hash = resp.Header.Get("X-Xet-Hash")
		return hash != ""
	})
	return hash
}

// fetchURLs lists every fetch_info url of the V1 reconstruction of fileHash that origin serves.
func fetchURLs(t *testing.T, origin, fileHash string) []string {
	t.Helper()
	resp, body := send(t, http.MethodGet, origin+"/v1/reconstructions/"+fileHash, "")
	var v1 download.ReconstructionResponseV1
	if resp.StatusCode != http.StatusOK || json.Unmarshal([]byte(body), &v1) != nil || len(v1.FetchInfo) == 0 {
		t.Fatalf("reconstruction of %s at %s = %d %q", fileHash, origin, resp.StatusCode, body)
	}
	var urls []string
	for _, entries := range v1.FetchInfo {
		for _, e := range entries {
			urls = append(urls, e.URL)
		}
	}
	return urls
}

func TestColdResolveIngestsOnceAndAnnouncesTheShard(t *testing.T) {
	data := clustertest.RandomData(97, 256<<10)
	hub := clustertest.NewHub(t)
	hub.Set(modelPath, data)
	tracker := newTracker(t)
	_, a := startNode(t, tracker.URL, hub.URL)
	if resp, got, err := do(t, http.MethodGet, a+modelPath); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("cold GET = %d, %d bytes, %v; want the hub's %d bytes", resp.StatusCode, len(got), err, len(data))
	}
	fileHash := waitReady(t, a+modelPath)
	if gets, _ := hub.CDN(); gets != 1 {
		t.Fatalf("hub CDN gets = %d, want the single ingest", gets)
	}
	for _, u := range fetchURLs(t, a, fileHash) {
		if !strings.HasPrefix(u, a+apis.XorbPathPrefix) {
			t.Fatalf("fetch url %s, want one on the peer itself %s", u, a)
		}
	}
	file := apis.File{Hash: fileHash, SHA256: clustertest.SHA256(data)}
	tracker.FindAnnounce(t, 0, func(x clustertest.Announce) bool {
		return x.Req.Peer.URL == a && slices.Contains(x.Req.AddShards, file)
	})
}

func TestWarmResolveOnASecondPeerPullsFromTheFirst(t *testing.T) {
	data := clustertest.RandomData(98, 256<<10)
	hub := clustertest.NewHub(t)
	hub.Set(modelPath, data)
	tracker := newTracker(t)
	pa, a := startNode(t, tracker.URL, hub.URL)
	fileHash := waitReady(t, a+modelPath)
	file := apis.File{Hash: fileHash, SHA256: clustertest.SHA256(data)}
	tracker.FindAnnounce(t, 0, func(x clustertest.Announce) bool { return slices.Contains(x.Req.AddShards, file) })
	// The tracker answers lookups with what the first peer announced, as a real one would.
	first := apis.Peer{ID: peerID(pa), URL: a}
	tracker.HoldShard(first, file)
	for _, u := range fetchURLs(t, a, fileHash) {
		h, err := xet.ParseXorbHash(strings.TrimPrefix(u, a+apis.XorbPathPrefix))
		if err != nil {
			t.Fatal(err)
		}
		tracker.Hold(first, h)
	}
	cdn, _ := hub.CDN()
	pb, b := startNode(t, tracker.URL, hub.URL)
	if got := waitReady(t, b+modelPath); got != fileHash {
		t.Fatalf("second peer resolves the file as %s, want %s", got, fileHash)
	}
	if gets, _ := hub.CDN(); gets != cdn {
		t.Fatalf("hub CDN gets %d -> %d: the second peer ingested from the hub", cdn, gets)
	}
	urls := fetchURLs(t, b, fileHash)
	for _, u := range urls {
		if !strings.HasPrefix(u, b+apis.XorbPathPrefix) {
			t.Fatalf("fetch url %s, want one on the second peer %s", u, b)
		}
	}
	// The xet client fetches every fetch_info url_range from the second peer by Range.
	xc, err := client.NewClient(client.WithUpstreamProvider(client.StaticUpstreamProvider(b, "")), client.WithCache(client.NewCache(t.TempDir(), 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	fh, err := xet.ParseFileHash(fileHash)
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	if err := xc.DownloadFileV1(t.Context(), fh, out); err != nil {
		t.Fatalf("download from the second peer: %v", err)
	}
	_ = out.Close()
	if got, err := os.ReadFile(out.Name()); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("downloaded %d bytes, %v; want the hub's %d", len(got), err, len(data))
	}
	for _, u := range urls {
		h, err := xet.ParseXorbHash(strings.TrimPrefix(u, b+apis.XorbPathPrefix))
		if err != nil {
			t.Fatal(err)
		}
		clustertest.WaitFor(t, clustertest.WaitTimeout, "the second peer to keep "+h.String(), admitted(pb, h))
	}
	tracker.FindAnnounce(t, 0, func(x clustertest.Announce) bool {
		return x.Req.Peer.URL == b && slices.Contains(x.Req.AddShards, file)
	})
	if v := clustertest.Scrape(t, a)[`xetpd_peer_xorb_gets_total{requester="peer",status="200"}`]; v < 1 {
		t.Fatalf("first peer served %v xorb GETs to peers, want the second peer's pull", v)
	}
	if !slices.ContainsFunc(tracker.Lookups(), func(l clustertest.Lookup) bool { return l.Req.Peer == peerID(pb) }) {
		t.Fatal("the tracker answered no lookup of the second peer")
	}
	if gets, _ := hub.CDN(); gets != cdn {
		t.Fatalf("hub CDN gets %d -> %d after the download", cdn, gets)
	}
}

const telemetryBody = `{"time":"2026-10-19T00:00:00Z","event":"download","session_id":"s1","user_agent":"xet-core/1.7.0","metrics":{}}`

func TestReadOnlySurface(t *testing.T) {
	hub := clustertest.NewHub(t)
	p, srv := newTestPeer(t, clustertest.NewFakeTracker(t).URL, func(c *Config) { c.Upstream, c.UpstreamToken = hub.URL, "up-tok" })
	hash := strings.Repeat("ab", 32)
	for _, tt := range []struct{ method, path string }{
		{http.MethodPost, "/api/models/x/commit/main"},
		{http.MethodPut, "/v1/xorbs/default/" + hash},
		{http.MethodPost, "/v1/xorbs/default/" + hash},
		{http.MethodPost, "/v1/shards"},
		{http.MethodPost, "/v1/telemetry/extra"},
		{http.MethodPost, "/api/models/org/repo/paths-info/main/extra"},
		{http.MethodDelete, "/api/models/org/repo"},
		{http.MethodGet, "/api/models/org/repo/xet-write-token/main"},
	} {
		if resp, body := send(t, tt.method, srv.URL+tt.path, "paths=a", "Content-Type", "application/x-www-form-urlencoded"); resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s = %d %q, want 403", tt.method, tt.path, resp.StatusCode, body)
		}
	}
	for _, path := range []string{"/xet-pd/v1/unknown", apis.PullPath} {
		if resp, body := send(t, http.MethodGet, srv.URL+path, ""); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s = %d %q, want 404", path, resp.StatusCode, body)
		}
	}
	if n := len(hub.Auth()); n != 0 {
		t.Fatalf("denied and control requests reached the upstream %d times", n)
	}
	resp, body := send(t, http.MethodPost, srv.URL+"/api/models/org/repo/paths-info/main", "paths=a.bin", "Content-Type", "application/x-www-form-urlencoded", "Authorization", "Bearer client-tok")
	if resp.StatusCode != http.StatusOK || body != `[{"path":"a.bin","type":"file"}]`+"\n" {
		t.Fatalf("POST paths-info = %d %q, want the hub's listing", resp.StatusCode, body)
	}
	if got := hub.Auth(); !slices.Equal(got, []string{"Bearer up-tok"}) {
		t.Fatalf("upstream saw Authorization %q, want only the upstream token", got)
	}
	if resp, body := send(t, http.MethodPost, srv.URL+"/v1/telemetry", telemetryBody); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/telemetry = %d %q, want the CAS's 200", resp.StatusCode, body)
	}
	if resp, body := send(t, http.MethodGet, srv.URL+"/api/models/org/repo/unknown", ""); resp.StatusCode != http.StatusNotFound || len(hub.Auth()) != 2 {
		t.Fatalf("GET of an unknown hub path = %d %q after %d upstream requests, want the hub's 404 through the proxy", resp.StatusCode, body, len(hub.Auth()))
	}
	resp, body = send(t, http.MethodGet, srv.URL+"/api/models/org/repo/xet-read-token/main", "")
	var tok struct {
		CASURL string `json:"casUrl"`
		Token  string `json:"accessToken"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal([]byte(body), &tok) != nil || tok.Token == "" || tok.CASURL != configOf(p).Advertise {
		t.Fatalf("read token = %d %q, want a token for the CAS at %s", resp.StatusCode, body, configOf(p).Advertise)
	}
}
