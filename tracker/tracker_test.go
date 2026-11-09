package tracker

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

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

func TestConfigValidation(t *testing.T) {
	if DefaultPeerTTL != 30*time.Second || DefaultReplicas != 2 || DefaultMinSeeds != 1 {
		t.Fatalf("defaults %v %d %d, want 30s 2 1", DefaultPeerTTL, DefaultReplicas, DefaultMinSeeds)
	}
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"negative ttl", Config{PeerTTL: -time.Second}, "negative config value"},
		{"negative replicas", Config{Replicas: -1}, "negative config value"},
		{"negative min seeds", Config{MinSeeds: -1}, "negative config value"},
		{"sub-second ttl", Config{PeerTTL: 500 * time.Millisecond}, "peer ttl below 1s"},
		{"min seeds over default replicas", Config{MinSeeds: 3}, "min seeds exceed replicas"},
		{"min seeds over replicas", Config{Replicas: 1, MinSeeds: 2}, "min seeds exceed replicas"},
		{"defaults", Config{}, ""},
		{"explicit", Config{Secret: "s", PeerTTL: 2 * time.Second, Replicas: 3, MinSeeds: 3}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := New(tt.cfg)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("New = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tr.Close() }()
			wantTTL, wantReplicas, wantSeeds := DefaultPeerTTL, DefaultReplicas, DefaultMinSeeds
			if tt.cfg != (Config{}) {
				wantTTL, wantReplicas, wantSeeds = 2*time.Second, 3, 3
			}
			ttl, minSeeds, seed := effectiveConfig(tr)
			if ttl != wantTTL || minSeeds != wantSeeds || seed != (seedConfig{replicas: wantReplicas, secret: tt.cfg.Secret, retry: time.Minute, tick: time.Second}) {
				t.Fatalf("effective ttl %v, min seeds %d, seeding %+v", ttl, minSeeds, seed)
			}
		})
	}
}

func TestControlRoutes(t *testing.T) {
	_, srv := newTestTracker(t, nil)
	tests := []struct {
		method, path string
		status       int
		allow        string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, ""},
		{http.MethodHead, "/healthz", http.StatusOK, ""},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, "GET, HEAD"},
		{http.MethodGet, "/readyz", http.StatusOK, ""},
		{http.MethodHead, "/readyz", http.StatusOK, ""},
		{http.MethodPost, "/readyz", http.StatusMethodNotAllowed, "GET, HEAD"},
		{http.MethodGet, "/metrics", http.StatusOK, ""},
		{http.MethodPost, "/metrics", http.StatusMethodNotAllowed, "GET, HEAD"},
		{http.MethodGet, apis.AnnouncePath, http.StatusMethodNotAllowed, http.MethodPost},
		{http.MethodGet, apis.LookupPath, http.StatusMethodNotAllowed, http.MethodPost},
		{http.MethodPost, apis.PeersPath, http.StatusMethodNotAllowed, http.MethodGet},
		{http.MethodGet, apis.PullPath, http.StatusNotFound, ""},
		{http.MethodPost, "/xet-pd/v1/other", http.StatusNotFound, ""},
		{http.MethodGet, "/xet-pd/", http.StatusNotFound, ""},
		{http.MethodGet, "/", http.StatusNotFound, ""},
		{http.MethodGet, "/api/x", http.StatusNotFound, ""},
		{http.MethodGet, apis.XorbPathPrefix + testHash, http.StatusNotFound, ""},
	}
	for _, tt := range tests {
		resp, body := request(t, tt.method, srv.URL+tt.path, "")
		if resp.StatusCode != tt.status || resp.Header.Get("Allow") != tt.allow {
			t.Fatalf("%s %s = %d Allow %q %q, want %d Allow %q", tt.method, tt.path, resp.StatusCode, resp.Header.Get("Allow"), body, tt.status, tt.allow)
		}
	}
	if resp, _ := request(t, http.MethodGet, srv.URL+"/metrics", ""); !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("GET /metrics Content-Type = %q, want text/plain", resp.Header.Get("Content-Type"))
	}
	resp, body := request(t, http.MethodGet, srv.URL+apis.PeersPath, "")
	want := `{"peers":[],"xorb_bytes":0,"xorb_gets":{"peer":0,"direct":0}}` + "\n"
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" || body != want {
		t.Fatalf("GET %s = %d %q %q, want %q", apis.PeersPath, resp.StatusCode, resp.Header.Get("Content-Type"), body, want)
	}
}

func TestReadyz(t *testing.T) {
	_, srv := newTestTracker(t, nil)
	if resp, body := request(t, http.MethodGet, srv.URL+"/readyz", ""); resp.StatusCode != http.StatusOK || body != "ok" {
		t.Fatalf("GET /readyz = %d %q, want 200 %q", resp.StatusCode, body, "ok")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	const secret = "s3cret"
	_, srv := newTestTracker(t, func(cfg *Config) { cfg.Secret, cfg.PeerTTL = secret, time.Hour })
	// Every tracker owns its registry: a second one in the process neither panics nor shares counts.
	_, second := newTestTracker(t, nil)
	resp, text := request(t, http.MethodGet, srv.URL+"/metrics", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics without credentials = %d, want 200", resp.StatusCode)
	}
	for _, typ := range []string{
		"announces_total counter", "lookups_total counter", "seed_requests_total counter",
		"seed_requests_in_flight gauge", "peers gauge", "xorbs gauge", "files gauge",
	} {
		if !strings.Contains("\n"+text, "\n# TYPE xetpd_tracker_"+typ+"\n") {
			t.Fatalf("GET /metrics lacks # TYPE xetpd_tracker_%s", typ)
		}
	}
	m := clustertest.Scrape(t, srv.URL)
	if _, ok := m["go_goroutines"]; !ok {
		t.Fatal("GET /metrics lacks go_goroutines")
	}
	for _, key := range []string{
		`xetpd_tracker_announces_total{result="ok"}`, `xetpd_tracker_announces_total{result="resync"}`, `xetpd_tracker_announces_total{result="rejected"}`,
		"xetpd_tracker_lookups_total", `xetpd_tracker_seed_requests_total{result="ok"}`, `xetpd_tracker_seed_requests_total{result="error"}`,
		"xetpd_tracker_seed_requests_in_flight", "xetpd_tracker_peers", "xetpd_tracker_xorbs", "xetpd_tracker_files",
	} {
		if v, ok := m[key]; !ok || v != 0 {
			t.Fatalf("fresh %s = %v (present %v), want a visible 0", key, v, ok)
		}
	}

	auth := []string{"Authorization", "Bearer " + secret}
	other := strings.Repeat("ef", 32)
	for _, req := range []apis.AnnounceRequest{
		{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e", Full: true, Add: []string{testHash}, AddFiles: []apis.File{seedFile}},
		{Peer: apis.Peer{ID: "p2", URL: "http://127.0.0.1:2"}, Epoch: "e", Full: true, Add: []string{testHash, other}, AddFiles: []apis.File{seedFile}},
		{Peer: apis.Peer{ID: "p3", URL: "http://127.0.0.1:3"}, Epoch: "e", Add: []string{testHash}},
	} {
		if status, body, _ := post(t, srv, announceJSON(t, req), auth...); status != http.StatusOK {
			t.Fatalf("announce of %s = %d %q", req.Peer.ID, status, body)
		}
	}
	if status, body, _ := post(t, srv, "nope", auth...); status != http.StatusBadRequest {
		t.Fatalf("malformed announce = %d %q, want 400", status, body)
	}
	if status, body, _ := postLookup(t, srv, `{"files":["`+seedFile.Hash+`"]}`, auth...); status != http.StatusOK {
		t.Fatalf("lookup = %d %q", status, body)
	}
	m = clustertest.Scrape(t, srv.URL)
	for key, want := range map[string]float64{
		`xetpd_tracker_announces_total{result="ok"}`:       2,
		`xetpd_tracker_announces_total{result="resync"}`:   1,
		`xetpd_tracker_announces_total{result="rejected"}`: 1,
		"xetpd_tracker_lookups_total":                      1,
		"xetpd_tracker_peers":                              2,
		"xetpd_tracker_xorbs":                              2,
		"xetpd_tracker_files":                              1,
	} {
		if m[key] != want {
			t.Fatalf("after two Full announces, a delta from an unknown peer, a malformed one and a lookup: %s = %v, want %v", key, m[key], want)
		}
	}
	if v, ok := clustertest.Scrape(t, second.URL)[`xetpd_tracker_announces_total{result="ok"}`]; !ok || v != 0 {
		t.Fatalf("second tracker announces ok = %v (present %v), want its own 0", v, ok)
	}
}

// An announce that completes a file wakes seeding at once; the pull outcome is counted.
func TestAnnounceSeedsCompleteFile(t *testing.T) {
	const secret = "s3cret"
	tr, srv := newTestTracker(t, func(cfg *Config) { cfg.Secret, cfg.PeerTTL = secret, time.Hour })
	auth := []string{"Authorization", "Bearer " + secret}
	peers := fakePeers(t, secret, "p1", "p2", "p3")
	announce := func(req apis.AnnounceRequest) {
		t.Helper()
		if status, body, _ := post(t, srv, announceJSON(t, req), auth...); status != http.StatusOK {
			t.Fatalf("announce of %s = %d %q", req.Peer.ID, status, body)
		}
	}
	for _, p := range peers {
		announce(apis.AnnounceRequest{Peer: p.Peer(), Epoch: "e", Full: true})
	}
	ranking := trackerRegistry(tr).rank(seedFile.Hash)
	top, holder := peers[ranking[0].ID], peers[ranking[len(ranking)-1].ID]
	announce(apis.AnnounceRequest{Peer: holder.Peer(), Epoch: "e", AddFiles: []apis.File{seedFile}})
	clustertest.WaitFor(t, time.Second, "a seed pull at the top-ranked non-holder", func() bool { return len(top.Pulls()) == 1 })
	settle(t, trackerSeeder(tr))
	for id, p := range peers {
		want := 0
		if p == top {
			want = 1
		}
		if len(p.Pulls()) != want {
			t.Fatalf("%s saw %d pulls, want %d", id, len(p.Pulls()), want)
		}
	}
	if got := top.Pulls()[0]; got.Req != (apis.PullRequest{File: seedFile.Hash, Seed: true}) || got.Header.Get("Authorization") != "Bearer "+secret {
		t.Fatalf("pull = %+v with Authorization %q", got.Req, got.Header.Get("Authorization"))
	}
	if n := clustertest.Scrape(t, srv.URL)[`xetpd_tracker_seed_requests_total{result="ok"}`]; n != 1 {
		t.Fatalf("seed requests ok = %v, want 1", n)
	}
	for _, p := range peers {
		p.SetPullStatus(http.StatusInternalServerError)
	}
	announce(apis.AnnounceRequest{Peer: holder.Peer(), Epoch: "e", AddFiles: []apis.File{{Hash: strings.Repeat("12", 32), SHA256: strings.Repeat("34", 32)}}})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a refused seed pull counted", func() bool {
		return clustertest.Scrape(t, srv.URL)[`xetpd_tracker_seed_requests_total{result="error"}`] == 1
	})
}

func TestCloseIsIdempotent(t *testing.T) {
	tr, srv := newTestTracker(t, nil)
	for range 2 {
		if err := tr.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if resp, _ := request(t, http.MethodGet, srv.URL+"/healthz", ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz after Close = %d", resp.StatusCode)
	}
}
