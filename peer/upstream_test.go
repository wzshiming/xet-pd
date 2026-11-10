package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/auth"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
	"github.com/wzshiming/xet-pd/tracker"
)

func TestRepoOfHubPaths(t *testing.T) {
	for path, want := range map[string]string{
		"/org/repo/resolve/main/f":             "org/repo",
		"/datasets/org/repo/resolve/main/f":    "org/repo",
		"/spaces/org/repo/resolve/main/f":      "org/repo",
		"/api/models/org/repo/paths-info/main": "org/repo",
		"/api/datasets/org/repo/tree/main":     "org/repo",
		"/api/whoami-v2":                       "",
		"/":                                    "",
		"/org":                                 "",
	} {
		if got := repoOf(path); got != want {
			t.Errorf("repoOf(%q) = %q, want %q", path, got, want)
		}
	}
}

// listing returns an announcer whose last reply listed peers, or one that has had no reply when peers is nil.
func listing(peers []apis.Peer) *announcer {
	a := &announcer{}
	if peers != nil {
		a.lastUpstreams.Store(&peers)
	}
	return a
}

func TestAccessUpstreamWithoutAccessPeersFails(t *testing.T) {
	for _, peers := range [][]apis.Peer{nil, {}} {
		base, token, err := (&accessUpstream{ann: listing(peers)}).Resolve(context.Background(), auth.Read)
		if !errors.Is(err, errNoUpstream) || base != "" || token != "" {
			t.Fatalf("Resolve with upstreams %#v = %q, %q, %v; want %v", peers, base, token, err, errNoUpstream)
		}
	}
}

func TestAccessUpstreamRanksTheLastListWhileNoneIsListed(t *testing.T) {
	peers := []apis.Peer{
		{ID: "p1", URL: "http://p1", Upstream: true}, {ID: "p2", URL: "http://p2", Upstream: true}, {ID: "p3", URL: "http://p3", Upstream: true},
		{ID: "p4", URL: "http://p4", Upstream: true}, {ID: "p5", URL: "http://p5", Upstream: true},
	}
	ann := listing(peers)
	up := &accessUpstream{ann: ann}
	if _, _, err := up.Resolve(context.Background(), auth.Read); err != nil {
		t.Fatal(err)
	}
	ann.lastUpstreams.Store(&[]apis.Peer{})
	for _, repo := range []string{"org/repo", "org/other", "user/model"} {
		base, token, err := up.Resolve(context.WithValue(context.Background(), repoKey{}, repo), auth.Read)
		if want := apis.Rank(repo, peers)[0].URL; err != nil || base != want || token != "" {
			t.Fatalf("Resolve(%q) with none listed after %v = %q, %q, %v; want the last list's first %q", repo, peers, base, token, err, want)
		}
	}
}

func TestAccessUpstreamKeepsTheLatestListItSaw(t *testing.T) {
	x, y := apis.Peer{ID: "x", URL: "http://x.test", Upstream: true}, apis.Peer{ID: "y", URL: "http://y.test", Upstream: true}
	ann := listing([]apis.Peer{x})
	up := &accessUpstream{ann: ann}
	if base, _, err := up.Resolve(context.Background(), auth.Read); err != nil || base != x.URL {
		t.Fatalf("Resolve with x listed = %q, %v; want %q", base, err, x.URL)
	}
	ann.lastUpstreams.Store(&[]apis.Peer{y})
	if base, _, err := up.Resolve(context.Background(), auth.Read); err != nil || base != y.URL {
		t.Fatalf("Resolve with y listed = %q, %v; want %q", base, err, y.URL)
	}
	ann.lastUpstreams.Store(&[]apis.Peer{})
	if base, _, err := up.Resolve(context.Background(), auth.Read); err != nil || base != y.URL {
		t.Fatalf("Resolve with none listed after x, then y = %q, %v; want y's %q", base, err, y.URL)
	}
}

func TestAccessUpstreamPicksTheRankFirstOfTheRepository(t *testing.T) {
	peers := []apis.Peer{
		{ID: "p1", URL: "http://p1", Upstream: true}, {ID: "p2", URL: "http://p2", Upstream: true}, {ID: "p3", URL: "http://p3", Upstream: true},
		{ID: "p4", URL: "http://p4", Upstream: true}, {ID: "p5", URL: "http://p5", Upstream: true},
	}
	up := &accessUpstream{ann: listing(peers)}
	for _, repo := range []string{"org/repo", "org/other", "user/model", ""} {
		for _, perm := range []auth.Permission{auth.Read, auth.Write} {
			base, token, err := up.Resolve(context.WithValue(context.Background(), repoKey{}, repo), perm)
			if want := apis.Rank(repo, peers)[0].URL; err != nil || base != want || token != "" {
				t.Fatalf("Resolve(%q, %v) = %q, %q, %v; want %q without a token", repo, perm, base, token, err, want)
			}
		}
	}
	// Outside a request no repository is set: the choice is Rank's first for "".
	if base, _, err := up.Resolve(context.Background(), auth.Read); err != nil || base != apis.Rank("", peers)[0].URL {
		t.Fatalf("Resolve outside a request = %q, %v; want %q", base, err, apis.Rank("", peers)[0].URL)
	}
}

func TestAccessUpstreamFollowsTheTrackerReplies(t *testing.T) {
	x, y := apis.Peer{ID: "x", URL: "http://x.test", Upstream: true}, apis.Peer{ID: "y", URL: "http://y.test", Upstream: true}
	fake := clustertest.NewFakeTracker(t)
	p, _ := newTestPeer(t, fake.URL, func(c *Config) { c.Upstream, c.Heartbeat = "", 20*time.Millisecond })
	up := &accessUpstream{ann: p.ann}
	ctx := context.WithValue(context.Background(), repoKey{}, "org/repo")
	resolves := func(what string, ok func(base string, err error) bool) {
		t.Helper()
		clustertest.WaitFor(t, clustertest.WaitTimeout, what, func() bool {
			base, _, err := up.Resolve(ctx, auth.Read)
			return ok(base, err)
		})
	}
	fake.SetUpstreams(x)
	resolves("a resolve to x after a reply listing x", func(base string, err error) bool { return err == nil && base == x.URL })
	fake.SetUpstreams(y)
	resolves("a resolve to y after a reply listing y", func(base string, err error) bool { return err == nil && base == y.URL })
	fake.SetUpstreams()
	awaitUpstreams(t, p)
	if base, _, err := up.Resolve(ctx, auth.Read); err != nil || base != y.URL {
		t.Fatalf("Resolve after a reply listing none = %q, %v; want the last listed %q", base, err, y.URL)
	}
}

// startTracker serves a real tracker requiring testSecret and returns its origin.
func startTracker(t *testing.T, cfg tracker.Config) string {
	t.Helper()
	cfg.Secret = testSecret
	tr, err := tracker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(tr)
	t.Cleanup(func() {
		srv.Close()
		_ = tr.Close()
	})
	return srv.URL
}

// awaitUpstreams waits until p's announcer lists exactly the access peers at urls.
func awaitUpstreams(t *testing.T, p *Peer, urls ...string) {
	t.Helper()
	want := slices.Sorted(slices.Values(urls))
	clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("the access peers %v", want), func() bool {
		var got []string
		for _, u := range p.ann.upstreams() {
			got = append(got, u.URL)
		}
		slices.Sort(got)
		return slices.Equal(got, want)
	})
}

// registered returns the entry of p in the registry of the tracker at trackerURL, the zero PeerStatus without one.
func registered(t *testing.T, trackerURL string, p *Peer) apis.PeerStatus {
	t.Helper()
	for _, s := range clustertest.Diag(t, trackerURL).Peers {
		if s.ID == peerID(p) {
			return s
		}
	}
	return apis.PeerStatus{}
}

// listed reports whether the tracker at trackerURL lists p with Upstream as given, one shard and at least one xorb.
func listed(t *testing.T, trackerURL string, p *Peer, upstream bool) bool {
	t.Helper()
	s := registered(t, trackerURL, p)
	return s.Upstream == upstream && s.Shards == 1 && s.Holdings >= 1
}

func TestInternalPeerColdResolvesThroughTheAccessPeer(t *testing.T) {
	data := clustertest.RandomData(99, 256<<10)
	hub := clustertest.NewHub(t)
	hub.Set(modelPath, data)
	atHub := &recorder{}
	front := httptest.NewServer(atHub.wrap(hub.Config.Handler))
	t.Cleanup(front.Close)
	trackerURL := startTracker(t, tracker.Config{Replicas: 1})
	pa, a, atA := startRecordedNode(t, trackerURL, front.URL)
	pb, b := startNode(t, trackerURL, "")
	awaitUpstreams(t, pb, a)
	if resp, got, err := do(t, http.MethodGet, b+modelPath); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("cold GET on the internal peer = %d, %d bytes, %v; want the hub's %d bytes", resp.StatusCode, len(got), err, len(data))
	}
	fileHash := waitReady(t, b+modelPath)
	if gets, _ := hub.CDN(); gets != 1 {
		t.Fatalf("hub CDN gets = %d, want the access peer's single ingest", gets)
	}
	resolves := 0
	for _, r := range atA.requests() {
		if strings.HasPrefix(r.path, "/xet-pd/") {
			continue
		}
		if r.peer != peerID(pb) {
			t.Fatalf("the access peer got %s %s with %s %q, want the internal peer's ID %q", r.method, r.path, apis.PeerHeader, r.peer, peerID(pb))
		}
		if strings.Contains(r.path, "/resolve/") {
			resolves++
		}
	}
	if resolves == 0 {
		t.Fatalf("the access peer got no resolve from the internal peer: %v", atA.requests())
	}
	for _, r := range atHub.requests() {
		if r.peer != "" {
			t.Fatalf("the hub got %s %s with %s %q, want none from an access peer", r.method, r.path, apis.PeerHeader, r.peer)
		}
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the tracker to list both peers with the shard and its xorbs", func() bool {
		return listed(t, trackerURL, pa, true) && listed(t, trackerURL, pb, false)
	})
	for _, u := range fetchURLs(t, b, fileHash) {
		if !strings.HasPrefix(u, b+apis.XorbPathPrefix) {
			t.Fatalf("fetch url %s, want one on the internal peer %s", u, b)
		}
	}
	settled(t, pb)
}

func TestInternalPeerWarmResolveTakesTheAccessPeersShard(t *testing.T) {
	data := clustertest.RandomData(100, 256<<10)
	hub := clustertest.NewHub(t)
	hub.Set(modelPath, data)
	trackerURL := startTracker(t, tracker.Config{Replicas: 1})
	pa, a, atA := startRecordedNode(t, trackerURL, hub.URL)
	if resp, got, err := do(t, http.MethodGet, a+modelPath); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("cold GET on the access peer = %d, %d bytes, %v; want the hub's %d bytes", resp.StatusCode, len(got), err, len(data))
	}
	fileHash := waitReady(t, a+modelPath)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the tracker to list the access peer with the shard", func() bool { return listed(t, trackerURL, pa, true) })
	pb, b := startNode(t, trackerURL, "")
	awaitUpstreams(t, pb, a)
	if got := waitReady(t, b+modelPath); got != fileHash {
		t.Fatalf("the internal peer resolves the file as %s, want %s", got, fileHash)
	}
	// The probe of the access peer carried the file's SHA-256, so the mirror published the shard it took instead of ingesting.
	if v := clustertest.Scrape(t, a)[`xetpd_peer_shard_gets_total{status="200"}`]; v < 1 {
		t.Fatalf("the access peer answered %v shard GETs, want the internal peer's", v)
	}
	for _, r := range atA.requests() {
		if r.peer == peerID(pb) && r.method == http.MethodGet && !strings.HasPrefix(r.path, "/xet-pd/") {
			t.Fatalf("the internal peer ingested through GET %s, want the shard alone", r.path)
		}
	}
	if resp, got, err := do(t, http.MethodGet, b+modelPath); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("warm GET on the internal peer = %d, %d bytes, %v; want the hub's %d bytes", resp.StatusCode, len(got), err, len(data))
	}
	if v := clustertest.Scrape(t, a)[`xetpd_peer_xorb_gets_total{requester="peer",status="200"}`]; v < 1 {
		t.Fatalf("the access peer served %v xorb GETs to peers, want the internal peer's pulls", v)
	}
	if gets, _ := hub.CDN(); gets != 1 {
		t.Fatalf("hub CDN gets = %d, want the access peer's ingest alone", gets)
	}
	settled(t, pb)
}

func TestInternalPeerSendsEachRepositoryToOneAccessPeer(t *testing.T) {
	hub := clustertest.NewHub(t)
	trackerURL := startTracker(t, tracker.Config{Replicas: 1})
	pa1, a1, at1 := startRecordedNode(t, trackerURL, hub.URL)
	pa2, a2, at2 := startRecordedNode(t, trackerURL, hub.URL)
	pb, b := startNode(t, trackerURL, "")
	awaitUpstreams(t, pb, a1, a2)
	access := []apis.Peer{{ID: peerID(pa1), URL: a1}, {ID: peerID(pa2), URL: a2}}
	peers, at := map[string]*Peer{a1: pa1, a2: pa2}, map[string]*recorder{a1: at1, a2: at2}
	repo, repo2 := "org/repo", ""
	first := apis.Rank(repo, access)[0].URL
	for i := range 64 {
		if name := fmt.Sprintf("org/repo-%d", i); apis.Rank(name, access)[0].URL != first {
			repo2 = name
			break
		}
	}
	if repo2 == "" {
		t.Fatal("no repository ranks the other access peer first")
	}
	other := apis.Rank(repo2, access)[0].URL
	resolve := func(path string, seed byte) {
		t.Helper()
		data := clustertest.RandomData(seed, 64<<10)
		hub.Set(path, data)
		if resp, got, err := do(t, http.MethodGet, b+path); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
			t.Fatalf("GET %s on the internal peer = %d, %d bytes, %v; want the hub's %d bytes", path, resp.StatusCode, len(got), err, len(data))
		}
	}
	resolves := func(origin, repo string) int {
		n := 0
		for _, r := range at[origin].requests() {
			if r.peer == peerID(pb) && strings.HasPrefix(r.path, "/"+repo+"/resolve/") {
				n++
			}
		}
		return n
	}
	for i := range 5 {
		resolve(fmt.Sprintf("/%s/resolve/main/f%d.bin", repo, i), byte(110+i))
	}
	resolve("/"+repo2+"/resolve/main/f.bin", 120)
	if got, stray := resolves(first, repo), resolves(other, repo); got < 5 || stray != 0 {
		t.Fatalf("%s: %d resolves at its Rank-first access peer %s, %d at %s; want at least 5 and none", repo, got, first, stray, other)
	}
	if got, stray := resolves(other, repo2), resolves(first, repo2); got < 1 || stray != 0 {
		t.Fatalf("%s: %d resolves at its Rank-first access peer %s, %d at %s; want some and none", repo2, got, other, stray, first)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "each access peer to hold the shards of its repositories alone", func() bool {
		return registered(t, trackerURL, peers[first]).Shards == 5 && registered(t, trackerURL, peers[other]).Shards == 1 && registered(t, trackerURL, pb).Shards == 6
	})
	settled(t, pb)
}

func TestInternalPeerWithoutAccessPeers(t *testing.T) {
	cached, fresh := "/org/repo/resolve/main/cached.bin", "/org/repo/resolve/main/fresh.bin"
	data := clustertest.RandomData(101, 64<<10)
	hub := clustertest.NewHub(t)
	hub.Set(cached, data)
	hub.Set(fresh, clustertest.RandomData(102, 64<<10))
	trackerURL := startTracker(t, tracker.Config{Replicas: 1, PeerTTL: time.Second})
	pb, b := startNode(t, trackerURL, "")
	// A cold GET probes a gone access peer for about 5 s before it fails.
	client := &http.Client{Timeout: 3 * clustertest.WaitTimeout}
	get := func(path string) (int, []byte) {
		t.Helper()
		resp, err := client.Get(b + path)
		if err != nil {
			t.Fatalf("GET %s on the internal peer: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the internal peer's first announce", pb.ann.ready.Load)
	if code, body := get(fresh); code != http.StatusBadGateway || string(body) != errNoUpstream.Error()+"\n" {
		t.Fatalf("GET %s before any access peer = %d %q, want 502 %q", fresh, code, body, errNoUpstream)
	}
	// Not startNode: the access peer's listener has to close with it.
	srv := httptest.NewUnstartedServer(nil)
	a := "http://" + srv.Listener.Addr().String()
	pa, err := New(Config{Advertise: a, Tracker: trackerURL, Upstream: hub.URL, DataDir: t.TempDir(), Secret: testSecret, Heartbeat: 50 * time.Millisecond})
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	srv.Config.Handler = pa
	srv.Start()
	t.Cleanup(func() {
		_ = pa.Close()
		srv.Close()
	})
	awaitUpstreams(t, pb, a)
	if code, body := get(cached); code != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("GET %s through the access peer = %d, %d bytes; want the hub's %d", cached, code, len(body), len(data))
	}
	waitReady(t, b+cached)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the tracker to list the access peer with the shard", func() bool { return listed(t, trackerURL, pa, true) })
	_ = pa.Close()
	srv.Close()
	awaitUpstreams(t, pb)
	cdn, _ := hub.CDN()
	if code, body := get(cached); code != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("GET %s once the access peer expired = %d, %d bytes; want the cached %d", cached, code, len(body), len(data))
	}
	if gets, _ := hub.CDN(); gets != cdn {
		t.Fatalf("hub CDN gets %d -> %d: the cached file was fetched again", cdn, gets)
	}
	start := time.Now()
	code, body := get(fresh)
	if code < 500 || code > 599 {
		t.Fatalf("GET %s once the access peer expired = %d %q, want a 5xx", fresh, code, body)
	}
	t.Logf("GET %s once the access peer expired = %d %q after %v", fresh, code, body, time.Since(start))
	settled(t, pb)
}
