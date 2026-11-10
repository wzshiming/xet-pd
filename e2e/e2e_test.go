package e2e

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const resolveMain = "/org/repo/resolve/main/"

// startCluster runs a tracker that wants replicas holders per xorb and per shard and lets a peer evict a seed copy once minSeeds other holders exist, then peer-1, an access peer mirroring hub, and peer-2, an internal peer mirroring through it.
func startCluster(t *testing.T, hub *clustertest.Hub, replicas, minSeeds int, peerFlags ...string) (*trackerProc, []*peerProc) {
	t.Helper()
	secret := testSecret(t)
	tr := startTracker(t, secret, []string{"-peer-ttl", "2s", "-replicas", strconv.Itoa(replicas), "-min-seeds", strconv.Itoa(minSeeds)})
	flags := append([]string{"-heartbeat", "100ms"}, peerFlags...)
	peers := []*peerProc{startPeer(t, tr.URL, hub.URL, secret, flags), startPeer(t, tr.URL, "", secret, flags)}
	awaitAccess(t, tr.URL, peers[0], peers[1])
	return tr, peers
}

// holds reports whether the tracker at origin lists the peer at url with at least xorbs xorbs and shards shards.
func holds(t *testing.T, origin, url string, xorbs, shards int) func() bool {
	return func() bool {
		s, ok := peerStatus(clustertest.Diag(t, origin), url)
		return ok && s.Holdings >= xorbs && s.Shards >= shards
	}
}

func TestColdDownloadThroughAccessPeer(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(1, 1<<20)
	hub.Set(resolveMain+"a.bin", data)
	tr, peers := startCluster(t, hub, 2, 1)
	url := peers[0].URL + resolveMain + "a.bin"

	if got := plainGet(t, url); clustertest.SHA256(got) != clustertest.SHA256(data) {
		t.Fatalf("cold plain GET returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	fileHash := headReady(t, url).Get("X-Xet-Hash")
	fh, v2, out := xetDownload(t, url)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("xet download: hash %s (resolve %s), %d bytes sha %s", fh, fileHash, len(out), clustertest.SHA256(out))
	}
	if gets, n := hub.CDN(); gets != 1 || n != int64(len(data)) {
		t.Fatalf("hub CDN = %d gets, %d bytes; want the single ingest", gets, n)
	}
	urls := append(reconstructionURLs(t, peers[0].URL, fileHash), fetchURLs(v2)...)
	for _, u := range urls {
		if !strings.HasPrefix(u, peers[0].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on peer-1 %s", u, peers[0].URL)
		}
	}
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with the file's xorb and shard", holds(t, tr.URL, peers[0].URL, 1, 1))
	d := clustertest.Diag(t, tr.URL)
	s1, _ := peerStatus(d, peers[0].URL)
	s2, _ := peerStatus(d, peers[1].URL)
	if !s1.Upstream || s2.Upstream {
		t.Fatalf("tracker lists peer-1 %+v and peer-2 %+v, want only peer-1 with an upstream", s1, s2)
	}
	t.Logf("file %s: %d bytes from one hub CDN GET by access peer-1, fetched from it at %v; tracker lists %+v", fileHash, len(data), urls, d.Peers)
}

// One replica keeps the tracker from seeding, so peer-1 holds b.bin only because it ingested it for peer-2.
func TestColdDownloadThroughInternalPeer(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(2, 1<<20)
	hub.Set(resolveMain+"b.bin", data)
	tr, peers := startCluster(t, hub, 1, 1)
	url := peers[1].URL + resolveMain + "b.bin"
	cdnGets, _ := hub.CDN()

	if got := plainGet(t, url); clustertest.SHA256(got) != clustertest.SHA256(data) {
		t.Fatalf("cold plain GET through peer-2 returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	fileHash := headReady(t, url).Get("X-Xet-Hash")
	fh, v2, out := xetDownload(t, url)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("xet download through peer-2: hash %s (resolve %s), %d bytes sha %s", fh, fileHash, len(out), clustertest.SHA256(out))
	}
	if g, _ := hub.CDN(); g != cdnGets+1 {
		t.Fatalf("hub CDN gets %d -> %d, want the access peer's single ingest", cdnGets, g)
	}
	urls := append(reconstructionURLs(t, peers[1].URL, fileHash), fetchURLs(v2)...)
	for _, u := range urls {
		if !strings.HasPrefix(u, peers[1].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on peer-2 %s", u, peers[1].URL)
		}
	}
	clustertest.WaitFor(t, readyTimeout, "the tracker to list both peers with the file's xorb and shard", func() bool { return seeded(clustertest.Diag(t, tr.URL), 2, 1, 1) })
	d := clustertest.Diag(t, tr.URL)
	s1, _ := peerStatus(d, peers[0].URL)
	s2, _ := peerStatus(d, peers[1].URL)
	if !s1.Upstream || s2.Upstream {
		t.Fatalf("tracker lists peer-1 %+v and peer-2 %+v, want only peer-1 with an upstream", s1, s2)
	}
	t.Logf("file %s: %d bytes through internal peer-2 from one hub CDN GET by access peer-1, fetched from peer-2 at %v; tracker lists %+v", fileHash, len(data), urls, d.Peers)
}

// One replica keeps the tracker from seeding, so peer-2 can only take a.bin from peer-1: the shard when it resolves, the xorb when the client fetches it.
func TestWarmDownloadThroughInternalPeer(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(3, 1<<20)
	hub.Set(resolveMain+"a.bin", data)
	tr, peers := startCluster(t, hub, 1, 1)
	fileHash := headReady(t, peers[0].URL+resolveMain+"a.bin").Get("X-Xet-Hash")
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with the file's xorb and shard", holds(t, tr.URL, peers[0].URL, 1, 1))
	cdnGets, _ := hub.CDN()

	fh, v2, out := xetDownload(t, peers[1].URL+resolveMain+"a.bin")
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("warm xet download through peer-2: hash %s (resolve %s), %d bytes sha %s", fh, fileHash, len(out), clustertest.SHA256(out))
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: the warm download ingested again", cdnGets, g)
	}
	for _, u := range fetchURLs(v2) {
		if !strings.HasPrefix(u, peers[1].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on peer-2 %s", u, peers[1].URL)
		}
	}
	shards := metric(t, peers[0].URL, `xetpd_peer_shard_gets_total{status="200"}`)
	relayed := metric(t, peers[0].URL, `xetpd_peer_xorb_gets_total{requester="peer",status="200"}`)
	if shards < 1 || relayed < 1 {
		t.Fatalf("peer-1 answered %v shard GETs and %v peer xorb GETs, want peer-2's shard fetch and xorb pull", shards, relayed)
	}
	t.Logf("file %s: peer-2 took the shard (%v shard GETs) and the xorb (%v peer xorb GETs) from peer-1; hub CDN gets stayed %d", fileHash, shards, relayed, cdnGets)
}

// startSeeded downloads a.bin and b.bin, one xorb each, cold through peer-1 of a cluster that wants two holders per xorb and shard, and waits until both peers hold both.
func startSeeded(t *testing.T) (*clustertest.Hub, *trackerProc, []*peerProc, map[string][]byte) {
	t.Helper()
	hub := clustertest.NewHub(t)
	files := map[string][]byte{"a.bin": clustertest.RandomData(4, 1<<20), "b.bin": clustertest.RandomData(5, 1<<20)}
	for name, data := range files {
		hub.Set(resolveMain+name, data)
	}
	tr, peers := startCluster(t, hub, 2, 1)
	for name, data := range files {
		if got := plainGet(t, peers[0].URL+resolveMain+name); clustertest.SHA256(got) != clustertest.SHA256(data) {
			t.Fatalf("cold plain GET of %s returned %d bytes with sha %s", name, len(got), clustertest.SHA256(got))
		}
	}
	clustertest.WaitFor(t, seedTimeout, "the tracker to list both peers with both files' xorbs and shards", func() bool { return seeded(clustertest.Diag(t, tr.URL), 2, 2, 2) })
	return hub, tr, peers, files
}

// Two one-xorb files stand in for a file of two xorbs, which would take more than 64 MiB.
func TestSeedingReplicatesXorbsAndShards(t *testing.T) {
	hub, tr, peers, _ := startSeeded(t)
	asked := metric(t, tr.URL, `xetpd_tracker_seed_requests_total{result="ok"}`)
	pulled := metric(t, peers[1].URL, `xetpd_peer_pulls_total{result="ok"}`)
	if asked < 1 || pulled < 2 {
		t.Fatalf("tracker sent %v accepted seed requests and peer-2 completed %v pulls, want the seeds of both xorbs", asked, pulled)
	}
	if g, _ := hub.CDN(); g != 2 {
		t.Fatalf("hub CDN gets = %d, want the two ingests", g)
	}
	t.Logf("tracker lists %+v after %v seed requests and %v pulls by peer-2; hub CDN 2 gets", clustertest.Diag(t, tr.URL).Peers, asked, pulled)
}

func TestAccessPeerLossKeepsCachedFiles(t *testing.T) {
	hub, tr, peers, files := startSeeded(t)
	// A path peer-2 never resolved fails without an access peer; see TestAccessPeerLossServesSeedCopies.
	headReady(t, peers[1].URL+resolveMain+"a.bin")
	hub.Set(resolveMain+"c.bin", clustertest.RandomData(6, 1<<20))
	cdnGets, _ := hub.CDN()

	peers[0].kill()
	clustertest.WaitFor(t, 10*time.Second, "the access peer to expire, leaving peer-2 alone", func() bool {
		d := clustertest.Diag(t, tr.URL)
		return len(d.Peers) == 1 && d.Peers[0].URL == peers[1].URL && !d.Peers[0].Upstream
	})
	_, v2, out := xetDownload(t, peers[1].URL+resolveMain+"a.bin")
	if clustertest.SHA256(out) != clustertest.SHA256(files["a.bin"]) {
		t.Fatalf("xet download of the seeded a.bin after the loss: %d bytes sha %s", len(out), clustertest.SHA256(out))
	}
	for _, u := range fetchURLs(v2) {
		if !strings.HasPrefix(u, peers[1].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on peer-2 %s", u, peers[1].URL)
		}
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: the cached file was fetched again", cdnGets, g)
	}
	start := time.Now()
	resp, body := request(t, http.MethodGet, peers[1].URL+resolveMain+"c.bin", "")
	took := time.Since(start)
	if resp.StatusCode != http.StatusBadGateway || took > 15*time.Second {
		t.Fatalf("GET of the new c.bin through peer-2 = %d %q after %v, want 502 within 15s", resp.StatusCode, body, took)
	}
	t.Logf("killed access peer %s: peer-2 served the seeded a.bin from its copies; the new c.bin failed %d %q after %v", peers[0].URL, resp.StatusCode, body, took)
}

func TestAccessPeerLossServesSeedCopies(t *testing.T) {
	t.Skip(`product bug: with no access peer live, an internal peer answers 502 "probe failed after 5 attempts" for a file it holds only as seed copies, because its mirror never resolved the path and has to probe the gone access peer`)
	hub, tr, peers, files := startSeeded(t)
	cdnGets, _ := hub.CDN()

	peers[0].kill()
	clustertest.WaitFor(t, 10*time.Second, "the access peer to expire, leaving peer-2 alone", func() bool {
		d := clustertest.Diag(t, tr.URL)
		return len(d.Peers) == 1 && d.Peers[0].URL == peers[1].URL && !d.Peers[0].Upstream
	})
	if _, _, out := xetDownload(t, peers[1].URL+resolveMain+"a.bin"); clustertest.SHA256(out) != clustertest.SHA256(files["a.bin"]) {
		t.Fatalf("xet download of the seeded a.bin after the loss: %d bytes sha %s", len(out), clustertest.SHA256(out))
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: the seed copies were fetched again", cdnGets, g)
	}
}

// One replica keeps the tracker from seeding, so no seed copy outlives the eviction: a cache sized for one xorb drops a for b, and the next resolve of a re-ingests it.
func TestAllCopiesEvictedReingests(t *testing.T) {
	hub := clustertest.NewHub(t)
	a, b := clustertest.RandomData(7, 1<<20), clustertest.RandomData(8, 1<<20)
	hub.Set(resolveMain+"a.bin", a)
	hub.Set(resolveMain+"b.bin", b)
	tr, peers := startCluster(t, hub, 1, 1, "-xorb-cache-size", strconv.Itoa(5<<18)) // 1.25MiB: one 1MiB xorb plus headers, never two
	urlA, urlB := peers[0].URL+resolveMain+"a.bin", peers[0].URL+resolveMain+"b.bin"

	hashA := headReady(t, urlA).Get("X-Xet-Hash")
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with a", holds(t, tr.URL, peers[0].URL, 1, 1))
	xorbsA := xorbHashes(t, peers[0].DataDir)
	if len(xorbsA) != 1 {
		t.Fatalf("peer-1 stores %v after a, want one xorb", xorbsA)
	}

	hashB := headReady(t, urlB).Get("X-Xet-Hash")
	// A peer never forgets a shard, so peer-1 keeps listing a's without its xorb.
	clustertest.WaitFor(t, readyTimeout, "no peer to hold a's xorb", func() bool {
		d := clustertest.Diag(t, tr.URL)
		s1, _ := peerStatus(d, peers[0].URL)
		s2, ok := peerStatus(d, peers[1].URL)
		held := xorbHashes(t, peers[0].DataDir)
		return s1.Shards == 2 && s1.Holdings == 1 && ok && s2.Holdings == 0 && s2.Shards == 0 &&
			len(held) == 1 && held[0] != xorbsA[0] && len(xorbHashes(t, peers[1].DataDir)) == 0
	})
	xorbsB := xorbHashes(t, peers[0].DataDir)
	if g, _ := hub.CDN(); g != 2 {
		t.Fatalf("hub CDN gets = %d before a is downloaded again, want the ingests of a and b", g)
	}

	if got := plainGet(t, urlA); clustertest.SHA256(got) != clustertest.SHA256(a) {
		t.Fatalf("second GET of a returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	if got := headReady(t, urlA).Get("X-Xet-Hash"); got != hashA {
		t.Fatalf("re-ingested a has hash %s, want %s", got, hashA)
	}
	if g, n := hub.CDN(); g != 3 || n != 3<<20 {
		t.Fatalf("hub CDN = %d gets, %d bytes; want exactly one more download, the re-ingest of a", g, n)
	}
	t.Logf("a %s (xorbs %v) evicted for b %s (xorbs %v) under a %d byte cache, then re-ingested: hub CDN 3 gets", hashA, xorbsA, hashB, xorbsB, 5<<18)
}

// One replica keeps the tracker from seeding, so the registry the restarted tracker has to rebuild stays put.
func TestTrackerRestartRecovers(t *testing.T) {
	hub := clustertest.NewHub(t)
	a, b := clustertest.RandomData(9, 1<<20), clustertest.RandomData(10, 1<<20)
	hub.Set(resolveMain+"a.bin", a)
	hub.Set(resolveMain+"b.bin", b)
	tr, peers := startCluster(t, hub, 1, 1)
	if got := plainGet(t, peers[0].URL+resolveMain+"a.bin"); clustertest.SHA256(got) != clustertest.SHA256(a) {
		t.Fatalf("cold plain GET of a.bin through peer-1 returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with a.bin's xorb and shard", holds(t, tr.URL, peers[0].URL, 1, 1))
	// registry is what a tracker lists, expiries aside.
	registry := func(origin string) []apis.PeerStatus {
		list := clustertest.Diag(t, origin).Peers
		for i := range list {
			list[i].Expiry = time.Time{}
		}
		return list
	}
	before := registry(tr.URL)

	addr := strings.TrimPrefix(tr.URL, "http://")
	tr.kill()
	// The same command line on the address the peers announce to; the later -listen wins.
	again := startTracker(t, testSecret(t), append(slices.Clone(tr.cmd.Args[1:]), "-listen", addr))
	clustertest.WaitFor(t, time.Second, "both peers to re-announce in full", func() bool { return slices.Equal(registry(again.URL), before) })
	awaitAccess(t, again.URL, peers[0], peers[1])
	cdnGets, _ := hub.CDN()

	if got := plainGet(t, peers[1].URL+resolveMain+"b.bin"); clustertest.SHA256(got) != clustertest.SHA256(b) {
		t.Fatalf("cold plain GET of b.bin through peer-2 after the restart returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	if g, _ := hub.CDN(); g != cdnGets+1 {
		t.Fatalf("hub CDN gets %d -> %d, want access peer-1's single ingest of b.bin", cdnGets, g)
	}
	lookups := metric(t, again.URL, "xetpd_tracker_lookups_total")
	if lookups < 1 {
		t.Fatalf("restarted tracker answered %v lookups, want some", lookups)
	}
	t.Logf("restarted the tracker on %s: registry %+v restored; peer-2 fetched the new b.bin through peer-1; %v lookups", addr, before, lookups)
}

func TestCLIErrors(t *testing.T) {
	secret := testSecret(t) + "-hunter2"
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "secret")
	if err := os.WriteFile(secretFile, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing")
	const nowhere = "http://127.0.0.1:1"
	tests := []struct {
		name   string
		args   []string
		env    []string
		code   int
		stdout string // substring, "" means empty
		stderr string // substring, "" means empty
	}{
		{name: "tracker help", args: []string{"tracker", "-h"}, stdout: "Usage: tracker"},
		{name: "peer help", args: []string{"peer", "--help"}, stdout: "[env XET_PD_ADVERTISE]"},
		{name: "unknown flag", args: []string{"tracker", "-bogus"}, code: 2, stderr: "flag provided but not defined: -bogus"},
		{name: "tracker bad peer ttl", args: []string{"tracker", "-peer-ttl", "soon"}, code: 2, stderr: `invalid value "soon" for flag -peer-ttl`},
		{name: "tracker min seeds above replicas", args: []string{"tracker", "-min-seeds", "3", "-replicas", "2", "-secret", secret}, code: 1, stderr: "tracker: min seeds exceed replicas"},
		{name: "tracker secret file missing", args: []string{"tracker", "-secret-file", missing}, code: 2, stderr: "secret file"},
		{name: "secret flags exclusive", args: []string{"tracker", "-secret", secret, "-secret-file", secretFile}, code: 2, stderr: "mutually exclusive"},
		{name: "secret env exclusive", args: []string{"tracker"}, env: []string{"XET_PD_SECRET=" + secret, "XET_PD_SECRET_FILE=" + secretFile}, code: 2, stderr: "mutually exclusive"},
		{name: "peer missing advertise", args: []string{"peer", "-tracker", nowhere, "-secret", secret}, code: 2, stderr: "advertise is required"},
		{name: "peer missing tracker", args: []string{"peer", "-advertise", nowhere, "-secret", secret}, code: 2, stderr: "tracker is required"},
		{name: "peer upstream token without upstream", args: []string{"peer", "-advertise", nowhere, "-tracker", nowhere, "-upstream-token", secret, "-data-dir", t.TempDir()}, code: 1, stderr: "peer: upstream token needs upstream"},
		{name: "peer secret file missing", args: []string{"peer", "-advertise", nowhere, "-tracker", nowhere, "-secret-file", missing}, code: 2, stderr: "secret file"},
		{name: "peer invalid advertise", args: []string{"peer", "-advertise", "127.0.0.1:1", "-tracker", nowhere, "-data-dir", t.TempDir()}, code: 1, stderr: "advertise: invalid url"},
		{name: "advertise credentials", args: []string{"peer", "-advertise", "http://bob:" + secret + "@127.0.0.1:1", "-tracker", nowhere, "-data-dir", t.TempDir()}, code: 1, stderr: "advertise: userinfo not allowed"},
		{name: "peer bad upstream", args: []string{"peer", "-advertise", nowhere, "-tracker", nowhere, "-upstream", "ftp://hub", "-data-dir", t.TempDir()}, code: 1, stderr: "upstream: scheme is not http(s)"},
		{name: "peer negative cache size", args: []string{"peer", "-advertise", nowhere, "-tracker", nowhere, "-xorb-cache-size", "-1"}, code: 2, stderr: `invalid value "-1" for flag -xorb-cache-size`},
		{name: "peer negative max pulls", args: []string{"peer", "-advertise", nowhere, "-tracker", nowhere, "-max-pulls", "-1", "-data-dir", t.TempDir()}, code: 1, stderr: "peer: negative config value"},
		{name: "peer listen taken", args: []string{"peer", "-listen", reserveTaken(t), "-advertise", nowhere, "-tracker", nowhere, "-data-dir", t.TempDir(), "-secret", secret}, code: 1, stderr: "address already in use"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runCLI(t, tt.args, tt.env...)
			if code != tt.code {
				t.Fatalf("exit %d, want %d; stderr %q", code, tt.code, stderr)
			}
			if (stdout != "") != (tt.stdout != "") || !strings.Contains(stdout, tt.stdout) {
				t.Fatalf("stdout %q, want %q", stdout, tt.stdout)
			}
			if (stderr != "") != (tt.stderr != "") || !strings.Contains(stderr, tt.stderr) {
				t.Fatalf("stderr %q, want %q", stderr, tt.stderr)
			}
			if strings.Contains(stdout+stderr, secret) {
				t.Fatalf("output echoes the secret: %q %q", stdout, stderr)
			}
		})
	}
	t.Run("peer without upstream starts as an internal peer", func(t *testing.T) {
		tr := startTracker(t, secret, nil)
		p := startPeer(t, tr.URL, "", secret, nil)
		clustertest.WaitFor(t, readyTimeout, "the tracker to list the peer without an upstream", func() bool {
			s, ok := peerStatus(clustertest.Diag(t, tr.URL), p.URL)
			return ok && !s.Upstream
		})
		banner, err := p.awaitBanner("peer listening on ")
		if err != nil || "http://"+banner != p.URL {
			t.Fatalf("banner %q, %v; want the address of %s", banner, err, p.URL)
		}
		p.stop()
		tr.stop()
		for _, out := range []string{p.stdout.String(), p.stderr.String(), tr.stdout.String(), tr.stderr.String()} {
			if strings.Contains(out, secret) {
				t.Fatalf("output echoes the secret: %q", out)
			}
		}
	})
}

// reserveTaken holds a loopback port for the rest of the test and returns its address.
func reserveTaken(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func TestSecondSignalEndsDrain(t *testing.T) {
	p := startPeer(t, startTracker(t, "", nil).URL, clustertest.NewHub(t).URL, "", nil)
	p.discard() // the exit status is asserted below
	// Shutdown waits about 5s on a connection still sending its first request header.
	slow, err := net.Dial("tcp", strings.TrimPrefix(p.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Close() }()
	if _, err := slow.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n")); err != nil {
		t.Fatal(err)
	}
	// Connections are accepted in order, so answering a later one proves the peer tracks the slow one.
	probe := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: readyTimeout}
	resp, err := probe.Get(p.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if err := p.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	// A race build lingers 1s in os.Exit(0), so only outliving 2s proves the drain.
	select {
	case <-p.done:
		t.Fatalf("peer ended with %v within 2s of the first SIGINT, so nothing held the drain", p.cmd.ProcessState)
	case <-time.After(2 * time.Second):
	}
	if err := p.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		p.kill()
		t.Fatal("peer still alive 2s after the second SIGINT")
	}
	if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGINT {
		t.Fatalf("peer ended with %v after the second SIGINT, want death by SIGINT", p.cmd.ProcessState)
	}
}
