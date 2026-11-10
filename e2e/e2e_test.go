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

const modelPath = "/org/repo/resolve/main/model.bin"

// startCluster runs a tracker that wants replicas complete holders per file and lets a peer evict a seed copy once minSeeds other holders exist, plus two peers mirroring hub.
func startCluster(t *testing.T, hub *clustertest.Hub, replicas, minSeeds int, peerFlags ...string) (*trackerProc, []*peerProc) {
	t.Helper()
	secret := testSecret(t)
	tr := startTracker(t, secret, []string{"-peer-ttl", "2s", "-replicas", strconv.Itoa(replicas), "-min-seeds", strconv.Itoa(minSeeds)})
	var peers []*peerProc
	for range 2 {
		peers = append(peers, startPeer(t, tr.URL, hub.URL, secret, append([]string{"-heartbeat", "100ms"}, peerFlags...)))
	}
	clustertest.WaitFor(t, readyTimeout, "both peers to register", func() bool { return len(clustertest.Diag(t, tr.URL).Peers) == 2 })
	return tr, peers
}

// holds reports whether the tracker at origin lists the peer at url with files complete files.
func holds(t *testing.T, origin, url string, files int) func() bool {
	return func() bool {
		s, ok := peerStatus(clustertest.Diag(t, origin), url)
		return ok && s.Shards == files
	}
}

func TestColdDownloadThroughPeer(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(1, 1<<20)
	hub.Set(modelPath, data)
	tr, peers := startCluster(t, hub, 2, 1)
	url := peers[0].URL + modelPath

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
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with the file", func() bool {
		s, ok := peerStatus(clustertest.Diag(t, tr.URL), peers[0].URL)
		return ok && s.Shards == 1 && s.Holdings >= 1
	})
	t.Logf("file %s: %d bytes from one hub CDN GET, fetched from peer-1 at %v", fileHash, len(data), urls)
}

// One replica keeps the tracker from seeding, so peer-2 can only get the file by pulling it from peer-1 on demand.
func TestWarmDownloadOnSecondPeerPullsFromFirst(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(2, 1<<20)
	hub.Set(modelPath, data)
	tr, peers := startCluster(t, hub, 1, 1)
	fileHash := headReady(t, peers[0].URL+modelPath).Get("X-Xet-Hash")
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with the file", holds(t, tr.URL, peers[0].URL, 1))
	cdnGets, _ := hub.CDN()

	fh, v2, out := xetDownload(t, peers[1].URL+modelPath)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("warm xet download: hash %s (resolve %s), %d bytes sha %s", fh, fileHash, len(out), clustertest.SHA256(out))
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: peer-2 ingested from the hub", cdnGets, g)
	}
	for _, u := range fetchURLs(v2) {
		if !strings.HasPrefix(u, peers[1].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on peer-2 %s", u, peers[1].URL)
		}
	}
	relayed := metric(t, peers[0].URL, `xetpd_peer_xorb_gets_total{requester="peer",status="200"}`)
	if relayed < 1 {
		t.Fatalf("peer-1 served %v xorb GETs to peers, want peer-2's pull", relayed)
	}
	clustertest.WaitFor(t, readyTimeout, "the tracker to list both peers with the file", func() bool { return seeded(clustertest.Diag(t, tr.URL), 2, 1) })
	t.Logf("file %s: peer-2 pulled it from peer-1 (%v relayed xorb GETs); hub CDN gets stayed %d", fileHash, relayed, cdnGets)
}

func TestSeedingReplicatesCompleteFiles(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(3, 1<<20)
	hub.Set(modelPath, data)
	tr, peers := startCluster(t, hub, 2, 1)
	fileHash := headReady(t, peers[0].URL+modelPath).Get("X-Xet-Hash")

	clustertest.WaitFor(t, seedTimeout, "the tracker to seed the file to peer-2", func() bool { return seeded(clustertest.Diag(t, tr.URL), 2, 1) })
	pulled := metric(t, peers[1].URL, `xetpd_peer_pulls_total{result="ok"}`)
	if pulled < 1 {
		t.Fatalf("peer-2 completed %v pulls, want the seed's", pulled)
	}
	asked := metric(t, tr.URL, `xetpd_tracker_seed_requests_total{result="ok"}`)
	if asked < 1 {
		t.Fatalf("tracker sent %v accepted seed requests, want one to peer-2", asked)
	}
	if g, _ := hub.CDN(); g != 1 {
		t.Fatalf("hub CDN gets = %d, want the single ingest", g)
	}
	held := xorbHashes(t, peers[0].DataDir)
	if got := xorbHashes(t, peers[1].DataDir); len(held) == 0 || !slices.Equal(got, held) {
		t.Fatalf("peer-2 stores %v, peer-1 %v", got, held)
	}
	t.Logf("file %s = xorbs %v on both peers after %v seed requests and %v pulls; hub CDN 1 get", fileHash, held, asked, pulled)
}

func TestPeerLossFailsOver(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(4, 1<<20)
	hub.Set(modelPath, data)
	tr, peers := startCluster(t, hub, 2, 1)
	fileHash := headReady(t, peers[0].URL+modelPath).Get("X-Xet-Hash")
	clustertest.WaitFor(t, seedTimeout, "the tracker to seed the file to peer-2", func() bool { return seeded(clustertest.Diag(t, tr.URL), 2, 1) })
	cdnGets, _ := hub.CDN()

	peers[0].kill()
	clustertest.WaitFor(t, 10*time.Second, "the dead peer to expire, leaving the survivor listed with the file", func() bool {
		d := clustertest.Diag(t, tr.URL)
		return len(d.Peers) == 1 && d.Peers[0].URL == peers[1].URL && d.Peers[0].Shards == 1
	})
	fh, v2, out := xetDownload(t, peers[1].URL+modelPath)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("xet download after the loss: hash %s, %d bytes sha %s", fh, len(out), clustertest.SHA256(out))
	}
	for _, u := range fetchURLs(v2) {
		if !strings.HasPrefix(u, peers[1].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on the survivor %s", u, peers[1].URL)
		}
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: the loss re-ingested", cdnGets, g)
	}
	t.Logf("killed %s; %s served %s from its seed copy; hub CDN gets %d", peers[0].URL, peers[1].URL, fileHash, cdnGets)
}

// One replica keeps the tracker from seeding, so no seed copy outlives the eviction: a cache sized for one xorb drops a for b, and the next resolve of a re-ingests it.
func TestAllCopiesEvictedReingests(t *testing.T) {
	hub := clustertest.NewHub(t)
	a, b := clustertest.RandomData(5, 1<<20), clustertest.RandomData(6, 1<<20)
	hub.Set("/org/repo/resolve/main/a.bin", a)
	hub.Set("/org/repo/resolve/main/b.bin", b)
	tr, peers := startCluster(t, hub, 1, 1, "-xorb-cache-size", strconv.Itoa(5<<18)) // 1.25MiB: one 1MiB xorb plus headers, never two
	urlA, urlB := peers[0].URL+"/org/repo/resolve/main/a.bin", peers[0].URL+"/org/repo/resolve/main/b.bin"

	hashA := headReady(t, urlA).Get("X-Xet-Hash")
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with a", holds(t, tr.URL, peers[0].URL, 1))
	xorbsA := xorbHashes(t, peers[0].DataDir)
	if len(xorbsA) != 1 {
		t.Fatalf("peer-1 stores %v after a, want one xorb", xorbsA)
	}

	hashB := headReady(t, urlB).Get("X-Xet-Hash")
	clustertest.WaitFor(t, readyTimeout, "no peer to hold a", func() bool {
		d := clustertest.Diag(t, tr.URL)
		s1, _ := peerStatus(d, peers[0].URL)
		s2, ok := peerStatus(d, peers[1].URL)
		held := xorbHashes(t, peers[0].DataDir)
		return s1.Shards == 1 && s1.Holdings == 1 && ok && s2.Holdings == 0 && s2.Shards == 0 &&
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

// One replica keeps the tracker from seeding, so the warm download through peer-2 needs the restarted tracker's lookup.
func TestTrackerRestartRecovers(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(7, 1<<20)
	hub.Set(modelPath, data)
	tr, peers := startCluster(t, hub, 1, 1)
	fileHash := headReady(t, peers[0].URL+modelPath).Get("X-Xet-Hash")
	clustertest.WaitFor(t, readyTimeout, "the tracker to list peer-1 with the file", holds(t, tr.URL, peers[0].URL, 1))
	// registry is what a tracker lists, expiries aside.
	registry := func(origin string) []apis.PeerStatus {
		list := clustertest.Diag(t, origin).Peers
		for i := range list {
			list[i].Expiry = time.Time{}
		}
		return list
	}
	before := registry(tr.URL)
	cdnGets, _ := hub.CDN()

	addr := strings.TrimPrefix(tr.URL, "http://")
	tr.kill()
	// The same command line on the address the peers announce to; the later -listen wins.
	again := startTracker(t, testSecret(t), append(slices.Clone(tr.cmd.Args[1:]), "-listen", addr))
	clustertest.WaitFor(t, time.Second, "both peers to re-announce in full", func() bool { return slices.Equal(registry(again.URL), before) })

	fh, v2, out := xetDownload(t, peers[1].URL+modelPath)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("warm xet download after the restart: hash %s, %d bytes sha %s", fh, len(out), clustertest.SHA256(out))
	}
	for _, u := range fetchURLs(v2) {
		if !strings.HasPrefix(u, peers[1].URL+apis.XorbPathPrefix) {
			t.Fatalf("fetch URL %s is not on peer-2 %s", u, peers[1].URL)
		}
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: peer-2 ingested instead of finding peer-1", cdnGets, g)
	}
	lookups := metric(t, again.URL, "xetpd_tracker_lookups_total")
	if lookups < 1 {
		t.Fatalf("restarted tracker answered %v lookups, want peer-2's", lookups)
	}
	t.Logf("restarted the tracker on %s: registry %+v restored; peer-2 found %s through %v lookups", addr, before, fileHash, lookups)
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
		{name: "peer empty upstream", args: []string{"peer", "-advertise", nowhere, "-tracker", nowhere, "-upstream", ""}, code: 2, stderr: "upstream is required"},
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
