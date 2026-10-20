package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const modelPath = "/org/repo/resolve/main/model.bin"

// startCluster runs a gateway that evicts every local xorb once minSeeds live seeds hold it, plus two registered seeds.
func startCluster(t *testing.T, hub *clustertest.Hub, minSeeds int, peerFlags ...string) (*gatewayProc, []*peerProc) {
	t.Helper()
	secret := testSecret(t)
	gw := startGateway(t, hub.URL, secret, []string{"-xorb-cache-size", "1", "-peer-ttl", "2s", "-replicas", "2", "-min-seeds", strconv.Itoa(minSeeds)})
	var peers []*peerProc
	for range 2 {
		peers = append(peers, startPeer(t, gw.URL, secret, append([]string{"-heartbeat", "100ms"}, peerFlags...)))
	}
	clustertest.WaitFor(t, readyTimeout, "both seeds to register", func() bool { return len(clustertest.Diag(t, gw.URL).Peers) == 2 })
	return gw, peers
}

// seedOf returns the one peer every URL points at.
func seedOf(t *testing.T, peers []*peerProc, urls []string) *peerProc {
	t.Helper()
	var found *peerProc
	for _, u := range urls {
		i := slices.IndexFunc(peers, func(p *peerProc) bool { return strings.HasPrefix(u, p.URL+"/v1/xorbs/") })
		if i < 0 || found != nil && found != peers[i] {
			t.Fatalf("fetch URLs %v do not point at one seed", urls)
		}
		found = peers[i]
	}
	if found == nil {
		t.Fatalf("no fetch URLs")
	}
	return found
}

func TestSeedsPulledAfterIngest(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(1, 1<<20)
	hub.Set(modelPath, data)
	gw, peers := startCluster(t, hub, 2)
	url := gw.URL + modelPath

	if got := plainGet(t, url); clustertest.SHA256(got) != clustertest.SHA256(data) {
		t.Fatalf("cold plain GET returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	fileHash := headReady(t, url).Get("X-Xet-Hash")
	clustertest.WaitFor(t, seedTimeout, "both seeds to hold the xorb and the gateway to evict it", func() bool { return seeded(clustertest.Diag(t, gw.URL), 2, 1) })
	if left := xorbHashes(t, filepath.Join(gw.StorageDir, "data")); len(left) != 0 {
		t.Fatalf("gateway still stores %v after seeding", left)
	}
	before := clustertest.Diag(t, gw.URL)
	if before.XorbGets != (apis.XorbGets{Peer: 2}) {
		t.Fatalf("xorb gets after seeding = %+v, want two seed pulls", before.XorbGets)
	}
	cdnGets, cdnBytes := hub.CDN()
	if cdnGets != 1 || cdnBytes != int64(len(data)) {
		t.Fatalf("hub CDN after ingest = %d gets, %d bytes", cdnGets, cdnBytes)
	}

	fh, v2, out := xetDownload(t, url)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("xet download: hash %s (resolve %s), %d bytes sha %s", fh, fileHash, len(out), clustertest.SHA256(out))
	}
	var hashes []string
	for h, fetches := range v2.Xorbs {
		hashes = append(hashes, h)
		for _, f := range fetches {
			if i := slices.IndexFunc(peers, func(p *peerProc) bool { return f.URL == p.URL+"/v1/xorbs/default/"+h }); i < 0 {
				t.Fatalf("xorb %s fetched from %s, want a seed", h, f.URL)
			}
		}
	}
	slices.Sort(hashes)
	for _, p := range peers {
		if got := xorbHashes(t, p.CacheDir); !slices.Equal(got, hashes) {
			t.Fatalf("seed %s stores %v, want %v", p.URL, got, hashes)
		}
	}
	if g, b := hub.CDN(); g != cdnGets || b != cdnBytes {
		t.Fatalf("hub CDN grew to %d gets, %d bytes during the xet download", g, b)
	}
	if after := clustertest.Diag(t, gw.URL); after.XorbGets != before.XorbGets {
		t.Fatalf("gateway xorb gets %+v -> %+v during the xet download", before.XorbGets, after.XorbGets)
	}
	t.Logf("file %s = xorbs %v on both seeds (%d bytes each); hub CDN %d get, %d bytes; gateway xorb gets %+v", fileHash, hashes, xorbSize(t, peers[0].CacheDir, hashes[0]), cdnGets, cdnBytes, before.XorbGets)
}

// xorbSize is the on-disk size of one xorb under a local store.
func xorbSize(t *testing.T, base, hash string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(base, "xorbs", hash[:2], hash[2:4], hash[4:]))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestPeerLossFailsOver(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(2, 1<<20)
	hub.Set(modelPath, data)
	gw, peers := startCluster(t, hub, 2)
	url := gw.URL + modelPath

	fileHash := headReady(t, url).Get("X-Xet-Hash")
	clustertest.WaitFor(t, seedTimeout, "both seeds to hold the xorb and the gateway to evict it", func() bool { return seeded(clustertest.Diag(t, gw.URL), 2, 1) })
	victim := seedOf(t, peers, reconstructionURLs(t, gw.URL, fileHash))
	survivor := peers[1-slices.Index(peers, victim)]
	cdnGets, _ := hub.CDN()

	victim.kill()
	clustertest.WaitFor(t, 10*time.Second, "the dead seed to expire", func() bool {
		d := clustertest.Diag(t, gw.URL)
		return len(d.Peers) == 1 && d.Peers[0].URL == survivor.URL && d.Peers[0].Holdings == 1
	})
	if got := seedOf(t, peers, reconstructionURLs(t, gw.URL, fileHash)); got != survivor {
		t.Fatalf("reconstruction after the loss points at %s, want survivor %s", got.URL, survivor.URL)
	}
	fh, v2, out := xetDownload(t, url)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("xet download after the loss: hash %s, %d bytes sha %s", fh, len(out), clustertest.SHA256(out))
	}
	if got := seedOf(t, peers, fetchURLs(v2)); got != survivor {
		t.Fatalf("v2 fetch URLs point at %s, want survivor %s", got.URL, survivor.URL)
	}
	if g, _ := hub.CDN(); g != cdnGets {
		t.Fatalf("hub CDN gets %d -> %d: the loss re-ingested", cdnGets, g)
	}
	if d := clustertest.Diag(t, gw.URL); d.XorbBytes != 0 {
		t.Fatalf("gateway staged %d bytes after the loss", d.XorbBytes)
	}
	t.Logf("killed %s; %s now serves %s; hub CDN gets %d", victim.URL, survivor.URL, fileHash, cdnGets)
}

// Seeds sized for one xorb evict a when b arrives, so the gateway re-ingests a from the hub on the next resolve.
func TestAllSeedsEvictedReingests(t *testing.T) {
	hub := clustertest.NewHub(t)
	a, b := clustertest.RandomData(3, 1<<20), clustertest.RandomData(4, 1<<20)
	hub.Set("/org/repo/resolve/main/a.bin", a)
	hub.Set("/org/repo/resolve/main/b.bin", b)
	gw, peers := startCluster(t, hub, 1, "-xorb-cache-size", strconv.Itoa(5<<18)) // 1.25MiB: one 1MiB xorb plus headers, never two
	urlA, urlB := gw.URL+"/org/repo/resolve/main/a.bin", gw.URL+"/org/repo/resolve/main/b.bin"
	cdn := func(gets int) func() bool {
		return func() bool {
			g, n := hub.CDN()
			return g == gets && n == int64(gets)<<20
		}
	}
	holding := func(want []string) func() bool {
		return func() bool {
			if !seeded(clustertest.Diag(t, gw.URL), 2, len(want)) {
				return false
			}
			for _, p := range peers {
				if !slices.Equal(xorbHashes(t, p.CacheDir), want) {
					return false
				}
			}
			return true
		}
	}

	hashA := headReady(t, urlA).Get("X-Xet-Hash")
	clustertest.WaitFor(t, ingestTimeout, "one CDN download of a", cdn(1))
	clustertest.WaitFor(t, seedTimeout, "both seeds to hold a", func() bool { return seeded(clustertest.Diag(t, gw.URL), 2, 1) })
	xorbsA := xorbHashes(t, peers[0].CacheDir)
	if len(xorbsA) != 1 || !slices.Equal(xorbHashes(t, peers[1].CacheDir), xorbsA) {
		t.Fatalf("seeds store %v and %v after a", xorbsA, xorbHashes(t, peers[1].CacheDir))
	}

	hashB := headReady(t, urlB).Get("X-Xet-Hash")
	clustertest.WaitFor(t, ingestTimeout, "one CDN download of b", cdn(2))
	clustertest.WaitFor(t, seedTimeout, "both seeds to evict a for b", func() bool {
		got := xorbHashes(t, peers[0].CacheDir)
		return len(got) == 1 && got[0] != xorbsA[0] && holding(got)()
	})
	xorbsB := xorbHashes(t, peers[0].CacheDir)
	clustertest.WaitFor(t, readyTimeout, "the reconstruction of a to fail", func() bool {
		resp, _ := request(t, http.MethodGet, gw.URL+"/v1/reconstructions/"+hashA, "")
		return resp.StatusCode == http.StatusNotFound
	})
	if resp, _ := request(t, http.MethodGet, gw.URL+"/v1/reconstructions/"+hashB, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("reconstruction of b = %d", resp.StatusCode)
	}
	if g, _ := hub.CDN(); g != 2 {
		t.Fatalf("hub CDN gets = %d before the resolve of a", g)
	}

	if got := headReady(t, urlA).Get("X-Xet-Hash"); got != hashA {
		t.Fatalf("re-ingested a has hash %s, want %s", got, hashA)
	}
	clustertest.WaitFor(t, ingestTimeout, "exactly one more CDN download of a", cdn(3))
	clustertest.WaitFor(t, seedTimeout, "both seeds to hold a again", holding(xorbsA))
	fh, _, out := xetDownload(t, urlA)
	if fh.String() != hashA || clustertest.SHA256(out) != clustertest.SHA256(a) {
		t.Fatalf("xet download of a: hash %s, %d bytes sha %s", fh, len(out), clustertest.SHA256(out))
	}
	if !cdn(3)() {
		g, n := hub.CDN()
		t.Fatalf("hub CDN after the download = %d gets, %d bytes", g, n)
	}
	if slices.Equal(xorbsA, xorbsB) {
		t.Fatalf("a and b share xorb %v", xorbsA)
	}
	t.Logf("a %s xorb %s (%d bytes) evicted for b %s xorb %s under a %d byte seed cache, then re-ingested: hub CDN 3 gets", hashA, xorbsA[0], xorbSize(t, peers[0].CacheDir, xorbsA[0]), hashB, xorbsB[0], 5<<18)
}

func TestNoPeersServesFromGateway(t *testing.T) {
	hub := clustertest.NewHub(t)
	data := clustertest.RandomData(5, 1<<20)
	hub.Set(modelPath, data)
	gw := startGateway(t, hub.URL, "", []string{"-xorb-cache-size", "1", "-peer-ttl", "2s"})
	url := gw.URL + modelPath

	fileHash := headReady(t, url).Get("X-Xet-Hash")
	before := clustertest.Diag(t, gw.URL)
	if len(before.Peers) != 0 || before.XorbBytes == 0 || before.XorbGets != (apis.XorbGets{}) {
		t.Fatalf("diagnostics after ingest = %+v", before)
	}
	stored := xorbHashes(t, filepath.Join(gw.StorageDir, "data"))
	if len(stored) == 0 {
		t.Fatal("gateway stores no xorb without seeds")
	}

	fh, v2, out := xetDownload(t, url)
	if fh.String() != fileHash || clustertest.SHA256(out) != clustertest.SHA256(data) {
		t.Fatalf("xet download: hash %s, %d bytes sha %s", fh, len(out), clustertest.SHA256(out))
	}
	for _, u := range fetchURLs(v2) {
		if !strings.HasPrefix(u, gw.URL+"/v1/xorbs/") {
			t.Fatalf("fetch URL %s is not the gateway", u)
		}
	}
	after := clustertest.Diag(t, gw.URL)
	if after.XorbGets.Direct == 0 || after.XorbGets.Peer != 0 || after.XorbBytes != before.XorbBytes {
		t.Fatalf("diagnostics after the xet download = %+v, before %+v", after, before)
	}
	if got := plainGet(t, url); clustertest.SHA256(got) != clustertest.SHA256(data) {
		t.Fatalf("warm plain GET returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	if g, n := hub.CDN(); g != 1 || n != int64(len(data)) {
		t.Fatalf("hub CDN = %d gets, %d bytes, want the single ingest", g, n)
	}

	hub.Close()
	if got := plainGet(t, gw.URL+"/org/repo/resolve/"+hub.Commit+"/model.bin"); clustertest.SHA256(got) != clustertest.SHA256(data) {
		t.Fatalf("offline commit GET returned %d bytes with sha %s", len(got), clustertest.SHA256(got))
	}
	if got := xorbHashes(t, filepath.Join(gw.StorageDir, "data")); !slices.Equal(got, stored) {
		t.Fatalf("gateway stores %v, want %v retained", got, stored)
	}
	t.Logf("gateway kept %v (%d staged bytes) with no seeds; xet download made %d direct xorb gets; offline commit GET served %d bytes", stored, after.XorbBytes, after.XorbGets.Direct, len(data))
}

// fakeGateway answers announces and serves one xorb whose body stalls after a flushed prefix.
type fakeGateway struct {
	*httptest.Server
	hash    xet.XorbHash
	body    []byte
	prefix  int
	release chan struct{}
	once    sync.Once

	mu        sync.Mutex
	gets      int
	announces []apis.AnnounceRequest
}

func newFakeGateway(t *testing.T, hash xet.XorbHash, body []byte, prefix int) *fakeGateway {
	g := &fakeGateway{hash: hash, body: body, prefix: prefix, release: make(chan struct{})}
	g.Server = httptest.NewServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.Close)
	t.Cleanup(g.open) // a stalled handler would hold Close
	return g
}

// open lets the stalled body finish.
func (g *fakeGateway) open() { g.once.Do(func() { close(g.release) }) }

func (g *fakeGateway) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == apis.AnnouncePath:
		var req apis.AnnounceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		g.mu.Lock()
		g.announces = append(g.announces, req)
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(apis.AnnounceResponse{TTL: 30})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/xorbs/default/"+g.hash.String():
		g.mu.Lock()
		g.gets++
		g.mu.Unlock()
		w.Header().Set("Content-Length", strconv.Itoa(len(g.body)))
		_, _ = w.Write(g.body[:g.prefix])
		w.(http.Flusher).Flush()
		select {
		case <-g.release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(g.body[g.prefix:])
	default:
		http.NotFound(w, r)
	}
}

func (g *fakeGateway) getCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gets
}

// added reports whether any announce so far adds the xorb.
func (g *fakeGateway) added() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, a := range g.announces {
		if slices.Contains(a.Add, g.hash.String()) {
			return true
		}
	}
	return false
}

func TestServeWhilePulling(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 6, 4, 64<<10)
	const prefix = 100 << 10
	fake := newFakeGateway(t, hash, body, prefix)
	secret := testSecret(t)
	p := startPeer(t, fake.URL, secret, []string{"-heartbeat", "100ms"})
	u := p.URL + "/v1/xorbs/default/" + hash.String()

	payload := fmt.Sprintf(`{"hash":"%s"}`, hash)
	if resp, out := request(t, http.MethodPost, p.URL+apis.PullPath, payload, "Content-Type", "application/json"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("pull without the secret = %d %q", resp.StatusCode, out)
	}
	if resp, out := request(t, http.MethodPost, p.URL+apis.PullPath, payload, "Content-Type", "application/json", "Authorization", "Bearer "+secret); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pull = %d %q", resp.StatusCode, out)
	}
	clustertest.WaitFor(t, readyTimeout, "the pull to reach the gateway", func() bool { return fake.getCount() == 1 })

	resp, got := request(t, http.MethodGet, u, "", "Range", fmt.Sprintf("bytes=0-%d", prefix-1))
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, body[:prefix]) {
		t.Fatalf("prefix range while the gateway stalls = %d, %d bytes", resp.StatusCode, len(got))
	}
	late := requestAsync(t, u, "Range", fmt.Sprintf("bytes=%d-%d", prefix, prefix))
	whole := requestAsync(t, u)
	select {
	case r := <-late:
		t.Fatalf("range past the flushed prefix answered %v %v before the gateway resumed", r.resp, r.err)
	case r := <-whole:
		t.Fatalf("whole GET completed %v %v before the gateway resumed", r.resp, r.err)
	case <-time.After(200 * time.Millisecond):
	}
	if got := xorbHashes(t, p.CacheDir); len(got) != 0 {
		t.Fatalf("xorb %v committed before the gateway finished", got)
	}
	if n := fake.getCount(); n != 1 {
		t.Fatalf("gateway GETs = %d while three readers wait, want 1", n)
	}
	if fake.added() {
		t.Fatal("xorb announced before the gateway finished")
	}

	fake.open()
	if r := <-late; r.err != nil || r.resp.StatusCode != http.StatusPartialContent || !bytes.Equal(r.body, body[prefix:prefix+1]) {
		t.Fatalf("late range = %v, %d bytes, %v", r.resp, len(r.body), r.err)
	}
	if r := <-whole; r.err != nil || r.resp.StatusCode != http.StatusOK || r.resp.ContentLength != int64(len(body)) || !bytes.Equal(r.body, body) {
		t.Fatalf("whole GET = %v, %d bytes, %v", r.resp, len(r.body), r.err)
	}
	clustertest.WaitFor(t, readyTimeout, "the commit to be announced", fake.added)
	if got := xorbHashes(t, p.CacheDir); !slices.Equal(got, []string{hash.String()}) {
		t.Fatalf("seed stores %v, want %s", got, hash)
	}
	if n := fake.getCount(); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
	t.Logf("xorb %s: %d bytes, %d flushed before the stall; one gateway GET shared by three readers", hash, len(body), prefix)
}

func TestCLIErrors(t *testing.T) {
	secret := testSecret(t) + "-hunter2"
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "secret")
	if err := os.WriteFile(secretFile, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		args   []string
		env    []string
		code   int
		stdout string // substring, "" means empty
		stderr string // substring, "" means empty
	}{
		{name: "no args", code: 2, stderr: "Usage: xet-pd <command>"},
		{name: "help", args: []string{"help"}, stdout: "Commands:"},
		{name: "gateway help", args: []string{"gateway", "-h"}, stdout: "Usage: xet-pd gateway"},
		{name: "peer help", args: []string{"peer", "--help"}, stdout: "[env XET_PD_ADVERTISE]"},
		{name: "unknown command", args: []string{"bogus"}, code: 2, stderr: `unknown command "bogus"`},
		{name: "unknown flag", args: []string{"gateway", "-bogus"}, code: 2, stderr: "flag provided but not defined: -bogus"},
		{name: "peer missing advertise", args: []string{"peer", "-gateway", "http://127.0.0.1:1"}, code: 2, stderr: "advertise is required"},
		{name: "gateway bad upstream", args: []string{"gateway", "-storage-dir", dir, "-upstream", "ftp://hub"}, code: 1, stderr: "upstream: scheme is not http(s)"},
		{name: "peer listen taken", args: []string{"peer", "-listen", reserveTaken(t), "-advertise", "http://127.0.0.1:1", "-gateway", "http://127.0.0.1:1", "-cache-dir", t.TempDir()}, code: 1, stderr: "address already in use"},
		{name: "secret flags exclusive", args: []string{"gateway", "-secret", secret, "-secret-file", secretFile}, code: 2, stderr: "mutually exclusive"},
		{name: "secret env exclusive", args: []string{"gateway"}, env: []string{"XET_PD_SECRET=" + secret, "XET_PD_SECRET_FILE=" + secretFile}, code: 2, stderr: "mutually exclusive"},
		{name: "secret file missing", args: []string{"peer", "-advertise", "http://127.0.0.1:1", "-gateway", "http://127.0.0.1:1", "-secret-file", filepath.Join(dir, "missing")}, code: 2, stderr: "secret file"},
		{name: "advertise credentials", args: []string{"peer", "-advertise", "http://bob:" + secret + "@127.0.0.1:1", "-gateway", "http://127.0.0.1:1", "-cache-dir", t.TempDir()}, code: 1, stderr: "advertise: userinfo not allowed"},
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
	gw := startGateway(t, clustertest.NewHub(t).URL, "", nil)
	gw.discard() // the exit status is asserted below
	// Shutdown waits about 5s on a connection still sending its first request header.
	slow, err := net.Dial("tcp", strings.TrimPrefix(gw.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Close() }()
	if _, err := slow.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n")); err != nil {
		t.Fatal(err)
	}
	// Connections are accepted in order, so answering a later one proves the gateway tracks the slow one.
	probe := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: readyTimeout}
	resp, err := probe.Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if err := gw.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	// A race build lingers 1s in os.Exit(0), so only outliving 2s proves the drain.
	select {
	case <-gw.done:
		t.Fatalf("gateway ended with %v within 2s of the first SIGINT, so nothing held the drain", gw.cmd.ProcessState)
	case <-time.After(2 * time.Second):
	}
	if err := gw.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gw.done:
	case <-time.After(2 * time.Second):
		gw.kill()
		t.Fatal("gateway still alive 2s after the second SIGINT")
	}
	if ws, ok := gw.cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGINT {
		t.Fatalf("gateway ended with %v after the second SIGINT, want death by SIGINT", gw.cmd.ProcessState)
	}
}
