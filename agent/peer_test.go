package agent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/token"

	"github.com/wzshiming/xet-pd/agent"
	"github.com/wzshiming/xet-pd/tracker"
)

// countingHub wraps the fake hub and counts every request that reaches the
// origin, splitting out the /cdn data route: peer-served flights may cost
// the origin metadata-only validator probes, but never data.
type countingHub struct {
	next  http.Handler
	total atomic.Int64
	data  atomic.Int64
}

func (c *countingHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.total.Add(1)
	if strings.HasPrefix(r.URL.Path, "/cdn/") {
		c.data.Add(1)
	}
	c.next.ServeHTTP(w, r)
}

// waitPeerReady polls a local-only HEAD until the agent advertises the file
// from its ready index (the index write trails ingest completion).
func waitPeerReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := noRedirectClient()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodHead, resolveURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Xet-PD-Local-Only", "1")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode < 400 {
			return resp
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("peer never advertised the file as locally ready")
	return nil
}

// TestPeerServesWarmFile is the S1 acceptance: with peer B warm, agent A's
// cold download must cost the origin zero data traffic — the resolve goes
// through the selector to B and the xet ingest follows B's absolute URLs;
// only metadata-sized validator probes may reach the origin. The origin
// uses a real 40-hex commit, so the mirror canonicalizes the branch and the
// peer must answer the commit-keyed probes too.
func TestPeerServesWarmFile(t *testing.T) {
	hub := newFakeHub()
	hub.commit = strings.Repeat("ab", 20)
	counting := &countingHub{next: hub}
	hubSrv := httptest.NewServer(counting)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/peer.bin"
	data := deterministicData(48 * 1024)
	wantSum := sha256.Sum256(data)
	hub.set(resolvePath, data)

	peerSrv := newAgentServer(t, hubSrv.URL, nil) // B: no peers, warms from origin
	if body := getBody(t, peerSrv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("peer warm-up body mismatch")
	}
	waitReady(t, peerSrv.URL+resolvePath)
	ready := waitPeerReady(t, peerSrv.URL+resolvePath)

	wantETag := `"` + fmt.Sprintf("%x", wantSum) + `"`
	if got := ready.Header.Get("ETag"); got != wantETag {
		t.Fatalf("local-only ETag = %q, want %q", got, wantETag)
	}
	if loc := ready.Header.Get("Location"); !strings.HasPrefix(loc, peerSrv.URL+"/xet-bridge/") {
		t.Fatalf("local-only Location = %q, want prefix %q", loc, peerSrv.URL+"/xet-bridge/")
	}

	// The mirror fetches by pinned commit, so the canonical path must be
	// advertised too.
	canonical := strings.Replace(resolvePath, "/main/", "/"+hub.commit+"/", 1)
	waitPeerReady(t, peerSrv.URL+canonical)

	counting.total.Store(0)
	counting.data.Store(0)

	agentSrv := newAgentServer(t, hubSrv.URL, nil, peerSrv.URL) // A: peers=[B]
	if body := getBody(t, agentSrv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("cold download via peer: body mismatch")
	}
	waitReady(t, agentSrv.URL+resolvePath)

	if n := counting.data.Load(); n != 0 {
		t.Fatalf("origin data route saw %d requests during peer-served download, want 0", n)
	}
}

// TestPeerDownFallsBackToOrigin: a dead peer only costs the probe; the
// download completes from the origin.
func TestPeerDownFallsBackToOrigin(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/fallback.bin"
	data := deterministicData(32 * 1024)
	hub.set(resolvePath, data)

	// Port 1 is never bound; a recycled ephemeral port could come alive again.
	const deadURL = "http://127.0.0.1:1"

	agentSrv := newAgentServer(t, hubSrv.URL, nil, deadURL)
	if body := getBody(t, agentSrv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("fallback download body mismatch")
	}
	waitReady(t, agentSrv.URL+resolvePath)
}

// TestLocalOnlyMissNeverTouchesOrigin: a local-only probe for a cold file is
// answered 404 by the agent itself — no ingest, no upstream contact.
func TestLocalOnlyMissNeverTouchesOrigin(t *testing.T) {
	hub := newFakeHub()
	counting := &countingHub{next: hub}
	hubSrv := httptest.NewServer(counting)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/cold.bin"
	hub.set(resolvePath, deterministicData(1024))

	agentSrv := newAgentServer(t, hubSrv.URL, nil)

	for _, method := range []string{http.MethodHead, http.MethodGet} {
		req, err := http.NewRequest(method, agentSrv.URL+resolvePath, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Xet-PD-Local-Only", "1")
		resp, err := noRedirectClient().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s local-only cold: status = %d, want 404", method, resp.StatusCode)
		}
	}
	// The 404s must not have started an ingest either: give any stray
	// background work a moment to surface, then check the origin was idle.
	time.Sleep(100 * time.Millisecond)
	if n := counting.total.Load(); n != 0 {
		t.Fatalf("origin saw %d requests from local-only misses, want 0", n)
	}
}

// TestColdClusterNoRecursion: two cold agents peering at each other must not
// loop; the download completes from the origin.
func TestColdClusterNoRecursion(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/mutual.bin"
	data := deterministicData(32 * 1024)
	hub.set(resolvePath, data)

	srvA, installA := newSwappableServer(t)
	srvB, installB := newSwappableServer(t)
	newAgentAt(t, srvA, installA, agent.Config{
		Upstream:   hubSrv.URL,
		StorageDir: t.TempDir(),
		Peers:      []string{srvB.URL},
	})
	newAgentAt(t, srvB, installB, agent.Config{
		Upstream:   hubSrv.URL,
		StorageDir: t.TempDir(),
		Peers:      []string{srvA.URL},
	})

	if body := getBody(t, srvA.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("mutual-cold download body mismatch")
	}
	waitReady(t, srvA.URL+resolvePath)
}

// TestReadyIndexSurvivesRestart: the peer ready index is plain files, so a
// rebuilt agent on the same storage dir advertises its files immediately and
// without any origin traffic.
func TestReadyIndexSurvivesRestart(t *testing.T) {
	hub := newFakeHub()
	hub.commit = strings.Repeat("cd", 20)
	counting := &countingHub{next: hub}
	hubSrv := httptest.NewServer(counting)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/restart.bin"
	data := deterministicData(16 * 1024)
	hub.set(resolvePath, data)

	dir := t.TempDir()
	srv, install := newSwappableServer(t)
	a1, err := agent.New(agent.Config{
		Upstream:    hubSrv.URL,
		StorageDir:  dir,
		ExternalURL: srv.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	install(a1)
	if body := getBody(t, srv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("warm-up body mismatch")
	}
	waitReady(t, srv.URL+resolvePath)
	waitPeerReady(t, srv.URL+resolvePath)
	if err := a1.Close(); err != nil {
		t.Fatal(err)
	}

	counting.total.Store(0)
	a2, err := agent.New(agent.Config{
		Upstream:    hubSrv.URL,
		StorageDir:  dir,
		ExternalURL: srv.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	install(a2)

	req, err := http.NewRequest(http.MethodHead, srv.URL+resolvePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Xet-PD-Local-Only", "1")
	resp, err := noRedirectClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		t.Fatalf("local-only after restart: status = %d, want ready", resp.StatusCode)
	}
	if n := counting.total.Load(); n != 0 {
		t.Fatalf("origin saw %d requests answering from the restarted index, want 0", n)
	}
}

// TestStalledPeerFallsBack: a peer that answers probes but hangs its
// responses afterward must not wedge the ingest — the response-header
// timeout kicks in and the download completes from the origin.
func TestStalledPeerFallsBack(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/stall.bin"
	data := deterministicData(16 * 1024)
	wantSum := sha256.Sum256(data)
	hub.set(resolvePath, data)

	// The stalled peer looks ready on HEAD (correct metadata) and then never
	// answers anything else.
	stall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			etag := `"` + hex.EncodeToString(wantSum[:]) + `"`
			w.Header().Set("ETag", etag)
			w.Header().Set("X-Linked-Etag", etag)
			w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
			w.Header().Set("X-Repo-Commit", "commit-stall")
			w.WriteHeader(http.StatusOK)
			return
		}
		<-r.Context().Done()
	}))
	defer stall.Close()

	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:    hubSrv.URL,
		StorageDir:  t.TempDir(),
		Peers:       []string{stall.URL},
		PeerTimeout: 300 * time.Millisecond,
	})

	if body := getBody(t, srv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("stalled-peer fallback body mismatch")
	}
	waitReady(t, srv.URL+resolvePath)
}

// newTrackerServer runs a tracker on its own directory.
func newTrackerServer(t *testing.T) *httptest.Server {
	t.Helper()
	h, err := tracker.NewHandler(tracker.WithStorageDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// waitAnnounced polls the tracker until some peer claims the resolve key.
func waitAnnounced(t *testing.T, trackerURL, key string) {
	t.Helper()
	c, err := tracker.NewClient(trackerURL)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{Keys: []string{key}})
		if err == nil && len(resp.Keys[key].Peers) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("file never announced to the tracker")
}

// TestTrackerDirectedPeerDiscovery is the S2 acceptance: the tracker names
// the holding peer, so a cold download probes only that peer — the origin
// sees zero requests and a non-holding static peer sees zero probes (S1
// would have broadcast to it).
func TestTrackerDirectedPeerDiscovery(t *testing.T) {
	hub := newFakeHub()
	hub.commit = strings.Repeat("ef", 20)
	counting := &countingHub{next: hub}
	hubSrv := httptest.NewServer(counting)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/tracked.bin"
	data := deterministicData(32 * 1024)
	hub.set(resolvePath, data)

	trackerSrv := newTrackerServer(t)

	// A: warm holder announcing to the tracker.
	srvA, installA := newSwappableServer(t)
	newAgentAt(t, srvA, installA, agent.Config{
		Upstream:   hubSrv.URL,
		StorageDir: t.TempDir(),
		Tracker:    trackerSrv.URL,
	})
	if body := getBody(t, srvA.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("holder warm-up body mismatch")
	}
	waitReady(t, srvA.URL+resolvePath)
	waitAnnounced(t, trackerSrv.URL, resolvePath)

	// C: a static peer that holds nothing; with the tracker directing, it
	// must never even be probed.
	var probesToC atomic.Int64
	srvC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probesToC.Add(1)
		http.NotFound(w, r)
	}))
	defer srvC.Close()

	counting.total.Store(0)
	counting.data.Store(0)

	// B: discovers the holder through the tracker despite listing only C.
	srvB, installB := newSwappableServer(t)
	newAgentAt(t, srvB, installB, agent.Config{
		Upstream:   hubSrv.URL,
		StorageDir: t.TempDir(),
		Tracker:    trackerSrv.URL,
		Peers:      []string{srvC.URL},
	})
	if body := getBody(t, srvB.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("tracker-directed download body mismatch")
	}
	waitReady(t, srvB.URL+resolvePath)

	if n := counting.data.Load(); n != 0 {
		t.Errorf("origin data route saw %d requests during tracker-directed download, want 0", n)
	}
	if n := probesToC.Load(); n != 0 {
		t.Errorf("non-holding peer saw %d probes, want 0 (tracker should direct)", n)
	}
}

// TestTrackerDownFallsBack: dead and hanging trackers degrade to S1
// static-peer probing — still with zero origin data traffic — and without
// peers to the origin. Correctness never depends on the tracker.
func TestTrackerDownFallsBack(t *testing.T) {
	hub := newFakeHub()
	counting := &countingHub{next: hub}
	hubSrv := httptest.NewServer(counting)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/untracked.bin"
	data := deterministicData(16 * 1024)
	hub.set(resolvePath, data)

	// Port 1 is never bound: a recycled ephemeral port could be re-assigned
	// to a later listener and silently resurrect the "dead" tracker.
	const deadURL = "http://127.0.0.1:1"

	// A tracker that accepts connections and then hangs until the client
	// gives up must not starve the static-peer probes either. The body must
	// be drained: the HTTP/1 server only notices a gone client (and cancels
	// the request context) once the handler consumed the request body.
	hanging := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer hanging.Close()

	// Warm holder, reachable only via the static list.
	peerSrv := newAgentServer(t, hubSrv.URL, nil)
	if body := getBody(t, peerSrv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("holder warm-up body mismatch")
	}
	waitReady(t, peerSrv.URL+resolvePath)
	waitPeerReady(t, peerSrv.URL+resolvePath)

	for _, tc := range []struct {
		name, trackerURL string
	}{
		{"dead tracker", deadURL},
		{"hanging tracker", hanging.URL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counting.total.Store(0)
			counting.data.Store(0)
			srvB, installB := newSwappableServer(t)
			// The timeout bounds the hanging-tracker lookup and each probe
			// alike: generous enough that loopback probes never miss it
			// even under a loaded race detector, small enough to keep the
			// hanging subtest quick.
			newAgentAt(t, srvB, installB, agent.Config{
				Upstream:    hubSrv.URL,
				StorageDir:  t.TempDir(),
				Tracker:     tc.trackerURL,
				Peers:       []string{peerSrv.URL},
				PeerTimeout: 2 * time.Second,
			})
			if body := getBody(t, srvB.URL+resolvePath); !bytes.Equal(body, data) {
				t.Fatal("static-peer fallback body mismatch")
			}
			waitReady(t, srvB.URL+resolvePath)
			if n := counting.data.Load(); n != 0 {
				t.Errorf("origin data route saw %d requests, want 0 (static peer must serve)", n)
			}
		})
	}

	t.Run("no peers falls back to origin", func(t *testing.T) {
		srvD, installD := newSwappableServer(t)
		newAgentAt(t, srvD, installD, agent.Config{
			Upstream:   hubSrv.URL,
			StorageDir: t.TempDir(),
			Tracker:    deadURL,
		})
		if body := getBody(t, srvD.URL+resolvePath); !bytes.Equal(body, data) {
			t.Fatal("origin fallback body mismatch")
		}
		waitReady(t, srvD.URL+resolvePath)
	})
}

// TestHandshakeTokenCannotAnnounce: the anonymous /xet-token mint must not
// authorize tracker announces — only the cluster-secret issuer does.
func TestHandshakeTokenCannotAnnounce(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/secret.bin"
	data := deterministicData(8 * 1024)
	hub.set(resolvePath, data)

	issuer, err := token.NewIssuer([]byte("cluster-secret"), 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	th, err := tracker.NewHandler(
		tracker.WithStorageDir(t.TempDir()),
		tracker.WithAuth(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	trackerSrv := httptest.NewServer(th)
	defer trackerSrv.Close()

	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:      hubSrv.URL,
		StorageDir:    t.TempDir(),
		Tracker:       trackerSrv.URL,
		ClusterSecret: "cluster-secret",
	})

	// The agent's own announce, minted from the cluster secret, lands.
	if body := getBody(t, srv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("warm-up body mismatch")
	}
	waitReady(t, srv.URL+resolvePath)
	waitAnnounced(t, trackerSrv.URL, resolvePath)

	// The anonymously minted handshake token must be rejected.
	var tok struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(getBody(t, srv.URL+"/xet-token"), &tok); err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken == "" {
		t.Fatal("empty accessToken")
	}
	req, err := http.NewRequest(http.MethodPost, trackerSrv.URL+"/v1/announce",
		strings.NewReader(`{"peer":"http://evil:1","adds":{"files":[{"sha256":"`+strings.Repeat("ee", 32)+`"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("announce with handshake token: status = %d, want 401", resp.StatusCode)
	}
}

// waitReadyNoFailure polls the resolve URL until the ready redirect, failing
// immediately on any server error: the flight must succeed on its first
// pass, never through the failure-backoff-retry cycle.
func waitReadyNoFailure(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := noRedirectClient()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusFound {
			return resp
		}
		if resp.StatusCode >= 500 {
			t.Fatalf("flight failed with status %d, want first-pass success", resp.StatusCode)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("agent never reached ready state")
	return nil
}

// TestPeerWrongBytesSelfEtagRejected is the S1 anchoring regression: a peer
// whose ready answer is self-consistent — its ETag matches the wrong bytes
// it serves — must not become the flight's upstream, since the mirror would
// verify those bytes against that same ETag and mark them ready. The
// validator is anchored at the origin instead: the peer is rejected, the
// cold download completes from the origin on its first pass, and the ready
// entry carries the origin's checksum.
func TestPeerWrongBytesSelfEtagRejected(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/anchored.bin"
	data := deterministicData(32 * 1024)
	wantSum := sha256.Sum256(data)
	hub.set(resolvePath, data)

	// The evil peer is a real agent warmed from a hub serving different
	// content at the same path, so its index entry is exactly the
	// self-consistent shape the finding describes.
	evilHub := newFakeHub()
	evilData := append([]byte("evil"), deterministicData(32 * 1024)[4:]...)
	evilHub.set(resolvePath, evilData)
	evilHubSrv := httptest.NewServer(evilHub)
	defer evilHubSrv.Close()

	srvE, installE := newSwappableServer(t)
	evil, err := agent.New(agent.Config{
		Upstream:    evilHubSrv.URL,
		StorageDir:  t.TempDir(),
		ExternalURL: srvE.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = evil.Close() })
	var evilProbes, evilBridgeHits atomic.Int64
	installE(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/xet-bridge/") {
			evilBridgeHits.Add(1)
		}
		if r.Header.Get("X-Xet-PD-Local-Only") != "" {
			evilProbes.Add(1)
		}
		evil.ServeHTTP(w, r)
	}))
	if body := getBody(t, srvE.URL+resolvePath); !bytes.Equal(body, evilData) {
		t.Fatal("evil peer warm-up body mismatch")
	}
	waitReady(t, srvE.URL+resolvePath)
	waitPeerReady(t, srvE.URL+resolvePath)

	agentSrv := newAgentServer(t, hubSrv.URL, nil, srvE.URL)
	if body := getBody(t, agentSrv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("cold download body differs from the origin's content")
	}
	ready := waitReadyNoFailure(t, agentSrv.URL+resolvePath)

	wantETag := `"` + hex.EncodeToString(wantSum[:]) + `"`
	if got := ready.Header.Get("ETag"); got != wantETag {
		t.Fatalf("ready ETag = %q, want the origin's %q", got, wantETag)
	}
	if got := evilProbes.Load(); got == 0 {
		t.Fatal("got 0 probes to the mismatching peer, want at least 1 (selector consulted it)")
	}
	if got := evilBridgeHits.Load(); got != 0 {
		t.Fatalf("got %d data fetches from the mismatching peer, want 0", got)
	}
}

// TestPeerValidatorSwapRejected: matching the origin validator at probe time
// is not enough — a peer could swap to a different self-consistent ETag on
// the proxied resolve itself, poisoning the mirror's probe metadata. Every
// proxied peer response is re-checked, so the swapped answer is discarded
// and the flight completes from the origin without a single failure.
func TestPeerValidatorSwapRejected(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/swap.bin"
	data := deterministicData(32 * 1024)
	trueSum := sha256.Sum256(data)
	hub.set(resolvePath, data)
	wrongSum := sha256.Sum256(append([]byte("swapped"), data...))

	var bridgeHits, reqs atomic.Int64
	swapping := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Xet-PD-Agent", "1")
		if strings.HasPrefix(r.URL.Path, "/xet-bridge/") {
			bridgeHits.Add(1)
			http.NotFound(w, r)
			return
		}
		// The first answer carries the true validator to pass the probe;
		// every later one swaps to a different self-consistent value.
		sum := trueSum
		if reqs.Add(1) > 1 {
			sum = wrongSum
		}
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		w.Header().Set("ETag", etag)
		w.Header().Set("X-Linked-Etag", etag)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
		w.Header().Set("X-Repo-Commit", "commit-swap")
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "http://"+r.Host+"/xet-bridge/"+hex.EncodeToString(sum[:]), http.StatusFound)
	}))
	defer swapping.Close()

	agentSrv := newAgentServer(t, hubSrv.URL, nil, swapping.URL)
	if body := getBody(t, agentSrv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("cold download body differs from the origin's content")
	}
	ready := waitReadyNoFailure(t, agentSrv.URL+resolvePath)

	wantETag := `"` + hex.EncodeToString(trueSum[:]) + `"`
	if got := ready.Header.Get("ETag"); got != wantETag {
		t.Fatalf("ready ETag = %q, want the origin's %q", got, wantETag)
	}
	if got := bridgeHits.Load(); got != 0 {
		t.Fatalf("got %d data fetches from the swapping peer, want 0", got)
	}
}

// TestOriginDownColdPeerHonestFailure: with the origin unreachable, a cold
// resolve must fail honestly even though a peer verifiably holds the file —
// unanchored peer data must never turn ready — while previously warm
// content keeps serving locally.
func TestOriginDownColdPeerHonestFailure(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const warmPath = "/org/repo/resolve/main/still-warm.bin"
	const coldPath = "/org/repo/resolve/main/held-by-peer.bin"
	warmData := deterministicData(16 * 1024)
	coldData := deterministicData(24 * 1024)
	hub.set(warmPath, warmData)
	hub.set(coldPath, coldData)

	// A holds the cold file; its data routes are counted.
	srvA, installA := newSwappableServer(t)
	peerA, err := agent.New(agent.Config{
		Upstream:    hubSrv.URL,
		StorageDir:  t.TempDir(),
		ExternalURL: srvA.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peerA.Close() })
	var peerBridgeHits atomic.Int64
	installA(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/xet-bridge/") {
			peerBridgeHits.Add(1)
		}
		peerA.ServeHTTP(w, r)
	}))
	if body := getBody(t, srvA.URL+coldPath); !bytes.Equal(body, coldData) {
		t.Fatal("peer warm-up body mismatch")
	}
	waitReady(t, srvA.URL+coldPath)
	waitPeerReady(t, srvA.URL+coldPath)

	// B warms only the other file, then loses the origin.
	srvB := newAgentServer(t, hubSrv.URL, nil, srvA.URL)
	if body := getBody(t, srvB.URL+warmPath); !bytes.Equal(body, warmData) {
		t.Fatal("warm-up body mismatch")
	}
	waitReady(t, srvB.URL+warmPath)

	hubSrv.Close()
	peerBridgeHits.Store(0)

	resp, err := http.Get(srvB.URL + coldPath)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 500 {
		t.Fatalf("cold resolve with origin down: status = %d, want honest server error", resp.StatusCode)
	}

	// No peer-sourced ready entry may exist, and no peer data was pulled.
	req, err := http.NewRequest(http.MethodHead, srvB.URL+coldPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Xet-PD-Local-Only", "1")
	probe, err := noRedirectClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = probe.Body.Close()
	if probe.StatusCode != http.StatusNotFound {
		t.Fatalf("local-only after failed flight: status = %d, want 404", probe.StatusCode)
	}
	if got := peerBridgeHits.Load(); got != 0 {
		t.Fatalf("got %d peer data fetches during origin outage, want 0", got)
	}
	if body := getBody(t, srvB.URL+warmPath); !bytes.Equal(body, warmData) {
		t.Fatal("previously warm content stopped serving during origin outage")
	}
}
