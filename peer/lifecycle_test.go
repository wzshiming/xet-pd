package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// pullAndSettle pulls hash through the peer and waits for the pull to drain.
func pullAndSettle(t *testing.T, p *Peer, srv *httptest.Server, hash xet.XorbHash, body []byte) {
	t.Helper()
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET %s = %d, %d bytes, %v", hash, resp.StatusCode, len(got), err)
	}
	settled(t, p)
}

// holdWriter is a ResponseWriter whose body writes wait for the gate to open.
type holdWriter struct {
	hdr     http.Header
	code    int
	buf     bytes.Buffer
	started chan struct{} // closed on the first body write
	gate    chan struct{}
	once    sync.Once
}

func newHoldWriter() *holdWriter {
	return &holdWriter{hdr: http.Header{}, started: make(chan struct{}), gate: make(chan struct{})}
}

func (h *holdWriter) Header() http.Header  { return h.hdr }
func (h *holdWriter) WriteHeader(code int) { h.code = code }
func (h *holdWriter) Write(p []byte) (int, error) {
	h.once.Do(func() { close(h.started) })
	<-h.gate
	return h.buf.Write(p)
}

// serveHeld runs the handler for hash against a holdWriter and waits until it blocks on the body.
func serveHeld(t *testing.T, p *Peer, hash xet.XorbHash) (*holdWriter, <-chan struct{}) {
	t.Helper()
	w := newHoldWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash.String(), nil))
	}()
	select {
	case <-w.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started writing the body")
	}
	return w, done
}

func finishHeld(t *testing.T, w *holdWriter, done <-chan struct{}, want []byte) {
	t.Helper()
	close(w.gate)
	<-done
	if w.code != http.StatusOK || !bytes.Equal(w.buf.Bytes(), want) {
		t.Fatalf("held response = %d, %d bytes", w.code, w.buf.Len())
	}
}

func TestStartupDropsCorruptObject(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 22, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 23, 2, 8<<10)
	dir := t.TempDir()
	seedStore(t, dir, a, bodyA)
	corrupt := bytes.Clone(bodyB)
	corrupt[len(corrupt)/2] ^= 0xff
	if err := os.MkdirAll(filepath.Dir(xorbPath(dir, b)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(xorbPath(dir, b), corrupt, 0o644); err != nil {
		t.Fatal(err)
	}
	tracker := clustertest.NewFakeTracker(t)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.DataDir, c.Heartbeat = dir, time.Hour })
	if got := tracker.Announce(t, 0).Req; !got.Full || !slices.Equal(got.Add, hashes(a)) {
		t.Fatalf("startup announce = %+v, want only %s", got, a)
	}
	if _, err := os.Stat(xorbPath(dir, b)); !os.IsNotExist(err) {
		t.Fatalf("corrupt object kept: %v", err)
	}
	if u := usage(p); u != int64(len(bodyA)) {
		t.Fatalf("usage = %d, want %d", u, len(bodyA))
	}
	if n := clustertest.Scrape(t, srv.URL)["xetpd_peer_restore_dropped_total"]; n != 1 {
		t.Fatalf("restore dropped = %v, want 1", n)
	}
}

func TestStartupSkipsUndecodableShard(t *testing.T) {
	dir := t.TempDir()
	tf := makeFile(t, 54)
	st := newTestStore(t, dir, "http://tracker.test")
	ingest(t, st, tf)
	if err := os.WriteFile(localShardPath(t, st, tf.hash), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	tracker := clustertest.NewFakeTracker(t)
	newTestPeer(t, tracker.URL, func(c *Config) { c.DataDir, c.Heartbeat = dir, time.Hour })
	if got := tracker.Announce(t, 0).Req; !got.Full || !slices.Equal(got.Add, hashes(tf.xorbHashes()...)) {
		t.Fatalf("startup announce = %+v, want the stored %v", got, tf.xorbHashes())
	}
}

func TestPullEntersInventoryOnlyAfterValidation(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 24, 4, 16<<10)
	release := make(chan struct{})
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	holder.GateXorb(hash, body, 4<<10, release)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	late := doAsync(t, xorbURL(srv, hash))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(holder.Requests()) == 1 })
	// Heartbeats keep flowing while the pull is gated; none may carry the object.
	before := len(tracker.Announces())
	clustertest.WaitFor(t, clustertest.WaitTimeout, "heartbeats", func() bool { return len(tracker.Announces()) >= before+3 })
	for i, a := range tracker.Announces() {
		if hasAdd(a, hash) || (i > 0 && (a.Req.Full || len(a.Req.Add)+len(a.Req.Remove) != 0)) {
			t.Fatalf("announce %d while gated = %+v", i, a.Req)
		}
	}
	close(release)
	if r := <-late; r.err != nil || r.resp.StatusCode != http.StatusOK || !bytes.Equal(r.body, body) {
		t.Fatalf("GET = %d, %v", r.resp.StatusCode, r.err)
	}
	added, _ := tracker.FindAnnounce(t, before, func(a clustertest.Announce) bool { return hasAdd(a, hash) })
	if added.Req.Full || !slices.Equal(added.Req.Add, hashes(hash)) || len(added.Req.Remove) != 0 {
		t.Fatalf("delta after commit = %+v", added.Req)
	}
	settled(t, p)
}

func TestAnnounceFailureRetriesAsFull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 25, 2, 8<<10)
	tracker, _ := cluster(t, map[xet.XorbHash][]byte{hash: body})
	var fails atomic.Int32
	fails.Store(2)
	tracker.SetReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		if slices.Contains(req.Add, hash.String()) && fails.Add(-1) >= 0 {
			return http.StatusInternalServerError, apis.AnnounceResponse{}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	tracker.Announce(t, 0)
	pullAndSettle(t, p, srv, hash, body)
	acked, i := tracker.FindAnnounce(t, 1, func(a clustertest.Announce) bool { return hasAdd(a, hash) && a.Code == http.StatusOK })
	// A failed delta may have been applied, so every retry carries the exact inventory.
	if !acked.Req.Full || !slices.Equal(acked.Req.Add, hashes(hash)) {
		t.Fatalf("acked retry = %+v, want Full", acked.Req)
	}
	var rejected []apis.AnnounceRequest
	for _, a := range tracker.Announces()[:i] {
		if hasAdd(a, hash) {
			if a.Code != http.StatusInternalServerError {
				t.Fatalf("announce before the ack = %d", a.Code)
			}
			rejected = append(rejected, a.Req)
		}
	}
	if len(rejected) != 2 || rejected[0].Full || !rejected[1].Full {
		t.Fatalf("rejected announces = %+v, want one delta then one Full", rejected)
	}
	// The acked object is not resent on later heartbeats.
	next := tracker.Announce(t, i+1)
	if next.Req.Full || len(next.Req.Add)+len(next.Req.Remove) != 0 {
		t.Fatalf("heartbeat after the ack = %+v", next.Req)
	}
}

// The gateway applies an announce but loses the reply; the inventory changes back before the retry, so only a Full can correct its view.
func TestAmbiguousAnnounceReplyForcesFullReconciliation(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 44, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 45, 2, 8<<10)
	modes := map[string]func(http.ResponseWriter){
		"status":    func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) },
		"malformed": func(w http.ResponseWriter) { _, _ = io.WriteString(w, `{"ttl":`) },
		"dropped": func(w http.ResponseWriter) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
		},
	}
	for name, lose := range modes {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			holdings := map[string]struct{}{}
			var reqs []apis.AnnounceRequest
			var loseNext atomic.Bool
			_, holder := cluster(t, map[xet.XorbHash][]byte{a: bodyA, b: bodyB})
			gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == apis.LookupPath {
					_ = json.NewEncoder(w).Encode(apis.LookupResponse{Xorbs: map[string][]apis.Peer{a.String(): {holder.Peer()}, b.String(): {holder.Peer()}}})
					return
				}
				var req apis.AnnounceRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if req.Full {
					holdings = map[string]struct{}{}
				}
				for _, h := range req.Add {
					holdings[h] = struct{}{}
				}
				for _, h := range req.Remove {
					delete(holdings, h)
				}
				reqs = append(reqs, req)
				mu.Unlock()
				if loseNext.Swap(false) {
					lose(w)
					return
				}
				_ = json.NewEncoder(w).Encode(apis.AnnounceResponse{TTL: 30})
			}))
			t.Cleanup(gw.Close)
			p, srv := newTestPeer(t, gw.URL, func(c *Config) {
				c.Heartbeat, c.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB)/2)
			})
			count := func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(reqs)
			}
			check := func(i int, want xet.XorbHash) {
				t.Helper()
				clustertest.WaitFor(t, clustertest.WaitTimeout, "announce", func() bool { return count() > i })
				mu.Lock()
				defer mu.Unlock()
				if got := slices.Sorted(maps.Keys(holdings)); !reqs[i].Full || !slices.Equal(got, hashes(want)) {
					t.Fatalf("retry = %+v, gateway holdings = %v, want Full with %v", reqs[i], got, hashes(want))
				}
			}
			clustertest.WaitFor(t, clustertest.WaitTimeout, "startup ack", func() bool { return count() == 1 })
			// Add a is applied but its reply is lost; a is evicted before the retry.
			loseNext.Store(true)
			pullAndSettle(t, p, srv, a, bodyA)
			clustertest.WaitFor(t, clustertest.WaitTimeout, "lost delta", func() bool { return count() == 2 })
			pullAndSettle(t, p, srv, b, bodyB)
			check(2, b)
			// Remove b is applied but its reply is lost; b is pulled again before the retry.
			loseNext.Store(true)
			pullAndSettle(t, p, srv, a, bodyA)
			clustertest.WaitFor(t, clustertest.WaitTimeout, "lost delta", func() bool { return count() == 4 })
			pullAndSettle(t, p, srv, b, bodyB)
			check(4, b)
		})
	}
}

func TestInFlightAnnounceKeepsConcurrentCommitForNextDelta(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 26, 2, 8<<10)
	tracker, _ := cluster(t, map[xet.XorbHash][]byte{hash: body})
	hold := make(chan struct{})
	tracker.SetReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		select {
		case <-hold:
		case <-r.Context().Done():
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	if first := tracker.Announce(t, 0); !first.Req.Full || len(first.Req.Add) != 0 {
		t.Fatalf("startup announce = %+v", first.Req)
	}
	pullAndSettle(t, p, srv, hash, body)
	tracker.SetReply(nil)
	close(hold)
	if second := tracker.Announce(t, 1); second.Req.Full || !slices.Equal(second.Req.Add, hashes(hash)) {
		t.Fatalf("delta after the held Full = %+v", second.Req)
	}
}

func TestResyncSendsFullWithCurrentInventory(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 27, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 28, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 29, 2, 8<<10)
	tracker, _ := cluster(t, map[xet.XorbHash][]byte{a: bodyA, b: bodyB, c: bodyC})
	var mode atomic.Int32 // 0 ok, 1 fail once, 2 resync once
	tracker.SetReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		switch mode.Swap(0) {
		case 1:
			return http.StatusInternalServerError, apis.AnnounceResponse{}
		case 2:
			return http.StatusOK, apis.AnnounceResponse{Resync: true, TTL: 30}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB))
	})
	tracker.Announce(t, 0)
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, b, bodyB)
	tracker.FindAnnounce(t, 1, func(x clustertest.Announce) bool { return hasAdd(x, b) && x.Code == http.StatusOK })
	n := len(tracker.Announces())
	// The eviction of a is lost with this delta; the retry is a Full with the exact inventory.
	mode.Store(1)
	pullAndSettle(t, p, srv, c, bodyC)
	lost := tracker.Replied(t, n)
	if lost.Code != http.StatusInternalServerError || !hasAdd(lost, c) || !hasRemove(lost, a) {
		t.Fatalf("lost delta = %d %+v", lost.Code, lost.Req)
	}
	wakeAnnouncer(p)
	retry := tracker.Announce(t, n+1)
	if !retry.Req.Full || !slices.Equal(retry.Req.Add, hashes(b, c)) || len(retry.Req.Remove) != 0 {
		t.Fatalf("retry after the lost delta = %+v, want Full %v", retry.Req, hashes(b, c))
	}
	// A resync demanded on a delta is answered with a Full at once.
	mode.Store(2)
	wakeAnnouncer(p)
	demanded := tracker.Announce(t, n+2)
	if demanded.Req.Full || len(demanded.Req.Add)+len(demanded.Req.Remove) != 0 {
		t.Fatalf("heartbeat before the resync = %+v", demanded.Req)
	}
	full := tracker.Announce(t, n+3)
	if !full.Req.Full || !slices.Equal(full.Req.Add, hashes(b, c)) || len(full.Req.Remove) != 0 {
		t.Fatalf("resync Full = %+v, want %v", full.Req, hashes(b, c))
	}
	if stored(t, p, a) || !stored(t, p, b) || !stored(t, p, c) {
		t.Fatal("store does not match the announced inventory")
	}
	awaitSamples(t, srv, map[string]float64{`xetpd_peer_announces_total{result="error"}`: 1, `xetpd_peer_announces_total{result="resync"}`: 1})
}

func TestAnnounceNeverFollowsRedirects(t *testing.T) {
	leak := clustertest.NewFakeTracker(t)
	tracker := clustertest.NewFakeTracker(t)
	var redirects atomic.Int32
	tracker.SetReply(func(w http.ResponseWriter, _ *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		if redirects.Add(1) == 1 {
			w.Header().Set("Location", leak.URL+apis.AnnouncePath)
			return http.StatusTemporaryRedirect, apis.AnnounceResponse{}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat, c.Secret = 20*time.Millisecond, "s3cret" })
	first := tracker.Replied(t, 0)
	second := tracker.Replied(t, 1)
	if first.Code != http.StatusTemporaryRedirect || !second.Req.Full || second.Code != http.StatusOK {
		t.Fatalf("announces = %d %+v, %d %+v", first.Code, first.Req, second.Code, second.Req)
	}
	if len(leak.Announces()) != 0 || len(leak.Lookups()) != 0 {
		t.Fatal("redirect target received the secret-bearing announce")
	}
}

func TestAnnounceRejectsMalformedReply(t *testing.T) {
	pad := strings.Repeat(" ", apis.MaxReplyBytes-len(`{"ttl":30}`))
	replies := []string{
		`{"ttl":30}{}`,                    // trailing value
		`{"ttl":`,                         // truncated
		`{"ttl":30}` + pad + `{}`,         // trailing value hidden past the read limit
		`{"ttl":30,"pad":"` + pad + `x"}`, // oversized
		`{"ttl":30}` + pad,                // exactly at the limit
	}
	var fulls, reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req apis.AnnounceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req.Full {
			fulls.Add(1)
		}
		_, _ = io.WriteString(w, replies[min(int(reqs.Add(1))-1, len(replies)-1)])
	}))
	t.Cleanup(srv.Close)
	newTestPeer(t, srv.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	// Every malformed reply is a failure: the Full is resent until a clean ack.
	clustertest.WaitFor(t, clustertest.WaitTimeout, "heartbeat after the ack", func() bool { return reqs.Load() >= int32(len(replies))+1 })
	if n := fulls.Load(); n != int32(len(replies)) {
		t.Fatalf("Full announces = %d, want %d", n, len(replies))
	}
}

func TestReadyzAfterFirstAnnounce(t *testing.T) {
	tracker := clustertest.NewFakeTracker(t)
	hold := make(chan struct{})
	tracker.SetReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		select {
		case <-hold:
		case <-r.Context().Done():
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	tracker.Announce(t, 0)
	for method, want := range map[string]string{http.MethodGet: "not announced yet\n", http.MethodHead: ""} {
		if resp, body, _ := do(t, method, srv.URL+"/readyz"); resp.StatusCode != http.StatusServiceUnavailable || string(body) != want {
			t.Fatalf("%s /readyz while the first announce is pending = %d %q, want 503 %q", method, resp.StatusCode, body, want)
		}
	}
	if resp, _, _ := do(t, http.MethodPost, srv.URL+"/readyz"); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /readyz = %d, want 405", resp.StatusCode)
	}
	close(hold)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "readiness once the gateway accepted the first announce", func() bool {
		resp, body, _ := do(t, http.MethodGet, srv.URL+"/readyz")
		return resp.StatusCode == http.StatusOK && len(body) == 0
	})
	// Readiness is sticky: a gateway failing later announces leaves the peer in service.
	tracker.SetReply(func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusInternalServerError, apis.AnnounceResponse{}
	})
	wakeAnnouncer(p)
	tracker.Announce(t, 1)
	wakeAnnouncer(p)
	tracker.Announce(t, 2) // sent only once the announcer handled the 500 to announce 1
	if resp, _, _ := do(t, http.MethodGet, srv.URL+"/readyz"); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /readyz after a failed announce = %d, want 200", resp.StatusCode)
	}
}

func TestMissingStoredObjectIsPulledAgain(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 46, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	pullAndSettle(t, p, srv, hash, body)
	// The stored file is lost underneath a live inventory entry.
	deleteStored(t, p, hash)
	lookups := len(tracker.Lookups())
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash), apis.PeerHeader, "gw"); resp.StatusCode != http.StatusNotFound || len(tracker.Lookups()) != lookups {
		t.Fatalf("relay GET of a missing object = %d after %d new lookups, want 404 and none", resp.StatusCode, len(tracker.Lookups())-lookups)
	}
	pullAndSettle(t, p, srv, hash, body)
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2 (second pull)", n)
	}
	if u := usage(p); u != int64(len(body)) || !stored(t, p, hash) {
		t.Fatalf("usage after the second pull = %d, present %v", u, stored(t, p, hash))
	}
}

func TestPullRestoresMissingStoredObject(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 49, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	seed, _ := newTestPeer(t, tracker.URL, nil)
	commitDirect(t, seed, hash, body)
	deleteStored(t, seed, hash)
	payload := `{"hash":"` + hash.String() + `"}`
	request := httptest.NewRequest(http.MethodPost, apis.PullPath, strings.NewReader(payload))
	response := httptest.NewRecorder()
	seed.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("pull status = %d, want %d", response.Code, http.StatusAccepted)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull of the missing object", func() bool { return len(holder.Requests()) == 1 })
	settled(t, seed)
	if !stored(t, seed, hash) {
		t.Fatal("accepted pull did not restore the missing object")
	}
}

func TestUnreadableStoredObjectIsNotDropped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
	hash, body := clustertest.MakeXorb(t, 48, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	// The stored file is replaced underneath by one nobody may read.
	deleteStored(t, p, hash)
	path := xorbPath(configOf(p).DataDir, hash)
	if err := os.WriteFile(path, body, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	// A transient read failure is not absence: no pull, the inventory stands.
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("GET of an unreadable object = %d", resp.StatusCode)
	}
	if usage(p) != int64(len(body)) || len(holder.Requests()) != 0 || len(tracker.Lookups()) != 0 {
		t.Fatalf("usage = %d, holder GETs = %d, lookups = %d", usage(p), len(holder.Requests()), len(tracker.Lookups()))
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash)); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET after recovery = %d, %v", resp.StatusCode, err)
	}
	settled(t, p)
}

func TestOpenXorbSurvivesEviction(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 49, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 50, 2, 8<<10)
	tracker := clustertest.NewFakeTracker(t)
	p, _ := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB)/2)
	})
	commitDirect(t, p, a, bodyA)
	// A reader has a open when LRU pressure arrives.
	rs, err := storeOf(p).GetXorbReadSeekCloser(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	commitDirect(t, p, b, bodyB)
	if !stored(t, p, a) {
		t.Fatal("open object evicted")
	}
	got, err := io.ReadAll(rs)
	_ = rs.Close()
	if err != nil || !bytes.Equal(got, bodyA) {
		t.Fatalf("read of the open object = %d bytes, %v", len(got), err)
	}
	// The read released its pin and the cache fits its cap again, without any pull.
	u, pins := usage(p), pinsOf(p, a)
	if u > configOf(p).XorbCacheSize || pins != 0 || len(tracker.Lookups()) != 0 {
		t.Fatalf("after release: usage %d, pins %d, lookups %d", u, pins, len(tracker.Lookups()))
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 30, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 31, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 32, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{a: bodyA, b: bodyB, c: bodyC})
	p, srv := newTestPeer(t, tracker.URL, func(cfg *Config) {
		cfg.Heartbeat, cfg.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB))
	})
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, b, bodyB)
	// Reading a makes b the eviction candidate.
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, c, bodyC)
	if stored(t, p, b) || !stored(t, p, a) || !stored(t, p, c) {
		t.Fatal("expected b evicted, a and c kept")
	}
	if u := usage(p); u != int64(len(bodyA)+len(bodyC)) {
		t.Fatalf("usage = %d", u)
	}
	removed, _ := tracker.FindAnnounce(t, 1, func(x clustertest.Announce) bool { return hasRemove(x, b) })
	if removed.Req.Full || !slices.Equal(removed.Req.Add, hashes(c)) || !slices.Equal(removed.Req.Remove, hashes(b)) {
		t.Fatalf("eviction delta = %+v", removed.Req)
	}
	if n := len(holder.Requests()); n != 3 {
		t.Fatalf("holder GETs = %d, want 3", n)
	}
	// A GET for the evicted object pulls again instead of trusting stale state.
	pullAndSettle(t, p, srv, b, bodyB)
	if stored(t, p, a) || !stored(t, p, b) || !stored(t, p, c) {
		t.Fatal("expected a evicted after pulling b again")
	}
}

func TestLRUProtectsActiveReadersAndConvergesOnRelease(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 33, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 34, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{a: bodyA, b: bodyB})
	p, srv := newTestPeer(t, tracker.URL, func(cfg *Config) {
		cfg.Heartbeat, cfg.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB)/2)
	})
	pullAndSettle(t, p, srv, a, bodyA)
	// a is read from the store, b streams from its pull; both readers are held mid-body.
	wa, doneA := serveHeld(t, p, a)
	wb, doneB := serveHeld(t, p, b)
	// The pull leaves the store's list only after its commit admitted b.
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b commit", func() bool { return pullOf(p, b) == nil })
	if !stored(t, p, a) || usage(p) != int64(len(bodyA)+len(bodyB)) {
		t.Fatal("object with an active reader was evicted")
	}
	finishHeld(t, wa, doneA, bodyA)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a evicted on release", func() bool { return !stored(t, p, a) })
	if !stored(t, p, b) {
		t.Fatal("streamed object evicted while its reader was attached")
	}
	finishHeld(t, wb, doneB, bodyB)
	settled(t, p)
	if !stored(t, p, b) || usage(p) != int64(len(bodyB)) {
		t.Fatalf("final usage = %d, b present %v", usage(p), stored(t, p, b))
	}
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2", n)
	}
}

func TestOversizedObjectServedThenEvicted(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 35, 2, 8<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	var resync atomic.Bool
	tracker.SetReply(func(_ http.ResponseWriter, _ *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusOK, apis.AnnounceResponse{Resync: resync.Swap(false), TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = 20*time.Millisecond, int64(len(body)/2)
	})
	pullAndSettle(t, p, srv, hash, body)
	if stored(t, p, hash) || usage(p) != 0 {
		t.Fatalf("oversized object kept, usage %d", usage(p))
	}
	n := len(tracker.Announces())
	resync.Store(true)
	full, _ := tracker.FindAnnounce(t, n+1, func(x clustertest.Announce) bool { return x.Req.Full })
	if len(full.Req.Add) != 0 {
		t.Fatalf("Full after eviction = %+v", full.Req)
	}
	// Repeated demand pulls again each time; no pull is left behind.
	pullAndSettle(t, p, srv, hash, body)
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2", n)
	}
}

func TestRestartRebuildsUsageAndOrder(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 36, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 37, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 38, 2, 8<<10)
	d, bodyD := clustertest.MakeXorb(t, 39, 2, 8<<10)
	dir := t.TempDir()
	now := time.Now()
	for _, o := range []struct {
		hash xet.XorbHash
		body []byte
		age  time.Duration
	}{{a, bodyA, time.Hour}, {b, bodyB, 3 * time.Hour}, {c, bodyC, 2 * time.Hour}} {
		seedStore(t, dir, o.hash, o.body)
		if err := os.Chtimes(xorbPath(dir, o.hash), now, now.Add(-o.age)); err != nil {
			t.Fatal(err)
		}
	}
	tracker, _ := cluster(t, map[xet.XorbHash][]byte{d: bodyD})
	p, srv := newTestPeer(t, tracker.URL, func(cfg *Config) {
		cfg.DataDir, cfg.Heartbeat, cfg.XorbCacheSize = dir, time.Hour, int64(len(bodyA)+len(bodyC))
	})
	// Oldest by mtime goes first at startup, and the Full matches what survived.
	if got := tracker.Announce(t, 0).Req; !got.Full || !slices.Equal(got.Add, hashes(a, c)) {
		t.Fatalf("startup Full = %+v, want %v", got, hashes(a, c))
	}
	if stored(t, p, b) || usage(p) != int64(len(bodyA)+len(bodyC)) {
		t.Fatalf("startup eviction: b present %v, usage %d", stored(t, p, b), usage(p))
	}
	pullAndSettle(t, p, srv, d, bodyD)
	if stored(t, p, c) || !stored(t, p, a) || !stored(t, p, d) {
		t.Fatal("expected c evicted next, a and d kept")
	}
}

func TestCloseCancelsHungAnnounceAndPull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 40, 4, 16<<10)
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{hash: body})
	holder.GateXorb(hash, body, 4<<10, make(chan struct{}))
	unblocked := make(chan struct{})
	tracker.SetReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		<-r.Context().Done()
		close(unblocked)
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	tracker.Announce(t, 0)
	late := doAsync(t, xorbURL(srv, hash))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(holder.Requests()) == 1 })
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the hung announce and pull")
	}
	select {
	case <-unblocked:
	case <-time.After(5 * time.Second):
		t.Fatal("gateway never saw the announce canceled")
	}
	// Close may cancel the pull before the gateway's headers arrive; the reader then gets a complete 502.
	if r := <-late; r.err == nil && r.resp.StatusCode == http.StatusOK {
		t.Fatal("reader completed after Close")
	}
	if stored(t, p, hash) {
		t.Fatal("canceled pull published the xorb")
	}
	if n := len(tracker.Announces()); n != 1 {
		t.Fatalf("announces after Close = %d, want 1", n)
	}
	// Reader release after shutdown must neither block nor panic.
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold GET after Close = %d", resp.StatusCode)
	}
}

// replyMinSeeds answers every announce with 200, a 30s TTL and n as MinSeeds.
func replyMinSeeds(n int) func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
	return func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusOK, apis.AnnounceResponse{TTL: 30, MinSeeds: n}
	}
}

// sortedFiles sorts files by hash, as announces list them.
func sortedFiles(files ...apis.File) []apis.File {
	return slices.SortedFunc(slices.Values(files), func(a, b apis.File) int { return strings.Compare(a.Hash, b.Hash) })
}

// seedsOverCap admits both xorbs of a seed file to a peer whose cap fits one, under tracker answering minSeeds, and returns once the pass this wakes logged nothing evictable and the tracker took the file.
func seedsOverCap(t *testing.T, tracker *clustertest.FakeTracker, minSeeds int) (*Peer, testFile, *warnings) {
	t.Helper()
	tf := makeFile(t, 60, 61)
	xs := tf.xorbHashes()
	tracker.SetReply(replyMinSeeds(minSeeds))
	warned := captureWarnings(t)
	p, _ := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(2*len(tf.xorbs[xs[0]])-1)
	})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the tracker's MinSeeds", func() bool { return minSeedsOf(p) == int64(minSeeds) })
	p.markSeed(tf.file.Hash)
	p.learn(tf.file, xs)
	for _, h := range xs {
		admitStored(t, p, h, tf.xorbs[h])
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a pass finding nothing evictable", func() bool { return warned.count("nothing evictable") == 1 })
	tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return len(a.Req.AddFiles) == 1 && a.Code == http.StatusOK })
	return p, tf, warned
}

func TestSeedCopiesStayUntilEnoughOtherHolders(t *testing.T) {
	tracker := clustertest.NewFakeTracker(t)
	p, tf, warned := seedsOverCap(t, tracker, 1)
	xs := tf.xorbHashes()
	over := func() bool { return usage(p) > configOf(p).XorbCacheSize }
	lookups := tracker.Lookups()
	if len(lookups) != 1 || !slices.Equal(slices.Sorted(slices.Values(lookups[0].Req.Xorbs)), hashes(xs...)) {
		t.Fatalf("lookups = %+v, want one carrying both seed copies", lookups)
	}
	if !stored(t, p, xs[0]) || !stored(t, p, xs[1]) || !over() {
		t.Fatalf("seed copies without other holders evicted: usage %d", usage(p))
	}
	p.evictPass()
	if n, logs := len(tracker.Lookups()), warned.count("nothing evictable"); n != 2 || logs != 1 || !stored(t, p, xs[0]) || !stored(t, p, xs[1]) {
		t.Fatalf("another pass: %d lookups and %d logs, want 2 and 1 with both seed copies kept", n, logs)
	}
	tracker.Hold(apis.Peer{ID: "other", URL: "http://other.test"}, xs...)
	n := len(tracker.Announces())
	p.evictPass()
	if len(tracker.Lookups()) != 3 || stored(t, p, xs[0]) || !stored(t, p, xs[1]) || over() {
		t.Fatalf("pass with another holder: %d lookups, usage %d, want 3 and the oldest seed copy evicted to fit", len(tracker.Lookups()), usage(p))
	}
	removed, _ := tracker.FindAnnounce(t, n, func(a clustertest.Announce) bool { return hasRemove(a, xs[0]) })
	if removed.Req.Full || !slices.Equal(removed.Req.Remove, hashes(xs[0])) || !slices.Equal(removed.Req.RemoveFiles, []string{tf.file.Hash}) {
		t.Fatalf("delta after the seed eviction = %+v, want %s and its file removed", removed.Req, xs[0])
	}
}

func TestSeedCopiesNeedMinSeedsOtherHolders(t *testing.T) {
	tracker := clustertest.NewFakeTracker(t)
	p, tf, _ := seedsOverCap(t, tracker, 2)
	xs := tf.xorbHashes()
	tracker.Hold(apis.Peer{ID: "one", URL: "http://one.test"}, xs...)
	p.evictPass()
	if !stored(t, p, xs[0]) || !stored(t, p, xs[1]) {
		t.Fatal("seed copy evicted with one other holder under MinSeeds 2")
	}
	tracker.Hold(apis.Peer{ID: "two", URL: "http://two.test"}, xs...)
	p.evictPass()
	if stored(t, p, xs[0]) || !stored(t, p, xs[1]) {
		t.Fatal("with two other holders under MinSeeds 2, want the oldest seed copy evicted and the other kept")
	}
}

func TestMinSeedsChangeWakesEviction(t *testing.T) {
	tracker := clustertest.NewFakeTracker(t)
	p, tf, _ := seedsOverCap(t, tracker, 2)
	xs := tf.xorbHashes()
	tracker.Hold(apis.Peer{ID: "one", URL: "http://one.test"}, xs...)
	tracker.SetReply(replyMinSeeds(1))
	wakeAnnouncer(p)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the oldest seed copy evicted once MinSeeds dropped to 1", func() bool { return !stored(t, p, xs[0]) })
	if !stored(t, p, xs[1]) {
		t.Fatal("eviction went past the cap")
	}
}

func TestSeedMarkedBeforeItsFileIsKnown(t *testing.T) {
	tf := makeFile(t, 62, 63)
	xs := tf.xorbHashes()
	other, otherBody := clustertest.MakeXorb(t, 64, 2, 8<<10)
	tracker := clustertest.NewFakeTracker(t)
	tracker.SetReply(replyMinSeeds(1))
	p, _ := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(2*len(tf.xorbs[xs[0]]))
	})
	p.markSeed(tf.file.Hash)
	ingest(t, storeOf(p), tf)
	commitDirect(t, p, other, otherBody)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the cache back under its cap", func() bool { return usage(p) <= configOf(p).XorbCacheSize })
	if !stored(t, p, xs[0]) || !stored(t, p, xs[1]) || stored(t, p, other) {
		t.Fatal("want the seed file's xorbs kept and the younger xorb evicted")
	}
}

func TestFileReportedAgainFollowsItsNewXorbs(t *testing.T) {
	tf := makeFile(t, 65, 66)
	xs := tf.xorbHashes()
	other, otherBody := clustertest.MakeXorb(t, 67, 2, 8<<10)
	tracker := clustertest.NewFakeTracker(t)
	tracker.SetReply(replyMinSeeds(1))
	p, _ := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(2*len(tf.xorbs[xs[0]]))
	})
	for _, h := range xs {
		commitDirect(t, p, h, tf.xorbs[h])
	}
	p.markSeed(tf.file.Hash)
	p.learn(tf.file, xs[:1])
	// A later shard describes the file over its other xorb only.
	p.learn(tf.file, xs[1:])
	commitDirect(t, p, other, otherBody)
	if stored(t, p, xs[0]) || !stored(t, p, xs[1]) || !stored(t, p, other) {
		t.Fatal("want the xorb the seed file no longer references evicted as the oldest and the one it does kept")
	}
}

func TestCompleteFileAnnouncedWhileAllItsXorbsAreCached(t *testing.T) {
	tf := makeFile(t, 68, 69)
	xs := tf.xorbHashes()
	other, otherBody := clustertest.MakeXorb(t, 70, 2, 8<<10)
	tracker := clustertest.NewFakeTracker(t)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(2*len(tf.xorbs[xs[0]]))
	})
	for _, h := range xs {
		commitDirect(t, p, h, tf.xorbs[h])
	}
	if _, err := storeOf(p).PutShard(context.Background(), tf.shard(t)); err != nil {
		t.Fatal(err)
	}
	added, i := tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return len(a.Req.AddFiles) > 0 })
	if added.Req.Full || !slices.Equal(added.Req.AddFiles, []apis.File{tf.file}) || len(added.Req.RemoveFiles) != 0 {
		t.Fatalf("announce once the file is complete = %+v, want a delta adding %+v", added.Req, tf.file)
	}
	// Admitting another xorb evicts the file's oldest one.
	commitDirect(t, p, other, otherBody)
	removed, j := tracker.FindAnnounce(t, i+1, func(a clustertest.Announce) bool { return len(a.Req.RemoveFiles) > 0 })
	if removed.Req.Full || !slices.Equal(removed.Req.Remove, hashes(xs[0])) || !slices.Equal(removed.Req.RemoveFiles, []string{tf.file.Hash}) || len(removed.Req.AddFiles) != 0 {
		t.Fatalf("delta after evicting %s = %+v, want it removed with its file", xs[0], removed.Req)
	}
	// Reading the file's other xorb leaves other the oldest, so re-admitting the evicted one evicts other.
	if resp, _, err := do(t, http.MethodGet, xorbURL(srv, xs[1])); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, %v", xs[1], resp.StatusCode, err)
	}
	commitDirect(t, p, xs[0], tf.xorbs[xs[0]])
	back, _ := tracker.FindAnnounce(t, j+1, func(a clustertest.Announce) bool { return len(a.Req.AddFiles) > 0 })
	if back.Req.Full || !slices.Equal(back.Req.AddFiles, []apis.File{tf.file}) || !slices.Equal(back.Req.Add, hashes(xs[0])) || !slices.Equal(back.Req.Remove, hashes(other)) {
		t.Fatalf("delta after re-admitting %s = %+v, want it and its file added back", xs[0], back.Req)
	}
}

func TestResyncFullCarriesEveryXorbAndCompleteFile(t *testing.T) {
	complete, dropped, partial := makeFile(t, 71, 72), makeFile(t, 73), makeFile(t, 74, 75)
	gone, kept := dropped.xorbHashes()[0], partial.xorbHashes()[0]
	tracker := clustertest.NewFakeTracker(t)
	var refused atomic.Bool
	tracker.SetReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		resync := !req.Full && slices.Contains(req.Remove, gone.String()) && refused.CompareAndSwap(false, true)
		return http.StatusOK, apis.AnnounceResponse{Resync: resync, TTL: 30}
	})
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	ingest(t, storeOf(p), complete)
	ingest(t, storeOf(p), dropped)
	commitDirect(t, p, kept, partial.xorbs[kept])
	p.learn(partial.file, partial.xorbHashes())
	tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool {
		return a.Code == http.StatusOK && slices.Contains(a.Req.AddFiles, dropped.file)
	})
	// The only xorb of a complete file is lost underneath; the tracker refuses the delta removing both.
	deleteStored(t, p, gone)
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, gone)); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET of the lost xorb = %d, want 404", resp.StatusCode)
	}
	delta, i := tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return hasRemove(a, gone) })
	if delta.Req.Full || !slices.Equal(delta.Req.RemoveFiles, []string{dropped.file.Hash}) {
		t.Fatalf("delta after the loss = %+v, want it to remove %s", delta.Req, dropped.file.Hash)
	}
	full := tracker.Announce(t, i+1)
	want := hashes(append(complete.xorbHashes(), kept)...)
	if !full.Req.Full || !slices.Equal(full.Req.Add, want) || !slices.Equal(full.Req.AddFiles, []apis.File{complete.file}) || len(full.Req.Remove)+len(full.Req.RemoveFiles) != 0 {
		t.Fatalf("Full after the resync = %+v, want every cached xorb %v and only %+v", full.Req, want, complete.file)
	}
}

func TestRestartAnnouncesOnlyCompleteFiles(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	ingested, broken := makeFile(t, 76, 77), makeFile(t, 78, 79)
	fetched, missing := makeFile(t, 80), makeFile(t, 81)
	for _, tf := range []testFile{fetched, missing} {
		holder.ServeShard(tf.file, tf.resp)
		tracker.HoldFile(holder.Peer(), tf.file)
	}
	dir := t.TempDir()
	st := newTestStore(t, dir, tracker.URL)
	ctx := context.Background()
	ingest(t, st, ingested)
	ingest(t, st, broken)
	if err := st.DeleteXorb(ctx, broken.xorbHashes()[0]); err != nil {
		t.Fatal(err)
	}
	for _, tf := range []testFile{fetched, missing} {
		if _, err := st.GetFileHashBySHA256(ctx, digestOf(t, tf.file)); err != nil {
			t.Fatal(err)
		}
	}
	for h, body := range fetched.xorbs {
		if _, err := st.PutXorb(ctx, h, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.Close()
	newTestPeer(t, tracker.URL, func(c *Config) { c.DataDir, c.Heartbeat, c.Secret = dir, time.Hour, testSecret })
	got := tracker.Announce(t, 0).Req
	wantXorbs := hashes(append(append(ingested.xorbHashes(), broken.xorbHashes()[1]), fetched.xorbHashes()...)...)
	if wantFiles := sortedFiles(ingested.file, fetched.file); !got.Full || !slices.Equal(got.Add, wantXorbs) || !slices.Equal(got.AddFiles, wantFiles) {
		t.Fatalf("startup announce = %+v, want Full with %v and only the complete %+v", got, wantXorbs, wantFiles)
	}
}

func TestLostXorbLeavesCacheAndAnnounce(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 82, 2, 8<<10)
	tracker := clustertest.NewFakeTracker(t)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	_, i := tracker.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return hasAdd(a, hash) && a.Code == http.StatusOK })
	deleteStored(t, p, hash)
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET of the lost xorb = %d, want 404", resp.StatusCode)
	}
	removed, _ := tracker.FindAnnounce(t, i+1, func(a clustertest.Announce) bool { return hasRemove(a, hash) })
	if removed.Req.Full || !slices.Equal(removed.Req.Remove, hashes(hash)) || len(removed.Req.Add) != 0 {
		t.Fatalf("delta after the loss = %+v, want %s removed", removed.Req, hash)
	}
	if u := usage(p); u != 0 || admitted(p, hash)() {
		t.Fatalf("after the loss: usage %d, cached %v, want 0 and false", u, admitted(p, hash)())
	}
}

func TestLateLostReportKeepsStoredXorb(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 83, 2, 8<<10)
	p, _ := newTestPeer(t, clustertest.NewFakeTracker(t).URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	// A reader that missed hash just before the commit reports it lost only now.
	p.lost(hash)
	if u := usage(p); u != int64(len(body)) || !admitted(p, hash)() {
		t.Fatalf("after a late lost report: usage %d, cached %v, want %d and true", u, admitted(p, hash)(), len(body))
	}
}

func TestConcurrentAdmitsSeedsEvictionsAndAnnouncesStayConsistent(t *testing.T) {
	var files []testFile
	for i := range 6 {
		files = append(files, makeFile(t, byte(100+2*i), byte(101+2*i)))
	}
	tracker, holder := clustertest.NewFakeTracker(t), clustertest.NewFakePeer(t, "holder")
	var replies atomic.Int32
	tracker.SetReply(func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		// MinSeeds alternates between 1 and 2, so every reply wakes eviction.
		return http.StatusOK, apis.AnnounceResponse{TTL: 30, MinSeeds: 1 + int(replies.Add(1)%2)}
	})
	for i, tf := range files {
		for h, body := range tf.xorbs {
			holder.ServeXorb(h, body)
			if i%2 == 0 {
				tracker.Hold(holder.Peer(), h)
			}
		}
	}
	size := int64(len(files[0].xorbs[files[0].xorbHashes()[0]]))
	p, _ := newTestPeer(t, tracker.URL, func(c *Config) { c.Heartbeat, c.XorbCacheSize = 2*time.Millisecond, 3*size })
	ctx := context.Background()
	var wg sync.WaitGroup
	for i, tf := range files {
		wg.Go(func() {
			for h, body := range tf.xorbs {
				if _, err := storeOf(p).PutXorb(ctx, h, bytes.NewReader(body)); err != nil {
					t.Error(err)
				}
			}
			p.learn(tf.file, tf.xorbHashes())
		})
		wg.Go(func() {
			if i%3 != 2 {
				p.markSeed(tf.file.Hash)
			}
		})
		wg.Go(func() {
			for h := range tf.xorbs {
				if rs, err := storeOf(p).GetXorbReadSeekCloser(ctx, h); err == nil {
					_ = rs.Close()
				}
			}
		})
		wg.Go(p.evictPass)
	}
	wg.Wait()
	// A seed file no other peer holds stays complete once its xorbs are back.
	tracker.SetReply(replyMinSeeds(1))
	for h, body := range files[1].xorbs {
		commitDirect(t, p, h, body)
	}
	settled(t, p)
	var announced []apis.File
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the tracker's view to match the cache and the disk", func() bool {
		cached := cachedXorbs(p)
		n := len(tracker.Announces())
		wakeAnnouncer(p)
		tracker.Replied(t, n)
		xorbView, fileView := map[string]bool{}, map[string]apis.File{}
		for _, a := range tracker.Announces() {
			if a.Code != http.StatusOK {
				continue
			}
			if a.Req.Full {
				clear(xorbView)
				clear(fileView)
			}
			for _, h := range a.Req.Add {
				xorbView[h] = true
			}
			for _, h := range a.Req.Remove {
				delete(xorbView, h)
			}
			for _, f := range a.Req.AddFiles {
				fileView[f.Hash] = f
			}
			for _, h := range a.Req.RemoveFiles {
				delete(fileView, h)
			}
		}
		local, _, err := storeOf(p).inventory(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var disk []xet.XorbHash
		for _, x := range local {
			disk = append(disk, x.Hash)
		}
		var complete []apis.File
		for _, tf := range files {
			if !slices.ContainsFunc(tf.xorbHashes(), func(h xet.XorbHash) bool { return !slices.Contains(cached, h.String()) }) {
				complete = append(complete, tf.file)
			}
		}
		announced = sortedFiles(slices.Collect(maps.Values(fileView))...)
		return slices.Equal(cached, cachedXorbs(p)) && slices.Equal(cached, slices.Sorted(maps.Keys(xorbView))) &&
			slices.Equal(cached, hashes(disk...)) && slices.Equal(announced, sortedFiles(complete...))
	})
	if !slices.Contains(announced, files[1].file) {
		t.Fatalf("tracker's files = %+v, want the protected seed file %+v among them", announced, files[1].file)
	}
}
