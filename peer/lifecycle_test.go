package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
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

// serveByHash serves the body registered for the requested xorb hash.
func serveByHash(bodies map[xet.XorbHash][]byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash, _ := xet.ParseXorbHash(path.Base(r.URL.Path))
		body, ok := bodies[hash]
		if !ok {
			http.NotFound(w, r)
			return
		}
		serveBody(body)(w, r)
	}
}

// settled waits until no pull, spool or pinned reader is left.
func settled(t *testing.T, p *Peer) {
	t.Helper()
	clustertest.WaitFor(t, waitTimeout, "spool cleanup", spoolEmpty(p))
	clustertest.WaitFor(t, waitTimeout, "readers released", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for e := range p.xorbs.Oldest() {
			if e.V.pins != 0 {
				return false
			}
		}
		return len(p.pulls) == 0
	})
}

// pullAndSettle pulls hash through the peer and waits for the pull to drain.
func pullAndSettle(t *testing.T, p *Peer, srv *httptest.Server, hash xet.XorbHash, body []byte) {
	t.Helper()
	resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash))
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET %s = %d, %d bytes, %v", hash, resp.StatusCode, len(got), err)
	}
	settled(t, p)
}

func usage(p *Peer) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.xorbs.Usage()
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
	g := newGateway(t, serveByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.CacheDir, c.Heartbeat = dir, time.Hour })
	if got := g.announce(t, 0).req; !got.Full || !slices.Equal(got.Add, hashes(a)) {
		t.Fatalf("startup announce = %+v, want only %s", got, a)
	}
	if _, err := os.Stat(xorbPath(dir, b)); !os.IsNotExist(err) {
		t.Fatalf("corrupt object kept: %v", err)
	}
	if u := usage(p); u != int64(len(bodyA)) {
		t.Fatalf("usage = %d, want %d", u, len(bodyA))
	}
}

func TestPullEntersInventoryOnlyAfterValidation(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 24, 4, 16<<10)
	release := make(chan struct{})
	g := newGateway(t, gatedBody(body, 4<<10, release))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	late := doAsync(t, xorbURL(srv, hash))
	clustertest.WaitFor(t, waitTimeout, "pull start", func() bool { return g.gets.Load() == 1 })
	// Heartbeats keep flowing while the pull is gated; none may carry the object.
	before := g.announceCount()
	clustertest.WaitFor(t, waitTimeout, "heartbeats", func() bool { return g.announceCount() >= before+3 })
	g.mu.Lock()
	for i, a := range g.announces {
		if hasAdd(a, hash) || (i > 0 && (a.req.Full || len(a.req.Add)+len(a.req.Remove) != 0)) {
			t.Fatalf("announce %d while gated = %+v", i, a.req)
		}
	}
	g.mu.Unlock()
	close(release)
	if r := <-late; r.err != nil || r.resp.StatusCode != http.StatusOK || !bytes.Equal(r.body, body) {
		t.Fatalf("GET = %d, %v", r.resp.StatusCode, r.err)
	}
	added, _ := g.findAnnounce(t, before, func(a announced) bool { return hasAdd(a, hash) })
	if added.req.Full || !slices.Equal(added.req.Add, hashes(hash)) || len(added.req.Remove) != 0 {
		t.Fatalf("delta after commit = %+v", added.req)
	}
	settled(t, p)
}

func TestAnnounceFailureRetriesAsFull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 25, 2, 8<<10)
	g := newGateway(t, serveBody(body))
	var fails atomic.Int32
	fails.Store(2)
	g.setReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		if slices.Contains(req.Add, hash.String()) && fails.Add(-1) >= 0 {
			return http.StatusInternalServerError, apis.AnnounceResponse{}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	g.announce(t, 0)
	pullAndSettle(t, p, srv, hash, body)
	acked, i := g.findAnnounce(t, 1, func(a announced) bool { return hasAdd(a, hash) && a.code == http.StatusOK })
	// A failed delta may have been applied, so every retry carries the exact inventory.
	if !acked.req.Full || !slices.Equal(acked.req.Add, hashes(hash)) {
		t.Fatalf("acked retry = %+v, want Full", acked.req)
	}
	var rejected []apis.AnnounceRequest
	g.mu.Lock()
	for _, a := range g.announces[:i] {
		if hasAdd(a, hash) {
			if a.code != http.StatusInternalServerError {
				t.Fatalf("announce before the ack = %d", a.code)
			}
			rejected = append(rejected, a.req)
		}
	}
	g.mu.Unlock()
	if len(rejected) != 2 || rejected[0].Full || !rejected[1].Full {
		t.Fatalf("rejected announces = %+v, want one delta then one Full", rejected)
	}
	// The acked object is not resent on later heartbeats.
	next := g.announce(t, i+1)
	if next.req.Full || len(next.req.Add)+len(next.req.Remove) != 0 {
		t.Fatalf("heartbeat after the ack = %+v", next.req)
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
			gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != apis.AnnouncePath {
					serveByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB})(w, r)
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
				clustertest.WaitFor(t, waitTimeout, "announce", func() bool { return count() > i })
				mu.Lock()
				defer mu.Unlock()
				if got := slices.Sorted(maps.Keys(holdings)); !reqs[i].Full || !slices.Equal(got, hashes(want)) {
					t.Fatalf("retry = %+v, gateway holdings = %v, want Full with %v", reqs[i], got, hashes(want))
				}
			}
			clustertest.WaitFor(t, waitTimeout, "startup ack", func() bool { return count() == 1 })
			// Add a is applied but its reply is lost; a is evicted before the retry.
			loseNext.Store(true)
			pullAndSettle(t, p, srv, a, bodyA)
			clustertest.WaitFor(t, waitTimeout, "lost delta", func() bool { return count() == 2 })
			pullAndSettle(t, p, srv, b, bodyB)
			check(2, b)
			// Remove b is applied but its reply is lost; b is pulled again before the retry.
			loseNext.Store(true)
			pullAndSettle(t, p, srv, a, bodyA)
			clustertest.WaitFor(t, waitTimeout, "lost delta", func() bool { return count() == 4 })
			pullAndSettle(t, p, srv, b, bodyB)
			check(4, b)
		})
	}
}

func TestInFlightAnnounceKeepsConcurrentCommitForNextDelta(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 26, 2, 8<<10)
	g := newGateway(t, serveBody(body))
	hold := make(chan struct{})
	g.setReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		select {
		case <-hold:
		case <-r.Context().Done():
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	if first := g.announce(t, 0); !first.req.Full || len(first.req.Add) != 0 {
		t.Fatalf("startup announce = %+v", first.req)
	}
	pullAndSettle(t, p, srv, hash, body)
	g.setReply(nil)
	close(hold)
	if second := g.announce(t, 1); second.req.Full || !slices.Equal(second.req.Add, hashes(hash)) {
		t.Fatalf("delta after the held Full = %+v", second.req)
	}
}

func TestResyncSendsFullWithCurrentInventory(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 27, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 28, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 29, 2, 8<<10)
	g := newGateway(t, serveByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB, c: bodyC}))
	var mode atomic.Int32 // 0 ok, 1 fail once, 2 resync once
	g.setReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		switch mode.Swap(0) {
		case 1:
			return http.StatusInternalServerError, apis.AnnounceResponse{}
		case 2:
			return http.StatusOK, apis.AnnounceResponse{Resync: true, TTL: 30}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB))
	})
	g.announce(t, 0)
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, b, bodyB)
	g.findAnnounce(t, 1, func(x announced) bool { return hasAdd(x, b) && x.code == http.StatusOK })
	n := g.announceCount()
	// The eviction of a is lost with this delta; the retry is a Full with the exact inventory.
	mode.Store(1)
	pullAndSettle(t, p, srv, c, bodyC)
	lost := g.announce(t, n)
	if lost.code != http.StatusInternalServerError || !hasAdd(lost, c) || !hasRemove(lost, a) {
		t.Fatalf("lost delta = %d %+v", lost.code, lost.req)
	}
	p.mu.Lock()
	p.kick()
	p.mu.Unlock()
	retry := g.announce(t, n+1)
	if !retry.req.Full || !slices.Equal(retry.req.Add, hashes(b, c)) || len(retry.req.Remove) != 0 {
		t.Fatalf("retry after the lost delta = %+v, want Full %v", retry.req, hashes(b, c))
	}
	// A resync demanded on a delta is answered with a Full at once.
	mode.Store(2)
	p.mu.Lock()
	p.kick()
	p.mu.Unlock()
	demanded := g.announce(t, n+2)
	if demanded.req.Full || len(demanded.req.Add)+len(demanded.req.Remove) != 0 {
		t.Fatalf("heartbeat before the resync = %+v", demanded.req)
	}
	full := g.announce(t, n+3)
	if !full.req.Full || !slices.Equal(full.req.Add, hashes(b, c)) || len(full.req.Remove) != 0 {
		t.Fatalf("resync Full = %+v, want %v", full.req, hashes(b, c))
	}
	if hasXorb(t, p, a) || !hasXorb(t, p, b) || !hasXorb(t, p, c) {
		t.Fatal("store does not match the announced inventory")
	}
}

func TestAnnounceNeverFollowsRedirects(t *testing.T) {
	leak := newGateway(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g := newGateway(t, serveByHash(nil))
	var redirects atomic.Int32
	g.setReply(func(w http.ResponseWriter, _ *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		if redirects.Add(1) == 1 {
			w.Header().Set("Location", leak.URL+apis.AnnouncePath)
			return http.StatusTemporaryRedirect, apis.AnnounceResponse{}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat, c.Secret = 20*time.Millisecond, "s3cret" })
	first := g.announce(t, 0)
	second := g.announce(t, 1)
	if first.code != http.StatusTemporaryRedirect || !second.req.Full || second.code != http.StatusOK {
		t.Fatalf("announces = %d %+v, %d %+v", first.code, first.req, second.code, second.req)
	}
	if leak.gets.Load() != 0 || leak.announceCount() != 0 {
		t.Fatal("redirect target received the secret-bearing announce")
	}
}

func TestAnnounceRejectsMalformedReply(t *testing.T) {
	pad := strings.Repeat(" ", maxReplyBytes-len(`{"ttl":30}`))
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
	clustertest.WaitFor(t, waitTimeout, "heartbeat after the ack", func() bool { return reqs.Load() >= int32(len(replies))+1 })
	if n := fulls.Load(); n != int32(len(replies)) {
		t.Fatalf("Full announces = %d, want %d", n, len(replies))
	}
}

func TestMissingStoredObjectLeavesInventoryAndPullsAgain(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 46, 2, 8<<10)
	g := newGateway(t, serveBody(body))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	g.announce(t, 0)
	pullAndSettle(t, p, srv, hash, body)
	_, n := g.findAnnounce(t, 1, func(a announced) bool { return hasAdd(a, hash) })
	// The stored file is lost underneath a live inventory entry.
	if err := p.store.DeleteXorb(context.Background(), hash); err != nil {
		t.Fatal(err)
	}
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash), apis.PeerHeader, "gw"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("relay GET of a missing object = %d", resp.StatusCode)
	}
	if u := usage(p); u != 0 {
		t.Fatalf("usage after the confirmed miss = %d, want 0", u)
	}
	if removed := g.announce(t, n+1); removed.req.Full || !slices.Equal(removed.req.Remove, hashes(hash)) || len(removed.req.Add) != 0 {
		t.Fatalf("announce after the confirmed miss = %+v, want Remove %s", removed.req, hash)
	}
	pullAndSettle(t, p, srv, hash, body)
	if n := g.gets.Load(); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2 (second pull)", n)
	}
	if u := usage(p); u != int64(len(body)) || !hasXorb(t, p, hash) {
		t.Fatalf("usage after the second pull = %d, present %v", u, hasXorb(t, p, hash))
	}
	if added := g.announce(t, n+2); added.req.Full || !slices.Equal(added.req.Add, hashes(hash)) || len(added.req.Remove) != 0 {
		t.Fatalf("announce after the second pull = %+v, want Add %s", added.req, hash)
	}
}

func TestPullRestoresMissingStoredObject(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 49, 2, 8<<10)
	gateway := newGateway(t, serveBody(body))
	seed, _ := newTestPeer(t, gateway.URL, nil)
	commitDirect(t, seed, hash, body)
	if err := seed.store.DeleteXorb(context.Background(), hash); err != nil {
		t.Fatal(err)
	}
	payload := `{"hash":"` + hash.String() + `"}`
	request := httptest.NewRequest(http.MethodPost, apis.PullPath, strings.NewReader(payload))
	response := httptest.NewRecorder()
	seed.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("pull status = %d, want %d", response.Code, http.StatusAccepted)
	}
	clustertest.WaitFor(t, waitTimeout, "pull of the missing object", func() bool { return gateway.gets.Load() == 1 })
	settled(t, seed)
	if !hasXorb(t, seed, hash) {
		t.Fatal("accepted pull did not restore the missing object")
	}
}

func TestStaleReaderDoesNotTouchNewerGeneration(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 47, 2, 8<<10)
	g := newGateway(t, serveByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash.String(), nil)
	// Two readers hold the first generation when its file vanishes.
	first, second := p.pin(hash), p.pin(hash)
	if first == nil || second == nil {
		t.Fatal("pin failed for a committed object")
	}
	if err := p.store.DeleteXorb(context.Background(), hash); err != nil {
		t.Fatal(err)
	}
	if err := p.serveStored(httptest.NewRecorder(), req, first); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("read of the missing file = %v", err)
	}
	if usage(p) != 0 {
		t.Fatalf("usage after the confirmed miss = %d", usage(p))
	}
	// A second pull admits a new generation; the stale reader confirming the same miss must not remove it.
	commitDirect(t, p, hash, body)
	p.drop(second)
	p.unpin(second)
	rd := p.pin(hash)
	if rd == nil || rd == first {
		t.Fatal("new generation missing from the inventory")
	}
	p.mu.Lock()
	pins := rd.V.pins
	p.mu.Unlock()
	if pins != 1 {
		t.Fatalf("new generation pins = %d after the stale unpin, want 1", pins)
	}
	w := httptest.NewRecorder()
	if err := p.serveStored(w, req, rd); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), body) || usage(p) != int64(len(body)) {
		t.Fatalf("new generation = %d, %d bytes, usage %d", w.Code, w.Body.Len(), usage(p))
	}
	settled(t, p)
}

// A stale reader confirming a miss on a replaced generation neither drops the replacement nor wakes the announcer (pins drop's identity check).
func TestStaleDropDoesNotAnnounce(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 72, 2, 8<<10)
	g := newGateway(t, serveByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	_, i := g.findAnnounce(t, 0, func(x announced) bool { return hasAdd(x, hash) })
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash.String(), nil)
	first, second := p.pin(hash), p.pin(hash)
	if first == nil || second == nil {
		t.Fatal("pin failed for a committed object")
	}
	if err := p.store.DeleteXorb(context.Background(), hash); err != nil {
		t.Fatal(err)
	}
	_ = p.serveStored(httptest.NewRecorder(), req, first) // confirms the miss: drops the first generation and announces it
	_, j := g.findAnnounce(t, i+1, func(x announced) bool { return hasRemove(x, hash) })
	commitDirect(t, p, hash, body)
	g.findAnnounce(t, j+1, func(x announced) bool { return hasAdd(x, hash) })
	n := g.announceCount()
	p.drop(second)
	p.unpin(second)
	time.Sleep(150 * time.Millisecond)
	if got := g.announceCount(); got != n {
		t.Fatalf("stale drop woke the announcer: %d announces, want %d", got, n)
	}
	rd := p.pin(hash)
	if rd == nil || rd == first {
		t.Fatal("new generation missing from the inventory")
	}
	p.unpin(rd)
}

func TestUnreadableStoredObjectIsNotDropped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
	hash, body := clustertest.MakeXorb(t, 48, 2, 8<<10)
	g := newGateway(t, serveByHash(nil))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	path := xorbPath(p.cfg.CacheDir, hash)
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	// A transient read failure is not absence: no pull, the inventory stands.
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("GET of an unreadable object = %d", resp.StatusCode)
	}
	if usage(p) != int64(len(body)) || g.gets.Load() != 0 {
		t.Fatalf("usage = %d, gateway GETs = %d", usage(p), g.gets.Load())
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if resp, got, err := do(t, http.MethodGet, xorbURL(srv, hash)); err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET after recovery = %d, %v", resp.StatusCode, err)
	}
	settled(t, p)
}

func TestReadyDecisionSurvivesEviction(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 49, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 50, 2, 8<<10)
	g := newGateway(t, serveByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB)/2)
	})
	commitDirect(t, p, a, bodyA)
	// The handler decided a is ready; LRU pressure arrives before it opens the file.
	f, rd, err := p.acquire(a, false)
	if err != nil || f != nil || rd == nil {
		t.Fatalf("acquire = %v, %v, %v, want a reserved ready", f, rd, err)
	}
	commitDirect(t, p, b, bodyB)
	if !hasXorb(t, p, a) {
		t.Fatal("ready object evicted between the decision and the read")
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+a.String(), nil)
	w := httptest.NewRecorder()
	if err := p.serveStored(w, req, rd); err != nil || w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), bodyA) {
		t.Fatalf("read after the ready decision = %d, %v", w.Code, err)
	}
	// The read released its pin and the cache fits its cap again, without any gateway fetch.
	p.mu.Lock()
	u, pins := p.xorbs.Usage(), rd.V.pins
	p.mu.Unlock()
	if u > p.cfg.XorbCacheSize || pins != 0 || g.gets.Load() != 0 {
		t.Fatalf("after release: usage %d, pins %d, gateway GETs %d", u, pins, g.gets.Load())
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 30, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 31, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 32, 2, 8<<10)
	g := newGateway(t, serveByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB, c: bodyC}))
	p, srv := newTestPeer(t, g.URL, func(cfg *Config) {
		cfg.Heartbeat, cfg.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB))
	})
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, b, bodyB)
	// Reading a makes b the eviction candidate.
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, c, bodyC)
	if hasXorb(t, p, b) || !hasXorb(t, p, a) || !hasXorb(t, p, c) {
		t.Fatal("expected b evicted, a and c kept")
	}
	if u := usage(p); u != int64(len(bodyA)+len(bodyC)) {
		t.Fatalf("usage = %d", u)
	}
	removed, _ := g.findAnnounce(t, 1, func(x announced) bool { return hasRemove(x, b) })
	if removed.req.Full || !slices.Equal(removed.req.Add, hashes(c)) || !slices.Equal(removed.req.Remove, hashes(b)) {
		t.Fatalf("eviction delta = %+v", removed.req)
	}
	if n := g.gets.Load(); n != 3 {
		t.Fatalf("gateway GETs = %d, want 3", n)
	}
	// A GET for the evicted object pulls again instead of trusting stale state.
	pullAndSettle(t, p, srv, b, bodyB)
	if hasXorb(t, p, a) || !hasXorb(t, p, b) || !hasXorb(t, p, c) {
		t.Fatal("expected a evicted after pulling b again")
	}
}

func TestLRUProtectsActiveReadersAndConvergesOnRelease(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 33, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 34, 2, 8<<10)
	g := newGateway(t, serveByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB}))
	p, srv := newTestPeer(t, g.URL, func(cfg *Config) {
		cfg.Heartbeat, cfg.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB)/2)
	})
	pullAndSettle(t, p, srv, a, bodyA)
	// a is read from the store, b streams from its pull; both readers are held mid-body.
	wa, doneA := serveHeld(t, p, a)
	wb, doneB := serveHeld(t, p, b)
	// The pull leaves p.pulls under the lock that admits b; the store file lands earlier.
	clustertest.WaitFor(t, waitTimeout, "b commit", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.pulls[b] == nil
	})
	if !hasXorb(t, p, a) || usage(p) != int64(len(bodyA)+len(bodyB)) {
		t.Fatal("object with an active reader was evicted")
	}
	finishHeld(t, wa, doneA, bodyA)
	clustertest.WaitFor(t, waitTimeout, "a evicted on release", func() bool { return !hasXorb(t, p, a) })
	if !hasXorb(t, p, b) {
		t.Fatal("streamed object evicted while its reader was attached")
	}
	finishHeld(t, wb, doneB, bodyB)
	settled(t, p)
	if !hasXorb(t, p, b) || usage(p) != int64(len(bodyB)) {
		t.Fatalf("final usage = %d, b present %v", usage(p), hasXorb(t, p, b))
	}
	if n := g.gets.Load(); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2", n)
	}
}

func TestOversizedObjectServedThenEvicted(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 35, 2, 8<<10)
	g := newGateway(t, serveBody(body))
	var resync atomic.Bool
	g.setReply(func(_ http.ResponseWriter, _ *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusOK, apis.AnnounceResponse{Resync: resync.Swap(false), TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = 20*time.Millisecond, int64(len(body)/2)
	})
	pullAndSettle(t, p, srv, hash, body)
	if hasXorb(t, p, hash) || usage(p) != 0 {
		t.Fatalf("oversized object kept, usage %d", usage(p))
	}
	n := g.announceCount()
	resync.Store(true)
	full, _ := g.findAnnounce(t, n+1, func(x announced) bool { return x.req.Full })
	if len(full.req.Add) != 0 {
		t.Fatalf("Full after eviction = %+v", full.req)
	}
	// Repeated demand pulls again each time; no pull is left behind.
	pullAndSettle(t, p, srv, hash, body)
	if n := g.gets.Load(); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2", n)
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
	g := newGateway(t, serveByHash(map[xet.XorbHash][]byte{d: bodyD}))
	p, srv := newTestPeer(t, g.URL, func(cfg *Config) {
		cfg.CacheDir, cfg.Heartbeat, cfg.XorbCacheSize = dir, time.Hour, int64(len(bodyA)+len(bodyC))
	})
	// Oldest by mtime goes first at startup, and the Full matches what survived.
	if got := g.announce(t, 0).req; !got.Full || !slices.Equal(got.Add, hashes(a, c)) {
		t.Fatalf("startup Full = %+v, want %v", got, hashes(a, c))
	}
	if hasXorb(t, p, b) || usage(p) != int64(len(bodyA)+len(bodyC)) {
		t.Fatalf("startup eviction: b present %v, usage %d", hasXorb(t, p, b), usage(p))
	}
	pullAndSettle(t, p, srv, d, bodyD)
	if hasXorb(t, p, c) || !hasXorb(t, p, a) || !hasXorb(t, p, d) {
		t.Fatal("expected c evicted next, a and d kept")
	}
}

func TestCloseCancelsHungAnnounceAndPull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 40, 4, 16<<10)
	g := newGateway(t, gatedBody(body, 4<<10, make(chan struct{})))
	unblocked := make(chan struct{})
	g.setReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		<-r.Context().Done()
		close(unblocked)
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	g.announce(t, 0)
	late := doAsync(t, xorbURL(srv, hash))
	clustertest.WaitFor(t, waitTimeout, "pull start", func() bool { return g.gets.Load() == 1 })
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
	if hasXorb(t, p, hash) {
		t.Fatal("canceled pull published the xorb")
	}
	if n := g.announceCount(); n != 1 {
		t.Fatalf("announces after Close = %d, want 1", n)
	}
	// Reader release after shutdown must neither block nor panic.
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold GET after Close = %d", resp.StatusCode)
	}
}
