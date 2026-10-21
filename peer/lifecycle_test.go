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

// settled waits until no pull, spool or pinned reader is left.
func settled(t *testing.T, p *Peer) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "readers released", func() bool {
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
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.CacheDir, c.Heartbeat = dir, time.Hour })
	if got := g.Announce(t, 0).Req; !got.Full || !slices.Equal(got.Add, hashes(a)) {
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
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, 4<<10, release))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	late := doAsync(t, xorbURL(srv, hash))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(g.Requests()) == 1 })
	// Heartbeats keep flowing while the pull is gated; none may carry the object.
	before := len(g.Announces())
	clustertest.WaitFor(t, clustertest.WaitTimeout, "heartbeats", func() bool { return len(g.Announces()) >= before+3 })
	for i, a := range g.Announces() {
		if hasAdd(a, hash) || (i > 0 && (a.Req.Full || len(a.Req.Add)+len(a.Req.Remove) != 0)) {
			t.Fatalf("announce %d while gated = %+v", i, a.Req)
		}
	}
	close(release)
	if r := <-late; r.err != nil || r.resp.StatusCode != http.StatusOK || !bytes.Equal(r.body, body) {
		t.Fatalf("GET = %d, %v", r.resp.StatusCode, r.err)
	}
	added, _ := g.FindAnnounce(t, before, func(a clustertest.Announce) bool { return hasAdd(a, hash) })
	if added.Req.Full || !slices.Equal(added.Req.Add, hashes(hash)) || len(added.Req.Remove) != 0 {
		t.Fatalf("delta after commit = %+v", added.Req)
	}
	settled(t, p)
}

func TestAnnounceFailureRetriesAsFull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 25, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
	var fails atomic.Int32
	fails.Store(2)
	g.SetReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		if slices.Contains(req.Add, hash.String()) && fails.Add(-1) >= 0 {
			return http.StatusInternalServerError, apis.AnnounceResponse{}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	g.Announce(t, 0)
	pullAndSettle(t, p, srv, hash, body)
	acked, i := g.FindAnnounce(t, 1, func(a clustertest.Announce) bool { return hasAdd(a, hash) && a.Code == http.StatusOK })
	// A failed delta may have been applied, so every retry carries the exact inventory.
	if !acked.Req.Full || !slices.Equal(acked.Req.Add, hashes(hash)) {
		t.Fatalf("acked retry = %+v, want Full", acked.Req)
	}
	var rejected []apis.AnnounceRequest
	for _, a := range g.Announces()[:i] {
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
	next := g.Announce(t, i+1)
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
			gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != apis.AnnouncePath {
					clustertest.ServeByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB})(w, r)
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
	g := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
	hold := make(chan struct{})
	g.SetReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		select {
		case <-hold:
		case <-r.Context().Done():
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	if first := g.Announce(t, 0); !first.Req.Full || len(first.Req.Add) != 0 {
		t.Fatalf("startup announce = %+v", first.Req)
	}
	pullAndSettle(t, p, srv, hash, body)
	g.SetReply(nil)
	close(hold)
	if second := g.Announce(t, 1); second.Req.Full || !slices.Equal(second.Req.Add, hashes(hash)) {
		t.Fatalf("delta after the held Full = %+v", second.Req)
	}
}

func TestResyncSendsFullWithCurrentInventory(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 27, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 28, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 29, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB, c: bodyC}))
	var mode atomic.Int32 // 0 ok, 1 fail once, 2 resync once
	g.SetReply(func(_ http.ResponseWriter, _ *http.Request, req apis.AnnounceRequest) (int, apis.AnnounceResponse) {
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
	g.Announce(t, 0)
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, b, bodyB)
	g.FindAnnounce(t, 1, func(x clustertest.Announce) bool { return hasAdd(x, b) && x.Code == http.StatusOK })
	n := len(g.Announces())
	// The eviction of a is lost with this delta; the retry is a Full with the exact inventory.
	mode.Store(1)
	pullAndSettle(t, p, srv, c, bodyC)
	lost := g.Announce(t, n)
	if lost.Code != http.StatusInternalServerError || !hasAdd(lost, c) || !hasRemove(lost, a) {
		t.Fatalf("lost delta = %d %+v", lost.Code, lost.Req)
	}
	p.mu.Lock()
	p.kick()
	p.mu.Unlock()
	retry := g.Announce(t, n+1)
	if !retry.Req.Full || !slices.Equal(retry.Req.Add, hashes(b, c)) || len(retry.Req.Remove) != 0 {
		t.Fatalf("retry after the lost delta = %+v, want Full %v", retry.Req, hashes(b, c))
	}
	// A resync demanded on a delta is answered with a Full at once.
	mode.Store(2)
	p.mu.Lock()
	p.kick()
	p.mu.Unlock()
	demanded := g.Announce(t, n+2)
	if demanded.Req.Full || len(demanded.Req.Add)+len(demanded.Req.Remove) != 0 {
		t.Fatalf("heartbeat before the resync = %+v", demanded.Req)
	}
	full := g.Announce(t, n+3)
	if !full.Req.Full || !slices.Equal(full.Req.Add, hashes(b, c)) || len(full.Req.Remove) != 0 {
		t.Fatalf("resync Full = %+v, want %v", full.Req, hashes(b, c))
	}
	if clustertest.HasXorb(t, p.store, a) || !clustertest.HasXorb(t, p.store, b) || !clustertest.HasXorb(t, p.store, c) {
		t.Fatal("store does not match the announced inventory")
	}
}

func TestAnnounceNeverFollowsRedirects(t *testing.T) {
	leak := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
	var redirects atomic.Int32
	g.SetReply(func(w http.ResponseWriter, _ *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		if redirects.Add(1) == 1 {
			w.Header().Set("Location", leak.URL+apis.AnnouncePath)
			return http.StatusTemporaryRedirect, apis.AnnounceResponse{}
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat, c.Secret = 20*time.Millisecond, "s3cret" })
	first := g.Announce(t, 0)
	second := g.Announce(t, 1)
	if first.Code != http.StatusTemporaryRedirect || !second.Req.Full || second.Code != http.StatusOK {
		t.Fatalf("announces = %d %+v, %d %+v", first.Code, first.Req, second.Code, second.Req)
	}
	if len(leak.Requests()) != 0 || len(leak.Announces()) != 0 {
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
	clustertest.WaitFor(t, clustertest.WaitTimeout, "heartbeat after the ack", func() bool { return reqs.Load() >= int32(len(replies))+1 })
	if n := fulls.Load(); n != int32(len(replies)) {
		t.Fatalf("Full announces = %d, want %d", n, len(replies))
	}
}

func TestMissingStoredObjectLeavesInventoryAndPullsAgain(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 46, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	g.Announce(t, 0)
	pullAndSettle(t, p, srv, hash, body)
	_, n := g.FindAnnounce(t, 1, func(a clustertest.Announce) bool { return hasAdd(a, hash) })
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
	if removed := g.Announce(t, n+1); removed.Req.Full || !slices.Equal(removed.Req.Remove, hashes(hash)) || len(removed.Req.Add) != 0 {
		t.Fatalf("announce after the confirmed miss = %+v, want Remove %s", removed.Req, hash)
	}
	pullAndSettle(t, p, srv, hash, body)
	if n := len(g.Requests()); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2 (second pull)", n)
	}
	if u := usage(p); u != int64(len(body)) || !clustertest.HasXorb(t, p.store, hash) {
		t.Fatalf("usage after the second pull = %d, present %v", u, clustertest.HasXorb(t, p.store, hash))
	}
	if added := g.Announce(t, n+2); added.Req.Full || !slices.Equal(added.Req.Add, hashes(hash)) || len(added.Req.Remove) != 0 {
		t.Fatalf("announce after the second pull = %+v, want Add %s", added.Req, hash)
	}
}

func TestPullRestoresMissingStoredObject(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 49, 2, 8<<10)
	gateway := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
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
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull of the missing object", func() bool { return len(gateway.Requests()) == 1 })
	settled(t, seed)
	if !clustertest.HasXorb(t, seed.store, hash) {
		t.Fatal("accepted pull did not restore the missing object")
	}
}

func TestStaleReaderDoesNotTouchNewerGeneration(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 47, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
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
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	commitDirect(t, p, hash, body)
	_, i := g.FindAnnounce(t, 0, func(x clustertest.Announce) bool { return hasAdd(x, hash) })
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash.String(), nil)
	first, second := p.pin(hash), p.pin(hash)
	if first == nil || second == nil {
		t.Fatal("pin failed for a committed object")
	}
	if err := p.store.DeleteXorb(context.Background(), hash); err != nil {
		t.Fatal(err)
	}
	_ = p.serveStored(httptest.NewRecorder(), req, first) // confirms the miss: drops the first generation and announces it
	_, j := g.FindAnnounce(t, i+1, func(x clustertest.Announce) bool { return hasRemove(x, hash) })
	commitDirect(t, p, hash, body)
	g.FindAnnounce(t, j+1, func(x clustertest.Announce) bool { return hasAdd(x, hash) })
	n := len(g.Announces())
	p.drop(second)
	p.unpin(second)
	time.Sleep(150 * time.Millisecond)
	if got := len(g.Announces()); got != n {
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
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
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
	if usage(p) != int64(len(body)) || len(g.Requests()) != 0 {
		t.Fatalf("usage = %d, gateway GETs = %d", usage(p), len(g.Requests()))
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
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
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
	if !clustertest.HasXorb(t, p.store, a) {
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
	if u > p.cfg.XorbCacheSize || pins != 0 || len(g.Requests()) != 0 {
		t.Fatalf("after release: usage %d, pins %d, gateway GETs %d", u, pins, len(g.Requests()))
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 30, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 31, 2, 8<<10)
	c, bodyC := clustertest.MakeXorb(t, 32, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB, c: bodyC}))
	p, srv := newTestPeer(t, g.URL, func(cfg *Config) {
		cfg.Heartbeat, cfg.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB))
	})
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, b, bodyB)
	// Reading a makes b the eviction candidate.
	pullAndSettle(t, p, srv, a, bodyA)
	pullAndSettle(t, p, srv, c, bodyC)
	if clustertest.HasXorb(t, p.store, b) || !clustertest.HasXorb(t, p.store, a) || !clustertest.HasXorb(t, p.store, c) {
		t.Fatal("expected b evicted, a and c kept")
	}
	if u := usage(p); u != int64(len(bodyA)+len(bodyC)) {
		t.Fatalf("usage = %d", u)
	}
	removed, _ := g.FindAnnounce(t, 1, func(x clustertest.Announce) bool { return hasRemove(x, b) })
	if removed.Req.Full || !slices.Equal(removed.Req.Add, hashes(c)) || !slices.Equal(removed.Req.Remove, hashes(b)) {
		t.Fatalf("eviction delta = %+v", removed.Req)
	}
	if n := len(g.Requests()); n != 3 {
		t.Fatalf("gateway GETs = %d, want 3", n)
	}
	// A GET for the evicted object pulls again instead of trusting stale state.
	pullAndSettle(t, p, srv, b, bodyB)
	if clustertest.HasXorb(t, p.store, a) || !clustertest.HasXorb(t, p.store, b) || !clustertest.HasXorb(t, p.store, c) {
		t.Fatal("expected a evicted after pulling b again")
	}
}

func TestLRUProtectsActiveReadersAndConvergesOnRelease(t *testing.T) {
	a, bodyA := clustertest.MakeXorb(t, 33, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 34, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB}))
	p, srv := newTestPeer(t, g.URL, func(cfg *Config) {
		cfg.Heartbeat, cfg.XorbCacheSize = time.Hour, int64(len(bodyA)+len(bodyB)/2)
	})
	pullAndSettle(t, p, srv, a, bodyA)
	// a is read from the store, b streams from its pull; both readers are held mid-body.
	wa, doneA := serveHeld(t, p, a)
	wb, doneB := serveHeld(t, p, b)
	// The pull leaves p.pulls under the lock that admits b; the store file lands earlier.
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b commit", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.pulls[b] == nil
	})
	if !clustertest.HasXorb(t, p.store, a) || usage(p) != int64(len(bodyA)+len(bodyB)) {
		t.Fatal("object with an active reader was evicted")
	}
	finishHeld(t, wa, doneA, bodyA)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a evicted on release", func() bool { return !clustertest.HasXorb(t, p.store, a) })
	if !clustertest.HasXorb(t, p.store, b) {
		t.Fatal("streamed object evicted while its reader was attached")
	}
	finishHeld(t, wb, doneB, bodyB)
	settled(t, p)
	if !clustertest.HasXorb(t, p.store, b) || usage(p) != int64(len(bodyB)) {
		t.Fatalf("final usage = %d, b present %v", usage(p), clustertest.HasXorb(t, p.store, b))
	}
	if n := len(g.Requests()); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2", n)
	}
}

func TestOversizedObjectServedThenEvicted(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 35, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, clustertest.ServeBody(body))
	var resync atomic.Bool
	g.SetReply(func(_ http.ResponseWriter, _ *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusOK, apis.AnnounceResponse{Resync: resync.Swap(false), TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) {
		c.Heartbeat, c.XorbCacheSize = 20*time.Millisecond, int64(len(body)/2)
	})
	pullAndSettle(t, p, srv, hash, body)
	if clustertest.HasXorb(t, p.store, hash) || usage(p) != 0 {
		t.Fatalf("oversized object kept, usage %d", usage(p))
	}
	n := len(g.Announces())
	resync.Store(true)
	full, _ := g.FindAnnounce(t, n+1, func(x clustertest.Announce) bool { return x.Req.Full })
	if len(full.Req.Add) != 0 {
		t.Fatalf("Full after eviction = %+v", full.Req)
	}
	// Repeated demand pulls again each time; no pull is left behind.
	pullAndSettle(t, p, srv, hash, body)
	if n := len(g.Requests()); n != 2 {
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
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(map[xet.XorbHash][]byte{d: bodyD}))
	p, srv := newTestPeer(t, g.URL, func(cfg *Config) {
		cfg.CacheDir, cfg.Heartbeat, cfg.XorbCacheSize = dir, time.Hour, int64(len(bodyA)+len(bodyC))
	})
	// Oldest by mtime goes first at startup, and the Full matches what survived.
	if got := g.Announce(t, 0).Req; !got.Full || !slices.Equal(got.Add, hashes(a, c)) {
		t.Fatalf("startup Full = %+v, want %v", got, hashes(a, c))
	}
	if clustertest.HasXorb(t, p.store, b) || usage(p) != int64(len(bodyA)+len(bodyC)) {
		t.Fatalf("startup eviction: b present %v, usage %d", clustertest.HasXorb(t, p.store, b), usage(p))
	}
	pullAndSettle(t, p, srv, d, bodyD)
	if clustertest.HasXorb(t, p.store, c) || !clustertest.HasXorb(t, p.store, a) || !clustertest.HasXorb(t, p.store, d) {
		t.Fatal("expected c evicted next, a and d kept")
	}
}

func TestCloseCancelsHungAnnounceAndPull(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 40, 4, 16<<10)
	g := clustertest.NewFakeGateway(t, clustertest.GatedBody(body, 4<<10, make(chan struct{})))
	unblocked := make(chan struct{})
	g.SetReply(func(_ http.ResponseWriter, r *http.Request, _ apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		<-r.Context().Done()
		close(unblocked)
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	g.Announce(t, 0)
	late := doAsync(t, xorbURL(srv, hash))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull start", func() bool { return len(g.Requests()) == 1 })
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
	if clustertest.HasXorb(t, p.store, hash) {
		t.Fatal("canceled pull published the xorb")
	}
	if n := len(g.Announces()); n != 1 {
		t.Fatalf("announces after Close = %d, want 1", n)
	}
	// Reader release after shutdown must neither block nor panic.
	if resp, _, _ := do(t, http.MethodGet, xorbURL(srv, hash)); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold GET after Close = %d", resp.StatusCode)
	}
}
