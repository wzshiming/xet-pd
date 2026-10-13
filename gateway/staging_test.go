package gateway

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// pull is one pull request as a fake seed saw it.
type pull struct {
	auth, ctype, body string
	staged            bool // the gateway held the complete bytes and the offsets sidecar when it asked
}

// seed is a fake peer registered in the store's registry; it records pull requests and answers status.
type seed struct {
	*httptest.Server
	peer     apis.Peer
	st       *store
	mu       sync.Mutex
	status   int
	redirect string
	block    chan struct{} // when set, requests wait for it or their cancellation
	pulls    []pull
	canceled int
}

func newSeed(t *testing.T, st *store, id string) *seed {
	t.Helper()
	s := &seed{st: st, status: http.StatusAccepted}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	s.peer = apis.Peer{ID: id, URL: s.URL}
	st.reg.announce(fullReq(s.peer, "e"))
	return s
}

func (s *seed) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != apis.PullPath {
		http.NotFound(w, r)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var req apis.PullRequest
	_ = json.Unmarshal(body, &req)
	hash, _ := xet.ParseXorbHash(req.Hash)
	ok, _ := s.st.Storage.HasXorb(r.Context(), hash)
	_, serr := s.st.readSidecar(hash)
	s.mu.Lock()
	s.pulls = append(s.pulls, pull{r.Header.Get("Authorization"), r.Header.Get("Content-Type"), string(body), ok && serr == nil})
	status, redirect, block := s.status, s.redirect, s.block
	s.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-r.Context().Done():
			s.mu.Lock()
			s.canceled++
			s.mu.Unlock()
			return
		}
	}
	if redirect != "" {
		http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
		return
	}
	w.WriteHeader(status)
}

func (s *seed) requests() []pull {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.pulls)
}

func (s *seed) answer(status int, redirect string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.redirect = status, redirect
}

// hang makes every following request wait until its client gives up.
func (s *seed) hang() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = make(chan struct{})
}

func (s *seed) cancellations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canceled
}

// holds announces hashes as held by this seed, as the peer does after a pull commits.
func (s *seed) holds(hashes ...xet.XorbHash) {
	var add []string
	for _, h := range hashes {
		add = append(add, h.String())
	}
	s.st.reg.announce(deltaReq(s.peer, "e", add, nil))
}

// newTestStaging runs maintenance on wakes only unless the config asks for a period.
func newTestStaging(t *testing.T, st *store, cfg stagingConfig) *staging {
	t.Helper()
	cfg.tick = cmp.Or(cfg.tick, time.Hour)
	m, err := newStaging(st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

const waitTimeout = 5 * time.Second

func hasLocal(t *testing.T, st *store, hash xet.XorbHash) bool {
	t.Helper()
	ok, err := st.Storage.HasXorb(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestPutSeedsReplicasOnce(t *testing.T) {
	x := makeXorb(t, 21, 2, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	seeds := map[string]*seed{}
	for _, id := range []string{"s1", "s2", "s3"} {
		seeds[id] = newSeed(t, st, id)
	}
	m := newTestStaging(t, st, stagingConfig{secret: "tok"})
	ctx := context.Background()
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	selected := reg.rank(x.hash.String())[:2]
	for _, p := range selected {
		clustertest.WaitFor(t, waitTimeout, "pull at "+p.ID, func() bool { return len(seeds[p.ID].requests()) == 1 })
	}
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil || inserted {
		t.Fatalf("dedup PutXorb = %v, %v", inserted, err)
	}
	m.kick()
	time.Sleep(30 * time.Millisecond)
	want, _ := json.Marshal(apis.PullRequest{Hash: x.hash.String()})
	for id, s := range seeds {
		reqs := s.requests()
		if !slices.ContainsFunc(selected, func(p ranked) bool { return p.ID == id }) {
			if len(reqs) != 0 {
				t.Fatalf("unselected seed %s was asked: %+v", id, reqs)
			}
			continue
		}
		if len(reqs) != 1 {
			t.Fatalf("seed %s saw %d pulls for one xorb, want exactly 1", id, len(reqs))
		}
		if r := reqs[0]; r.auth != "Bearer tok" || r.ctype != "application/json" || r.body != string(want) || !r.staged {
			t.Fatalf("pull at %s = %+v, want bearer secret, JSON %s over a staged xorb", id, r, want)
		}
	}
	if got := m.snapshot(); got != int64(len(x.body)) {
		t.Fatalf("usage = %d, want %d", got, len(x.body))
	}
	// Bytes re-inserted after an out-of-band delete are a new copy: counted once and seeded again.
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil || !inserted {
		t.Fatalf("PutXorb after delete = %v, %v, want inserted", inserted, err)
	}
	if got := m.snapshot(); got != int64(len(x.body)) {
		t.Fatalf("usage after re-insertion = %d, want %d", got, len(x.body))
	}
	for _, p := range selected {
		clustertest.WaitFor(t, waitTimeout, "second pull at "+p.ID, func() bool { return len(seeds[p.ID].requests()) == 2 })
	}
}

// A 202 only means a seed started pulling; a copy leaves the cache once enough seeds announce it.
func TestAcceptedIsNotAHolding(t *testing.T) {
	for _, minSeeds := range []int{1, 2} {
		t.Run(fmt.Sprintf("minSeeds=%d", minSeeds), func(t *testing.T) {
			x := makeXorb(t, 22, 2, 4<<10)
			st, _ := newTestStore(t, t.TempDir())
			s1, s2 := newSeed(t, st, "s1"), newSeed(t, st, "s2")
			m := newTestStaging(t, st, stagingConfig{cacheSize: 1, minSeeds: minSeeds})
			ctx := context.Background()
			if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
				t.Fatal(err)
			}
			for _, s := range []*seed{s1, s2} {
				clustertest.WaitFor(t, waitTimeout, "pull at "+s.peer.ID, func() bool {
					if !hasLocal(t, st, x.hash) {
						t.Fatal("xorb evicted on 202 alone")
					}
					return len(s.requests()) == 1
				})
			}
			time.Sleep(30 * time.Millisecond)
			if !hasLocal(t, st, x.hash) {
				t.Fatal("xorb evicted on 202 alone")
			}
			s1.holds(x.hash)
			m.kick()
			if minSeeds == 2 {
				time.Sleep(30 * time.Millisecond)
				if !hasLocal(t, st, x.hash) {
					t.Fatal("xorb evicted with one holder, want two")
				}
				s2.holds(x.hash)
				m.kick()
			}
			clustertest.WaitFor(t, waitTimeout, "eviction", func() bool { return !hasLocal(t, st, x.hash) && m.snapshot() == 0 })
			if got := m.snapshot(); got != 0 {
				t.Fatalf("usage after eviction = %d, want 0", got)
			}
			if _, err := st.readSidecar(x.hash); err != nil {
				t.Fatalf("sidecar must survive eviction: %v", err)
			}
			if start, end, err := st.GetXorbDataRange(ctx, x.hash, 0, 2); err != nil || end <= start {
				t.Fatalf("GetXorbDataRange after eviction = %d, %d, %v", start, end, err)
			}
		})
	}
}

func TestNoPeersRetainsUntilRegistered(t *testing.T) {
	x := makeXorb(t, 23, 2, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	m := newTestStaging(t, st, stagingConfig{cacheSize: 1})
	if _, err := st.PutXorb(context.Background(), x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if !hasLocal(t, st, x.hash) {
		t.Fatal("xorb evicted with no seed at all")
	}
	s := newSeed(t, st, "s1")
	m.kick()
	clustertest.WaitFor(t, waitTimeout, "pull at the newly registered seed", func() bool { return len(s.requests()) == 1 })
	s.holds(x.hash)
	m.kick()
	clustertest.WaitFor(t, waitTimeout, "eviction", func() bool { return !hasLocal(t, st, x.hash) })
}

// Failed and refused pulls are retried per (xorb, seed) after the backoff; redirects are not followed.
func TestFailedPullRetriesAfterBackoff(t *testing.T) {
	x := makeXorb(t, 24, 2, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { elsewhere.Add(1) }))
	t.Cleanup(other.Close)
	s := newSeed(t, st, "s1")
	s.answer(http.StatusInternalServerError, "")
	newTestStaging(t, st, stagingConfig{secret: "tok", retry: 150 * time.Millisecond, tick: 5 * time.Millisecond})
	if _, err := st.PutXorb(context.Background(), x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	clustertest.WaitFor(t, waitTimeout, "first pull", func() bool { return len(s.requests()) == 1 })
	time.Sleep(40 * time.Millisecond)
	if n := len(s.requests()); n != 1 {
		t.Fatalf("%d pulls within the backoff, want 1", n)
	}
	clustertest.WaitFor(t, waitTimeout, "retry after backoff", func() bool { return len(s.requests()) == 2 })
	s.answer(0, other.URL+apis.PullPath)
	clustertest.WaitFor(t, waitTimeout, "retries past a redirect", func() bool { return len(s.requests()) >= 4 })
	if n := elsewhere.Load(); n != 0 {
		t.Fatalf("redirect target saw %d requests, want none", n)
	}
	for _, r := range s.requests() {
		if r.auth != "Bearer tok" {
			t.Fatalf("pull without the secret: %+v", r)
		}
	}
}

func TestEvictionIsLRU(t *testing.T) {
	a, b := makeXorb(t, 25, 2, 4<<10), makeXorb(t, 26, 2, 4<<10)
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("rangeRead=%v", rangeRead), func(t *testing.T) {
			st, _ := newTestStore(t, t.TempDir())
			s := newSeed(t, st, "s1")
			m := newTestStaging(t, st, stagingConfig{cacheSize: int64(len(a.body)+len(b.body)) - 1})
			ctx := context.Background()
			first, second := a, b
			if rangeRead {
				first, second = b, a
			}
			for _, x := range []*fixture{first, second} {
				if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
					t.Fatal(err)
				}
			}
			var rd io.Closer
			var err error
			if rangeRead {
				rd, err = st.GetXorbRangeReadCloser(ctx, first.hash, 0, 15)
			} else {
				rd, err = st.GetXorbReadSeekCloser(ctx, first.hash)
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = rd.Close()
			time.Sleep(30 * time.Millisecond)
			if !hasLocal(t, st, a.hash) || !hasLocal(t, st, b.hash) {
				t.Fatal("evicted without holders")
			}
			s.holds(a.hash, b.hash)
			m.kick()
			clustertest.WaitFor(t, waitTimeout, "eviction of the untouched copy", func() bool {
				return !hasLocal(t, st, second.hash) && m.snapshot() == int64(len(first.body))
			})
			if !hasLocal(t, st, first.hash) {
				t.Fatal("recently read copy evicted")
			}
			if got := m.snapshot(); got != int64(len(first.body)) {
				t.Fatalf("usage = %d, want %d", got, len(first.body))
			}
		})
	}
}

// A pinned victim stays until its last pin goes, which wakes maintenance without a tick.
func TestPinnedXorbEvictedAfterUnpin(t *testing.T) {
	x := makeXorb(t, 27, 2, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	s := newSeed(t, st, "s1")
	m := newTestStaging(t, st, stagingConfig{cacheSize: 1})
	st.pin(x.hash)
	if _, err := st.PutXorb(context.Background(), x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	s.holds(x.hash)
	m.kick()
	time.Sleep(30 * time.Millisecond)
	if !hasLocal(t, st, x.hash) {
		t.Fatal("pinned xorb evicted")
	}
	st.unpin(x.hash)
	clustertest.WaitFor(t, waitTimeout, "eviction after unpin", func() bool { return !hasLocal(t, st, x.hash) && m.snapshot() == 0 })
	if got := m.snapshot(); got != 0 {
		t.Fatalf("usage = %d, want 0", got)
	}
}

func TestRestartScansExistingObjects(t *testing.T) {
	a, b := makeXorb(t, 28, 2, 4<<10), makeXorb(t, 29, 2, 4<<10)
	dir := t.TempDir()
	st, _ := newTestStore(t, dir)
	m := newTestStaging(t, st, stagingConfig{})
	ctx := context.Background()
	for _, x := range []*fixture{a, b} {
		if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
			t.Fatal(err)
		}
	}
	_ = m.Close()
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(storage.ObjectKey("xorbs", a.hash.String()))), old, old); err != nil {
		t.Fatal(err)
	}
	st, _ = newTestStore(t, dir)
	s1 := newSeed(t, st, "s1")
	m = newTestStaging(t, st, stagingConfig{cacheSize: int64(len(a.body)+len(b.body)) - 1})
	if got := m.snapshot(); got != int64(len(a.body)+len(b.body)) {
		t.Fatalf("usage after restart = %d, want %d", got, len(a.body)+len(b.body))
	}
	clustertest.WaitFor(t, waitTimeout, "pulls of the scanned copies", func() bool { return len(s1.requests()) == 2 })
	s2 := newSeed(t, st, "s2")
	m.kick()
	clustertest.WaitFor(t, waitTimeout, "pulls at the late seed", func() bool { return len(s2.requests()) == 2 })
	s1.holds(a.hash, b.hash)
	m.kick()
	clustertest.WaitFor(t, waitTimeout, "eviction of the oldest copy", func() bool {
		return !hasLocal(t, st, a.hash) && m.snapshot() == int64(len(b.body))
	})
	if !hasLocal(t, st, b.hash) {
		t.Fatal("newer copy evicted before the older one")
	}
	if got := m.snapshot(); got != int64(len(b.body)) {
		t.Fatalf("usage = %d, want %d", got, len(b.body))
	}
}

func TestCloseCancelsHungPull(t *testing.T) {
	x, y := makeXorb(t, 30, 1, 4<<10), makeXorb(t, 31, 1, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	s := newSeed(t, st, "s1")
	s.hang()
	m := newTestStaging(t, st, stagingConfig{})
	ctx := context.Background()
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	clustertest.WaitFor(t, waitTimeout, "hung pull", func() bool { return len(s.requests()) == 1 })
	done := make(chan struct{})
	go func() {
		_ = m.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not return while a pull request hung")
	}
	clustertest.WaitFor(t, waitTimeout, "seed to see the cancellation", func() bool { return s.cancellations() == 1 })
	s2 := newSeed(t, st, "s2")
	m.kick()
	if _, err := st.PutXorb(ctx, y.hash, bytes.NewReader(y.body)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if n := len(s.requests()) + len(s2.requests()); n != 1 {
		t.Fatalf("%d pull requests after Close, want the 1 from before", n)
	}
	_ = m.Close()
}
