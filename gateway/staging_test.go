package gateway

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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
	staged            bool      // the gateway held the complete bytes and the offsets sidecar when it asked
	at                time.Time // arrival
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
	at := time.Now()
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
	s.pulls = append(s.pulls, pull{r.Header.Get("Authorization"), r.Header.Get("Content-Type"), string(body), ok && serr == nil, at})
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

// newTestStaging takes New's replicas and minSeeds defaults and runs maintenance on wakes only unless the config asks for a period.
func newTestStaging(t *testing.T, st *store, cfg stagingConfig) *staging {
	t.Helper()
	cfg.tick = cmp.Or(cfg.tick, time.Hour)
	cfg.replicas = cmp.Or(cfg.replicas, 2)
	cfg.minSeeds = cmp.Or(cfg.minSeeds, 1)
	m, err := newStaging(st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// awaitPass waits for a maintenance pass whose check runs after the call to find the cache over its cap with nothing evictable.
func awaitPass(t *testing.T, m *staging) {
	t.Helper()
	// A pass may take its view before the call yet warn after the reset; one starting after that warning cannot.
	for range 2 {
		m.mu.Lock()
		m.warned = false
		m.mu.Unlock()
		m.kick()
		clustertest.WaitFor(t, clustertest.WaitTimeout, "a maintenance pass to find nothing evictable", func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()
			return m.warned
		})
	}
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
		clustertest.WaitFor(t, clustertest.WaitTimeout, "pull at "+p.ID, func() bool { return len(seeds[p.ID].requests()) == 1 })
	}
	m.mu.Lock()
	before := maps.Clone(m.xorbs.Get(x.hash).V.asked)
	m.mu.Unlock()
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil || inserted {
		t.Fatalf("dedup PutXorb = %v, %v", inserted, err)
	}
	// asked alone gates another pull, and a pass may already have refilled a reset one, so the dedup must leave it unchanged.
	m.mu.Lock()
	asked := maps.Clone(m.xorbs.Get(x.hash).V.asked)
	m.mu.Unlock()
	if !reflect.DeepEqual(asked, before) {
		t.Fatalf("asked after the dedup PutXorb = %v, want it unchanged from %v", asked, before)
	}
	if len(asked) != len(selected) {
		t.Fatalf("asked after the dedup PutXorb = %v, want only the selected seeds", asked)
	}
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
		clustertest.WaitFor(t, clustertest.WaitTimeout, "second pull at "+p.ID, func() bool { return len(seeds[p.ID].requests()) == 2 })
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
				clustertest.WaitFor(t, clustertest.WaitTimeout, "pull at "+s.peer.ID, func() bool {
					if !clustertest.HasXorb(t, st.Storage, x.hash) {
						t.Fatal("xorb evicted on 202 alone")
					}
					return len(s.requests()) == 1
				})
			}
			awaitPass(t, m)
			if !clustertest.HasXorb(t, st.Storage, x.hash) {
				t.Fatal("xorb evicted on 202 alone")
			}
			s1.holds(x.hash)
			m.kick()
			if minSeeds == 2 {
				awaitPass(t, m)
				if !clustertest.HasXorb(t, st.Storage, x.hash) {
					t.Fatal("xorb evicted with one holder, want two")
				}
				s2.holds(x.hash)
				m.kick()
			}
			clustertest.WaitFor(t, clustertest.WaitTimeout, "eviction", func() bool { return !clustertest.HasXorb(t, st.Storage, x.hash) && m.snapshot() == 0 })
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
	awaitPass(t, m)
	if !clustertest.HasXorb(t, st.Storage, x.hash) {
		t.Fatal("xorb evicted with no seed at all")
	}
	s := newSeed(t, st, "s1")
	m.kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull at the newly registered seed", func() bool { return len(s.requests()) == 1 })
	s.holds(x.hash)
	m.kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "eviction", func() bool { return !clustertest.HasXorb(t, st.Storage, x.hash) })
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
	const retry = 150 * time.Millisecond
	before := time.Now()
	newTestStaging(t, st, stagingConfig{secret: "tok", retry: retry, tick: 5 * time.Millisecond})
	if _, err := st.PutXorb(context.Background(), x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "first pull", func() bool { return len(s.requests()) == 1 })
	clustertest.WaitFor(t, clustertest.WaitTimeout, "retry after backoff", func() bool { return len(s.requests()) == 2 })
	// A re-ask needs now >= first ask + retry, and every ask comes from maintenance started after before.
	if at := s.requests()[1].at; at.Before(before.Add(retry)) {
		t.Fatalf("retry arrived %v after staging started, want no earlier than the %v backoff", at.Sub(before), retry)
	}
	s.answer(0, other.URL+apis.PullPath)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "retries past a redirect", func() bool { return len(s.requests()) >= 4 })
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
			awaitPass(t, m)
			if !clustertest.HasXorb(t, st.Storage, a.hash) || !clustertest.HasXorb(t, st.Storage, b.hash) {
				t.Fatal("evicted without holders")
			}
			s.holds(a.hash, b.hash)
			m.kick()
			clustertest.WaitFor(t, clustertest.WaitTimeout, "eviction of the untouched copy", func() bool {
				return !clustertest.HasXorb(t, st.Storage, second.hash) && m.snapshot() == int64(len(first.body))
			})
			if !clustertest.HasXorb(t, st.Storage, first.hash) {
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
	awaitPass(t, m)
	if !clustertest.HasXorb(t, st.Storage, x.hash) {
		t.Fatal("pinned xorb evicted")
	}
	// A pass already under way at awaitPass's kick leaves that kick in wake, where it would stand in for unpin's.
	select {
	case <-m.wake:
	default:
	}
	st.unpin(x.hash)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "eviction after unpin", func() bool { return !clustertest.HasXorb(t, st.Storage, x.hash) && m.snapshot() == 0 })
	if got := m.snapshot(); got != 0 {
		t.Fatalf("usage = %d, want 0", got)
	}
}

// A victim whose delete is refused is skipped for the pass: maintenance goes on to seed and Close returns.
func TestRefusedVictimIsSkipped(t *testing.T) {
	x, y := makeXorb(t, 60, 2, 4<<10), makeXorb(t, 61, 2, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	s := newSeed(t, st, "s1")
	m := newTestStaging(t, st, stagingConfig{cacheSize: 1})
	st.pin(x.hash)
	t.Cleanup(func() { st.unpin(x.hash) }) // registered after m's Close cleanup, so it runs first and a spinning eviction can end
	ctx := context.Background()
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	s.holds(x.hash)
	if _, err := st.PutXorb(ctx, y.hash, bytes.NewReader(y.body)); err != nil {
		t.Fatal(err)
	}
	m.kick()
	want, _ := json.Marshal(apis.PullRequest{Hash: y.hash.String()})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull of y past the refused victim", func() bool {
		return slices.ContainsFunc(s.requests(), func(r pull) bool { return r.body == string(want) })
	})
	if !clustertest.HasXorb(t, st.Storage, x.hash) {
		t.Fatal("pinned victim deleted")
	}
	done := make(chan struct{})
	go func() { _ = m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(clustertest.WaitTimeout):
		t.Fatal("Close did not return while the refused victim stayed over the cap")
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
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pulls of the scanned copies", func() bool { return len(s1.requests()) == 2 })
	s2 := newSeed(t, st, "s2")
	m.kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pulls at the late seed", func() bool { return len(s2.requests()) == 2 })
	s1.holds(a.hash, b.hash)
	m.kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "eviction of the oldest copy", func() bool {
		return !clustertest.HasXorb(t, st.Storage, a.hash) && m.snapshot() == int64(len(b.body))
	})
	if !clustertest.HasXorb(t, st.Storage, b.hash) {
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
	clustertest.WaitFor(t, clustertest.WaitTimeout, "hung pull", func() bool { return len(s.requests()) == 1 })
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
	clustertest.WaitFor(t, clustertest.WaitTimeout, "seed to see the cancellation", func() bool { return s.cancellations() == 1 })
	s2 := newSeed(t, st, "s2")
	m.kick()
	if _, err := st.PutXorb(ctx, y.hash, bytes.NewReader(y.body)); err != nil {
		t.Fatal(err)
	}
	// Close reaped maintenance and every pull goroutine, so nothing is left to send another pull.
	if n := len(s.requests()) + len(s2.requests()); n != 1 {
		t.Fatalf("%d pull requests after Close, want the 1 from before", n)
	}
	_ = m.Close()
}

// The staging index stays free to uploads and reads while a large Full announce holds the registry write lock.
func TestRegistryStallLeavesIndexFree(t *testing.T) {
	// A zero cache size skips eviction, so the pass first reaches the registry while seeding.
	for _, tc := range []struct {
		phase     string
		cacheSize int64
	}{{"seeding", 0}, {"eviction", 1}} {
		t.Run(tc.phase, func(t *testing.T) {
			x := makeXorb(t, 62, 2, 4<<10)
			st, _ := newTestStore(t, t.TempDir())
			newSeed(t, st, "s1")
			if _, err := st.PutXorb(context.Background(), x.hash, bytes.NewReader(x.body)); err != nil {
				t.Fatal(err)
			}
			var held atomic.Bool
			var first sync.Once
			resume := make(chan struct{})
			st.reg.now = func() time.Time { // set before the maintenance goroutine exists
				first.Do(func() { held.Store(true); <-resume }) // the first pass waits here for its first view
				return time.Now()
			}
			release := sync.OnceFunc(func() { close(resume) })
			m := newTestStaging(t, st, stagingConfig{cacheSize: tc.cacheSize})
			t.Cleanup(release) // runs before m's Close, so a held pass can end
			free := func(state string) {
				t.Helper()
				for i := range 10 {
					usage := make(chan int64, 1)
					go func() { usage <- m.snapshot() }()
					select {
					case <-usage:
					case <-time.After(time.Second):
						t.Fatalf("snapshot %d blocked over 1s while %s: the %s pass holds the staging index across a registry call", i, state, tc.phase)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			clustertest.WaitFor(t, clustertest.WaitTimeout, "the first pass to ask for its view", held.Load)
			m.reg.mu.Lock()
			t.Cleanup(m.reg.mu.Unlock) // registered after m's Close cleanup, so it runs first and the stalled pass can end
			release()
			time.Sleep(20 * time.Millisecond) // the released pass reaches live()'s read lock within nanoseconds
			free("the pass waits for its view")
			m.reg.mu.Unlock()
			m.reg.mu.Lock() // live() completes under the handoff; the pass now waits in rank or holderCount
			free("the pass ranks or counts holders")
		})
	}
}
