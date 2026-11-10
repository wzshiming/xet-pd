package tracker

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

var seedFile = apis.File{Hash: strings.Repeat("ab", 32), SHA256: strings.Repeat("cd", 32)}

// fakePeers serves one clustertest.FakePeer per id, each requiring secret.
func fakePeers(t *testing.T, secret string, ids ...string) map[string]*clustertest.FakePeer {
	out := map[string]*clustertest.FakePeer{}
	for _, id := range ids {
		out[id] = clustertest.NewFakePeer(t, id)
		out[id].Secret = secret
	}
	return out
}

// hold announces p in full holding xorbs and the shards of files, and notes the change as serveAnnounce does.
func hold(reg *registry, s *seeder, p apis.Peer, xorbs []string, files ...apis.File) {
	_, changed := reg.announce(apis.AnnounceRequest{Peer: p, Epoch: "e", Full: true, Add: xorbs, AddShards: files})
	s.note(changed)
}

func TestSeedAsksTopRankedNonHolderOnce(t *testing.T) {
	const secret = "s3cret"
	for _, tt := range []struct {
		name, key string
		xorbs     []string
		files     []apis.File
		want      apis.PullRequest
	}{
		{"xorb", testHash, []string{testHash}, nil, apis.PullRequest{Xorbs: []string{testHash}, Seed: true}},
		{"shard", seedFile.Hash, nil, []apis.File{seedFile}, apis.PullRequest{Shards: []string{seedFile.Hash}, Seed: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reg := newRegistry(time.Hour, 1)
			s := newTestSeeder(t, reg, seedConfig{replicas: 2, secret: secret})
			peers := fakePeers(t, secret, "p1", "p2", "p3", "p4")
			var all []apis.Peer
			for _, p := range peers {
				hold(reg, s, p.Peer(), nil)
				all = append(all, p.Peer())
			}
			ranking := apis.Rank(tt.key, all)
			top, holder := peers[ranking[0].ID], peers[ranking[len(ranking)-1].ID]
			hold(reg, s, holder.Peer(), tt.xorbs, tt.files...)
			clustertest.WaitFor(t, time.Second, "a pull at the top-ranked non-holder", func() bool { return len(top.Pulls()) > 0 })
			settle(t, s)
			settle(t, s)
			for id, p := range peers {
				want := 0
				if p == top {
					want = 1
				}
				if got := p.Pulls(); len(got) != want {
					t.Fatalf("%s saw %d pulls, want %d", id, len(got), want)
				}
			}
			got := top.Pulls()[0]
			if !reflect.DeepEqual(got.Req, tt.want) || got.Header.Get("Authorization") != "Bearer "+secret {
				t.Fatalf("pull = %+v with Authorization %q, want %+v with the bearer secret", got.Req, got.Header.Get("Authorization"), tt.want)
			}
		})
	}
}

// The objects a pass asks one peer for travel in one pull request, xorbs and shards together.
func TestSeedBatchesObjectsPerPeer(t *testing.T) {
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	peers := fakePeers(t, "", "holder", "p", "q")
	hold(reg, s, peers["p"].Peer(), nil)
	hold(reg, s, peers["q"].Peer(), nil)
	var keys []string // ranked p above q, in ascending order
	for i := 0; len(keys) < 3; i++ {
		if k := fmt.Sprintf("%064x", i); apis.Rank(k, []apis.Peer{peers["p"].Peer(), peers["q"].Peer()})[0].ID == "p" {
			keys = append(keys, k)
		}
	}
	hold(reg, s, peers["holder"].Peer(), keys[:2], apis.File{Hash: keys[2], SHA256: seedFile.SHA256})
	got := peers["p"].Pull(t, 0).Req
	slices.Sort(got.Xorbs)
	if want := (apis.PullRequest{Xorbs: keys[:2], Shards: keys[2:], Seed: true}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pull at p = %+v, want one request for all three objects %+v", got, want)
	}
	settle(t, s)
	if n, m := len(peers["p"].Pulls()), len(peers["q"].Pulls()); n != 1 || m != 0 {
		t.Fatalf("p saw %d pulls and q %d, want 1 and 0", n, m)
	}
}

// A peer due more than MaxPullBatch objects gets the rest in a later pass.
func TestSeedCapsPullBatch(t *testing.T) {
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	peers := fakePeers(t, "", "holder", "seed")
	hold(reg, s, peers["seed"].Peer(), nil)
	xorbs := make([]string, apis.MaxPullBatch)
	for i := range xorbs {
		xorbs[i] = fmt.Sprintf("%064x", i)
	}
	hold(reg, s, peers["holder"].Peer(), xorbs, seedFile)
	first, second := peers["seed"].Pull(t, 0).Req, peers["seed"].Pull(t, 1).Req
	if n := len(first.Xorbs) + len(first.Shards); n != apis.MaxPullBatch {
		t.Fatalf("first pull carries %d objects, want %d", n, apis.MaxPullBatch)
	}
	got := slices.Concat(first.Xorbs, first.Shards, second.Xorbs, second.Shards)
	slices.Sort(got)
	want := append(slices.Clone(xorbs), seedFile.Hash)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("the two pulls carry %d objects, want each of the %d once", len(got), len(want))
	}
	settle(t, s)
	if n := len(peers["seed"].Pulls()); n != 2 {
		t.Fatalf("seed saw %d pulls, want 2", n)
	}
}

func TestSeedSkipsReplicatedObject(t *testing.T) {
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	peers := fakePeers(t, "", "p1", "p2", "p3")
	reg.announce(fullReq(peers["p1"].Peer(), "e", testHash))
	reg.announce(fullReq(peers["p3"].Peer(), "e"))
	hold(reg, s, peers["p2"].Peer(), []string{testHash})
	settle(t, s)
	for id, p := range peers {
		if got := p.Pulls(); len(got) != 0 {
			t.Fatalf("%s saw pulls %+v of a xorb with as many holders as replicas, want none", id, got)
		}
	}
	_, changed := reg.announce(deltaReq(peers["p2"].Peer(), "e", nil, []string{testHash}))
	s.note(changed)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a pull once a holder dropped the xorb", func() bool {
		return len(peers["p2"].Pulls())+len(peers["p3"].Pulls()) == 1
	})
}

// A 202 only means the peer started pulling: it is asked again after the cooldown until it announces the xorb.
func TestAcceptedSeedIsNotAHolding(t *testing.T) {
	const retry = 50 * time.Millisecond
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2, retry: retry, tick: 5 * time.Millisecond})
	peers := fakePeers(t, "", "holder", "seed")
	hold(reg, s, peers["seed"].Peer(), nil)
	hold(reg, s, peers["holder"].Peer(), []string{testHash})
	peers["seed"].Pull(t, 1)
	hold(reg, s, peers["seed"].Peer(), []string{testHash})
	settle(t, s)
	n := len(peers["seed"].Pulls())
	time.Sleep(4 * retry)
	settle(t, s)
	if got := len(peers["seed"].Pulls()); got != n {
		t.Fatalf("pulls after the seed announced the xorb = %d, want %d", got, n)
	}
}

func TestFailedSeedRetriesAfterCooldown(t *testing.T) {
	const retry = 150 * time.Millisecond
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2, retry: retry, tick: 5 * time.Millisecond})
	peers := fakePeers(t, "", "holder", "seed")
	peers["seed"].SetPullStatus(http.StatusInternalServerError)
	hold(reg, s, peers["seed"].Peer(), nil)
	start := time.Now()
	hold(reg, s, peers["holder"].Peer(), []string{testHash})
	peers["seed"].Pull(t, 0)
	peers["seed"].Pull(t, 1)
	// The retry can only follow the first pull by the cooldown, and the first pull follows start.
	if elapsed := time.Since(start); elapsed < retry {
		t.Fatalf("retry %v after the first ask could start, want no earlier than the %v cooldown", elapsed, retry)
	}
}

func TestExpiredHolderRetriggersSeeding(t *testing.T) {
	reg := newRegistry(time.Minute, 1)
	var clock atomic.Int64
	clock.Store(time.Unix(1000, 0).UnixNano())
	reg.now = func() time.Time { return time.Unix(0, clock.Load()) }
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	peers := fakePeers(t, "", "gone", "stays", "seed")
	hold(reg, s, peers["gone"].Peer(), []string{testHash})
	clock.Add(int64(30 * time.Second))
	hold(reg, s, peers["stays"].Peer(), []string{testHash})
	hold(reg, s, peers["seed"].Peer(), nil)
	settle(t, s)
	if got := peers["seed"].Pulls(); len(got) != 0 {
		t.Fatalf("pulls while the xorb is replicated = %+v, want none", got)
	}
	clock.Add(int64(30 * time.Second))
	hold(reg, s, peers["seed"].Peer(), nil)
	if got := peers["seed"].Pull(t, 0); !slices.Equal(got.Req.Xorbs, []string{testHash}) {
		t.Fatalf("pull after a holder expired = %+v, want the xorb", got.Req)
	}
}

func TestSeedDoesNotFollowRedirects(t *testing.T) {
	var elsewhere, asked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { elsewhere.Add(1) }))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		http.Redirect(w, r, target.URL+apis.PullPath, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	hold(reg, s, apis.Peer{ID: "redirector", URL: redirector.URL}, nil)
	hold(reg, s, fakePeers(t, "", "holder")["holder"].Peer(), []string{testHash})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the pull at the redirecting peer", func() bool { return asked.Load() == 1 })
	settle(t, s)
	if n := elsewhere.Load(); n != 0 {
		t.Fatalf("redirect target saw %d requests, want none", n)
	}
}

func TestCloseCancelsHungSeed(t *testing.T) {
	var asked, canceled atomic.Int32
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // net/http watches for a hang-up only once the body is read
		asked.Add(1)
		<-r.Context().Done()
		canceled.Add(1)
	}))
	t.Cleanup(hung.Close)
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	hold(reg, s, apis.Peer{ID: "hung", URL: hung.URL}, nil)
	holder := fakePeers(t, "", "holder")["holder"].Peer()
	hold(reg, s, holder, []string{testHash})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the hung pull", func() bool { return asked.Load() == 1 })
	done := make(chan struct{})
	go func() {
		_ = s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not return while a pull hung")
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the peer to see the cancellation", func() bool { return canceled.Load() == 1 })
	// Close reaped the pass loop and every pull, so nothing is left to ask again.
	hold(reg, s, holder, []string{testHash})
	if n := asked.Load(); n != 1 {
		t.Fatalf("%d pulls after Close, want the 1 from before", n)
	}
}

// Requests a hung peer never answers keep their pull slots, so no pass sends more than maxPulls at once.
func TestSeedRespectsPullCap(t *testing.T) {
	var asked atomic.Int32
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // net/http watches for a hang-up only once the body is read
		asked.Add(1)
		<-r.Context().Done()
	}))
	t.Cleanup(hung.Close)
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	var hungPeers []apis.Peer
	for i := range maxPulls + 2 {
		p := apis.Peer{ID: fmt.Sprintf("hung%d", i), URL: hung.URL}
		reg.announce(fullReq(p, "e"))
		hungPeers = append(hungPeers, p)
	}
	var xorbs []string // one ranking each hung peer first
	for _, p := range hungPeers {
		for i := 0; ; i++ {
			if h := fmt.Sprintf("%s-%d", p.ID, i); apis.Rank(h, hungPeers)[0] == p {
				xorbs = append(xorbs, h)
				break
			}
		}
	}
	hold(reg, s, fakePeers(t, "", "holder")["holder"].Peer(), xorbs)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a request in every pull slot", func() bool { return asked.Load() == maxPulls })
	s.seed()
	s.seed()
	if n := asked.Load(); n != maxPulls || len(s.sem) != maxPulls {
		t.Fatalf("hung peer saw %d requests with %d slots taken, want %d of each", n, len(s.sem), maxPulls)
	}
}
