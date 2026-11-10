package tracker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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

// hold announces p in full holding files complete and notes the change, as serveAnnounce does.
func hold(reg *registry, s *seeder, p apis.Peer, files ...apis.File) {
	_, changed := reg.announce(apis.AnnounceRequest{Peer: p, Epoch: "e", Full: true, AddFiles: files})
	s.note(changed)
}

func TestSeedAsksTopRankedNonHolderOnce(t *testing.T) {
	const secret = "s3cret"
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2, secret: secret})
	peers := fakePeers(t, secret, "p1", "p2", "p3", "p4")
	for _, p := range peers {
		hold(reg, s, p.Peer())
	}
	ranking := reg.rank(seedFile.Hash)
	top, holder := peers[ranking[0].ID], peers[ranking[len(ranking)-1].ID]
	hold(reg, s, holder.Peer(), seedFile)
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
	if !reflect.DeepEqual(got.Req, apis.PullRequest{File: seedFile.Hash, Seed: true}) || got.Header.Get("Authorization") != "Bearer "+secret {
		t.Fatalf("pull = %+v with Authorization %q, want a seed pull of the file with the bearer secret", got.Req, got.Header.Get("Authorization"))
	}
}

func TestSeedSkipsReplicatedFile(t *testing.T) {
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2})
	peers := fakePeers(t, "", "p1", "p2", "p3")
	for id, p := range peers {
		req := apis.AnnounceRequest{Peer: p.Peer(), Epoch: "e", Full: true}
		if id != "p3" {
			req.AddFiles = []apis.File{seedFile}
		}
		reg.announce(req)
	}
	s.note([]string{seedFile.Hash})
	settle(t, s)
	if got := peers["p3"].Pulls(); len(got) != 0 {
		t.Fatalf("pulls of a file with as many complete holders as replicas = %+v, want none", got)
	}
	_, changed := reg.announce(apis.AnnounceRequest{Peer: peers["p2"].Peer(), Epoch: "e", RemoveFiles: []string{seedFile.Hash}})
	s.note(changed)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a pull once a holder dropped the file", func() bool {
		return len(peers["p2"].Pulls())+len(peers["p3"].Pulls()) == 1
	})
}

// A 202 only means the peer started pulling: it is asked again after the cooldown until it announces the file.
func TestAcceptedSeedIsNotAHolding(t *testing.T) {
	const retry = 50 * time.Millisecond
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2, retry: retry, tick: 5 * time.Millisecond})
	peers := fakePeers(t, "", "holder", "seed")
	hold(reg, s, peers["seed"].Peer())
	hold(reg, s, peers["holder"].Peer(), seedFile)
	peers["seed"].Pull(t, 1)
	hold(reg, s, peers["seed"].Peer(), seedFile)
	settle(t, s)
	n := len(peers["seed"].Pulls())
	time.Sleep(4 * retry)
	settle(t, s)
	if got := len(peers["seed"].Pulls()); got != n {
		t.Fatalf("pulls after the seed announced the file = %d, want %d", got, n)
	}
}

func TestFailedSeedRetriesAfterCooldown(t *testing.T) {
	const retry = 150 * time.Millisecond
	reg := newRegistry(time.Hour, 1)
	s := newTestSeeder(t, reg, seedConfig{replicas: 2, retry: retry, tick: 5 * time.Millisecond})
	peers := fakePeers(t, "", "holder", "seed")
	peers["seed"].SetPullStatus(http.StatusInternalServerError)
	hold(reg, s, peers["seed"].Peer())
	start := time.Now()
	hold(reg, s, peers["holder"].Peer(), seedFile)
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
	hold(reg, s, peers["gone"].Peer(), seedFile)
	clock.Add(int64(30 * time.Second))
	hold(reg, s, peers["stays"].Peer(), seedFile)
	hold(reg, s, peers["seed"].Peer())
	settle(t, s)
	if got := peers["seed"].Pulls(); len(got) != 0 {
		t.Fatalf("pulls while the file is replicated = %+v, want none", got)
	}
	clock.Add(int64(30 * time.Second))
	hold(reg, s, peers["seed"].Peer())
	if got := peers["seed"].Pull(t, 0); got.Req.File != seedFile.Hash {
		t.Fatalf("pull after a holder expired = %+v, want the file", got.Req)
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
	hold(reg, s, apis.Peer{ID: "redirector", URL: redirector.URL})
	hold(reg, s, fakePeers(t, "", "holder")["holder"].Peer(), seedFile)
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
	hold(reg, s, apis.Peer{ID: "hung", URL: hung.URL})
	hold(reg, s, fakePeers(t, "", "holder")["holder"].Peer(), seedFile)
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
	s.note([]string{seedFile.Hash})
	if n := asked.Load(); n != 1 {
		t.Fatalf("%d pulls after Close, want the 1 from before", n)
	}
}
