package tracker

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

func testPeer(i int) apis.Peer {
	return apis.Peer{ID: fmt.Sprintf("p%d", i), URL: fmt.Sprintf("http://p%d", i)}
}

func testFile(i int) apis.File {
	return apis.File{Hash: fmt.Sprintf("f%d", i), SHA256: fmt.Sprintf("s%d", i)}
}

func fullReq(p apis.Peer, epoch string, hashes ...string) apis.AnnounceRequest {
	return apis.AnnounceRequest{Peer: p, Epoch: epoch, Full: true, Add: hashes}
}

func deltaReq(p apis.Peer, epoch string, add, remove []string) apis.AnnounceRequest {
	return apis.AnnounceRequest{Peer: p, Epoch: epoch, Add: add, Remove: remove}
}

// shardsReq is a Full announce of p holding the shards of files and no xorbs.
func shardsReq(p apis.Peer, epoch string, files ...apis.File) apis.AnnounceRequest {
	return apis.AnnounceRequest{Peer: p, Epoch: epoch, Full: true, AddShards: files}
}

// xorbHolders lists the live holders of xorb hash as a lookup by no peer answers it.
func xorbHolders(r *registry, hash string) []apis.Peer {
	return r.lookup(apis.LookupRequest{Xorbs: []string{hash}}).Xorbs[hash]
}

// shardHolders lists the live holders of file's shard as a lookup by no peer answers it.
func shardHolders(r *registry, file string) []apis.Peer {
	return r.lookup(apis.LookupRequest{Files: []string{file}}).Files[file].Peers
}

func TestLookupEmptyRegistry(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	if got := r.lookup(apis.LookupRequest{Xorbs: []string{"h1"}, Files: []string{"f1"}, SHA256s: []string{"s1"}}); len(got.Xorbs)+len(got.Files)+len(got.SHA256s) != 0 {
		t.Fatalf("lookup on empty registry = %+v, want no key", got)
	}
	if got := r.holders(object{hash: "h1"}); len(got) != 0 {
		t.Fatalf("holders on empty registry = %v", got)
	}
	if live, up := r.live(), r.upstreams(); len(live)+len(up) != 0 {
		t.Fatalf("live %v and upstreams %v on empty registry, want none", live, up)
	}
}

func TestConcurrentAnnounceAndLookup(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			p := testPeer(i)
			p.Upstream = i%2 == 0
			r.announce(fullReq(p, "e"))
			for j := range 200 {
				h, f := fmt.Sprintf("h%d", j%10), testFile(j%10)
				r.announce(apis.AnnounceRequest{Peer: p, Epoch: "e",
					Add: []string{h}, Remove: []string{fmt.Sprintf("h%d", (j+5)%10)},
					AddShards: []apis.File{f}, RemoveShards: []string{testFile((j + 5) % 10).Hash}})
				r.lookup(apis.LookupRequest{Peer: p.ID, Xorbs: []string{h}, Files: []string{f.Hash}, SHA256s: []string{f.SHA256}})
				r.holders(object{hash: h})
				r.holders(object{shard: true, hash: f.Hash})
				r.upstreams()
				r.live()
				r.snapshot()
				r.stats()
			}
		})
	}
	wg.Wait()
	if live, up := len(r.live()), len(r.upstreams()); live != 8 || up != 4 {
		t.Fatalf("live peers = %d with %d upstreams, want 8 with 4", live, up)
	}
}

func TestTTLExpiryIsStrict(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	if resp, _ := r.announce(fullReq(testPeer(1), "e", "h1")); resp.TTL != 60 {
		t.Fatalf("TTL = %d, want 60", resp.TTL)
	}
	*now = base.Add(time.Minute - time.Nanosecond)
	if got := xorbHolders(r, "h1"); !slices.Equal(got, []apis.Peer{testPeer(1)}) {
		t.Fatalf("holders just before expiry = %v, want the live holder", got)
	}
	*now = base.Add(time.Minute)
	if len(xorbHolders(r, "h1")) != 0 || len(r.live()) != 0 {
		t.Fatal("expired peer must be excluded from holders and live peers")
	}
}

func TestEmptyDeltaRenewsHeartbeat(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	r.announce(fullReq(testPeer(1), "e", "h1"))
	*now = base.Add(50 * time.Second)
	if resp, _ := r.announce(deltaReq(testPeer(1), "e", nil, nil)); resp.Resync {
		t.Fatal("heartbeat from known peer must not request resync")
	}
	*now = base.Add(time.Minute + 49*time.Second)
	if n := len(xorbHolders(r, "h1")); n != 1 {
		t.Fatalf("holders after renewal = %d, want 1 (holdings kept)", n)
	}
	*now = base.Add(time.Minute + 50*time.Second)
	if n := len(xorbHolders(r, "h1")); n != 0 {
		t.Fatalf("holders at renewed expiry = %d, want 0", n)
	}
}

func TestStalePeersArePruned(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	r.announce(fullReq(testPeer(1), "e", "h1"))
	*now = now.Add(time.Minute)
	r.announce(fullReq(testPeer(2), "e"))
	if n := registeredPeers(r); n != 1 {
		t.Fatalf("peers retained = %d, want stale peer pruned", n)
	}
}

func holderCounts(r *registry, hashes ...string) []int {
	var out []int
	for _, h := range hashes {
		out = append(out, len(xorbHolders(r, h)))
	}
	return out
}

func shardHolderCounts(r *registry, files ...apis.File) []int {
	var out []int
	for _, f := range files {
		out = append(out, len(shardHolders(r, f.Hash)))
	}
	return out
}

func TestFullReplacesInventory(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	r.announce(fullReq(testPeer(1), "e", "h1", "h2"))
	r.announce(fullReq(testPeer(1), "e", "h3"))
	if got := holderCounts(r, "h1", "h2", "h3"); !slices.Equal(got, []int{0, 0, 1}) {
		t.Fatalf("holders after full = %v, want [0 0 1]", got)
	}
}

func TestFullReplacesShards(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f1, f2, f3 := testFile(1), testFile(2), testFile(3)
	r.announce(shardsReq(testPeer(1), "e", f1, f2))
	if got := shardHolderCounts(r, f1, f2, f3); !slices.Equal(got, []int{1, 1, 0}) {
		t.Fatalf("shard holders after the first full = %v, want [1 1 0]", got)
	}
	r.announce(shardsReq(testPeer(1), "e", f3))
	if got := shardHolderCounts(r, f1, f2, f3); !slices.Equal(got, []int{0, 0, 1}) {
		t.Fatalf("shard holders after the second full = %v, want [0 0 1]", got)
	}
}

func TestDeltaAddRemoveIdempotentRemoveWins(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	r.announce(fullReq(testPeer(1), "e"))
	delta := func(add, remove []string) {
		if resp, _ := r.announce(deltaReq(testPeer(1), "e", add, remove)); resp.Resync {
			t.Fatalf("delta %v/%v requested resync", add, remove)
		}
	}
	delta([]string{"h1", "h1"}, nil)
	delta([]string{"h1"}, nil)
	if got := holderCounts(r, "h1"); !slices.Equal(got, []int{1}) {
		t.Fatalf("holders after repeated add = %v, want [1]", got)
	}
	delta(nil, []string{"h1"})
	delta(nil, []string{"h1", "unknown"})
	if got := holderCounts(r, "h1"); !slices.Equal(got, []int{0}) {
		t.Fatalf("holders after repeated remove = %v, want [0]", got)
	}
	delta([]string{"h2"}, []string{"h2"})
	if got := holderCounts(r, "h2"); !slices.Equal(got, []int{0}) {
		t.Fatalf("same hash in add and remove = %v, want remove to win", got)
	}
	delta([]string{"h2"}, nil)
	if got := holderCounts(r, "h2"); !slices.Equal(got, []int{1}) {
		t.Fatalf("holders after re-add = %v, want [1]", got)
	}
}

func TestShardDeltaAddRemoveIdempotent(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f1, f2 := testFile(1), testFile(2)
	r.announce(shardsReq(testPeer(1), "e", f1))
	delta := func(add []apis.File, remove []string) {
		t.Helper()
		if resp, _ := r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", AddShards: add, RemoveShards: remove}); resp.Resync {
			t.Fatalf("shard delta %v/%v requested resync", add, remove)
		}
	}
	delta([]apis.File{f2, f2}, nil)
	delta([]apis.File{f2}, nil)
	if got := shardHolderCounts(r, f1, f2); !slices.Equal(got, []int{1, 1}) {
		t.Fatalf("shard holders after repeated adds = %v, want [1 1]", got)
	}
	delta(nil, []string{f1.Hash})
	delta(nil, []string{f1.Hash, "unknown"})
	if got := shardHolderCounts(r, f1, f2); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("shard holders after repeated removes = %v, want [0 1]", got)
	}
	if got := holderCounts(r, f2.Hash); !slices.Equal(got, []int{0}) {
		t.Fatalf("xorb holders of a held shard's hash = %v, want shards kept apart from xorbs", got)
	}
}

func TestRemoveShardsWinsOverAddShards(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	r.announce(shardsReq(testPeer(1), "e"))
	r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", AddShards: []apis.File{f}, RemoveShards: []string{f.Hash}})
	if got := shardHolders(r, f.Hash); len(got) != 0 {
		t.Fatalf("shard holders of a shard added and removed in one request = %v, want none", got)
	}
	if got := r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}); len(got.SHA256s) != 0 {
		t.Fatalf("lookup by the SHA-256 of a removed shard = %+v, want it omitted", got)
	}
	r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", AddShards: []apis.File{f}})
	if got := shardHolders(r, f.Hash); !slices.Equal(got, []apis.Peer{testPeer(1)}) {
		t.Fatalf("shard holders after AddShards alone = %v, want the shard held", got)
	}
}

// The latest announce of a SHA-256 decides the file it names.
func TestSHA256MapsToFile(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	r.announce(shardsReq(testPeer(1), "e", f))
	bySHA := func() apis.FileLocation {
		return r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}).SHA256s[f.SHA256]
	}
	if got, want := bySHA(), (apis.FileLocation{File: f, Peers: []apis.Peer{testPeer(1)}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("lookup by SHA-256 = %+v, want %+v", got, want)
	}
	same := apis.File{Hash: "f9", SHA256: f.SHA256}
	r.announce(shardsReq(testPeer(2), "e", same))
	if got, want := bySHA(), (apis.FileLocation{File: same, Peers: []apis.Peer{testPeer(2)}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("lookup by SHA-256 after another file claimed it = %+v, want %+v", got, want)
	}
}

// A SHA-256 names a file until no live peer holds the file's shard, so the file announced again under another one is not found by it.
func TestSHA256OfUnheldShardIsForgotten(t *testing.T) {
	f := testFile(1)
	for name, unhold := range map[string]func(r *registry, now *time.Time){
		"RemoveShards": func(r *registry, _ *time.Time) {
			r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", RemoveShards: []string{f.Hash}})
		},
		"expiry": func(r *registry, now *time.Time) {
			*now = now.Add(time.Minute)
			r.announce(shardsReq(testPeer(2), "e"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, now := clockedRegistry(time.Minute)
			r.announce(shardsReq(testPeer(1), "e", f))
			if got := r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}).SHA256s[f.SHA256].File; got != f {
				t.Fatalf("file named by the SHA-256 of a held shard = %+v, want %+v", got, f)
			}
			unhold(r, now)
			r.announce(shardsReq(testPeer(2), "e", apis.File{Hash: f.Hash, SHA256: "s9"}))
			if got := r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}); len(got.SHA256s) != 0 {
				t.Fatalf("lookup by the SHA-256 of a shard no live peer held since = %+v, want it omitted", got)
			}
		})
	}
}

func TestExpiryRemovesShards(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	f := testFile(1)
	r.announce(shardsReq(testPeer(1), "e", f))
	*now = now.Add(time.Minute - time.Nanosecond)
	if got := shardHolders(r, f.Hash); !slices.Equal(got, []apis.Peer{testPeer(1)}) {
		t.Fatalf("shard holders just before expiry = %v, want the live holder", got)
	}
	*now = now.Add(time.Nanosecond)
	if got := shardHolders(r, f.Hash); len(got) != 0 {
		t.Fatalf("shard holders at expiry = %v, want none", got)
	}
	r.announce(shardsReq(testPeer(2), "e"))
	if got := r.lookup(apis.LookupRequest{Files: []string{f.Hash}, SHA256s: []string{f.SHA256}}); len(got.Files)+len(got.SHA256s) != 0 {
		t.Fatalf("lookup after the holder expired = %+v, want both keys omitted", got)
	}
}

func TestAnnounceListsChangedObjects(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	p := testPeer(1)
	f1, f2, f3, f4 := testFile(1), testFile(2), testFile(3), testFile(4)
	xorb := func(h string) object { return object{hash: h} }
	shard := func(f apis.File) object { return object{shard: true, hash: f.Hash} }
	check := func(what string, req apis.AnnounceRequest, want ...object) {
		t.Helper()
		_, list := r.announce(req)
		got, wantSet := map[object]bool{}, map[object]bool{}
		for _, o := range list {
			got[o] = true
		}
		for _, o := range want {
			wantSet[o] = true
		}
		if len(got) != len(list) || !maps.Equal(got, wantSet) {
			t.Fatalf("changed by %s = %v, want %v", what, list, want)
		}
	}
	check("a first full", apis.AnnounceRequest{Peer: p, Epoch: "e", Full: true, Add: []string{"h1", "h2"}, AddShards: []apis.File{f1, f2}},
		xorb("h1"), xorb("h2"), shard(f1), shard(f2))
	check("adds", apis.AnnounceRequest{Peer: p, Epoch: "e", Add: []string{"h1", "h3"}, AddShards: []apis.File{f1, f3}},
		xorb("h3"), shard(f3))
	check("removes", apis.AnnounceRequest{Peer: p, Epoch: "e", Remove: []string{"h2", "unknown"}, RemoveShards: []string{f2.Hash, "unknown"}},
		xorb("h2"), shard(f2))
	check("adds the same request removes", apis.AnnounceRequest{Peer: p, Epoch: "e", Add: []string{"h4"}, Remove: []string{"h4"}, AddShards: []apis.File{f4}, RemoveShards: []string{f4.Hash}})
	check("a full replacing the peer", apis.AnnounceRequest{Peer: p, Epoch: "e2", Full: true, Add: []string{"h3", "h5"}, AddShards: []apis.File{f3}},
		xorb("h1"), xorb("h3"), xorb("h5"), shard(f1), shard(f3))
	*now = now.Add(time.Minute)
	check("an announce sweeping the expired peer", fullReq(testPeer(2), "e"),
		xorb("h3"), xorb("h5"), shard(f3))
	check("a full after expiry", apis.AnnounceRequest{Peer: p, Epoch: "e3", Full: true, Add: []string{"h1"}, AddShards: []apis.File{f1}},
		xorb("h1"), shard(f1))
	check("a resync", apis.AnnounceRequest{Peer: p, Epoch: "e4"}, xorb("h1"), shard(f1))
}

func TestLookupExcludesAsker(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	for i := 1; i <= 2; i++ {
		r.announce(apis.AnnounceRequest{Peer: testPeer(i), Epoch: "e", Full: true, Add: []string{"h1"}, AddShards: []apis.File{f}})
	}
	for asker, other := range map[string]apis.Peer{"p1": testPeer(2), "p2": testPeer(1)} {
		got := r.lookup(apis.LookupRequest{Peer: asker, Xorbs: []string{"h1"}, Files: []string{f.Hash}, SHA256s: []string{f.SHA256}})
		loc := apis.FileLocation{File: f, Peers: []apis.Peer{other}}
		want := apis.LookupResponse{
			Xorbs:   map[string][]apis.Peer{"h1": {other}},
			Files:   map[string]apis.FileLocation{f.Hash: loc},
			SHA256s: map[string]apis.FileLocation{f.SHA256: loc},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("lookup by %s = %+v, want %+v", asker, got, want)
		}
	}
}

func TestLookupOrderEqualsRankOrder(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	var peers []apis.Peer
	for i := range 5 {
		peers = append(peers, testPeer(i))
		r.announce(apis.AnnounceRequest{Peer: testPeer(i), Epoch: "e", Full: true, Add: []string{"h1"}, AddShards: []apis.File{f}})
	}
	xorbOrder, shardOrder := apis.Rank("h1", peers), apis.Rank(f.Hash, peers)
	byID := func(a, b apis.Peer) int { return strings.Compare(a.ID, b.ID) }
	if slices.IsSortedFunc(xorbOrder, byID) || slices.IsSortedFunc(shardOrder, byID) {
		t.Fatal("rank order equals ID order, pick other keys")
	}
	got := r.lookup(apis.LookupRequest{Xorbs: []string{"h1"}, Files: []string{f.Hash}, SHA256s: []string{f.SHA256}})
	if !slices.Equal(got.Xorbs["h1"], xorbOrder) {
		t.Fatalf("xorb holders = %v, want rank order %v", got.Xorbs["h1"], xorbOrder)
	}
	if !slices.Equal(got.Files[f.Hash].Peers, shardOrder) || !slices.Equal(got.SHA256s[f.SHA256].Peers, shardOrder) {
		t.Fatalf("shard holders = %v by hash, %v by SHA-256; want rank order %v", got.Files[f.Hash].Peers, got.SHA256s[f.SHA256].Peers, shardOrder)
	}
}

// p5 holds a xorb and a shard under each other's hashes, which holders must keep apart.
func TestHoldersOfXorbAndShard(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	var holders []apis.Peer
	for i := range 5 {
		holders = append(holders, testPeer(i))
		r.announce(apis.AnnounceRequest{Peer: testPeer(i), Epoch: "e", Full: true, Add: []string{"h1"}, AddShards: []apis.File{f}})
	}
	r.announce(apis.AnnounceRequest{Peer: testPeer(5), Epoch: "e", Full: true, Add: []string{f.Hash}, AddShards: []apis.File{{Hash: "h1", SHA256: "s9"}}})
	for _, o := range []object{{hash: "h1"}, {shard: true, hash: f.Hash}} {
		if got, want := r.holders(o), apis.Rank(o.hash, holders); !slices.Equal(got, want) {
			t.Fatalf("holders(%+v) = %v, want its holders in rank order %v", o, got, want)
		}
	}
}

func TestLookupOmitsKeysWithoutHolders(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", Full: true, Add: []string{"h1"}, AddShards: []apis.File{f}})
	r.announce(fullReq(testPeer(2), "e"))
	req := apis.LookupRequest{Peer: "p1", Xorbs: []string{"h1", "h2"}, Files: []string{f.Hash, "f9"}, SHA256s: []string{f.SHA256, "s9"}}
	if got := r.lookup(req); len(got.Xorbs)+len(got.Files)+len(got.SHA256s) != 0 {
		t.Fatalf("lookup by the only holder = %+v, want every key omitted", got)
	}
	req.Peer = "p2"
	got := r.lookup(req)
	if _, ok := got.Xorbs["h2"]; ok || len(got.Xorbs) != 1 {
		t.Fatalf("xorbs = %v, want only h1", got.Xorbs)
	}
	if _, ok := got.Files["f9"]; ok || len(got.Files) != 1 {
		t.Fatalf("files = %v, want only %s", got.Files, f.Hash)
	}
	if _, ok := got.SHA256s["s9"]; ok || len(got.SHA256s) != 1 {
		t.Fatalf("sha256s = %v, want only %s", got.SHA256s, f.SHA256)
	}
}

func TestRestartedEpochDeltaResyncsAndDropsHoldings(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	r.announce(fullReq(testPeer(1), "e1", "h1"))
	if resp, _ := r.announce(deltaReq(testPeer(1), "e2", []string{"h2"}, nil)); !resp.Resync {
		t.Fatal("delta from a new epoch must request resync")
	}
	if got := holderCounts(r, "h1", "h2"); !slices.Equal(got, []int{0, 0}) {
		t.Fatalf("holders after epoch change = %v, want old incarnation dropped and delta ignored", got)
	}
	if got := r.live(); len(got) != 0 {
		t.Fatalf("live peers = %v, want the restarted peer unregistered before a full inventory", got)
	}
	if resp, _ := r.announce(fullReq(testPeer(1), "e2", "h2")); resp.Resync {
		t.Fatal("full from the new epoch must be accepted")
	}
	if got := holderCounts(r, "h1", "h2"); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("holders after new full = %v, want [0 1]", got)
	}
	if resp, _ := r.announce(deltaReq(testPeer(1), "e2", nil, nil)); resp.Resync {
		t.Fatal("heartbeat after full must be accepted")
	}
}

func TestUnknownPeerHeartbeatResyncs(t *testing.T) {
	r := newRegistry(time.Minute, 2)
	if resp, _ := r.announce(deltaReq(testPeer(1), "e", nil, nil)); !reflect.DeepEqual(resp, apis.AnnounceResponse{Resync: true, TTL: 60, MinSeeds: 2}) {
		t.Fatalf("unknown heartbeat response = %+v, want resync with TTL and MinSeeds", resp)
	}
	if got := r.live(); len(got) != 0 {
		t.Fatalf("live peers = %v, want the unknown peer left unregistered by a delta", got)
	}
	if resp, _ := r.announce(fullReq(testPeer(1), "e", "h1")); !reflect.DeepEqual(resp, apis.AnnounceResponse{TTL: 60, MinSeeds: 2}) {
		t.Fatalf("full from unknown peer = %+v, want accepted with TTL and MinSeeds", resp)
	}
	if got := xorbHolders(r, "h1"); !slices.Equal(got, []apis.Peer{testPeer(1)}) {
		t.Fatalf("holders after full = %v, want the new peer", got)
	}
}

func TestURLChangeRequiresFullInventory(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	r.announce(fullReq(testPeer(1), "e", "h1"))
	moved := apis.Peer{ID: testPeer(1).ID, URL: "http://moved"}
	if resp, _ := r.announce(deltaReq(moved, "e", nil, nil)); !resp.Resync {
		t.Fatal("delta with changed URL must request resync")
	}
	if got := holderCounts(r, "h1"); !slices.Equal(got, []int{0}) {
		t.Fatalf("holders after URL change = %v, want unreachable holdings dropped", got)
	}
	r.announce(fullReq(moved, "e", "h1"))
	if got := xorbHolders(r, "h1"); !slices.Equal(got, []apis.Peer{moved}) {
		t.Fatalf("holders after full = %v, want %v", got, moved)
	}
}

func TestUpstreamChangeRequiresFullInventory(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	access := apis.Peer{ID: "p1", URL: "http://p1", Upstream: true}
	if resp, _ := r.announce(fullReq(access, "e", "h1")); !slices.Equal(resp.Upstreams, []apis.Peer{access}) {
		t.Fatalf("full of an access peer = %+v, want it listed as upstream", resp)
	}
	internal := apis.Peer{ID: access.ID, URL: access.URL}
	if resp, _ := r.announce(deltaReq(internal, "e", nil, nil)); !resp.Resync || len(resp.Upstreams) != 0 {
		t.Fatalf("delta with Upstream cleared = %+v, want resync and no upstream left", resp)
	}
	if got := holderCounts(r, "h1"); !slices.Equal(got, []int{0}) {
		t.Fatalf("holders after the Upstream change = %v, want the peer's holdings dropped", got)
	}
}

func TestUpstreamsAreLiveAccessPeersByID(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	access := func(i int) apis.Peer {
		p := testPeer(i)
		p.Upstream = true
		return p
	}
	r.announce(fullReq(access(3), "e"))
	r.announce(fullReq(testPeer(2), "e"))
	*now = base.Add(30 * time.Second)
	r.announce(fullReq(access(1), "e"))
	if got, want := r.upstreams(), []apis.Peer{access(1), access(3)}; !slices.Equal(got, want) {
		t.Fatalf("upstreams = %v, want the live access peers by ID %v", got, want)
	}
	*now = base.Add(time.Minute)
	if got, want := r.upstreams(), []apis.Peer{access(1)}; !slices.Equal(got, want) {
		t.Fatalf("upstreams once p3 expired = %v, want %v", got, want)
	}
}

func TestSnapshotListsLivePeers(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	access := apis.Peer{ID: "p2", URL: "http://p2", Upstream: true}
	r.announce(apis.AnnounceRequest{Peer: access, Epoch: "e2", Full: true, Add: []string{"h1", "h2"}, AddShards: []apis.File{testFile(1)}})
	r.announce(fullReq(testPeer(1), "e1"))
	want := []apis.PeerStatus{
		{ID: "p1", URL: "http://p1", Epoch: "e1", Expiry: base.Add(time.Minute)},
		{ID: "p2", URL: "http://p2", Upstream: true, Epoch: "e2", Expiry: base.Add(time.Minute), Holdings: 2, Shards: 1},
	}
	if got := r.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
	b, err := json.Marshal(want[1])
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"id":"p2","url":"http://p2","upstream":true,"epoch":"e2","expiry":"1970-01-01T00:17:40Z","holdings":2,"shards":1,"files":0}`
	if string(b) != wantJSON {
		t.Fatalf("snapshot JSON = %s, want %s", b, wantJSON)
	}
	*now = base.Add(time.Minute)
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after expiry = %+v, want empty", got)
	}
}
