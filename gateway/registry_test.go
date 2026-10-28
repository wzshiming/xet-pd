package gateway

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

func testPeer(i int) apis.Peer {
	return apis.Peer{ID: fmt.Sprintf("p%d", i), URL: fmt.Sprintf("http://p%d", i)}
}

// nodePeer is testPeer(i) running on node.
func nodePeer(i int, node string) apis.Peer {
	p := testPeer(i)
	p.Node = node
	return p
}

func fullReq(p apis.Peer, epoch string, hashes ...string) apis.AnnounceRequest {
	return apis.AnnounceRequest{Peer: p, Epoch: epoch, Full: true, Add: hashes}
}

func deltaReq(p apis.Peer, epoch string, add, remove []string) apis.AnnounceRequest {
	return apis.AnnounceRequest{Peer: p, Epoch: epoch, Add: add, Remove: remove}
}

func TestPickEmptyRegistry(t *testing.T) {
	r := newRegistry(time.Minute)
	if p, out := r.pick("h1", ""); out != pickNone {
		t.Fatalf("pick on empty registry = %v %v, want pickNone", p, out)
	}
	if got := r.rank("h1"); len(got) != 0 {
		t.Fatalf("rank on empty registry = %v", got)
	}
	if got := r.holders("h1"); len(got) != 0 || r.aliveHolders("h1") != 0 {
		t.Fatalf("holders on empty registry = %v", got)
	}
}

func TestRankingIndependentOfRegistrationOrder(t *testing.T) {
	const n = 5
	var want []ranked
	for order := range 2 {
		r := newRegistry(time.Minute)
		for i := range n {
			if order == 0 {
				r.announce(fullReq(testPeer(i), "e"))
			} else {
				r.announce(fullReq(testPeer(n-1-i), "e"))
			}
		}
		got := r.rank("h1")
		if len(got) != n {
			t.Fatalf("rank count = %d, want %d", len(got), n)
		}
		if p, out := r.pick("h1", ""); out != pickFallback || p != got[0].Peer {
			t.Fatalf("pick without holders = %v, want top seed %v", p, got[0].Peer)
		}
		if order == 0 {
			want = got
			continue
		}
		if !slices.Equal(got, want) {
			t.Fatalf("rank depends on registration order: %v vs %v", got, want)
		}
	}
}

func TestSeedsDistinctAndSpread(t *testing.T) {
	r := newRegistry(time.Minute)
	for i := range 5 {
		r.announce(fullReq(testPeer(i), "e"))
	}
	all := r.rank("h1")
	if len(all) != 5 {
		t.Fatalf("rank = %v, want all 5 peers", all)
	}
	seen := map[apis.Peer]bool{}
	for _, p := range all {
		if seen[p.Peer] {
			t.Fatalf("duplicate seed %v in %v", p.Peer, all)
		}
		seen[p.Peer] = true
	}
	tops := map[apis.Peer]bool{}
	for i := range 50 {
		p, _ := r.pick(fmt.Sprintf("hash%d", i), "")
		tops[p] = true
	}
	if len(tops) < 2 {
		t.Fatalf("ranking does not spread hashes across peers: %v", tops)
	}
}

func TestPickPrefersLiveHolder(t *testing.T) {
	r := newRegistry(time.Minute)
	for i := range 5 {
		r.announce(fullReq(testPeer(i), "e"))
	}
	ranking := r.rank("h1")
	low, lower := ranking[3].Peer, ranking[4].Peer
	r.announce(deltaReq(lower, "e", []string{"h1"}, nil))
	if p, out := r.pick("h1", ""); out != pickFallback || p != lower {
		t.Fatalf("pick = %v, want lowest-ranked holder %v", p, lower)
	}
	r.announce(deltaReq(low, "e", []string{"h1"}, nil))
	if p, _ := r.pick("h1", ""); p != low {
		t.Fatalf("pick = %v, want higher-ranked holder %v", p, low)
	}
	if got := r.holders("h1"); !slices.Equal(got, []apis.Peer{low, lower}) {
		t.Fatalf("holders = %v, want ranked holders %v", got, []apis.Peer{low, lower})
	}
	if n := r.aliveHolders("h1"); n != 2 {
		t.Fatalf("aliveHolders = %d, want 2", n)
	}
	if got := r.rank("h1"); !slices.EqualFunc(got, ranking, func(a, b ranked) bool { return a.Peer == b.Peer }) {
		t.Fatalf("rank changed with holdings: %v, want %v", got, ranking)
	}
	if p, _ := r.pick("h2", ""); p != r.rank("h2")[0].Peer || r.aliveHolders("h2") != 0 {
		t.Fatalf("other hash affected: pick = %v, want %v", p, r.rank("h2")[0].Peer)
	}
}

func TestConcurrentAnnounceAndLookup(t *testing.T) {
	r := newRegistry(time.Minute)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			p := testPeer(i)
			r.announce(fullReq(p, "e"))
			for j := range 200 {
				h := fmt.Sprintf("h%d", j%10)
				r.announce(deltaReq(p, "e", []string{h}, []string{fmt.Sprintf("h%d", (j+5)%10)}))
				r.pick(h, "")
				r.rank(h)
				r.holders(h)
				r.aliveHolders(h)
				r.snapshot()
			}
		})
	}
	wg.Wait()
	if got := r.rank("h1"); len(got) != 8 {
		t.Fatalf("live peers = %d, want 8", len(got))
	}
}

func TestTTLExpiryIsStrict(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	if resp := r.announce(fullReq(testPeer(1), "e", "h1")); resp.TTL != 60 {
		t.Fatalf("TTL = %d, want 60", resp.TTL)
	}
	*now = base.Add(time.Minute - time.Nanosecond)
	if p, out := r.pick("h1", ""); out != pickFallback || p != testPeer(1) {
		t.Fatalf("pick just before expiry = %v %v, want live holder", p, out)
	}
	*now = base.Add(time.Minute)
	if _, out := r.pick("h1", ""); out != pickNone {
		t.Fatal("pick at expiry must report pickNone")
	}
	if r.aliveHolders("h1") != 0 || len(r.rank("h1")) != 0 || len(r.holders("h1")) != 0 {
		t.Fatal("expired peer must be excluded from holders and rank")
	}
}

func TestEmptyDeltaRenewsHeartbeat(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	r.announce(fullReq(testPeer(1), "e", "h1"))
	*now = base.Add(50 * time.Second)
	if resp := r.announce(deltaReq(testPeer(1), "e", nil, nil)); resp.Resync {
		t.Fatal("heartbeat from known peer must not request resync")
	}
	*now = base.Add(time.Minute + 49*time.Second)
	if n := r.aliveHolders("h1"); n != 1 {
		t.Fatalf("aliveHolders after renewal = %d, want 1 (holdings kept)", n)
	}
	*now = base.Add(time.Minute + 50*time.Second)
	if n := r.aliveHolders("h1"); n != 0 {
		t.Fatalf("aliveHolders at renewed expiry = %d, want 0", n)
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
		out = append(out, r.aliveHolders(h))
	}
	return out
}

func TestFullReplacesInventory(t *testing.T) {
	r := newRegistry(time.Minute)
	r.announce(fullReq(testPeer(1), "e", "h1", "h2"))
	r.announce(fullReq(testPeer(1), "e", "h3"))
	if got := holderCounts(r, "h1", "h2", "h3"); !slices.Equal(got, []int{0, 0, 1}) {
		t.Fatalf("holders after full = %v, want [0 0 1]", got)
	}
}

func TestDeltaAddRemoveIdempotentRemoveWins(t *testing.T) {
	r := newRegistry(time.Minute)
	r.announce(fullReq(testPeer(1), "e"))
	delta := func(add, remove []string) {
		if resp := r.announce(deltaReq(testPeer(1), "e", add, remove)); resp.Resync {
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

func TestRestartedEpochDeltaResyncsAndDropsHoldings(t *testing.T) {
	r := newRegistry(time.Minute)
	r.announce(fullReq(testPeer(1), "e1", "h1"))
	if resp := r.announce(deltaReq(testPeer(1), "e2", []string{"h2"}, nil)); !resp.Resync {
		t.Fatal("delta from a new epoch must request resync")
	}
	if got := holderCounts(r, "h1", "h2"); !slices.Equal(got, []int{0, 0}) {
		t.Fatalf("holders after epoch change = %v, want old incarnation dropped and delta ignored", got)
	}
	if _, out := r.pick("h1", ""); out != pickNone {
		t.Fatal("restarted peer must not be selectable before a full inventory")
	}
	if resp := r.announce(fullReq(testPeer(1), "e2", "h2")); resp.Resync {
		t.Fatal("full from the new epoch must be accepted")
	}
	if got := holderCounts(r, "h1", "h2"); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("holders after new full = %v, want [0 1]", got)
	}
	if resp := r.announce(deltaReq(testPeer(1), "e2", nil, nil)); resp.Resync {
		t.Fatal("heartbeat after full must be accepted")
	}
}

func TestUnknownPeerHeartbeatResyncs(t *testing.T) {
	r := newRegistry(time.Minute)
	if resp := r.announce(deltaReq(testPeer(1), "e", nil, nil)); !resp.Resync || resp.TTL != 60 {
		t.Fatalf("unknown heartbeat response = %+v, want resync with TTL", resp)
	}
	if _, out := r.pick("h1", ""); out != pickNone {
		t.Fatal("unknown peer must not be registered by a delta")
	}
	if resp := r.announce(fullReq(testPeer(1), "e", "h1")); resp.Resync {
		t.Fatal("full from unknown peer must be accepted")
	}
	if p, out := r.pick("h1", ""); out != pickFallback || p != testPeer(1) {
		t.Fatalf("pick after full = %v %v, want new peer", p, out)
	}
}

func TestURLChangeRequiresFullInventory(t *testing.T) {
	r := newRegistry(time.Minute)
	r.announce(fullReq(testPeer(1), "e", "h1"))
	moved := apis.Peer{ID: testPeer(1).ID, URL: "http://moved"}
	if resp := r.announce(deltaReq(moved, "e", nil, nil)); !resp.Resync {
		t.Fatal("delta with changed URL must request resync")
	}
	if got := holderCounts(r, "h1"); !slices.Equal(got, []int{0}) {
		t.Fatalf("holders after URL change = %v, want unreachable holdings dropped", got)
	}
	r.announce(fullReq(moved, "e", "h1"))
	if p, _ := r.pick("h1", ""); p != moved {
		t.Fatalf("pick after full = %v, want %v", p, moved)
	}
}

func TestNodeChangeRequiresFullInventory(t *testing.T) {
	r := newRegistry(time.Minute)
	r.announce(fullReq(nodePeer(1, "n1"), "e", "h1"))
	moved := nodePeer(1, "n2")
	if resp := r.announce(deltaReq(moved, "e", nil, nil)); !resp.Resync {
		t.Fatal("delta with changed node must request resync")
	}
	if got := holderCounts(r, "h1"); !slices.Equal(got, []int{0}) {
		t.Fatalf("holders after node change = %v, want the holdings dropped", got)
	}
	r.announce(fullReq(moved, "e", "h1"))
	if p, out := r.pick("h1", "n2"); p != moved || out != pickLocal {
		t.Fatalf("pick for a client on n2 after full = %v %v, want %v local", p, out, moved)
	}
}

func TestPickPrefersClientNodePeer(t *testing.T) {
	r := newRegistry(time.Minute)
	for i := range 4 {
		r.announce(fullReq(nodePeer(i, fmt.Sprintf("n%d", i)), "e"))
	}
	r.announce(fullReq(testPeer(4), "e"))
	var onNodes []apis.Peer
	for _, p := range r.rank("h1") {
		if p.Node != "" {
			onNodes = append(onNodes, p.Peer)
		}
	}
	best, local := onNodes[0], onNodes[len(onNodes)-1]
	r.announce(deltaReq(best, "e", []string{"h1"}, nil))
	if p, out := r.pick("h1", local.Node); p != local || out != pickLocal {
		t.Fatalf("pick for a client on %s = %v %v, want its node's non-holder %v local over holder %v", local.Node, p, out, local, best)
	}
	r.announce(deltaReq(local, "e", []string{"h1"}, nil))
	if p, out := r.pick("h1", local.Node); p != local || out != pickLocal {
		t.Fatalf("pick for a client on %s = %v %v, want its node's holder %v local over better-ranked %v", local.Node, p, out, local, best)
	}
	// testPeer(4) runs on no node: a client on none must not count as its neighbour.
	for _, node := range []string{"elsewhere", ""} {
		if p, out := r.pick("h1", node); p != best || out != pickFallback {
			t.Fatalf("pick for a client on %q = %v %v, want best holder %v as fallback", node, p, out, best)
		}
	}

	r = newRegistry(time.Minute)
	for i := 1; i <= 3; i++ {
		r.announce(fullReq(nodePeer(i, "n1"), "e"))
	}
	// Find a hash that ranks p3 above p2, so ID order and rank order disagree between the two holders.
	var hash string
	for i := 0; hash == ""; i++ {
		var ids []string
		for _, p := range r.rank(fmt.Sprintf("h%d", i)) {
			ids = append(ids, p.ID)
		}
		if slices.Index(ids, "p3") < slices.Index(ids, "p2") {
			hash = fmt.Sprintf("h%d", i)
		}
	}
	for _, i := range []int{2, 3} {
		r.announce(deltaReq(nodePeer(i, "n1"), "e", []string{hash}, nil))
	}
	if p, out := r.pick(hash, "n1"); p != nodePeer(2, "n1") || out != pickLocal {
		t.Fatalf("pick among peers on n1 = %v %v, want holder p2 (holders first, then lowest ID) local", p, out)
	}
}

func TestPickLocalPeerExpiredFallsBack(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	local, holder := nodePeer(1, "n1"), nodePeer(2, "n2")
	r.announce(fullReq(local, "e"))
	*now = base.Add(30 * time.Second)
	r.announce(fullReq(holder, "e", "h1"))
	if p, out := r.pick("h1", "n1"); p != local || out != pickLocal {
		t.Fatalf("pick while the local peer lives = %v %v, want %v local", p, out, local)
	}
	*now = base.Add(time.Minute)
	if p, out := r.pick("h1", "n1"); p != holder || out != pickFallback {
		t.Fatalf("pick once the local peer expired = %v %v, want holder %v as fallback", p, out, holder)
	}
}

func TestPickNoLivePeerIsNone(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	if p, out := r.pick("h1", "n1"); p != (apis.Peer{}) || out != pickNone {
		t.Fatalf("pick on empty registry = %v %v, want none", p, out)
	}
	r.announce(fullReq(nodePeer(1, "n1"), "e", "h1"))
	*now = now.Add(time.Minute)
	if p, out := r.pick("h1", "n1"); p != (apis.Peer{}) || out != pickNone {
		t.Fatalf("pick with the local holder expired = %v %v, want none", p, out)
	}
}

func TestSnapshotListsLivePeers(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	r.announce(fullReq(testPeer(2), "e2", "h1", "h2"))
	r.announce(fullReq(nodePeer(1, "n1"), "e1"))
	want := []apis.PeerStatus{
		{ID: "p1", URL: "http://p1", Node: "n1", Epoch: "e1", Expiry: base.Add(time.Minute)},
		{ID: "p2", URL: "http://p2", Epoch: "e2", Expiry: base.Add(time.Minute), Holdings: 2},
	}
	if got := r.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
	b, err := json.Marshal(want[1])
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"id":"p2","url":"http://p2","epoch":"e2","expiry":"1970-01-01T00:17:40Z","holdings":2}`
	if string(b) != wantJSON {
		t.Fatalf("snapshot JSON = %s, want %s", b, wantJSON)
	}
	if b, err = json.Marshal(want[0]); err != nil {
		t.Fatal(err)
	}
	const nodeJSON = `{"id":"p1","url":"http://p1","node":"n1","epoch":"e1","expiry":"1970-01-01T00:17:40Z","holdings":0}`
	if string(b) != nodeJSON {
		t.Fatalf("snapshot JSON of a peer on a node = %s, want %s", b, nodeJSON)
	}
	*now = base.Add(time.Minute)
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after expiry = %+v, want empty", got)
	}
}
