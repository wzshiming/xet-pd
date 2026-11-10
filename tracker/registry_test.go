package tracker

import (
	"encoding/json"
	"fmt"
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

// filesReq is a Full announce of p holding files complete and no xorbs.
func filesReq(p apis.Peer, epoch string, files ...apis.File) apis.AnnounceRequest {
	return apis.AnnounceRequest{Peer: p, Epoch: epoch, Full: true, AddFiles: files}
}

// xorbHolders lists the live holders of xorb hash as a lookup by no peer answers it.
func xorbHolders(r *registry, hash string) []apis.Peer {
	return r.lookup(apis.LookupRequest{Xorbs: []string{hash}}).Xorbs[hash]
}

// peersOf drops the scores and holder flags of a ranking.
func peersOf(list []ranked) []apis.Peer {
	var out []apis.Peer
	for _, p := range list {
		out = append(out, p.Peer)
	}
	return out
}

func TestLookupEmptyRegistry(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	if got := r.lookup(apis.LookupRequest{Xorbs: []string{"h1"}, Files: []string{"f1"}, SHA256s: []string{"s1"}}); len(got.Xorbs)+len(got.Files)+len(got.SHA256s) != 0 {
		t.Fatalf("lookup on empty registry = %+v, want no key", got)
	}
	if got := r.rank("h1"); len(got) != 0 {
		t.Fatalf("rank on empty registry = %v", got)
	}
	if got := r.fileHolders("f1"); len(got) != 0 {
		t.Fatalf("fileHolders on empty registry = %v", got)
	}
}

func TestRankingIndependentOfRegistrationOrder(t *testing.T) {
	const n = 5
	var want []ranked
	for order := range 2 {
		r := newRegistry(time.Minute, 1)
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
	r := newRegistry(time.Minute, 1)
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
		tops[r.rank(fmt.Sprintf("hash%d", i))[0].Peer] = true
	}
	if len(tops) < 2 {
		t.Fatalf("ranking does not spread hashes across peers: %v", tops)
	}
}

func TestLookupListsXorbHoldersInRankOrder(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	for i := range 5 {
		r.announce(fullReq(testPeer(i), "e"))
	}
	ranking := r.rank("h1")
	low, lower := ranking[3].Peer, ranking[4].Peer
	r.announce(deltaReq(lower, "e", []string{"h1"}, nil))
	if got := xorbHolders(r, "h1"); !slices.Equal(got, []apis.Peer{lower}) {
		t.Fatalf("holders = %v, want the lowest-ranked holder %v", got, lower)
	}
	r.announce(deltaReq(low, "e", []string{"h1"}, nil))
	if got := xorbHolders(r, "h1"); !slices.Equal(got, []apis.Peer{low, lower}) {
		t.Fatalf("holders = %v, want ranked holders %v", got, []apis.Peer{low, lower})
	}
	got := r.rank("h1")
	if !slices.EqualFunc(got, ranking, func(a, b ranked) bool { return a.Peer == b.Peer }) {
		t.Fatalf("rank changed with holdings: %v, want %v", got, ranking)
	}
	for i, p := range got {
		if p.holder != (i >= 3) {
			t.Fatalf("rank %v flags holders wrong, want only the last two", got)
		}
	}
	if got := xorbHolders(r, "h2"); len(got) != 0 {
		t.Fatalf("other hash affected: holders = %v", got)
	}
}

func TestConcurrentAnnounceAndLookup(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			p := testPeer(i)
			r.announce(fullReq(p, "e"))
			for j := range 200 {
				h, f := fmt.Sprintf("h%d", j%10), testFile(j%10)
				r.announce(apis.AnnounceRequest{Peer: p, Epoch: "e",
					Add: []string{h}, Remove: []string{fmt.Sprintf("h%d", (j+5)%10)},
					AddFiles: []apis.File{f}, RemoveFiles: []string{testFile((j + 5) % 10).Hash}})
				r.lookup(apis.LookupRequest{Peer: p.ID, Xorbs: []string{h}, Files: []string{f.Hash}, SHA256s: []string{f.SHA256}})
				r.fileHolders(f.Hash)
				r.rank(h)
				r.snapshot()
				r.stats()
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
	if resp, _ := r.announce(fullReq(testPeer(1), "e", "h1")); resp.TTL != 60 {
		t.Fatalf("TTL = %d, want 60", resp.TTL)
	}
	*now = base.Add(time.Minute - time.Nanosecond)
	if got := xorbHolders(r, "h1"); !slices.Equal(got, []apis.Peer{testPeer(1)}) {
		t.Fatalf("holders just before expiry = %v, want the live holder", got)
	}
	*now = base.Add(time.Minute)
	if len(xorbHolders(r, "h1")) != 0 || len(r.rank("h1")) != 0 {
		t.Fatal("expired peer must be excluded from holders and rank")
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

func fileHolderCounts(r *registry, files ...apis.File) []int {
	var out []int
	for _, f := range files {
		out = append(out, len(r.fileHolders(f.Hash)))
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

func TestFullReplacesFiles(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f1, f2, f3 := testFile(1), testFile(2), testFile(3)
	r.announce(filesReq(testPeer(1), "e", f1, f2))
	if got := fileHolderCounts(r, f1, f2, f3); !slices.Equal(got, []int{1, 1, 0}) {
		t.Fatalf("complete holders after the first full = %v, want [1 1 0]", got)
	}
	r.announce(filesReq(testPeer(1), "e", f3))
	if got := fileHolderCounts(r, f1, f2, f3); !slices.Equal(got, []int{0, 0, 1}) {
		t.Fatalf("complete holders after the second full = %v, want [0 0 1]", got)
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

func TestFileDeltaAddRemoveIdempotent(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f1, f2 := testFile(1), testFile(2)
	r.announce(filesReq(testPeer(1), "e", f1))
	delta := func(add []apis.File, remove []string) {
		t.Helper()
		if resp, _ := r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", AddFiles: add, RemoveFiles: remove}); resp.Resync {
			t.Fatalf("file delta %v/%v requested resync", add, remove)
		}
	}
	delta([]apis.File{f2, f2}, nil)
	delta([]apis.File{f2}, nil)
	if got := fileHolderCounts(r, f1, f2); !slices.Equal(got, []int{1, 1}) {
		t.Fatalf("complete holders after repeated adds = %v, want [1 1]", got)
	}
	delta(nil, []string{f1.Hash})
	delta(nil, []string{f1.Hash, "unknown"})
	if got := fileHolderCounts(r, f1, f2); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("complete holders after repeated removes = %v, want [0 1]", got)
	}
	if got := holderCounts(r, f2.Hash); !slices.Equal(got, []int{0}) {
		t.Fatalf("xorb holders of a complete file's hash = %v, want files kept apart from xorbs", got)
	}
}

func TestRemoveFilesWinsOverAddFiles(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	r.announce(filesReq(testPeer(1), "e"))
	_, changed := r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", AddFiles: []apis.File{f}, RemoveFiles: []string{f.Hash}})
	if got := r.fileHolders(f.Hash); len(got) != 0 {
		t.Fatalf("complete holders of a file added and removed in one request = %v, want none", got)
	}
	if len(changed) != 0 {
		t.Fatalf("changed = %v, want nothing for a file neither held before nor after", changed)
	}
	if got := r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}); len(got.SHA256s) != 0 {
		t.Fatalf("lookup by the SHA-256 of a removed file = %+v, want it omitted", got)
	}
	if _, changed = r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", AddFiles: []apis.File{f}}); !slices.Equal(changed, []string{f.Hash}) || len(r.fileHolders(f.Hash)) != 1 {
		t.Fatalf("AddFiles alone = changed %v, holders %v; want the file held", changed, r.fileHolders(f.Hash))
	}
}

// The latest announce of a SHA-256 decides the file it names.
func TestSHA256MapsToFile(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	r.announce(filesReq(testPeer(1), "e", f))
	bySHA := func() apis.FileLocation {
		return r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}).SHA256s[f.SHA256]
	}
	if got, want := bySHA(), (apis.FileLocation{File: f, Peers: []apis.Peer{testPeer(1)}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("lookup by SHA-256 = %+v, want %+v", got, want)
	}
	same := apis.File{Hash: "f9", SHA256: f.SHA256}
	r.announce(filesReq(testPeer(2), "e", same))
	if got, want := bySHA(), (apis.FileLocation{File: same, Peers: []apis.Peer{testPeer(2)}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("lookup by SHA-256 after another file claimed it = %+v, want %+v", got, want)
	}
}

// A SHA-256 stops naming a file once no live peer holds the file, so the file announced again under another one is not found by it.
func TestSHA256OfUnheldFileIsForgotten(t *testing.T) {
	f := testFile(1)
	for name, unhold := range map[string]func(r *registry, now *time.Time){
		"RemoveFiles": func(r *registry, _ *time.Time) {
			r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", RemoveFiles: []string{f.Hash}})
		},
		"expiry": func(r *registry, now *time.Time) {
			*now = now.Add(time.Minute)
			r.announce(filesReq(testPeer(2), "e"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, now := clockedRegistry(time.Minute)
			r.announce(filesReq(testPeer(1), "e", f))
			unhold(r, now)
			r.announce(filesReq(testPeer(2), "e", apis.File{Hash: f.Hash, SHA256: "s9"}))
			if got := r.lookup(apis.LookupRequest{SHA256s: []string{f.SHA256}}); len(got.SHA256s) != 0 {
				t.Fatalf("lookup by the SHA-256 of a file no live peer held since = %+v, want it omitted", got)
			}
		})
	}
}

func TestExpiryRemovesFiles(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	f := testFile(1)
	r.announce(filesReq(testPeer(1), "e", f))
	*now = now.Add(time.Minute - time.Nanosecond)
	if got := r.fileHolders(f.Hash); !slices.Equal(got, []apis.Peer{testPeer(1)}) {
		t.Fatalf("complete holders just before expiry = %v, want the live holder", got)
	}
	*now = now.Add(time.Nanosecond)
	if got := r.fileHolders(f.Hash); len(got) != 0 {
		t.Fatalf("complete holders at expiry = %v, want none", got)
	}
	r.announce(filesReq(testPeer(2), "e"))
	if got := r.lookup(apis.LookupRequest{Files: []string{f.Hash}, SHA256s: []string{f.SHA256}}); len(got.Files)+len(got.SHA256s) != 0 {
		t.Fatalf("lookup after the holder expired = %+v, want both keys omitted", got)
	}
}

func TestAnnounceListsChangedFiles(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	p := testPeer(1)
	f1, f2, f3 := testFile(1), testFile(2), testFile(3)
	changed := func(req apis.AnnounceRequest) []string {
		t.Helper()
		_, got := r.announce(req)
		slices.Sort(got)
		return got
	}
	if got := changed(filesReq(p, "e", f1, f2)); !slices.Equal(got, []string{f1.Hash, f2.Hash}) {
		t.Fatalf("changed by a first full = %v, want the files it added", got)
	}
	if got := changed(apis.AnnounceRequest{Peer: p, Epoch: "e", AddFiles: []apis.File{f1, f3}}); !slices.Equal(got, []string{f3.Hash}) {
		t.Fatalf("changed by AddFiles = %v, want only the file not held before", got)
	}
	if got := changed(apis.AnnounceRequest{Peer: p, Epoch: "e", RemoveFiles: []string{f2.Hash, "unknown"}}); !slices.Equal(got, []string{f2.Hash}) {
		t.Fatalf("changed by RemoveFiles = %v, want only the file held before", got)
	}
	if got := changed(deltaReq(p, "e", []string{"h1"}, nil)); len(got) != 0 {
		t.Fatalf("changed by a xorb delta = %v, want none", got)
	}
	if got := changed(filesReq(p, "e2", f3)); !slices.Equal(got, []string{f1.Hash, f3.Hash}) {
		t.Fatalf("changed by a full replacing the peer = %v, want every file of the replaced peer", got)
	}
	*now = now.Add(time.Minute)
	if got := changed(filesReq(testPeer(2), "e")); !slices.Equal(got, []string{f3.Hash}) {
		t.Fatalf("changed by an announce sweeping the expired peer = %v, want its files", got)
	}
	changed(filesReq(p, "e3", f1))
	if got := changed(apis.AnnounceRequest{Peer: p, Epoch: "e4"}); !slices.Equal(got, []string{f1.Hash}) {
		t.Fatalf("changed by a resync = %v, want the files of the dropped peer", got)
	}
}

func TestLookupExcludesAsker(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	for i := 1; i <= 2; i++ {
		r.announce(apis.AnnounceRequest{Peer: testPeer(i), Epoch: "e", Full: true, Add: []string{"h1"}, AddFiles: []apis.File{f}})
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
	for i := range 5 {
		r.announce(apis.AnnounceRequest{Peer: testPeer(i), Epoch: "e", Full: true, Add: []string{"h1"}, AddFiles: []apis.File{f}})
	}
	byID := func(a, b apis.Peer) int { return strings.Compare(a.ID, b.ID) }
	if slices.IsSortedFunc(peersOf(r.rank("h1")), byID) || slices.IsSortedFunc(peersOf(r.rank(f.Hash)), byID) {
		t.Fatal("rank order equals ID order, pick other keys")
	}
	got := r.lookup(apis.LookupRequest{Xorbs: []string{"h1"}, Files: []string{f.Hash}, SHA256s: []string{f.SHA256}})
	if want := peersOf(r.rank("h1")); !slices.Equal(got.Xorbs["h1"], want) {
		t.Fatalf("xorb holders = %v, want rank order %v", got.Xorbs["h1"], want)
	}
	want := peersOf(r.rank(f.Hash))
	if !slices.Equal(got.Files[f.Hash].Peers, want) || !slices.Equal(got.SHA256s[f.SHA256].Peers, want) || !slices.Equal(r.fileHolders(f.Hash), want) {
		t.Fatalf("complete holders = %v by hash, %v by SHA-256, %v from fileHolders; want rank order %v",
			got.Files[f.Hash].Peers, got.SHA256s[f.SHA256].Peers, r.fileHolders(f.Hash), want)
	}
}

func TestLookupOmitsKeysWithoutHolders(t *testing.T) {
	r := newRegistry(time.Minute, 1)
	f := testFile(1)
	r.announce(apis.AnnounceRequest{Peer: testPeer(1), Epoch: "e", Full: true, Add: []string{"h1"}, AddFiles: []apis.File{f}})
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
	if got := r.rank("h1"); len(got) != 0 {
		t.Fatalf("rank = %v, want the restarted peer unranked before a full inventory", got)
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
	if got := r.rank("h1"); len(got) != 0 {
		t.Fatalf("rank = %v, want the unknown peer left unregistered by a delta", got)
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

func TestSnapshotListsLivePeers(t *testing.T) {
	r, now := clockedRegistry(time.Minute)
	base := *now
	r.announce(apis.AnnounceRequest{Peer: testPeer(2), Epoch: "e2", Full: true, Add: []string{"h1", "h2"}, AddFiles: []apis.File{testFile(1)}})
	r.announce(fullReq(testPeer(1), "e1"))
	want := []apis.PeerStatus{
		{ID: "p1", URL: "http://p1", Epoch: "e1", Expiry: base.Add(time.Minute)},
		{ID: "p2", URL: "http://p2", Epoch: "e2", Expiry: base.Add(time.Minute), Holdings: 2, Files: 1},
	}
	if got := r.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
	b, err := json.Marshal(want[1])
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"id":"p2","url":"http://p2","epoch":"e2","expiry":"1970-01-01T00:17:40Z","holdings":2,"shards":0,"files":1}`
	if string(b) != wantJSON {
		t.Fatalf("snapshot JSON = %s, want %s", b, wantJSON)
	}
	*now = base.Add(time.Minute)
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after expiry = %+v, want empty", got)
	}
}
