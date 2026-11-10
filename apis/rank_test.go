package apis

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// tieKey gives tieLow and tieHigh the same rendezvous weight; a collision search found the pair.
const tieKey, tieLow, tieHigh = "org/repo", "168baef8f684ff7f", "91b9b16c4aa2c3e2"

// weight is the score tracker/registry.go's order() gives a peer at 61685df.
func weight(key, id string) uint64 {
	h := fnv.New64a()
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(key))))
	h.Write([]byte(key))
	h.Write([]byte(id))
	return h.Sum64()
}

// trackerOrder is tracker/registry.go's order() at 61685df without its liveness filter and holder marks.
func trackerOrder(key string, peers []Peer) []Peer {
	type ranked struct {
		Peer
		score uint64
	}
	list := make([]ranked, 0, len(peers))
	for _, p := range peers {
		list = append(list, ranked{Peer: p, score: weight(key, p.ID)})
	}
	slices.SortFunc(list, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(b.score, a.score), strings.Compare(a.ID, b.ID))
	})
	out := make([]Peer, 0, len(list))
	for _, r := range list {
		out = append(out, r.Peer)
	}
	return out
}

// fivePeers returns new p1 to p5 in ID order, which is not their order under org/repo.
func fivePeers() []Peer {
	return []Peer{{ID: "p1", URL: "http://p1"}, {ID: "p2", URL: "http://p2"}, {ID: "p3", URL: "http://p3"}, {ID: "p4", URL: "http://p4"}, {ID: "p5", URL: "http://p5"}}
}

// The golden order was computed outside Go, so a per-process hash seed would fail it.
func TestRankIsDeterministic(t *testing.T) {
	t.Parallel()
	peers := fivePeers()
	want := []Peer{peers[4], peers[3], peers[0], peers[2], peers[1]}
	if got := Rank("org/repo", peers); !slices.Equal(got, want) {
		t.Fatalf("Rank(org/repo, p1..p5) = %v, want %v", got, want)
	}
}

func TestRankIgnoresInputOrder(t *testing.T) {
	t.Parallel()
	want := Rank("org/repo", fivePeers())
	r := rand.New(rand.NewPCG(1, 2))
	for range 20 {
		in := fivePeers()
		r.Shuffle(len(in), func(i, j int) { in[i], in[j] = in[j], in[i] })
		if got := Rank("org/repo", in); !slices.Equal(got, want) {
			t.Fatalf("Rank(org/repo, %v) = %v, want %v", in, got, want)
		}
	}
}

func TestRankLeavesInputUnchanged(t *testing.T) {
	t.Parallel()
	in := fivePeers()
	got := Rank("org/repo", in)
	got[0] = Peer{ID: "written"}
	if want := fivePeers(); !slices.Equal(in, want) {
		t.Fatalf("input after Rank and a write to its result = %v, want %v", in, want)
	}
}

func TestRankBreaksTiesByID(t *testing.T) {
	t.Parallel()
	if weight(tieKey, tieLow) != weight(tieKey, tieHigh) {
		t.Fatal("fixture: tieLow and tieHigh no longer tie under tieKey")
	}
	low, high := Peer{ID: tieLow, URL: "http://low"}, Peer{ID: tieHigh, URL: "http://high"}
	for _, in := range [][]Peer{{low, high}, {high, low}} {
		if got := Rank(tieKey, in); !slices.Equal(got, []Peer{low, high}) {
			t.Errorf("Rank(%q, %v) = %v, want the lower ID first", tieKey, in, got)
		}
	}
}

func TestRankOfNoPeersIsEmpty(t *testing.T) {
	t.Parallel()
	for _, in := range [][]Peer{nil, {}} {
		if got := Rank("org/repo", in); got == nil || len(got) != 0 {
			t.Errorf("Rank(%#v) = %#v, want an empty non-nil slice", in, got)
		}
	}
}

func TestRankMatchesTrackerOrder(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 1000 {
		key := make([]byte, r.IntN(24))
		for j := range key {
			key[j] = byte(r.Uint32())
		}
		peers := make([]Peer, r.IntN(16))
		for j := range peers {
			peers[j] = Peer{ID: fmt.Sprintf("%d-%x", j, r.Uint32()), URL: fmt.Sprintf("http://%x", r.Uint32()), Upstream: r.IntN(2) == 0}
		}
		if got, want := Rank(string(key), peers), trackerOrder(string(key), peers); !slices.Equal(got, want) {
			t.Fatalf("case %d: Rank(%q, %v) = %v, want %v", i, key, peers, got, want)
		}
	}
}
