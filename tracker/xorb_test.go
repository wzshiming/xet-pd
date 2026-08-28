package tracker_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/tracker"
)

var xorbA = strings.Repeat("ab", 32)

func announceXorb(t *testing.T, c *tracker.Client, peer, hash string, ttl int64, spans ...tracker.ChunkSpan) {
	t.Helper()
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: peer,
		TTL:  ttl,
		Adds: tracker.AnnounceSet{Xorbs: []tracker.XorbAdd{{Hash: hash, Ranges: spans}}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func lookupXorb(t *testing.T, c *tracker.Client, hash string, span tracker.ChunkSpan) []tracker.XorbLocation {
	t.Helper()
	resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{
		Xorbs: []tracker.XorbQuery{{Hash: hash, Range: span}},
	})
	if err != nil {
		t.Fatal(err)
	}
	locs := resp.Xorbs[fmt.Sprintf("%s:%d-%d", hash, span.Start, span.End)]
	sort.Slice(locs, func(i, j int) bool { return locs[i].Peer < locs[j].Peer })
	return locs
}

func peerNames(locs []tracker.XorbLocation) []string {
	var names []string
	for _, loc := range locs {
		names = append(names, loc.Peer)
	}
	return names
}

// TestXorbAnnounceLookupRoundtrip: lookups return exactly the peers whose
// announced ranges fully contain the query, keyed by the query itself.
func TestXorbAnnounceLookupRoundtrip(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	announceXorb(t, c, "http://a:8880", xorbA, 0, tracker.ChunkSpan{Start: 2, End: 8})
	announceXorb(t, c, "http://b:8880", xorbA, 0, tracker.ChunkSpan{Start: 3, End: 5})

	tests := []struct {
		name  string
		span  tracker.ChunkSpan
		peers []string
	}{
		{"exact match plus containing peer", tracker.ChunkSpan{Start: 3, End: 5}, []string{"http://a:8880", "http://b:8880"}},
		{"partial overlap excluded", tracker.ChunkSpan{Start: 2, End: 6}, []string{"http://a:8880"}},
		{"uncovered range misses", tracker.ChunkSpan{Start: 0, End: 3}, nil},
		{"inverted range misses", tracker.ChunkSpan{Start: 5, End: 3}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locs := lookupXorb(t, c, xorbA, tt.span)
			if got := peerNames(locs); !reflect.DeepEqual(got, tt.peers) {
				t.Fatalf("got peers = %v, want %v", got, tt.peers)
			}
		})
	}

	t.Run("response key is the query", func(t *testing.T) {
		resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{
			Xorbs: []tracker.XorbQuery{
				{Hash: xorbA, Range: tracker.ChunkSpan{Start: 3, End: 5}},
				{Hash: xorbA, Range: tracker.ChunkSpan{Start: 2, End: 6}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		for key, wantN := range map[string]int{xorbA + ":3-5": 2, xorbA + ":2-6": 1} {
			if got := len(resp.Xorbs[key]); got != wantN {
				t.Errorf("got %d locations under key %q, want %d", got, key, wantN)
			}
		}
	})

	t.Run("location carries the peer's full ranges", func(t *testing.T) {
		locs := lookupXorb(t, c, xorbA, tracker.ChunkSpan{Start: 2, End: 6})
		want := []tracker.ChunkSpan{{Start: 2, End: 8}}
		if len(locs) != 1 || !reflect.DeepEqual(locs[0].Ranges, want) {
			t.Fatalf("got locations = %+v, want ranges %v", locs, want)
		}
	})
}

// TestXorbRangesMergeOnWrite: adjacent and overlapping announced spans merge
// into one stored range, so containment spans announce boundaries.
func TestXorbRangesMergeOnWrite(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	announceXorb(t, c, "http://a:8880", xorbA, 0, tracker.ChunkSpan{Start: 0, End: 4})
	announceXorb(t, c, "http://a:8880", xorbA, 0, tracker.ChunkSpan{Start: 4, End: 8}, tracker.ChunkSpan{Start: 6, End: 10})

	locs := lookupXorb(t, c, xorbA, tracker.ChunkSpan{Start: 1, End: 9})
	want := []tracker.ChunkSpan{{Start: 0, End: 10}}
	if len(locs) != 1 || !reflect.DeepEqual(locs[0].Ranges, want) {
		t.Fatalf("got locations = %+v, want single peer with ranges %v", locs, want)
	}
}

// TestXorbClaimExpires: an expired xorb claim stops resolving.
func TestXorbClaimExpires(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	announceXorb(t, c, "http://a:8880", xorbA, 1, tracker.ChunkSpan{Start: 0, End: 8})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if locs := lookupXorb(t, c, xorbA, tracker.ChunkSpan{Start: 1, End: 2}); len(locs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("xorb claim with 1s TTL still returned")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestXorbRemove: a remove withdraws the peer's whole claim on the xorb.
func TestXorbRemove(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	announceXorb(t, c, "http://a:8880", xorbA, 0, tracker.ChunkSpan{Start: 0, End: 8})
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer:    "http://a:8880",
		Removes: tracker.AnnounceSet{Xorbs: []tracker.XorbAdd{{Hash: xorbA}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if locs := lookupXorb(t, c, xorbA, tracker.ChunkSpan{Start: 0, End: 8}); len(locs) != 0 {
		t.Fatalf("got locations = %+v after remove, want none", locs)
	}
}

// TestXorbIndexSurvivesRestart: xorb claims are plain files, so a new
// handler over the same directory answers immediately.
func TestXorbIndexSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	_, c := newTracker(t, dir)
	announceXorb(t, c, "http://a:8880", xorbA, 0, tracker.ChunkSpan{Start: 0, End: 8})

	_, c2 := newTracker(t, dir)
	locs := lookupXorb(t, c2, xorbA, tracker.ChunkSpan{Start: 2, End: 4})
	if got := peerNames(locs); !reflect.DeepEqual(got, []string{"http://a:8880"}) {
		t.Fatalf("got restarted lookup peers = %v, want the announced peer", got)
	}
}

// TestXorbHostileHash: a non-hex xorb hash is neither indexed nor allowed to
// touch the filesystem.
func TestXorbHostileHash(t *testing.T) {
	dir := t.TempDir()
	_, c := newTracker(t, dir)
	hostile := "../files/" + shaA
	announceXorb(t, c, "http://a:8880", hostile, 0, tracker.ChunkSpan{Start: 0, End: 8})

	if locs := lookupXorb(t, c, hostile, tracker.ChunkSpan{Start: 0, End: 8}); len(locs) != 0 {
		t.Fatalf("got locations = %+v for hostile hash, want none", locs)
	}
	for _, sub := range []string{"files", "keys", "xorbs"} {
		entries, err := os.ReadDir(filepath.Join(dir, sub))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("got %d entries under %s after hostile announce, want 0", len(entries), sub)
		}
	}
}

// TestXorbSpanCaps: invalid spans are dropped and at most maxSpansPerXorb
// (256) survive per announce.
func TestXorbSpanCaps(t *testing.T) {
	_, c := newTracker(t, t.TempDir())

	t.Run("inverted and empty spans rejected", func(t *testing.T) {
		announceXorb(t, c, "http://z:8880", xorbA, 0,
			tracker.ChunkSpan{Start: 5, End: 5}, tracker.ChunkSpan{Start: 6, End: 2})
		if locs := lookupXorb(t, c, xorbA, tracker.ChunkSpan{Start: 5, End: 5}); len(locs) != 0 {
			t.Fatalf("got locations = %+v for empty span, want none", locs)
		}
	})

	t.Run("ranges per xorb clamped", func(t *testing.T) {
		// 300 disjoint one-chunk spans: only the first 256 are taken.
		spans := make([]tracker.ChunkSpan, 300)
		for i := range spans {
			spans[i] = tracker.ChunkSpan{Start: uint32(2 * i), End: uint32(2*i + 1)}
		}
		announceXorb(t, c, "http://a:8880", xorbA, 0, spans...)
		locs := lookupXorb(t, c, xorbA, spans[0])
		if len(locs) != 1 {
			t.Fatalf("got %d locations, want 1", len(locs))
		}
		if got := len(locs[0].Ranges); got != 256 {
			t.Fatalf("got %d stored ranges, want 256", got)
		}
		if locs := lookupXorb(t, c, xorbA, spans[299]); len(locs) != 0 {
			t.Fatalf("got locations = %+v beyond the cap, want none", locs)
		}
	})
}

// TestXorbStoredCapNewestWins: when merging an announce into the stored
// claim would exceed the stored cap, the newest announce replaces the claim.
func TestXorbStoredCapNewestWins(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	old := make([]tracker.ChunkSpan, 200)
	for i := range old {
		old[i] = tracker.ChunkSpan{Start: uint32(4 * i), End: uint32(4*i + 1)}
	}
	announceXorb(t, c, "http://a:8880", xorbA, 0, old...)
	newer := make([]tracker.ChunkSpan, 200)
	for i := range newer {
		newer[i] = tracker.ChunkSpan{Start: uint32(4*i + 2), End: uint32(4*i + 3)}
	}
	announceXorb(t, c, "http://a:8880", xorbA, 0, newer...)

	locs := lookupXorb(t, c, xorbA, newer[0])
	if len(locs) != 1 {
		t.Fatalf("got %d locations for a newest span, want 1", len(locs))
	}
	if !reflect.DeepEqual(locs[0].Ranges, newer) {
		t.Fatalf("got %d stored ranges, want exactly the %d newest spans", len(locs[0].Ranges), len(newer))
	}
	if locs := lookupXorb(t, c, xorbA, old[0]); len(locs) != 0 {
		t.Fatalf("got locations = %+v for a replaced old span, want none", locs)
	}
}

// TestXorbsCountTowardItemLimit: xorb adds share the 8192-item request cap.
func TestXorbsCountTowardItemLimit(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	adds := make([]tracker.XorbAdd, 8193)
	for i := range adds {
		adds[i] = tracker.XorbAdd{Hash: xorbA, Ranges: []tracker.ChunkSpan{{Start: 0, End: 1}}}
	}
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: "http://a:8880",
		Adds: tracker.AnnounceSet{Xorbs: adds},
	})
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("got announce error = %v, want status 400 for too many entries", err)
	}
}
