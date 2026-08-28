package agent

import (
	"fmt"
	"testing"

	"github.com/wzshiming/xet-pd/tracker"
)

// TestQuarantineAddReportsRegistration pins the add contract the peer serve
// path is gated on: registered (or already registered) items report true,
// while a full file or a full registry refuses new items instead of
// silently evicting entries a live flight still needs.
func TestQuarantineAddReportsRegistration(t *testing.T) {
	item := quarItem{xorbHash: "aa", span: tracker.ChunkSpan{Start: 0, End: 1}, peer: "http://p1"}

	t.Run("dedup and cross-file registration", func(t *testing.T) {
		q := newQuarantine()
		if !q.add("file-1", item) {
			t.Fatal("got fresh add = false, want true")
		}
		if !q.add("file-1", item) {
			t.Fatal("got duplicate add = false, want true (no-op success)")
		}
		if !q.add("file-2", item) {
			t.Fatal("got second-file add of the same segment = false, want true")
		}
		if got := len(q.take("file-1")); got != 1 {
			t.Fatalf("got %d items after duplicate adds, want 1", got)
		}
		if got := len(q.take("file-2")); got != 1 {
			t.Fatalf("got %d items under the second file, want 1", got)
		}
	})

	t.Run("item cap refuses without eviction", func(t *testing.T) {
		q := newQuarantine()
		for i := 0; i < maxQuarantineItems; i++ {
			it := quarItem{xorbHash: fmt.Sprintf("%064d", i), span: tracker.ChunkSpan{Start: 0, End: 1}}
			if !q.add("file", it) {
				t.Fatalf("got add %d = false while filling the file, want true", i)
			}
		}
		if q.add("file", quarItem{xorbHash: "overflow", span: tracker.ChunkSpan{Start: 0, End: 1}}) {
			t.Fatal("got add past the item cap = true, want refusal")
		}
		if got := len(q.take("file")); got != maxQuarantineItems {
			t.Fatalf("got %d items after refused overflow, want %d intact", got, maxQuarantineItems)
		}
	})

	t.Run("file cap refuses new files only", func(t *testing.T) {
		q := newQuarantine()
		for i := 0; i < maxQuarantineFiles; i++ {
			if !q.add(fmt.Sprintf("file-%d", i), item) {
				t.Fatalf("got add for file %d = false while filling the registry, want true", i)
			}
		}
		if q.add("file-new", item) {
			t.Fatal("got add for a new file past the cap = true, want refusal")
		}
		other := quarItem{xorbHash: "bb", span: tracker.ChunkSpan{Start: 1, End: 2}, peer: "http://p2"}
		if !q.add("file-0", other) {
			t.Fatal("got add for an existing file at the cap = false, want true")
		}
		if got := len(q.take("file-0")); got != 2 {
			t.Fatalf("got %d items for the oldest file after the cap, want 2 (never evicted)", got)
		}
	})

	t.Run("multi-add is all or nothing", func(t *testing.T) {
		q := newQuarantine()
		for i := 0; i < maxQuarantineItems-1; i++ {
			it := quarItem{xorbHash: fmt.Sprintf("%064d", i), span: tracker.ChunkSpan{Start: 0, End: 1}}
			if !q.add("file", it) {
				t.Fatalf("got add %d = false while filling the file, want true", i)
			}
		}
		// One free slot, two fresh items: neither may land.
		a := quarItem{xorbHash: "fresh-a", span: tracker.ChunkSpan{Start: 0, End: 1}}
		b := quarItem{xorbHash: "fresh-b", span: tracker.ChunkSpan{Start: 0, End: 1}}
		if q.add("file", a, b) {
			t.Fatal("got multi-add past the item cap = true, want atomic refusal")
		}
		if got := len(q.take("file")); got != maxQuarantineItems-1 {
			t.Fatalf("got %d items after refused multi-add, want %d (none inserted)", got, maxQuarantineItems-1)
		}
	})

	t.Run("multi-add dedups within the batch and against existing", func(t *testing.T) {
		q := newQuarantine()
		other := quarItem{xorbHash: "bb", span: tracker.ChunkSpan{Start: 1, End: 2}, peer: "http://p2"}
		if !q.add("file", item) {
			t.Fatal("got seeding add = false, want true")
		}
		if !q.add("file", item, other, other) {
			t.Fatal("got deduplicating multi-add = false, want true")
		}
		if got := len(q.take("file")); got != 2 {
			t.Fatalf("got %d items after deduplicating multi-add, want 2", got)
		}
	})
}
