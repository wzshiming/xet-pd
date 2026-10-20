package lru_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/internal/lru"
)

// testIndex adds an entry per name, oldest first: the i-th has hash {i+1}, Size i+1 and V name.
func testIndex(names ...string) (*lru.Index[string], []*lru.Entry[string]) {
	x := new(lru.Index[string])
	es := make([]*lru.Entry[string], len(names))
	for i, name := range names {
		es[i] = x.Add(xet.XorbHash{byte(i + 1)}, int64(i+1), name)
	}
	return x, es
}

// testOrder lists the V of x's entries least recently used first.
func testOrder(x *lru.Index[string]) []string {
	var vs []string
	for e := range x.Oldest() {
		vs = append(vs, e.V)
	}
	return vs
}

func TestZeroIndexIsEmptyAndUsable(t *testing.T) {
	var x lru.Index[string]
	h := xet.XorbHash{1}
	if x.Len() != 0 || x.Usage() != 0 || x.Get(h) != nil || len(testOrder(&x)) != 0 {
		t.Fatalf("zero Index: Len %d, Usage %d, Get %v, Oldest %v, want an empty index", x.Len(), x.Usage(), x.Get(h), testOrder(&x))
	}
	if e := x.Add(h, 1, "a"); e == nil || x.Get(h) != e || x.Len() != 1 {
		t.Errorf("Add on the zero Index: Get %v, Len %d, want the added entry, 1", x.Get(h), x.Len())
	}
}

func TestAddIndexesInOrder(t *testing.T) {
	x, es := testIndex("a", "b", "c")
	if x.Len() != 3 || x.Usage() != 6 {
		t.Errorf("Len %d, Usage %d, want 3, 6", x.Len(), x.Usage())
	}
	for i, name := range []string{"a", "b", "c"} {
		h := xet.XorbHash{byte(i + 1)}
		if e := x.Get(h); e == nil || e != es[i] || e.Hash != h || e.Size != int64(i+1) || e.V != name {
			t.Errorf("Get(%s) = %+v, want the entry Add returned, Size %d, V %q", name, e, i+1, name)
		}
	}
	if got := testOrder(x); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("Oldest = %v, want [a b c]", got)
	}
}

func TestGetKeepsOrder(t *testing.T) {
	x, _ := testIndex("a", "b")
	x.Get(xet.XorbHash{1})
	if got := testOrder(x); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("after Get(a) Oldest = %v, want [a b]", got)
	}
}

func TestTouchMarksMostRecent(t *testing.T) {
	x, es := testIndex("a", "b", "c")
	x.Touch(es[0])
	if got := testOrder(x); !slices.Equal(got, []string{"b", "c", "a"}) {
		t.Errorf("after Touch(a) Oldest = %v, want [b c a]", got)
	}
	x.Touch(es[1])
	if got := testOrder(x); !slices.Equal(got, []string{"c", "a", "b"}) {
		t.Errorf("after Touch(b) Oldest = %v, want [c a b]", got)
	}
}

func TestRemoveUnindexesOnce(t *testing.T) {
	x, es := testIndex("a", "b", "c")
	x.Remove(es[1])
	if x.Len() != 2 || x.Usage() != 4 || x.Get(xet.XorbHash{2}) != nil || !slices.Equal(testOrder(x), []string{"a", "c"}) {
		t.Fatalf("after Remove(b): Len %d, Usage %d, Get(b) %v, Oldest %v, want 2, 4, <nil>, [a c]", x.Len(), x.Usage(), x.Get(xet.XorbHash{2}), testOrder(x))
	}
	x.Remove(es[1])
	x.Touch(es[1])
	if x.Len() != 2 || x.Usage() != 4 || !slices.Equal(testOrder(x), []string{"a", "c"}) {
		t.Errorf("after Remove(b) and Touch(b) again: Len %d, Usage %d, Oldest %v, want 2, 4, [a c]", x.Len(), x.Usage(), testOrder(x))
	}
}

func TestAddReplacesSameHash(t *testing.T) {
	var x lru.Index[string]
	a := xet.XorbHash{1}
	stale := x.Add(a, 10, "v1")
	x.Add(xet.XorbHash{2}, 1, "b")
	fresh := x.Add(a, 20, "v2")
	if fresh == nil || x.Get(a) != fresh || fresh.V != "v2" || x.Len() != 2 || x.Usage() != 21 || !slices.Equal(testOrder(&x), []string{"b", "v2"}) {
		t.Fatalf("after Add(a) again: Get(a) %+v, Len %d, Usage %d, Oldest %v, want the v2 entry, 2, 21, [b v2]", x.Get(a), x.Len(), x.Usage(), testOrder(&x))
	}
	x.Add(xet.XorbHash{3}, 2, "c")
	x.Touch(stale)
	if got := testOrder(&x); !slices.Equal(got, []string{"b", "v2", "c"}) {
		t.Errorf("after Touch(stale v1) Oldest = %v, want [b v2 c]", got)
	}
	x.Remove(stale)
	if x.Get(a) != fresh || x.Len() != 3 || x.Usage() != 23 {
		t.Errorf("after Remove(stale v1): Get(a) %+v, Len %d, Usage %d, want the v2 entry, 3, 23", x.Get(a), x.Len(), x.Usage())
	}
}

func TestOldest(t *testing.T) {
	t.Run("remove_yielded", func(t *testing.T) {
		x, _ := testIndex("a", "b", "c", "d")
		var seen []string
		for e := range x.Oldest() {
			seen = append(seen, e.V)
			if e.Size%2 == 1 {
				x.Remove(e)
			}
		}
		if !slices.Equal(seen, []string{"a", "b", "c", "d"}) || x.Len() != 2 || x.Usage() != 6 || !slices.Equal(testOrder(x), []string{"b", "d"}) {
			t.Errorf("Remove of odd sizes while ranging Oldest: saw %v, then Len %d, Usage %d, Oldest %v, want [a b c d], 2, 6, [b d]", seen, x.Len(), x.Usage(), testOrder(x))
		}
	})
	t.Run("break", func(t *testing.T) {
		x, _ := testIndex("a", "b", "c", "d")
		var seen []string
		for e := range x.Oldest() {
			seen = append(seen, e.V)
			break
		}
		if !slices.Equal(seen, []string{"a"}) || x.Len() != 4 || x.Usage() != 10 || !slices.Equal(testOrder(x), []string{"a", "b", "c", "d"}) {
			t.Errorf("break after the first of Oldest: saw %v, then Len %d, Usage %d, Oldest %v, want [a], 4, 10, [a b c d]", seen, x.Len(), x.Usage(), testOrder(x))
		}
	})
}

type fakeRow struct {
	name  string
	size  int64
	mtime time.Time
}

type fakeCtxKey struct{}

// fakeWalker hands its rows to fn in order, then returns err; ctxValue is what the walk's ctx carried under fakeCtxKey.
type fakeWalker struct {
	rows     []fakeRow
	err      error
	ctxValue any
}

func (w *fakeWalker) WalkXorbs(ctx context.Context, fn func(hash string, size int64, modTime time.Time) error) error {
	w.ctxValue = ctx.Value(fakeCtxKey{})
	for _, r := range w.rows {
		if err := fn(r.name, r.size, r.mtime); err != nil {
			return err
		}
	}
	return w.err
}

func testParse(t *testing.T, s string) xet.XorbHash {
	t.Helper()
	h, err := xet.ParseXorbHash(s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestScan(t *testing.T) {
	at := func(sec int64) time.Time { return time.Unix(1_700_000_000+sec, 0) }
	n1, n3, n4 := strings.Repeat("c", 64), strings.Repeat("b", 64), strings.Repeat("a", 64)
	// lo sorts first as a hash string but last as bytes: xet stores each 16-digit group little-endian.
	lo, hi := strings.Repeat("0", 15)+"1"+strings.Repeat("0", 48), "01"+strings.Repeat("0", 62)

	t.Run("sorted", func(t *testing.T) {
		w := &fakeWalker{rows: []fakeRow{
			{n3, 30, at(3)},
			{hi, 21, at(2)},
			{"not-a-hash", 90, at(0)},
			{n4, 40, at(4)},
			{lo, 20, at(2)},
			{strings.Repeat("1", 63), 91, at(1)},
			{n1, 10, at(1)},
			{strings.Repeat("g", 64), 92, at(5)},
		}}
		want := []lru.Xorb{
			{Hash: testParse(t, n1), Size: 10},
			{Hash: testParse(t, lo), Size: 20},
			{Hash: testParse(t, hi), Size: 21},
			{Hash: testParse(t, n3), Size: 30},
			{Hash: testParse(t, n4), Size: 40},
		}
		if got, err := lru.Scan(t.Context(), w); err != nil || !slices.Equal(got, want) {
			t.Errorf("Scan = %v, %v, want %v by mtime then hash string, invalid names skipped", got, err, want)
		}
	})
	t.Run("walk_error", func(t *testing.T) {
		walkErr := errors.New("walk failed")
		w := &fakeWalker{rows: []fakeRow{{n1, 10, at(1)}}, err: walkErr}
		if got, err := lru.Scan(t.Context(), w); !errors.Is(err, walkErr) || len(got) != 0 {
			t.Errorf("Scan = %v, %v, want no xorbs, %v", got, err, walkErr)
		}
	})
	t.Run("ctx", func(t *testing.T) {
		w := &fakeWalker{}
		ctx := context.WithValue(t.Context(), fakeCtxKey{}, "scan")
		if _, err := lru.Scan(ctx, w); err != nil || w.ctxValue != "scan" {
			t.Errorf("Scan: err %v, WalkXorbs ctx value %v, want <nil>, scan", err, w.ctxValue)
		}
	})
}
