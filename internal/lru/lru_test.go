package lru_test

import (
	"slices"
	"testing"

	"github.com/wzshiming/xet-pd/internal/lru"
)

// testIndex adds an entry per name, oldest first: the i-th has key i+1, Size i+1 and V name.
func testIndex(names ...string) (*lru.Index[int, string], []*lru.Entry[int, string]) {
	x := new(lru.Index[int, string])
	es := make([]*lru.Entry[int, string], len(names))
	for i, name := range names {
		es[i] = x.Add(i+1, int64(i+1), name)
	}
	return x, es
}

// testOrder lists the V of x's entries least recently used first.
func testOrder(x *lru.Index[int, string]) []string {
	var vs []string
	for e := range x.Oldest() {
		vs = append(vs, e.V)
	}
	return vs
}

func TestZeroIndexIsEmptyAndUsable(t *testing.T) {
	t.Parallel()
	var x lru.Index[int, string]
	k := 1
	if x.Len() != 0 || x.Usage() != 0 || x.Get(k) != nil || len(testOrder(&x)) != 0 {
		t.Fatalf("zero Index: Len %d, Usage %d, Get %v, Oldest %v, want an empty index", x.Len(), x.Usage(), x.Get(k), testOrder(&x))
	}
	if e := x.Add(k, 1, "a"); e == nil || x.Get(k) != e || x.Len() != 1 {
		t.Errorf("Add on the zero Index: Get %v, Len %d, want the added entry, 1", x.Get(k), x.Len())
	}
}

func TestAddIndexesInOrder(t *testing.T) {
	t.Parallel()
	x, es := testIndex("a", "b", "c")
	if x.Len() != 3 || x.Usage() != 6 {
		t.Errorf("Len %d, Usage %d, want 3, 6", x.Len(), x.Usage())
	}
	for i, name := range []string{"a", "b", "c"} {
		k := i + 1
		if e := x.Get(k); e == nil || e != es[i] || e.Key != k || e.Size != int64(i+1) || e.V != name {
			t.Errorf("Get(%s) = %+v, want the entry Add returned, Size %d, V %q", name, e, i+1, name)
		}
	}
	if got := testOrder(x); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("Oldest = %v, want [a b c]", got)
	}
}

func TestGetKeepsOrder(t *testing.T) {
	t.Parallel()
	x, _ := testIndex("a", "b")
	x.Get(1)
	if got := testOrder(x); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("after Get(a) Oldest = %v, want [a b]", got)
	}
}

func TestTouchMarksMostRecent(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	x, es := testIndex("a", "b", "c")
	x.Remove(es[1])
	if x.Len() != 2 || x.Usage() != 4 || x.Get(2) != nil || !slices.Equal(testOrder(x), []string{"a", "c"}) {
		t.Fatalf("after Remove(b): Len %d, Usage %d, Get(b) %v, Oldest %v, want 2, 4, <nil>, [a c]", x.Len(), x.Usage(), x.Get(2), testOrder(x))
	}
	x.Remove(es[1])
	x.Touch(es[1])
	if x.Len() != 2 || x.Usage() != 4 || !slices.Equal(testOrder(x), []string{"a", "c"}) {
		t.Errorf("after Remove(b) and Touch(b) again: Len %d, Usage %d, Oldest %v, want 2, 4, [a c]", x.Len(), x.Usage(), testOrder(x))
	}
}

func TestAddReplacesSameHash(t *testing.T) {
	t.Parallel()
	var x lru.Index[int, string]
	a := 1
	stale := x.Add(a, 10, "v1")
	x.Add(2, 1, "b")
	fresh := x.Add(a, 20, "v2")
	if fresh == nil || x.Get(a) != fresh || fresh.V != "v2" || x.Len() != 2 || x.Usage() != 21 || !slices.Equal(testOrder(&x), []string{"b", "v2"}) {
		t.Fatalf("after Add(a) again: Get(a) %+v, Len %d, Usage %d, Oldest %v, want the v2 entry, 2, 21, [b v2]", x.Get(a), x.Len(), x.Usage(), testOrder(&x))
	}
	x.Add(3, 2, "c")
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
	t.Parallel()
	t.Run("remove_yielded", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
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
