// Package lru indexes xorbs by hash in least-recently-used order and tracks their byte total; callers hold their own lock.
package lru

import (
	"cmp"
	"container/list"
	"context"
	"iter"
	"slices"
	"time"

	"github.com/wzshiming/xet"
)

// Entry is one indexed xorb; V is the caller's per-xorb state.
type Entry[V any] struct {
	Hash xet.XorbHash
	Size int64
	V    V
	elem *list.Element
}

// Index is ready to use as a zero value.
type Index[V any] struct {
	entries map[xet.XorbHash]*list.Element
	order   list.List // front is least recently used
	usage   int64
}

func (x *Index[V]) Len() int { return len(x.entries) }

// Usage is the sum of Size over the indexed entries.
func (x *Index[V]) Usage() int64 { return x.usage }

// Get returns the entry indexed under hash, nil when absent; it does not change the order.
func (x *Index[V]) Get(hash xet.XorbHash) *Entry[V] {
	if el := x.entries[hash]; el != nil {
		return el.Value.(*Entry[V])
	}
	return nil
}

// Add indexes hash as the most recently used entry; an entry already indexed under hash is removed first.
func (x *Index[V]) Add(hash xet.XorbHash, size int64, v V) *Entry[V] {
	if old := x.Get(hash); old != nil {
		x.Remove(old)
	}
	if x.entries == nil {
		x.entries = map[xet.XorbHash]*list.Element{}
	}
	e := &Entry[V]{Hash: hash, Size: size, V: v}
	e.elem = x.order.PushBack(e)
	x.entries[hash] = e.elem
	x.usage += size
	return e
}

// Touch marks e most recently used; a removed or replaced e is ignored.
func (x *Index[V]) Touch(e *Entry[V]) {
	if x.entries[e.Hash] == e.elem {
		x.order.MoveToBack(e.elem)
	}
}

// Remove unindexes e; a removed or replaced e is ignored, so a replacement under the same hash stays.
func (x *Index[V]) Remove(e *Entry[V]) {
	if x.entries[e.Hash] == e.elem {
		x.order.Remove(e.elem)
		delete(x.entries, e.Hash)
		x.usage -= e.Size
	}
}

// Oldest yields the entries least recently used first; the loop body may Remove the entry it was handed.
func (x *Index[V]) Oldest() iter.Seq[*Entry[V]] {
	return func(yield func(*Entry[V]) bool) {
		for el := x.order.Front(); el != nil; {
			e := el.Value.(*Entry[V])
			el = el.Next()
			if !yield(e) {
				return
			}
		}
	}
}

// Walker is the part of xet's local storage that lists stored xorbs.
type Walker interface {
	WalkXorbs(ctx context.Context, fn func(hash string, size int64, modTime time.Time) error) error
}

type Xorb struct {
	Hash xet.XorbHash
	Size int64
}

// Scan lists w's xorbs least recently modified first, ties by hash string; names that xet.ParseXorbHash rejects are skipped; a walk error is returned as is.
func Scan(ctx context.Context, w Walker) ([]Xorb, error) {
	type found struct {
		Xorb
		modTime time.Time
	}
	var objs []found
	err := w.WalkXorbs(ctx, func(name string, size int64, modTime time.Time) error {
		if hash, err := xet.ParseXorbHash(name); err == nil {
			objs = append(objs, found{Xorb{hash, size}, modTime})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(objs, func(a, b found) int {
		return cmp.Or(a.modTime.Compare(b.modTime), cmp.Compare(a.Hash.String(), b.Hash.String()))
	})
	xorbs := make([]Xorb, len(objs))
	for i, o := range objs {
		xorbs[i] = o.Xorb
	}
	return xorbs, nil
}
