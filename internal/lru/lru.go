// Package lru indexes entries by key in least-recently-used order and tracks their byte total; callers hold their own lock.
package lru

import (
	"container/list"
	"iter"
)

// Entry is one indexed key; V is the caller's per-key state.
type Entry[K comparable, V any] struct {
	Key  K
	Size int64
	V    V
	elem *list.Element
}

// Index is ready to use as a zero value.
type Index[K comparable, V any] struct {
	entries map[K]*list.Element
	order   list.List // front is least recently used
	usage   int64
}

func (x *Index[K, V]) Len() int { return len(x.entries) }

// Usage is the sum of Size over the indexed entries.
func (x *Index[K, V]) Usage() int64 { return x.usage }

// Get returns the entry indexed under key, nil when absent; it does not change the order.
func (x *Index[K, V]) Get(key K) *Entry[K, V] {
	if el := x.entries[key]; el != nil {
		return el.Value.(*Entry[K, V])
	}
	return nil
}

// Add indexes key as the most recently used entry; an entry already indexed under key is removed first.
func (x *Index[K, V]) Add(key K, size int64, v V) *Entry[K, V] {
	if old := x.Get(key); old != nil {
		x.Remove(old)
	}
	if x.entries == nil {
		x.entries = map[K]*list.Element{}
	}
	e := &Entry[K, V]{Key: key, Size: size, V: v}
	e.elem = x.order.PushBack(e)
	x.entries[key] = e.elem
	x.usage += size
	return e
}

// Touch marks e most recently used; a removed or replaced e is ignored.
func (x *Index[K, V]) Touch(e *Entry[K, V]) {
	x.order.MoveToBack(e.elem)
}

// Remove unindexes e; a removed or replaced e is ignored, so a replacement under the same key stays.
func (x *Index[K, V]) Remove(e *Entry[K, V]) {
	if x.entries[e.Key] == e.elem {
		x.order.Remove(e.elem)
		delete(x.entries, e.Key)
		x.usage -= e.Size
	}
}

// Oldest yields the entries least recently used first; the loop body may Remove the entry it was handed.
func (x *Index[K, V]) Oldest() iter.Seq[*Entry[K, V]] {
	return func(yield func(*Entry[K, V]) bool) {
		for el := x.order.Front(); el != nil; {
			e := el.Value.(*Entry[K, V])
			el = el.Next()
			if !yield(e) {
				return
			}
		}
	}
}
