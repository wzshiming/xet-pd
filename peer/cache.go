package peer

import (
	"cmp"
	"container/list"
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// ready is a committed xorb; all fields are guarded by p.mu.
type ready struct {
	hash xet.XorbHash
	size int64
	pins int   // open readers of the stored file
	fill *fill // readers still streaming from the spool hold fill.refs
}

// scan indexes the committed xorbs oldest first, deleting any that fail validation.
func (p *Peer) scan() error {
	type found struct {
		ready
		mtime time.Time
	}
	var objs []found
	ctx := context.Background()
	err := p.store.WalkXorbs(ctx, func(name string, size int64, mtime time.Time) error {
		hash, err := xet.ParseXorbHash(name)
		if err != nil {
			return nil
		}
		if err := p.validateStored(ctx, hash); err != nil {
			slog.Warn("peer: dropping invalid xorb", "hash", name, "err", err)
			return p.store.DeleteXorb(ctx, hash)
		}
		objs = append(objs, found{ready{hash: hash, size: size}, mtime})
		return nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(objs, func(a, b found) int {
		return cmp.Or(a.mtime.Compare(b.mtime), cmp.Compare(a.hash.String(), b.hash.String()))
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, o := range objs {
		p.admit(o.hash, o.size, nil)
	}
	p.evict()
	return nil
}

func (p *Peer) validateStored(ctx context.Context, hash xet.XorbHash) error {
	obj, err := p.store.GetXorbReadSeekCloser(ctx, hash)
	if err != nil {
		return err
	}
	defer func() { _ = obj.Close() }()
	return xorb.Validate(obj, hash)
}

// admit records a committed xorb as most recently used and trims the cache; caller holds p.mu.
func (p *Peer) admit(hash xet.XorbHash, size int64, f *fill) {
	if e := p.index[hash]; e != nil {
		if f != nil {
			e.Value.(*ready).fill = f
		}
		p.lru.MoveToBack(e)
		return
	}
	p.index[hash] = p.lru.PushBack(&ready{hash: hash, size: size, fill: f})
	p.usage += size
	p.notify()
	p.evict()
}

// evict deletes least recently used objects nobody reads until usage fits; caller holds p.mu.
func (p *Peer) evict() {
	for e := p.lru.Front(); e != nil && p.cfg.CacheSize > 0 && p.usage > p.cfg.CacheSize; {
		next := e.Next()
		r := e.Value.(*ready)
		if r.pins == 0 && (r.fill == nil || r.fill.refs == 0) {
			if err := p.store.DeleteXorb(context.Background(), r.hash); err != nil {
				slog.Warn("peer: evict", "hash", r.hash, "err", err)
			} else {
				p.forget(e)
			}
		}
		e = next
	}
}

// forget removes a committed xorb from the inventory; caller holds p.mu.
func (p *Peer) forget(e *list.Element) {
	r := e.Value.(*ready)
	p.lru.Remove(e)
	delete(p.index, r.hash)
	p.usage -= r.size
	p.notify()
}

// drop forgets r after its file was confirmed missing; a newer generation of the same hash stays.
func (p *Peer) drop(r *ready) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.index[r.hash]; e != nil && e.Value.(*ready) == r {
		p.forget(e)
	}
}

// pin marks hash as in use and most recently used; the file stays until unpin.
func (p *Peer) pin(hash xet.XorbHash) *ready {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pinLocked(hash)
}

func (p *Peer) pinLocked(hash xet.XorbHash) *ready {
	e := p.index[hash]
	if e == nil {
		return nil
	}
	r := e.Value.(*ready)
	r.pins++
	p.lru.MoveToBack(e)
	return r
}

func (p *Peer) unpin(r *ready) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r.pins--
	p.evict()
}

// notify wakes the announcer without blocking; safe under p.mu and after Close.
func (p *Peer) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// inventory snapshots the committed hashes.
func (p *Peer) inventory() map[string]struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.wake: // changes before this snapshot are covered by it
	default:
	}
	out := make(map[string]struct{}, len(p.index))
	for h := range p.index {
		out[h.String()] = struct{}{}
	}
	return out
}
