package peer

import (
	"context"
	"log/slog"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/internal/lru"
)

// ready is the state of a committed xorb in p.xorbs; guarded by p.mu.
type ready struct {
	pins int   // open readers of the stored file
	pull *pull // readers still streaming from the spool hold pull.refs
}

// scan indexes the committed xorbs oldest first, deleting any that fail validation.
func (p *Peer) scan() error {
	ctx := context.Background()
	xorbs, err := lru.Scan(ctx, p.store)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, x := range xorbs {
		if err := p.validateStored(ctx, x.Hash); err != nil {
			slog.Warn("peer: dropping invalid xorb", "hash", x.Hash, "err", err)
			if err := p.store.DeleteXorb(ctx, x.Hash); err != nil {
				return err
			}
			continue
		}
		p.admit(x.Hash, x.Size, nil)
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
func (p *Peer) admit(hash xet.XorbHash, size int64, f *pull) {
	if e := p.xorbs.Get(hash); e != nil {
		if f != nil {
			e.V.pull = f
		}
		p.xorbs.Touch(e)
		return
	}
	p.xorbs.Add(hash, size, ready{pull: f})
	p.kick()
	p.evict()
}

// evict deletes least recently used objects nobody reads until usage fits; caller holds p.mu.
func (p *Peer) evict() {
	for e := range p.xorbs.Oldest() {
		if p.cfg.XorbCacheSize <= 0 || p.xorbs.Usage() <= p.cfg.XorbCacheSize {
			return
		}
		if e.V.pins == 0 && (e.V.pull == nil || e.V.pull.refs == 0) {
			if err := p.store.DeleteXorb(context.Background(), e.Hash); err != nil {
				slog.Warn("peer: evict", "hash", e.Hash, "err", err)
			} else {
				p.forget(e)
			}
		}
	}
}

// forget removes a committed xorb from the inventory; caller holds p.mu.
func (p *Peer) forget(e *lru.Entry[ready]) {
	p.xorbs.Remove(e)
	p.kick()
}

// drop forgets e after its file was confirmed missing; a newer generation of the same hash stays.
func (p *Peer) drop(e *lru.Entry[ready]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.xorbs.Get(e.Hash) == e {
		p.forget(e)
	}
}

// pin marks hash as in use and most recently used; the file stays until unpin.
func (p *Peer) pin(hash xet.XorbHash) *lru.Entry[ready] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pinLocked(hash)
}

func (p *Peer) pinLocked(hash xet.XorbHash) *lru.Entry[ready] {
	e := p.xorbs.Get(hash)
	if e != nil {
		e.V.pins++
		p.xorbs.Touch(e)
	}
	return e
}

func (p *Peer) unpin(e *lru.Entry[ready]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e.V.pins--
	p.evict()
}

// kick wakes the announcer without blocking; safe under p.mu and after Close.
func (p *Peer) kick() {
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
	out := make(map[string]struct{}, p.xorbs.Len())
	for e := range p.xorbs.Oldest() {
		out[e.Hash.String()] = struct{}{}
	}
	return out
}
