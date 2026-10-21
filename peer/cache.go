package peer

import (
	"context"
	"log/slog"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/internal/inventory"
	"github.com/wzshiming/xet-pd/internal/lru"
)

// ready is the state of a committed xorb in p.xorbs; guarded by p.mu.
type ready struct {
	pins int   // open readers of the stored file
	pull *pull // readers still streaming from the spool hold pull.refs
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

// validateUnchecked validates the startup scan in its order, admitting the files that pass and deleting the rest; Close stops it before its next verdict.
func (p *Peer) validateUnchecked(xorbs []inventory.Xorb) {
	for _, x := range xorbs {
		if p.ctx.Err() != nil {
			return
		}
		obj, err := p.store.GetXorbReadSeekCloser(p.ctx, x.Hash)
		if err == nil {
			err = xorb.Validate(obj, x.Hash)
			_ = obj.Close()
		}
		p.mu.Lock()
		if p.ctx.Err() != nil { // the read may have failed for the cancellation rather than the file
			p.mu.Unlock()
			return
		}
		_, ok := p.unchecked[x.Hash]
		switch {
		case !ok: // a pull replaced the file meanwhile
		case err == nil:
			delete(p.unchecked, x.Hash)
			p.admit(x.Hash, x.Size, nil)
		default:
			slog.Warn("peer: dropping invalid xorb", "hash", x.Hash, "err", err)
			if p.pulls[x.Hash] != nil {
				p.unchecked[x.Hash] = true // the pull replaces the file, or deletes it if the pull fails
			} else if err := p.discard(x.Hash); err != nil {
				slog.Warn("peer: delete invalid xorb", "hash", x.Hash, "err", err)
			}
		}
		p.mu.Unlock()
	}
}

// discard deletes the file of an unchecked hash, which stays unchecked until the file is gone; caller holds p.mu.
func (p *Peer) discard(hash xet.XorbHash) error {
	if err := p.store.DeleteXorb(context.Background(), hash); err != nil {
		return err
	}
	delete(p.unchecked, hash)
	return nil
}

// evict deletes least recently used objects nobody reads until usage fits; caller holds p.mu.
func (p *Peer) evict() {
	for e := range p.xorbs.Oldest() {
		if p.cfg.XorbCacheSize <= 0 || p.xorbs.Usage() <= p.cfg.XorbCacheSize {
			return
		}
		if e.V.pins == 0 && (e.V.pull == nil || e.V.pull.refs == 0) {
			if err := p.store.DeleteXorb(context.Background(), e.Key); err != nil {
				slog.Warn("peer: evict", "hash", e.Key, "err", err)
			} else {
				p.forget(e)
			}
		}
	}
}

// forget removes a committed xorb from the inventory; caller holds p.mu.
func (p *Peer) forget(e *lru.Entry[xet.XorbHash, ready]) {
	p.xorbs.Remove(e)
	p.kick()
}

// drop forgets e after its file was confirmed missing; a newer generation of the same hash stays.
func (p *Peer) drop(e *lru.Entry[xet.XorbHash, ready]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.xorbs.Get(e.Key) == e {
		p.forget(e)
	}
}

// pin marks hash as in use and most recently used; the file stays until unpin.
func (p *Peer) pin(hash xet.XorbHash) *lru.Entry[xet.XorbHash, ready] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pinLocked(hash)
}

func (p *Peer) pinLocked(hash xet.XorbHash) *lru.Entry[xet.XorbHash, ready] {
	e := p.xorbs.Get(hash)
	if e != nil {
		e.V.pins++
		p.xorbs.Touch(e)
	}
	return e
}

func (p *Peer) unpin(e *lru.Entry[xet.XorbHash, ready]) {
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
