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

// restore admits the stored xorbs oldest first, deleting invalid ones; local.Storage only honours ctx in WalkXorbs.
func (p *Peer) restore() error {
	xorbs, err := inventory.Scan(p.ctx, p.store)
	if err != nil {
		return err
	}
	valid := xorbs[:0]
	for _, x := range xorbs {
		if err := p.verify(x.Hash); err != nil {
			slog.Warn("peer: dropping invalid xorb", "hash", x.Hash, "err", err)
			if err := p.store.DeleteXorb(context.Background(), x.Hash); err != nil {
				return err
			}
			continue
		}
		valid = append(valid, x)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, x := range valid {
		p.admit(x.Hash, x.Size, nil)
	}
	p.evict()
	return nil
}

// verify validates the stored copy of hash.
func (p *Peer) verify(hash xet.XorbHash) error {
	obj, err := p.store.GetXorbReadSeekCloser(context.Background(), hash)
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
	p.ann.kick()
	p.evict()
}

// evict deletes least recently used objects nobody reads until usage fits; caller holds p.mu.
func (p *Peer) evict() {
	for e := range p.xorbs.Oldest() {
		if p.cfg.XorbCacheSize <= 0 || p.xorbs.Usage() <= p.cfg.XorbCacheSize {
			return
		}
		if e.V.pins == 0 && (e.V.pull == nil || e.V.pull.refs == 0) {
			// Unlinking under p.mu keeps a pin or acquire from slipping in between this check and the delete.
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
	p.ann.kick()
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
