package peer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// ready is the state of a committed xorb in p.xorbs; the store keeps its pins.
type ready struct{}

// restore admits the stored xorbs oldest first, deleting invalid ones; local.Storage only honours ctx in WalkXorbs.
func (p *Peer) restore() error {
	xorbs, _, err := p.st.inventory(p.ctx)
	if err != nil {
		return err
	}
	valid := xorbs[:0]
	for _, x := range xorbs {
		if err := p.verify(x.Hash); err != nil {
			slog.Warn("peer: dropping invalid xorb", "hash", x.Hash, "err", err)
			if err := p.st.DeleteXorb(context.Background(), x.Hash); err != nil {
				return err
			}
			p.metrics.restoreDropped.Inc()
			continue
		}
		valid = append(valid, x)
	}
	for _, x := range valid {
		p.admit(x.Hash, x.Size, false)
	}
	return nil
}

// verify validates the stored copy of hash.
func (p *Peer) verify(hash xet.XorbHash) error {
	obj, err := p.st.Storage.GetXorbReadSeekCloser(context.Background(), hash)
	if err != nil {
		return err
	}
	defer func() { _ = obj.Close() }()
	return xorb.Validate(obj, hash)
}

// admit records a stored xorb as most recently used and trims the cache; inserted bytes replace the entry of an earlier copy.
func (p *Peer) admit(hash xet.XorbHash, size int64, inserted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.xorbs.Get(hash); e != nil && !inserted {
		p.xorbs.Touch(e)
		return
	}
	p.xorbs.Add(hash, size, ready{})
	p.ann.kick()
	p.evict()
}

// touch marks a stored xorb most recently used.
func (p *Peer) touch(hash xet.XorbHash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.xorbs.Get(hash); e != nil {
		p.xorbs.Touch(e)
	}
}

// evict deletes least recently used objects until usage fits; one the store refuses, pinned by a reader or writer, stays for this pass; caller holds p.mu.
func (p *Peer) evict() {
	for e := range p.xorbs.Oldest() {
		if p.cfg.XorbCacheSize <= 0 || p.xorbs.Usage() <= p.cfg.XorbCacheSize {
			return
		}
		if err := p.st.DeleteXorb(context.Background(), e.Key); err != nil {
			if !errors.Is(err, errPinned) {
				slog.Warn("peer: evict", "hash", e.Key, "err", err)
			}
			continue
		}
		p.xorbs.Remove(e)
		p.ann.kick()
	}
}
