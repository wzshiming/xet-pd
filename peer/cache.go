package peer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
)

// lookupTimeout bounds the tracker lookup of an eviction pass.
const lookupTimeout = 5 * time.Second

// ready is the state of a committed xorb in p.xorbs; the store keeps its pins.
type ready struct{}

// restore registers the known files, then admits the stored xorbs oldest first, deleting invalid ones; local.Storage only honours ctx in WalkXorbs.
func (p *Peer) restore() error {
	xorbs, files, err := p.st.inventory(p.ctx)
	if err != nil {
		return err
	}
	for _, f := range files {
		p.learn(f.File, f.Xorbs)
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
	p.trim()
}

// touch marks a stored xorb most recently used.
func (p *Peer) touch(hash xet.XorbHash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.xorbs.Get(hash); e != nil {
		p.xorbs.Touch(e)
	}
}

// learn records the xorbs file references as the store reported them, replacing an earlier report.
func (p *Peer) learn(file apis.File, xorbs []xet.XorbHash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.files[file.Hash].Xorbs {
		delete(p.xorbFiles[h], file.Hash)
		if len(p.xorbFiles[h]) == 0 {
			delete(p.xorbFiles, h)
		}
	}
	p.files[file.Hash] = fileEntry{file, xorbs}
	for _, h := range xorbs {
		if p.xorbFiles[h] == nil {
			p.xorbFiles[h] = map[string]struct{}{}
		}
		p.xorbFiles[h][file.Hash] = struct{}{}
	}
	p.ann.kick()
}

// markSeed makes the copies of the xorbs fileHash references, known now or later, seed copies.
func (p *Peer) markSeed(fileHash string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seeds[fileHash] = struct{}{}
}

// lost forgets hash once a read found its bytes gone underneath; a copy stored again since that read stays.
func (p *Peer) lost(hash xet.XorbHash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.xorbs.Get(hash)
	if e == nil {
		return
	}
	if ok, err := p.st.Storage.HasXorb(context.Background(), hash); err != nil || ok {
		return
	}
	p.xorbs.Remove(e)
	p.ann.kick()
}

// retryEviction trims the cache once some xorb lost its last pin.
func (p *Peer) retryEviction() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trim()
}

// trim evicts what needs no tracker answer and wakes the evictor for the seed copies left over the cap; caller holds p.mu.
func (p *Peer) trim() {
	if len(p.evict(nil)) > 0 {
		p.kickEviction()
	}
}

// kickEviction wakes the evictor without blocking; safe under p.mu and after Close.
func (p *Peer) kickEviction() {
	select {
	case p.evictions <- struct{}{}:
	default:
	}
}

// evictor runs an eviction pass on every wake until p.ctx ends.
func (p *Peer) evictor() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.evictions:
			p.evictPass()
		}
	}
}

// evictPass asks the tracker in one lookup, made without p.mu, about the seed copies over the cap, then evicts with its answer; a cache left over the cap is logged once.
func (p *Peer) evictPass() {
	p.mu.Lock()
	seeds := p.evict(nil)
	p.mu.Unlock()
	if len(seeds) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, lookupTimeout)
	resp, err := p.st.lookup(ctx, apis.LookupRequest{Xorbs: seeds})
	cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evict(resp.Xorbs)
	if !p.fits() && !p.warned {
		slog.Warn("peer: xorbs over the cache size, nothing evictable yet", "usage", p.xorbs.Usage(), "limit", p.cfg.XorbCacheSize, "err", err)
		p.warned = true
	}
}

// fits reports whether the cached bytes are within XorbCacheSize; caller holds p.mu.
func (p *Peer) fits() bool {
	return p.cfg.XorbCacheSize <= 0 || p.xorbs.Usage() <= p.cfg.XorbCacheSize
}

// evict deletes copies oldest first until usage fits, skipping those the store refuses and seed copies held lists with fewer than MinSeeds other holders; it returns the seed copies it kept unless usage fits; caller holds p.mu.
func (p *Peer) evict(held map[string][]apis.Peer) []string {
	minSeeds := int(p.ann.minSeeds.Load())
	var seeds []string
	for e := range p.xorbs.Oldest() {
		if p.fits() {
			break
		}
		var seed bool
		for f := range p.xorbFiles[e.Key] {
			_, marked := p.seeds[f]
			seed = seed || marked
		}
		if h := e.Key.String(); seed && len(held[h]) < minSeeds {
			seeds = append(seeds, h)
			continue
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
	if p.fits() {
		p.warned = false
		return nil
	}
	return seeds
}
