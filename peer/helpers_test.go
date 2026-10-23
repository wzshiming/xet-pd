package peer

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/internal/clustertest"
	"github.com/wzshiming/xet-pd/internal/lru"
)

func stored(t testing.TB, p *Peer, h xet.XorbHash) bool {
	t.Helper()
	return clustertest.HasXorb(t, p.store, h)
}

// deleteStored loses h's file underneath p, whose inventory still lists it.
func deleteStored(t testing.TB, p *Peer, h xet.XorbHash) {
	t.Helper()
	if err := p.store.DeleteXorb(context.Background(), h); err != nil {
		t.Fatal(err)
	}
}

// commitDirect stores body underneath a running peer and registers it as a finished pull would.
func commitDirect(t *testing.T, p *Peer, hash xet.XorbHash, body []byte) {
	t.Helper()
	if _, err := p.store.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.admit(hash, int64(len(body)), nil)
	p.mu.Unlock()
}

func spoolEmpty(p *Peer) func() bool {
	return func() bool {
		left, _ := filepath.Glob(filepath.Join(p.spoolDir, "*"))
		return len(left) == 0
	}
}

// configOf returns the config as New normalized it, fixed since, so read without p.mu.
func configOf(p *Peer) Config { return p.cfg }

func peerID(p *Peer) string { return p.id }

func peerEpoch(p *Peer) string { return p.epoch }

func usage(p *Peer) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.xorbs.Usage()
}

func admitted(p *Peer, hash xet.XorbHash) func() bool {
	return func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.xorbs.Get(hash) != nil
	}
}

func pinsOf(p *Peer, e *lru.Entry[xet.XorbHash, ready]) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return e.V.pins
}

// settled waits until no pull, spool or pinned reader is left.
func settled(t *testing.T, p *Peer) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "readers released", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for e := range p.xorbs.Oldest() {
			if e.V.pins != 0 {
				return false
			}
		}
		return len(p.pulls) == 0
	})
}

func noPulls(p *Peer) func() bool {
	return func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.pulls) == 0
	}
}

func pullOf(p *Peer, hash xet.XorbHash) *pull {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pulls[hash]
}

// pullStatus snapshots h's listed pull under a single hold of p.mu; all zero when none is listed.
func pullStatus(p *Peer, h xet.XorbHash) (f *pull, refs int, queued, keep bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f = p.pulls[h]; f != nil {
		refs, queued, keep = f.refs, f.queued, f.keep
	}
	return f, refs, queued, keep
}

// pullDone takes no lock: done is set when the pull is made and only closed after.
func pullDone(f *pull) <-chan struct{} { return f.done }

// plantPull lists a bare pull for h, which nothing runs or releases, until the test ends.
func plantPull(t testing.TB, p *Peer, h xet.XorbHash) *pull {
	t.Helper()
	f := &pull{}
	p.mu.Lock()
	p.pulls[h] = f
	p.mu.Unlock()
	t.Cleanup(func() {
		p.mu.Lock()
		delete(p.pulls, h)
		p.mu.Unlock()
	})
	return f
}

// cooldownOf reads h's cooldown entry under p.mu; ok holds for a listed entry, expired or not.
func cooldownOf(p *Peer, h xet.XorbHash) (err error, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.cooldown[h]
	return c.err, ok
}

func wakePending(p *Peer) bool { return len(p.ann.wake) != 0 }

// wakeAnnouncer kicks under p.mu, as the production callers do.
func wakeAnnouncer(p *Peer) {
	p.mu.Lock()
	p.ann.kick()
	p.mu.Unlock()
}
