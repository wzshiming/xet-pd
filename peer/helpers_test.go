package peer

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

func stored(t testing.TB, p *Peer, h xet.XorbHash) bool {
	t.Helper()
	return clustertest.HasXorb(t, p.st.Storage, h)
}

// deleteStored loses h's file underneath p, whose inventory still lists it.
func deleteStored(t testing.TB, p *Peer, h xet.XorbHash) {
	t.Helper()
	if err := p.st.Storage.DeleteXorb(context.Background(), h); err != nil {
		t.Fatal(err)
	}
}

// commitDirect stores body underneath a running peer, which admits it as it admits a finished pull.
func commitDirect(t *testing.T, p *Peer, hash xet.XorbHash, body []byte) {
	t.Helper()
	if _, err := p.st.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

func spoolEmpty(p *Peer) func() bool {
	return func() bool {
		left, _ := filepath.Glob(filepath.Join(p.st.spoolDir, "*"))
		return len(left) == 0
	}
}

// storeOf returns the store New built, fixed since, so read without p.mu.
func storeOf(p *Peer) *store { return p.st }

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

func pinsOf(p *Peer, hash xet.XorbHash) int {
	p.st.mu.Lock()
	defer p.st.mu.Unlock()
	return p.st.pins[hash]
}

// settled waits until no pull, spool or pinned reader is left.
func settled(t *testing.T, p *Peer) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "spool cleanup", spoolEmpty(p))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "readers released", func() bool {
		p.st.mu.Lock()
		defer p.st.mu.Unlock()
		return len(p.st.pins) == 0 && len(p.st.pulls) == 0
	})
}

func noPulls(p *Peer) func() bool {
	return func() bool {
		p.st.mu.Lock()
		defer p.st.mu.Unlock()
		return len(p.st.pulls) == 0
	}
}

func pullOf(p *Peer, hash xet.XorbHash) *pull {
	p.st.mu.Lock()
	defer p.st.mu.Unlock()
	return p.st.pulls[hash]
}

// pullStatus snapshots h's listed pull under a single hold of the store's lock; all zero when none is listed.
func pullStatus(p *Peer, h xet.XorbHash) (f *pull, refs int, queued, keep bool) {
	p.st.mu.Lock()
	defer p.st.mu.Unlock()
	if f = p.st.pulls[h]; f != nil {
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
	p.st.mu.Lock()
	p.st.pulls[h] = f
	p.st.mu.Unlock()
	t.Cleanup(func() {
		p.st.mu.Lock()
		delete(p.st.pulls, h)
		p.st.mu.Unlock()
	})
	return f
}

// cooldownOf reads h's cooldown entry under the store's lock; ok holds for a listed entry, expired or not.
func cooldownOf(p *Peer, h xet.XorbHash) (err error, ok bool) {
	p.st.mu.Lock()
	defer p.st.mu.Unlock()
	c, ok := p.st.cooldown[h]
	return c.err, ok
}

// wakeAnnouncer kicks under p.mu, as the production callers do.
func wakeAnnouncer(p *Peer) {
	p.mu.Lock()
	p.ann.kick()
	p.mu.Unlock()
}

type storedEvent struct {
	hash     xet.XorbHash
	size     int64
	inserted bool
}

type fileEvent struct {
	file  apis.File
	xorbs []xet.XorbHash
}

// storeEvents is what a store reported through the hooks watch set.
type storeEvents struct {
	mu     sync.Mutex
	stored []storedEvent
	files  []fileEvent
	reads  []xet.XorbHash
	idles  int
}

// watch records every hook of st; call it before st is shared, as its owner does.
func watch(st *store) *storeEvents {
	ev := &storeEvents{}
	st.onStored = func(hash xet.XorbHash, size int64, inserted bool) {
		ev.mu.Lock()
		defer ev.mu.Unlock()
		ev.stored = append(ev.stored, storedEvent{hash, size, inserted})
	}
	st.onFile = func(file apis.File, xorbs []xet.XorbHash) {
		ev.mu.Lock()
		defer ev.mu.Unlock()
		ev.files = append(ev.files, fileEvent{file, xorbs})
	}
	st.onRead = func(hash xet.XorbHash) {
		ev.mu.Lock()
		defer ev.mu.Unlock()
		ev.reads = append(ev.reads, hash)
	}
	st.onIdle = func() {
		ev.mu.Lock()
		defer ev.mu.Unlock()
		ev.idles++
	}
	return ev
}

func (ev *storeEvents) storedXorbs() []storedEvent {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	return slices.Clone(ev.stored)
}

func (ev *storeEvents) knownFiles() []fileEvent {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	return slices.Clone(ev.files)
}

func (ev *storeEvents) readXorbs() []xet.XorbHash {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	return slices.Clone(ev.reads)
}

func (ev *storeEvents) idleCount() int {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	return ev.idles
}

func sidecarPath(st *store, hash xet.XorbHash) string { return st.sidecar.path(hash) }

func sidecarOffsets(st *store, hash xet.XorbHash) ([]uint64, error) { return st.sidecar.read(hash) }
