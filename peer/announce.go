package peer

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

const announceTimeout = 5 * time.Second

// announcer reconciles the tracker's view with the inventory.
type announcer struct {
	p        *Peer
	control  *http.Client  // carries the secret and never follows redirects
	wake     chan struct{} // coalesced inventory change signal
	ready    atomic.Bool   // the tracker took an announce without asking to resync; never cleared
	minSeeds atomic.Int64  // MinSeeds of the last successful reply carrying a positive one, 1 until then

	lastUpstreams atomic.Pointer[[]apis.Peer] // Upstreams of the last successful reply, nil before it

	// Only run touches these.
	acked                holdings // inventory the tracker confirmed
	full, failed, warned bool     // full: the next request carries the whole inventory
}

// holdings is what an announce reports: the cached xorbs and the known shards, by hash.
type holdings struct {
	xorbs  map[string]struct{}
	shards map[string]apis.File
}

// run announces at start, on every inventory change and on every heartbeat until p.ctx ends.
func (a *announcer) run() {
	p := a.p
	tick := time.NewTicker(p.cfg.Heartbeat)
	defer tick.Stop()
	for {
		wasFull := a.full
		snap := a.snapshot()
		out, err := a.post(a.request(snap))
		if err != nil {
			p.metrics.announce("error")
			if !a.failed {
				slog.Warn("peer: announce", "err", err)
			}
			// The tracker may have applied this request, so only a Full can resync it.
			a.failed, a.full = true, true
		} else {
			a.failed = false
			a.acked, a.full = snap, out.Resync
			a.lastUpstreams.Store(&out.Upstreams)
			if out.MinSeeds > 0 && a.minSeeds.Swap(int64(out.MinSeeds)) != int64(out.MinSeeds) {
				p.kickEviction()
			}
			if out.Resync {
				p.metrics.announce("resync")
			} else {
				p.metrics.announce("ok")
				a.ready.Store(true)
			}
			if !a.warned && out.TTL > 0 && p.cfg.Heartbeat >= time.Duration(out.TTL)*time.Second {
				slog.Warn("peer: heartbeat not shorter than tracker TTL", "heartbeat", p.cfg.Heartbeat, "ttl", out.TTL)
				a.warned = true
			}
			// A resync to a delta is answered at once; a resync to a Full waits for the heartbeat.
			if out.Resync && !wasFull && p.ctx.Err() == nil {
				continue
			}
		}
		select {
		case <-p.ctx.Done():
			return
		case <-tick.C:
		case <-a.wake:
		}
	}
}

// snapshot copies the cached xorbs and the known shards under p.mu.
func (a *announcer) snapshot() holdings {
	p := a.p
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-a.wake: // changes before this snapshot are covered by it
	default:
	}
	snap := holdings{xorbs: make(map[string]struct{}, p.xorbs.Len()), shards: maps.Clone(p.shards)}
	for e := range p.xorbs.Oldest() {
		snap.xorbs[e.Key.String()] = struct{}{}
	}
	return snap
}

// request announces snap whole when full, else as its delta from acked.
func (a *announcer) request(snap holdings) apis.AnnounceRequest {
	p := a.p
	req := apis.AnnounceRequest{Peer: apis.Peer{ID: p.id, URL: p.cfg.Advertise, Upstream: p.cfg.Upstream != ""}, Epoch: p.epoch, Full: a.full}
	req.Add, req.Remove = delta(snap.xorbs, a.acked.xorbs, a.full)
	var added []string
	added, req.RemoveShards = delta(snap.shards, a.acked.shards, a.full)
	for _, h := range added {
		req.AddShards = append(req.AddShards, snap.shards[h])
	}
	return req
}

// delta lists, sorted, the keys of snap that acked lacks, all of them when full, and the keys of acked that snap lacks, none when full.
func delta[V any](snap, acked map[string]V, full bool) (add, remove []string) {
	for k := range snap {
		if _, ok := acked[k]; full || !ok {
			add = append(add, k)
		}
	}
	for k := range acked {
		if _, ok := snap[k]; !ok && !full {
			remove = append(remove, k)
		}
	}
	slices.Sort(add)
	slices.Sort(remove)
	return add, remove
}

// post sends req to the tracker and decodes its reply.
func (a *announcer) post(req apis.AnnounceRequest) (apis.AnnounceResponse, error) {
	ctx, cancel := context.WithTimeout(a.p.ctx, announceTimeout)
	defer cancel()
	return apis.Announce(ctx, a.control, a.p.cfg.Tracker, a.p.cfg.Secret, req)
}

// upstreams returns a copy of the Upstreams of the last successful reply, none before the first.
func (a *announcer) upstreams() []apis.Peer {
	if ups := a.lastUpstreams.Load(); ups != nil {
		return slices.Clone(*ups)
	}
	return nil
}

// kick wakes the announcer without blocking; safe under p.mu and after Close.
func (a *announcer) kick() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}
