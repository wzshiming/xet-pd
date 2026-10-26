package peer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

const (
	announceTimeout = 5 * time.Second
	maxReplyBytes   = 4096
)

// announcer reconciles the gateway's view with the inventory.
type announcer struct {
	p       *Peer
	control *http.Client  // carries the secret and never follows redirects
	wake    chan struct{} // coalesced inventory change signal
	ready   atomic.Bool   // the gateway took an announce without asking to resync; never cleared

	// Only run touches these.
	acked                map[string]struct{} // inventory the gateway confirmed
	full, failed, warned bool                // full: the next request carries the whole inventory
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
			// The gateway may have applied this request, so only a Full can resync it.
			a.failed, a.full = true, true
		} else {
			a.failed = false
			a.acked, a.full = snap, out.Resync
			if out.Resync {
				p.metrics.announce("resync")
			} else {
				p.metrics.announce("ok")
				a.ready.Store(true)
			}
			if !a.warned && out.TTL > 0 && p.cfg.Heartbeat >= time.Duration(out.TTL)*time.Second {
				slog.Warn("peer: heartbeat not shorter than gateway TTL", "heartbeat", p.cfg.Heartbeat, "ttl", out.TTL)
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

// snapshot copies the inventory under p.mu.
func (a *announcer) snapshot() map[string]struct{} {
	p := a.p
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-a.wake: // changes before this snapshot are covered by it
	default:
	}
	snap := make(map[string]struct{}, p.xorbs.Len())
	for e := range p.xorbs.Oldest() {
		snap[e.Key.String()] = struct{}{}
	}
	return snap
}

// request announces snap whole when full, else as its delta from acked.
func (a *announcer) request(snap map[string]struct{}) apis.AnnounceRequest {
	p := a.p
	req := apis.AnnounceRequest{Peer: apis.Peer{ID: p.id, URL: p.cfg.Advertise, Node: p.cfg.Node}, Epoch: p.epoch, Full: a.full}
	for h := range snap {
		if _, ok := a.acked[h]; a.full || !ok {
			req.Add = append(req.Add, h)
		}
	}
	for h := range a.acked {
		if _, ok := snap[h]; !ok && !a.full {
			req.Remove = append(req.Remove, h)
		}
	}
	slices.Sort(req.Add)
	slices.Sort(req.Remove)
	return req
}

// post sends req to the gateway and decodes its reply.
func (a *announcer) post(req apis.AnnounceRequest) (apis.AnnounceResponse, error) {
	p := a.p
	var out apis.AnnounceResponse
	ctx, cancel := context.WithTimeout(p.ctx, announceTimeout)
	defer cancel()
	resp, err := apis.PostJSON(ctx, a.control, p.cfg.Gateway+apis.AnnouncePath, p.cfg.Secret, req)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return out, fmt.Errorf("gateway status %s", resp.Status)
	}
	if derr := apis.DecodeJSON(resp.Body, maxReplyBytes, &out); derr != nil {
		return out, fmt.Errorf("gateway reply: %w", derr)
	}
	return out, nil
}

// kick wakes the announcer without blocking; safe under p.mu and after Close.
func (a *announcer) kick() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}
