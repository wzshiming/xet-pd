package peer

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

const (
	announceTimeout = 5 * time.Second
	maxReplyBytes   = 4096
)

// announcer reconciles the gateway's view with the inventory from one goroutine.
type announcer struct {
	p      *Peer
	acked  map[string]struct{} // inventory the gateway confirmed
	full   bool                // next request carries the whole inventory
	failed bool
	warned bool
}

func (p *Peer) announceLoop() {
	defer p.wg.Done()
	a := &announcer{p: p, full: true}
	tick := time.NewTicker(p.cfg.Heartbeat)
	defer tick.Stop()
	for {
		wasFull := a.full
		// A resync to a delta is answered at once; a resync to a Full waits for the heartbeat.
		if a.announce() && !wasFull && p.ctx.Err() == nil {
			continue
		}
		select {
		case <-p.ctx.Done():
			return
		case <-tick.C:
		case <-p.wake:
		}
	}
}

// announce sends the difference to the acked inventory and reports a resync demand.
func (a *announcer) announce() bool {
	p := a.p
	snap := p.inventory()
	req := apis.AnnounceRequest{Peer: apis.Peer{ID: p.id, URL: p.cfg.Advertise}, Epoch: p.epoch, Full: a.full}
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
	resp, err := a.send(req)
	if err != nil {
		if !a.failed {
			slog.Warn("peer: announce", "err", err)
		}
		// The gateway may have applied this request, so only a Full can resync it.
		a.failed, a.full = true, true
		return false
	}
	a.failed = false
	a.acked, a.full = snap, resp.Resync
	if !a.warned && resp.TTL > 0 && p.cfg.Heartbeat >= time.Duration(resp.TTL)*time.Second {
		slog.Warn("peer: heartbeat not shorter than gateway TTL", "heartbeat", p.cfg.Heartbeat, "ttl", resp.TTL)
		a.warned = true
	}
	return resp.Resync
}

func (a *announcer) send(req apis.AnnounceRequest) (apis.AnnounceResponse, error) {
	p := a.p
	var out apis.AnnounceResponse
	ctx, cancel := context.WithTimeout(p.ctx, announceTimeout)
	defer cancel()
	resp, err := apis.PostJSON(ctx, p.control, p.cfg.Gateway+apis.AnnouncePath, p.cfg.Secret, req)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return out, fmt.Errorf("gateway status %s", resp.Status)
	}
	if err := apis.DecodeJSON(resp.Body, maxReplyBytes, &out); err != nil {
		return out, fmt.Errorf("gateway reply: %w", err)
	}
	return out, nil
}
