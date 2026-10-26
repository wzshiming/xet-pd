package gateway

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

type peerState struct {
	apis.Peer
	epoch    string
	expiry   time.Time
	holdings map[string]struct{}
}

type ranked struct {
	apis.Peer
	holder bool
	score  uint64
}

type registry struct {
	mu    sync.RWMutex
	ttl   time.Duration
	now   func() time.Time // set before concurrent use
	peers map[string]*peerState
}

func newRegistry(ttl time.Duration) *registry {
	return &registry{ttl: ttl, now: time.Now, peers: map[string]*peerState{}}
}

func (r *registry) announce(req apis.AnnounceRequest) apis.AnnounceResponse {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, p := range r.peers {
		if !now.Before(p.expiry) {
			delete(r.peers, id)
		}
	}
	resp := apis.AnnounceResponse{TTL: int64(r.ttl / time.Second)}
	p := r.peers[req.Peer.ID]
	if p == nil || p.epoch != req.Epoch || p.Peer != req.Peer || req.Full {
		if !req.Full {
			delete(r.peers, req.Peer.ID)
			resp.Resync = true
			return resp
		}
		p = &peerState{Peer: req.Peer, epoch: req.Epoch, holdings: map[string]struct{}{}}
		r.peers[req.Peer.ID] = p
	}
	p.expiry = now.Add(r.ttl)
	for _, h := range req.Add {
		p.holdings[h] = struct{}{}
	}
	for _, h := range req.Remove {
		delete(p.holdings, h)
	}
	return resp
}

// rank lists live peers best first for hash, ties broken by ID.
func (r *registry) rank(hash string) []ranked {
	now := r.now()
	r.mu.RLock()
	list := make([]ranked, 0, len(r.peers))
	for _, p := range r.peers {
		if !now.Before(p.expiry) {
			continue
		}
		_, holder := p.holdings[hash]
		// The score is the rendezvous weight of the peer for hash; the length prefix keeps the pair unambiguous.
		h := fnv.New64a()
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(hash))))
		h.Write([]byte(hash))
		h.Write([]byte(p.ID))
		list = append(list, ranked{Peer: p.Peer, holder: holder, score: h.Sum64()})
	}
	r.mu.RUnlock()
	slices.SortFunc(list, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(b.score, a.score), strings.Compare(a.ID, b.ID))
	})
	return list
}

type pickOutcome uint8

const (
	pickNone pickOutcome = iota
	pickFallback
	pickLocal
)

// pick chooses the peer a client on node fetches hash from: a live peer on that node, holders first then by ID; else the best holder, else the top-ranked peer; pickNone when none is live, and an empty node is local to nothing.
func (r *registry) pick(hash, node string) (apis.Peer, pickOutcome) {
	list := r.rank(hash)
	var local, fallback *ranked
	for i := range list {
		p := &list[i]
		switch {
		case node != "" && p.Node == node:
			if local == nil || p.holder && !local.holder || p.holder == local.holder && p.ID < local.ID {
				local = p
			}
		case fallback == nil || p.holder && !fallback.holder:
			fallback = p
		}
	}
	switch {
	case local != nil:
		return local.Peer, pickLocal
	case fallback != nil:
		return fallback.Peer, pickFallback
	}
	return apis.Peer{}, pickNone
}

func (r *registry) holders(hash string) []apis.Peer {
	var out []apis.Peer
	for _, p := range r.rank(hash) {
		if p.holder {
			out = append(out, p.Peer)
		}
	}
	return out
}

func (r *registry) aliveHolders(hash string) int {
	return len(r.holders(hash))
}

func (r *registry) snapshot() []apis.PeerStatus {
	now := r.now()
	r.mu.RLock()
	out := make([]apis.PeerStatus, 0, len(r.peers))
	for _, p := range r.peers {
		if now.Before(p.expiry) {
			out = append(out, apis.PeerStatus{ID: p.ID, URL: p.URL, Node: p.Node, Epoch: p.epoch, Expiry: p.expiry, Holdings: len(p.holdings)})
		}
	}
	r.mu.RUnlock()
	slices.SortFunc(out, func(a, b apis.PeerStatus) int { return strings.Compare(a.ID, b.ID) })
	return out
}
