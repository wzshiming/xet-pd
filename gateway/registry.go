// Package gateway implements the xet-pd central gateway.
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

// peerState is fixed once published but for expiry and the holdings' contents, which change under the registry's write lock.
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
	var full *peerState
	if req.Full {
		full = &peerState{Peer: req.Peer, epoch: req.Epoch, holdings: make(map[string]struct{}, len(req.Add))}
		for _, h := range req.Add {
			full.holdings[h] = struct{}{}
		}
		for _, h := range req.Remove {
			delete(full.holdings, h)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, p := range r.peers {
		if !now.Before(p.expiry) {
			delete(r.peers, id)
		}
	}
	resp := apis.AnnounceResponse{TTL: int64(r.ttl / time.Second)}
	if full != nil {
		full.expiry = now.Add(r.ttl)
		r.peers[req.Peer.ID] = full
		return resp
	}
	p := r.peers[req.Peer.ID]
	if p == nil || p.epoch != req.Epoch || p.Peer != req.Peer {
		delete(r.peers, req.Peer.ID)
		resp.Resync = true
		return resp
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

// liveView is the peers alive at one instant; one pruned or replaced since keeps counting with its last holdings until the view is dropped.
type liveView struct {
	r     *registry
	peers []*peerState
}

func (r *registry) live() liveView {
	now := r.now()
	r.mu.RLock()
	peers := make([]*peerState, 0, len(r.peers))
	for _, p := range r.peers {
		if now.Before(p.expiry) {
			peers = append(peers, p)
		}
	}
	r.mu.RUnlock()
	return liveView{r: r, peers: peers}
}

// rank lists the view's peers best first for hash, ties broken by ID.
func (v liveView) rank(hash string) []ranked {
	list := make([]ranked, len(v.peers))
	v.r.mu.RLock()
	for i, p := range v.peers {
		_, list[i].holder = p.holdings[hash]
	}
	v.r.mu.RUnlock()
	for i, p := range v.peers {
		// The score is the rendezvous weight of the peer for hash; the length prefix keeps the pair unambiguous.
		h := fnv.New64a()
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(hash))))
		h.Write([]byte(hash))
		h.Write([]byte(p.ID))
		list[i].Peer, list[i].score = p.Peer, h.Sum64()
	}
	slices.SortFunc(list, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(b.score, a.score), strings.Compare(a.ID, b.ID))
	})
	return list
}

func (v liveView) holderCount(hash string) int {
	n := 0
	v.r.mu.RLock()
	for _, p := range v.peers {
		if _, ok := p.holdings[hash]; ok {
			n++
		}
	}
	v.r.mu.RUnlock()
	return n
}

// rank lists live peers best first for hash, ties broken by ID.
func (r *registry) rank(hash string) []ranked {
	return r.live().rank(hash)
}

func (r *registry) pick(hash string) (apis.Peer, bool) {
	list := r.rank(hash)
	for _, p := range list {
		if p.holder {
			return p.Peer, true
		}
	}
	if len(list) == 0 {
		return apis.Peer{}, false
	}
	return list[0].Peer, true
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
	return r.live().holderCount(hash)
}

func (r *registry) snapshot() []apis.PeerStatus {
	now := r.now()
	r.mu.RLock()
	out := make([]apis.PeerStatus, 0, len(r.peers))
	for _, p := range r.peers {
		if now.Before(p.expiry) {
			out = append(out, apis.PeerStatus{ID: p.ID, URL: p.URL, Epoch: p.epoch, Expiry: p.expiry, Holdings: len(p.holdings)})
		}
	}
	r.mu.RUnlock()
	slices.SortFunc(out, func(a, b apis.PeerStatus) int { return strings.Compare(a.ID, b.ID) })
	return out
}
