package tracker

import (
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

type peerState struct {
	apis.Peer
	epoch  string
	expiry time.Time
	xorbs  map[string]struct{}
	shards map[string]string // file hash -> SHA-256
}

// object is a xorb, or with shard set the shard of a file, named by its hash.
type object struct {
	shard bool
	hash  string
}

// holds reports whether p announced o.
func (p *peerState) holds(o object) bool {
	if o.shard {
		_, ok := p.shards[o.hash]
		return ok
	}
	_, ok := p.xorbs[o.hash]
	return ok
}

type registry struct {
	mu       sync.RWMutex
	ttl      time.Duration
	minSeeds int
	now      func() time.Time // set before concurrent use
	peers    map[string]*peerState
	sha256s  map[string]string // SHA-256 -> file hash, as last announced
}

func newRegistry(ttl time.Duration, minSeeds int) *registry {
	return &registry{ttl: ttl, minSeeds: minSeeds, now: time.Now, peers: map[string]*peerState{}, sha256s: map[string]string{}}
}

// announce applies req and lists the objects whose live holders it changed.
func (r *registry) announce(req apis.AnnounceRequest) (apis.AnnounceResponse, []object) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := map[object]struct{}{}
	// forget drops sha's entry for file once no live peer holds file's shard.
	forget := func(file, sha string) {
		if r.sha256s[sha] != file {
			return
		}
		for _, q := range r.peers {
			if _, ok := q.shards[file]; ok && now.Before(q.expiry) {
				return
			}
		}
		delete(r.sha256s, sha)
	}
	drop := func(p *peerState) {
		delete(r.peers, p.ID)
		for h := range p.xorbs {
			changed[object{hash: h}] = struct{}{}
		}
		for f, sha := range p.shards {
			changed[object{shard: true, hash: f}] = struct{}{}
			forget(f, sha)
		}
	}
	for _, p := range r.peers {
		if !now.Before(p.expiry) {
			drop(p)
		}
	}
	resp := apis.AnnounceResponse{TTL: int64(r.ttl / time.Second), MinSeeds: r.minSeeds}
	p := r.peers[req.Peer.ID]
	if p == nil || p.epoch != req.Epoch || p.Peer != req.Peer || req.Full {
		if p != nil {
			drop(p)
		}
		if !req.Full {
			resp.Resync = true
			resp.Upstreams = r.upstreamsAt(now)
			return resp, slices.Collect(maps.Keys(changed))
		}
		p = &peerState{Peer: req.Peer, epoch: req.Epoch, xorbs: map[string]struct{}{}, shards: map[string]string{}}
		r.peers[req.Peer.ID] = p
	}
	p.expiry = now.Add(r.ttl)
	held := map[object]bool{} // whether p held each object req names before req applies
	for _, h := range slices.Concat(req.Add, req.Remove) {
		o := object{hash: h}
		held[o] = p.holds(o)
	}
	for _, f := range req.AddShards {
		o := object{shard: true, hash: f.Hash}
		held[o] = p.holds(o)
	}
	for _, h := range req.RemoveShards {
		o := object{shard: true, hash: h}
		held[o] = p.holds(o)
	}
	for _, h := range req.Add {
		p.xorbs[h] = struct{}{}
	}
	for _, h := range req.Remove {
		delete(p.xorbs, h)
	}
	for _, f := range req.AddShards {
		p.shards[f.Hash] = f.SHA256
	}
	for _, h := range req.RemoveShards {
		sha := p.shards[h]
		delete(p.shards, h)
		forget(h, sha)
	}
	for _, f := range req.AddShards {
		if p.shards[f.Hash] == f.SHA256 {
			r.sha256s[f.SHA256] = f.Hash
		}
	}
	for o, was := range held {
		if p.holds(o) != was {
			changed[o] = struct{}{}
		}
	}
	resp.Upstreams = r.upstreamsAt(now)
	return resp, slices.Collect(maps.Keys(changed))
}

// upstreams lists the live peers whose Upstream is set, sorted by ID.
func (r *registry) upstreams() []apis.Peer {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.upstreamsAt(now)
}

// upstreamsAt is upstreams with r.mu held.
func (r *registry) upstreamsAt(now time.Time) []apis.Peer {
	var out []apis.Peer
	for _, p := range r.peers {
		if p.Upstream && now.Before(p.expiry) {
			out = append(out, p.Peer)
		}
	}
	slices.SortFunc(out, func(a, b apis.Peer) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// live lists the live peers.
func (r *registry) live() []apis.Peer {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]apis.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		if now.Before(p.expiry) {
			out = append(out, p.Peer)
		}
	}
	return out
}

// holders lists the live holders of o in rank order.
func (r *registry) holders(o object) []apis.Peer {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.holdersAt(o, now)
}

// holdersAt is holders with r.mu held.
func (r *registry) holdersAt(o object, now time.Time) []apis.Peer {
	var out []apis.Peer
	for _, p := range r.peers {
		if now.Before(p.expiry) && p.holds(o) {
			out = append(out, p.Peer)
		}
	}
	return apis.Rank(o.hash, out)
}

// locate lists the live holders of file's shard but asker in rank order, with the SHA-256 the best of them announced.
func (r *registry) locate(file, asker string) apis.FileLocation {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	var loc apis.FileLocation
	for _, p := range r.holdersAt(object{shard: true, hash: file}, now) {
		if p.ID == asker {
			continue
		}
		if loc.Peers == nil {
			loc.File = apis.File{Hash: file, SHA256: r.peers[p.ID].shards[file]}
		}
		loc.Peers = append(loc.Peers, p)
	}
	return loc
}

// lookup answers req with the live holders of each key but the asker, leaving out keys none remains for.
func (r *registry) lookup(req apis.LookupRequest) apis.LookupResponse {
	resp := apis.LookupResponse{Xorbs: map[string][]apis.Peer{}, SHA256s: map[string]apis.FileLocation{}, Files: map[string]apis.FileLocation{}}
	for _, h := range req.Xorbs {
		if peers := slices.DeleteFunc(r.holders(object{hash: h}), func(p apis.Peer) bool { return p.ID == req.Peer }); len(peers) > 0 {
			resp.Xorbs[h] = peers
		}
	}
	for _, f := range req.Files {
		if loc := r.locate(f, req.Peer); loc.Peers != nil {
			resp.Files[f] = loc
		}
	}
	for _, s := range req.SHA256s {
		r.mu.RLock()
		f, ok := r.sha256s[s]
		r.mu.RUnlock()
		if !ok {
			continue
		}
		if loc := r.locate(f, req.Peer); loc.Peers != nil {
			loc.SHA256 = s
			resp.SHA256s[s] = loc
		}
	}
	return resp
}

func (r *registry) snapshot() []apis.PeerStatus {
	now := r.now()
	r.mu.RLock()
	out := make([]apis.PeerStatus, 0, len(r.peers))
	for _, p := range r.peers {
		if now.Before(p.expiry) {
			out = append(out, apis.PeerStatus{ID: p.ID, URL: p.URL, Upstream: p.Upstream, Epoch: p.epoch, Expiry: p.expiry, Holdings: len(p.xorbs), Shards: len(p.shards)})
		}
	}
	r.mu.RUnlock()
	slices.SortFunc(out, func(a, b apis.PeerStatus) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// stats counts the live peers and the distinct xorbs and shards they hold.
func (r *registry) stats() (peers, xorbs, shards int) {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	xs, ss := map[string]struct{}{}, map[string]struct{}{}
	for _, p := range r.peers {
		if !now.Before(p.expiry) {
			continue
		}
		peers++
		for h := range p.xorbs {
			xs[h] = struct{}{}
		}
		for f := range p.shards {
			ss[f] = struct{}{}
		}
	}
	return peers, len(xs), len(ss)
}
