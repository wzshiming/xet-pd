package tracker

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
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
	files  map[string]string // file hash -> SHA-256
}

type ranked struct {
	apis.Peer
	holder bool
	score  uint64
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

// announce applies req and lists the files whose live complete holders it changed.
func (r *registry) announce(req apis.AnnounceRequest) (apis.AnnounceResponse, []string) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := map[string]struct{}{}
	// forget drops sha's entry for file once no live peer holds file.
	forget := func(file, sha string) {
		if r.sha256s[sha] != file {
			return
		}
		for _, q := range r.peers {
			if _, ok := q.files[file]; ok && now.Before(q.expiry) {
				return
			}
		}
		delete(r.sha256s, sha)
	}
	drop := func(p *peerState) {
		delete(r.peers, p.ID)
		for f, sha := range p.files {
			changed[f] = struct{}{}
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
			return resp, slices.Collect(maps.Keys(changed))
		}
		p = &peerState{Peer: req.Peer, epoch: req.Epoch, xorbs: map[string]struct{}{}, files: map[string]string{}}
		r.peers[req.Peer.ID] = p
	}
	p.expiry = now.Add(r.ttl)
	for _, h := range req.Add {
		p.xorbs[h] = struct{}{}
	}
	for _, h := range req.Remove {
		delete(p.xorbs, h)
	}
	held := map[string]bool{} // whether p held each file req names before req applies
	for _, f := range req.AddFiles {
		_, held[f.Hash] = p.files[f.Hash]
	}
	for _, h := range req.RemoveFiles {
		_, held[h] = p.files[h]
	}
	for _, f := range req.AddFiles {
		p.files[f.Hash] = f.SHA256
	}
	for _, h := range req.RemoveFiles {
		sha := p.files[h]
		delete(p.files, h)
		forget(h, sha)
	}
	for _, f := range req.AddFiles {
		if p.files[f.Hash] == f.SHA256 {
			r.sha256s[f.SHA256] = f.Hash
		}
	}
	for f, was := range held {
		if _, is := p.files[f]; is != was {
			changed[f] = struct{}{}
		}
	}
	return resp, slices.Collect(maps.Keys(changed))
}

// rank lists live peers best first for hash, ties broken by ID; holder marks the holders of xorb hash.
func (r *registry) rank(hash string) []ranked {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.order(hash)
}

// order is rank with r.mu held.
func (r *registry) order(hash string) []ranked {
	now := r.now()
	list := make([]ranked, 0, len(r.peers))
	for _, p := range r.peers {
		if !now.Before(p.expiry) {
			continue
		}
		_, holder := p.xorbs[hash]
		// The score is the rendezvous weight of the peer for hash; the length prefix keeps the pair unambiguous.
		h := fnv.New64a()
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(hash))))
		h.Write([]byte(hash))
		h.Write([]byte(p.ID))
		list = append(list, ranked{Peer: p.Peer, holder: holder, score: h.Sum64()})
	}
	slices.SortFunc(list, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(b.score, a.score), strings.Compare(a.ID, b.ID))
	})
	return list
}

// locate lists the live complete holders of file but asker in rank order, with the SHA-256 the best of them announced.
func (r *registry) locate(file, asker string) apis.FileLocation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var loc apis.FileLocation
	for _, p := range r.order(file) {
		if sha, ok := r.peers[p.ID].files[file]; ok && p.ID != asker {
			if loc.Peers == nil {
				loc.File = apis.File{Hash: file, SHA256: sha}
			}
			loc.Peers = append(loc.Peers, p.Peer)
		}
	}
	return loc
}

// fileHolders lists the live complete holders of file in rank order.
func (r *registry) fileHolders(file string) []apis.Peer {
	return r.locate(file, "").Peers
}

// lookup answers req with the live holders of each key but the asker, leaving out keys none remains for.
func (r *registry) lookup(req apis.LookupRequest) apis.LookupResponse {
	resp := apis.LookupResponse{Xorbs: map[string][]apis.Peer{}, SHA256s: map[string]apis.FileLocation{}, Files: map[string]apis.FileLocation{}}
	for _, h := range req.Xorbs {
		var peers []apis.Peer
		for _, p := range r.rank(h) {
			if p.holder && p.ID != req.Peer {
				peers = append(peers, p.Peer)
			}
		}
		if peers != nil {
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
			out = append(out, apis.PeerStatus{ID: p.ID, URL: p.URL, Epoch: p.epoch, Expiry: p.expiry, Holdings: len(p.xorbs), Files: len(p.files)})
		}
	}
	r.mu.RUnlock()
	slices.SortFunc(out, func(a, b apis.PeerStatus) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// stats counts the live peers and the distinct xorbs and complete files they hold.
func (r *registry) stats() (peers, xorbs, files int) {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	xs, fs := map[string]struct{}{}, map[string]struct{}{}
	for _, p := range r.peers {
		if !now.Before(p.expiry) {
			continue
		}
		peers++
		for h := range p.xorbs {
			xs[h] = struct{}{}
		}
		for f := range p.files {
			fs[f] = struct{}{}
		}
	}
	return peers, len(xs), len(fs)
}
