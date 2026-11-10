package apis

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"slices"
	"strings"
)

// Rank returns peers in a new slice, highest rendezvous weight for key first, ties by ID; the tracker and peers share it so every node picks the same peer for the same key.
func Rank(key string, peers []Peer) []Peer {
	type weighted struct {
		Peer
		weight uint64
	}
	list := make([]weighted, 0, len(peers))
	for _, p := range peers {
		// The length prefix keeps the key and the ID apart.
		h := fnv.New64a()
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(key))))
		h.Write([]byte(key))
		h.Write([]byte(p.ID))
		list = append(list, weighted{Peer: p, weight: h.Sum64()})
	}
	slices.SortFunc(list, func(a, b weighted) int {
		return cmp.Or(cmp.Compare(b.weight, a.weight), strings.Compare(a.ID, b.ID))
	})
	out := make([]Peer, 0, len(list))
	for _, w := range list {
		out = append(out, w.Peer)
	}
	return out
}
