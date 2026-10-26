// Package apis is the wire contract between the gateway and its peers, standard library only: the protocol in apis.go, request handling in http.go, outbound calls in client.go.
package apis

import "time"

const (
	// AnnouncePath is POST on the gateway, bearer-guarded: a peer registers its identity and xorb inventory (AnnounceRequest, answered by AnnounceResponse).
	AnnouncePath = "/xet-pd/v1/announce"
	// PullPath is POST on a peer, bearer-guarded: the gateway asks it to fetch one xorb (PullRequest, answered 202 once the pull is under way or the xorb already held).
	PullPath = "/xet-pd/v1/pull"
	// PeersPath is GET on the gateway, unguarded: the live registry and xorb counters (PeersResponse).
	PeersPath = "/xet-pd/v1/peers"
	// PeerHeader marks a xorb GET as a relay: a pulling peer sends its ID, the gateway sends "gateway"; a peer answers it only from its own copy, never by pulling through, and the gateway tallies it under XorbGets.Peer.
	PeerHeader = "X-Xet-PD-Peer"
)

// XorbPathPrefix is xet-core's fixed CAS route of xorb bytes: the gateway and every peer serve GET XorbPathPrefix + hash, whole or by Range; XorbURL composes it.
const XorbPathPrefix = "/v1/xorbs/default/"

// MaxXorbBytes caps a packed xorb, chunk headers and footer included: peers pull nothing larger and the gateway trusts no offset beyond it.
const MaxXorbBytes = 128 << 20

// Label bounds: Peer.ID and AnnounceRequest.Epoch take up to MaxIDLen bytes, Peer.Node up to MaxNodeLen, the length of a Kubernetes node name.
const (
	MaxIDLen   = 128
	MaxNodeLen = 253
)

// ValidLabel reports whether s is 1 to maxLen bytes of [A-Za-z0-9._-], the character set of Peer.ID, Peer.Node and AnnounceRequest.Epoch that keeps them safe in headers and logs.
func ValidLabel(s string, maxLen int) bool {
	if s == "" || len(s) > maxLen {
		return false
	}
	for _, c := range []byte(s) {
		switch {
		case c == '-', c == '_', c == '.', c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		default:
			return false
		}
	}
	return true
}

// PullRequest is the PullPath body.
type PullRequest struct {
	// Hash is the xorb to fetch from the gateway, 64 hex digits in either case.
	Hash string `json:"hash"`
}

// Peer identifies a peer; it stays comparable because the registry compares it by value.
type Peer struct {
	// ID is stable across restarts: 1 to 128 of [A-Za-z0-9._-].
	ID string `json:"id"`
	// URL is the peer's origin, scheme://host[:port] as Origin canonicalizes it, where its xorbs are fetched and pulls are posted.
	URL string `json:"url"`
	// Node names where the peer runs, as the gateway's locality resolver names nodes: 1 to 253 of [A-Za-z0-9._-] when set; empty when unknown, and such a peer is local to no client.
	Node string `json:"node,omitempty"`
}

// AnnounceRequest is the AnnouncePath body: who announces and what it holds, whole or as a delta against what the gateway last accepted.
type AnnounceRequest struct {
	// Peer is the announcer; a delta from an unknown or expired peer, or one whose URL or Node changed, is refused with Resync.
	Peer Peer `json:"peer"`
	// Epoch is drawn at random once per peer start, 1 to 128 of [A-Za-z0-9._-]; a delta under another epoch than the registered one is refused with Resync.
	Epoch string `json:"epoch"`
	// Full discards the registered inventory before Add and Remove apply and (re)registers the peer; a delta adjusts it.
	Full bool `json:"full,omitempty"`
	// Add lists xorb hashes the peer holds, 64 hex digits each, lowercased by the gateway; one invalid hash rejects the whole announce.
	Add []string `json:"add,omitempty"`
	// Remove lists xorb hashes the peer dropped, in the form of Add; it applies after Add.
	Remove []string `json:"remove,omitempty"`
}

// AnnounceResponse answers every announce that passed validation (200), a refused delta included; see Resync.
type AnnounceResponse struct {
	// Resync, never set for a Full, means the delta was discarded and the gateway holds nothing for the peer until its next announce, which must be Full.
	Resync bool `json:"resync,omitempty"`
	// TTL is the announce validity in seconds: a peer silent for longer is dropped with its holdings, so it must announce more often.
	TTL int64 `json:"ttl"`
}

// PeerStatus is one live peer in PeersResponse.
type PeerStatus struct {
	// ID is the peer's Peer.ID.
	ID string `json:"id"`
	// URL is the peer's canonical origin.
	URL string `json:"url"`
	// Node is the peer's Peer.Node, omitted when empty.
	Node string `json:"node,omitempty"`
	// Epoch is the epoch of the peer's last accepted announce.
	Epoch string `json:"epoch"`
	// Expiry is when the peer is dropped unless it announces again: its last accepted announce plus TTL.
	Expiry time.Time `json:"expiry"`
	// Holdings counts the distinct xorbs the peer announced.
	Holdings int `json:"holdings"`
}

// XorbGets counts the xorb GETs the gateway received since it started, by requester; failed ones count too.
type XorbGets struct {
	// Peer counts GETs carrying PeerHeader, peers pulling.
	Peer uint64 `json:"peer"`
	// Direct counts GETs without it, xet clients.
	Direct uint64 `json:"direct"`
}

// PeersResponse is the GET PeersPath schema: the live peer registry and xorb traffic counters.
type PeersResponse struct {
	// Peers lists the live peers sorted by ID.
	Peers []PeerStatus `json:"peers"`
	// XorbBytes is the size of the xorb copies the gateway itself keeps on disk.
	XorbBytes int64 `json:"xorb_bytes"`
	// XorbGets tallies the xorb GETs the gateway received.
	XorbGets XorbGets `json:"xorb_gets"`
}
