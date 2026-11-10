// Package apis is the wire contract between the tracker and its peers, standard library only: the protocol in apis.go, request handling in http.go, outbound calls in client.go.
package apis

import "time"

const (
	// AnnouncePath is POST on the tracker, bearer-guarded: a peer registers its identity and xorb inventory (AnnounceRequest, answered by AnnounceResponse).
	AnnouncePath = "/xet-pd/v1/announce"
	// PullPath is POST on a peer, bearer-guarded: the tracker asks it to fetch one file complete (PullRequest, answered 202 once the pull is under way).
	PullPath = "/xet-pd/v1/pull"
	// PeersPath is GET on the tracker, unguarded: the live registry (PeersResponse).
	PeersPath = "/xet-pd/v1/peers"
	// LookupPath is POST on the tracker, bearer-guarded: a peer asks which live peers hold xorbs and complete files (LookupRequest, answered by LookupResponse).
	LookupPath = "/xet-pd/v1/lookup"
	// ShardPathPrefix + file hash is GET on a peer, bearer-guarded: the requester sends PeerHeader and gets the file's ShardResponse, or 404 when the peer has no shard for it.
	ShardPathPrefix = "/xet-pd/v1/shards/"
	// PeerHeader marks a xorb GET as a relay: a pulling peer sends its ID; a peer answers it only from its own copy, never by pulling through.
	PeerHeader = "X-Xet-PD-Peer"
)

// XorbPathPrefix is xet-core's fixed CAS route of xorb bytes: every peer serves GET XorbPathPrefix + hash, whole or by Range; XorbURL composes it.
const XorbPathPrefix = "/v1/xorbs/default/"

// MaxXorbBytes caps a packed xorb, chunk headers and footer included: peers pull nothing larger and trust no offset beyond it.
const MaxXorbBytes = 128 << 20

const (
	// MaxAnnounceBytes caps an announce request body, a Full inventory of about 500k xorbs.
	MaxAnnounceBytes = 32 << 20
	// MaxLookupBytes caps a lookup request body and its reply.
	MaxLookupBytes = 32 << 20
	// MaxShardBytes caps a ShardResponse reply.
	MaxShardBytes = 64 << 20
	// MaxReplyBytes caps an announce reply and a pull request body, and how much of a reply is drained before its connection is reused.
	MaxReplyBytes = 4096
	// MaxPullBytes caps a pull request body.
	MaxPullBytes = 1 << 20
	// MaxPullBatch caps the Xorbs and Shards entries of one pull request together.
	MaxPullBatch = 256
)

// MaxIDLen bounds Peer.ID and AnnounceRequest.Epoch, in bytes.
const MaxIDLen = 128

// ValidLabel reports whether s is 1 to maxLen bytes of [A-Za-z0-9._-], the character set of Peer.ID and AnnounceRequest.Epoch that keeps them safe in headers and logs.
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

// PullRequest is the PullPath body: at least one entry across Xorbs and Shards, or a File; 202 means accepted, the pulls run asynchronously.
type PullRequest struct {
	// Xorbs lists hashes of xorbs to pull whole.
	Xorbs []string `json:"xorbs,omitempty"`
	// Shards lists hashes of files, in the form of File.Hash, whose ShardResponse the peer fetches.
	Shards []string `json:"shards,omitempty"`
	// File, in the form of File.Hash, names a file to pull complete: its shard and every xorb the shard references. Superseded by Xorbs/Shards.
	File string `json:"file,omitempty"`
	// Seed marks the copy as a seed copy, which the peer keeps until MinSeeds other live holders have it; it applies to every listed xorb.
	Seed bool `json:"seed,omitempty"`
}

// Peer identifies a peer; it stays comparable because the registry compares it by value.
type Peer struct {
	// ID is stable across restarts: 1 to 128 of [A-Za-z0-9._-].
	ID string `json:"id"`
	// URL is the peer's origin, scheme://host[:port] as Origin canonicalizes it, where its xorbs are fetched and pulls are posted.
	URL string `json:"url"`
	// Upstream is true when the peer is configured with an external hub and fetches from it; peers without it mirror through such peers.
	Upstream bool `json:"upstream,omitempty"`
}

// File identifies a file by its xet file hash and its SHA-256; it stays comparable so it can be compared and keyed by value.
type File struct {
	// Hash is the xet file hash, 64 hex digits as xet.FileHash.String prints it.
	Hash string `json:"hash"`
	// SHA256 is the SHA-256 digest of the file's bytes, 64 lowercase hex digits.
	SHA256 string `json:"sha256"`
}

// AnnounceRequest is the AnnouncePath body: who announces and what it holds, whole or as a delta against what the tracker last accepted.
type AnnounceRequest struct {
	// Peer is the announcer; a delta from an unknown or expired peer, or one whose URL changed, is refused with Resync.
	Peer Peer `json:"peer"`
	// Epoch is drawn at random once per peer start, 1 to 128 of [A-Za-z0-9._-]; a delta under another epoch than the registered one is refused with Resync.
	Epoch string `json:"epoch"`
	// Full replaces the registered xorb, shard and file sets: it discards them before the lists apply and (re)registers the peer; a delta applies Add and Remove to the xorbs, AddShards and RemoveShards to the shards, AddFiles and RemoveFiles to the files.
	Full bool `json:"full,omitempty"`
	// Add lists xorb hashes the peer holds, 64 hex digits each, lowercased by the tracker; one invalid hash rejects the whole announce.
	Add []string `json:"add,omitempty"`
	// Remove lists xorb hashes the peer dropped, in the form of Add; it applies after Add.
	Remove []string `json:"remove,omitempty"`
	// AddShards lists files whose shard the peer holds: the shard bytes and the packed chunk offsets of every xorb it references, as ShardResponse carries them, whether or not it holds those xorbs.
	AddShards []File `json:"add_shards,omitempty"`
	// RemoveShards lists hashes of files whose shard the peer dropped, in the form of File.Hash; it applies after AddShards, so it wins inside one request.
	RemoveShards []string `json:"remove_shards,omitempty"`
	// AddFiles lists files the peer holds complete, meaning it holds their shard and every xorb the shard references. Superseded by AddShards/RemoveShards.
	AddFiles []File `json:"add_files,omitempty"`
	// RemoveFiles lists hashes of files the peer no longer holds complete, in the form of File.Hash. Superseded by AddShards/RemoveShards.
	RemoveFiles []string `json:"remove_files,omitempty"`
}

// AnnounceResponse answers every announce that passed validation (200), a refused delta included; see Resync.
type AnnounceResponse struct {
	// Resync, never set for a Full, means the delta was discarded and the tracker holds nothing for the peer until its next announce, which must be Full.
	Resync bool `json:"resync,omitempty"`
	// TTL is the announce validity in seconds: a peer silent for longer is dropped with its holdings, so it must announce more often.
	TTL int64 `json:"ttl"`
	// MinSeeds is how many other live holders a seed copy needs before the peer may evict it.
	MinSeeds int `json:"min_seeds,omitempty"`
	// Upstreams lists the live peers whose Upstream is set, sorted by ID, on every reply; omitted when there are none.
	Upstreams []Peer `json:"upstreams,omitempty"`
}

// PeerStatus is one live peer in PeersResponse.
type PeerStatus struct {
	// ID is the peer's Peer.ID.
	ID string `json:"id"`
	// URL is the peer's canonical origin.
	URL string `json:"url"`
	// Upstream is the peer's Peer.Upstream.
	Upstream bool `json:"upstream,omitempty"`
	// Epoch is the epoch of the peer's last accepted announce.
	Epoch string `json:"epoch"`
	// Expiry is when the peer is dropped unless it announces again: its last accepted announce plus TTL.
	Expiry time.Time `json:"expiry"`
	// Holdings counts the distinct xorbs the peer announced.
	Holdings int `json:"holdings"`
	// Shards counts the shards the peer announced.
	Shards int `json:"shards"`
	// Files counts the complete files the peer announced. Superseded by Shards.
	Files int `json:"files"`
}

// PeersResponse is the GET PeersPath schema: the live peer registry.
type PeersResponse struct {
	// Peers lists the live peers sorted by ID.
	Peers []PeerStatus `json:"peers"`
}

// LookupRequest is the LookupPath body: what the asking peer wants live holders of.
type LookupRequest struct {
	// Peer is the asking peer's ID, excluded from every answer.
	Peer string `json:"peer,omitempty"`
	// Xorbs lists xorb hashes, 64 hex digits each.
	Xorbs []string `json:"xorbs,omitempty"`
	// SHA256s lists file SHA-256 digests in the form of File.SHA256.
	SHA256s []string `json:"sha256s,omitempty"`
	// Files lists file hashes in the form of File.Hash.
	Files []string `json:"files,omitempty"`
}

// FileLocation is a file and the live peers holding its shard.
type FileLocation struct {
	// File is embedded, so its hash and sha256 sit beside peers on the wire.
	File
	// Peers lists the live peers holding the file's shard, best first.
	Peers []Peer `json:"peers"`
}

// LookupResponse answers a LookupRequest; a key with no live holder is absent.
type LookupResponse struct {
	// Xorbs maps a requested xorb hash to its live holders, best first.
	Xorbs map[string][]Peer `json:"xorbs,omitempty"`
	// SHA256s maps a requested SHA-256 to the file it names and the live peers holding the file's shard, best first.
	SHA256s map[string]FileLocation `json:"sha256s,omitempty"`
	// Files maps a requested file hash to the file and the live peers holding the file's shard, best first.
	Files map[string]FileLocation `json:"files,omitempty"`
}

// ShardResponse is a peer's reply to GET ShardPathPrefix + file hash: the file's shard and the chunk offsets of the xorbs it references.
type ShardResponse struct {
	// Shard is the footerless serialized shard storage.EncodeShard produces; base64 on the wire.
	Shard []byte `json:"shard"`
	// Offsets maps every xorb the shard references to its cumulative packed chunk ends, as Storage.GetXorbChunkOffsets returns them.
	Offsets map[string][]uint64 `json:"offsets"`
}
