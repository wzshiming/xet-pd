// Package apis defines the xet-pd control wire protocol and the HTTP plumbing the gateway and peers share.
package apis

import "time"

const (
	AnnouncePath = "/xet-pd/v1/announce"
	PullPath     = "/xet-pd/v1/pull"
	PeersPath    = "/xet-pd/v1/peers"
	PeerHeader   = "X-Xet-PD-Peer"
)

// XorbPathPrefix is xet-core's fixed CAS xorb route prefix, advertised by the gateway and fetched by peers.
const XorbPathPrefix = "/v1/xorbs/default/"

// MaxXorbBytes caps a packed xorb, chunk headers and footer included: peers pull nothing larger and the gateway trusts no offset beyond it.
const MaxXorbBytes = 128 << 20

type PullRequest struct {
	Hash string `json:"hash"`
}

type Peer struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type AnnounceRequest struct {
	Peer   Peer     `json:"peer"`
	Epoch  string   `json:"epoch"`
	Full   bool     `json:"full,omitempty"`
	Add    []string `json:"add,omitempty"`
	Remove []string `json:"remove,omitempty"`
}

type AnnounceResponse struct {
	Resync bool `json:"resync,omitempty"`
	// TTL is the announce validity in seconds.
	TTL int64 `json:"ttl"`
}

type PeerStatus struct {
	ID       string    `json:"id"`
	URL      string    `json:"url"`
	Epoch    string    `json:"epoch"`
	Expiry   time.Time `json:"expiry"`
	Holdings int       `json:"holdings"`
}

type XorbGets struct {
	Peer   uint64 `json:"peer"`
	Direct uint64 `json:"direct"`
}

// ServedBytes counts the response body bytes of xorb GETs, split as XorbGets is.
type ServedBytes struct {
	Peer   uint64 `json:"peer"`
	Direct uint64 `json:"direct"`
}

// PullCounters counts the pull requests sent to seeds: a 202 is accepted, any other answer or a cancellation failed.
type PullCounters struct {
	Sent     uint64 `json:"sent"`
	Accepted uint64 `json:"accepted"`
	Failed   uint64 `json:"failed"`
}

// EvictionCounters counts the local xorb copies the gateway deleted and their bytes.
type EvictionCounters struct {
	Count uint64 `json:"count"`
	Bytes uint64 `json:"bytes"`
}

// RelayCounters counts the holders relays tried and the attempts that failed.
type RelayCounters struct {
	Attempts uint64 `json:"attempts"`
	Failures uint64 `json:"failures"`
}

// AnnounceCounters counts the accepted announces, the Full ones and those answered with a resync.
type AnnounceCounters struct {
	Total  uint64 `json:"total"`
	Full   uint64 `json:"full"`
	Resync uint64 `json:"resync"`
}

// PeersResponse is the GET PeersPath schema: the live peer registry, the staged xorb bytes and counters kept since the gateway started.
type PeersResponse struct {
	Peers           []PeerStatus     `json:"peers"`
	XorbBytes       int64            `json:"xorb_bytes"`
	XorbGets        XorbGets         `json:"xorb_gets"`
	XorbBytesServed ServedBytes      `json:"xorb_bytes_served"`
	Pulls           PullCounters     `json:"pulls"`
	Evictions       EvictionCounters `json:"evictions"`
	Relays          RelayCounters    `json:"relays"`
	Announces       AnnounceCounters `json:"announces"`
}
