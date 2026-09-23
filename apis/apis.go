// Package apis defines the xet-pd control wire protocol shared by the gateway and peers.
package apis

import "time"

const (
	AnnouncePath = "/xet-pd/v1/announce"
	PullPath     = "/xet-pd/v1/pull"
	PeersPath    = "/xet-pd/v1/peers"
	PeerHeader   = "X-Xet-PD-Peer"
)

type PullRequest struct {
	Namespace string `json:"namespace"`
	Hash      string `json:"hash"`
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

// PeersResponse is the GET PeersPath schema: the live peer registry and xorb traffic counters.
type PeersResponse struct {
	Peers     []PeerStatus `json:"peers"`
	XorbBytes int64        `json:"xorb_bytes"`
	XorbGets  XorbGets     `json:"xorb_gets"`
}
