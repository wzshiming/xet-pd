package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// maxAnnounceBytes bounds one announce body, a Full inventory of about 500k xorbs.
const maxAnnounceBytes = 32 << 20

// serveAnnounce registers a peer inventory; the registry changes only once the whole request validated.
func (g *Gateway) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, g.secret) {
		return
	}
	var req apis.AnnounceRequest
	if !apis.ReadJSON(w, r, maxAnnounceBytes, &req) {
		return
	}
	if err := canonicalAnnounce(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := g.reg.announce(req)
	g.staging.kick()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// canonicalAnnounce rewrites req to the form the registry keys: origin-only URL, lowercase hashes.
func canonicalAnnounce(req *apis.AnnounceRequest) error {
	if !validID(req.Peer.ID) || !validID(req.Epoch) {
		return errors.New("invalid peer id or epoch")
	}
	u, err := apis.Origin(req.Peer.URL)
	if err != nil {
		return fmt.Errorf("peer url: %w", err)
	}
	req.Peer.URL = u
	for _, list := range [][]string{req.Add, req.Remove} {
		for i, h := range list {
			hash, err := xet.ParseXorbHash(h)
			if err != nil {
				return fmt.Errorf("xorb hash: %w", err)
			}
			list[i] = hash.String()
		}
	}
	return nil
}

// validID is the apis.Peer ID and Epoch rule; it keeps them safe in headers and logs.
func validID(s string) bool {
	if s == "" || len(s) > 128 {
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
