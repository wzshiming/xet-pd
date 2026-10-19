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
	// Canonical form, as the registry keys it: origin-only URL, lowercase hashes.
	var err error
	if !validID(req.Peer.ID) || !validID(req.Epoch) {
		err = errors.New("invalid peer id or epoch")
	} else if u, uerr := apis.Origin(req.Peer.URL); uerr != nil {
		err = fmt.Errorf("peer url: %w", uerr)
	} else {
		req.Peer.URL = u
	lists:
		for _, list := range [][]string{req.Add, req.Remove} {
			for i, h := range list {
				hash, herr := xet.ParseXorbHash(h)
				if herr != nil {
					err = fmt.Errorf("xorb hash: %w", herr)
					break lists
				}
				list[i] = hash.String()
			}
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := g.reg.announce(req)
	g.staging.kick()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// validID accepts a peer ID or epoch: 1 to 128 of [A-Za-z0-9._-], safe in headers and logs.
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
