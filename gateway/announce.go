package gateway

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"

	"github.com/wzshiming/xet-pd/apis"
)

// maxAnnounceBytes bounds one announce body, a Full inventory of about 500k xorbs.
const maxAnnounceBytes = 32 << 20

// serveAnnounce registers a peer inventory; the registry changes only once the whole request validated.
func (g *Gateway) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	if g.secret != "" {
		tok, ok := auth.BearerToken(r)
		if !ok || subtle.ConstantTimeCompare([]byte(tok), []byte(g.secret)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	var req apis.AnnounceRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAnnounceBytes))
	err := dec.Decode(&req)
	if err == nil {
		switch err = dec.Decode(&struct{}{}); err {
		case io.EOF:
			err = nil
		case nil:
			err = errors.New("trailing data")
		}
	}
	if err != nil {
		status := http.StatusBadRequest
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok { // the limit may hit while draining trailing whitespace
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return
	}
	req, err = canonical(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := g.reg.announce(req)
	g.manager.kick()
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

// canonical validates an announce and returns it as the registry keys it: origin-only URL, lowercase hashes.
func canonical(req apis.AnnounceRequest) (apis.AnnounceRequest, error) {
	if !validID(req.Peer.ID) || !validID(req.Epoch) {
		return req, errors.New("invalid peer id or epoch")
	}
	u, err := origin(req.Peer.URL)
	if err != nil {
		return req, fmt.Errorf("peer url: %w", err)
	}
	req.Peer.URL = u
	for _, list := range [][]string{req.Add, req.Remove} {
		for i, h := range list {
			hash, err := xet.ParseXorbHash(h)
			if err != nil {
				return req, fmt.Errorf("xorb hash: %w", err)
			}
			list[i] = hash.String()
		}
	}
	return req, nil
}
