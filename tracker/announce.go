package tracker

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// serveAnnounce registers a peer inventory and notes the files whose holders changed; the registry changes only once the whole request validated.
func (tr *Tracker) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, tr.secret) {
		tr.metrics.announce("rejected")
		return
	}
	var req apis.AnnounceRequest
	if !apis.ReadJSON(w, r, apis.MaxAnnounceBytes, &req) {
		tr.metrics.announce("rejected")
		return
	}
	if err := canonicalAnnounce(&req); err != nil {
		tr.metrics.announce("rejected")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, changed := tr.reg.announce(req)
	if resp.Resync {
		tr.metrics.announce("resync")
	} else {
		tr.metrics.announce("ok")
	}
	tr.seeder.note(changed)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// canonicalAnnounce rewrites req to the form the registry keys: origin-only URL, lowercase hashes.
func canonicalAnnounce(req *apis.AnnounceRequest) error {
	if !apis.ValidLabel(req.Peer.ID, apis.MaxIDLen) || !apis.ValidLabel(req.Epoch, apis.MaxIDLen) {
		return errors.New("invalid peer id or epoch")
	}
	u, err := apis.Origin(req.Peer.URL)
	if err != nil {
		return fmt.Errorf("peer url: %w", err)
	}
	req.Peer.URL = u
	for i := range req.AddFiles {
		f := &req.AddFiles[i]
		if f.Hash, err = fileHash(f.Hash); err != nil {
			return err
		}
		if f.SHA256, err = sha256Hex(f.SHA256); err != nil {
			return err
		}
	}
	return errors.Join(canonical(req.Add, xorbHash), canonical(req.Remove, xorbHash), canonical(req.RemoveFiles, fileHash))
}

// errInvalidFile rejects a file hash or SHA-256 without echoing it.
var errInvalidFile = errors.New("invalid file")

// canonical rewrites each hash in list to the form parse returns.
func canonical(list []string, parse func(string) (string, error)) error {
	for i, s := range list {
		c, err := parse(s)
		if err != nil {
			return err
		}
		list[i] = c
	}
	return nil
}

func xorbHash(s string) (string, error) {
	h, err := xet.ParseXorbHash(s)
	if err != nil {
		return "", fmt.Errorf("xorb hash: %w", err)
	}
	return h.String(), nil
}

func fileHash(s string) (string, error) {
	h, err := xet.ParseFileHash(s)
	if err != nil {
		return "", errInvalidFile
	}
	return h.String(), nil
}

// sha256Hex lowercases a SHA-256 digest of 64 hex digits.
func sha256Hex(s string) (string, error) {
	if _, err := hex.DecodeString(s); err != nil || len(s) != 64 {
		return "", errInvalidFile
	}
	return strings.ToLower(s), nil
}
