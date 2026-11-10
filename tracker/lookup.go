package tracker

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wzshiming/xet-pd/apis"
)

// serveLookup answers which live peers other than the asker hold the requested xorbs and file shards.
func (tr *Tracker) serveLookup(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, tr.secret) {
		return
	}
	var req apis.LookupRequest
	if !apis.ReadJSON(w, r, apis.MaxLookupBytes, &req) {
		return
	}
	if err := errors.Join(canonical(req.Xorbs, xorbHash), canonical(req.Files, fileHash), canonical(req.SHA256s, sha256Hex)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tr.metrics.lookup()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tr.reg.lookup(req))
}
