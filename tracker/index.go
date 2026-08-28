package tracker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// indexEntry is one persisted location record: which peers claim the content
// and until when (unix seconds) each claim stays valid. Entries under files/
// are keyed by sha256, entries under keys/ by resolve path.
type indexEntry struct {
	Key      string           `json:"key,omitempty"`
	SHA256   string           `json:"sha256,omitempty"`
	FileHash string           `json:"file_hash,omitempty"`
	Peers    map[string]int64 `json:"peers"`
}

// location filters the entry down to live peers.
func (e *indexEntry) location(now time.Time) (FileLocation, bool) {
	loc := FileLocation{SHA256: e.SHA256, FileHash: e.FileHash}
	cut := now.Unix()
	for peer, expiry := range e.Peers {
		if expiry > cut {
			loc.Peers = append(loc.Peers, peer)
		}
	}
	if len(loc.Peers) == 0 {
		return FileLocation{}, false
	}
	return loc, true
}

// filePath maps a sha256 to its index file; non-hex input maps to a name
// that cannot exist, so hostile identifiers never touch the filesystem.
func (h *Handler) filePath(sha string) string {
	if !hexSHA256Re.MatchString(sha) {
		return ""
	}
	return filepath.Join(h.dir, "files", sha+".json")
}

// keyPath maps a resolve-path key to its index file, hashed so
// request-derived names cannot traverse outside the directory.
func (h *Handler) keyPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(h.dir, "keys", hex.EncodeToString(sum[:])+".json")
}

// read loads one entry, returning nil for anything missing or unreadable.
func (h *Handler) read(path string) *indexEntry {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var e indexEntry
	if json.Unmarshal(data, &e) != nil || len(e.Peers) == 0 {
		return nil
	}
	return &e
}

// write persists an entry atomically, pruning peers whose claims expired;
// an entry with no live peers left is removed instead.
func (h *Handler) write(path string, e *indexEntry, now time.Time) error {
	cut := now.Unix()
	for peer, expiry := range e.Peers {
		if expiry <= cut {
			delete(e.Peers, peer)
		}
	}
	if len(e.Peers) == 0 {
		_ = os.Remove(path)
		return nil
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal index entry: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize index entry: %w", err)
	}
	return nil
}

// upsert records one announced file under its sha256 and every resolve-path
// alias. Caller holds h.mu.
func (h *Handler) upsert(add FileAdd, peer string, expiry int64, now time.Time) {
	if path := h.filePath(add.SHA256); path != "" {
		e := h.read(path)
		if e == nil {
			e = &indexEntry{SHA256: add.SHA256, Peers: map[string]int64{}}
		}
		if add.FileHash != "" {
			e.FileHash = add.FileHash
		}
		e.Peers[peer] = expiry
		_ = h.write(path, e, now)
	}
	for _, key := range add.Keys {
		if key == "" {
			continue
		}
		path := h.keyPath(key)
		e := h.read(path)
		if e == nil || e.Key != key {
			e = &indexEntry{Key: key, Peers: map[string]int64{}}
		}
		if add.SHA256 != "" {
			e.SHA256 = add.SHA256
		}
		if add.FileHash != "" {
			e.FileHash = add.FileHash
		}
		e.Peers[peer] = expiry
		_ = h.write(path, e, now)
	}
}

// withdraw drops one peer's claim from the sha256 entry and every alias.
// Caller holds h.mu.
func (h *Handler) withdraw(rm FileAdd, peer string, now time.Time) {
	if path := h.filePath(rm.SHA256); path != "" {
		if e := h.read(path); e != nil {
			delete(e.Peers, peer)
			_ = h.write(path, e, now)
		}
	}
	for _, key := range rm.Keys {
		path := h.keyPath(key)
		if e := h.read(path); e != nil && e.Key == key {
			delete(e.Peers, peer)
			_ = h.write(path, e, now)
		}
	}
}

// xorbEntry is one persisted xorb location record: which chunk ranges each
// peer claims and until when (unix seconds) the claim stays valid.
type xorbEntry struct {
	Hash  string               `json:"hash"`
	Peers map[string]xorbClaim `json:"peers"`
}

// xorbClaim is one peer's merged chunk ranges of one xorb.
type xorbClaim struct {
	Ranges []ChunkSpan `json:"ranges"`
	Expiry int64       `json:"expiry"`
}

// locations filters the entry down to live peers whose claims fully contain
// the queried span.
func (e *xorbEntry) locations(span ChunkSpan, now time.Time) []XorbLocation {
	var locs []XorbLocation
	cut := now.Unix()
	for peer, claim := range e.Peers {
		if claim.Expiry <= cut {
			continue
		}
		for _, r := range claim.Ranges {
			if r.contains(span) {
				locs = append(locs, XorbLocation{Peer: peer, Ranges: claim.Ranges})
				break
			}
		}
	}
	return locs
}

// xorbPath maps a xorb hash to its index file; non-hex input maps to a name
// that cannot exist, so hostile identifiers never touch the filesystem.
func (h *Handler) xorbPath(hash string) string {
	if !hexSHA256Re.MatchString(hash) {
		return ""
	}
	return filepath.Join(h.dir, "xorbs", hash+".json")
}

// readXorb loads one xorb entry, returning nil for anything missing or
// unreadable.
func (h *Handler) readXorb(path string) *xorbEntry {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var e xorbEntry
	if json.Unmarshal(data, &e) != nil || len(e.Peers) == 0 {
		return nil
	}
	return &e
}

// writeXorb persists a xorb entry atomically, pruning peers whose claims
// expired or hold no valid range; an entry with no live peers is removed.
func (h *Handler) writeXorb(path string, e *xorbEntry, now time.Time) error {
	cut := now.Unix()
	for peer, claim := range e.Peers {
		if claim.Expiry <= cut || len(claim.Ranges) == 0 {
			delete(e.Peers, peer)
		}
	}
	if len(e.Peers) == 0 {
		_ = os.Remove(path)
		return nil
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal xorb entry: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write xorb entry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize xorb entry: %w", err)
	}
	return nil
}

// upsertXorb merges one announced xorb claim into the index. Invalid spans
// are dropped and at most maxSpansPerXorb spans per announce are taken; when
// the merge with the stored claim would still exceed the cap, the newest
// announce replaces the stored claim instead of growing it. Caller holds
// h.mu.
func (h *Handler) upsertXorb(add XorbAdd, peer string, expiry int64, now time.Time) {
	path := h.xorbPath(add.Hash)
	if path == "" {
		return
	}
	ranges := add.Ranges
	if len(ranges) > maxSpansPerXorb {
		ranges = ranges[:maxSpansPerXorb]
	}
	ranges = MergeSpans(ranges)
	if len(ranges) == 0 {
		return
	}
	e := h.readXorb(path)
	if e == nil {
		e = &xorbEntry{Hash: add.Hash, Peers: map[string]xorbClaim{}}
	}
	merged := MergeSpans(append(append([]ChunkSpan{}, e.Peers[peer].Ranges...), ranges...))
	if len(merged) > maxSpansPerXorb {
		merged = ranges
	}
	e.Peers[peer] = xorbClaim{Ranges: merged, Expiry: expiry}
	_ = h.writeXorb(path, e, now)
}

// withdrawXorb drops one peer's whole claim on the xorb. Caller holds h.mu.
func (h *Handler) withdrawXorb(rm XorbAdd, peer string, now time.Time) {
	path := h.xorbPath(rm.Hash)
	if path == "" {
		return
	}
	if e := h.readXorb(path); e != nil {
		delete(e.Peers, peer)
		_ = h.writeXorb(path, e, now)
	}
}
