// Package tracker implements the cluster content index: agents announce
// which files they hold and look up which peers hold a file, replacing the
// O(peers) broadcast probing of static peer lists. Entries are persisted as
// one JSON file per key — the files are the index — so a restarted tracker
// answers lookups immediately; a lost index only degrades efficiency until
// announces rebuild it.
package tracker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileAdd names one held file: the content keys plus the resolve-path aliases
// under which cold clients will ask for it before they know the sha256.
type FileAdd struct {
	SHA256   string   `json:"sha256,omitempty"`
	FileHash string   `json:"fileHash,omitempty"`
	Keys     []string `json:"keys,omitempty"`
}

// ChunkSpan is a contiguous chunk index range [Start, End) of one xorb,
// matching the fetch_info range semantics of the reconstruction API.
type ChunkSpan struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"` // Exclusive
}

// contains reports whether other lies fully within the span.
func (s ChunkSpan) contains(other ChunkSpan) bool {
	return s.Start <= other.Start && other.End <= s.End
}

// XorbAdd announces the chunk ranges of one xorb a peer can serve. In
// Removes the ranges are ignored: a remove withdraws the peer's whole claim
// on the xorb, mirroring how file removes withdraw the peer entirely.
type XorbAdd struct {
	Hash   string      `json:"hash"`
	Ranges []ChunkSpan `json:"ranges,omitempty"`
}

// AnnounceSet groups announced entries by kind: whole files (S2) and xorb
// chunk ranges (S3).
type AnnounceSet struct {
	Files []FileAdd `json:"files,omitempty"`
	Xorbs []XorbAdd `json:"xorbs,omitempty"`
}

// AnnounceRequest registers (or withdraws) a peer's holdings. TTL is in
// seconds; zero means the tracker default. Announces double as keepalives.
type AnnounceRequest struct {
	Peer    string      `json:"peer"`
	TTL     int64       `json:"ttl,omitempty"`
	Adds    AnnounceSet `json:"adds,omitempty"`
	Removes AnnounceSet `json:"removes,omitempty"`
}

// AnnounceResponse acknowledges an announce.
type AnnounceResponse struct {
	OK bool `json:"ok"`
}

// LookupRequest asks which peers hold the given files, by sha256 or by
// resolve-path key, and which peers cover the given xorb chunk ranges.
type LookupRequest struct {
	Files []string    `json:"files,omitempty"`
	Keys  []string    `json:"keys,omitempty"`
	Xorbs []XorbQuery `json:"xorbs,omitempty"`
}

// XorbQuery asks which peers hold a chunk range of one xorb.
type XorbQuery struct {
	Hash  string    `json:"hash"`
	Range ChunkSpan `json:"range"`
}

// FileLocation is one lookup answer: the content identity and the peers
// whose claims have not expired. Lookups may be stale; consumers verify with
// a probe and fall back on miss.
type FileLocation struct {
	SHA256   string   `json:"sha256,omitempty"`
	FileHash string   `json:"fileHash,omitempty"`
	Peers    []string `json:"peers"`
}

// XorbLocation is one xorb lookup answer: a peer whose announced ranges
// fully contain the queried span, along with everything it claims for the
// xorb so callers can plan further fetches without extra lookups.
type XorbLocation struct {
	Peer   string      `json:"peer"`
	Ranges []ChunkSpan `json:"ranges,omitempty"`
}

// LookupResponse maps each requested identity to its location; misses are
// simply absent. Xorb answers are keyed by the query itself,
// "{hash}:{start}-{end}", so multiple queries on one hash cannot collide.
type LookupResponse struct {
	Files map[string]FileLocation   `json:"files,omitempty"`
	Keys  map[string]FileLocation   `json:"keys,omitempty"`
	Xorbs map[string][]XorbLocation `json:"xorbs,omitempty"`
}

// MergeSpans sorts spans and merges overlapping or adjacent ones, dropping
// empty and inverted entries. Announcers use it to coalesce claims before
// sending; the tracker uses it to keep stored claims minimal.
func MergeSpans(spans []ChunkSpan) []ChunkSpan {
	valid := make([]ChunkSpan, 0, len(spans))
	for _, s := range spans {
		if s.Start < s.End {
			valid = append(valid, s)
		}
	}
	sort.Slice(valid, func(i, j int) bool {
		if valid[i].Start != valid[j].Start {
			return valid[i].Start < valid[j].Start
		}
		return valid[i].End < valid[j].End
	})
	merged := valid[:0]
	for _, s := range valid {
		if n := len(merged); n > 0 && s.Start <= merged[n-1].End {
			if s.End > merged[n-1].End {
				merged[n-1].End = s.End
			}
			continue
		}
		merged = append(merged, s)
	}
	return merged
}

const (
	// DefaultTTL is how long an announced claim stays valid without a
	// keepalive; announces every 5 minutes keep well within it.
	DefaultTTL = 15 * time.Minute
	// maxTTL caps client-requested TTLs so a bad client cannot pin
	// stale claims for hours.
	maxTTL = time.Hour
	// maxBodySize bounds announce and lookup request bodies.
	maxBodySize = 8 << 20
	// maxItems bounds how many identifiers one request may carry.
	maxItems = 8192
	// maxSpansPerXorb bounds both the ranges accepted per xorb per announce
	// and the merged ranges stored per peer per xorb.
	maxSpansPerXorb = 256
)

var hexSHA256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Handler is the tracker HTTP server: POST /v1/announce (write, gated by the
// auth function when set) and POST /v1/lookup (read, anonymous).
type Handler struct {
	dir  string
	auth func(token string) bool

	mu sync.Mutex
}

// Option configures the Handler.
type Option func(*Handler)

// WithStorageDir sets the directory holding the index files. Required.
func WithStorageDir(dir string) Option {
	return func(h *Handler) { h.dir = dir }
}

// WithAuth gates announces on a bearer-token validator, e.g. the cluster
// token issuer's Validate. Unset leaves announces open (trusted network).
func WithAuth(auth func(token string) bool) Option {
	return func(h *Handler) { h.auth = auth }
}

// NewHandler creates a tracker handler.
func NewHandler(opts ...Option) (*Handler, error) {
	h := &Handler{}
	for _, opt := range opts {
		opt(h)
	}
	if h.dir == "" {
		return nil, fmt.Errorf("tracker: storage dir is required")
	}
	for _, sub := range []string{"files", "keys", "xorbs"} {
		if err := os.MkdirAll(filepath.Join(h.dir, sub), 0755); err != nil {
			return nil, fmt.Errorf("tracker: create index dir: %w", err)
		}
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/announce":
		h.handleAnnounce(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/lookup":
		h.handleLookup(w, r)
	default:
		http.NotFound(w, r)
	}
}

// authenticate mirrors the Bearer parsing of xet's CAS server.
func (h *Handler) authenticate(r *http.Request) bool {
	if h.auth == nil {
		return true
	}
	parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return false
	}
	return h.auth(parts[1])
}

// decodeBody decodes exactly one bounded JSON value from the request.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodySize))
	if dec.Decode(v) != nil || dec.More() {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return false
	}
	return true
}

func (h *Handler) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var req AnnounceRequest
	if !decodeBody(w, r, &req) {
		return
	}
	peer := strings.TrimRight(strings.TrimSpace(req.Peer), "/")
	if peer == "" {
		http.Error(w, "Peer URL is required", http.StatusBadRequest)
		return
	}
	if len(req.Adds.Files)+len(req.Removes.Files)+len(req.Adds.Xorbs)+len(req.Removes.Xorbs) > maxItems {
		http.Error(w, "Too many entries", http.StatusBadRequest)
		return
	}
	ttl := DefaultTTL
	if req.TTL > 0 {
		if req.TTL >= int64(maxTTL/time.Second) {
			ttl = maxTTL
		} else {
			ttl = time.Duration(req.TTL) * time.Second
		}
	}
	now := time.Now()
	expiry := now.Add(ttl).Unix()

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, add := range req.Adds.Files {
		h.upsert(add, peer, expiry, now)
	}
	for _, add := range req.Adds.Xorbs {
		h.upsertXorb(add, peer, expiry, now)
	}
	for _, rm := range req.Removes.Files {
		h.withdraw(rm, peer, now)
	}
	for _, rm := range req.Removes.Xorbs {
		h.withdrawXorb(rm, peer, now)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(AnnounceResponse{OK: true})
}

func (h *Handler) handleLookup(w http.ResponseWriter, r *http.Request) {
	var req LookupRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if len(req.Files)+len(req.Keys)+len(req.Xorbs) > maxItems {
		http.Error(w, "Too many entries", http.StatusBadRequest)
		return
	}
	now := time.Now()
	resp := LookupResponse{}
	for _, sha := range req.Files {
		if e := h.read(h.filePath(sha)); e != nil {
			if loc, ok := e.location(now); ok {
				if resp.Files == nil {
					resp.Files = map[string]FileLocation{}
				}
				resp.Files[sha] = loc
			}
		}
	}
	for _, key := range req.Keys {
		if e := h.read(h.keyPath(key)); e != nil && e.Key == key {
			if loc, ok := e.location(now); ok {
				if resp.Keys == nil {
					resp.Keys = map[string]FileLocation{}
				}
				resp.Keys[key] = loc
			}
		}
	}
	for _, q := range req.Xorbs {
		if q.Range.Start >= q.Range.End {
			continue
		}
		e := h.readXorb(h.xorbPath(q.Hash))
		if e == nil {
			continue
		}
		if locs := e.locations(q.Range, now); len(locs) > 0 {
			if resp.Xorbs == nil {
				resp.Xorbs = map[string][]XorbLocation{}
			}
			resp.Xorbs[fmt.Sprintf("%s:%d-%d", q.Hash, q.Range.Start, q.Range.End)] = locs
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
