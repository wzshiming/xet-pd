package agent

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"

	"github.com/wzshiming/xet-pd/segstore"
)

// chunksHeader asks the xorb route for packed chunks [start, end) in the
// serving node's own encoding, immune to the byte-offset drift a re-encoded
// xorb would have across peers.
const chunksHeader = "X-Xet-PD-Chunks"

// xorbPathRe is the CAS xorb download route shape the pinned server routes on.
var xorbPathRe = regexp.MustCompile(`^/v1/xorbs/([^/]+)/([0-9a-f]{64})$`)

// chunksValueRe is the chunksHeader value shape: "{start}-{end}".
var chunksValueRe = regexp.MustCompile(`^([0-9]+)-([0-9]+)$`)

// byteRangeRe is the only Range form the gate serves: a single bounded
// range. Open-ended, suffix and multi ranges cannot be answered from a
// partial xorb.
var byteRangeRe = regexp.MustCompile(`^bytes=([0-9]+)-([0-9]+)$`)

// segmentGate answers xorb downloads from stored segments when the full xorb
// is not in local storage: chunk-keyed and single bounded byte-range requests
// that a segment covers are served from it, everything else is a plain 404 so
// the caller's fallback chain moves on. Full xorbs pass through untouched to
// the inner CAS server. The gate answers purely locally and never ingests.
type segmentGate struct {
	next http.Handler
	stor storage.Storage
	segs *segstore.Store
}

func newSegmentGate(next http.Handler, stor storage.Storage, segs *segstore.Store) *segmentGate {
	return &segmentGate{next: next, stor: stor, segs: segs}
}

func (g *segmentGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		g.next.ServeHTTP(w, r)
		return
	}
	m := xorbPathRe.FindStringSubmatch(r.URL.Path)
	if m == nil {
		g.next.ServeHTTP(w, r)
		return
	}
	hash, err := xet.ParseXorbHash(m[2])
	if err != nil {
		g.next.ServeHTTP(w, r)
		return
	}
	has, err := g.stor.HasXorb(r.Context(), m[1], hash)
	if err != nil {
		g.next.ServeHTTP(w, r)
		return
	}
	// Repeated header lines would smuggle a multi-range past the
	// single-range parsers, so anything but exactly one value is a miss.
	if vs := r.Header.Values(chunksHeader); len(vs) > 0 {
		if len(vs) != 1 {
			http.NotFound(w, r)
			return
		}
		if has {
			g.serveStoredChunks(w, r, m[1], hash, vs[0])
			return
		}
		g.serveChunks(w, r, m[2], vs[0])
		return
	}
	if has {
		g.next.ServeHTTP(w, r)
		return
	}
	if vs := r.Header.Values("Range"); len(vs) > 0 {
		if len(vs) == 1 {
			g.serveByteRange(w, r, m[2], vs[0])
			return
		}
		http.NotFound(w, r)
		return
	}
	http.NotFound(w, r)
}

// parseChunksValue parses the chunksHeader value into a chunk range.
func parseChunksValue(value string) (chunkStart, chunkEnd uint32, ok bool) {
	m := chunksValueRe.FindStringSubmatch(value)
	if m == nil {
		return 0, 0, false
	}
	start, err1 := strconv.ParseUint(m[1], 10, 32)
	end, err2 := strconv.ParseUint(m[2], 10, 32)
	if err1 != nil || err2 != nil || start >= end {
		return 0, 0, false
	}
	return uint32(start), uint32(end), true
}

// serveStoredChunks answers a chunk-keyed request from the full local xorb:
// exactly the packed bytes of chunks [start, end) in this node's encoding,
// status 200 — the same shape the segment-backed path answers with, so
// chunk-keyed fetchers need not care which one holds the data.
func (g *segmentGate) serveStoredChunks(w http.ResponseWriter, r *http.Request, namespace string, hash xet.XorbHash, value string) {
	chunkStart, chunkEnd, ok := parseChunksValue(value)
	if !ok {
		http.NotFound(w, r)
		return
	}
	startByte, endByte, err := g.stor.GetXorbDataRange(r.Context(), namespace, hash, chunkStart, chunkEnd)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	f, err := g.stor.GetXorbReadSeekCloser(r.Context(), namespace, hash)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer func() {
		_ = f.Close()
	}()
	g.serveSegment(w, r, f, startByte, endByte, http.StatusOK, "")
}

// serveChunks answers a chunk-keyed request from a covering segment: exactly
// the packed bytes of chunks [start, end), status 200.
func (g *segmentGate) serveChunks(w http.ResponseWriter, r *http.Request, xorbHash, value string) {
	chunkStart, chunkEnd, ok := parseChunksValue(value)
	if !ok {
		http.NotFound(w, r)
		return
	}
	seg, ok := g.segs.Find(xorbHash, chunkStart, chunkEnd)
	if !ok {
		http.NotFound(w, r)
		return
	}
	f, ok := g.openSegment(seg)
	if !ok {
		http.NotFound(w, r)
		return
	}
	defer func() { _ = f.Close() }()
	startByte, endByte, err := seg.ChunkRange(f, chunkStart, chunkEnd)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	g.serveSegment(w, r, f, startByte, endByte, http.StatusOK, "")
}

// serveByteRange answers a single bounded byte range from a segment covering
// it, status 206 with an unknown complete length.
func (g *segmentGate) serveByteRange(w http.ResponseWriter, r *http.Request, xorbHash, value string) {
	m := byteRangeRe.FindStringSubmatch(value)
	if m == nil {
		http.NotFound(w, r)
		return
	}
	start, err1 := strconv.ParseInt(m[1], 10, 64)
	end, err2 := strconv.ParseInt(m[2], 10, 64)
	if err1 != nil || err2 != nil || start > end {
		http.NotFound(w, r)
		return
	}
	seg, ok := g.segs.FindByteRange(xorbHash, start, end)
	if !ok {
		http.NotFound(w, r)
		return
	}
	f, ok := g.openSegment(seg)
	if !ok {
		http.NotFound(w, r)
		return
	}
	defer func() { _ = f.Close() }()
	contentRange := fmt.Sprintf("bytes %d-%d/*", start, end)
	g.serveSegment(w, r, f, start-seg.ByteStart, end-seg.ByteStart, http.StatusPartialContent, contentRange)
}

// openSegment opens the segment data and checks it against the meta on this
// one handle — size, then the full chunk-header walk, so even an equal-size
// corrupted file is a miss before any header is written. All offsets and
// bytes then come from the same handle, immune to a concurrent replacement.
func (g *segmentGate) openSegment(seg segstore.Segment) (io.ReadSeekCloser, bool) {
	f, err := g.segs.Open(seg)
	if err != nil {
		return nil, false
	}
	if size, err := f.Seek(0, io.SeekEnd); err != nil || size != seg.Size {
		_ = f.Close()
		return nil, false
	}
	if start, end, err := seg.ChunkRange(f, seg.ChunkStart, seg.ChunkEnd); err != nil || start != 0 || end != seg.Size-1 {
		_ = f.Close()
		return nil, false
	}
	return f, true
}

// serveSegment writes bytes [startByte, endByte] of the validated open data
// file; HEAD sends the same headers without the body.
func (g *segmentGate) serveSegment(w http.ResponseWriter, r *http.Request, f io.ReadSeeker, startByte, endByte int64, status int, contentRange string) {
	if _, err := f.Seek(startByte, io.SeekStart); err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(endByte-startByte+1, 10))
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = io.CopyN(w, f, endByte-startByte+1)
	}
}
