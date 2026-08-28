package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

// capCAS is a minimal upload.ClientAdapter capturing packed xorbs in memory,
// so in-package tests slice segments from real encoder output.
type capCAS struct {
	mu    sync.Mutex
	xorbs map[string][]byte
}

func (c *capCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.xorbs[xorbHash.String()]
	return ok, nil
}

func (c *capCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xorbs[xorbHash.String()] = data
	return &upload.XorbUploadResponse{WasInserted: true}, nil
}

func (c *capCAS) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return &upload.ShardUploadResponse{Result: 1}, nil
}

func (c *capCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, h := range chunkHashes {
		results[h] = &upload.DeduplicationResult{ChunkHash: h, IsNew: true}
	}
	return results, nil
}

// packedXorb is one real packed xorb built by the xet upload pipeline.
type packedXorb struct {
	hash    string
	data    []byte
	offsets []uint64
}

var (
	packedXorbOnce sync.Once
	packedXorbVal  packedXorb
	packedXorbErr  error
)

// buildPackedXorb uploads seeded pseudo-random data through the real chunker
// and encoder, yielding one multi-chunk xorb.
func buildPackedXorb(t *testing.T) packedXorb {
	t.Helper()
	packedXorbOnce.Do(func() {
		src := make([]byte, 2<<20)
		_, _ = mrand.New(mrand.NewSource(11)).Read(src)
		cas := &capCAS{xorbs: map[string][]byte{}}
		if _, err := upload.UploadFile(context.Background(), cas, bytes.NewReader(src)); err != nil {
			packedXorbErr = fmt.Errorf("upload fixture file: %w", err)
			return
		}
		if len(cas.xorbs) != 1 {
			packedXorbErr = fmt.Errorf("got %d fixture xorbs, want 1", len(cas.xorbs))
			return
		}
		for hash, data := range cas.xorbs {
			packedXorbVal.hash, packedXorbVal.data = hash, data
		}
		offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(packedXorbVal.data))
		if err != nil {
			packedXorbErr = fmt.Errorf("scan fixture xorb: %w", err)
			return
		}
		if len(offsets) < 8 {
			packedXorbErr = fmt.Errorf("fixture xorb has %d chunks, want at least 8", len(offsets))
			return
		}
		packedXorbVal.offsets = offsets
	})
	if packedXorbErr != nil {
		t.Fatal(packedXorbErr)
	}
	return packedXorbVal
}

// slice returns chunks [chunkStart, chunkEnd) of the fixture xorb with their
// packed byte range.
func (fx packedXorb) slice(t *testing.T, chunkStart, chunkEnd uint32) (byteStart, byteEnd int64, data []byte) {
	t.Helper()
	byteStart, byteEnd, err := xorb.ChunkDataRangeFromOffsets(fx.offsets, chunkStart, chunkEnd)
	if err != nil {
		t.Fatal(err)
	}
	return byteStart, byteEnd, fx.data[byteStart : byteEnd+1]
}

const captureFileHash = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// captureOrigin is a fake origin CAS: it serves a canned reconstruction
// response and the matching term byte ranges of the fixture xorb.
type captureOrigin struct {
	fx packedXorb
	// markRecon / markTerm stamp the agent marker on the respective
	// responses, simulating a cluster peer as the source.
	markRecon bool
	markTerm  bool
	// kind selects the reconstruction shape: "v1", "v2" or "batch".
	kind string
	// byteStart/byteEnd/chunkStart/chunkEnd describe the single advertised
	// fetch entry.
	chunkStart, chunkEnd uint32
	byteStart, byteEnd   int64
	// truncate makes the term response die halfway through the body.
	truncate bool
}

func (o *captureOrigin) reconstruction(termURL string) any {
	terms := []download.Term{{Hash: o.fx.hash, Range: download.ChunkRange{Start: o.chunkStart, End: o.chunkEnd}}}
	fetchInfo := map[string][]download.FetchInfoEntry{
		o.fx.hash: {{
			Range:    download.ChunkRange{Start: o.chunkStart, End: o.chunkEnd},
			URL:      termURL,
			URLRange: download.ByteRange{Start: o.byteStart, End: o.byteEnd},
		}},
	}
	switch o.kind {
	case "v2":
		return download.ReconstructionResponseV2{
			Terms: terms,
			Xorbs: map[string][]download.XorbMultiRangeFetch{
				o.fx.hash: {{
					URL: termURL,
					Ranges: []download.XorbRangeDescriptor{{
						Chunks: download.ChunkRange{Start: o.chunkStart, End: o.chunkEnd},
						Bytes:  download.ByteRange{Start: o.byteStart, End: o.byteEnd},
					}},
				}},
			},
		}
	case "batch":
		return download.BatchReconstructionResponse{
			Files:     map[string][]download.Term{captureFileHash: terms},
			FetchInfo: fetchInfo,
		}
	default:
		return download.ReconstructionResponseV1{Terms: terms, FetchInfo: fetchInfo}
	}
}

// reconstructionPath is the request path serving this origin's shape.
func (o *captureOrigin) reconstructionPath() string {
	if o.kind == "batch" {
		return "/reconstructions"
	}
	if o.kind == "" {
		return "/v1/reconstructions/" + captureFileHash
	}
	return "/" + o.kind + "/reconstructions/" + captureFileHash
}

func (o *captureOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/reconstructions/"+captureFileHash) || r.URL.Path == "/reconstructions":
		if o.markRecon {
			w.Header().Set(agentMarkerHeader, "1")
		}
		body, err := json.Marshal(o.reconstruction("http://" + r.Host + "/term/xorb"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	case r.URL.Path == "/term/xorb":
		m := byteRangeRe.FindStringSubmatch(r.Header.Get("Range"))
		if m == nil {
			http.Error(w, "Range required", http.StatusBadRequest)
			return
		}
		if o.markTerm {
			w.Header().Set(agentMarkerHeader, "1")
		}
		// Hostile advertised ranges may exceed the fixture; serve whatever
		// fits so the transport still sees a well-formed response.
		body := o.fx.data
		if o.byteStart >= 0 && o.byteEnd >= o.byteStart && o.byteEnd < int64(len(o.fx.data)) {
			body = o.fx.data[o.byteStart : o.byteEnd+1]
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		if o.truncate {
			_, _ = w.Write(body[:len(body)/2])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler) // kill the connection mid-body
		}
		_, _ = w.Write(body)
	default:
		http.NotFound(w, r)
	}
}

// captureHarness runs one origin and a capture transport over a fresh
// segment store, collecting notified spans.
type captureHarness struct {
	origin  *httptest.Server
	segs    *segstore.Store
	capture *captureTransport
	client  *http.Client

	mu       sync.Mutex
	notified []tracker.ChunkSpan
}

func newCaptureHarness(t *testing.T, o *captureOrigin) *captureHarness {
	t.Helper()
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h := &captureHarness{segs: segs}
	h.capture = newCaptureTransport(http.DefaultTransport, segs, func(hash string, span tracker.ChunkSpan) {
		h.mu.Lock()
		defer h.mu.Unlock()
		if hash == o.fx.hash {
			h.notified = append(h.notified, span)
		}
	}, nil)
	h.client = &http.Client{Transport: h.capture}
	h.origin = httptest.NewServer(o)
	t.Cleanup(h.origin.Close)
	return h
}

// getReconstruction fetches the canned reconstruction through the capture
// transport and returns the body it served.
func (h *captureHarness) getReconstruction(t *testing.T, path string) []byte {
	t.Helper()
	resp, err := h.client.Get(h.origin.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got reconstruction status = %d, want 200", resp.StatusCode)
	}
	return body
}

// getTerm fetches the term byte range through the capture transport.
func (h *captureHarness) getTerm(t *testing.T, byteStart, byteEnd int64) ([]byte, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, h.origin.URL+"/term/xorb", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", byteStart, byteEnd))
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(resp.Body)
}

// storedSegment reads one stored segment's bytes.
func storedSegment(t *testing.T, segs *segstore.Store, seg segstore.Segment) []byte {
	t.Helper()
	f, err := segs.Open(seg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCaptureStoresOriginSegments(t *testing.T) {
	fx := buildPackedXorb(t)
	for _, kind := range []string{"v1", "v2", "batch"} {
		t.Run(kind, func(t *testing.T) {
			byteStart, byteEnd, want := fx.slice(t, 2, 6)
			o := &captureOrigin{fx: fx, kind: kind, chunkStart: 2, chunkEnd: 6, byteStart: byteStart, byteEnd: byteEnd}
			h := newCaptureHarness(t, o)

			reconBody := h.getReconstruction(t, o.reconstructionPath())
			canned, err := json.Marshal(o.reconstruction(h.origin.URL + "/term/xorb"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reconBody, canned) {
				t.Fatalf("got reconstruction body %d bytes, want the canned response byte-identical", len(reconBody))
			}

			termBody, err := h.getTerm(t, byteStart, byteEnd)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(termBody, want) {
				t.Fatal("got term body differing from origin bytes")
			}

			seg, ok := h.segs.Find(fx.hash, 2, 6)
			if !ok {
				t.Fatal("got no stored segment, want chunks [2, 6) captured")
			}
			if seg.ByteStart != byteStart || seg.ByteEnd != byteEnd {
				t.Fatalf("got segment bytes [%d, %d], want [%d, %d]", seg.ByteStart, seg.ByteEnd, byteStart, byteEnd)
			}
			if got := storedSegment(t, h.segs, seg); !bytes.Equal(got, want) {
				t.Fatal("got stored segment bytes differing from origin bytes")
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			if len(h.notified) != 1 || h.notified[0] != (tracker.ChunkSpan{Start: 2, End: 6}) {
				t.Fatalf("got notified spans = %v, want [{2 6}]", h.notified)
			}
		})
	}
}

func TestCaptureSkipsUnwantedSources(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, _ := fx.slice(t, 2, 6)
	base := captureOrigin{fx: fx, chunkStart: 2, chunkEnd: 6, byteStart: byteStart, byteEnd: byteEnd}

	tests := []struct {
		name   string
		origin captureOrigin
	}{
		{"peer-marked reconstruction not recorded", func() captureOrigin { o := base; o.markRecon = true; return o }()},
		{"peer-marked term fetch not captured", func() captureOrigin { o := base; o.markTerm = true; return o }()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := tt.origin
			h := newCaptureHarness(t, &o)
			_ = h.getReconstruction(t, "/v1/reconstructions/"+captureFileHash)
			if _, err := h.getTerm(t, byteStart, byteEnd); err != nil {
				t.Fatal(err)
			}
			if seg, ok := h.segs.Find(fx.hash, 2, 6); ok {
				t.Fatalf("got captured segment %+v, want none", seg)
			}
		})
	}
}

func TestCapturePartialBodyDiscarded(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, _ := fx.slice(t, 2, 6)
	o := &captureOrigin{fx: fx, chunkStart: 2, chunkEnd: 6, byteStart: byteStart, byteEnd: byteEnd, truncate: true}
	h := newCaptureHarness(t, o)
	_ = h.getReconstruction(t, "/v1/reconstructions/"+captureFileHash)
	if _, err := h.getTerm(t, byteStart, byteEnd); err == nil {
		t.Fatal("got complete term body from truncating origin, want a read error")
	}
	if seg, ok := h.segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got captured segment %+v from a truncated body, want none", seg)
	}
}

// failingCloser yields a complete body but fails the close, which must veto
// the commit: a transport that cannot even close cleanly proves nothing
// about the stream it delivered.
type failingCloser struct {
	io.Reader
}

func (failingCloser) Close() error { return fmt.Errorf("close failed") }

func TestCaptureFailedCloseDiscarded(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	body := &captureBody{
		inner: failingCloser{Reader: bytes.NewReader(want)},
		t:     capture,
		info:  fetchInfo{xorbHash: fx.hash, chunkStart: 2, chunkEnd: 6},
		span:  byteSpan{start: byteStart, end: byteEnd},
		want:  int64(len(want)),
	}
	if got, err := io.ReadAll(body); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got read = %d bytes, %v; want the full body", len(got), err)
	}
	if err := body.Close(); err == nil {
		t.Fatal("got nil from Close, want the inner close error surfaced")
	}
	if seg, ok := segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got captured segment %+v despite failed close, want none", seg)
	}
}

// TestCaptureClosedTransportSkipsLateWrites: a body closing after the
// transport shut down must not touch the segment store.
func TestCaptureClosedTransportSkipsLateWrites(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	body := &captureBody{
		inner: io.NopCloser(bytes.NewReader(want)),
		t:     capture,
		info:  fetchInfo{xorbHash: fx.hash, chunkStart: 2, chunkEnd: 6},
		span:  byteSpan{start: byteStart, end: byteEnd},
		want:  int64(len(want)),
	}
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	capture.close()
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if seg, ok := segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got captured segment %+v written after transport close, want none", seg)
	}
}

// TestReconFailedCloseNotParsed: a reconstruction body whose close fails
// records nothing even when the accumulated JSON was complete.
func TestReconFailedCloseNotParsed(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, _ := fx.slice(t, 2, 6)
	o := &captureOrigin{fx: fx, chunkStart: 2, chunkEnd: 6, byteStart: byteStart, byteEnd: byteEnd}
	canned, err := json.Marshal(o.reconstruction("http://origin/term/xorb"))
	if err != nil {
		t.Fatal(err)
	}
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	body := &reconBody{inner: failingCloser{Reader: bytes.NewReader(canned)}, t: capture}
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err == nil {
		t.Fatal("got nil from Close, want the inner close error surfaced")
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.entries) != 0 {
		t.Fatalf("got %d recorded entries despite failed close, want 0", len(capture.entries))
	}
}

func TestCaptureOversizedSegmentSkipped(t *testing.T) {
	fx := buildPackedXorb(t)
	// The advertised byte range exceeds the capture cap; the origin still
	// answers with a small body, which must pass through uncaptured.
	o := &captureOrigin{fx: fx, chunkStart: 0, chunkEnd: 4, byteStart: 0, byteEnd: maxCaptureSegment + 1}
	h := newCaptureHarness(t, o)
	_ = h.getReconstruction(t, "/v1/reconstructions/"+captureFileHash)
	if _, err := h.getTerm(t, 0, maxCaptureSegment+1); err != nil {
		t.Fatal(err)
	}
	if seg, ok := h.segs.Find(fx.hash, 0, 4); ok {
		t.Fatalf("got captured segment %+v over the size cap, want none", seg)
	}
}

func TestCaptureHostileByteRangeHarmless(t *testing.T) {
	fx := buildPackedXorb(t)
	// A malicious reconstruction advertising the maximum byte offset must
	// neither be recorded nor break the matching term request downstream.
	o := &captureOrigin{fx: fx, chunkStart: 0, chunkEnd: 4, byteStart: 0, byteEnd: math.MaxInt64}
	h := newCaptureHarness(t, o)
	_ = h.getReconstruction(t, "/v1/reconstructions/"+captureFileHash)
	body, err := h.getTerm(t, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, fx.data) {
		t.Fatal("got term body differing from origin bytes under hostile range")
	}
	if seg, ok := h.segs.Find(fx.hash, 0, 4); ok {
		t.Fatalf("got captured segment %+v from hostile range, want none", seg)
	}
	h.capture.mu.Lock()
	defer h.capture.mu.Unlock()
	if len(h.capture.entries) != 0 {
		t.Fatalf("got %d recorded entries from hostile range, want 0", len(h.capture.entries))
	}
}

func TestCaptureWrongLengthOKStatusSkipped(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Status 200 with a Content-Length different from the requested
		// range: not the exact-whole-range shape, so no capture.
		_, _ = w.Write(want[:len(want)-1])
	}))
	defer origin.Close()

	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	capture.record(origin.URL+"/term/xorb", byteStart, byteEnd, fx.hash, 2, 6, captureFileHash, nil)

	req, err := http.NewRequest(http.MethodGet, origin.URL+"/term/xorb", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", byteStart, byteEnd))
	resp, err := (&http.Client{Transport: capture}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if seg, ok := segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got captured segment %+v from a wrong-length 200, want none", seg)
	}
}

func TestCaptureOversizedReconstructionPassesThrough(t *testing.T) {
	pad := bytes.Repeat([]byte("x"), maxReconstructionBody+2)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Chunked (unknown length), so the size gate must come from reading.
		_, _ = w.Write(pad)
	}))
	defer origin.Close()

	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	resp, err := (&http.Client{Transport: capture}).Get(origin.URL + "/v1/reconstructions/" + captureFileHash)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, pad) {
		t.Fatalf("got %d passthrough bytes, want %d unchanged", len(body), len(pad))
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.entries) != 0 {
		t.Fatalf("got %d recorded entries from an oversized body, want 0", len(capture.entries))
	}
}

func TestCaptureFetchTableBounds(t *testing.T) {
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fx := buildPackedXorb(t)
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)

	t.Run("oldest evicted past the cap", func(t *testing.T) {
		for i := range maxFetchEntries + 1 {
			capture.record(fmt.Sprintf("http://origin/term/%d", i), 0, 99, fx.hash, 0, 4, captureFileHash, nil)
		}
		if _, ok := capture.lookup(fetchKey{url: "http://origin/term/0", start: 0, end: 99}); ok {
			t.Fatal("got oldest entry still present past the cap, want evicted")
		}
		last := fmt.Sprintf("http://origin/term/%d", maxFetchEntries)
		if _, ok := capture.lookup(fetchKey{url: last, start: 0, end: 99}); !ok {
			t.Fatal("got newest entry missing, want present")
		}
		if got := len(capture.entries); got != maxFetchEntries {
			t.Fatalf("got %d entries, want %d", got, maxFetchEntries)
		}
	})

	t.Run("expired entries are dead and pruned", func(t *testing.T) {
		k := fetchKey{url: "http://origin/term/expired", start: 0, end: 99}
		capture.mu.Lock()
		capture.entries[k] = tableEntry{info: fetchInfo{xorbHash: fx.hash, chunkStart: 0, chunkEnd: 4}, expires: time.Now().Add(-time.Second)}
		capture.mu.Unlock()
		if _, ok := capture.lookup(k); ok {
			t.Fatal("got expired entry from lookup, want a miss")
		}
	})

	t.Run("invalid entries never recorded", func(t *testing.T) {
		before := len(capture.entries)
		capture.record("http://origin/x", 10, 5, fx.hash, 0, 4, captureFileHash, nil)      // inverted bytes
		capture.record("http://origin/x", 0, 99, fx.hash, 4, 4, captureFileHash, nil)      // empty chunks
		capture.record("http://origin/x", 0, 99, "not-a-hash", 0, 4, captureFileHash, nil) // hostile hash
		capture.record("", 0, 99, fx.hash, 0, 4, captureFileHash, nil)                     // missing url
		capture.record("http://origin/x", -1, 99, fx.hash, 0, 4, captureFileHash, nil)     // negative start
		if got := len(capture.entries); got != before {
			t.Fatalf("got %d entries after invalid records, want %d", got, before)
		}
	})
}
