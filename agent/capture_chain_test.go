package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/storage"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

// chainEntry is one advertised fetch entry of the chain origin's canned
// reconstruction. The byte range may deliberately disagree with the true
// packed range to exercise fall-through paths.
type chainEntry struct {
	chunkStart, chunkEnd uint32
	byteStart, byteEnd   int64
}

// chainOrigin is a fake origin CAS serving a canned V1 reconstruction over
// any number of fetch entries plus the matching term byte ranges, counting
// term hits per range.
type chainOrigin struct {
	fx      packedXorb
	entries []chainEntry
	// terms, when set, overrides the reconstruction's terms — letting a
	// fetch entry advertise more chunks than the file consumes; nil mirrors
	// the entries one to one.
	terms []download.ChunkRange

	mu       sync.Mutex
	termHits map[string]int
}

func (o *chainOrigin) reconstruction(termURL string) download.ReconstructionResponseV1 {
	var terms []download.Term
	if o.terms != nil {
		for _, r := range o.terms {
			terms = append(terms, download.Term{Hash: o.fx.hash, Range: r})
		}
	} else {
		for _, e := range o.entries {
			terms = append(terms, download.Term{Hash: o.fx.hash, Range: download.ChunkRange{Start: e.chunkStart, End: e.chunkEnd}})
		}
	}
	var fetches []download.FetchInfoEntry
	for _, e := range o.entries {
		fetches = append(fetches, download.FetchInfoEntry{
			Range:    download.ChunkRange{Start: e.chunkStart, End: e.chunkEnd},
			URL:      termURL,
			URLRange: download.ByteRange{Start: e.byteStart, End: e.byteEnd},
		})
	}
	return download.ReconstructionResponseV1{
		Terms:     terms,
		FetchInfo: map[string][]download.FetchInfoEntry{o.fx.hash: fetches},
	}
}

func (o *chainOrigin) termCount(byteStart, byteEnd int64) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.termHits[fmt.Sprintf("%d-%d", byteStart, byteEnd)]
}

func (o *chainOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/reconstructions/" + captureFileHash:
		body, err := json.Marshal(o.reconstruction("http://" + r.Host + "/term/xorb"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	case "/term/xorb":
		m := byteRangeRe.FindStringSubmatch(r.Header.Get("Range"))
		if m == nil {
			http.Error(w, "Range required", http.StatusBadRequest)
			return
		}
		start, _ := strconv.ParseInt(m[1], 10, 64)
		end, _ := strconv.ParseInt(m[2], 10, 64)
		o.mu.Lock()
		o.termHits[fmt.Sprintf("%d-%d", start, end)]++
		o.mu.Unlock()
		if start < 0 || end < start || end >= int64(len(o.fx.data)) {
			http.Error(w, "range out of bounds", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := o.fx.data[start : end+1]
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.fx.data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	default:
		http.NotFound(w, r)
	}
}

// cannedTracker answers /v1/lookup with a fixed xorb location map and
// counts lookups, standing in for the real tracker where candidate order
// must be deterministic. Answers may be swapped and lookups may be failed
// mid-test to model peers announcing (or the tracker dying) between calls.
type cannedTracker struct {
	mu      sync.Mutex
	answers map[string][]tracker.XorbLocation
	fail    bool
	lookups atomic.Int64
}

func (ct *cannedTracker) setAnswers(answers map[string][]tracker.XorbLocation) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.answers = answers
}

func (ct *cannedTracker) setFail(fail bool) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.fail = fail
}

func (ct *cannedTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/lookup" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	ct.lookups.Add(1)
	ct.mu.Lock()
	answers, fail := ct.answers, ct.fail
	ct.mu.Unlock()
	if fail {
		http.Error(w, "tracker down", http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(tracker.LookupResponse{Xorbs: answers})
}

// chainPeer is a fake cluster peer serving term byte ranges of the fixture
// xorb, with knobs for every rejection path.
type chainPeer struct {
	fx       packedXorb
	noMarker bool
	status   int // 0 means 206
	corrupt  bool
	short    bool
	hits     atomic.Int64
}

func (p *chainPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.hits.Add(1)
	m := byteRangeRe.FindStringSubmatch(r.Header.Get("Range"))
	if m == nil {
		http.Error(w, "Range required", http.StatusBadRequest)
		return
	}
	start, _ := strconv.ParseInt(m[1], 10, 64)
	end, _ := strconv.ParseInt(m[2], 10, 64)
	if start < 0 || end < start || end >= int64(len(p.fx.data)) {
		http.Error(w, "range out of bounds", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	body := append([]byte(nil), p.fx.data[start:end+1]...)
	if p.corrupt {
		// Break the first chunk header so the length still matches but the
		// structural walk cannot.
		body[0] ^= 0xff
		body[3] ^= 0xff
	}
	if p.short {
		body = body[:len(body)-1]
	}
	if !p.noMarker {
		w.Header().Set(agentMarkerHeader, "1")
	}
	status := p.status
	if status == 0 {
		status = http.StatusPartialContent
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, end))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// chainConfig assembles one source-chain harness.
type chainConfig struct {
	entries []chainEntry
	// terms overrides the reconstruction's terms; nil mirrors the entries.
	terms []download.ChunkRange
	// answers, when non-nil, runs a canned tracker with these lookup
	// answers; nil configures no tracker at all.
	answers map[string][]tracker.XorbLocation
	stor    storage.Storage
	self    string
}

// chainHarness drives the capture transport's source chain directly against
// a fake origin, canned tracker and fake peers.
type chainHarness struct {
	origin  *httptest.Server
	o       *chainOrigin
	tracker *cannedTracker
	segs    *segstore.Store
	quar    *quarantine
	capture *captureTransport
	client  *http.Client

	mu       sync.Mutex
	notified []tracker.ChunkSpan
}

func newChainHarness(t *testing.T, cfg chainConfig) *chainHarness {
	t.Helper()
	fx := buildPackedXorb(t)
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h := &chainHarness{
		o:    &chainOrigin{fx: fx, entries: cfg.entries, terms: cfg.terms, termHits: map[string]int{}},
		segs: segs,
		quar: newQuarantine(),
	}
	var tc *tracker.Client
	if cfg.answers != nil {
		h.tracker = &cannedTracker{answers: cfg.answers}
		srv := httptest.NewServer(h.tracker)
		t.Cleanup(srv.Close)
		tc, err = tracker.NewClient(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
	}
	chain := newSourceChain(cfg.stor, tc, cfg.self, 2*time.Second, 8, h.quar)
	h.capture = newCaptureTransport(http.DefaultTransport, segs, func(hash string, span tracker.ChunkSpan) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.notified = append(h.notified, span)
	}, chain)
	h.client = &http.Client{Transport: h.capture}
	h.origin = httptest.NewServer(h.o)
	t.Cleanup(h.origin.Close)
	return h
}

// recon fetches and closes the canned reconstruction, which records the
// fetch entries and warms the peer cache.
func (h *chainHarness) recon(t *testing.T) {
	t.Helper()
	resp, err := h.client.Get(h.origin.URL + "/v1/reconstructions/" + captureFileHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got reconstruction status = %d, want 200", resp.StatusCode)
	}
}

// term fetches one term byte range through the transport.
func (h *chainHarness) term(t *testing.T, byteStart, byteEnd int64) (*http.Response, []byte) {
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func (h *chainHarness) notifiedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.notified)
}

// spanKey is the lookup response key for one chunk span of the fixture.
func spanKey(fx packedXorb, chunkStart, chunkEnd uint32) string {
	return fmt.Sprintf("%s:%d-%d", fx.hash, chunkStart, chunkEnd)
}

func newChainPeerServer(t *testing.T, p *chainPeer) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return srv
}

// TestChainServesLocalSegment: a term fetch covered by a stored segment is
// answered without contacting the origin, byte-identical to what the origin
// would have served.
func TestChainServesLocalSegment(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{entries: []chainEntry{{2, 6, byteStart, byteEnd}}})
	if err := h.segs.Put(fx.hash, 2, 6, byteStart, byteEnd, want); err != nil {
		t.Fatal(err)
	}
	h.recon(t)
	resp, body := h.term(t, byteStart, byteEnd)
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("got status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, want) {
		t.Fatal("got term body differing from the origin bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served locally)", got)
	}
}

// TestChainServesLocalSegmentSubRange: a stored segment containing the
// requested bytes serves a sub-slice via offset math.
func TestChainServesLocalSegmentSubRange(t *testing.T) {
	fx := buildPackedXorb(t)
	segStart, segEnd, segData := fx.slice(t, 1, 7)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{entries: []chainEntry{{2, 6, byteStart, byteEnd}}})
	if err := h.segs.Put(fx.hash, 1, 7, segStart, segEnd, segData); err != nil {
		t.Fatal(err)
	}
	h.recon(t)
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got sub-range body differing from the origin bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served locally)", got)
	}
}

// TestChainSynthesizedResponseShape pins the synthesized 206 to what the
// pinned consumer verifiably reads: the status line, the Content-Range
// starting exactly at the requested offset with a "*" total (httpseek then
// hands the body through unwrapped), and the exact content length.
func TestChainSynthesizedResponseShape(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{entries: []chainEntry{{2, 6, byteStart, byteEnd}}})
	if err := h.segs.Put(fx.hash, 2, 6, byteStart, byteEnd, want); err != nil {
		t.Fatal(err)
	}
	h.recon(t)
	resp, body := h.term(t, byteStart, byteEnd)
	if resp.Status != "206 Partial Content" || resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("got status %q (%d), want \"206 Partial Content\" (206)", resp.Status, resp.StatusCode)
	}
	if resp.Proto != "HTTP/1.1" || resp.ProtoMajor != 1 || resp.ProtoMinor != 1 {
		t.Fatalf("got proto %q (%d.%d), want HTTP/1.1", resp.Proto, resp.ProtoMajor, resp.ProtoMinor)
	}
	if want := int64(len(want)); resp.ContentLength != want {
		t.Fatalf("got ContentLength = %d, want %d", resp.ContentLength, want)
	}
	if got, want := resp.Header.Get("Content-Length"), strconv.Itoa(len(want)); got != want {
		t.Fatalf("got Content-Length header = %q, want %q", got, want)
	}
	if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/*", byteStart, byteEnd); got != want {
		t.Fatalf("got Content-Range = %q, want %q", got, want)
	}
	if got := int64(len(body)); got != resp.ContentLength {
		t.Fatalf("got %d body bytes, want %d matching ContentLength", got, resp.ContentLength)
	}
}

// newChainStorage builds a FileStorage already holding the fixture xorb.
func newChainStorage(t *testing.T, fx packedXorb) storage.Storage {
	t.Helper()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := xet.ParseXorbHash(fx.hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stor.PutXorb(context.Background(), "default", hash, bytes.NewReader(fx.data)); err != nil {
		t.Fatal(err)
	}
	return stor
}

// TestChainServesLocalXorb: with the full xorb in local storage and the
// local packed span exactly as long as the advertised one, the term fetch
// never reaches the origin.
func TestChainServesLocalXorb(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		stor:    newChainStorage(t, fx),
	})
	h.recon(t)
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got term body differing from the stored xorb bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served from the local xorb)", got)
	}
	if seg, ok := h.segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got stored segment %+v from a local-xorb serve, want none", seg)
	}
}

// TestChainLocalXorbLengthMismatchFallsBack: when the advertised byte range
// disagrees with the local packed span length, the encodings differ and the
// fetch falls through to the origin.
func TestChainLocalXorbLengthMismatchFallsBack(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, _ := fx.slice(t, 2, 6)
	// One byte short of the true span: the origin honors it, local must not.
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd - 1}},
		stor:    newChainStorage(t, fx),
	})
	h.recon(t)
	_, body := h.term(t, byteStart, byteEnd-1)
	if !bytes.Equal(body, fx.data[byteStart:byteEnd]) {
		t.Fatal("got fall-through body differing from the origin bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd-1); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1 (length mismatch falls back)", got)
	}
}

// TestChainPeerHitQuarantines: a validated peer body is served with zero
// origin traffic and stored quarantined — invisible to Find, never
// notified to the announcer, registered under the file hash for the
// verdict.
func TestChainPeerHitQuarantines(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	peer := &chainPeer{fx: fx}
	peerSrv := newChainPeerServer(t, peer)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{
			spanKey(fx, 2, 6): {{Peer: peerSrv.URL}},
		},
	})
	h.recon(t)
	if got := h.tracker.lookups.Load(); got != 1 {
		t.Fatalf("got %d tracker lookups after one reconstruction, want 1", got)
	}
	resp, body := h.term(t, byteStart, byteEnd)
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("got status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, want) {
		t.Fatal("got peer-served body differing from the origin bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served by the peer)", got)
	}
	if got := peer.hits.Load(); got != 1 {
		t.Fatalf("got %d peer fetches, want 1", got)
	}
	if seg, ok := h.segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got quarantined segment %+v from Find, want a miss", seg)
	}
	var stored []segstore.Segment
	if err := h.segs.Walk(func(seg segstore.Segment) error {
		stored = append(stored, seg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 || !stored[0].Quarantined {
		t.Fatalf("got stored segments %+v, want exactly one quarantined", stored)
	}
	if stored[0].ByteStart != byteStart || stored[0].ByteEnd != byteEnd {
		t.Fatalf("got segment bytes [%d, %d], want [%d, %d]", stored[0].ByteStart, stored[0].ByteEnd, byteStart, byteEnd)
	}
	if stored[0].FileHash != captureFileHash || stored[0].Peer != canonicalPeerURL(peerSrv.URL) || stored[0].QuarantinedAt <= 0 {
		t.Fatalf("got persisted ownership (%q, %q, %d), want (%q, %q, >0)",
			stored[0].FileHash, stored[0].Peer, stored[0].QuarantinedAt, captureFileHash, canonicalPeerURL(peerSrv.URL))
	}
	if got := h.notifiedCount(); got != 0 {
		t.Fatalf("got %d announcer notifications for a quarantined segment, want 0", got)
	}
	items := h.quar.take(captureFileHash)
	wantItem := quarItem{xorbHash: fx.hash, span: tracker.ChunkSpan{Start: 2, End: 6}, peer: canonicalPeerURL(peerSrv.URL)}
	if len(items) == 1 {
		// The registration timestamp only drives the sweep.
		items[0].added = time.Time{}
	}
	if len(items) != 1 || items[0] != wantItem {
		t.Fatalf("got quarantine items = %+v, want [%+v]", items, wantItem)
	}
}

// TestChainPeerRejectedFallsBack: every vetting failure — missing agent
// marker, wrong status, wrong length, structurally broken bytes — cools the
// peer and falls back to the origin with correct client bytes.
func TestChainPeerRejectedFallsBack(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)

	tests := []struct {
		name string
		peer *chainPeer
	}{
		{"missing marker header", &chainPeer{fx: fx, noMarker: true}},
		{"non-206 status", &chainPeer{fx: fx, status: http.StatusOK}},
		{"wrong content length", &chainPeer{fx: fx, short: true}},
		{"garbage structure right length", &chainPeer{fx: fx, corrupt: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peerSrv := newChainPeerServer(t, tt.peer)
			h := newChainHarness(t, chainConfig{
				entries: []chainEntry{{2, 6, byteStart, byteEnd}},
				answers: map[string][]tracker.XorbLocation{
					spanKey(fx, 2, 6): {{Peer: peerSrv.URL}},
				},
			})
			h.recon(t)
			_, body := h.term(t, byteStart, byteEnd)
			if !bytes.Equal(body, want) {
				t.Fatal("got fallback body differing from the origin bytes")
			}
			if got := h.o.termCount(byteStart, byteEnd); got != 1 {
				t.Fatalf("got %d origin term fetches, want 1 (peer rejected)", got)
			}
			if got := tt.peer.hits.Load(); got != 1 {
				t.Fatalf("got %d peer fetches, want 1", got)
			}
			if !h.capture.cooledDown(canonicalPeerURL(peerSrv.URL)) {
				t.Fatal("got rejected peer not cooling down, want cooldown")
			}
			if items := h.quar.take(captureFileHash); len(items) != 0 {
				t.Fatalf("got quarantine items = %+v from a rejected peer, want none", items)
			}
		})
	}
}

// TestChainPeerNotServedWhenBookkeepingFails: a validated peer body whose
// quarantine bookkeeping cannot complete — the transport is draining, the
// registry refuses the file, or a different stored encoding owns the
// segment key — is never served: the peer cools down and the term falls
// back to the origin with correct bytes.
func TestChainPeerNotServedWhenBookkeepingFails(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)

	tests := []struct {
		name string
		prep func(t *testing.T, h *chainHarness)
	}{
		{"transport closed", func(t *testing.T, h *chainHarness) {
			h.capture.close()
		}},
		{"registry refuses the file", func(t *testing.T, h *chainHarness) {
			filler := quarItem{xorbHash: fx.hash, span: tracker.ChunkSpan{Start: 0, End: 1}}
			for i := 0; i < maxQuarantineFiles; i++ {
				if !h.quar.add(fmt.Sprintf("%064d", i), filler) {
					t.Fatalf("got add %d = false while filling the registry, want true", i)
				}
			}
		}},
		{"conflicting stored encoding", func(t *testing.T, h *chainHarness) {
			foreign := bytes.Repeat(packOneChunk(t, "foreign"), 4)
			if created, err := h.segs.PutQuarantined(fx.hash, 2, 6, 0, int64(len(foreign))-1, foreign, captureFileHash, "http://127.0.0.1:9"); err != nil || !created {
				t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &chainPeer{fx: fx}
			peerSrv := newChainPeerServer(t, peer)
			h := newChainHarness(t, chainConfig{
				entries: []chainEntry{{2, 6, byteStart, byteEnd}},
				answers: map[string][]tracker.XorbLocation{
					spanKey(fx, 2, 6): {{Peer: peerSrv.URL}},
				},
			})
			h.recon(t)
			tt.prep(t, h)
			_, body := h.term(t, byteStart, byteEnd)
			if !bytes.Equal(body, want) {
				t.Fatal("got fallback body differing from the origin bytes")
			}
			if got := h.o.termCount(byteStart, byteEnd); got != 1 {
				t.Fatalf("got %d origin term fetches, want 1 (peer bytes refused)", got)
			}
			if got := peer.hits.Load(); got != 1 {
				t.Fatalf("got %d peer fetches, want 1", got)
			}
			if !h.capture.cooledDown(canonicalPeerURL(peerSrv.URL)) {
				t.Fatal("got refused peer not cooling down, want cooldown")
			}
			if items := h.quar.take(captureFileHash); len(items) != 0 {
				t.Fatalf("got quarantine items = %+v after refused bookkeeping, want none", items)
			}
		})
	}
}

// TestSweepQuarantineAfterCloseIsNoop: the sweep mutates segment storage,
// so it registers as a write — after close it does nothing, keeping the
// contract that nothing touches the store once close returns.
func TestSweepQuarantineAfterCloseIsNoop(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, data := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{entries: []chainEntry{{2, 6, byteStart, byteEnd}}})
	if created, err := h.segs.PutQuarantined(fx.hash, 2, 6, byteStart, byteEnd, data, captureFileHash, "http://127.0.0.1:9"); err != nil || !created {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
	}
	if !h.quar.add(captureFileHash, quarItem{xorbHash: fx.hash, span: tracker.ChunkSpan{Start: 2, End: 6}, peer: "http://127.0.0.1:9"}) {
		t.Fatal("got quarantine add = false, want registered")
	}
	// Backdate the item past the TTL: a live sweep would purge it.
	h.quar.mu.Lock()
	h.quar.files[captureFileHash][0].added = time.Now().Add(-2 * quarantineTTL)
	h.quar.mu.Unlock()

	h.capture.close()
	h.capture.sweepQuarantine()
	// The durable-meta walk is a write like the registry path: a post-close
	// sweep must not purge even a segment far past the TTL.
	h.capture.sweepQuarantineBefore(time.Now().Add(time.Hour))
	if items := h.quar.take(captureFileHash); len(items) != 1 {
		t.Fatalf("got %d registry items after a post-close sweep, want 1 untouched", len(items))
	}
	var onDisk bool
	if err := h.segs.Walk(func(seg segstore.Segment) error {
		if seg.XorbHash == fx.hash {
			onDisk = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !onDisk {
		t.Fatal("got segment deleted by a post-close sweep, want untouched")
	}
}

// segmentOnDisk reports whether any stored segment (quarantined or not)
// exists for the xorb, bypassing Find's quarantine filter.
func segmentOnDisk(t *testing.T, segs *segstore.Store, xorbHash string) bool {
	t.Helper()
	var onDisk bool
	if err := segs.Walk(func(seg segstore.Segment) error {
		if seg.XorbHash == xorbHash {
			onDisk = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return onDisk
}

// TestSweepPurgesRecordlessQuarantinedSegment — Finding B regression: a
// quarantined segment whose registry record did not survive a restart is
// invisible to Find while it waits, kept until the TTL passes, and then
// purged by the sweep from its persisted ownership alone — including the
// peer cooldown and the chunk-cache purge a failed verdict would apply.
func TestSweepPurgesRecordlessQuarantinedSegment(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, data := fx.slice(t, 2, 6)
	const peer = "http://127.0.0.1:9"
	dir := t.TempDir()
	before, err := segstore.New(segstore.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if created, err := before.PutQuarantined(fx.hash, 2, 6, byteStart, byteEnd, data, captureFileHash, peer); err != nil || !created {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
	}

	// Restart: a fresh store over the same directory, an empty registry.
	segs, err := segstore.New(segstore.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, newSourceChain(nil, nil, "", time.Second, 1, newQuarantine()))
	var purgedChunks []string
	capture.purgeChunks = func(hash string) { purgedChunks = append(purgedChunks, hash) }

	if seg, ok := segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got Find = %+v for a recordless quarantined segment, want a miss", seg)
	}
	capture.sweepQuarantine()
	if !segmentOnDisk(t, segs, fx.hash) {
		t.Fatal("got quarantined segment purged within the TTL, want kept for a possible settle")
	}
	capture.sweepQuarantineBefore(time.Now().Add(time.Hour))
	if segmentOnDisk(t, segs, fx.hash) {
		t.Fatal("got quarantined segment still on disk past the TTL, want purged from its meta")
	}
	if !capture.cooledDown(peer) {
		t.Fatal("got supplying peer not cooled by the meta-driven purge, want cooldown")
	}
	if len(purgedChunks) != 1 || purgedChunks[0] != fx.hash {
		t.Fatalf("got chunk-cache purges %v, want exactly [%s]", purgedChunks, fx.hash)
	}
}

// TestSweepPurgesQuarantinedMetaWithoutTimestamp: a quarantined meta
// written before durable ownership has no timestamp — no registry record
// can have survived that binary's restart, so the very next sweep purges it
// — while an old verified meta keeps loading and serving exactly as today.
func TestSweepPurgesQuarantinedMetaWithoutTimestamp(t *testing.T) {
	data := packOneChunk(t, "pre-upgrade segment")
	quarHash := strings.Repeat("11", 32)
	okHash := strings.Repeat("22", 32)
	dir := t.TempDir()
	// A zero-ownership Segment marshals without the new fields — exactly
	// the meta shape the pre-upgrade binary wrote.
	for hash, quarantined := range map[string]bool{quarHash: true, okHash: false} {
		if err := os.MkdirAll(filepath.Join(dir, hash), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, hash, "0-1"), data, 0644); err != nil {
			t.Fatal(err)
		}
		meta, err := json.Marshal(segstore.Segment{
			XorbHash:    hash,
			ChunkStart:  0,
			ChunkEnd:    1,
			ByteStart:   0,
			ByteEnd:     int64(len(data)) - 1,
			Size:        int64(len(data)),
			Quarantined: quarantined,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, hash, "0-1.json"), meta, 0644); err != nil {
			t.Fatal(err)
		}
	}
	segs, err := segstore.New(segstore.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, newSourceChain(nil, nil, "", time.Second, 1, newQuarantine()))

	capture.sweepQuarantine()
	if segmentOnDisk(t, segs, quarHash) {
		t.Fatal("got timestampless quarantined segment kept, want purged immediately (unsettleable)")
	}
	if seg, ok := segs.Find(okHash, 0, 1); !ok || seg.Quarantined {
		t.Fatalf("got Find(old verified meta) = (%+v, %v), want it served exactly as today", seg, ok)
	}
}

// TestSweepRetriesPurgeAfterDeleteFailure — Finding B regression: expiring
// a registry record whose Delete fails must not strand the segment — the
// persisted meta still owns it, so a later sweep purges it with no record
// left at all.
func TestSweepRetriesPurgeAfterDeleteFailure(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, data := fx.slice(t, 2, 6)
	const peer = "http://127.0.0.1:9"
	dir := t.TempDir()
	segs, err := segstore.New(segstore.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	quar := newQuarantine()
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, newSourceChain(nil, nil, "", time.Second, 1, quar))
	if created, err := segs.PutQuarantined(fx.hash, 2, 6, byteStart, byteEnd, data, captureFileHash, peer); err != nil || !created {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
	}
	if !quar.add(captureFileHash, quarItem{xorbHash: fx.hash, span: tracker.ChunkSpan{Start: 2, End: 6}, peer: peer}) {
		t.Fatal("got quarantine add = false, want registered")
	}

	segDir := filepath.Join(dir, fx.hash)
	if err := os.Chmod(segDir, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(segDir, 0755) })
	capture.sweepQuarantineBefore(time.Now().Add(time.Hour))
	if items := quar.take(captureFileHash); len(items) != 0 {
		t.Fatalf("got %d registry items after the expiring sweep, want 0 (record consumed)", len(items))
	}
	if !segmentOnDisk(t, segs, fx.hash) {
		t.Fatal("fixture broken: the read-only dir did not make Delete fail")
	}

	if err := os.Chmod(segDir, 0755); err != nil {
		t.Fatal(err)
	}
	capture.sweepQuarantineBefore(time.Now().Add(time.Hour))
	if segmentOnDisk(t, segs, fx.hash) {
		t.Fatal("got segment still on disk after the retry sweep, want purged from its meta")
	}
}

// TestSweepSparesRefreshedAndReleasedSegments: an expiry decision made from
// a stale registry item must not delete a segment whose quarantine a newer
// flight refreshed meanwhile, nor one a verdict already released — the
// purge re-reads the meta under the store lock and stands down. The dead
// flight's chunk-cache liability is xorb-wide, though: its fetch may have
// decoded chunks beyond any stored segment, so the cache purge must fire
// even when the segment delete stands down.
func TestSweepSparesRefreshedAndReleasedSegments(t *testing.T) {
	fx := buildPackedXorb(t)
	releasedStart, releasedEnd, releasedData := fx.slice(t, 2, 6)
	refreshedStart, refreshedEnd, refreshedData := fx.slice(t, 6, 8)
	const peer = "http://127.0.0.1:9"
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	quar := newQuarantine()
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, newSourceChain(nil, nil, "", time.Second, 1, quar))
	var purgedChunks []string
	capture.purgeChunks = func(hash string) { purgedChunks = append(purgedChunks, hash) }

	for _, seed := range []struct {
		span       tracker.ChunkSpan
		start, end int64
		data       []byte
	}{
		{tracker.ChunkSpan{Start: 2, End: 6}, releasedStart, releasedEnd, releasedData},
		{tracker.ChunkSpan{Start: 6, End: 8}, refreshedStart, refreshedEnd, refreshedData},
	} {
		if created, err := segs.PutQuarantined(fx.hash, seed.span.Start, seed.span.End, seed.start, seed.end, seed.data, captureFileHash, peer); err != nil || !created {
			t.Fatalf("got PutQuarantined(%+v) = (%v, %v), want (true, nil)", seed.span, created, err)
		}
		if !quar.add(captureFileHash, quarItem{xorbHash: fx.hash, span: seed.span, peer: peer}) {
			t.Fatal("got quarantine add = false, want registered")
		}
	}
	// The registry items go stale past the TTL…
	quar.mu.Lock()
	for i := range quar.files[captureFileHash] {
		quar.files[captureFileHash][i].added = time.Now().Add(-2 * quarantineTTL)
	}
	quar.mu.Unlock()
	// …while a verdict released the first segment and a newer flight
	// re-owned the second with an identical re-store (fresh meta time).
	if released, err := segs.Release(fx.hash, 2, 6); err != nil || !released {
		t.Fatalf("got Release = (%v, %v), want (true, nil)", released, err)
	}
	otherFile := strings.Repeat("ee", 32)
	if owned, err := segs.PutQuarantined(fx.hash, 6, 8, refreshedStart, refreshedEnd, refreshedData, otherFile, peer); err != nil || !owned {
		t.Fatalf("got refreshing re-store = (%v, %v), want (true, nil)", owned, err)
	}

	capture.sweepQuarantine()

	if seg, ok := segs.Find(fx.hash, 2, 6); !ok || seg.Quarantined {
		t.Fatalf("got Find(released segment) = (%+v, %v) after the sweep, want it spared", seg, ok)
	}
	var refreshed *segstore.Segment
	if err := segs.Walk(func(seg segstore.Segment) error {
		if seg.ChunkStart == 6 && seg.ChunkEnd == 8 {
			refreshed = &seg
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if refreshed == nil || !refreshed.Quarantined || refreshed.FileHash != otherFile {
		t.Fatalf("got refreshed segment %+v after the sweep, want it spared under the new owner", refreshed)
	}
	if capture.cooledDown(peer) {
		t.Fatal("got peer cooled by a stood-down purge, want untouched")
	}
	if len(purgedChunks) == 0 {
		t.Fatal("got no chunk-cache purge from the expired flight, want its xorb cache dropped despite the stand-downs")
	}
	for _, hash := range purgedChunks {
		if hash != fx.hash {
			t.Fatalf("got chunk-cache purge for %s, want only %s", hash, fx.hash)
		}
	}
}

// TestChainSecondFileRegistersSharedQuarantinedSegment: a term of a second
// file resolving to a segment some earlier flight already quarantined
// byte-identically is served and registered under the second file too, so
// either file's verdict can settle the shared segment.
func TestChainSecondFileRegistersSharedQuarantinedSegment(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	peer := &chainPeer{fx: fx}
	peerSrv := newChainPeerServer(t, peer)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{
			spanKey(fx, 2, 6): {{Peer: peerSrv.URL}},
		},
	})
	// An earlier flight of another file quarantined the exact same bytes.
	otherFile := strings.Repeat("ee", 32)
	if created, err := h.segs.PutQuarantined(fx.hash, 2, 6, byteStart, byteEnd, want, otherFile, canonicalPeerURL(peerSrv.URL)); err != nil || !created {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
	}
	wantItem := quarItem{xorbHash: fx.hash, span: tracker.ChunkSpan{Start: 2, End: 6}, peer: canonicalPeerURL(peerSrv.URL)}
	if !h.quar.add(otherFile, wantItem) {
		t.Fatal("got prior-file quarantine add = false, want registered")
	}

	h.recon(t)
	resp, body := h.term(t, byteStart, byteEnd)
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, want) {
		t.Fatalf("got (status = %d, body match = %v), want peer-served exact bytes", resp.StatusCode, bytes.Equal(body, want))
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served by the peer)", got)
	}
	if h.capture.cooledDown(canonicalPeerURL(peerSrv.URL)) {
		t.Fatal("got peer cooled after an identical re-store, want untouched")
	}
	for _, fileHash := range []string{captureFileHash, otherFile} {
		items := h.quar.take(fileHash)
		if len(items) == 1 {
			// The registration timestamp only drives the sweep.
			items[0].added = time.Time{}
		}
		if len(items) != 1 || items[0] != wantItem {
			t.Fatalf("got quarantine items = %+v under %s, want [%+v]", items, fileHash, wantItem)
		}
	}
}

// TestChainPeerSelfExclusionAndAttemptCap: the agent's own URL is skipped
// without contact, at most two candidates are attempted, and a cooled peer
// stays skipped on a later term fetch.
func TestChainPeerSelfExclusionAndAttemptCap(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	byteStart2, byteEnd2, want2 := fx.slice(t, 6, 8)

	var selfHits atomic.Int64
	selfSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfHits.Add(1)
		http.Error(w, "self must never be contacted", http.StatusInternalServerError)
	}))
	t.Cleanup(selfSrv.Close)

	bad1 := &chainPeer{fx: fx, status: http.StatusInternalServerError}
	bad2 := &chainPeer{fx: fx, noMarker: true}
	good := &chainPeer{fx: fx}
	bad1Srv := newChainPeerServer(t, bad1)
	bad2Srv := newChainPeerServer(t, bad2)
	goodSrv := newChainPeerServer(t, good)

	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}, {6, 8, byteStart2, byteEnd2}},
		answers: map[string][]tracker.XorbLocation{
			spanKey(fx, 2, 6): {{Peer: selfSrv.URL}, {Peer: bad1Srv.URL}, {Peer: bad2Srv.URL}, {Peer: goodSrv.URL}},
			spanKey(fx, 6, 8): {{Peer: bad1Srv.URL}},
		},
		self: selfSrv.URL,
	})
	h.recon(t)

	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got fallback body differing from the origin bytes")
	}
	if got := selfHits.Load(); got != 0 {
		t.Fatalf("got %d fetches to self, want 0 (self-exclusion)", got)
	}
	if got := bad1.hits.Load(); got != 1 {
		t.Fatalf("got %d fetches to the first bad peer, want 1", got)
	}
	if got := bad2.hits.Load(); got != 1 {
		t.Fatalf("got %d fetches to the second bad peer, want 1", got)
	}
	if got := good.hits.Load(); got != 0 {
		t.Fatalf("got %d fetches to the third candidate, want 0 (attempt cap)", got)
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1 (all attempts failed)", got)
	}

	// Second term fetch: bad1 is cooling down, so its claim on the second
	// span is skipped without contact.
	_, body2 := h.term(t, byteStart2, byteEnd2)
	if !bytes.Equal(body2, want2) {
		t.Fatal("got second-term body differing from the origin bytes")
	}
	if got := bad1.hits.Load(); got != 1 {
		t.Fatalf("got %d fetches to the cooled peer after the second term, want 1 (cooldown respected)", got)
	}
	if got := h.o.termCount(byteStart2, byteEnd2); got != 1 {
		t.Fatalf("got %d origin term fetches for the second term, want 1", got)
	}
}

// TestChainNoTrackerGoesStraightToOrigin: without a tracker no lookup is
// issued and no peer is ever contacted — the chain is local then origin.
func TestChainNoTrackerGoesStraightToOrigin(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{entries: []chainEntry{{2, 6, byteStart, byteEnd}}})
	h.recon(t)
	h.capture.mu.Lock()
	cached := len(h.capture.peerCache)
	h.capture.mu.Unlock()
	if cached != 0 {
		t.Fatalf("got %d peer cache entries without a tracker, want 0", cached)
	}
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got origin body differing from the fixture bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1", got)
	}
}

// TestChainOriginTeeOverQuarantinedDoesNotNotify: when the origin fallback
// re-fetches a span whose only stored segment is quarantined, the keep-first
// store keeps it quarantined — and nothing is announced, because the agent
// could not serve the claim.
func TestChainOriginTeeOverQuarantinedDoesNotNotify(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{entries: []chainEntry{{2, 6, byteStart, byteEnd}}})
	if created, err := h.segs.PutQuarantined(fx.hash, 2, 6, byteStart, byteEnd, want, captureFileHash, "http://127.0.0.1:9"); err != nil || !created {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
	}
	h.recon(t)
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got term body differing from the origin bytes")
	}
	// The quarantined segment is not servable, so the fetch went to the
	// origin and the tee could not announce a servable claim.
	if got := h.o.termCount(byteStart, byteEnd); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1 (quarantined is not served)", got)
	}
	if got := h.notifiedCount(); got != 0 {
		t.Fatalf("got %d announcer notifications, want 0 (segment stayed quarantined)", got)
	}
	if seg, ok := h.segs.Find(fx.hash, 2, 6); ok {
		t.Fatalf("got segment %+v from Find, want it still quarantined", seg)
	}
}

// TestChainRejectsUserinfoCandidate: a tracker answer carrying URL
// credentials is dropped before any contact — net/http would turn the
// userinfo into an Authorization header, which peers must never receive.
func TestChainRejectsUserinfoCandidate(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	peer := &chainPeer{fx: fx}
	peerSrv := newChainPeerServer(t, peer)
	withCreds := "http://user:secret@" + strings.TrimPrefix(peerSrv.URL, "http://")
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{
			spanKey(fx, 2, 6): {{Peer: withCreds}},
		},
	})
	h.recon(t)
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got fallback body differing from the origin bytes")
	}
	if got := peer.hits.Load(); got != 0 {
		t.Fatalf("got %d fetches to the userinfo candidate, want 0 (rejected)", got)
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1", got)
	}
}

// TestChainCandidateCapAndDedupe: cached candidates per span are deduplicated
// and capped, so a hostile tracker cannot bloat the cache.
func TestChainCandidateCapAndDedupe(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, _ := fx.slice(t, 2, 6)
	var locs []tracker.XorbLocation
	for range 3 {
		locs = append(locs, tracker.XorbLocation{Peer: "http://10.0.0.1:1"})
	}
	for i := range 10 {
		locs = append(locs, tracker.XorbLocation{Peer: fmt.Sprintf("http://10.0.0.%d:1", i+2)})
	}
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{spanKey(fx, 2, 6): locs},
	})
	h.recon(t)
	peers := h.capture.cachedPeers(spanKey(fx, 2, 6))
	if len(peers) != maxPeersPerSpan {
		t.Fatalf("got %d cached candidates, want %d (capped)", len(peers), maxPeersPerSpan)
	}
	seen := map[string]struct{}{}
	for _, p := range peers {
		if _, ok := seen[p]; ok {
			t.Fatalf("got duplicate cached candidate %q, want deduplicated", p)
		}
		seen[p] = struct{}{}
	}
}

// TestBatchRangeAttribution: batch reconstructions attribute each fetch
// entry to a file whose terms consume it under the pinned client's own
// smallest-containing selection; entries no term selects stay unattributed.
func TestBatchRangeAttribution(t *testing.T) {
	fx := buildPackedXorb(t)
	fileA := strings.Repeat("aa", 32)
	fileB := strings.Repeat("bb", 32)
	byteStartA, byteEndA, _ := fx.slice(t, 2, 6)
	byteStartB, byteEndB, _ := fx.slice(t, 6, 8)
	byteStartO, byteEndO, _ := fx.slice(t, 0, 2)
	byteStartW, byteEndW, _ := fx.slice(t, 2, 8)
	const termURL = "http://origin/term/xorb"
	rec := download.BatchReconstructionResponse{
		Files: map[string][]download.Term{
			fileA: {{Hash: fx.hash, Range: download.ChunkRange{Start: 2, End: 6}}},
			fileB: {{Hash: fx.hash, Range: download.ChunkRange{Start: 6, End: 8}}},
		},
		FetchInfo: map[string][]download.FetchInfoEntry{
			fx.hash: {
				{Range: download.ChunkRange{Start: 2, End: 6}, URL: termURL, URLRange: download.ByteRange{Start: byteStartA, End: byteEndA}},
				{Range: download.ChunkRange{Start: 6, End: 8}, URL: termURL, URLRange: download.ByteRange{Start: byteStartB, End: byteEndB}},
				// A range no file's terms consume: never attributed.
				{Range: download.ChunkRange{Start: 0, End: 2}, URL: termURL, URLRange: download.ByteRange{Start: byteStartO, End: byteEndO}},
				// Contains both terms, but each term selects its narrower
				// entry (smallest byte span), so this one is never fetched
				// and never attributed.
				{Range: download.ChunkRange{Start: 2, End: 8}, URL: termURL, URLRange: download.ByteRange{Start: byteStartW, End: byteEndW}},
			},
		},
	}
	canned, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	body := &reconBody{inner: io.NopCloser(bytes.NewReader(canned)), t: capture}
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		start, end   int64
		wantFileHash string
	}{
		{"range consumed by file A", byteStartA, byteEndA, fileA},
		{"range consumed by file B", byteStartB, byteEndB, fileB},
		{"unconsumed range unattributed", byteStartO, byteEndO, ""},
		{"unselected containing range unattributed", byteStartW, byteEndW, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, ok := capture.lookup(fetchKey{url: termURL, start: tt.start, end: tt.end})
			if !ok {
				t.Fatal("got no recorded entry, want one")
			}
			if info.fileHash != tt.wantFileHash {
				t.Fatalf("got fileHash = %q, want %q", info.fileHash, tt.wantFileHash)
			}
		})
	}
}

// TestChainWiderEntryQuarantinesOnlyCoveredChunks — Finding A regression: a
// fetch entry advertising more chunks than the file's terms consume, served
// from a peer, must quarantine and register exactly the term-covered chunk
// sub-ranges. The client still receives the full requested byte range, and
// after the file's passing verdict releases the registered items, the
// uncovered chunks are never findable or announced — the file's checksum
// never validated them.
func TestChainWiderEntryQuarantinesOnlyCoveredChunks(t *testing.T) {
	fx := buildPackedXorb(t)
	// The entry advertises chunks [2, 8); the file's terms consume only
	// [3, 5) and [6, 8), leaving [2, 3) and [5, 6) unvalidated.
	entryStart, entryEnd, entryBytes := fx.slice(t, 2, 8)
	covered := []tracker.ChunkSpan{{Start: 3, End: 5}, {Start: 6, End: 8}}
	uncovered := []tracker.ChunkSpan{{Start: 2, End: 3}, {Start: 5, End: 6}, {Start: 2, End: 8}}
	peer := &chainPeer{fx: fx}
	peerSrv := newChainPeerServer(t, peer)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 8, entryStart, entryEnd}},
		terms:   []download.ChunkRange{{Start: 3, End: 5}, {Start: 6, End: 8}},
		answers: map[string][]tracker.XorbLocation{
			spanKey(fx, 2, 8): {{Peer: peerSrv.URL}},
		},
	})
	h.recon(t)

	resp, body := h.term(t, entryStart, entryEnd)
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, entryBytes) {
		t.Fatalf("got (status = %d, body match = %v), want the full entry byte range peer-served", resp.StatusCode, bytes.Equal(body, entryBytes))
	}
	if got := h.o.termCount(entryStart, entryEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served by the peer)", got)
	}

	stored := map[tracker.ChunkSpan]segstore.Segment{}
	if err := h.segs.Walk(func(seg segstore.Segment) error {
		stored[tracker.ChunkSpan{Start: seg.ChunkStart, End: seg.ChunkEnd}] = seg
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(stored) != len(covered) {
		t.Fatalf("got %d stored segments %+v, want %d (covered sub-ranges only)", len(stored), stored, len(covered))
	}
	for _, span := range covered {
		seg, ok := stored[span]
		if !ok || !seg.Quarantined {
			t.Fatalf("got stored[%+v] = (%+v, %v), want a quarantined segment", span, seg, ok)
		}
		wantStart, wantEnd, wantData := fx.slice(t, span.Start, span.End)
		if seg.ByteStart != wantStart || seg.ByteEnd != wantEnd {
			t.Fatalf("got segment bytes [%d, %d] for %+v, want [%d, %d]", seg.ByteStart, seg.ByteEnd, span, wantStart, wantEnd)
		}
		f, err := h.segs.Open(seg)
		if err != nil {
			t.Fatal(err)
		}
		gotData, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil || !bytes.Equal(gotData, wantData) {
			t.Fatalf("got stored bytes for %+v differing from the origin slice (err = %v)", span, err)
		}
	}

	// Settle the flight the way ingestVerdict's success path does: release
	// every registered item and announce what released.
	items := h.quar.take(captureFileHash)
	if len(items) != len(covered) {
		t.Fatalf("got %d registered quarantine items %+v, want %d covered spans", len(items), items, len(covered))
	}
	for _, it := range items {
		released, err := h.segs.Release(it.xorbHash, it.span.Start, it.span.End)
		if err != nil || !released {
			t.Fatalf("got Release(%+v) = (%v, %v), want (true, nil)", it.span, released, err)
		}
		h.capture.notify(it.xorbHash, it.span)
	}

	for _, span := range covered {
		seg, ok := h.segs.Find(fx.hash, span.Start, span.End)
		if !ok || seg.Quarantined {
			t.Fatalf("got Find(%+v) = (%+v, %v) after release, want the released segment", span, seg, ok)
		}
	}
	for _, span := range uncovered {
		if seg, ok := h.segs.Find(fx.hash, span.Start, span.End); ok {
			t.Fatalf("got Find(%+v) = %+v, want a miss (never validated by the file)", span, seg)
		}
	}
	if seg, ok := h.segs.FindByteRange(fx.hash, entryStart, entryEnd); ok {
		t.Fatalf("got FindByteRange(full entry) = %+v, want a miss", seg)
	}
	h.mu.Lock()
	notified := append([]tracker.ChunkSpan(nil), h.notified...)
	h.mu.Unlock()
	if len(notified) != len(covered) {
		t.Fatalf("got announced spans %+v, want exactly the covered %+v", notified, covered)
	}
	for _, span := range notified {
		if span != covered[0] && span != covered[1] {
			t.Fatalf("got announced span %+v outside the covered set %+v", span, covered)
		}
	}
}

// TestRecordedTermCoverage pins the per-entry covered-union computation
// across the three reconstruction shapes: only chunk ranges the attributed
// file's own terms select accumulate, overlapping and adjacent term ranges
// merge, and entries no term selects carry no coverage.
func TestRecordedTermCoverage(t *testing.T) {
	fileB := strings.Repeat("bb", 32)
	const xorbHash = "77777777777777777777777777777777777777777777777777777777777777aa"
	const termURL = "http://origin/term/xorb"
	entry := func(cs, ce uint32, bs, be int64) download.FetchInfoEntry {
		return download.FetchInfoEntry{
			Range:    download.ChunkRange{Start: cs, End: ce},
			URL:      termURL,
			URLRange: download.ByteRange{Start: bs, End: be},
		}
	}

	tests := []struct {
		name     string
		v2       bool
		fileHash string
		body     any
		key      fetchKey
		want     []tracker.ChunkSpan
	}{
		{
			name:     "v1 disjoint and overlapping terms merge per entry",
			fileHash: captureFileHash,
			body: download.ReconstructionResponseV1{
				Terms: []download.Term{
					{Hash: xorbHash, Range: download.ChunkRange{Start: 1, End: 3}},
					{Hash: xorbHash, Range: download.ChunkRange{Start: 5, End: 7}},
					{Hash: xorbHash, Range: download.ChunkRange{Start: 6, End: 8}},
				},
				FetchInfo: map[string][]download.FetchInfoEntry{xorbHash: {entry(0, 10, 0, 999)}},
			},
			key:  fetchKey{url: termURL, start: 0, end: 999},
			want: []tracker.ChunkSpan{{Start: 1, End: 3}, {Start: 5, End: 8}},
		},
		{
			name:     "v1 unselected wide entry carries no coverage",
			fileHash: captureFileHash,
			body: download.ReconstructionResponseV1{
				Terms: []download.Term{{Hash: xorbHash, Range: download.ChunkRange{Start: 2, End: 6}}},
				FetchInfo: map[string][]download.FetchInfoEntry{xorbHash: {
					entry(2, 6, 1000, 1499),
					entry(0, 8, 2000, 2999),
				}},
			},
			key:  fetchKey{url: termURL, start: 2000, end: 2999},
			want: nil,
		},
		{
			name:     "v2 wide entry keeps only the term range",
			v2:       true,
			fileHash: captureFileHash,
			body: download.ReconstructionResponseV2{
				Terms: []download.Term{{Hash: xorbHash, Range: download.ChunkRange{Start: 2, End: 6}}},
				Xorbs: map[string][]download.XorbMultiRangeFetch{xorbHash: {{
					URL: termURL,
					Ranges: []download.XorbRangeDescriptor{{
						Chunks: download.ChunkRange{Start: 0, End: 8},
						Bytes:  download.ByteRange{Start: 0, End: 999},
					}},
				}}},
			},
			key:  fetchKey{url: termURL, start: 0, end: 999},
			want: []tracker.ChunkSpan{{Start: 2, End: 6}},
		},
		{
			name: "batch coverage comes from the attributed file only",
			body: download.BatchReconstructionResponse{
				Files: map[string][]download.Term{
					// Sorted key order attributes the shared entry to the
					// "aa..." file; the second file's term must not extend
					// its coverage.
					strings.Repeat("aa", 32): {{Hash: xorbHash, Range: download.ChunkRange{Start: 2, End: 4}}},
					fileB:                    {{Hash: xorbHash, Range: download.ChunkRange{Start: 4, End: 6}}},
				},
				FetchInfo: map[string][]download.FetchInfoEntry{xorbHash: {entry(0, 8, 0, 999)}},
			},
			key:  fetchKey{url: termURL, start: 0, end: 999},
			want: []tracker.ChunkSpan{{Start: 2, End: 4}},
		},
		{
			// A resumed reconstruction decodes its whole first term but
			// discards the skipped prefix — those chunks never reach the
			// checksum, so the first term's selected entry is poisoned
			// outright (attribution stripped from birth, eviction-immune):
			// no clean same-key record may exist while this flight can
			// still fetch it.
			name:     "v1 resumed first term poisons its entry",
			fileHash: captureFileHash,
			body: download.ReconstructionResponseV1{
				OffsetIntoFirstRange: 7,
				Terms: []download.Term{
					{Hash: xorbHash, Range: download.ChunkRange{Start: 0, End: 4}},
					{Hash: xorbHash, Range: download.ChunkRange{Start: 5, End: 7}},
				},
				FetchInfo: map[string][]download.FetchInfoEntry{xorbHash: {entry(0, 8, 0, 999)}},
			},
			key:  fetchKey{url: termURL, start: 0, end: 999},
			want: nil,
		},
		{
			// Entries the resumed first term does not select keep the
			// coverage of the later terms that do.
			name:     "v1 resumed later term keeps its own entry covered",
			fileHash: captureFileHash,
			body: download.ReconstructionResponseV1{
				OffsetIntoFirstRange: 7,
				Terms: []download.Term{
					{Hash: xorbHash, Range: download.ChunkRange{Start: 0, End: 4}},
					{Hash: xorbHash, Range: download.ChunkRange{Start: 5, End: 7}},
				},
				FetchInfo: map[string][]download.FetchInfoEntry{xorbHash: {
					entry(0, 8, 0, 999),
					entry(5, 7, 1000, 1499),
				}},
			},
			key:  fetchKey{url: termURL, start: 1000, end: 1499},
			want: []tracker.ChunkSpan{{Start: 5, End: 7}},
		},
		{
			name:     "v2 resumed single-term reconstruction has no coverage",
			v2:       true,
			fileHash: captureFileHash,
			body: download.ReconstructionResponseV2{
				OffsetIntoFirstRange: 7,
				Terms:                []download.Term{{Hash: xorbHash, Range: download.ChunkRange{Start: 2, End: 6}}},
				Xorbs: map[string][]download.XorbMultiRangeFetch{xorbHash: {{
					URL: termURL,
					Ranges: []download.XorbRangeDescriptor{{
						Chunks: download.ChunkRange{Start: 2, End: 6},
						Bytes:  download.ByteRange{Start: 0, End: 999},
					}},
				}}},
			},
			key:  fetchKey{url: termURL, start: 0, end: 999},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canned, err := json.Marshal(tt.body)
			if err != nil {
				t.Fatal(err)
			}
			segs, err := segstore.New(segstore.WithDir(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
			body := &reconBody{inner: io.NopCloser(bytes.NewReader(canned)), t: capture, v2: tt.v2, fileHash: tt.fileHash}
			if _, err := io.ReadAll(body); err != nil {
				t.Fatal(err)
			}
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
			info, ok := capture.lookup(tt.key)
			if !ok {
				t.Fatal("got no recorded entry, want one")
			}
			if !slices.Equal(info.covered, tt.want) {
				t.Fatalf("got covered = %+v, want %+v", info.covered, tt.want)
			}
		})
	}
}

// TestConflictingRecordsPoisonFetchKey: two live reconstructions can name
// the same fetch key with different validation scopes — a resumed and a
// full reconstruction of one file, or two files sharing a fetch. A term
// fetch cannot tell which flight it serves, so any disagreement strips the
// key's attribution (no peer step, nothing quarantined) until the entry
// expires; identical observations and refreshes after expiry stay clean.
func TestConflictingRecordsPoisonFetchKey(t *testing.T) {
	const termURL = "http://origin/term/xorb"
	xorbHash := strings.Repeat("77", 32)
	full := []tracker.ChunkSpan{{Start: 0, End: 8}}
	key := fetchKey{url: termURL, start: 0, end: 999}
	newCapture := func(t *testing.T) *captureTransport {
		t.Helper()
		segs, err := segstore.New(segstore.WithDir(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		return newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	}
	assertAttribution := func(t *testing.T, capture *captureTransport, wantFile string, wantCovered []tracker.ChunkSpan) {
		t.Helper()
		info, ok := capture.lookup(key)
		if !ok {
			t.Fatal("got no recorded entry, want one")
		}
		if info.fileHash != wantFile || !slices.Equal(info.covered, wantCovered) {
			t.Fatalf("got (fileHash = %q, covered = %+v), want (%q, %+v)", info.fileHash, info.covered, wantFile, wantCovered)
		}
	}

	t.Run("resumed observation poisons a full one", func(t *testing.T) {
		capture := newCapture(t)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		// The resumed alias of the same reconstruction: no coverage at all.
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, nil)
		assertAttribution(t, capture, "", nil)
		// The poison sticks even when the full observation is re-recorded.
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		assertAttribution(t, capture, "", nil)
	})

	t.Run("second file poisons the key", func(t *testing.T) {
		capture := newCapture(t)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, strings.Repeat("bb", 32), full)
		assertAttribution(t, capture, "", nil)
	})

	t.Run("identical re-record keeps attribution", func(t *testing.T) {
		capture := newCapture(t)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		assertAttribution(t, capture, captureFileHash, full)
	})

	t.Run("expired observation carries no live flight", func(t *testing.T) {
		capture := newCapture(t)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		capture.mu.Lock()
		e := capture.entries[key]
		e.expires = time.Now().Add(-time.Second)
		capture.entries[key] = e
		capture.mu.Unlock()
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, nil)
		assertAttribution(t, capture, captureFileHash, nil)
	})

	t.Run("birth poison precedes any observation", func(t *testing.T) {
		capture := newCapture(t)
		capture.poisonKey(key)
		capture.record(termURL, 0, 999, xorbHash, 0, 8, captureFileHash, full)
		assertAttribution(t, capture, "", nil)
	})
}

// TestPoisonedEntryEvictionImmunity: a poisoned key holds its table slot
// until it expires — capacity pressure evicts clean entries around it, and
// a table left with only live poison refuses new keys — so no eviction
// interleaving can ever launder a key a suppressed flight may still fetch.
func TestPoisonedEntryEvictionImmunity(t *testing.T) {
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	xorbHash := strings.Repeat("77", 32)
	poisoned := fetchKey{url: "http://origin/term/poisoned", start: 0, end: 99}
	capture.record(poisoned.url, poisoned.start, poisoned.end, xorbHash, 0, 4, captureFileHash, nil)
	capture.poisonKey(poisoned)
	for i := range maxFetchEntries - 1 {
		capture.record(fmt.Sprintf("http://origin/term/%d", i), 0, 99, xorbHash, 0, 4, captureFileHash, nil)
	}
	// The table is at capacity with the poisoned key as its oldest entry:
	// the next new key must evict the oldest clean entry instead.
	capture.record("http://origin/term/overflow", 0, 99, xorbHash, 0, 4, captureFileHash, nil)
	if info, ok := capture.lookup(poisoned); !ok || info.fileHash != "" {
		t.Fatalf("got poisoned entry (info = %+v, ok = %v) after eviction pressure, want it retained and unattributed", info, ok)
	}
	if _, ok := capture.lookup(fetchKey{url: "http://origin/term/0", start: 0, end: 99}); ok {
		t.Fatal("got oldest clean entry still present, want it evicted instead of the poison")
	}
	if _, ok := capture.lookup(fetchKey{url: "http://origin/term/overflow", start: 0, end: 99}); !ok {
		t.Fatal("got overflow key missing, want it recorded over an evicted clean entry")
	}

	// With every live entry poisoned, a new key is refused outright.
	capture.mu.Lock()
	for k, e := range capture.entries {
		e.conflicted = true
		capture.entries[k] = e
	}
	size := len(capture.entries)
	capture.mu.Unlock()
	if capture.record("http://origin/term/refused", 0, 99, xorbHash, 0, 4, captureFileHash, nil) {
		t.Fatal("got record = true into a fully poisoned table, want refusal")
	}
	if _, ok := capture.lookup(fetchKey{url: "http://origin/term/refused", start: 0, end: 99}); ok {
		t.Fatal("got refused key present, want it absent")
	}
	capture.mu.Lock()
	got := len(capture.entries)
	capture.mu.Unlock()
	if got != size {
		t.Fatalf("got %d entries after refusal, want %d unchanged", got, size)
	}
}

// TestPoisonSaturationStandsDownAttribution: when a birth poison cannot
// allocate a slot, a slot freeing a moment later must not hand the
// suppressed key clean coverage — the table stands down instead, stripping
// attribution from every record until the refused flight's table lifetime
// has passed.
func TestPoisonSaturationStandsDownAttribution(t *testing.T) {
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := newCaptureTransport(http.DefaultTransport, segs, nil, nil)
	xorbHash := strings.Repeat("77", 32)
	for i := range maxFetchEntries {
		capture.record(fmt.Sprintf("http://origin/term/%d", i), 0, 99, xorbHash, 0, 4, captureFileHash, nil)
	}
	capture.mu.Lock()
	for k, e := range capture.entries {
		e.conflicted = true
		capture.entries[k] = e
	}
	capture.mu.Unlock()

	// The resumed flight's birth poison finds no slot to free.
	suppressed := fetchKey{url: "http://origin/term/suppressed", start: 0, end: 99}
	capture.poisonKey(suppressed)
	if _, ok := capture.lookup(suppressed); ok {
		t.Fatal("got suppressed key present after refused poison, want absent")
	}
	// An unrelated poison expires, freeing a slot for a clean full record
	// of the very key the poison could not protect.
	capture.mu.Lock()
	victim := capture.order[0]
	e := capture.entries[victim]
	e.expires = time.Now().Add(-time.Second)
	capture.entries[victim] = e
	capture.mu.Unlock()
	if !capture.record(suppressed.url, suppressed.start, suppressed.end, xorbHash, 0, 4, captureFileHash,
		[]tracker.ChunkSpan{{Start: 0, End: 4}}) {
		t.Fatal("got record = false after a slot freed, want the entry recorded (unattributed)")
	}
	info, ok := capture.lookup(suppressed)
	if !ok {
		t.Fatal("got no entry for the suppressed key, want an unattributed one")
	}
	if info.fileHash != "" || info.covered != nil {
		t.Fatalf("got (fileHash = %q, covered = %+v) during the stand-down, want stripped attribution", info.fileHash, info.covered)
	}
}

// TestChainConflictedKeyNeverGoesToPeers: a resumed and a full
// reconstruction of one file disagree over the same fetch key — the resumed
// flight's checksum never hashes the skipped prefix, so the full
// observation must not lend the key its coverage. The term fetch skips the
// peer step entirely and falls through to the origin.
func TestChainConflictedKeyNeverGoesToPeers(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	peer := &chainPeer{fx: fx}
	peerSrv := newChainPeerServer(t, peer)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{
			spanKey(fx, 2, 6): {{Peer: peerSrv.URL}},
		},
	})
	// The resumed alias records the key first — no coverage — then the full
	// reconstruction lands while that flight is still live. Its coverage
	// must not be credited to the resumed flight's term fetch.
	resumed := h.o.reconstruction(h.origin.URL + "/term/xorb")
	resumed.OffsetIntoFirstRange = 7
	canned, err := json.Marshal(resumed)
	if err != nil {
		t.Fatal(err)
	}
	body := &reconBody{inner: io.NopCloser(bytes.NewReader(canned)), t: h.capture, fileHash: captureFileHash}
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	h.recon(t)

	_, got := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(got, want) {
		t.Fatal("got term body differing from the origin bytes")
	}
	if hits := peer.hits.Load(); hits != 0 {
		t.Fatalf("got %d peer fetches for a conflicted key, want 0", hits)
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1 (conflicted key falls through)", got)
	}
	if items := h.quar.take(captureFileHash); len(items) != 0 {
		t.Fatalf("got quarantine items %+v for a conflicted key, want none", items)
	}
}

// TestChainPeerCacheHoldsLargeReconstruction: every span of one large
// reconstruction keeps its candidates — the cache cap matches the fetch
// table cap, so a single reconstruction can never evict its own answers.
func TestChainPeerCacheHoldsLargeReconstruction(t *testing.T) {
	fx := buildPackedXorb(t)
	const spans = 1200
	entries := make([]chainEntry, 0, spans)
	answers := map[string][]tracker.XorbLocation{}
	for i := range spans {
		cs, ce := uint32(i), uint32(i+1)
		entries = append(entries, chainEntry{cs, ce, int64(i) * 100, int64(i)*100 + 99})
		answers[spanKey(fx, cs, ce)] = []tracker.XorbLocation{{Peer: "http://10.0.0.1:1"}}
	}
	h := newChainHarness(t, chainConfig{entries: entries, answers: answers})
	h.recon(t)
	for _, i := range []int{0, spans - 1} {
		if peers := h.capture.cachedPeers(spanKey(fx, uint32(i), uint32(i+1))); len(peers) != 1 {
			t.Fatalf("got %d cached candidates for span %d, want 1 (no self-eviction)", len(peers), i)
		}
	}
}

// TestChainPeerCacheRefreshSurvivesFullCache: refreshing the oldest key of
// a full cache moves it to the back of the eviction order, so inserting the
// same lookup's new keys evicts stale foreign entries, never an answer the
// lookup just delivered.
func TestChainPeerCacheRefreshSurvivesFullCache(t *testing.T) {
	fx := buildPackedXorb(t)
	oldKey := spanKey(fx, 0, 1)
	newKey := spanKey(fx, 1, 2)
	h := newChainHarness(t, chainConfig{
		answers: map[string][]tracker.XorbLocation{
			oldKey: {{Peer: "http://10.0.0.1:1"}},
			newKey: {{Peer: "http://10.0.0.2:1"}},
		},
	})
	// Prefill to capacity with the target key as the oldest entry.
	h.capture.mu.Lock()
	expires := time.Now().Add(peerLookupTTL)
	h.capture.peerCache[oldKey] = peerCacheEntry{peers: []string{"http://10.0.0.1:1"}, expires: expires}
	h.capture.peerOrder = append(h.capture.peerOrder, oldKey)
	for i := range maxPeerLookups - 1 {
		key := fmt.Sprintf("foreign-%d", i)
		h.capture.peerCache[key] = peerCacheEntry{peers: []string{"http://10.0.0.9:1"}, expires: expires}
		h.capture.peerOrder = append(h.capture.peerOrder, key)
	}
	h.capture.mu.Unlock()

	h.capture.fillPeerCache([]tracker.XorbQuery{
		{Hash: fx.hash, Range: tracker.ChunkSpan{Start: 0, End: 1}},
		{Hash: fx.hash, Range: tracker.ChunkSpan{Start: 1, End: 2}},
	})

	if peers := h.capture.cachedPeers(oldKey); len(peers) != 1 {
		t.Fatalf("got %d candidates for the refreshed oldest key, want 1 (must survive)", len(peers))
	}
	if peers := h.capture.cachedPeers(newKey); len(peers) != 1 {
		t.Fatalf("got %d candidates for the new key, want 1", len(peers))
	}
	h.capture.mu.Lock()
	size := len(h.capture.peerCache)
	h.capture.mu.Unlock()
	if size > maxPeerLookups {
		t.Fatalf("got %d cached keys, want at most %d", size, maxPeerLookups)
	}
}

// TestChainLookupOnMissDiscoversLatePeer: a peer that announced only after
// the reconstruction-time lookup answered empty is still found — the term
// fetch re-resolves its span with a single-span lookup and serves from the
// peer with zero origin traffic.
func TestChainLookupOnMissDiscoversLatePeer(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	peer := &chainPeer{fx: fx}
	peerSrv := newChainPeerServer(t, peer)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{},
	})
	h.recon(t)
	if got := h.tracker.lookups.Load(); got != 1 {
		t.Fatalf("got %d tracker lookups after the reconstruction, want 1", got)
	}
	// The peer announces between the reconstruction parse and the term fetch.
	h.tracker.setAnswers(map[string][]tracker.XorbLocation{
		spanKey(fx, 2, 6): {{Peer: peerSrv.URL}},
	})
	resp, body := h.term(t, byteStart, byteEnd)
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("got status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, want) {
		t.Fatal("got peer-served body differing from the origin bytes")
	}
	if got := h.tracker.lookups.Load(); got != 2 {
		t.Fatalf("got %d tracker lookups after the term fetch, want 2 (one per-span on miss)", got)
	}
	if got := peer.hits.Load(); got != 1 {
		t.Fatalf("got %d peer fetches, want 1", got)
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 0 {
		t.Fatalf("got %d origin term fetches, want 0 (served by the late peer)", got)
	}
}

// TestChainLookupOnMissNegativeCached: an empty single-span answer is
// cached, so an immediate retry of the span goes to the origin without
// another tracker call.
func TestChainLookupOnMissNegativeCached(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{},
	})
	h.recon(t)
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got origin body differing from the fixture bytes")
	}
	if got := h.tracker.lookups.Load(); got != 2 {
		t.Fatalf("got %d tracker lookups after the first term fetch, want 2", got)
	}
	// Drop the teed segment so the retry reaches the peer step again
	// instead of the local store.
	if err := h.segs.Delete(fx.hash, 2, 6); err != nil {
		t.Fatal(err)
	}
	_, body = h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got retry body differing from the fixture bytes")
	}
	if got := h.tracker.lookups.Load(); got != 2 {
		t.Fatalf("got %d tracker lookups after the retry, want 2 (negative answer cached)", got)
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 2 {
		t.Fatalf("got %d origin term fetches, want 2", got)
	}
}

// TestChainLookupOnMissErrorFallsBack: a failing tracker only costs the
// bounded lookup — the term fetch falls back to the origin with correct
// bytes, and the failure is cached like an empty answer.
func TestChainLookupOnMissErrorFallsBack(t *testing.T) {
	fx := buildPackedXorb(t)
	byteStart, byteEnd, want := fx.slice(t, 2, 6)
	h := newChainHarness(t, chainConfig{
		entries: []chainEntry{{2, 6, byteStart, byteEnd}},
		answers: map[string][]tracker.XorbLocation{},
	})
	h.recon(t)
	h.tracker.setFail(true)
	_, body := h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got fallback body differing from the fixture bytes")
	}
	if got := h.o.termCount(byteStart, byteEnd); got != 1 {
		t.Fatalf("got %d origin term fetches, want 1", got)
	}
	if got := h.tracker.lookups.Load(); got != 2 {
		t.Fatalf("got %d tracker lookups after the failed on-miss lookup, want 2", got)
	}
	if err := h.segs.Delete(fx.hash, 2, 6); err != nil {
		t.Fatal(err)
	}
	_, body = h.term(t, byteStart, byteEnd)
	if !bytes.Equal(body, want) {
		t.Fatal("got retry body differing from the fixture bytes")
	}
	if got := h.tracker.lookups.Load(); got != 2 {
		t.Fatalf("got %d tracker lookups after the retry, want 2 (error cached as negative)", got)
	}
}
