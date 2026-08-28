package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

const (
	// maxReconstructionBody bounds how much of a reconstruction response is
	// buffered for parsing; larger bodies pass through unparsed.
	maxReconstructionBody = 16 << 20
	// maxCaptureSegment bounds how large a term fetch body may be teed into
	// the segment store.
	maxCaptureSegment = 32 << 20
	// fetchEntryTTL expires recorded fetch entries: term fetches for a
	// reconstruction happen right after it, so an hour is generous.
	fetchEntryTTL = time.Hour
	// maxFetchEntries caps the in-memory fetch table.
	maxFetchEntries = 4096
	// peerLookupTTL expires cached tracker answers: term fetches follow
	// their reconstruction within seconds, so stale peer sets never linger.
	peerLookupTTL = 30 * time.Second
	// maxPeerLookups caps the peer candidate cache. It matches the fetch
	// table cap so one reconstruction's answers never evict each other;
	// concurrent reconstructions past the shared cap degrade to the origin.
	maxPeerLookups = maxFetchEntries
	// maxPeersPerSpan caps the candidates kept per span — only
	// maxPeerAttempts are ever contacted, the rest is headroom for self and
	// cooldown skips — so a hostile tracker cannot bloat the cache.
	maxPeersPerSpan = 8
	// maxPeerURLLen rejects absurd candidate URLs before parsing.
	maxPeerURLLen = 2048
	// maxLookupSpans bounds one reconstruction's tracker query. It matches
	// the fetch table cap: spans beyond it would be evicted from the table
	// anyway, so candidates for them could never be used. A reconstruction
	// larger than every one of these aligned caps degrades to the origin
	// for the overflow, never to wrong answers.
	maxLookupSpans = maxFetchEntries
	// peerLookupTimeout bounds the synchronous tracker lookup on the
	// reconstruction parse path, tighter than the client's own 10s.
	peerLookupTimeout = 5 * time.Second
	// missLookupTimeout bounds the single-span tracker lookup a term fetch
	// runs when its span has no live cached answer — the origin fallback is
	// waiting on it, so it is tighter than the parse-path lookup.
	missLookupTimeout = 2 * time.Second
	// negativePeerTTL caches empty or failed single-span lookups briefly:
	// long enough that a cold cluster's term retries do not hammer the
	// tracker, short enough that peers announcing mid-flight are discovered.
	negativePeerTTL = 2 * time.Second
	// peerCooldown is how long a failed peer is skipped for term fetches.
	peerCooldown = 15 * time.Minute
	// maxCooldownPeers caps the cooldown map; at the cap new failures go
	// unrecorded, costing at most extra fetch attempts, never correctness.
	maxCooldownPeers = 4096
	// maxPeerAttempts is how many peers one term fetch contacts before
	// falling back to the origin.
	maxPeerAttempts = 2
)

// reconstructionPathRe matches the versioned reconstruction endpoints the
// pinned client calls, anchored at the path end so casUrl prefixes pass.
// The trailing element is the file hash the reconstruction describes.
var reconstructionPathRe = regexp.MustCompile(`/v([12])/reconstructions/([0-9a-f]{64})$`)

// captureXorbHashRe guards fetch-table entries the same way segstore guards
// paths: only canonical lowercase hex hashes are recorded.
var captureXorbHashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// fetchKey identifies one term fetch: the exact URL and inclusive byte range
// the reconstruction response instructed the client to request.
type fetchKey struct {
	url        string
	start, end int64
}

// fetchInfo is what a matching term fetch body contains: packed bytes of
// chunks [chunkStart, chunkEnd) of the xorb. fileHash names the
// reconstruction file the fetch serves, so peer-sourced segments can be
// quarantined under the file whose checksum verdict settles them; empty
// when the reconstruction did not attribute one. covered is the merged
// union of that file's term chunk ranges selecting this entry — exactly the
// chunks the file's checksum verdict will have validated; an entry may be
// wider than what its file consumes, and only covered chunks may ever be
// quarantined under it.
type fetchInfo struct {
	xorbHash   string
	chunkStart uint32
	chunkEnd   uint32
	fileHash   string
	covered    []tracker.ChunkSpan
}

type tableEntry struct {
	info    fetchInfo
	expires time.Time
	// conflicted marks a key whose attribution can no longer be trusted:
	// live observations disagreed — two flights consume the same fetch with
	// different validation scopes (resumed vs full, or different files) — or
	// the entry serves a resumed first term, whose skipped prefix the
	// consuming flight's checksum never hashes. It sticks until the entry
	// expires and shields the entry from capacity eviction, so a re-record
	// can never launder the key clean while a suppressed flight may still
	// fetch it.
	conflicted bool
}

// peerCacheEntry is one cached tracker answer: the peers claiming a chunk
// span, keyed like the lookup response.
type peerCacheEntry struct {
	peers   []string
	expires time.Time
}

// sourceChain holds what the per-term source chain needs beyond capture
// itself: local full-xorb storage, peer discovery and the quarantine
// registry. A nil tracker disables the peer step, degrading the chain to
// local followed by the origin.
type sourceChain struct {
	stor        storage.Storage
	tracker     *tracker.Client
	self        string
	peerTimeout time.Duration
	peerClient  *http.Client
	quar        *quarantine
}

func newSourceChain(stor storage.Storage, tc *tracker.Client, self string, peerTimeout time.Duration, maxPeerConns int, quar *quarantine) *sourceChain {
	// The peer transport mirrors the selector's settings plus the disabled
	// keep-alives every term-fetch transport in this repo runs with.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxConnsPerHost = maxPeerConns
	tr.ResponseHeaderTimeout = peerTimeout
	tr.DisableKeepAlives = true
	return &sourceChain{
		stor:        stor,
		tracker:     tc,
		self:        canonicalPeerURL(self),
		peerTimeout: peerTimeout,
		quar:        quar,
		peerClient: &http.Client{
			Transport: tr,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// captureTransport wraps the mirror's xet client transport to harvest
// segments from ingest traffic it already carries: reconstruction responses
// name which (url, byte range) serves which chunk range, and matching term
// fetch bodies are teed into the segment store. Capture is strictly
// best-effort — every request and body is served bit-identical with or
// without it, and any capture failure is silent. Responses marked
// X-Xet-PD-Agent come from cluster peers, not the origin, and are skipped
// (peer-sourced capture needs quarantine, a later stage).
type captureTransport struct {
	inner http.RoundTripper
	segs  *segstore.Store
	// notify announces a freshly captured segment; nil without a tracker.
	notify func(xorbHash string, span tracker.ChunkSpan)
	// purgeChunks drops the client's decoded-chunk cache for one xorb when
	// a quarantined segment is purged; nil skips it.
	purgeChunks func(xorbHash string)
	// chain, when set, serves recorded term fetches from local and peer
	// sources before the origin; nil passes everything through.
	chain *sourceChain

	mu      sync.Mutex
	entries map[fetchKey]tableEntry
	// order tracks insertion order for pruning and eviction; keys of
	// already-removed entries may linger and are skipped.
	order []fetchKey
	// peerCache holds tracker lookup answers keyed "{hash}:{start}-{end}",
	// with peerOrder tracking insertion for pruning and eviction.
	peerCache map[string]peerCacheEntry
	peerOrder []string
	// attributionStandDown, while in the future, strips attribution from
	// every record: it is armed when a birth poison could not allocate a
	// slot, so no clean coverage can appear anywhere while the flight that
	// needed the poison may still fetch. One deadline bounds the state.
	attributionStandDown time.Time
	// cooldown skips recently failed peers, keyed by canonical URL.
	cooldown map[string]time.Time
	// closed stops further segment writes; writes tracks in-flight ones so
	// close can drain them.
	closed bool
	writes sync.WaitGroup
}

func newCaptureTransport(inner http.RoundTripper, segs *segstore.Store, notify func(string, tracker.ChunkSpan), chain *sourceChain) *captureTransport {
	return &captureTransport{
		inner:     inner,
		segs:      segs,
		notify:    notify,
		chain:     chain,
		entries:   map[fetchKey]tableEntry{},
		peerCache: map[string]peerCacheEntry{},
		cooldown:  map[string]time.Time{},
	}
}

// close stops future segment writes and waits for in-flight ones, so
// nothing touches the segment store after shutdown returns. Response bodies
// still open merely relay bytes afterwards.
func (t *captureTransport) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.writes.Wait()
}

// beginWrite registers one segment write unless the transport is closed.
func (t *captureTransport) beginWrite() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.writes.Add(1)
	return true
}

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet {
		if resp := t.serveFromChain(req); resp != nil {
			return resp, nil
		}
	}
	resp, err := t.inner.RoundTrip(req)
	if err != nil || resp == nil || req.Method != http.MethodGet ||
		resp.Header.Get(agentMarkerHeader) != "" {
		return resp, err
	}
	m := reconstructionPathRe.FindStringSubmatch(req.URL.Path)
	if m != nil || strings.HasSuffix(req.URL.Path, "/reconstructions") {
		if resp.StatusCode == http.StatusOK &&
			strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") &&
			resp.ContentLength <= maxReconstructionBody {
			var fileHash string
			if m != nil {
				fileHash = m[2]
			}
			// The body is teed as the caller consumes it — never drained
			// here, so a slow origin cannot stall the caller's streaming
			// decode.
			resp.Body = &reconBody{inner: resp.Body, t: t, v2: m != nil && m[1] == "2", fileHash: fileHash}
		}
		return resp, nil
	}
	t.observeTermFetch(req, resp)
	return resp, nil
}

// reconBody relays a reconstruction response unchanged while accumulating
// it, and records the fetch entries it describes once — on a successful
// close, the pinned client's one guaranteed step. Oversized or errored
// bodies are relayed but never parsed.
type reconBody struct {
	inner io.ReadCloser
	t     *captureTransport
	v2    bool
	// fileHash is the file the reconstruction describes, taken from the
	// request path; empty for the batch endpoint, whose files come from the
	// response keys.
	fileHash string

	mu   sync.Mutex
	buf  []byte
	over bool
	done bool
}

func (b *reconBody) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)
	b.mu.Lock()
	if n > 0 && !b.over && !b.done {
		if len(b.buf)+n > maxReconstructionBody {
			b.over, b.buf = true, nil
		} else {
			b.buf = append(b.buf, p[:n]...)
		}
	}
	if err != nil && err != io.EOF && !b.done {
		b.over, b.buf = true, nil
	}
	b.mu.Unlock()
	return n, err
}

func (b *reconBody) Close() error {
	err := b.inner.Close()
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return err
	}
	b.done = true
	buf, over := b.buf, b.over
	b.buf = nil
	b.mu.Unlock()
	if err != nil || over || len(buf) == 0 {
		return err
	}
	b.parse(buf)
	return err
}

// parse records the body's fetch entries and warms the peer candidate
// cache with one tracker lookup over their distinct spans. A JSON decoder
// may stop short of trailing whitespace before closing, so completeness is
// proven by the unmarshal itself: truncated bodies fail it and record
// nothing.
func (b *reconBody) parse(buf []byte) {
	seen := map[tracker.XorbQuery]struct{}{}
	var queries []tracker.XorbQuery
	collect := func(hash string, chunkStart, chunkEnd uint32) {
		q := tracker.XorbQuery{Hash: hash, Range: tracker.ChunkSpan{Start: chunkStart, End: chunkEnd}}
		if _, ok := seen[q]; ok || len(queries) >= maxLookupSpans {
			return
		}
		seen[q] = struct{}{}
		queries = append(queries, q)
	}
	if b.v2 {
		var rec download.ReconstructionResponseV2
		if json.Unmarshal(buf, &rec) != nil {
			return
		}
		coverage := termCoverageV2(rec.Terms, rec.Xorbs, rec.OffsetIntoFirstRange)
		for hash, fetches := range rec.Xorbs {
			for _, f := range fetches {
				for _, rg := range f.Ranges {
					id := fetchEntryID{hash: hash, chunkStart: rg.Chunks.Start, chunkEnd: rg.Chunks.End, url: f.URL, byteStart: rg.Bytes.Start, byteEnd: rg.Bytes.End}
					if b.t.record(f.URL, rg.Bytes.Start, rg.Bytes.End, hash, rg.Chunks.Start, rg.Chunks.End, b.fileHash, coverage[id]) {
						collect(hash, rg.Chunks.Start, rg.Chunks.End)
					}
				}
			}
		}
		if rec.OffsetIntoFirstRange > 0 && len(rec.Terms) > 0 {
			if url, rg := selectFetchV2(rec.Xorbs[rec.Terms[0].Hash], rec.Terms[0].Range); rg != nil {
				b.t.poisonKey(fetchKey{url: url, start: rg.Bytes.Start, end: rg.Bytes.End})
			}
		}
		b.t.fillPeerCache(queries)
		return
	}
	// V1 and the batch endpoint share the fetch_info shape.
	var rec download.ReconstructionResponseV1
	if json.Unmarshal(buf, &rec) != nil {
		return
	}
	var rangeFiles map[fetchEntryID]string
	var coverage map[fetchEntryID][]tracker.ChunkSpan
	if b.fileHash == "" {
		rangeFiles, coverage = batchRangeFiles(buf)
	} else {
		coverage = termCoverage(rec.Terms, rec.FetchInfo, rec.OffsetIntoFirstRange)
	}
	for hash, fetches := range rec.FetchInfo {
		for _, f := range fetches {
			fileHash := b.fileHash
			if fileHash == "" {
				fileHash = rangeFiles[batchEntryID(hash, f)]
			}
			if b.t.record(f.URL, f.URLRange.Start, f.URLRange.End, hash, f.Range.Start, f.Range.End, fileHash, coverage[batchEntryID(hash, f)]) {
				collect(hash, f.Range.Start, f.Range.End)
			}
		}
	}
	if b.fileHash != "" && rec.OffsetIntoFirstRange > 0 && len(rec.Terms) > 0 {
		if f := selectFetchV1(rec.FetchInfo[rec.Terms[0].Hash], rec.Terms[0].Range); f != nil {
			b.t.poisonKey(fetchKey{url: f.URL, start: f.URLRange.Start, end: f.URLRange.End})
		}
	}
	b.t.fillPeerCache(queries)
}

// fetchEntryID is the full identity of one batch fetch entry.
type fetchEntryID struct {
	hash                 string
	chunkStart, chunkEnd uint32
	url                  string
	byteStart, byteEnd   int64
}

func batchEntryID(hash string, f download.FetchInfoEntry) fetchEntryID {
	return fetchEntryID{
		hash:       hash,
		chunkStart: f.Range.Start,
		chunkEnd:   f.Range.End,
		url:        f.URL,
		byteStart:  f.URLRange.Start,
		byteEnd:    f.URLRange.End,
	}
}

// batchRangeFiles attributes each fetch entry of a batch reconstruction to
// the first file, in key order, whose terms actually consume it, running
// the pinned client's own selection per term: the containing entry with
// the smallest byte span. Entries no term selects stay unattributed — the
// client never fetches them, so nothing should be quarantined under a file
// that never consumed them. The second map carries each entry's coverage:
// the merged term ranges of the attributed file only, since only that
// file's verdict settles segments quarantined under it.
func batchRangeFiles(buf []byte) (map[fetchEntryID]string, map[fetchEntryID][]tracker.ChunkSpan) {
	var rec download.BatchReconstructionResponse
	if json.Unmarshal(buf, &rec) != nil || len(rec.Files) == 0 {
		return nil, nil
	}
	fileHashes := make([]string, 0, len(rec.Files))
	for fileHash := range rec.Files {
		fileHashes = append(fileHashes, fileHash)
	}
	sort.Strings(fileHashes)
	byEntry := map[fetchEntryID]string{}
	coverage := map[fetchEntryID][]tracker.ChunkSpan{}
	for _, fileHash := range fileHashes {
		for _, term := range rec.Files[fileHash] {
			if term.Range.Start >= term.Range.End {
				continue
			}
			f := selectFetchV1(rec.FetchInfo[term.Hash], term.Range)
			if f == nil {
				continue
			}
			id := batchEntryID(term.Hash, *f)
			owner, ok := byEntry[id]
			if !ok {
				owner = fileHash
				byEntry[id] = fileHash
			}
			if owner == fileHash {
				coverage[id] = append(coverage[id], tracker.ChunkSpan{Start: term.Range.Start, End: term.Range.End})
			}
		}
	}
	for id, spans := range coverage {
		coverage[id] = tracker.MergeSpans(spans)
	}
	return byEntry, coverage
}

// termCoverage maps each fetch entry of a single-file V1 reconstruction to
// the merged union of the term chunk ranges selecting it under the pinned
// client's smallest-containing rule — exactly the chunks the file's
// checksum verdict will have validated. A resumed reconstruction
// (offsetIntoFirst > 0) excludes its first term: the reader decodes the
// whole term but discards the skipped prefix, so those chunks never reach
// the checksum. The batch endpoint never carries an offset — the pinned
// client builds its per-file readers with offset zero.
func termCoverage(terms []download.Term, fetchInfo map[string][]download.FetchInfoEntry, offsetIntoFirst int64) map[fetchEntryID][]tracker.ChunkSpan {
	coverage := map[fetchEntryID][]tracker.ChunkSpan{}
	for i, term := range terms {
		if i == 0 && offsetIntoFirst > 0 {
			continue
		}
		if term.Range.Start >= term.Range.End {
			continue
		}
		f := selectFetchV1(fetchInfo[term.Hash], term.Range)
		if f == nil {
			continue
		}
		id := batchEntryID(term.Hash, *f)
		coverage[id] = append(coverage[id], tracker.ChunkSpan{Start: term.Range.Start, End: term.Range.End})
	}
	for id, spans := range coverage {
		coverage[id] = tracker.MergeSpans(spans)
	}
	return coverage
}

// termCoverageV2 is termCoverage over the V2 response shape.
func termCoverageV2(terms []download.Term, xorbs map[string][]download.XorbMultiRangeFetch, offsetIntoFirst int64) map[fetchEntryID][]tracker.ChunkSpan {
	coverage := map[fetchEntryID][]tracker.ChunkSpan{}
	for i, term := range terms {
		if i == 0 && offsetIntoFirst > 0 {
			continue
		}
		if term.Range.Start >= term.Range.End {
			continue
		}
		url, rg := selectFetchV2(xorbs[term.Hash], term.Range)
		if rg == nil {
			continue
		}
		id := fetchEntryID{hash: term.Hash, chunkStart: rg.Chunks.Start, chunkEnd: rg.Chunks.End, url: url, byteStart: rg.Bytes.Start, byteEnd: rg.Bytes.End}
		coverage[id] = append(coverage[id], tracker.ChunkSpan{Start: term.Range.Start, End: term.Range.End})
	}
	for id, spans := range coverage {
		coverage[id] = tracker.MergeSpans(spans)
	}
	return coverage
}

// selectFetchV1 mirrors the pinned selectFetchInfoV1: the entry containing
// the term's chunk range with the smallest byte span, first wins ties.
func selectFetchV1(fetches []download.FetchInfoEntry, termRange download.ChunkRange) *download.FetchInfoEntry {
	var best *download.FetchInfoEntry
	for i := range fetches {
		if fetches[i].Range.Start > termRange.Start || fetches[i].Range.End < termRange.End {
			continue
		}
		if best == nil || fetches[i].URLRange.End-fetches[i].URLRange.Start < best.URLRange.End-best.URLRange.Start {
			best = &fetches[i]
		}
	}
	return best
}

// selectFetchV2 mirrors the pinned selectFetchInfoV2: the range descriptor
// containing the term's chunk range with the smallest byte span, first wins
// ties, returned with the URL of the fetch entry holding it.
func selectFetchV2(fetches []download.XorbMultiRangeFetch, termRange download.ChunkRange) (string, *download.XorbRangeDescriptor) {
	var bestURL string
	var best *download.XorbRangeDescriptor
	for i := range fetches {
		for j := range fetches[i].Ranges {
			rg := &fetches[i].Ranges[j]
			if rg.Chunks.Start > termRange.Start || rg.Chunks.End < termRange.End {
				continue
			}
			if best == nil || rg.Bytes.End-rg.Bytes.Start < best.Bytes.End-best.Bytes.Start {
				bestURL, best = fetches[i].URL, rg
			}
		}
	}
	return bestURL, best
}

// record remembers one fetch entry, pruning expired entries and evicting
// the oldest past the cap, and reports whether the entry was accepted.
// Entries larger than the capture cap could never be teed, so they are not
// worth a slot — and rejecting them here keeps hostile byte ranges out of
// the arithmetic downstream. A malformed file hash is dropped rather than
// the entry: it only gates the peer step, as do the covered spans, which
// are kept only where they fall inside the entry's own chunk range.
// A re-record disagreeing with a live observation of the same key — a
// resumed and a full reconstruction of one file, or two files sharing a
// fetch — poisons the key's attribution until it expires: a term fetch
// cannot tell which flight it serves, and bytes quarantined under the
// wrong observation could be released by a checksum that never hashed
// them. The structural fields still take the newest observation: the local
// sources re-validate what they serve, and the origin needs no table.
func (t *captureTransport) record(url string, byteStart, byteEnd int64, xorbHash string, chunkStart, chunkEnd uint32, fileHash string, covered []tracker.ChunkSpan) bool {
	if url == "" || byteStart < 0 || byteEnd < byteStart || byteEnd-byteStart >= maxCaptureSegment ||
		chunkStart >= chunkEnd || !captureXorbHashRe.MatchString(xorbHash) {
		return false
	}
	if !captureXorbHashRe.MatchString(fileHash) {
		fileHash = ""
	}
	var kept []tracker.ChunkSpan
	for _, span := range covered {
		if span.Start >= chunkStart && span.End <= chunkEnd && span.Start < span.End {
			kept = append(kept, span)
		}
	}
	kept = tracker.MergeSpans(kept)
	k := fetchKey{url: url, start: byteStart, end: byteEnd}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prune(now)
	if now.Before(t.attributionStandDown) {
		fileHash, kept = "", nil
	}
	conflicted := false
	if e, ok := t.entries[k]; ok {
		if e.expires.After(now) {
			conflicted = e.conflicted ||
				e.info.xorbHash != xorbHash || e.info.chunkStart != chunkStart || e.info.chunkEnd != chunkEnd ||
				e.info.fileHash != fileHash || !slices.Equal(e.info.covered, kept)
		}
	} else {
		for len(t.entries) >= maxFetchEntries {
			// Refusing the new key once only poisoned entries remain keeps
			// the unserved term on the origin — never on a laundered key.
			if !t.evictOldest(now) {
				return false
			}
		}
		t.order = append(t.order, k)
	}
	if conflicted {
		fileHash, kept = "", nil
	}
	t.entries[k] = tableEntry{
		info:       fetchInfo{xorbHash: xorbHash, chunkStart: chunkStart, chunkEnd: chunkEnd, fileHash: fileHash, covered: kept},
		expires:    now.Add(fetchEntryTTL),
		conflicted: conflicted,
	}
	return true
}

// poisonKey marks one fetch key untrustworthy for attribution from birth:
// the resumed reconstruction naming it will fetch it while hashing less
// than any normal observation of the same file would claim, so no clean
// same-key record may exist while that flight can still fetch — not even
// one arriving before any conflict could be detected. An existing live
// entry keeps its structural fields; a missing one is created poisoned
// under the same cap rules as record. When no slot can be freed, the whole
// table stands down instead: attribution is stripped from every record
// until the flight's table lifetime has passed, so a slot freeing a moment
// later cannot hand the suppressed key clean coverage.
func (t *captureTransport) poisonKey(k fetchKey) {
	if k.url == "" || k.start < 0 || k.end < k.start || k.end-k.start >= maxCaptureSegment {
		return
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prune(now)
	e, ok := t.entries[k]
	if !ok || !e.expires.After(now) {
		if !ok {
			for len(t.entries) >= maxFetchEntries {
				if !t.evictOldest(now) {
					if until := now.Add(fetchEntryTTL); until.After(t.attributionStandDown) {
						t.attributionStandDown = until
					}
					return
				}
			}
			t.order = append(t.order, k)
		}
		e = tableEntry{}
	}
	e.info.fileHash, e.info.covered = "", nil
	e.conflicted = true
	e.expires = now.Add(fetchEntryTTL)
	t.entries[k] = e
}

// lookup returns the live entry for one fetch key.
func (t *captureTransport) lookup(k fetchKey) (fetchInfo, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[k]
	if !ok || !e.expires.After(time.Now()) {
		return fetchInfo{}, false
	}
	return e.info, true
}

// fillPeerCache resolves one reconstruction's spans to holding peers with a
// single tracker lookup and caches the answers. It runs synchronously on
// the reconstruction parse path: the pinned client closes the body before
// any term fetch is issued, so the cache is warm exactly when needed, and
// the tracker client's timeout bounds the stall a dead tracker can add.
func (t *captureTransport) fillPeerCache(queries []tracker.XorbQuery) {
	if t.chain == nil || t.chain.tracker == nil || len(queries) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerLookupTimeout)
	defer cancel()
	resp, err := t.chain.tracker.Lookup(ctx, &tracker.LookupRequest{Xorbs: queries})
	if err != nil {
		return
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prunePeerCache(now)
	// Only keys this lookup asked for are cached, so a hostile tracker
	// cannot grow the cache past the query set. Empty answers are not
	// cached here: the term fetches that follow re-ask per span, so peers
	// announcing between this parse and the fetch are still discovered.
	for _, q := range queries {
		key := fmt.Sprintf("%s:%d-%d", q.Hash, q.Range.Start, q.Range.End)
		peers := vetCandidates(resp.Xorbs[key])
		if len(peers) == 0 {
			continue
		}
		t.storePeerAnswerLocked(key, peers, now.Add(peerLookupTTL))
	}
}

// vetCandidates canonicalizes, filters, deduplicates and caps one span's
// tracker-supplied candidates. The result is never nil, so callers can
// cache it as a definite answer.
func vetCandidates(locs []tracker.XorbLocation) []string {
	peers := make([]string, 0, min(len(locs), maxPeersPerSpan))
	seen := map[string]struct{}{}
	for _, loc := range locs {
		if len(peers) >= maxPeersPerSpan {
			break
		}
		peer := peerBaseURL(loc.Peer)
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		peers = append(peers, peer)
	}
	return peers
}

// storePeerAnswerLocked caches one span's answer under the FIFO caps. A
// refresh moves the key to the back of the order, so the answers of the
// lookup being stored are always the last to be evicted — a full warm
// cache can never evict a key the lookup just delivered. Caller holds t.mu.
func (t *captureTransport) storePeerAnswerLocked(key string, peers []string, expires time.Time) {
	if _, ok := t.peerCache[key]; ok {
		for i, k := range t.peerOrder {
			if k == key {
				t.peerOrder = append(t.peerOrder[:i], t.peerOrder[i+1:]...)
				break
			}
		}
	} else {
		for len(t.peerCache) >= maxPeerLookups {
			t.evictOldestPeerCache()
		}
	}
	t.peerOrder = append(t.peerOrder, key)
	t.peerCache[key] = peerCacheEntry{peers: peers, expires: expires}
}

// lookupSpanOnMiss resolves one span's candidates with a bounded tracker
// lookup when a term fetch finds no live cached answer, so peers that
// announced after the reconstruction was parsed are discovered mid-flight.
// The vetted answer is cached under the span key either way: positives at
// the standard TTL, empty or failed lookups briefly, so retries of a cold
// span do not hammer the tracker. Lookups run under the request context —
// close never waits on them, and the timeout bounds any straggler.
func (t *captureTransport) lookupSpanOnMiss(ctx context.Context, key string, q tracker.XorbQuery) []string {
	ctx, cancel := context.WithTimeout(ctx, missLookupTimeout)
	defer cancel()
	var peers []string
	resp, err := t.chain.tracker.Lookup(ctx, &tracker.LookupRequest{Xorbs: []tracker.XorbQuery{q}})
	if err == nil {
		peers = vetCandidates(resp.Xorbs[key])
	}
	ttl := peerLookupTTL
	if len(peers) == 0 {
		peers = []string{}
		ttl = negativePeerTTL
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prunePeerCache(now)
	t.storePeerAnswerLocked(key, peers, now.Add(ttl))
	return peers
}

// peerBaseURL vets one tracker-supplied candidate into a canonical base
// URL: http or https, a host, no userinfo — net/http turns URL
// credentials into an Authorization header, which peers must never
// receive — and no query or fragment, which have no place in a base URL
// and would corrupt every path appended to it. Anything else is dropped.
func peerBaseURL(s string) string {
	if s == "" || len(s) > maxPeerURLLen {
		return ""
	}
	s = canonicalPeerURL(s)
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return ""
	}
	return s
}

// cachedPeers returns the live cached candidates for one span key. Nil
// means no live entry; a non-nil empty slice is a cached negative answer —
// a recent lookup found no peers, so the span goes to the origin without
// asking again.
func (t *captureTransport) cachedPeers(key string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.peerCache[key]
	if !ok || !e.expires.After(time.Now()) {
		return nil
	}
	return e.peers
}

// prunePeerCache drops expired peer cache entries from the front of the
// insertion order, mirroring prune. Caller holds t.mu.
func (t *captureTransport) prunePeerCache(now time.Time) {
	for len(t.peerOrder) > 0 {
		key := t.peerOrder[0]
		e, ok := t.peerCache[key]
		if ok && e.expires.After(now) {
			return
		}
		if ok {
			delete(t.peerCache, key)
		}
		t.peerOrder = t.peerOrder[1:]
	}
}

// evictOldestPeerCache removes the oldest live peer cache entry. Caller
// holds t.mu.
func (t *captureTransport) evictOldestPeerCache() {
	for len(t.peerOrder) > 0 {
		key := t.peerOrder[0]
		t.peerOrder = t.peerOrder[1:]
		if _, ok := t.peerCache[key]; ok {
			delete(t.peerCache, key)
			return
		}
	}
}

// coolDown skips a failed peer for peerCooldown. Expired entries are
// pruned on insert; a full map drops the new entry, which only costs
// retries against a bad peer, never correctness.
func (t *captureTransport) coolDown(peer string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for p, expires := range t.cooldown {
		if !expires.After(now) {
			delete(t.cooldown, p)
		}
	}
	if _, ok := t.cooldown[peer]; !ok && len(t.cooldown) >= maxCooldownPeers {
		return
	}
	t.cooldown[peer] = now.Add(peerCooldown)
}

// cooledDown reports whether a peer is still cooling down.
func (t *captureTransport) cooledDown(peer string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cooldown[peer].After(time.Now())
}

// sweepQuarantine purges quarantined segments whose ingest flight never
// settled within quarantineTTL, with the same treatment a failed verdict
// applies: the segment is deleted, the supplying peer cools down and the
// client's chunk cache for the xorb is dropped. Registry items expire
// first; then the store is walked for quarantined metas past the TTL, so
// segments whose registry record is gone — restart, cap refusal, a settle
// whose Release or Delete failed — are still purged from their persisted
// ownership alone. It runs from the agent's sweeper ticker, registered as
// a write so close never races it.
func (t *captureTransport) sweepQuarantine() {
	t.sweepQuarantineBefore(time.Now().Add(-quarantineTTL))
}

func (t *captureTransport) sweepQuarantineBefore(cutoff time.Time) {
	if !t.beginWrite() {
		return
	}
	defer t.writes.Done()
	if t.chain != nil && t.chain.quar != nil {
		for _, it := range t.chain.quar.expire(cutoff) {
			t.purgeQuarantined(it.xorbHash, it.span, it.peer, cutoff)
		}
	}
	_ = t.segs.Walk(func(seg segstore.Segment) error {
		if !seg.Quarantined {
			return nil
		}
		// A quarantined meta without a timestamp predates durable ownership;
		// no registry record can have survived that binary's restart, so the
		// segment is unsettleable and purged immediately.
		if seg.QuarantinedAt > 0 && !time.Unix(seg.QuarantinedAt, 0).Before(cutoff) {
			return nil
		}
		t.purgeQuarantined(seg.XorbHash, tracker.ChunkSpan{Start: seg.ChunkStart, End: seg.ChunkEnd}, seg.Peer, cutoff)
		return nil
	})
}

// expireQuarantineRegistry purges only registry-tracked expired items — the
// cheap sweep run before each quarantine store to free registry room; the
// ticker sweep additionally walks the store for recordless segments.
func (t *captureTransport) expireQuarantineRegistry() {
	if t.chain == nil || t.chain.quar == nil || !t.beginWrite() {
		return
	}
	defer t.writes.Done()
	cutoff := time.Now().Add(-quarantineTTL)
	for _, it := range t.chain.quar.expire(cutoff) {
		t.purgeQuarantined(it.xorbHash, it.span, it.peer, cutoff)
	}
}

// purgeQuarantined applies the failed-verdict treatment to one quarantined
// segment the sweep deemed expired. The chunk-cache purge commits first and
// unconditionally: the expiring flight is dead, and its fetch may have
// decoded chunks beyond any stored segment into the cache — standing down
// later must not leave them replayable, and a racing re-owner only pays a
// refetch. The segment delete then re-reads the meta under the store lock,
// so a segment a concurrent flight re-owned or a verdict released is never
// lost to a stale expiry decision — and only an actual removal cools the
// peer. A failed delete leaves the durable meta in place for the next
// sweep. Caller holds a write registration.
func (t *captureTransport) purgeQuarantined(xorbHash string, span tracker.ChunkSpan, peer string, cutoff time.Time) {
	if t.purgeChunks != nil {
		t.purgeChunks(xorbHash)
	}
	removed, err := t.segs.DeleteQuarantinedBefore(xorbHash, span.Start, span.End, cutoff)
	if err != nil || !removed {
		return
	}
	if peer != "" {
		t.coolDown(peer)
	}
}

// parseTermRange extracts the single bounded byte range of a term fetch.
// end-start is overflow-safe here: the regex admits only non-negative
// values, so the difference of two in-range int64s stays in range.
func parseTermRange(req *http.Request) (start, end int64, ok bool) {
	m := byteRangeRe.FindStringSubmatch(req.Header.Get("Range"))
	if m == nil {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(m[1], 10, 64)
	end, err2 := strconv.ParseInt(m[2], 10, 64)
	if err1 != nil || err2 != nil || start > end || end-start >= maxCaptureSegment {
		return 0, 0, false
	}
	return start, end, true
}

// serveFromChain answers a recorded term fetch from the best available
// source — a stored segment, the full local xorb, then cluster peers — or
// returns nil to let the request pass through to the origin. Synthesized
// responses replicate exactly what the pinned client consumes from an
// origin 206: the status code, a Content-Range whose start equals the
// requested offset (httpseek rejects anything else), and the exact body
// bytes its xorb decoder walks.
func (t *captureTransport) serveFromChain(req *http.Request) *http.Response {
	start, end, ok := parseTermRange(req)
	if !ok {
		return nil
	}
	info, ok := t.lookup(fetchKey{url: req.URL.String(), start: start, end: end})
	if !ok {
		return nil
	}
	if resp := t.serveLocalSegment(req, info, start, end); resp != nil {
		return resp
	}
	if resp := t.serveLocalXorb(req, info, start, end); resp != nil {
		return resp
	}
	return t.serveFromPeers(req, info, start, end)
}

// serveLocalSegment answers from a stored segment covering the requested
// bytes. The sub-slice is re-validated against the entry's chunk count on
// this read, so disk corruption degrades to the next source instead of
// feeding the client's decoder broken headers.
func (t *captureTransport) serveLocalSegment(req *http.Request, info fetchInfo, start, end int64) *http.Response {
	seg, ok := t.segs.FindByteRange(info.xorbHash, start, end)
	if !ok {
		return nil
	}
	f, err := t.segs.Open(seg)
	if err != nil {
		return nil
	}
	defer func() {
		_ = f.Close()
	}()
	if _, err := f.Seek(start-seg.ByteStart, io.SeekStart); err != nil {
		return nil
	}
	buf := make([]byte, end-start+1)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil
	}
	if segstore.ValidateChunkSequence(buf, info.chunkEnd-info.chunkStart) != nil {
		return nil
	}
	return synthesize206(req, buf, start, end)
}

// serveLocalXorb answers from the full locally stored xorb, but only when
// the local packed span for the entry's chunks is exactly as long as the
// advertised byte range — equal lengths mean the encodings agree, so the
// local bytes are a valid stand-in. Term fetch URLs are opaque origin URLs
// with no namespace to extract; FileStorage ignores the namespace argument,
// so the client's default is used.
func (t *captureTransport) serveLocalXorb(req *http.Request, info fetchInfo, start, end int64) *http.Response {
	if t.chain == nil || t.chain.stor == nil {
		return nil
	}
	hash, err := xet.ParseXorbHash(info.xorbHash)
	if err != nil {
		return nil
	}
	ctx := req.Context()
	has, err := t.chain.stor.HasXorb(ctx, "default", hash)
	if err != nil || !has {
		return nil
	}
	byteStart, byteEnd, err := t.chain.stor.GetXorbDataRange(ctx, "default", hash, info.chunkStart, info.chunkEnd)
	if err != nil || byteEnd-byteStart != end-start {
		return nil
	}
	f, err := t.chain.stor.GetXorbReadSeekCloser(ctx, "default", hash)
	if err != nil {
		return nil
	}
	defer func() {
		_ = f.Close()
	}()
	if _, err := f.Seek(byteStart, io.SeekStart); err != nil {
		return nil
	}
	buf := make([]byte, end-start+1)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil
	}
	return synthesize206(req, buf, start, end)
}

// serveFromPeers tries at most maxPeerAttempts cached candidates for the
// entry's exact span, sequentially: the covered chunk sub-ranges of the
// first validated body are stored in quarantine — invisible to serving and
// announcing until the file's ingest verdict releases them — registered
// for that verdict, and only then is the body served. A body whose
// bookkeeping fails is never served: unbooked bytes would enter the
// client's decoded-chunk cache with no verdict able to purge them, so the
// peer cools down and the chain moves on. A span with no live cached
// answer is re-resolved with a bounded single-span lookup first, so peers
// that announced after the reconstruction was parsed are still found.
// Failed peers cool down; running out of candidates falls back to the
// origin. Entries without a file hash or without covered terms never go to
// peers: their quarantined segments could never be released.
func (t *captureTransport) serveFromPeers(req *http.Request, info fetchInfo, start, end int64) *http.Response {
	if t.chain == nil || t.chain.tracker == nil || info.fileHash == "" || len(info.covered) == 0 {
		return nil
	}
	key := fmt.Sprintf("%s:%d-%d", info.xorbHash, info.chunkStart, info.chunkEnd)
	peers := t.cachedPeers(key)
	if peers == nil {
		peers = t.lookupSpanOnMiss(req.Context(), key, tracker.XorbQuery{
			Hash:  info.xorbHash,
			Range: tracker.ChunkSpan{Start: info.chunkStart, End: info.chunkEnd},
		})
	}
	attempts := 0
	for _, peer := range peers {
		// Candidates were vetted and canonicalized when cached.
		if peer == t.chain.self || t.cooledDown(peer) {
			continue
		}
		if attempts >= maxPeerAttempts {
			break
		}
		attempts++
		buf := t.fetchFromPeer(req.Context(), peer, info, start, end)
		if buf == nil {
			t.coolDown(peer)
			continue
		}
		// Expired never-settled flights leave first, freeing registry room.
		t.expireQuarantineRegistry()
		if !t.storeQuarantined(peer, info, start, buf) {
			t.coolDown(peer)
			continue
		}
		return synthesize206(req, buf, start, end)
	}
	return nil
}

// fetchFromPeer fetches the term bytes from one peer's xorb route and
// returns them only fully vetted: a 206 from a marked agent promising and
// delivering exactly the expected length, whose bytes walk as exactly the
// entry's chunks. Anything else is nil. The header wait is bounded by the
// peer client's transport; the deadline bounds the body too — spans are
// capped at 32MiB, which any healthy peer link moves well within it, so a
// trickling peer cannot pin a term fetch much past the origin alternative.
func (t *captureTransport) fetchFromPeer(ctx context.Context, peer string, info fetchInfo, start, end int64) []byte {
	want := end - start + 1
	ctx, cancel := context.WithTimeout(ctx, max(4*t.chain.peerTimeout, 30*time.Second))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/xorbs/default/%s", peer, info.xorbHash), nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := t.chain.peerClient.Do(req)
	if err != nil {
		return nil
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusPartialContent ||
		resp.Header.Get(agentMarkerHeader) == "" ||
		resp.ContentLength != want {
		return nil
	}
	// The transport enforces Content-Length framing, so a full read of want
	// bytes is a complete body.
	buf := make([]byte, want)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil
	}
	if segstore.ValidateChunkSequence(buf, info.chunkEnd-info.chunkStart) != nil {
		return nil
	}
	return buf
}

// storeQuarantined persists the covered chunk sub-ranges of one validated
// peer-sourced term body in quarantine — with the file hash, peer and time
// persisted in each meta — and registers them atomically under the file
// hash for the ingest verdict, reporting whether everything took: the
// caller must not serve the bytes otherwise. Only chunks the file's own
// terms consume are stored; an entry wider than the terms would otherwise
// let chunks the verdict never validated ride a passing checksum into
// release. The announcer is never notified here: quarantined segments must
// stay invisible until released. Byte-identical sub-ranges some earlier
// flight already quarantined register again under this file, so this
// file's verdict settles them too (a failed one conservatively purges the
// shared segments); partially stored sub-ranges left by a failed run stay
// recordless and invisible until the sweep purges them from their metas.
func (t *captureTransport) storeQuarantined(peer string, info fetchInfo, start int64, buf []byte) bool {
	if t.chain.quar == nil || !t.beginWrite() {
		return false
	}
	defer t.writes.Done()
	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(buf))
	if err != nil {
		return false
	}
	// A wider entry leaves chunks outside the covered union in the client's
	// decoded-chunk cache; the verdict must drop that cache even on success.
	partial := len(info.covered) != 1 ||
		info.covered[0] != (tracker.ChunkSpan{Start: info.chunkStart, End: info.chunkEnd})
	items := make([]quarItem, 0, len(info.covered))
	for _, span := range info.covered {
		subStart, subEnd, err := xorb.ChunkDataRangeFromOffsets(offsets, span.Start-info.chunkStart, span.End-info.chunkStart)
		if err != nil {
			return false
		}
		stored, err := t.segs.PutQuarantined(info.xorbHash, span.Start, span.End,
			start+subStart, start+subEnd, buf[subStart:subEnd+1], info.fileHash, peer)
		if err != nil || !stored {
			return false
		}
		items = append(items, quarItem{xorbHash: info.xorbHash, span: span, peer: peer, partial: partial})
	}
	return t.chain.quar.add(info.fileHash, items...)
}

// synthesize206 builds the in-memory stand-in for an origin term fetch
// response. The pinned consumer (httpseek behind DownloadXorbWithURL)
// requires status 206 with a Content-Range whose start equals the requested
// offset and whose end does not exceed the requested end; the "*" total
// makes it hand the body through unwrapped. The body cannot fail, so the
// client's resume machinery never engages.
func synthesize206(req *http.Request, body []byte, start, end int64) *http.Response {
	h := http.Header{}
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, end))
	return &http.Response{
		Status:        "206 Partial Content",
		StatusCode:    http.StatusPartialContent,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

// prune drops expired entries from the front of the insertion order. A
// refreshed entry keeps its old position and stops the walk early; the cap
// still bounds the table. Caller holds t.mu.
func (t *captureTransport) prune(now time.Time) {
	for len(t.order) > 0 {
		k := t.order[0]
		e, ok := t.entries[k]
		if ok && e.expires.After(now) {
			return
		}
		if ok {
			delete(t.entries, k)
		}
		t.order = t.order[1:]
	}
}

// evictOldest removes the oldest live unpoisoned entry, reporting whether
// a slot was freed. Poisoned entries are immune while live: evicting one
// would let a fresh record re-arm a key a suppressed flight may still
// fetch. Caller holds t.mu.
func (t *captureTransport) evictOldest(now time.Time) bool {
	for i := 0; i < len(t.order); i++ {
		k := t.order[i]
		e, ok := t.entries[k]
		if ok && e.conflicted && e.expires.After(now) {
			continue
		}
		t.order = append(t.order[:i], t.order[i+1:]...)
		if ok {
			delete(t.entries, k)
			return true
		}
		i--
	}
	return false
}

// observeTermFetch tees a term fetch body into the segment store when the
// request matches a recorded entry and the response promises exactly the
// expected bytes.
func (t *captureTransport) observeTermFetch(req *http.Request, resp *http.Response) {
	start, end, ok := parseTermRange(req)
	if !ok {
		return
	}
	want := end - start + 1
	info, ok := t.lookup(fetchKey{url: req.URL.String(), start: start, end: end})
	if !ok {
		return
	}
	if resp.StatusCode != http.StatusPartialContent &&
		(resp.StatusCode != http.StatusOK || resp.ContentLength != want) {
		return
	}
	if _, covered := t.segs.Find(info.xorbHash, info.chunkStart, info.chunkEnd); covered {
		return
	}
	resp.Body = &captureBody{
		inner: resp.Body,
		t:     t,
		info:  info,
		span:  byteSpan{start: start, end: end},
		want:  want,
		buf:   make([]byte, 0, want),
	}
}

type byteSpan struct {
	start, end int64
}

// captureBody accumulates a term fetch body while relaying it unchanged.
// Only a body seen complete — exactly the expected bytes followed by a
// successful close — is stored; anything shorter, longer or errored is
// discarded. The mutex covers the caller's right to Close concurrently with
// a Read.
type captureBody struct {
	inner io.ReadCloser
	t     *captureTransport
	info  fetchInfo
	span  byteSpan
	want  int64

	mu   sync.Mutex
	buf  []byte
	over bool
	done bool
}

func (b *captureBody) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)
	b.mu.Lock()
	if n > 0 && !b.over && !b.done {
		if int64(len(b.buf)+n) > b.want {
			b.over, b.buf = true, nil
		} else {
			b.buf = append(b.buf, p[:n]...)
		}
	}
	// An errored stream is never stored, even at the expected length: the
	// error may have cut trailing bytes that would have overrun it.
	if err != nil && err != io.EOF && !b.done {
		b.over, b.buf = true, nil
	}
	b.mu.Unlock()
	return n, err
}

// Close is the only commit point: readers may never observe EOF — decoders
// stop at the last chunk — and the pinned client always closes term bodies,
// so a body that accumulated exactly the expected bytes and closed cleanly
// is complete. The full entry range is stored even when wider than the
// file's terms: origin bytes are trusted by design — the origin is the
// authority a verdict would check against.
func (b *captureBody) Close() error {
	err := b.inner.Close()
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return err
	}
	b.done = true
	buf, over := b.buf, b.over
	b.buf = nil
	b.mu.Unlock()
	if err != nil || over || int64(len(buf)) != b.want {
		return err
	}
	if !b.t.beginWrite() {
		return err
	}
	defer b.t.writes.Done()
	if putErr := b.t.segs.Put(b.info.xorbHash, b.info.chunkStart, b.info.chunkEnd, b.span.start, b.span.end, buf); putErr == nil && b.t.notify != nil {
		// A successful Put may have kept a first-stored quarantined encoding
		// for the key instead (keep-first), so only claims Find can actually
		// serve are announced.
		if _, ok := b.t.segs.Find(b.info.xorbHash, b.info.chunkStart, b.info.chunkEnd); ok {
			b.t.notify(b.info.xorbHash, tracker.ChunkSpan{Start: b.info.chunkStart, End: b.info.chunkEnd})
		}
	}
	return err
}
