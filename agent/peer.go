package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet/mirror"

	"github.com/wzshiming/xet-pd/tracker"
)

// localOnlyHeader marks a peer probe: answer only from local ready state and
// never contact the upstream, so probes cannot recurse through the cluster.
const localOnlyHeader = "X-Xet-PD-Local-Only"

// peerReadyTTL bounds how long a mutable-revision index entry may answer
// local-only probes: past it the probing agent falls back to the origin for
// fresh metadata, so a cluster of mutual peers cannot revalidate a moved
// branch against itself forever. Commit-pinned entries never expire — their
// content is immutable.
const peerReadyTTL = 15 * time.Minute

// resolvePathRe is the public hub download path shape, the same pattern the
// mirror routes on.
var resolvePathRe = regexp.MustCompile(`^/(.+?)/resolve/([^/]+)/(.+)$`)

// commitRe matches an immutable 40-hex commit revision, the same shape the
// mirror pins branches to.
var commitRe = regexp.MustCompile(`^[0-9a-f]{40}$`)

// readyEntry is one persisted ready file, keyed by its resolve path: exactly
// what a local-only answer needs to mimic the mirror's ready resolve
// response.
type readyEntry struct {
	Key       string    `json:"key"`
	SHA256    string    `json:"sha256"`
	FileHash  string    `json:"file_hash,omitempty"`
	Size      int64     `json:"size"`
	ETag      string    `json:"etag,omitempty"`
	Commit    string    `json:"commit,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// readyIndex stores one JSON file per ready resolve path — the object key is
// the file path — and reads it back per lookup: the files are the index, so
// restarts need no recovery step.
type readyIndex struct {
	dir string
}

func (ix readyIndex) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(ix.dir, hex.EncodeToString(sum[:])+".json")
}

func (ix readyIndex) get(key string) *readyEntry {
	data, err := os.ReadFile(ix.path(key))
	if err != nil {
		return nil
	}
	var e readyEntry
	if json.Unmarshal(data, &e) != nil || e.Key != key || e.SHA256 == "" {
		return nil
	}
	return &e
}

func (ix readyIndex) put(e *readyEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal ready entry: %w", err)
	}
	path := ix.path(e.Key)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create ready index dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write ready entry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize ready entry: %w", err)
	}
	return nil
}

func (ix readyIndex) delete(key string) {
	_ = os.Remove(ix.path(key))
}

// walk calls fn for every persisted ready entry.
func (ix readyIndex) walk(fn func(*readyEntry)) {
	dirEntries, err := os.ReadDir(ix.dir)
	if err != nil {
		return
	}
	for _, de := range dirEntries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(ix.dir, de.Name()))
		if err != nil {
			continue
		}
		var e readyEntry
		if json.Unmarshal(data, &e) != nil || e.Key == "" || e.SHA256 == "" {
			continue
		}
		fn(&e)
	}
}

// peerGate wraps the composed handler with the serve side of peer exchange.
type peerGate struct {
	next     http.Handler
	mirror   *mirror.Handler
	idx      readyIndex
	external string
	// notify, when set, reports a freshly recorded ready entry and its
	// resolve-path aliases, feeding tracker announces.
	notify func(e *readyEntry, keys []string)
	// verdict, when set, reports each settled ingest flight so quarantined
	// segments can be released or purged. keys are every resolve path form
	// the flight is known under — the downstream path, plus the canonical
	// commit alias when the entry names one. fileHash is empty when the
	// flight failed — failed entries carry none — leaving recovery to the
	// caller.
	verdict func(keys []string, fileHash string, ok bool)
	// pin, when set, re-teaches the selector's branch→commit pin from each
	// successful flight, so later warm-branch failures — which carry no
	// commit of their own — can still be translated.
	pin func(repo, rev, commit string)

	mu       sync.Mutex
	inflight map[string]struct{}
	closed   bool
	// done unblocks recorders waiting on ingest flights at shutdown;
	// recorders tracks them so close can drain before storage disappears.
	done      chan struct{}
	recorders sync.WaitGroup
}

func newPeerGate(next http.Handler, m *mirror.Handler, idx readyIndex, external string) *peerGate {
	return &peerGate{
		next:     next,
		mirror:   m,
		idx:      idx,
		external: strings.TrimRight(external, "/"),
		inflight: map[string]struct{}{},
		done:     make(chan struct{}),
	}
}

func (g *peerGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m := resolvePathRe.FindStringSubmatch(r.URL.EscapedPath())
	if r.Header.Get(localOnlyHeader) != "" {
		g.serveLocalOnly(w, r, m)
		return
	}
	if m != nil && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		g.startRecord(m[0], m[1], m[2], m[3])
	}
	g.next.ServeHTTP(w, r)
}

// startRecord launches one tracked record goroutine, unless the gate is
// already closed.
func (g *peerGate) startRecord(key, repo, rev, path string) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.recorders.Add(1)
	g.mu.Unlock()
	go func() {
		defer g.recorders.Done()
		g.record(key, repo, rev, path)
	}()
}

// close stops accepting new record work, unblocks recorders waiting on
// ingest flights and waits for them, so nothing writes to the indexes after
// shutdown returns. The mirror's own ingest goroutines keep running — they
// are process-lifetime work, as upstream xetd treats them.
func (g *peerGate) close() {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		close(g.done)
	}
	g.mu.Unlock()
	g.recorders.Wait()
}

// serveLocalOnly answers strictly from the ready index: a hit mimics the
// mirror's ready resolve response with URLs on this agent, anything else is
// 404. The request never reaches the mirror, so it can never ingest or probe
// the origin. Mutable-revision entries older than peerReadyTTL are withheld.
func (g *peerGate) serveLocalOnly(w http.ResponseWriter, r *http.Request, m []string) {
	if m == nil || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		http.NotFound(w, r)
		return
	}
	e := g.idx.get(m[0])
	if e == nil || (!commitRe.MatchString(m[2]) && time.Since(e.CheckedAt) > peerReadyTTL) {
		http.NotFound(w, r)
		return
	}
	base := g.base(r)
	if e.ETag != "" {
		quoted := `"` + e.ETag + `"`
		w.Header().Set("ETag", quoted)
		w.Header().Set("X-Linked-Etag", quoted)
	}
	w.Header().Set("X-Linked-Size", strconv.FormatInt(e.Size, 10))
	if e.Commit != "" {
		w.Header().Set("X-Repo-Commit", e.Commit)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	if e.FileHash != "" {
		w.Header().Add("Link", fmt.Sprintf("<%s/xet-token>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", base, base, e.FileHash))
		w.Header().Set("X-Xet-Hash", e.FileHash)
	}
	if e.Size == 0 {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, base+"/xet-bridge/"+e.SHA256, http.StatusFound)
}

// base mirrors the mirror's externalBase derivation so local-only answers
// carry the same URLs the mirror would mint.
func (g *peerGate) base(r *http.Request) string {
	if g.external != "" {
		return g.external
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}

// record keeps the ready index in step with the mirror by joining its ingest
// flight for the file (a cheap join once the entry is ready) and persisting
// the outcome.
func (g *peerGate) record(key, repo, rev, path string) {
	if e := g.idx.get(key); e != nil && time.Since(e.CheckedAt) < time.Minute {
		return
	}
	g.mu.Lock()
	if _, busy := g.inflight[key]; busy {
		g.mu.Unlock()
		return
	}
	g.inflight[key] = struct{}{}
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.inflight, key)
		g.mu.Unlock()
	}()

	in, err := g.mirror.Ingest(repo, rev, path)
	if err != nil {
		return
	}
	// A hung upstream must not pin shutdown: the ingest flight keeps running
	// in the mirror, but this recorder leaves without touching the index.
	select {
	case <-in.Done():
	case <-g.done:
		return
	}
	e, err := in.Entry()
	if err != nil || e == nil {
		g.idx.delete(key)
		if g.verdict != nil {
			g.verdict([]string{key}, "", false)
		}
		return
	}
	entry := readyEntry{
		SHA256:    e.SHA256,
		FileHash:  e.FileHash,
		Size:      e.Size,
		ETag:      e.ETag,
		Commit:    e.Commit,
		CheckedAt: time.Now(),
	}
	entry.Key = key
	_ = g.idx.put(&entry)
	keys := []string{key}
	// The mirror re-keys branch revisions to their pinned commit before
	// fetching, so probing peers use the canonical commit path: alias the
	// entry under it too.
	if commitRe.MatchString(e.Commit) && rev != e.Commit {
		entry.Key = "/" + repo + "/resolve/" + e.Commit + "/" + path
		_ = g.idx.put(&entry)
		keys = append(keys, entry.Key)
		if g.pin != nil {
			g.pin(repo, rev, e.Commit)
		}
	}
	if g.notify != nil {
		g.notify(&entry, keys)
	}
	if g.verdict != nil {
		g.verdict(keys, e.FileHash, true)
	}
}

// peerSelector is the mirror's upstream: a loopback-only handler that routes
// each resolve request to the first peer already holding the file — probed
// with localOnlyHeader so a cold cluster answers 404 instead of recursing —
// and everything else, plus every peer miss, to the origin. The origin keeps
// whatever Authorization the mirror attached; peers never see it.
type peerSelector struct {
	origin string
	// originHost is the origin's host part: the only host the mirror's
	// credential may ever be sent to, matching the pinned authInjector.
	originHost string
	peers      []string
	timeout    time.Duration
	// tracker, when set, names the peers holding a resolve key so only they
	// are probed; misses and errors fall back to broadcasting over peers.
	tracker *tracker.Client
	// self is this agent's own external URL, filtered from tracker answers
	// so an agent never selects itself as its upstream.
	self string
	// cooled, when set, reports peers still cooling down from a failed
	// flight, so retries do not keep choosing them.
	cooled func(peer string) bool
	// peerClient bounds the wait for peer response headers so a peer that
	// stalls after a successful probe cannot hang an ingest; originClient
	// leaves the origin unbounded (bodies can stream for a long time).
	peerClient   *http.Client
	originClient *http.Client

	// resolveHashes remembers the xet file hash the origin advertised for
	// each proxied resolve path, so a failed ingest — whose entry carries
	// no file hash — can still purge the segments quarantined under it.
	// branchCommits remembers which upstream commit each proxied branch
	// resolve was pinned to: once the mirror's branch mapping is warm it
	// resolves only the commit path, so a verdict reported under the branch
	// path needs the pin to reach the remembered hash. branchPinTTL bounds
	// pin life; non-positive never expires, for mirrors that never re-probe.
	// originETags memoizes the origin's content validator per resolve path
	// — entries may be negative — and resolvePeers remembers which peer
	// served each proxied resolve, for the failure verdict to cool.
	hashMu        sync.Mutex
	resolveHashes map[string]resolveHash
	branchCommits map[string]resolveHash
	originETags   map[string]resolveHash
	resolvePeers  map[string]resolveHash
	branchPinTTL  time.Duration
}

const (
	// resolveHashTTL bounds how long a proxied resolve's file hash or
	// branch pin is remembered; every ingest retry resolves again,
	// refreshing the entry.
	resolveHashTTL = time.Hour
	// maxResolveHashes caps each map; when full new notes are dropped,
	// costing only sweep-delayed purges, never correctness.
	maxResolveHashes = 4096
	// originETagNegTTL briefly remembers that the origin exposed no usable
	// validator — unreachable, refusing, or a non-sha etag — so a burst of
	// cold requests costs one probe, not one per request.
	originETagNegTTL = 30 * time.Second
)

// resolveHash is one remembered resolve-derived value with its expiry.
type resolveHash struct {
	hash    string
	expires time.Time
}

// live reports whether the entry is set and not expired; a zero expiry
// never expires (branch pins under a mirror that never revalidates).
func (e resolveHash) live(now time.Time) bool {
	return e.hash != "" && (e.expires.IsZero() || e.expires.After(now))
}

func newPeerSelector(origin string, peers []string, tc *tracker.Client, self string, timeout time.Duration, maxConns int) *peerSelector {
	noRedirect := func(*http.Request, []*http.Request) error {
		// Redirects go back verbatim: the mirror's probe and fetch clients
		// own redirect handling.
		return http.ErrUseLastResponse
	}
	peerTr := http.DefaultTransport.(*http.Transport).Clone()
	peerTr.MaxConnsPerHost = maxConns
	peerTr.ResponseHeaderTimeout = timeout
	normalized := make([]string, 0, len(peers))
	for _, p := range peers {
		if p = peerBaseURL(p); p != "" {
			normalized = append(normalized, p)
		}
	}
	trimmed := strings.TrimRight(origin, "/")
	originHost := ""
	if u, err := url.Parse(trimmed); err == nil {
		originHost = u.Host
	}
	return &peerSelector{
		origin:        trimmed,
		originHost:    originHost,
		peers:         normalized,
		tracker:       tc,
		self:          canonicalPeerURL(self),
		timeout:       timeout,
		peerClient:    &http.Client{Transport: peerTr, CheckRedirect: noRedirect},
		originClient:  &http.Client{Transport: http.DefaultTransport, CheckRedirect: noRedirect},
		resolveHashes: map[string]resolveHash{},
		branchCommits: map[string]resolveHash{},
		originETags:   map[string]resolveHash{},
		resolvePeers:  map[string]resolveHash{},
		branchPinTTL:  resolveHashTTL,
	}
}

// canonicalPeerURL normalizes a peer base URL for identity comparison:
// lower-cased scheme and host, default ports dropped, trailing slash
// trimmed. Unparseable inputs come back trimmed only.
func canonicalPeerURL(s string) string {
	s = strings.TrimRight(strings.TrimSpace(s), "/")
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return s
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)
	switch {
	case u.Scheme == "http" && strings.HasSuffix(host, ":80"):
		host = strings.TrimSuffix(host, ":80")
	case u.Scheme == "https" && strings.HasSuffix(host, ":443"):
		host = strings.TrimSuffix(host, ":443")
	}
	u.Host = host
	return u.String()
}

func (s *peerSelector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	target := path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	if resolvePathRe.MatchString(path) {
		// The origin's validator anchors every peer decision; it is fetched
		// at most once per request, and only once some candidate actually
		// claims to hold the file.
		val := &lazyValidator{fetch: func() string {
			ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
			defer cancel()
			return s.originValidator(ctx, r.Header.Get("Authorization"), path)
		}}
		if peer := s.pickPeer(r.Context(), path, target, val); peer != "" {
			if s.proxy(w, r, peer, target, true, val.get()) {
				return
			}
		}
	}
	_ = s.proxy(w, r, s.origin, target, false, "")
}

// lazyValidator memoizes one origin validator fetch across the candidate
// probes of a single selector request.
type lazyValidator struct {
	fetch func() string
	once  sync.Once
	val   string
}

func (l *lazyValidator) get() string {
	l.once.Do(func() { l.val = l.fetch() })
	return l.val
}

// pickPeer returns the first peer that verifiably holds the file — probe
// hit plus a validator matching the origin's — or "" when the whole
// cluster misses. Lookup, directed probes and the broadcast fallback each
// get their own deadline, so a hanging tracker or stale indexed peer
// cannot starve the healthy static peers of probe time.
func (s *peerSelector) pickPeer(ctx context.Context, path, target string, val *lazyValidator) string {
	if s.tracker != nil {
		if peer := s.trackerPick(ctx, path, target, val); peer != "" {
			return peer
		}
	}
	return s.broadcastPick(ctx, target, val)
}

// maxDirectedProbes caps how many tracker-indexed peers are tried, so a
// poisoned or stale answer costs a bounded number of probes before the
// static and origin fallbacks take over.
const maxDirectedProbes = 3

// trackerPick probes only the peers the tracker indexes for this key.
// Lookups may be stale, so a local-only probe still guards every answer;
// candidates are vetted by the same helper the term-fetch chain uses, so
// out-of-policy URLs from a hostile tracker are never probed at all.
func (s *peerSelector) trackerPick(ctx context.Context, path, target string, val *lazyValidator) string {
	lookupCtx, cancel := context.WithTimeout(ctx, s.timeout)
	resp, err := s.tracker.Lookup(lookupCtx, &tracker.LookupRequest{Keys: []string{path}})
	cancel()
	if err != nil {
		return ""
	}
	peers := resp.Keys[path].Peers
	if len(peers) > maxDirectedProbes {
		peers = peers[:maxDirectedProbes]
	}
	for _, peer := range peers {
		peer = peerBaseURL(peer)
		if peer == "" || peer == s.self || (s.cooled != nil && s.cooled(peer)) {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, s.timeout)
		etag, ok := s.probePeer(probeCtx, peer, target)
		cancel()
		if !ok {
			continue
		}
		want := val.get()
		if want == "" {
			// No anchor exists, so no candidate can ever qualify.
			return ""
		}
		if etag == want {
			return peer
		}
	}
	return ""
}

// broadcastPick concurrently probes every static peer (S1).
func (s *peerSelector) broadcastPick(ctx context.Context, target string, val *lazyValidator) string {
	if len(s.peers) == 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	type hit struct{ peer, etag string }
	hits := make(chan hit, len(s.peers))
	var wg sync.WaitGroup
	for _, peer := range s.peers {
		if s.cooled != nil && s.cooled(peer) {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			if etag, ok := s.probePeer(ctx, peer, target); ok {
				hits <- hit{peer: peer, etag: etag}
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(hits)
	}()
	for h := range hits {
		want := val.get()
		if want == "" {
			return ""
		}
		if h.etag == want {
			return h.peer
		}
	}
	return ""
}

// probePeer asks one peer whether it locally holds the file, returning the
// content validator it advertises. A hit must look like a ready resolve
// answer (an entity tag is always present), so an arbitrary service
// squatting on a peer address is not mistaken for one; whether the
// validator matches the origin's is the caller's decision.
func (s *peerSelector) probePeer(ctx context.Context, peer, target string) (string, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, peer+target, nil)
	if err != nil {
		return "", false
	}
	req.Header.Set(localOnlyHeader, "1")
	resp, err := s.peerClient.Do(req)
	if err != nil {
		return "", false
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", false
	}
	etag := headerValidator(resp.Header)
	return etag, etag != ""
}

// headerValidator extracts the content validator from resolve response
// headers the way the mirror's probe does: X-Linked-Etag wins over ETag,
// weak markers and quotes trimmed.
func headerValidator(h http.Header) string {
	e := h.Get("X-Linked-Etag")
	if e == "" {
		e = h.Get("ETag")
	}
	e = strings.TrimPrefix(strings.TrimSpace(e), "W/")
	return strings.Trim(e, `"`)
}

// noteResolve remembers the file hash the origin advertised for one
// resolve path. Expired entries are pruned on insert; a full map drops the
// note, which only delays the purge to the quarantine sweep.
func (s *peerSelector) noteResolve(path, hash string) {
	now := time.Now()
	s.hashMu.Lock()
	defer s.hashMu.Unlock()
	for p, e := range s.resolveHashes {
		if !e.expires.After(now) {
			delete(s.resolveHashes, p)
		}
	}
	if _, ok := s.resolveHashes[path]; !ok && len(s.resolveHashes) >= maxResolveHashes {
		return
	}
	s.resolveHashes[path] = resolveHash{hash: hash, expires: now.Add(resolveHashTTL)}
}

// noteBranchCommit remembers the upstream commit a proxied branch resolve
// was pinned to, keyed like the mirror's own branch mapping; successful
// ingest flights re-teach it too. Pins are in-memory: after a restart
// under a still-warm persisted mirror mapping, flights that fail before
// any success re-teaches the branch degrade to the quarantine sweep.
// Expired entries are pruned on insert; a full map drops the note, which
// only delays purges to the sweep.
func (s *peerSelector) noteBranchCommit(repo, rev, commit string) {
	now := time.Now()
	s.hashMu.Lock()
	defer s.hashMu.Unlock()
	for k, e := range s.branchCommits {
		if !e.live(now) {
			delete(s.branchCommits, k)
		}
	}
	key := repo + "\x00" + rev
	if _, ok := s.branchCommits[key]; !ok && len(s.branchCommits) >= maxResolveHashes {
		return
	}
	var expires time.Time
	if s.branchPinTTL > 0 {
		expires = now.Add(s.branchPinTTL)
	}
	s.branchCommits[key] = resolveHash{hash: commit, expires: expires}
}

// fileHashFor returns the live remembered file hash for one resolve path.
// A branch-form path with no exact note is retried under its remembered
// commit pin: a warm mirror resolves only the commit path, so that is
// where the selector saw the hash.
func (s *peerSelector) fileHashFor(path string) string {
	now := time.Now()
	s.hashMu.Lock()
	defer s.hashMu.Unlock()
	if e := s.resolveHashes[path]; e.live(now) {
		return e.hash
	}
	m := resolvePathRe.FindStringSubmatch(path)
	if m == nil || commitRe.MatchString(m[2]) {
		return ""
	}
	pin := s.branchCommits[m[1]+"\x00"+m[2]]
	if !pin.live(now) {
		return ""
	}
	if e := s.resolveHashes["/"+m[1]+"/resolve/"+pin.hash+"/"+m[3]]; e.live(now) {
		return e.hash
	}
	return ""
}

// originValidator returns the origin's content validator for one resolve
// path: the trimmed X-Linked-Etag/ETag in 64-hex sha256 form — exactly the
// value the mirror's ingest verifies the spooled bytes against. Missing
// notes are fetched with a metadata-only HEAD and memoized: for the note
// TTL on immutable commit paths, for the peer staleness budget on mutable
// ones, and briefly even when the origin yields none. Empty means no
// usable validator — an unreachable or refusing origin, or a non-sha etag
// the ingest could not verify against — and no peer may serve the flight.
func (s *peerSelector) originValidator(ctx context.Context, auth, path string) string {
	now := time.Now()
	s.hashMu.Lock()
	if e, ok := s.originETags[path]; ok && e.expires.After(now) {
		s.hashMu.Unlock()
		return e.hash
	}
	s.hashMu.Unlock()
	etag := s.fetchOriginMeta(ctx, auth, path)
	ttl := originETagNegTTL
	if etag != "" {
		ttl = peerReadyTTL
		if m := resolvePathRe.FindStringSubmatch(path); m != nil && commitRe.MatchString(m[2]) {
			ttl = resolveHashTTL
		}
	}
	s.hashMu.Lock()
	defer s.hashMu.Unlock()
	for p, e := range s.originETags {
		if !e.expires.After(now) {
			delete(s.originETags, p)
		}
	}
	if _, ok := s.originETags[path]; !ok && len(s.originETags) >= maxResolveHashes {
		return etag
	}
	s.originETags[path] = resolveHash{hash: etag, expires: now.Add(ttl)}
	return etag
}

// fetchOriginMeta issues the anchoring HEAD against the origin, following
// redirects like the mirror's own probe — earlier hops win, and the chase
// stops once a validator appears, so the data route is never touched —
// attaching the mirror's credential only toward the origin host, the same
// rule the pinned authInjector applies. The response doubles as an origin
// resolve sighting: the file hash and branch pin are noted as if the
// mirror's own traffic had been proxied. It returns the 64-hex validator,
// or "" when the origin errors, refuses, or advertises none.
func (s *peerSelector) fetchOriginMeta(ctx context.Context, auth, path string) string {
	cur := s.origin + path
	var etag, fileHash, commit string
	success := false
	for range 8 {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, cur, nil)
		if err != nil {
			return ""
		}
		req.Header.Set("Accept-Encoding", "identity")
		if auth != "" && req.URL.Host == s.originHost {
			req.Header.Set("Authorization", auth)
		}
		resp, err := s.originClient.Do(req)
		if err != nil {
			return ""
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 400 {
			return ""
		}
		if etag == "" {
			etag = headerValidator(resp.Header)
		}
		if fileHash == "" {
			fileHash = resp.Header.Get("X-Xet-Hash")
		}
		if commit == "" {
			commit = resp.Header.Get("X-Repo-Commit")
		}
		if resp.StatusCode < 300 || etag != "" {
			// A definitive answer, or a redirect hop already naming the
			// validator: earlier hops win, so following further could
			// never change it and would only touch the data route.
			success = true
			break
		}
		loc := resp.Header.Get("Location")
		if loc == "" {
			return ""
		}
		u, err := req.URL.Parse(loc)
		if err != nil {
			return ""
		}
		cur = u.String()
	}
	if !success {
		return ""
	}
	if m := resolvePathRe.FindStringSubmatch(path); m != nil {
		if captureXorbHashRe.MatchString(fileHash) {
			s.noteResolve(path, fileHash)
		}
		if commitRe.MatchString(commit) && !commitRe.MatchString(m[2]) {
			s.noteBranchCommit(m[1], m[2], commit)
		}
	}
	if !captureXorbHashRe.MatchString(etag) {
		return ""
	}
	return etag
}

// notePeerServed remembers which peer served the last proxied resolve for
// one path, so a failed checksum verdict can cool it, reporting whether the
// note was stored. Expired entries are pruned on insert; a full map refuses
// new paths — the caller must then not use the peer, since a flight whose
// supplier the verdict could never reach must not exist.
func (s *peerSelector) notePeerServed(path, peer string) bool {
	now := time.Now()
	s.hashMu.Lock()
	defer s.hashMu.Unlock()
	for p, e := range s.resolvePeers {
		if !e.live(now) {
			delete(s.resolvePeers, p)
		}
	}
	if _, ok := s.resolvePeers[path]; !ok && len(s.resolvePeers) >= maxResolveHashes {
		return false
	}
	s.resolvePeers[path] = resolveHash{hash: peer, expires: now.Add(resolveHashTTL)}
	return true
}

// takeResolvePeers returns and forgets the peers remembered for a settled
// flight's resolve paths — each key plus its commit-pinned form, the same
// translation fileHashFor applies — so one verdict cools each supplier at
// most once. Like the branch pins themselves, the memory is in-process:
// after a restart under a persisted warm branch mapping, failures land
// under the branch path while notes sit under the commit path, unreachable
// until the mapping's next refresh re-teaches the pin — until then retries
// keep failing honestly, never serving unanchored bytes.
func (s *peerSelector) takeResolvePeers(keys []string) []string {
	now := time.Now()
	s.hashMu.Lock()
	defer s.hashMu.Unlock()
	var peers []string
	take := func(path string) {
		e, ok := s.resolvePeers[path]
		if !ok {
			return
		}
		delete(s.resolvePeers, path)
		if !e.live(now) {
			return
		}
		for _, p := range peers {
			if p == e.hash {
				return
			}
		}
		peers = append(peers, e.hash)
	}
	for _, key := range keys {
		take(key)
		m := resolvePathRe.FindStringSubmatch(key)
		if m == nil || commitRe.MatchString(m[2]) {
			continue
		}
		if pin := s.branchCommits[m[1]+"\x00"+m[2]]; pin.live(now) {
			take("/" + m[1] + "/resolve/" + pin.hash + "/" + m[3])
		}
	}
	return peers
}

// proxy forwards the request to base and relays the response verbatim. For
// peers the origin credential is stripped and the local-only marker added;
// peer transport errors, error statuses and validator mismatches report
// false so the caller can fall back to the origin (resolve requests carry no
// body, so a retry against the other backend is always safe). Origin
// responses get hub-host URLs in Link and Location headers rewritten to this
// selector, so follow-up hops that need the origin credential keep flowing
// through the injecting client.
func (s *peerSelector) proxy(w http.ResponseWriter, r *http.Request, base, target string, isPeer bool, want string) bool {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, base+target, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	req.Header = r.Header.Clone()
	req.Header.Del("Connection")
	client := s.originClient
	if isPeer {
		req.Header.Del("Authorization")
		req.Header.Set(localOnlyHeader, "1")
		client = s.peerClient
	}
	resp, err := client.Do(req)
	if err != nil {
		if isPeer {
			return false
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if isPeer {
		if resp.StatusCode >= 400 {
			return false
		}
		// The peer's answer feeds the mirror's probe and ingest metadata
		// verbatim, so the validator must match the origin's on this very
		// response — a probe-time match could be swapped afterwards. And a
		// peer the verdict could never reach must not serve at all.
		if want == "" || headerValidator(resp.Header) != want ||
			!s.notePeerServed(r.URL.EscapedPath(), base) {
			return false
		}
	}
	// Only origin answers are remembered: flights resolved from a peer
	// fetch through it too, and the capture transport skips marked
	// responses, so they can never quarantine anything.
	if !isPeer && resp.StatusCode < 400 {
		if m := resolvePathRe.FindStringSubmatch(r.URL.EscapedPath()); m != nil {
			if fh := resp.Header.Get("X-Xet-Hash"); captureXorbHashRe.MatchString(fh) {
				s.noteResolve(r.URL.EscapedPath(), fh)
			}
			if commit := resp.Header.Get("X-Repo-Commit"); commitRe.MatchString(commit) &&
				!commitRe.MatchString(m[2]) {
				s.noteBranchCommit(m[1], m[2], commit)
			}
		}
	}
	h := w.Header()
	for k, vs := range resp.Header {
		for _, v := range vs {
			if !isPeer && (k == "Link" || k == "Location") {
				v = strings.ReplaceAll(v, s.origin+"/", "http://"+r.Host+"/")
			}
			h.Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	body := io.Reader(resp.Body)
	if isPeer {
		// Ready resolve answers are metadata-sized; the cap keeps a
		// misbehaving peer from streaming junk into an ingest.
		body = io.LimitReader(body, 1<<20)
	}
	_, _ = io.Copy(w, body)
	return true
}
