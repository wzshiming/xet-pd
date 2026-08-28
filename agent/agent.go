// Package agent composes the xet mirror facade and CAS server into a single
// HF_ENDPOINT-compatible transparent cache handler, mirroring the wiring of
// xet's cmd/xetd in mirror mode.
package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

// agentMarkerHeader stamps every response served by the agent, so peers can
// tell each other's responses from origin ones — the capture transport only
// harvests origin-sourced bytes.
const agentMarkerHeader = "X-Xet-PD-Agent"

// Config holds the agent composition parameters.
type Config struct {
	// Upstream is the hub base URL to mirror, e.g. https://huggingface.co. Required.
	Upstream string
	// UpstreamToken is the bearer credential used against the upstream hub.
	// It is never forwarded to downstream clients or peers.
	UpstreamToken string
	// StorageDir is the root data directory: xorbs and shards live under
	// storage/, the mirror index, spool and chunk cache under mirror/, the
	// peer ready index under peer/. Required.
	StorageDir string
	// ExternalURL is the externally visible base URL used in Link headers,
	// minted tokens and xorb fetch URLs. Empty derives it from each request.
	ExternalURL string
	// ClusterSecret seeds the tracker announce credential (S2): agents and
	// tracker sharing it accept each other's announce tokens. The anonymous
	// protocol handshake tokens are minted from a separate per-process
	// issuer, so nothing anonymously mintable is ever accepted for
	// announces. Empty sends announces bare (open tracker, trusted network).
	ClusterSecret string
	// RevalidateInterval overrides how often ready branch entries are
	// re-checked against the upstream: zero re-checks every request,
	// negative never does. Nil keeps the mirror default.
	RevalidateInterval *time.Duration
	// Peers lists sibling agent base URLs probed before the origin on cold
	// resolves (S1). Empty keeps the single-node S0 behavior.
	Peers []string
	// Tracker is the tracker base URL (S2): resolve lookups ask it which
	// peers hold the file instead of broadcasting probes, and ready files
	// are announced to it — announces need ExternalURL as the peer identity.
	// Empty, unreachable or restarting trackers degrade back to the static
	// peer list and the origin.
	Tracker string
	// PeerTimeout bounds each peer readiness probe and header wait.
	// Non-positive means 2s.
	PeerTimeout time.Duration
	// MaxPeerConns caps connections per peer host. Non-positive means 64.
	MaxPeerConns int
}

// Agent is the composed read-only cache handler plus the background
// resources it owns.
type Agent struct {
	http.Handler
	selector  *http.Server
	announcer *announcer
	segments  *segstore.Store
	gate      *peerGate
	capture   *captureTransport
	// sel is the loopback selector handler; its proxied resolve responses
	// name the file hash of each ingest key, recovering the identity of
	// failed flights for ingestVerdict.
	sel *peerSelector
	// quar tracks peer-sourced quarantined segments per file until the
	// ingest checksum verdict settles them (S3c-2).
	quar *quarantine
	// sweepCancel and sweepStopped bound the quarantine sweeper goroutine,
	// which runs regardless of tracker or announcer.
	sweepCancel  context.CancelFunc
	sweepStopped chan struct{}

	storageDir string
}

// Close stops the quarantine sweeper, drains the background recorders and
// in-flight segment captures, then shuts down the tracker announcer and the
// loopback upstream selector, if any.
func (a *Agent) Close() error {
	if a.sweepCancel != nil {
		a.sweepCancel()
		<-a.sweepStopped
	}
	if a.gate != nil {
		a.gate.close()
	}
	if a.capture != nil {
		a.capture.close()
	}
	if a.announcer != nil {
		a.announcer.stop()
	}
	if a.selector != nil {
		return a.selector.Close()
	}
	return nil
}

// New composes storage, token issuer, mirror and CAS server into the agent
// handler: CAS routes match first and fall through to the mirror, which
// proxies whatever it does not handle to the upstream. The surface is an
// anonymous read-only cache: no route requires credentials, and readOnly
// rejects every write before it can reach the CAS server or the upstream.
// With peers configured, the mirror's upstream becomes a loopback selector
// that prefers ready peers over the origin.
func New(cfg Config) (*Agent, error) {
	if cfg.Upstream == "" {
		return nil, fmt.Errorf("agent: upstream is required")
	}
	if cfg.StorageDir == "" {
		return nil, fmt.Errorf("agent: storage dir is required")
	}
	peerTimeout := cfg.PeerTimeout
	if peerTimeout <= 0 {
		peerTimeout = 2 * time.Second
	}
	maxPeerConns := cfg.MaxPeerConns
	if maxPeerConns <= 0 {
		maxPeerConns = 64
	}
	// Peers are vetted like tracker candidates: net/http would turn URL
	// credentials into an Authorization header toward a peer.
	peers := make([]string, len(cfg.Peers))
	for i, p := range cfg.Peers {
		vetted := peerBaseURL(p)
		if vetted == "" {
			return nil, fmt.Errorf("agent: invalid peer URL %q", p)
		}
		peers[i] = vetted
	}

	a := &Agent{storageDir: cfg.StorageDir}

	stor, err := storage.NewFileStorage(
		storage.WithBasePath(filepath.Join(cfg.StorageDir, "storage")),
		storage.WithBaseURL(cfg.ExternalURL),
	)
	if err != nil {
		return nil, fmt.Errorf("agent: create storage: %w", err)
	}

	segs, err := segstore.New(segstore.WithDir(filepath.Join(cfg.StorageDir, "segments")))
	if err != nil {
		return nil, fmt.Errorf("agent: create segment store: %w", err)
	}
	a.segments = segs

	// Handshake tokens are protocol furniture minted for anyone, so they
	// come from a throwaway per-process issuer — never from the cluster
	// secret, which gates tracker announces.
	mintIssuer, err := token.NewIssuer(nil, 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("agent: create token issuer: %w", err)
	}

	var trackerClient *tracker.Client
	if cfg.Tracker != "" {
		var clientOpts []tracker.ClientOption
		if cfg.ClusterSecret != "" {
			clusterIssuer, err := token.NewIssuer([]byte(cfg.ClusterSecret), 15*time.Minute)
			if err != nil {
				return nil, fmt.Errorf("agent: create cluster token issuer: %w", err)
			}
			clientOpts = append(clientOpts, tracker.WithMintToken(clusterIssuer.Mint))
		}
		trackerClient, err = tracker.NewClient(cfg.Tracker, clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("agent: create tracker client: %w", err)
		}
	}

	idx := readyIndex{dir: filepath.Join(cfg.StorageDir, "peer", "ready")}
	// Announcing needs a reachable identity for this agent; without an
	// external URL the agent still consumes the tracker for lookups.
	if trackerClient != nil && cfg.ExternalURL != "" {
		a.announcer = newAnnouncer(trackerClient, strings.TrimRight(cfg.ExternalURL, "/"), idx, stor, segs)
	}

	// The capture transport rides the mirror's own upstream traffic to
	// harvest segments and serve recorded term fetches from local and peer
	// sources; the inner transport replicates what the client would have
	// built for itself.
	innerTransport := http.DefaultTransport.(*http.Transport).Clone()
	innerTransport.DisableKeepAlives = true
	var notifySegment func(string, tracker.ChunkSpan)
	if a.announcer != nil {
		notifySegment = a.announcer.notifySegment
	}
	a.quar = newQuarantine()
	capture := newCaptureTransport(innerTransport, segs, notifySegment,
		newSourceChain(stor, trackerClient, cfg.ExternalURL, peerTimeout, maxPeerConns, a.quar))
	capture.purgeChunks = a.purgeChunkCache
	a.capture = capture

	mirrorDir := filepath.Join(cfg.StorageDir, "mirror")
	xetClient, err := client.NewClient(
		client.WithCacheDir(filepath.Join(mirrorDir, "chunks")),
		client.WithHTTPClient(&http.Client{Transport: capture}),
	)
	if err != nil {
		return nil, fmt.Errorf("agent: create xet client: %w", err)
	}

	proxy, err := mirror.NewUpstreamProxy(cfg.Upstream, cfg.UpstreamToken)
	if err != nil {
		return nil, fmt.Errorf("agent: create upstream proxy: %w", err)
	}

	// With peers or a tracker, the mirror talks to the loopback selector
	// instead of the origin; the selector forwards the mirror's own
	// Authorization to the origin and strips it toward peers.
	mirrorUpstream := cfg.Upstream
	if len(peers) > 0 || trackerClient != nil {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("agent: listen for peer selector: %w", err)
		}
		sel := newPeerSelector(cfg.Upstream, peers, trackerClient, cfg.ExternalURL, peerTimeout, maxPeerConns)
		// File-level picks skip peers the term chain or a failed flight
		// already cooled, and failed flights cool through the same state.
		sel.cooled = capture.cooledDown
		// Branch pins must outlive the mirror's branch mapping, which is
		// only re-probed — re-teaching the selector — on this cadence; a
		// mirror that never revalidates never re-probes at all.
		if cfg.RevalidateInterval != nil {
			if d := *cfg.RevalidateInterval; d < 0 {
				sel.branchPinTTL = 0
			} else if 2*d > sel.branchPinTTL {
				sel.branchPinTTL = 2 * d
			}
		}
		a.sel = sel
		a.selector = &http.Server{Handler: sel}
		go func() { _ = a.selector.Serve(ln) }()
		mirrorUpstream = "http://" + ln.Addr().String()
	}

	mirrorOpts := []mirror.Option{
		mirror.WithStorage(stor),
		mirror.WithUpstream(mirrorUpstream),
		mirror.WithUpstreamToken(cfg.UpstreamToken),
		mirror.WithExternalURL(cfg.ExternalURL),
		mirror.WithCacheDir(mirrorDir),
		mirror.WithClient(xetClient),
		mirror.WithMintToken(mintIssuer.Mint),
		mirror.WithNext(proxy),
	}
	if cfg.RevalidateInterval != nil {
		mirrorOpts = append(mirrorOpts, mirror.WithRevalidateInterval(*cfg.RevalidateInterval))
	}
	m, err := mirror.NewHandler(mirrorOpts...)
	if err != nil {
		if a.selector != nil {
			_ = a.selector.Close()
		}
		return nil, fmt.Errorf("agent: create mirror: %w", err)
	}

	// No WithAuthFunc: the pinned server then allows all requests, which is
	// exactly the anonymous-read design. Writes never reach it — readOnly
	// rejects them first.
	cas := server.NewHandler(
		server.WithStorage(stor),
		server.WithNext(m),
	)
	// Segments only answer xorb downloads the CAS server would miss: full
	// local xorbs pass through it unchanged.
	segGate := newSegmentGate(cas, stor, segs)
	gate := newPeerGate(segGate, m, idx, cfg.ExternalURL)
	a.gate = gate
	gate.verdict = a.ingestVerdict
	if a.sel != nil {
		gate.pin = a.sel.noteBranchCommit
	}
	if a.announcer != nil {
		gate.notify = a.announcer.notify
		go a.announcer.run()
	}
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	a.sweepCancel = sweepCancel
	a.sweepStopped = make(chan struct{})
	go a.runSweeper(sweepCtx)
	a.Handler = markAgent(readOnly(gate))
	return a, nil
}

// runSweeper purges expired quarantined segments on a fixed cadence,
// starting with a recovery pass over segments an earlier process left
// behind. It runs unconditionally — quarantine hygiene must not depend on
// a tracker or announcer being configured — and Close stops it before
// draining the capture transport, so no sweep runs after Close returns.
func (a *Agent) runSweeper(ctx context.Context) {
	defer close(a.sweepStopped)
	a.capture.sweepQuarantine()
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.capture.sweepQuarantine()
		case <-ctx.Done():
			return
		}
	}
}

// ingestVerdict settles the quarantined segments of one ingest flight: a
// passed checksum releases them for serving and announcing, a failure
// purges them and cools the peers that supplied them. Quarantine is keyed
// by the origin reconstruction's file hash, while a settled flight reports
// the locally recomputed one — identical only when local chunking matches
// the origin's — so the origin hash the selector remembers for any of the
// flight's path forms is settled too: keys carry the downstream path plus
// the canonical commit alias when known, and the selector translates warm
// branch paths to the commit path itself. When no hash is recoverable, the
// sweep purges the items later. Only registry items settle: a registry
// record is the proof the segment's bytes were served into the flight the
// checksum covered, so recordless quarantined segments are never released
// — the sweep purges them from their persisted metas, which also backstop
// any Release or Delete failure here. It runs inside gate recorders, which
// Close drains before any other resource, so everything touched here is
// still alive.
func (a *Agent) ingestVerdict(keys []string, fileHash string, ok bool) {
	hashes := make([]string, 0, len(keys)+1)
	add := func(h string) {
		if h == "" {
			return
		}
		for _, seen := range hashes {
			if seen == h {
				return
			}
		}
		hashes = append(hashes, h)
	}
	add(fileHash)
	if a.sel != nil {
		for _, key := range keys {
			add(a.sel.fileHashFor(key))
		}
		// A peer that served the whole flight at the file level (S1) leaves
		// no per-term records: on failure it is cooled directly. The
		// decoded-chunk cache is dropped on every settlement, success too —
		// the flight decoded whatever ranges the peer's reconstruction
		// chose, and the checksum vouches only for the file's own bytes,
		// never for neighboring chunks the cache kept under the same xorbs;
		// nothing narrower than the whole cache is attributable here.
		if peers := a.sel.takeResolvePeers(keys); len(peers) > 0 {
			if !ok && a.capture != nil {
				for _, p := range peers {
					a.capture.coolDown(p)
				}
			}
			a.purgeAllChunkCache()
		}
	}
	// Chunks a wider fetch entry decoded beyond the covered union sit in the
	// client's chunk cache unvalidated by this checksum — a later flight
	// could replay them — so those xorbs lose their cache even on success.
	partialXorbs := map[string]struct{}{}
	for _, h := range hashes {
		for _, it := range a.quar.take(h) {
			if !ok {
				a.capture.coolDown(it.peer)
				_ = a.segments.Delete(it.xorbHash, it.span.Start, it.span.End)
				a.purgeChunkCache(it.xorbHash)
				continue
			}
			if it.partial {
				partialXorbs[it.xorbHash] = struct{}{}
			}
			released, err := a.segments.Release(it.xorbHash, it.span.Start, it.span.End)
			if err == nil && released && a.announcer != nil {
				a.announcer.notifySegment(it.xorbHash, it.span)
			}
		}
	}
	for xorbHash := range partialXorbs {
		a.purgeChunkCache(xorbHash)
	}
}

// purgeChunkCache drops the xet client's decoded-chunk cache for one xorb.
// The client caches chunks it decoded without re-verifying them on later
// reads, so a flight fed content-mismatched peer bytes would otherwise
// replay them from this cache on every retry, never reaching the origin.
func (a *Agent) purgeChunkCache(xorbHash string) {
	if !captureXorbHashRe.MatchString(xorbHash) {
		return
	}
	_ = os.RemoveAll(filepath.Join(a.storageDir, "mirror", "chunks", xorbHash[:2], xorbHash[2:]))
}

// purgeAllChunkCache drops the client's decoded-chunk cache entirely, for
// failed flights whose poisoned xorbs cannot be named one by one.
func (a *Agent) purgeAllChunkCache() {
	dir := filepath.Join(a.storageDir, "mirror", "chunks")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(dir, e.Name()))
	}
}

// markAgent stamps every response with the agent marker before the handler
// runs, so the header survives whichever WriteHeader path answers.
func markAgent(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(agentMarkerHeader, "1")
		next.ServeHTTP(w, r)
	})
}

// readOnly makes the agent surface strictly read-only for its anonymous
// clients: every mutating request is rejected outright — CAS uploads (xorbs,
// shards) and their dedup queries, hub commits and uploads through the
// upstream proxy — as is the xet-write-token route, which would otherwise be
// proxied upstream with the agent's injected credentials. The hub API uses
// POST for one read-only query, paths-info, which stays allowed.
func readOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The escaped path is what the upstream routes on: a revision may
		// carry percent-encoded slashes (refs%2Fpr%2F1), which the decoded
		// path would turn into extra segments.
		path := r.URL.EscapedPath()
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			if strings.HasPrefix(path, "/api/") && strings.Contains(path, "/xet-write-token/") {
				http.Error(w, "Write tokens are not issued: read-only agent", http.StatusForbidden)
				return
			}
		case http.MethodPost:
			if isPathsInfoQuery(path) {
				break
			}
			fallthrough
		default:
			http.Error(w, "Write operations are rejected: read-only agent", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// pathsInfoRe matches POST /api/{type}s/{repo}/paths-info/{rev} with the
// route element anchored at its own position: the repository is one or two
// segments and the revision is the last one. A substring match would admit
// every write route of a repository literally named "paths-info", such as
// /api/models/org/paths-info/commit/main.
var pathsInfoRe = regexp.MustCompile(`^/api/[^/]+/[^/]+(?:/[^/]+)?/paths-info/[^/]+$`)

// isPathsInfoQuery reports whether the escaped path is the hub's one
// read-only POST query. Dot segments are rejected so the allowlist cannot be
// steered onto another upstream route.
func isPathsInfoQuery(path string) bool {
	return pathsInfoRe.MatchString(path) && !strings.Contains(path, "..")
}
