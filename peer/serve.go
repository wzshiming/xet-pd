package peer

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

func (p *Peer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	// Shadows the CAS xorb route: relays stay local and requesters are counted.
	mux.HandleFunc("GET /v1/xorbs/{prefix}/{hash}", p.serveXorb)
	mux.HandleFunc("POST "+apis.PullPath, p.servePull)
	mux.HandleFunc("GET "+apis.ShardPathPrefix+"{hash}", p.serveShard)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", p.serveReadyz)
	mux.Handle("GET /metrics", p.metrics.handler())
	mux.Handle("/xet-pd/", http.NotFoundHandler()) // unknown control paths never reach the upstream
	mux.HandleFunc("/", p.servePublic)
	return mux
}

func (p *Peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

// pathsInfoRe admits the one hub POST that only reads: /api/{type}s/{repo}/paths-info/{rev}.
var pathsInfoRe = regexp.MustCompile(`^/api/(models|datasets|spaces)/.+/paths-info/[^/]+$`)

// telemetryPath takes xet-core >= 1.7.0's per-transfer summary; the CAS handler validates and discards it.
const telemetryPath = "/v1/telemetry"

// servePublic keeps the CAS and hub surface read-only: GET and HEAD plus two POSTs that write nothing, paths-info reading the hub and telemetry being discarded.
func (p *Peer) servePublic(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
	case r.Method == http.MethodPost && pathsInfoRe.MatchString(r.URL.EscapedPath()):
	case r.Method == http.MethodPost && r.URL.Path == telemetryPath:
	default:
		http.Error(w, "read-only peer", http.StatusForbidden)
		return
	}
	p.cas.ServeHTTP(w, r)
}

func (p *Peer) serveReadyz(w http.ResponseWriter, _ *http.Request) {
	if !p.ann.ready.Load() {
		http.Error(w, "not announced yet", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// statusWriter records the first status code written through it.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

// ReadFrom hands the copy to the wrapped *http.response: that makes net/http flush the response header after the first
// 512 bytes instead of buffering 2 KiB, which a client of a pull still in flight waits for.
func (w *statusWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(w.ResponseWriter, r)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func serveContent(w http.ResponseWriter, r *http.Request, hash xet.XorbHash, content io.ReadSeeker) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+hash.String()+`"`)
	http.ServeContent(w, r, "", time.Time{}, content)
}

// serveXorb serves a xorb with ranges: a relay (apis.PeerHeader) and a HEAD only from the local copy, any other GET streaming the pull of a missing one.
func (p *Peer) serveXorb(w http.ResponseWriter, r *http.Request) {
	relay := r.Header.Get(apis.PeerHeader) != ""
	requester := "client"
	if relay {
		requester = "peer"
	}
	sw := &statusWriter{ResponseWriter: w}
	// The GET pattern routes HEAD here too; only GETs count.
	if r.Method == http.MethodGet {
		// A handler that never sets a code answers net/http's implicit 200.
		defer func() { p.metrics.get(requester, cmp.Or(sw.code, http.StatusOK)) }()
	}
	w = sw
	hash, err := xet.ParseXorbHash(r.PathValue("hash"))
	if err != nil {
		http.Error(w, "invalid xorb path", http.StatusBadRequest)
		return
	}
	open := p.st.GetXorbReadSeekCloser
	if relay || r.Method == http.MethodHead {
		open = p.st.openLocal
	}
	rs, err := open(r.Context(), hash)
	if errors.Is(err, fs.ErrNotExist) && r.Method == http.MethodHead {
		var held bool
		switch held, err = p.st.HasXorb(r.Context(), hash); {
		case err == nil && held:
			w.WriteHeader(http.StatusOK) // held by a live peer: no pull, so no length
			return
		case err == nil:
			http.NotFound(w, r)
			return
		}
	}
	if err != nil {
		http.Error(w, "xorb unavailable", statusOf(err))
		return
	}
	defer func() { _ = rs.Close() }()
	serveContent(w, r, hash, rs)
}

// servePull checks a batch of xorbs and shards, marks the xorbs seed copies when asked, answers 202 and pulls the batch in the background.
func (p *Peer) servePull(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, p.cfg.Secret) {
		return
	}
	var req apis.PullRequest
	if !apis.ReadJSON(w, r, apis.MaxPullBytes, &req) {
		return
	}
	switch n := len(req.Xorbs) + len(req.Shards); {
	case n == 0:
		http.Error(w, "empty pull", http.StatusBadRequest)
		return
	case n > apis.MaxPullBatch:
		http.Error(w, "too many entries", http.StatusBadRequest)
		return
	}
	xorbs, xerr := parseAll(req.Xorbs, xet.ParseXorbHash)
	shards, serr := parseAll(req.Shards, xet.ParseFileHash)
	if xerr != nil || serr != nil {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	closed := p.ctx.Err() != nil
	if !closed {
		p.wg.Add(1)
	}
	p.mu.Unlock()
	if closed {
		http.Error(w, errClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	if req.Seed {
		p.markSeed(xorbs...)
	}
	w.WriteHeader(http.StatusAccepted)
	go func() {
		defer p.wg.Done()
		p.pullBatch(xorbs, shards)
	}()
}

// parseAll parses every entry of list, failing on the first one parse rejects.
func parseAll[T any](list []string, parse func(string) (T, error)) ([]T, error) {
	out := make([]T, len(list))
	for i, s := range list {
		var err error
		if out[i], err = parse(s); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// pullBatch pulls the xorbs whole and fetches the shards within PullTimeout, each even when another failed; a failure is logged unless the peer is closing.
func (p *Peer) pullBatch(xorbs []xet.XorbHash, shards []xet.FileHash) {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.PullTimeout)
	defer cancel()
	warn := func(kind string, hash any, err error) {
		if err != nil && p.ctx.Err() == nil {
			slog.Warn("peer: pull", kind, hash, "err", err)
		}
	}
	pulls := make([]*pull, len(xorbs))
	for i, h := range xorbs {
		var err error
		pulls[i], err = p.st.acquire(h, true)
		warn("xorb", h, err)
	}
	for _, h := range shards {
		_, err := p.st.ensureShard(ctx, h)
		warn("shard", h, err)
	}
	for i, f := range pulls {
		if f != nil {
			warn("xorb", xorbs[i], f.wait(ctx))
		}
	}
}

// serveShard answers a peer fetching the shard of a file this peer knows, with the offsets of every xorb it references.
func (p *Peer) serveShard(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w}
	defer func() { p.metrics.shardGet(cmp.Or(sw.code, http.StatusOK)) }()
	w = sw
	if !apis.Authorize(w, r, p.cfg.Secret) {
		return
	}
	hash, err := xet.ParseFileHash(r.PathValue("hash"))
	if err != nil {
		http.Error(w, "invalid file hash", http.StatusBadRequest)
		return
	}
	resp, err := p.st.shardResponse(r.Context(), hash)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		http.NotFound(w, r)
		return
	case err != nil:
		http.Error(w, "shard unavailable", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func statusOf(err error) int {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return http.StatusNotFound
	case errors.Is(err, errClosed):
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}
