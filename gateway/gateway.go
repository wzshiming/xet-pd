package gateway

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/hf"

	"github.com/wzshiming/xet-pd/apis"
)

// Config configures New; zero values take the noted defaults.
type Config struct {
	StorageDir    string // required: data/ holds the store, mirror/ the index, spools and chunk cache
	Upstream      string // required hub origin, scheme://host[:port]
	UpstreamToken string
	SigningKey    string        // HMAC key of CAS read tokens; empty leaves CAS routes open
	ExternalURL   string        // origin advertised in hub links; empty derives it from each request
	Secret        string        // bearer token of announces and pull requests
	PeerTTL       time.Duration // announce validity, default 30s, at least 1s
	RelayTimeout  time.Duration // dial, TLS handshake and response headers of one relay attempt, default 5s
	Replicas      int           // seeds asked to hold each xorb, default 2
	MinSeeds      int           // live holders before the local copy may go, default 1
	MaxPulls      int           // parallel pull requests to seeds, default 8
	XorbCacheSize int64         // bytes of local xorbs kept, 0 unlimited
}

// Gateway is the xet-pd central service: a hub mirror whose xorb bytes live on registered peers.
type Gateway struct {
	mux       *http.ServeMux
	cas       http.Handler
	reg       *registry
	st        *store
	staging   *staging
	secret    string
	dataDir   string
	transport *http.Transport // upstream xet downloads

	peerGets, directGets                    atomic.Uint64
	servedPeer, servedDirect                atomic.Uint64
	announces, announceFull, announceResync atomic.Uint64
	once                                    sync.Once
}

// New composes the store, hub mirror, CAS server and staging behind one read-only handler.
func New(cfg Config) (*Gateway, error) {
	var err error
	if cfg.StorageDir == "" {
		return nil, errors.New("gateway: storage dir is required")
	}
	if cfg.Upstream == "" {
		return nil, errors.New("gateway: upstream is required")
	}
	if cfg.Upstream, err = apis.Origin(cfg.Upstream); err != nil {
		return nil, fmt.Errorf("gateway: upstream: %w", err)
	}
	if cfg.ExternalURL != "" {
		if cfg.ExternalURL, err = apis.Origin(cfg.ExternalURL); err != nil {
			return nil, fmt.Errorf("gateway: external url: %w", err)
		}
	}
	if cfg.PeerTTL < 0 || cfg.RelayTimeout < 0 || cfg.Replicas < 0 || cfg.MinSeeds < 0 || cfg.MaxPulls < 0 || cfg.XorbCacheSize < 0 {
		return nil, errors.New("gateway: negative config value")
	}
	cfg.PeerTTL = cmp.Or(cfg.PeerTTL, 30*time.Second)
	if cfg.PeerTTL < time.Second {
		return nil, errors.New("gateway: peer ttl below 1s")
	}
	cfg.RelayTimeout = cmp.Or(cfg.RelayTimeout, 5*time.Second)
	cfg.Replicas = cmp.Or(cfg.Replicas, 2)
	cfg.MinSeeds = cmp.Or(cfg.MinSeeds, 1)
	cfg.MaxPulls = cmp.Or(cfg.MaxPulls, 8)
	if cfg.MinSeeds > cfg.Replicas {
		return nil, errors.New("gateway: min seeds exceed replicas")
	}
	reg := newRegistry(cfg.PeerTTL)
	dataDir := filepath.Join(cfg.StorageDir, "data")
	st, err := newStore(dataDir, reg, cfg.RelayTimeout)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	issuer, err := auth.NewIssuer([]byte(cfg.SigningKey), 0, nil)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	mirrorDir := filepath.Join(cfg.StorageDir, "mirror")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	provider := client.StaticUpstreamProvider(cfg.Upstream, cfg.UpstreamToken)
	mir, err := mirror.NewMirror(
		mirror.WithStorage(st),
		mirror.WithCacheDir(mirrorDir),
		mirror.WithTransport(transport),
	)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	hub := hf.NewHandler(
		hf.WithMirror(mir),
		hf.WithUpstreamProvider(provider),
		hf.WithExternalURL(cfg.ExternalURL),
		hf.WithMinter(hf.MinterFunc(func(_ *http.Request, req hf.TokenRequest) (string, int64, error) {
			if req.Permission != auth.Read {
				return "", 0, auth.ErrForbidden // answered here, never delegated to the upstream
			}
			return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
		})),
		hf.WithNext(hf.NewUpstreamProxy(provider)),
	)
	var authorizer auth.Authorizer
	if cfg.SigningKey != "" {
		authorizer = issuer
	}
	cas := server.NewHandler(server.WithStorage(st), server.WithAuthorizer(authorizer), server.WithNext(hub))
	staging, err := newStaging(st, stagingConfig{replicas: cfg.Replicas, minSeeds: cfg.MinSeeds, maxPulls: cfg.MaxPulls, cacheSize: cfg.XorbCacheSize, secret: cfg.Secret})
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	g := &Gateway{mux: http.NewServeMux(), cas: cas, reg: reg, st: st, staging: staging, secret: cfg.Secret, dataDir: dataDir, transport: transport}
	g.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g.mux.HandleFunc("GET /readyz", g.serveReady)
	g.mux.HandleFunc(apis.AnnouncePath, route(http.MethodPost, g.serveAnnounce))
	g.mux.HandleFunc(apis.PeersPath, route(http.MethodGet, g.servePeers))
	g.mux.Handle("/xet-pd/", http.NotFoundHandler()) // unknown control paths never reach the upstream
	g.mux.HandleFunc("/", g.servePublic)
	return g, nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mux.ServeHTTP(w, r)
}

// Close stops seeding and drops idle connections; ingests the mirror runs in the background finish on their own.
func (g *Gateway) Close() error {
	g.once.Do(func() {
		_ = g.staging.Close()
		g.transport.CloseIdleConnections()
		g.st.transport.CloseIdleConnections()
	})
	return nil
}

// route answers a control route for one method and 405 otherwise.
func route(method string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// serveReady creates and removes a probe file in the data directory's root, which the xorb walks never enter.
func (g *Gateway) serveReady(w http.ResponseWriter, _ *http.Request) {
	f, err := os.CreateTemp(g.dataDir, ".readyz-*")
	if err == nil {
		err = errors.Join(f.Close(), os.Remove(f.Name()))
	}
	if err != nil {
		slog.Warn("gateway: readyz probe", "err", err)
		http.Error(w, "storage: probe failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// pathsInfoRe admits the one hub POST that only reads: /api/{type}s/{repo}/paths-info/{rev}.
var pathsInfoRe = regexp.MustCompile(`^/api/(models|datasets|spaces)/.+/paths-info/[^/]+$`)

// telemetryPath takes xet-core >= 1.7.0's per-transfer summary; the CAS handler validates and discards it.
const telemetryPath = "/v1/telemetry"

// servePublic keeps the CAS and hub surface read-only (GET/HEAD plus two POSTs that write nothing: paths-info reads the hub, telemetry is discarded) and tallies xorb GETs and the body bytes they write; peer pulls carry apis.PeerHeader.
func (g *Gateway) servePublic(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
	case r.Method == http.MethodPost && pathsInfoRe.MatchString(r.URL.EscapedPath()):
	case r.Method == http.MethodPost && r.URL.Path == telemetryPath:
	default:
		http.Error(w, "read-only gateway", http.StatusForbidden)
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
		if r.Header.Get(apis.PeerHeader) != "" {
			g.peerGets.Add(1)
			w = &countingWriter{ResponseWriter: w, n: &g.servedPeer}
		} else {
			g.directGets.Add(1)
			w = &countingWriter{ResponseWriter: w, n: &g.servedDirect}
		}
	}
	g.cas.ServeHTTP(w, r)
}

// countingWriter adds the body bytes written through it to n.
type countingWriter struct {
	http.ResponseWriter
	n *atomic.Uint64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n.Add(uint64(n))
	return n, err
}

// ReadFrom keeps the inner writer's ReadFrom, which sends local files with sendfile.
func (c *countingWriter) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(r)
		c.n.Add(uint64(n))
		return n, err
	}
	return io.Copy(writerOnly{c}, r)
}

func (c *countingWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the inner writer.
func (c *countingWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

// writerOnly hides ReadFrom so io.Copy does not call back into it.
type writerOnly struct{ io.Writer }

func (g *Gateway) servePeers(w http.ResponseWriter, _ *http.Request) {
	m, st := g.staging, g.st
	d := apis.PeersResponse{
		Peers:           g.reg.snapshot(),
		XorbBytes:       m.snapshot(),
		XorbGets:        apis.XorbGets{Peer: g.peerGets.Load(), Direct: g.directGets.Load()},
		XorbBytesServed: apis.ServedBytes{Peer: g.servedPeer.Load(), Direct: g.servedDirect.Load()},
		Pulls:           apis.PullCounters{Sent: m.pullsSent.Load(), Accepted: m.pullsAccepted.Load(), Failed: m.pullsFailed.Load()},
		Evictions:       apis.EvictionCounters{Count: m.evictions.Load(), Bytes: m.evictedBytes.Load()},
		Relays:          apis.RelayCounters{Attempts: st.relayTries.Load(), Failures: st.relayErrs.Load()},
		Announces:       apis.AnnounceCounters{Total: g.announces.Load(), Full: g.announceFull.Load(), Resync: g.announceResync.Load()},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d)
}
