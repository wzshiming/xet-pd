// Package gateway implements the xet-pd central gateway.
package gateway

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	DataDir       string // required: data/ holds the store, mirror/ the index, spools and chunk cache
	Upstream      string // required hub origin, scheme://host[:port]
	UpstreamToken string
	SigningKey    string        // HMAC key of CAS read tokens; empty leaves CAS routes open
	ExternalURL   string        // origin advertised in hub links; empty derives it from each request
	Secret        string        // bearer token of announces and pull requests
	PeerTTL       time.Duration // announce validity, default 30s, at least 1s
	Replicas      int           // seeds asked to hold each xorb, default 2
	MinSeeds      int           // live holders before the local copy may go, default 1
	XorbCacheSize int64         // bytes of local xorbs kept, 0 unlimited
	Locality      NodeResolver  // resolves client addresses to nodes so each client is sent to the peer on its node; nil picks by holdings alone
}

// NodeResolver names the node a client runs on from the host of its remote address; Synced tells whether it has seen the whole cluster once.
type NodeResolver interface {
	NodeOf(ip string) (node string, ok bool)
	Synced() bool
}

// Values the zero Config fields take; cmd/gateway shares them as flag defaults.
const (
	DefaultPeerTTL  = 30 * time.Second
	DefaultReplicas = 2
	DefaultMinSeeds = 1
)

// normalize validates cfg, reduces its URLs to origins and fills in the defaults.
func (cfg *Config) normalize() error {
	var err error
	if cfg.DataDir == "" {
		return errors.New("data dir is required")
	}
	if cfg.Upstream == "" {
		return errors.New("upstream is required")
	}
	if cfg.Upstream, err = apis.Origin(cfg.Upstream); err != nil {
		return fmt.Errorf("upstream: %w", err)
	}
	if cfg.ExternalURL != "" {
		if cfg.ExternalURL, err = apis.Origin(cfg.ExternalURL); err != nil {
			return fmt.Errorf("external url: %w", err)
		}
	}
	if cfg.PeerTTL < 0 || cfg.Replicas < 0 || cfg.MinSeeds < 0 || cfg.XorbCacheSize < 0 {
		return errors.New("negative config value")
	}
	cfg.PeerTTL = cmp.Or(cfg.PeerTTL, DefaultPeerTTL)
	if cfg.PeerTTL < time.Second {
		return errors.New("peer ttl below 1s")
	}
	cfg.Replicas = cmp.Or(cfg.Replicas, DefaultReplicas)
	cfg.MinSeeds = cmp.Or(cfg.MinSeeds, DefaultMinSeeds)
	if cfg.MinSeeds > cfg.Replicas {
		return errors.New("min seeds exceed replicas")
	}
	return nil
}

// Gateway is the xet-pd central service: a hub mirror whose xorb bytes live on registered peers.
type Gateway struct {
	mux       *http.ServeMux
	cas       http.Handler
	reg       *registry
	st        *store
	staging   *staging
	secret    string
	locality  NodeResolver
	transport *http.Transport // upstream xet downloads

	peerGets, directGets atomic.Uint64
	once                 sync.Once
}

// New composes the store, hub mirror, CAS server and staging behind one read-only handler.
func New(cfg Config) (*Gateway, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	reg := newRegistry(cfg.PeerTTL)
	st, err := newStore(filepath.Join(cfg.DataDir, "data"), reg)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	cas, err := newCAS(cfg, st, transport)
	if err != nil {
		return nil, err
	}
	staging, err := newStaging(st, stagingConfig{replicas: cfg.Replicas, minSeeds: cfg.MinSeeds, cacheSize: cfg.XorbCacheSize, secret: cfg.Secret})
	if err != nil {
		return nil, err
	}
	g := &Gateway{mux: http.NewServeMux(), cas: cas, reg: reg, st: st, staging: staging, secret: cfg.Secret, locality: cfg.Locality, transport: transport}
	g.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g.mux.HandleFunc(apis.AnnouncePath, route(http.MethodPost, g.serveAnnounce))
	g.mux.HandleFunc(apis.PeersPath, route(http.MethodGet, g.servePeers))
	g.mux.Handle("/xet-pd/", http.NotFoundHandler()) // unknown control paths never reach the upstream
	g.mux.HandleFunc("/", g.servePublic)
	return g, nil
}

// newCAS serves the CAS routes over st and falls through to a hub mirror of cfg.Upstream; transport carries the upstream downloads.
func newCAS(cfg Config, st *store, transport *http.Transport) (http.Handler, error) {
	issuer, err := auth.NewIssuer([]byte(cfg.SigningKey), 0, nil)
	if err != nil {
		return nil, err
	}
	provider := client.StaticUpstreamProvider(cfg.Upstream, cfg.UpstreamToken)
	mir, err := mirror.NewMirror(
		mirror.WithStorage(st),
		mirror.WithCacheDir(filepath.Join(cfg.DataDir, "mirror")),
		mirror.WithTransport(transport),
	)
	if err != nil {
		return nil, err
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
	return server.NewHandler(server.WithStorage(st), server.WithAuthorizer(authorizer), server.WithNext(hub)), nil
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

// pathsInfoRe admits the one hub POST that only reads: /api/{type}s/{repo}/paths-info/{rev}.
var pathsInfoRe = regexp.MustCompile(`^/api/(models|datasets|spaces)/.+/paths-info/[^/]+$`)

// telemetryPath takes xet-core >= 1.7.0's per-transfer summary; the CAS handler validates and discards it.
const telemetryPath = "/v1/telemetry"

// servePublic keeps the CAS and hub surface read-only (GET/HEAD plus two POSTs that write nothing: paths-info reads the hub, telemetry is discarded) and tallies xorb GETs; peer pulls carry apis.PeerHeader; a client request is tagged with the node its address resolves to.
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
		} else {
			g.directGets.Add(1)
		}
	}
	if g.locality != nil && r.Header.Get(apis.PeerHeader) == "" {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if node, ok := g.locality.NodeOf(host); ok {
				r = r.WithContext(withClientNode(r.Context(), node))
			}
		}
	}
	g.cas.ServeHTTP(w, r)
}

type clientNodeKey struct{}

func withClientNode(ctx context.Context, node string) context.Context {
	return context.WithValue(ctx, clientNodeKey{}, node)
}

func clientNode(ctx context.Context) string {
	node, _ := ctx.Value(clientNodeKey{}).(string)
	return node
}

func (g *Gateway) servePeers(w http.ResponseWriter, _ *http.Request) {
	d := apis.PeersResponse{Peers: g.reg.snapshot(), XorbBytes: g.staging.snapshot(), XorbGets: apis.XorbGets{Peer: g.peerGets.Load(), Direct: g.directGets.Load()}}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d)
}
