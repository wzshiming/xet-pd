package gateway

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
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

// chunkCacheSize bounds the chunk cache of upstream xet downloads; the gateway keeps no xorb bytes of its own.
const chunkCacheSize = 64 << 20

// Config configures New; zero values take the noted defaults.
type Config struct {
	StorageDir    string // required: data/ holds the store, mirror/ the index, spools and chunk cache
	Upstream      string // required hub origin, scheme://host[:port]
	UpstreamToken string
	SigningKey    string        // HMAC key of CAS read tokens; empty leaves CAS routes open
	ExternalURL   string        // origin advertised in hub links; empty derives it from each request
	Secret        string        // bearer token of announces and pull requests
	PeerTTL       time.Duration // announce validity, default 30s, at least 1s
	Replicas      int           // seeds asked to hold each xorb, default 2
	MinSeeds      int           // live holders before the local copy may go, default 1
	XorbCacheSize int64         // bytes of local xorbs kept, 0 unlimited
}

func (c *Config) normalize() error {
	var err error
	if c.StorageDir == "" {
		return errors.New("storage dir is required")
	}
	if c.Upstream == "" {
		return errors.New("upstream is required")
	}
	if c.Upstream, err = origin(c.Upstream); err != nil {
		return fmt.Errorf("upstream: %w", err)
	}
	if c.ExternalURL != "" {
		if c.ExternalURL, err = origin(c.ExternalURL); err != nil {
			return fmt.Errorf("external url: %w", err)
		}
	}
	if c.PeerTTL < 0 || c.Replicas < 0 || c.MinSeeds < 0 || c.XorbCacheSize < 0 {
		return errors.New("negative config value")
	}
	c.PeerTTL = cmp.Or(c.PeerTTL, 30*time.Second)
	if c.PeerTTL < time.Second {
		return errors.New("peer ttl below 1s")
	}
	c.Replicas = cmp.Or(c.Replicas, 2)
	c.MinSeeds = cmp.Or(c.MinSeeds, 1)
	if c.MinSeeds > c.Replicas {
		return errors.New("min seeds exceed replicas")
	}
	return nil
}

// origin accepts an absolute http(s) origin: a host, an optional port 1..65535 ("host:" is rejected) and no userinfo, path, query or fragment.
func origin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("invalid url") // the parse error quotes raw, credentials included
	}
	port, err := strconv.Atoi(u.Port())
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return "", errors.New("scheme is not http(s)")
	case u.Hostname() == "" || u.Opaque != "":
		return "", errors.New("missing host")
	case strings.HasSuffix(u.Host, ":"+u.Port()) && (err != nil || port < 1 || port > 65535):
		return "", errors.New("invalid port")
	case u.User != nil:
		return "", errors.New("userinfo not allowed")
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "":
		return "", errors.New("query and fragment not allowed")
	case u.Path != "" && u.Path != "/":
		return "", errors.New("base path not supported")
	}
	return u.Scheme + "://" + u.Host, nil
}

// Gateway is the xet-pd central service: a hub mirror whose xorb bytes live on registered peers.
type Gateway struct {
	mux       *http.ServeMux
	cas       http.Handler
	reg       *registry
	st        *store
	manager   *staging
	secret    string
	transport *http.Transport // upstream xet downloads

	peerGets, directGets atomic.Uint64
	once                 sync.Once
}

// New composes the store, hub mirror, CAS server and staging manager behind one read-only handler.
func New(cfg Config) (*Gateway, error) {
	g, err := build(cfg)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	return g, nil
}

func build(cfg Config) (*Gateway, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	reg := newRegistry(cfg.PeerTTL)
	st, err := newStore(filepath.Join(cfg.StorageDir, "data"), reg)
	if err != nil {
		return nil, err
	}
	issuer, err := auth.NewIssuer([]byte(cfg.SigningKey), 0, nil)
	if err != nil {
		return nil, err
	}
	mirrorDir := filepath.Join(cfg.StorageDir, "mirror")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	xetClient, err := client.NewClient(
		client.WithCacheDir(filepath.Join(mirrorDir, "chunks")),
		client.WithCacheSize(chunkCacheSize),
		client.WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		return nil, err
	}
	proxy, err := hf.NewUpstreamProxy(cfg.Upstream, cfg.UpstreamToken)
	if err != nil {
		return nil, err
	}
	mir, err := mirror.NewMirror(
		mirror.WithStorage(st),
		mirror.WithUpstream(cfg.Upstream),
		mirror.WithUpstreamToken(cfg.UpstreamToken),
		mirror.WithCacheDir(mirrorDir),
		mirror.WithClient(xetClient),
	)
	if err != nil {
		return nil, err
	}
	hub := hf.NewHandler(
		hf.WithMirror(mir),
		hf.WithExternalURL(cfg.ExternalURL),
		hf.WithMinter(hf.MinterFunc(func(_ *http.Request, req hf.TokenRequest) (string, int64, error) {
			if req.Permission != auth.Read {
				return "", 0, auth.ErrForbidden // answered here, never delegated to the upstream
			}
			return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
		})),
		hf.WithNext(proxy),
	)
	var authorizer auth.Authorizer
	if cfg.SigningKey != "" {
		authorizer = issuer
	}
	cas := server.NewHandler(server.WithStorage(st), server.WithAuthorizer(authorizer), server.WithNext(hub))
	manager, err := newStaging(st, stagingConfig{replicas: cfg.Replicas, minSeeds: cfg.MinSeeds, cacheSize: cfg.XorbCacheSize, secret: cfg.Secret})
	if err != nil {
		return nil, err
	}
	g := &Gateway{mux: http.NewServeMux(), cas: cas, reg: reg, st: st, manager: manager, secret: cfg.Secret, transport: transport}
	g.mux.HandleFunc("/healthz", control(http.MethodGet, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	g.mux.HandleFunc(apis.AnnouncePath, control(http.MethodPost, g.serveAnnounce))
	g.mux.HandleFunc(apis.PeersPath, control(http.MethodGet, g.servePeers))
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
		_ = g.manager.Close()
		g.transport.CloseIdleConnections()
		g.st.transport.CloseIdleConnections()
	})
	return nil
}

// control answers a control route for one method and 405 otherwise.
func control(method string, h http.HandlerFunc) http.HandlerFunc {
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

// servePublic keeps the CAS and hub surface read-only and tallies xorb GETs; peer fills carry apis.PeerHeader.
func (g *Gateway) servePublic(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
	case r.Method == http.MethodPost && pathsInfoRe.MatchString(r.URL.EscapedPath()):
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
	g.cas.ServeHTTP(w, r)
}

func (g *Gateway) servePeers(w http.ResponseWriter, _ *http.Request) {
	d := apis.PeersResponse{Peers: g.reg.snapshot(), XorbBytes: g.manager.snapshot(), XorbGets: apis.XorbGets{Peer: g.peerGets.Load(), Direct: g.directGets.Load()}}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d)
}
