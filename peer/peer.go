// Package peer implements an xet-pd peer: its node's read-only HF endpoint, a hub mirror and CAS over
// a store that pulls missing xorbs from the peers holding them and serves them while they are still being pulled.
package peer

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/hf"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/lru"
)

// Config configures New; zero MaxPulls, PullTimeout and Heartbeat take the defaults below.
type Config struct {
	Advertise     string        // public origin of this peer, scheme://host[:port]; also the hub's external URL
	Tracker       string        // tracker origin: announces and lookups go there
	Upstream      string        // origin of the upstream hub the mirror ingests from; empty mirrors through the access peers the tracker lists
	UpstreamToken string        // optional bearer token for the upstream hub, only with Upstream
	DataDir       string        // data directory: data/ the local store, spool/ in-flight pulls, mirror/ the hub mirror
	XorbCacheSize int64         // bytes of committed xorbs to keep, 0 unlimited; spools are extra
	MaxPulls      int           // parallel pulls from other peers, default 8
	PullTimeout   time.Duration // per pull including queueing, default 10m
	Heartbeat     time.Duration // announce interval, default 10s
	Secret        string        // optional bearer token shared with the tracker and the other peers
}

// Defaults New applies to zero Config values; cmd/peer shows them as flag defaults.
const (
	DefaultMaxPulls    = 8
	DefaultPullTimeout = 10 * time.Minute
	DefaultHeartbeat   = 10 * time.Second
)

// normalize canonicalizes the origins, rejects an upstream token without an upstream, a missing data dir and negative values and fills in the defaults.
func (c *Config) normalize() error {
	var err error
	if c.Advertise, err = apis.Origin(c.Advertise); err != nil {
		return fmt.Errorf("advertise: %w", err)
	}
	if c.Tracker, err = apis.Origin(c.Tracker); err != nil {
		return fmt.Errorf("tracker: %w", err)
	}
	switch {
	case c.Upstream != "":
		if c.Upstream, err = apis.Origin(c.Upstream); err != nil {
			return fmt.Errorf("upstream: %w", err)
		}
	case c.UpstreamToken != "":
		return errors.New("upstream token needs upstream")
	}
	if c.DataDir == "" {
		return errors.New("data dir is required")
	}
	if c.XorbCacheSize < 0 || c.MaxPulls < 0 || c.PullTimeout < 0 || c.Heartbeat < 0 {
		return errors.New("negative config value")
	}
	c.MaxPulls = cmp.Or(c.MaxPulls, DefaultMaxPulls)
	c.PullTimeout = cmp.Or(c.PullTimeout, DefaultPullTimeout)
	c.Heartbeat = cmp.Or(c.Heartbeat, DefaultHeartbeat)
	return nil
}

// Peer is a running node endpoint: it serves HTTP until Close.
type Peer struct {
	cfg       Config
	id        string // stable, derived from Advertise
	epoch     string // random per start
	st        *store
	cas       http.Handler    // CAS routes over st, falling through to the hub mirror
	transport *http.Transport // upstream hub downloads
	mux       *http.ServeMux
	ann       *announcer
	metrics   *peerMetrics

	evictions chan struct{} // coalesced eviction wake signal

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	mu     sync.Mutex                     // guards xorbs, shards, seeds and warned
	xorbs  lru.Index[xet.XorbHash, ready] // committed xorbs
	shards map[string]apis.File           // known shards by file hash, never forgotten
	seeds  map[xet.XorbHash]struct{}      // xorbs whose copies are seed copies
	warned bool                           // over the cap with nothing evictable, logged once until it fits
}

// New normalizes cfg, restores the xorbs already in DataDir, starts announcing them to the tracker and serves the hub mirror and CAS over them.
func New(cfg Config) (*Peer, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	issuer, err := auth.NewIssuer(nil, 0, nil)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(cfg.Advertise))
	id := hex.EncodeToString(sum[:16])
	control := apis.NewClient(apis.NewTransport())
	metrics := newPeerMetrics()
	st, err := newStore(storeConfig{
		Dir: cfg.DataDir, Self: apis.Peer{ID: id, URL: cfg.Advertise}, Tracker: cfg.Tracker, Secret: cfg.Secret,
		Client: control, MaxPulls: cfg.MaxPulls, PullTimeout: cfg.PullTimeout, Metrics: metrics,
	})
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		cfg:       cfg,
		id:        id,
		epoch:     hex.EncodeToString(nonce[:]),
		st:        st,
		transport: transport,
		ctx:       ctx,
		cancel:    cancel,
		metrics:   metrics,

		evictions: make(chan struct{}, 1),
		shards:    map[string]apis.File{},
		seeds:     map[xet.XorbHash]struct{}{},
	}
	p.metrics.reg.MustRegister(peerState{p})
	p.ann = &announcer{p: p, control: control, wake: make(chan struct{}, 1), full: true}
	p.ann.minSeeds.Store(1)
	provider := client.StaticUpstreamProvider(cfg.Upstream, cfg.UpstreamToken)
	var upstream http.RoundTripper = transport
	if cfg.Upstream == "" {
		provider, upstream = accessUpstream{p.ann}, peerTransport{base: transport, id: id}
	}
	mir, err := mirror.NewMirror(mirror.WithStorage(st), mirror.WithCacheDir(filepath.Join(cfg.DataDir, "mirror")), mirror.WithTransport(upstream))
	if err != nil {
		cancel()
		_ = st.Close()
		return nil, err
	}
	hub := hf.NewHandler(
		hf.WithMirror(mir),
		hf.WithUpstreamProvider(provider),
		hf.WithExternalURL(cfg.Advertise),
		hf.WithMinter(hf.MinterFunc(func(_ *http.Request, req hf.TokenRequest) (string, int64, error) {
			if req.Permission != auth.Read {
				return "", 0, auth.ErrForbidden // answered here, never delegated to the upstream
			}
			return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
		})),
		hf.WithNext(hf.NewUpstreamProxy(provider)),
	)
	// No authorizer: the CAS serves the clients of its own node.
	p.cas = server.NewHandler(server.WithStorage(st), server.WithNext(hub))
	st.onStored, st.onRead, st.onIdle, st.onShard, st.onLost = p.admit, p.touch, p.retryEviction, p.holdShard, p.lost
	p.mux = p.routes()
	if err := p.restore(); err != nil {
		cancel()
		_ = st.Close()
		return nil, err
	}
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		p.ann.run()
	}()
	go func() {
		defer p.wg.Done()
		p.evictor()
	}()
	return p, nil
}

// Close stops the announcer and the evictor, cancels in-flight pulls, waits for them and drops idle connections; ingests the mirror started finish on their own.
func (p *Peer) Close() error {
	p.once.Do(func() {
		p.mu.Lock()
		p.cancel() // servePull adds to p.wg under p.mu only while p.ctx lives
		p.mu.Unlock()
		_ = p.st.Close()
		p.wg.Wait()
		p.ann.control.CloseIdleConnections()
		p.transport.CloseIdleConnections()
	})
	return nil
}

type peerStats struct {
	bytes                             int64
	shards, entries, pulls, cooldowns int
}

func (p *Peer) stats() peerStats {
	pulls, cooldowns := p.st.stats()
	p.mu.Lock()
	defer p.mu.Unlock()
	return peerStats{bytes: p.xorbs.Usage(), shards: len(p.shards), entries: p.xorbs.Len(), pulls: pulls, cooldowns: cooldowns}
}
