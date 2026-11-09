// Package peer implements an xet-pd seed: it caches whole xorbs pulled from
// the peers holding them and serves them, with range support, while they are still being pulled.
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
	"sync"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/lru"
)

// Config configures New; zero MaxPulls, PullTimeout and Heartbeat take the defaults below.
type Config struct {
	Advertise     string        // public origin of this peer, scheme://host[:port]
	Node          string        // node this peer runs on, as the gateway's locality resolver names it; empty announces none
	Gateway       string        // tracker origin: announces and lookups go there
	DataDir       string        // data directory: data/ the local store, spool/ in-flight pulls
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

// normalize canonicalizes both origins, rejects an invalid node, a missing data dir or negative values and fills in the defaults.
func (c *Config) normalize() error {
	var err error
	if c.Advertise, err = apis.Origin(c.Advertise); err != nil {
		return fmt.Errorf("advertise: %w", err)
	}
	if c.Gateway, err = apis.Origin(c.Gateway); err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	if c.Node != "" && !apis.ValidLabel(c.Node, apis.MaxNodeLen) {
		return fmt.Errorf("node %q: expected 1 to %d of [A-Za-z0-9._-]", c.Node, apis.MaxNodeLen)
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

// Peer is a running seed: it serves HTTP until Close.
type Peer struct {
	cfg     Config
	id      string // stable, derived from Advertise
	epoch   string // random per start
	st      *store
	mux     *http.ServeMux
	ann     *announcer
	metrics *peerMetrics

	evictions chan struct{} // coalesced eviction wake signal

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	mu        sync.Mutex                           // guards xorbs, files, xorbFiles, seeds and warned
	xorbs     lru.Index[xet.XorbHash, ready]       // committed xorbs
	files     map[string]fileEntry                 // known files by hash
	xorbFiles map[xet.XorbHash]map[string]struct{} // hashes of the known files referencing each xorb
	seeds     map[string]struct{}                  // hashes of the files whose xorbs are seed copies
	warned    bool                                 // over the cap with nothing evictable, logged once until it fits
}

// New normalizes cfg, restores the xorbs already in DataDir and starts announcing them to the tracker.
func New(cfg Config) (*Peer, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(cfg.Advertise))
	id := hex.EncodeToString(sum[:16])
	client := apis.NewClient(apis.NewTransport())
	metrics := newPeerMetrics()
	st, err := newStore(storeConfig{
		Dir: cfg.DataDir, Self: apis.Peer{ID: id, URL: cfg.Advertise}, Tracker: cfg.Gateway, Secret: cfg.Secret,
		Client: client, MaxPulls: cfg.MaxPulls, PullTimeout: cfg.PullTimeout, Metrics: metrics,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		cfg:     cfg,
		id:      id,
		epoch:   hex.EncodeToString(nonce[:]),
		st:      st,
		ctx:     ctx,
		cancel:  cancel,
		metrics: metrics,

		evictions: make(chan struct{}, 1),
		files:     map[string]fileEntry{},
		xorbFiles: map[xet.XorbHash]map[string]struct{}{},
		seeds:     map[string]struct{}{},
	}
	p.metrics.reg.MustRegister(peerState{p})
	p.ann = &announcer{p: p, control: client, wake: make(chan struct{}, 1), full: true}
	p.ann.minSeeds.Store(1)
	st.onStored, st.onRead, st.onIdle, st.onFile, st.onLost = p.admit, p.touch, p.retryEviction, p.learn, p.lost
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

// Close stops the announcer and the evictor, cancels in-flight pulls, waits for them and drops idle connections.
func (p *Peer) Close() error {
	p.once.Do(func() {
		p.cancel()
		_ = p.st.Close()
		p.wg.Wait()
		p.ann.control.CloseIdleConnections()
	})
	return nil
}

type peerStats struct {
	bytes                     int64
	entries, pulls, cooldowns int
}

func (p *Peer) stats() peerStats {
	pulls, cooldowns := p.st.stats()
	p.mu.Lock()
	defer p.mu.Unlock()
	return peerStats{bytes: p.xorbs.Usage(), entries: p.xorbs.Len(), pulls: pulls, cooldowns: cooldowns}
}
