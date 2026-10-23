// Package peer implements an xet-pd seed: it caches whole xorbs pulled through
// the gateway and serves them, with range support, while they are still being pulled.
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
	"github.com/wzshiming/xet/storage/local"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/lru"
)

// Config configures New; zero MaxPulls, PullTimeout and Heartbeat take the defaults below.
type Config struct {
	Advertise     string        // public origin of this peer, scheme://host[:port]
	Gateway       string        // upstream gateway origin
	DataDir       string        // data directory: xorbs/ committed objects, spool/ in-flight pulls
	XorbCacheSize int64         // bytes of committed xorbs to keep, 0 unlimited; spools are extra
	MaxPulls      int           // parallel pulls from the gateway, default 8
	PullTimeout   time.Duration // per pull including queueing, default 10m
	Heartbeat     time.Duration // announce interval, default 10s
	Secret        string        // optional bearer token shared with the gateway for pull and announce
}

// Defaults New applies to zero Config values; cmd/peer shows them as flag defaults.
const (
	DefaultMaxPulls    = 8
	DefaultPullTimeout = 10 * time.Minute
	DefaultHeartbeat   = 10 * time.Second
)

// normalize canonicalizes both origins, rejects a missing data dir or negative values and fills in the defaults.
func (c *Config) normalize() error {
	var err error
	if c.Advertise, err = apis.Origin(c.Advertise); err != nil {
		return fmt.Errorf("advertise: %w", err)
	}
	if c.Gateway, err = apis.Origin(c.Gateway); err != nil {
		return fmt.Errorf("gateway: %w", err)
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
	cfg      Config
	id       string // stable, derived from Advertise
	epoch    string // random per start
	store    *local.Storage
	spoolDir string
	client   *http.Client // data fetches, no secret
	mux      *http.ServeMux
	ann      *announcer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	sem    chan struct{} // bounds parallel upstream pulls
	once   sync.Once

	mu       sync.Mutex // guards pulls, cooldown, xorbs with each entry's pins and pull, each pull's refs, queued and keep, and cancel-vs-acquire
	pulls    map[xet.XorbHash]*pull
	cooldown map[xet.XorbHash]cooled
	xorbs    lru.Index[xet.XorbHash, ready] // committed xorbs
}

// New normalizes cfg, restores the xorbs already in DataDir and starts announcing them to the gateway.
func New(cfg Config) (*Peer, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	store, err := local.NewStorage(local.WithBasePath(cfg.DataDir))
	if err != nil {
		return nil, err
	}
	spoolDir, err := openSpool(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(cfg.Advertise))
	transport := apis.NewTransport()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		cfg:      cfg,
		id:       hex.EncodeToString(sum[:16]),
		epoch:    hex.EncodeToString(nonce[:]),
		store:    store,
		spoolDir: spoolDir,
		client:   &http.Client{Transport: transport},
		ctx:      ctx,
		cancel:   cancel,
		sem:      make(chan struct{}, cfg.MaxPulls),
		pulls:    map[xet.XorbHash]*pull{},
		cooldown: map[xet.XorbHash]cooled{},
	}
	p.ann = &announcer{p: p, control: apis.NewClient(transport), wake: make(chan struct{}, 1), full: true}
	p.mux = p.routes()
	if err := p.restore(); err != nil {
		cancel()
		return nil, err
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.ann.run()
	}()
	return p, nil
}

// Close stops the announcer, cancels in-flight pulls, waits for them and drops idle upstream connections.
func (p *Peer) Close() error {
	p.once.Do(func() {
		p.mu.Lock()
		p.cancel() // acquire checks p.ctx under p.mu, so no pull starts after Close
		p.mu.Unlock()
		p.wg.Wait()
		p.client.CloseIdleConnections()
	})
	return nil
}
