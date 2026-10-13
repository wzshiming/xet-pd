// Package peer implements an xet-pd seed: it caches whole xorbs pulled through
// the gateway and serves them, with range support, while they are still being pulled.
package peer

import (
	"cmp"
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage/local"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/control"
)

type Config struct {
	Advertise     string        // public origin of this peer, scheme://host[:port]
	Gateway       string        // upstream gateway origin
	CacheDir      string        // data directory: xorbs/ committed objects, spool/ in-flight pulls
	XorbCacheSize int64         // bytes of committed xorbs to keep, 0 unlimited; spools are extra
	MaxPulls      int           // parallel pulls from the gateway, default 8
	PullTimeout   time.Duration // per pull including queueing, default 10m
	Heartbeat     time.Duration // announce interval, default 10s
	Secret        string        // optional bearer token shared with the gateway for pull and announce
}

func (c *Config) normalize() error {
	var err error
	if c.Advertise, err = control.Origin(c.Advertise); err != nil {
		return fmt.Errorf("advertise: %w", err)
	}
	if c.Gateway, err = control.Origin(c.Gateway); err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	if c.CacheDir == "" {
		return errors.New("cache dir is required")
	}
	if c.XorbCacheSize < 0 || c.MaxPulls < 0 || c.PullTimeout < 0 || c.Heartbeat < 0 {
		return errors.New("negative config value")
	}
	c.MaxPulls = cmp.Or(c.MaxPulls, 8)
	c.PullTimeout = cmp.Or(c.PullTimeout, 10*time.Minute)
	c.Heartbeat = cmp.Or(c.Heartbeat, 10*time.Second)
	return nil
}

type Peer struct {
	cfg      Config
	id       string // stable, derived from Advertise
	epoch    string // random per start
	store    *local.Storage
	spoolDir string
	client   *http.Client // data fetches, no secret
	control  *http.Client // announces, carries the secret and never follows redirects
	mux      *http.ServeMux
	wake     chan struct{} // coalesced inventory change signal for the announcer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	sem    chan struct{} // bounds parallel upstream pulls
	once   sync.Once

	mu       sync.Mutex // guards pulls, cooldown, the inventory and cancel-vs-acquire
	pulls    map[xet.XorbHash]*pull
	cooldown map[xet.XorbHash]cooled
	index    map[xet.XorbHash]*list.Element // committed xorbs, element value *ready
	lru      *list.List                     // front is least recently used
	usage    int64                          // bytes of committed xorbs
}

func New(cfg Config) (*Peer, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	store, err := local.NewStorage(local.WithBasePath(cfg.CacheDir))
	if err != nil {
		return nil, err
	}
	spoolDir := filepath.Join(cfg.CacheDir, "spool")
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return nil, err
	}
	stale, _ := filepath.Glob(filepath.Join(spoolDir, "*.part"))
	for _, f := range stale {
		_ = os.Remove(f)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(cfg.Advertise))
	transport := control.NewTransport()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		cfg:      cfg,
		id:       hex.EncodeToString(sum[:16]),
		epoch:    hex.EncodeToString(nonce[:]),
		store:    store,
		spoolDir: spoolDir,
		client:   &http.Client{Transport: transport},
		control:  control.NewClient(transport),
		mux:      http.NewServeMux(),
		wake:     make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
		sem:      make(chan struct{}, cfg.MaxPulls),
		pulls:    map[xet.XorbHash]*pull{},
		cooldown: map[xet.XorbHash]cooled{},
		index:    map[xet.XorbHash]*list.Element{},
		lru:      list.New(),
	}
	if err := p.scan(); err != nil {
		cancel()
		return nil, err
	}
	p.wg.Add(1)
	go p.announceLoop()
	p.mux.HandleFunc("GET /v1/xorbs/{prefix}/{hash}", p.serveXorb)
	p.mux.HandleFunc("POST "+apis.PullPath, p.servePull)
	p.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return p, nil
}

func (p *Peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

// Close stops the announcer, cancels in-flight pulls, waits for them and drops idle upstream connections.
func (p *Peer) Close() error {
	p.once.Do(func() {
		p.mu.Lock()
		p.cancel()
		p.mu.Unlock()
		p.wg.Wait()
		p.client.CloseIdleConnections()
	})
	return nil
}

func serveContent(w http.ResponseWriter, r *http.Request, hash xet.XorbHash, content io.ReadSeeker) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+hash.String()+`"`)
	http.ServeContent(w, r, "", time.Time{}, content)
}

// serveStored serves a pinned committed xorb and releases the pin; a missing file leaves the inventory so the next request pulls again.
func (p *Peer) serveStored(w http.ResponseWriter, r *http.Request, rd *ready) error {
	defer p.unpin(rd)
	obj, err := p.store.GetXorbReadSeekCloser(r.Context(), rd.hash)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			p.drop(rd)
		}
		return err
	}
	defer func() { _ = obj.Close() }()
	serveContent(w, r, rd.hash, obj)
	return nil
}

// serveXorb serves a committed xorb or streams it from a pull; requests carrying apis.PeerHeader are relays and never pull.
func (p *Peer) serveXorb(w http.ResponseWriter, r *http.Request) {
	hash, err := xet.ParseXorbHash(r.PathValue("hash"))
	if err != nil {
		http.Error(w, "invalid xorb path", http.StatusBadRequest)
		return
	}
	if stored := p.pin(hash); stored != nil {
		err := p.serveStored(w, r, stored)
		if err == nil {
			return
		}
		if !errors.Is(err, fs.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if r.Header.Get(apis.PeerHeader) != "" {
		http.NotFound(w, r)
		return
	}
	f, stored, err := p.acquire(hash, false)
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	if stored != nil {
		if err := p.serveStored(w, r, stored); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer f.release()
	stream, size, err := f.wait(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	rs, err := stream.NewReadSeeker(0, int(size))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = rs.Close() }()
	// Reads block on the upstream regardless of the client; closing wakes them.
	stop := context.AfterFunc(r.Context(), func() { _ = rs.Close() })
	defer stop()
	serveContent(w, r, hash, rs)
}

// maxPullBytes bounds one pull body, which names a single xorb.
const maxPullBytes = 4096

// servePull starts (or joins) a pull on behalf of the gateway and answers 202 at once.
func (p *Peer) servePull(w http.ResponseWriter, r *http.Request) {
	if !control.Authorized(r, p.cfg.Secret) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req apis.PullRequest
	if !control.ReadJSON(w, r, maxPullBytes, &req) {
		return
	}
	hash, err := xet.ParseXorbHash(req.Hash)
	if err != nil {
		http.Error(w, "invalid pull request", http.StatusBadRequest)
		return
	}
	f, stored, err := p.acquire(hash, true)
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	if f != nil {
		f.release()
	} else {
		p.unpin(stored)
	}
	w.WriteHeader(http.StatusAccepted)
}
