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
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage/local"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/inventory"
	"github.com/wzshiming/xet-pd/internal/lru"
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

const (
	announceTimeout = 5 * time.Second
	maxReplyBytes   = 4096
)

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

	mu        sync.Mutex // guards pulls, cooldown, the inventory, unchecked and cancel-vs-acquire
	pulls     map[xet.XorbHash]*pull
	cooldown  map[xet.XorbHash]cooled
	xorbs     lru.Index[xet.XorbHash, ready] // committed xorbs
	unchecked map[xet.XorbHash]bool          // files of the startup scan awaiting validation; true: invalid, left to the in-flight pull
}

func New(cfg Config) (*Peer, error) {
	var err error
	if cfg.Advertise, err = apis.Origin(cfg.Advertise); err != nil {
		return nil, fmt.Errorf("advertise: %w", err)
	}
	if cfg.Gateway, err = apis.Origin(cfg.Gateway); err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	if cfg.CacheDir == "" {
		return nil, errors.New("cache dir is required")
	}
	if cfg.XorbCacheSize < 0 || cfg.MaxPulls < 0 || cfg.PullTimeout < 0 || cfg.Heartbeat < 0 {
		return nil, errors.New("negative config value")
	}
	cfg.MaxPulls = cmp.Or(cfg.MaxPulls, 8)
	cfg.PullTimeout = cmp.Or(cfg.PullTimeout, 10*time.Minute)
	cfg.Heartbeat = cmp.Or(cfg.Heartbeat, 10*time.Second)
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
	transport := apis.NewTransport()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		cfg:      cfg,
		id:       hex.EncodeToString(sum[:16]),
		epoch:    hex.EncodeToString(nonce[:]),
		store:    store,
		spoolDir: spoolDir,
		client:   &http.Client{Transport: transport},
		control:  apis.NewClient(transport),
		mux:      http.NewServeMux(),
		wake:     make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
		sem:      make(chan struct{}, cfg.MaxPulls),
		pulls:    map[xet.XorbHash]*pull{},
		cooldown: map[xet.XorbHash]cooled{},
	}
	xorbs, err := inventory.Scan(context.Background(), p.store)
	if err != nil {
		cancel()
		return nil, err
	}
	p.unchecked = make(map[xet.XorbHash]bool, len(xorbs))
	for _, x := range xorbs {
		p.unchecked[x.Hash] = false
	}
	// Validation reads every byte, so it runs behind New and admits the files as they pass.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.validateUnchecked(xorbs)
	}()
	// The announcer reconciles the gateway's view with the inventory.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var acked map[string]struct{} // inventory the gateway confirmed
		full := true                  // next request carries the whole inventory
		var failed, warned bool
		tick := time.NewTicker(p.cfg.Heartbeat)
		defer tick.Stop()
		for {
			wasFull := full
			p.mu.Lock()
			select {
			case <-p.wake: // changes before this snapshot are covered by it
			default:
			}
			snap := make(map[string]struct{}, p.xorbs.Len())
			for e := range p.xorbs.Oldest() {
				snap[e.Key.String()] = struct{}{}
			}
			p.mu.Unlock()
			req := apis.AnnounceRequest{Peer: apis.Peer{ID: p.id, URL: p.cfg.Advertise}, Epoch: p.epoch, Full: full}
			for h := range snap {
				if _, ok := acked[h]; full || !ok {
					req.Add = append(req.Add, h)
				}
			}
			for h := range acked {
				if _, ok := snap[h]; !ok && !full {
					req.Remove = append(req.Remove, h)
				}
			}
			slices.Sort(req.Add)
			slices.Sort(req.Remove)
			var out apis.AnnounceResponse
			ctx, cancel := context.WithTimeout(p.ctx, announceTimeout)
			resp, err := apis.PostJSON(ctx, p.control, p.cfg.Gateway+apis.AnnouncePath, p.cfg.Secret, req)
			if err == nil {
				if resp.StatusCode/100 != 2 {
					err = fmt.Errorf("gateway status %s", resp.Status)
				} else if derr := apis.DecodeJSON(resp.Body, maxReplyBytes, &out); derr != nil {
					err = fmt.Errorf("gateway reply: %w", derr)
				}
				_ = resp.Body.Close()
			}
			cancel()
			if err != nil {
				if !failed {
					slog.Warn("peer: announce", "err", err)
				}
				// The gateway may have applied this request, so only a Full can resync it.
				failed, full = true, true
			} else {
				failed = false
				acked, full = snap, out.Resync
				if !warned && out.TTL > 0 && p.cfg.Heartbeat >= time.Duration(out.TTL)*time.Second {
					slog.Warn("peer: heartbeat not shorter than gateway TTL", "heartbeat", p.cfg.Heartbeat, "ttl", out.TTL)
					warned = true
				}
				// A resync to a delta is answered at once; a resync to a Full waits for the heartbeat.
				if out.Resync && !wasFull && p.ctx.Err() == nil {
					continue
				}
			}
			select {
			case <-p.ctx.Done():
				return
			case <-tick.C:
			case <-p.wake:
			}
		}
	}()
	p.mux.HandleFunc("GET /v1/xorbs/{prefix}/{hash}", p.serveXorb)
	p.mux.HandleFunc("POST "+apis.PullPath, p.servePull)
	p.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return p, nil
}

func (p *Peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

// Validated reports whether every xorb found on disk at startup has been validated, deleted or replaced; it stays false once Close interrupts that.
func (p *Peer) Validated() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.unchecked) == 0
}

// Close stops the announcer and the startup validation, cancels in-flight pulls, waits for them and drops idle upstream connections.
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
func (p *Peer) serveStored(w http.ResponseWriter, r *http.Request, rd *lru.Entry[xet.XorbHash, ready]) error {
	defer p.unpin(rd)
	obj, err := p.store.GetXorbReadSeekCloser(r.Context(), rd.Key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			p.drop(rd)
		}
		return err
	}
	defer func() { _ = obj.Close() }()
	serveContent(w, r, rd.Key, obj)
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
	// The stream can be served once headers are handled and the size is known, or once the pull finished.
	size := int64(-1)
	select {
	case <-f.started:
		select {
		case <-f.done:
		default:
			size = f.length
		}
	case <-r.Context().Done():
		err = r.Context().Err()
	}
	if err == nil && size < 1 {
		select {
		case <-f.done:
			err, size = f.err, f.size
		case <-r.Context().Done():
			err = r.Context().Err()
		}
	}
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	rs, err := f.stream.NewReadSeeker(0, int(size))
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
	if !apis.Authorize(w, r, p.cfg.Secret) {
		return
	}
	var req apis.PullRequest
	if !apis.ReadJSON(w, r, maxPullBytes, &req) {
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
