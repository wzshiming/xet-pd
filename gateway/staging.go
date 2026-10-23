package gateway

import (
	"cmp"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/inventory"
	"github.com/wzshiming/xet-pd/internal/lru"
)

// pullTimeout bounds one control request to a seed.
const pullTimeout = 2 * time.Second

// stagingConfig tunes seeding and the local xorb cache; zero values take the noted defaults.
type stagingConfig struct {
	replicas  int           // seeds asked to pull each local xorb
	minSeeds  int           // live holders required before a local copy may be evicted
	cacheSize int64         // soft cap on local xorb bytes, 0 unlimited
	secret    string        // bearer token of control requests; never sent on data requests
	retry     time.Duration // pause per (xorb, seed) after a pull request, default 1m
	tick      time.Duration // maintenance period besides wakes, default 1s
}

// maxPulls bounds parallel pull requests.
const maxPulls = 8

// maxPullReplyBytes bounds the reply body drained so the connection can be reused.
const maxPullReplyBytes = 4096

// staged is the seeding state of one durable local xorb.
type staged struct {
	asked map[string]time.Time // seed ID -> when it may be asked again
}

// staging asks seeds to replicate local xorbs and evicts replicated ones under cache pressure.
type staging struct {
	cfg    stagingConfig
	st     *store
	reg    *registry
	client *http.Client // control requests: carries the secret, never follows redirects
	wake   chan struct{}
	sem    chan struct{} // bounds in-flight pull requests
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	mu     sync.Mutex                      // guards xorbs, warned and every staged.asked; ordered before reg.mu, never held across store or network calls
	xorbs  lru.Index[xet.XorbHash, staged] // local copies
	warned bool                            // over the cap with nothing evictable, logged once until it clears
}

// newStaging indexes the store's local xorbs and hooks the store; call before the store is shared.
func newStaging(st *store, cfg stagingConfig) (*staging, error) {
	cfg.retry = cmp.Or(cfg.retry, time.Minute)
	cfg.tick = cmp.Or(cfg.tick, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	m := &staging{
		cfg:    cfg,
		st:     st,
		reg:    st.reg,
		client: apis.NewClient(apis.NewTransport()),
		wake:   make(chan struct{}, 1),
		sem:    make(chan struct{}, maxPulls),
		ctx:    ctx,
		cancel: cancel,
	}
	xorbs, err := inventory.Scan(m.ctx, st)
	if err != nil {
		cancel()
		return nil, err
	}
	for _, x := range xorbs {
		m.xorbs.Add(x.Hash, x.Size, staged{asked: map[string]time.Time{}})
	}
	st.onStored, st.onRead, st.onIdle = m.admit, m.touch, m.kick
	m.wg.Add(1)
	go m.run()
	return m, nil
}

// run is the maintenance goroutine: one pass per tick or wake until Close.
func (m *staging) run() {
	defer m.wg.Done()
	tick := time.NewTicker(m.cfg.tick)
	defer tick.Stop()
	for {
		m.evict()
		m.seed()
		select {
		case <-m.ctx.Done():
			return
		case <-tick.C:
		case <-m.wake:
		}
	}
}

// evict deletes the least recently used copies with enough live holders until usage fits the cap; a refused delete skips that copy for the pass.
func (m *staging) evict() {
	if m.cfg.cacheSize <= 0 {
		return
	}
	skip := map[*lru.Entry[xet.XorbHash, staged]]bool{}
	var skipErr error
	for {
		m.mu.Lock()
		if m.xorbs.Usage() <= m.cfg.cacheSize {
			m.warned = false
			m.mu.Unlock()
			break
		}
		var victim *lru.Entry[xet.XorbHash, staged]
		for e := range m.xorbs.Oldest() {
			if !skip[e] && m.reg.aliveHolders(e.Key.String()) >= m.cfg.minSeeds {
				victim = e
				break
			}
		}
		if victim == nil {
			if !m.warned {
				slog.Warn("gateway: local xorbs over the cache size, nothing evictable yet", "usage", m.xorbs.Usage(), "limit", m.cfg.cacheSize, "err", skipErr)
				m.warned = true
			}
			m.mu.Unlock()
			break
		}
		m.mu.Unlock()
		err := m.st.DeleteXorb(context.Background(), victim.Key)
		m.mu.Lock()
		if err != nil {
			skip[victim], skipErr = true, err
		} else {
			m.xorbs.Remove(victim) // a copy re-inserted meanwhile is a new entry and stays
		}
		m.mu.Unlock()
	}
}

// seed asks the best-ranked non-holders of each copy to pull it, oldest copy first, until every pull slot is busy.
func (m *staging) seed() {
	now := time.Now()
	m.mu.Lock()
seeding:
	for e := range m.xorbs.Oldest() {
		for id, next := range e.V.asked {
			if !now.Before(next) {
				delete(e.V.asked, id)
			}
		}
		ranked := m.reg.rank(e.Key.String())
		for _, p := range ranked[:min(len(ranked), m.cfg.replicas)] {
			if _, asked := e.V.asked[p.ID]; p.holder || asked {
				continue
			}
			select {
			case m.sem <- struct{}{}:
			default:
				break seeding
			}
			e.V.asked[p.ID] = now.Add(m.cfg.retry)
			m.wg.Add(1)
			go m.pull(p.Peer, e.Key)
		}
	}
	m.mu.Unlock()
}

// pull asks one seed to pull hash; only 202 counts, failures are logged unless staging is closing.
func (m *staging) pull(p apis.Peer, hash xet.XorbHash) {
	defer m.wg.Done()
	defer m.kick()
	defer func() { <-m.sem }()
	ctx, cancel := context.WithTimeout(m.ctx, pullTimeout)
	resp, err := apis.PostJSON(ctx, m.client, p.URL+apis.PullPath, m.cfg.secret, apis.PullRequest{Hash: hash.String()})
	if err == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxPullReplyBytes))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			err = errors.New(resp.Status)
		}
	}
	cancel()
	if err != nil && m.ctx.Err() == nil {
		slog.Warn("gateway: pull request", "peer", p.ID, "hash", hash, "err", err)
	}
}

// admit records a durable local xorb; inserted bytes supersede any copy tracked under the hash.
func (m *staging) admit(hash xet.XorbHash, size int64, inserted bool) {
	m.mu.Lock()
	if !inserted && m.xorbs.Get(hash) != nil {
		m.mu.Unlock()
		return
	}
	m.xorbs.Add(hash, size, staged{asked: map[string]time.Time{}})
	m.mu.Unlock()
	m.kick()
}

// touch marks a local xorb most recently used.
func (m *staging) touch(hash xet.XorbHash) {
	m.mu.Lock()
	if e := m.xorbs.Get(hash); e != nil {
		m.xorbs.Touch(e)
	}
	m.mu.Unlock()
}

// kick schedules a maintenance pass without blocking; safe after Close.
func (m *staging) kick() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// snapshot reports the bytes of tracked local xorbs.
func (m *staging) snapshot() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.xorbs.Usage()
}

// Close stops maintenance, cancels in-flight pull requests and waits for them.
func (m *staging) Close() error {
	m.once.Do(func() {
		m.cancel()
		m.wg.Wait()
		m.client.CloseIdleConnections()
	})
	return nil
}
