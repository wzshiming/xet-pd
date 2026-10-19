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
	"github.com/wzshiming/xet-pd/internal/lru"
)

// pullTimeout bounds one control request to a seed.
const pullTimeout = 2 * time.Second

// stagingConfig tunes seeding and the local xorb cache; zero values take the noted defaults.
type stagingConfig struct {
	replicas  int           // seeds asked to pull each local xorb, default 2
	minSeeds  int           // live holders required before a local copy may be evicted, default 1
	cacheSize int64         // soft cap on local xorb bytes, 0 unlimited
	secret    string        // bearer token of control requests; never sent on data requests
	retry     time.Duration // pause per (xorb, seed) after a pull request, default 1m
	tick      time.Duration // maintenance period besides wakes, default 1s
	maxPulls  int           // parallel pull requests, default 8
}

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

	mu     sync.Mutex        // guards the fields below; never held across store or network calls
	xorbs  lru.Index[staged] // local copies
	warned bool              // over the cap with nothing evictable, logged once until it clears
}

// newStaging indexes the store's local xorbs and hooks the store; call before the store is shared.
func newStaging(st *store, cfg stagingConfig) (*staging, error) {
	cfg.replicas = cmp.Or(cfg.replicas, 2)
	cfg.minSeeds = cmp.Or(cfg.minSeeds, 1)
	cfg.retry = cmp.Or(cfg.retry, time.Minute)
	cfg.tick = cmp.Or(cfg.tick, time.Second)
	cfg.maxPulls = cmp.Or(cfg.maxPulls, 8)
	ctx, cancel := context.WithCancel(context.Background())
	m := &staging{
		cfg:    cfg,
		st:     st,
		reg:    st.reg,
		client: apis.NewClient(apis.NewTransport()),
		wake:   make(chan struct{}, 1),
		sem:    make(chan struct{}, cfg.maxPulls),
		ctx:    ctx,
		cancel: cancel,
	}
	if err := m.scan(); err != nil {
		cancel()
		return nil, err
	}
	st.onStored, st.onRead, st.onIdle = m.stored, m.touch, m.kick
	m.wg.Add(1)
	go m.loop()
	return m, nil
}

// scan indexes the local xorbs oldest first, before m is shared.
func (m *staging) scan() error {
	xorbs, err := lru.Scan(context.Background(), m.st)
	if err != nil {
		return err
	}
	for _, x := range xorbs {
		m.xorbs.Add(x.Hash, x.Size, staged{asked: map[string]time.Time{}})
	}
	return nil
}

// stored records a durable local xorb; inserted bytes supersede any copy tracked under the hash.
func (m *staging) stored(hash xet.XorbHash, size int64, inserted bool) {
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

func (m *staging) loop() {
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

// evict deletes least recently used local copies that enough live seeds hold until usage fits the cap.
func (m *staging) evict() {
	if m.cfg.cacheSize <= 0 {
		return
	}
	skip := map[*lru.Entry[staged]]bool{}
	var skipErr error
	for {
		m.mu.Lock()
		if m.xorbs.Usage() <= m.cfg.cacheSize {
			m.warned = false
			m.mu.Unlock()
			return
		}
		var victim *lru.Entry[staged]
		for e := range m.xorbs.Oldest() {
			if !skip[e] && m.reg.aliveHolders(e.Hash.String()) >= m.cfg.minSeeds {
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
			return
		}
		m.mu.Unlock()
		err := m.st.DeleteXorb(context.Background(), victim.Hash)
		m.mu.Lock()
		switch {
		case err != nil:
			skip[victim], skipErr = true, err
		case m.xorbs.Get(victim.Hash) == victim: // a copy re-inserted meanwhile is a new entry and stays
			m.xorbs.Remove(victim)
		}
		m.mu.Unlock()
	}
}

// seed asks the best-ranked live seeds not yet holding each local xorb to pull it, within maxPulls.
func (m *staging) seed() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for e := range m.xorbs.Oldest() {
		for id, next := range e.V.asked {
			if !now.Before(next) {
				delete(e.V.asked, id)
			}
		}
		ranked := m.reg.rank(e.Hash.String())
		for _, p := range ranked[:min(len(ranked), m.cfg.replicas)] {
			if _, asked := e.V.asked[p.ID]; p.holder || asked {
				continue
			}
			select {
			case m.sem <- struct{}{}:
			default:
				return
			}
			e.V.asked[p.ID] = now.Add(m.cfg.retry)
			m.wg.Add(1)
			go m.pull(p.Peer, e.Hash)
		}
	}
}

func (m *staging) pull(p apis.Peer, hash xet.XorbHash) {
	defer m.wg.Done()
	defer m.kick()
	defer func() { <-m.sem }()
	if err := m.request(p, hash); err != nil && m.ctx.Err() == nil {
		slog.Warn("gateway: pull request", "peer", p.ID, "hash", hash, "err", err)
	}
}

// request asks one seed to pull the xorb; only a 202 counts as accepted.
func (m *staging) request(p apis.Peer, hash xet.XorbHash) error {
	ctx, cancel := context.WithTimeout(m.ctx, pullTimeout)
	defer cancel()
	resp, err := apis.PostJSON(ctx, m.client, p.URL+apis.PullPath, m.cfg.secret, apis.PullRequest{Hash: hash.String()})
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return errors.New(resp.Status)
	}
	return nil
}
