package gateway

import (
	"cmp"
	"container/list"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/control"
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
	jobs      int           // parallel pull requests, default 8
}

// staged is one durable local xorb; its LRU position is its recency.
type staged struct {
	hash  xet.XorbHash
	size  int64
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

	mu     sync.Mutex // guards the fields below; never held across store or network calls
	index  map[xet.XorbHash]*list.Element
	lru    *list.List // front is least recently used
	usage  int64
	warned bool // over the cap with nothing evictable, logged once until it clears
}

// newStaging indexes the store's local xorbs and hooks the store; call before the store is shared.
func newStaging(st *store, cfg stagingConfig) (*staging, error) {
	cfg.replicas = cmp.Or(cfg.replicas, 2)
	cfg.minSeeds = cmp.Or(cfg.minSeeds, 1)
	cfg.retry = cmp.Or(cfg.retry, time.Minute)
	cfg.tick = cmp.Or(cfg.tick, time.Second)
	cfg.jobs = cmp.Or(cfg.jobs, 8)
	ctx, cancel := context.WithCancel(context.Background())
	m := &staging{
		cfg:    cfg,
		st:     st,
		reg:    st.reg,
		client: control.NewClient(control.NewTransport()),
		wake:   make(chan struct{}, 1),
		sem:    make(chan struct{}, cfg.jobs),
		ctx:    ctx,
		cancel: cancel,
		index:  map[xet.XorbHash]*list.Element{},
		lru:    list.New(),
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

// scan indexes the local xorbs oldest first.
func (m *staging) scan() error {
	type found struct {
		hash  xet.XorbHash
		size  int64
		mtime time.Time
	}
	var objs []found
	err := m.st.WalkXorbs(context.Background(), func(name string, size int64, mtime time.Time) error {
		if hash, err := xet.ParseXorbHash(name); err == nil {
			objs = append(objs, found{hash, size, mtime})
		}
		return nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(objs, func(a, b found) int {
		return cmp.Or(a.mtime.Compare(b.mtime), cmp.Compare(a.hash.String(), b.hash.String()))
	})
	for _, o := range objs {
		m.add(o.hash, o.size)
	}
	return nil
}

// add tracks a new local copy as most recently used; caller holds m.mu or owns m alone.
func (m *staging) add(hash xet.XorbHash, size int64) {
	m.index[hash] = m.lru.PushBack(&staged{hash: hash, size: size, asked: map[string]time.Time{}})
	m.usage += size
}

// forget untracks e; caller holds m.mu.
func (m *staging) forget(e *list.Element) {
	s := m.lru.Remove(e).(*staged)
	delete(m.index, s.hash)
	m.usage -= s.size
}

// stored records a durable local xorb; inserted bytes supersede any copy tracked under the hash.
func (m *staging) stored(hash xet.XorbHash, size int64, inserted bool) {
	m.mu.Lock()
	if e := m.index[hash]; e != nil {
		if !inserted {
			m.mu.Unlock()
			return
		}
		m.forget(e)
	}
	m.add(hash, size)
	m.mu.Unlock()
	m.kick()
}

// touch marks a local xorb most recently used.
func (m *staging) touch(hash xet.XorbHash) {
	m.mu.Lock()
	if e := m.index[hash]; e != nil {
		m.lru.MoveToBack(e)
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
	return m.usage
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
	skip := map[*list.Element]bool{}
	var skipErr error
	for {
		m.mu.Lock()
		if m.usage <= m.cfg.cacheSize {
			m.warned = false
			m.mu.Unlock()
			return
		}
		var victim *list.Element
		for e := m.lru.Front(); e != nil && victim == nil; e = e.Next() {
			if !skip[e] && m.reg.aliveHolders(e.Value.(*staged).hash.String()) >= m.cfg.minSeeds {
				victim = e
			}
		}
		if victim == nil {
			if !m.warned {
				slog.Warn("gateway: local xorbs over the cache size, nothing evictable yet", "usage", m.usage, "limit", m.cfg.cacheSize, "err", skipErr)
				m.warned = true
			}
			m.mu.Unlock()
			return
		}
		s := victim.Value.(*staged)
		m.mu.Unlock()
		err := m.st.DeleteXorb(context.Background(), s.hash)
		m.mu.Lock()
		switch {
		case err != nil:
			skip[victim], skipErr = true, err
		case m.index[s.hash] == victim: // a copy re-inserted meanwhile is a new entry and stays
			m.forget(victim)
		}
		m.mu.Unlock()
	}
}

// seed asks the best-ranked live seeds not yet holding each local xorb to pull it, within the job slots.
func (m *staging) seed() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for e := m.lru.Front(); e != nil; e = e.Next() {
		s := e.Value.(*staged)
		for id, next := range s.asked {
			if !now.Before(next) {
				delete(s.asked, id)
			}
		}
		ranked := m.reg.rank(s.hash.String())
		for _, p := range ranked[:min(len(ranked), m.cfg.replicas)] {
			if _, asked := s.asked[p.ID]; p.holder || asked {
				continue
			}
			select {
			case m.sem <- struct{}{}:
			default:
				return
			}
			s.asked[p.ID] = now.Add(m.cfg.retry)
			m.wg.Add(1)
			go m.pull(p.Peer, s.hash)
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
	resp, err := control.PostJSON(ctx, m.client, p.URL+apis.PullPath, m.cfg.secret, apis.PullRequest{Hash: hash.String()})
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
