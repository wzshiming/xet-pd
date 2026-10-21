package gateway

import (
	"cmp"
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

	mu     sync.Mutex                      // guards the fields below; never held across registry, store or network calls
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
	xorbs, err := inventory.Scan(context.Background(), st)
	if err != nil {
		cancel()
		return nil, err
	}
	for _, x := range xorbs {
		m.xorbs.Add(x.Hash, x.Size, staged{asked: map[string]time.Time{}})
	}
	st.onStored, st.onRead, st.onIdle = m.stored, m.touch, m.kick
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		tick := time.NewTicker(m.cfg.tick)
		defer tick.Stop()
		type ask struct {
			p    apis.Peer
			hash xet.XorbHash
		}
		var order []*lru.Entry[xet.XorbHash, staged]
		for {
			if m.cfg.cacheSize > 0 {
				m.mu.Lock()
				over := m.xorbs.Usage() > m.cfg.cacheSize
				if over {
					order = slices.AppendSeq(order[:0], m.xorbs.Oldest())
				} else {
					m.warned = false
				}
				m.mu.Unlock()
				if over {
					view := m.reg.live()
					var skipErr error
					for _, e := range order {
						if view.holderCount(e.Key.String()) < m.cfg.minSeeds {
							continue
						}
						m.mu.Lock()
						current := m.xorbs.Get(e.Key) == e
						m.mu.Unlock()
						if !current {
							continue
						}
						err := m.st.DeleteXorb(context.Background(), e.Key)
						m.mu.Lock()
						if err != nil {
							skipErr = err
						} else {
							m.xorbs.Remove(e) // a copy re-inserted meanwhile is a new entry and stays
							if over = m.xorbs.Usage() > m.cfg.cacheSize; !over {
								m.warned = false
							}
						}
						m.mu.Unlock()
						if !over {
							break
						}
					}
					if over {
						m.mu.Lock()
						if !m.warned {
							slog.Warn("gateway: local xorbs over the cache size, nothing evictable yet", "usage", m.xorbs.Usage(), "limit", m.cfg.cacheSize, "err", skipErr)
							m.warned = true
						}
						m.mu.Unlock()
					}
				}
			}
			view := m.reg.live()
			now := time.Now()
			m.mu.Lock()
			order = slices.AppendSeq(order[:0], m.xorbs.Oldest())
			m.mu.Unlock()
			var asks []ask
		seeding:
			for _, e := range order {
				ranked := view.rank(e.Key.String())
				m.mu.Lock()
				if m.xorbs.Get(e.Key) != e {
					m.mu.Unlock()
					continue
				}
				for id, next := range e.V.asked {
					if !now.Before(next) {
						delete(e.V.asked, id)
					}
				}
				for _, p := range ranked[:min(len(ranked), m.cfg.replicas)] {
					if _, asked := e.V.asked[p.ID]; p.holder || asked {
						continue
					}
					select {
					case m.sem <- struct{}{}:
					default:
						m.mu.Unlock()
						break seeding
					}
					e.V.asked[p.ID] = now.Add(m.cfg.retry)
					asks = append(asks, ask{p.Peer, e.Key})
				}
				m.mu.Unlock()
			}
			for _, a := range asks {
				m.wg.Add(1)
				go func(p apis.Peer, hash xet.XorbHash) {
					defer m.wg.Done()
					defer m.kick()
					defer func() { <-m.sem }()
					ctx, cancel := context.WithTimeout(m.ctx, pullTimeout)
					resp, err := apis.PostJSON(ctx, m.client, p.URL+apis.PullPath, m.cfg.secret, apis.PullRequest{Hash: hash.String()})
					if err == nil {
						_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
						_ = resp.Body.Close()
						if resp.StatusCode != http.StatusAccepted {
							err = errors.New(resp.Status)
						}
					}
					cancel()
					if err != nil && m.ctx.Err() == nil {
						slog.Warn("gateway: pull request", "peer", p.ID, "hash", hash, "err", err)
					}
				}(a.p, a.hash)
			}
			select {
			case <-m.ctx.Done():
				return
			case <-tick.C:
			case <-m.wake:
			}
		}
	}()
	return m, nil
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
