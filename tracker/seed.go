package tracker

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

// pullTimeout bounds one pull request to a peer.
const pullTimeout = 2 * time.Second

// seedConfig tunes seeding; zero values take the noted defaults.
type seedConfig struct {
	replicas int           // live holders asked for per xorb and per shard
	secret   string        // bearer token of pull requests
	retry    time.Duration // pause per (object, peer) after a pull request, default 1m
	tick     time.Duration // pass period besides wakes, default 1s
	metrics  *trackerMetrics
}

// maxPulls bounds parallel pull requests.
const maxPulls = 8

// ask is an object and the peer asked to pull it.
type ask struct {
	object
	peer string
}

// seeder asks live peers to pull xorbs and shards until each has replicas live holders.
type seeder struct {
	cfg    seedConfig
	reg    *registry
	client *http.Client // carries the secret, never follows redirects
	wake   chan struct{}
	sem    chan struct{} // bounds in-flight pull requests
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	mu    sync.Mutex          // guards want and asked; ordered before reg.mu, never held across network calls
	want  map[object]struct{} // objects that may lack holders
	asked map[ask]time.Time   // when each ask may be repeated
}

func newSeeder(reg *registry, cfg seedConfig) *seeder {
	cfg.retry = cmp.Or(cfg.retry, time.Minute)
	cfg.tick = cmp.Or(cfg.tick, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	s := &seeder{
		cfg:    cfg,
		reg:    reg,
		client: apis.NewClient(apis.NewTransport()),
		wake:   make(chan struct{}, 1),
		sem:    make(chan struct{}, maxPulls),
		ctx:    ctx,
		cancel: cancel,
		want:   map[object]struct{}{},
		asked:  map[ask]time.Time{},
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// run is the seeding goroutine: one pass per tick or wake until Close.
func (s *seeder) run() {
	defer s.wg.Done()
	tick := time.NewTicker(s.cfg.tick)
	defer tick.Stop()
	for {
		s.seed()
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
		case <-s.wake:
		}
	}
}

// note adds objects to the wanted ones and wakes a pass.
func (s *seeder) note(objects []object) {
	s.mu.Lock()
	for _, o := range objects {
		s.want[o] = struct{}{}
	}
	s.mu.Unlock()
	s.kick()
}

// seed asks the top-ranked live non-holders of each wanted object to pull it, in one request per peer, until every pull slot is busy; an object leaves want once it has no holder or replicas of them.
func (s *seeder) seed() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for a, next := range s.asked {
		if !now.Before(next) {
			delete(s.asked, a)
		}
	}
	live := s.reg.live()
	batches := map[apis.Peer][]object{}
	for o := range s.want {
		holders := s.reg.holders(o)
		if len(holders) == 0 || len(holders) >= s.cfg.replicas {
			delete(s.want, o)
			continue
		}
		candidates := slices.DeleteFunc(apis.Rank(o.hash, live), func(p apis.Peer) bool {
			return slices.ContainsFunc(holders, func(h apis.Peer) bool { return h.ID == p.ID })
		})
		// An asked candidate still counts towards the replicas, so its ask is not passed on to the next one.
		for _, p := range candidates[:min(s.cfg.replicas-len(holders), len(candidates))] {
			if _, ok := s.asked[ask{o, p.ID}]; !ok {
				batches[p] = append(batches[p], o)
			}
		}
	}
	for _, p := range slices.SortedFunc(maps.Keys(batches), func(a, b apis.Peer) int { return strings.Compare(a.ID, b.ID) }) {
		batch := batches[p][:min(len(batches[p]), apis.MaxPullBatch)]
		select {
		case s.sem <- struct{}{}:
		default:
			return
		}
		var req apis.PullRequest
		for _, o := range batch {
			s.asked[ask{o, p.ID}] = now.Add(s.cfg.retry)
			if o.shard {
				req.Shards = append(req.Shards, o.hash)
			} else {
				req.Xorbs = append(req.Xorbs, o.hash)
			}
		}
		s.wg.Add(1)
		go s.pull(p, req)
	}
}

// pull asks p to pull the xorbs and shards req lists as seed copies; only 202 counts, failures are logged unless the seeder is closing.
func (s *seeder) pull(p apis.Peer, req apis.PullRequest) {
	defer s.wg.Done()
	defer s.kick()
	defer func() { <-s.sem }()
	req.Seed = true
	ctx, cancel := context.WithTimeout(s.ctx, pullTimeout)
	err := apis.Pull(ctx, s.client, p.URL, s.cfg.secret, req)
	cancel()
	s.cfg.metrics.seedRequest(err == nil)
	if err != nil && s.ctx.Err() == nil {
		slog.Warn("tracker: seed request", "peer", p.ID, "xorbs", len(req.Xorbs), "shards", len(req.Shards), "err", err)
	}
}

// kick schedules a pass without blocking; safe after Close.
func (s *seeder) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Close stops seeding, cancels the pull requests in flight and waits for them.
func (s *seeder) Close() error {
	s.once.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.client.CloseIdleConnections()
	})
	return nil
}
